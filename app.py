from flask import Flask, render_template_string, send_file, redirect, url_for, jsonify, Response
import requests
from datetime import datetime
import pandas as pd
import os
import json
import threading
import time
import queue
import functools
from threading import Lock

# ==== CONFIG ====
EFORM_URL = "https://www.iamsmart.gov.hk/data/eform.txt"
TIMEOUT = 15
CSV_FILE = "link_history.csv"
JSON_FILE = "history.json"
AUTO_REFRESH_INTERVAL = 3600  # 1 hour in seconds
MAX_WORKERS = 10

SELF_PING_INTERVAL = 600  # seconds (10 minutes)
SELF_URL = os.environ.get("SELF_URL")  # set in Render dashboard

app = Flask(__name__)

# Progress and locking mechanism
check_lock = Lock()
progress_queue = queue.Queue()
is_checking = False

# Global progress tracking
progress_status = {
    "state": "idle",  # idle, fetching, checking, complete
    "message": "",
    "current": 0,
    "total": 0,
    "current_item": ""
}

def with_check_lock(f):
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        global is_checking
        if not check_lock.acquire(blocking=False):
            return jsonify({"error": "Another check is in progress"}), 409
        is_checking = True
        try:
            return f(*args, **kwargs)
        finally:
            is_checking = False
            check_lock.release()
    return wrapper

def update_progress(state, message="", current=0, total=0, current_item=""):
    progress_data = {
        "state": state,
        "message": message,
        "current": current,
        "total": total,
        "current_item": current_item
    }
    progress_queue.put(progress_data)

# ==== HTML TEMPLATE ====
HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Gov e-Form Link Checker</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    <script src="https://code.jquery.com/jquery-3.6.0.min.js"></script>
    <script src="https://cdn.datatables.net/1.13.4/js/jquery.dataTables.min.js"></script>
    <link rel="stylesheet" href="https://cdn.datatables.net/1.13.4/css/jquery.dataTables.min.css">
    <style>
        .overlay {
            display: none;
            position: fixed;
            top: 0;
            left: 0;
            width: 100%;
            height: 100%;
            background: rgba(0, 0, 0, 0.5);
            z-index: 1000;
        }
        .progress-popup {
            position: fixed;
            top: 50%;
            left: 50%;
            transform: translate(-50%, -50%);
            width: 80%;
            max-width: 600px;
            background: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 0 20px rgba(0, 0, 0, 0.2);
            z-index: 1001;
        }
        .timestamp-info {
            position: absolute;
            top: 10px;
            right: 20px;
            text-align: right;
            font-size: 0.85rem;
            color: #666;
        }
    </style>
</head>
<body class="p-4">
    <!-- Progress Overlay -->
    <div id="progressSection" class="overlay">
        <div class="progress-popup">
            <h4 id="progressState">Status: <span id="progressStateText">Idle</span></h4>
            <p id="progressMessage"></p>
            <div class="progress">
                <div id="progressBar" class="progress-bar progress-bar-striped progress-bar-animated" 
                     role="progressbar" style="width: 0%"></div>
            </div>
        </div>
    </div>

    <!-- Timestamp Info -->
    <div class="timestamp-info">
        <div>Last checked: {{ last_checked or 'N/A' }}</div>
        {% if last_auto_refresh %}
        <div>Last auto-refresh: {{ last_auto_refresh }}</div>
        {% endif %}
    </div>

    <h1>Government e-Form Link Status</h1>
    
    <!-- Status Cards -->
    <div class="row mb-3">
        <div class="col-sm">
            <div class="card border-success mb-3">
            <div class="card-body text-success">
                <h5 class="card-title">✅ OK </h5>
                <p class="card-text display-6">{{ ok_count }}</p>
            </div>
            </div>
        </div>
        <div class="col-sm">
            <div class="card border-danger mb-3">
            <div class="card-body text-danger">
                <h5 class="card-title">❌ Errors / Broken</h5>
                <p class="card-text display-6">{{ error_count }}</p>
            </div>
            </div>
        </div>
        <div class="col-sm">
            <div class="card border-primary mb-3">
            <div class="card-body text-primary">
                <h5 class="card-title">🔢 Total Links</h5>
                <p class="card-text display-6">{{ total_count }}</p>
            </div>
            </div>
        </div>
    </div>
    <a href="{{ url_for('refresh') }}" class="btn btn-primary mb-3">🔄 Refresh</a>
    <a href="{{ url_for('download_csv') }}" class="btn btn-success mb-3">⬇ Export All (CSV)</a>
    <a href="{{ url_for('download_errors') }}" class="btn btn-danger mb-3">⬇ Export Errors Only</a>
    <table id="linkTable" class="table table-striped table-hover">
        <thead class="table-dark">
            <tr>
                <th>#</th>
                <th>Department</th>
                <th>Title</th>
                <th>Lang</th>
                <th>URL</th>
                <th>Status</th>
                <th>First Broken</th>
            </tr>
        </thead>
        <tbody>
        {% for row in results %}
            <tr>
                <td>{{ loop.index }}</td>
                <td>{{ row['department'] }}</td>
                <td>{{ row['title'] }}</td>
                <td>{{ row['lang'] }}</td>
                <td><a href="{{ row['url'] }}" target="_blank">{{ row['url'] }}</a></td>
                <td class="{% if row['status'] == 'OK' %}text-success{% elif row['status'].startswith('Error') or row['status'] == 'Broken' %}text-danger{% else %}text-warning{% endif %}">
                    {{ row['status'] }}
                </td>
                <td>{{ row['first_broken'] or '' }}</td>
            </tr>
        {% endfor %}
        </tbody>
    </table>
    <script>
        $(document).ready(function() {
            $('#linkTable').DataTable({
                "order": [[5, "asc"]]
            });
            
            let eventSource;
            
            function startEventSource() {
				if (eventSource) {
					eventSource.close();
				}
                eventSource = new EventSource('/progress-stream');
                
                eventSource.onmessage = function(event) {
                    const data = JSON.parse(event.data);
                    if (data.state !== 'idle') {
                        $('#progressSection').show();
                        $('#progressStateText').text(data.state);
                        $('#progressMessage').text(data.message);
                        if (data.total > 0) {
                            let percentage = (data.current / data.total) * 100;
                            $('#progressBar').css('width', percentage + '%');
                        }
                    }
                    
                    if (data.state === 'complete') {
                        eventSource.close();
                        location.reload();
                    }
                };
                
                eventSource.onerror = function() {
					console.error('EventSource failed:', e);
					eventSource.close();
					$('#progressSection').hide();
					// Try to reconnect after a delay
					setTimeout(startEventSource, 3000);
                };
            }
            
            // Start progress tracking when refresh is clicked
            $('a[href="/refresh"]').click(function(e) {
                e.preventDefault();
                $('#progressSection').show();
                $.ajax({
                    url: $(this).attr('href'),
                    method: 'GET',
                    success: function() {
                        startEventSource();
                    },
                    error: function(xhr) {
                        if (xhr.status === 409) {
                            alert('Another check is currently in progress');
                            $('#progressSection').hide();
                        }
                    }
                });
            });
        });
    </script>
</body>
</html>
"""

# ==== DATA FUNCTIONS ====

def get_links_with_meta():
    update_progress("fetching", "Fetching forms data...")
    # Try up to 3 times before giving up
    for attempt in range(3):
        try:
            print(f"Fetching forms, attempt {attempt+1}...")
            resp = requests.get(EFORM_URL, timeout=15)
            resp.raise_for_status()
            forms = resp.json()  # Will raise ValueError if invalid
            if not isinstance(forms, list) or not forms:
                raise ValueError("Empty or invalid JSON structure")
            break
        except requests.exceptions.Timeout:
            print("⚠ Timeout when fetching data.")
        except requests.exceptions.RequestException as e:
            print(f"⚠ Request error: {e}")
        except ValueError as ve:
            print(f"⚠ JSON parse error: {ve}")
        time.sleep(2)
    else:
        # All attempts failed
        print("❌ All attempts to fetch forms failed. Returning empty list.")
        return []

    records = []
    for form in forms:
        if form.get("en_department") and form.get("en_title"):
            dept, title = form["en_department"].strip(), form["en_title"].strip()
        elif form.get("tc_department") and form.get("tc_title"):
            dept, title = form["tc_department"].strip(), form["tc_title"].strip()
        else:
            dept, title = form.get("sc_department", "").strip(), form.get("sc_title", "").strip()

        for lang in ("en", "tc", "sc"):
            url_key = f"{lang}_url"
            if form.get(url_key):
                records.append({
                    "department": dept,
                    "title": title,
                    "lang": lang,
                    "url": form[url_key].strip()
                })
    return records


def check_link(url, index, total, department, title, lang):
    update_progress("checking", f"Checking {department} - {title} ({lang})", index, total)
    try:
        r = requests.head(url, allow_redirects=True, timeout=TIMEOUT)
        if r.status_code >= 400:
            return f"Error {r.status_code}"
        return "OK"
    except requests.RequestException:
        return "Broken"

@with_check_lock
def run_check(auto=False):
    update_progress("fetching", "Getting forms data...")
    records = get_links_with_meta()
    if not records:
        update_progress("complete", "No records found")
        return json.load(open(JSON_FILE))["results"] if os.path.exists(JSON_FILE) else []
    
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    history = {}
    if os.path.exists(JSON_FILE):
        with open(JSON_FILE, 'r', encoding='utf-8') as f:
            history = json.load(f)
            if not isinstance(history, dict):
                history = {}

    old_results = history.get("results", [])

    results = []
    total = len(records)
    
    # Sequential processing instead of ThreadPoolExecutor
    for index, rec in enumerate(records, 1):
        status = check_link(
            rec["url"], 
            index, 
            total, 
            rec["department"],
            rec["title"],
            rec["lang"]
        )
        
        first_broken = None
        if status != "OK":
            prev = next((h for h in old_results if h["url"] == rec["url"] and h["status"] != "OK"), None)
            first_broken = prev["first_broken"] if prev and prev.get("first_broken") else now

        results.append({
            "department": rec["department"],
            "title": rec["title"],
            "lang": rec["lang"],
            "url": rec["url"],
            "status": status,
            "first_broken": first_broken
        })

    status_order = {'Error': 0, 'Broken': 0, 'OK': 1}
    results.sort(key=lambda r: (status_order.get(r["status"].split()[0], 1), r["department"], r["title"]))

    data = {
        "results": results,
        "last_checked": now
    }
    if auto:
        data["last_auto_refresh"] = now
    elif "last_auto_refresh" in history:
        data["last_auto_refresh"] = history["last_auto_refresh"]

    with open(JSON_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    pd.DataFrame(results).to_csv(CSV_FILE, index=False)
    update_progress("complete", "Check completed")
    return results

def background_scheduler():
    while True:
        try:
            print(f"[Scheduler] Auto-refresh at {datetime.now()}")
            run_check(auto=True)
        except Exception as e:
            print(f"[Scheduler] Error: {e}")
        time.sleep(AUTO_REFRESH_INTERVAL)
        
def self_pinger():
    """Periodically call the app's own URL to keep Render awake."""
    if not SELF_URL:
        print("No SELF_URL set — self-pinger disabled.")
        return
    # Small delay so we don't ping before the app is ready
    time.sleep(30)
    while True:
        try:
            print(f"[Self‑Pinger] Pinging {SELF_URL} at {datetime.now()}")
            requests.get(SELF_URL, timeout=10)
        except Exception as e:
            print(f"[Self‑Pinger] Error: {e}")
        time.sleep(SELF_PING_INTERVAL)

# Start scheduler on app init
threading.Thread(target=background_scheduler, daemon=True).start()
threading.Thread(target=self_pinger, daemon=True).start()

# ==== ROUTES ====
@app.route("/")
def index():
    if not os.path.exists(JSON_FILE):
        results = []
        last_checked = None
        last_auto_refresh = None
    else:
        with open(JSON_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        results = data.get("results", [])
        last_checked = data.get("last_checked")
        last_auto_refresh = data.get("last_auto_refresh")
        
    ok_count = sum(1 for r in results if r["status"] == "OK")
    error_count = sum(1 for r in results if r["status"] != "OK")
    total_count = len(results)

    return render_template_string(
        HTML_TEMPLATE,
        results=results,
        last_checked=last_checked,
        last_auto_refresh=last_auto_refresh,
        ok_count=ok_count,
        error_count=error_count,
        total_count=total_count
    )

@app.route("/refresh")
def refresh():
    run_check()
    return redirect(url_for('index'))

@app.route("/download_csv")
def download_csv():
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return send_file(CSV_FILE, as_attachment=True, download_name=f"link_history_{timestamp}.csv")

@app.route("/download_errors")
def download_errors():
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    df = pd.read_csv(CSV_FILE)
    errors = df[df['status'] != 'OK']
    errors_file = f"errors_only_{timestamp}.csv"
    errors.to_csv(errors_file, index=False)
    return send_file(errors_file, as_attachment=True)

@app.route('/progress-stream')
def progress_stream():
    def generate():
        while True:
            try:
                # Get progress update with timeout
                progress_data = progress_queue.get(timeout=30)
                yield f"data: {json.dumps(progress_data)}\n\n"
                
                if progress_data['state'] == 'complete':
                    break
            except queue.Empty:
                # Send keep-alive ping every 30 seconds
                yield f"data: {json.dumps({'state': 'idle', 'message': 'waiting...', 'current': 0, 'total': 0})}\n\n"
    
    return Response(generate(), mimetype='text/event-stream')

# ==== ENTRYPOINT ====
if __name__ == "__main__":
    # app.run(debug=True)
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)
