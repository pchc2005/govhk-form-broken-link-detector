from flask import Flask, render_template_string, send_file, redirect, url_for
import requests
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import pandas as pd
import os
import json
import threading
import time

# ==== CONFIG ====
EFORM_URL = "https://www.iamsmart.gov.hk/data/eform.txt"
TIMEOUT = 15
CSV_FILE = "link_history.csv"
JSON_FILE = "history.json"
AUTO_REFRESH_INTERVAL = 900  # seconds (15 min)
MAX_WORKERS = 10

app = Flask(__name__)

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
</head>
<body class="p-4">
    <h1>Government e-Form Link Status</h1>
    <p>Last checked: {{ last_checked or 'N/A' }}</p>
    {% if last_auto_refresh %}
    <p>Last auto-refresh: {{ last_auto_refresh }}</p>
    {% endif %}
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
        });
    </script>
</body>
</html>
"""

# ==== DATA FUNCTIONS ====

def get_links_with_meta():
    """Fetch JSON and return list of dicts with dept, title, lang, url."""
    resp = requests.get(EFORM_URL, timeout=TIMEOUT)
    resp.raise_for_status()
    forms = resp.json()

    records = []
    for form in forms:
        # Department/title priority: en > tc > sc
        if form.get("en_department") and form.get("en_title"):
            dept = form["en_department"].strip()
            title = form["en_title"].strip()
        elif form.get("tc_department") and form.get("tc_title"):
            dept = form["tc_department"].strip()
            title = form["tc_title"].strip()
        else:
            dept = form.get("sc_department", "").strip()
            title = form.get("sc_title", "").strip()

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

def check_link(url):
    try:
        r = requests.head(url, allow_redirects=True, timeout=TIMEOUT)
        if r.status_code >= 400:
            return f"Error {r.status_code}"
        return "OK"
    except requests.RequestException:
        return "Broken"

def run_check(auto=False):
    records = get_links_with_meta()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    history = {}
    if os.path.exists(JSON_FILE):
        with open(JSON_FILE, 'r', encoding='utf-8') as f:
            history = json.load(f)
            if not isinstance(history, dict):
                history = {}

    old_results = history.get("results", [])

    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        statuses = list(executor.map(lambda rec: check_link(rec["url"]), records))

    for rec, status in zip(records, statuses):
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
    return results

def background_check():
    threading.Thread(target=run_check, kwargs={"auto": False}, daemon=True).start()

def background_scheduler():
    while True:
        try:
            print(f"[Scheduler] Auto-refresh at {datetime.now()}")
            run_check(auto=True)
        except Exception as e:
            print(f"[Scheduler] Error: {e}")
        time.sleep(AUTO_REFRESH_INTERVAL)

# Start scheduler on app init
threading.Thread(target=background_scheduler, daemon=True).start()

# ==== ROUTES ====
@app.route("/")
def index():
    if not os.path.exists(JSON_FILE):
        background_check()
        results = []
        last_checked = None
        last_auto_refresh = None
    else:
        with open(JSON_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        results = data.get("results", [])
        last_checked = data.get("last_checked")
        last_auto_refresh = data.get("last_auto_refresh")

    return render_template_string(
        HTML_TEMPLATE,
        results=results,
        last_checked=last_checked,
        last_auto_refresh=last_auto_refresh
    )

@app.route("/refresh")
def refresh():
    background_check()
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

# ==== ENTRYPOINT ====
if __name__ == "__main__":
    # app.run(debug=True)
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)
