from flask import Flask, render_template, send_file, redirect, url_for, jsonify, Response
import requests
from requests.exceptions import SSLError
from datetime import datetime
import pandas as pd
import os
import json
import threading
import time
import queue
import functools
import csv
from threading import Lock

# ==== CONFIG ====
EFORM_URL = "https://www.iamsmart.gov.hk/data/eform.txt"
SERVICES_URL = "https://www.iamsmart.gov.hk/data/gov_dep_data.txt"

TIMEOUT = 15

CSV_FILE = "link_history.csv"
JSON_FILE = "history.json"
CSV_FILE_SERVICES = "link_history_services.csv"
JSON_FILE_SERVICES = "history_services.json"

AUTO_REFRESH_INTERVAL = 3600  # 1 hour in seconds
MAX_WORKERS = 10

SELF_PING_INTERVAL = 600  # seconds (10 minutes)
SELF_URL = os.environ.get("SELF_URL")  # set in Render dashboard

DEBUG_MODE = False  # Set to False to disable debug logging
DEBUG_LOG_FILE = "link_check_debug.csv"


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
progress_status_dataset = None

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
		"current_item": current_item,
        "dataset": progress_status_dataset
	}
	progress_queue.put(progress_data)

# ==== DATA FUNCTIONS ====

def get_services_with_meta():
	update_progress("fetching", "[services] Fetching services data...")
	for attempt in range(3):
		try:
			resp = requests.get(SERVICES_URL, timeout=TIMEOUT)
			resp.encoding = 'utf-8-sig'  # force correct decoding
			resp.raise_for_status()
			items = resp.json()
			if not isinstance(items, list) or not items:
				raise ValueError("Empty/invalid JSON")
			break
		except Exception as e:
			print(f"[Services] Fetch error: {e}")
			time.sleep(2)
	else:
		return []

	records = []
	for form in items:
		dept = form.get("en_department") or form.get("tc_department") or form.get("sc_department") or ""
		dept = dept.strip()
		title = (form.get("en_fun") or "").strip() or (form.get("en_fun_a") or "").strip()
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

def get_links_with_meta():
	update_progress("fetching", "Fetching forms data...")
	# Try up to 3 times before giving up
	for attempt in range(3):
		try:
			print(f"Fetching forms, attempt {attempt+1}...")
			resp = requests.get(EFORM_URL, timeout=15)
			resp.encoding = 'utf-8-sig'  # force correct decoding
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

    def try_request(method, verify=True):
        try:
            r = requests.request(method, url, allow_redirects=True, timeout=TIMEOUT, stream=True, verify=verify)
            status_code = r.status_code
            r.close()
            return status_code, None, verify
        except SSLError as e:
            err_str = str(e)
            # Special-case: unsafe legacy renegotiation
            if "UNSAFE_LEGACY_RENEGOTIATION_DISABLED" in err_str:
                return None, "TLS Legacy Renegotiation Unsupported", verify
            
            # Special-case: handshake failure
            if "SSLV3_ALERT_HANDSHAKE_FAILURE" in err_str:
                return None, "TLS Handshake Failure", verify
            
            # Retry once with verify=False for other SSL errors
            if verify:
                try:
                    r = requests.request(method, url, allow_redirects=True, timeout=TIMEOUT, stream=True, verify=False)
                    status_code = r.status_code
                    r.close()
                    return status_code, f"SSL verify failed, bypassed: {err_str}", False
                except requests.RequestException as e2:
                    return None, f"SSL bypass also failed: {e2}", False
            return None, f"SSL error: {err_str}", verify
        except requests.RequestException as e:
            return None, str(e), verify

    head_status, head_error, head_verified = try_request("HEAD")
    get_status, get_error, get_verified = None, None, True

    # Fall back to GET if HEAD fails or is blocked
    if head_status is None or head_status in (405, 403) or head_status >= 400:
        get_status, get_error, get_verified = try_request("GET")

    # Decide final status
    if head_error == "TLS Legacy Renegotiation Unsupported" or get_error == "TLS Legacy Renegotiation Unsupported":
        final_status = "TLS Legacy Renegotiation Unsupported"
    elif head_status is None and get_status is None:
        final_status = "Broken"
    else:
        status_to_use = get_status if get_status is not None else head_status
        if status_to_use is not None and status_to_use >= 400:
            if status_to_use in (401, 403):
                final_status = f"Restricted {status_to_use}"
            else:
                final_status = f"Error {status_to_use}"
        else:
            final_status = "OK"

    # Debug logging
    if DEBUG_MODE:
        try:
            file_exists = os.path.exists(DEBUG_LOG_FILE)
            with open(DEBUG_LOG_FILE, 'a', newline='', encoding='utf-8-sig') as f:
                writer = csv.writer(f)
                if not file_exists:
                    writer.writerow([
                        "timestamp", "department", "title", "lang", "url",
                        "head_status", "head_error", "head_verified",
                        "get_status", "get_error", "get_verified",
                        "final_status"
                    ])
                writer.writerow([
                    datetime.now().isoformat(), department, title, lang, url,
                    head_status, head_error, head_verified,
                    get_status, get_error, get_verified,
                    final_status
                ])
        except Exception as e:
            print(f"[DebugLog] Failed to write debug log: {e}")

    return final_status

@with_check_lock
def run_check(auto=False, dataset="eforms"):
    global progress_status_dataset
    progress_status_dataset = dataset

    if dataset == "eforms":
        update_progress("fetching", "[eforms] Getting forms data...")
        records = get_links_with_meta()
        json_file, csv_file = JSON_FILE, CSV_FILE
    elif dataset == "services":
        update_progress("fetching", "[services] Getting services data...")
        records = get_services_with_meta()
        json_file, csv_file = JSON_FILE_SERVICES, CSV_FILE_SERVICES
    else:
        update_progress("complete", f"Unknown dataset: {dataset}")
        return []

    if not records:
        update_progress("complete", f"No records found for {dataset}")
        return json.load(open(json_file))["results"] if os.path.exists(json_file) else []

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    history = {}
    if os.path.exists(json_file):
        with open(json_file, 'r', encoding='utf-8') as f:
            history = json.load(f) or {}

    old_results = history.get("results", [])
    results = []
    total = len(records)

    for index, rec in enumerate(records, 1):
        status = check_link(rec["url"], index, total, rec["department"], rec["title"], rec["lang"])
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

    data = {"results": results, "last_checked": now}
    if auto:
        data["last_auto_refresh"] = now
    elif "last_auto_refresh" in history:
        data["last_auto_refresh"] = history["last_auto_refresh"]

    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    pd.DataFrame(results).to_csv(csv_file, index=False, encoding='utf-8-sig')

    update_progress("complete", f"[{dataset}] Check completed")
    progress_status_dataset = None
    return results

def load_history(path):
    if not os.path.exists(path):
        return {"results": [], "last_checked": None, "last_auto_refresh": None}
    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    return {
        "results": data.get("results", []),
        "last_checked": data.get("last_checked"),
        "last_auto_refresh": data.get("last_auto_refresh")
    }

def background_scheduler():
	while True:
		try:
			print(f"[Scheduler] Auto-refresh at {datetime.now()}")
			run_check(auto=True, dataset="eforms")
			run_check(auto=True, dataset="services")
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
  
def classify_counts(results):
    ok_count = 0
    warning_count = 0
    error_count = 0

    for r in results:
        status = r["status"]

        # --- Warnings ---
        # Any TLS-level incompatibility or SSL bypass
        if (
            status.startswith("TLS")  # e.g. TLS Legacy Renegotiation Unsupported, TLS Handshake Failure
            or "SSL verify failed" in status
            or "SSL bypass" in status
        ):
            warning_count += 1

        # --- OK ---
        elif status == "OK":
            ok_count += 1

        # --- Errors ---
        else:
            error_count += 1

    return ok_count, warning_count, error_count

# Start scheduler on app init
threading.Thread(target=background_scheduler, daemon=True).start()
threading.Thread(target=self_pinger, daemon=True).start()

# ==== ROUTES ====
@app.route("/")
def index():
    eforms_data = load_history(JSON_FILE)
    services_data = load_history(JSON_FILE_SERVICES)

    eforms_ok_count, eforms_warning_count, eforms_error_count = classify_counts(eforms_data["results"])
    services_ok_count, services_warning_count, services_error_count = classify_counts(services_data["results"])

    return render_template(
        'index.html',
        # E‑Forms data
        eforms_results=eforms_data["results"],
        eforms_last_checked=eforms_data["last_checked"],
        eforms_last_auto_refresh=eforms_data["last_auto_refresh"],
        eforms_ok_count=eforms_ok_count,
        eforms_warning_count=eforms_warning_count,
        eforms_error_count=eforms_error_count,
        eforms_total_count=len(eforms_data["results"]),

        # Services data
        services_results=services_data["results"],
        services_last_checked=services_data["last_checked"],
        services_last_auto_refresh=services_data["last_auto_refresh"],
        services_ok_count=services_ok_count,
        services_error_count=services_error_count,
        services_warning_count=services_warning_count,
        services_total_count=len(services_data["results"]),

        # Global flags
        is_checking=is_checking
    )


@app.route("/refresh/<dataset>")
def refresh(dataset):
    if dataset not in ("eforms", "services"):
        return jsonify({"error": "Unknown dataset"}), 400
    update_progress("starting", f"[{dataset}] Warming up...", 0, 0)
    run_check(dataset=dataset)
    return redirect(url_for('index'))

@app.route("/download_csv/<dataset>")
def download_csv(dataset):
    if dataset == "eforms":
        path = CSV_FILE
    elif dataset == "services":
        path = CSV_FILE_SERVICES
    else:
        return jsonify({"error": "Unknown dataset"}), 400
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return send_file(path, as_attachment=True, download_name=f"link_history_{dataset}_{timestamp}.csv")

@app.route("/download_errors/<dataset>")
def download_errors(dataset):
    if dataset == "eforms":
        path = CSV_FILE
    elif dataset == "services":
        path = CSV_FILE_SERVICES
    else:
        return jsonify({"error": "Unknown dataset"}), 400

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    df = pd.read_csv(path, encoding='utf-8')
    errors = df[df['status'] != 'OK']
    errors_file = f"errors_only_{dataset}_{timestamp}.csv"
    errors.to_csv(errors_file, index=False)
    return send_file(errors_file, as_attachment=True)

@app.route('/progress-stream')
def progress_stream():
	def generate():
		try:
			while True:
				try:
					# Wait at most 10s for new progress info
					progress_data = progress_queue.get(timeout=10)
					yield f"data: {json.dumps(progress_data)}\n\n"

					if progress_data['state'] == 'complete':
						break

				except queue.Empty:
					# SSE comment = ignored by JS, keeps connection alive quietly
					yield ":keep-alive\n\n"

		except GeneratorExit:
			# Client disconnected or worker shutting down
			pass

	return Response(generate(), mimetype='text/event-stream')

# ==== ENTRYPOINT ====
if __name__ == "__main__":
	# app.run(debug=True)
	port = int(os.environ.get("PORT", 5000))
	app.run(host="0.0.0.0", port=port, debug=True)
