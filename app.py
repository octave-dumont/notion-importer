"""
app.py — Notion Prospection Importer
Flask backend with SSE streaming progress.
"""

import os
import re
import io
import json
import time
import threading
import uuid
import requests
import pandas as pd
from flask import Flask, request, jsonify, Response, render_template, stream_with_context
from flask_cors import CORS
from werkzeug.utils import secure_filename

app = Flask(__name__)
CORS(app)
app.config["MAX_CONTENT_LENGTH"] = 10 * 1024 * 1024  # 10 MB max

# ── Notion config ─────────────────────────────────────────────────────────────

DATABASE_ID        = os.environ.get("NOTION_DATABASE_ID", "e243602070b1493fad77a2cf0c24f54b")
MEMBRES_DB_ID      = os.environ.get("NOTION_MEMBRES_DB_ID", "558f2f0da6d44f5a8c21eac0edaff687")
NOTION_API_VERSION = "2022-06-28"
NOTION_BASE_URL    = "https://api.notion.com/v1"

NOTION_SCHEMA = {
    "Nom de l'entreprise":   "title",
    "Mail contact":          "email",
    "Numéro contact":        "phone_number",
    "Prénom-Nom contact":    "rich_text",
    "Poste contact":         "rich_text",
    "Objet de la demande":   "rich_text",
    "Notes":                 "rich_text",
    "Statut du Partenariat": "select",
    "Type d'entreprise":     "select",
    "Date d'envoi":          "date",
}
VALID_STATUT   = {"En Cours", "Demande", "Done"}
VALID_TYPE_ENT = {"PE", "VC", "Cabinet de conseil", "Startup", "Grand Groupe", "Association"}

# ── In-memory job store ───────────────────────────────────────────────────────

jobs = {}
jobs_lock = threading.Lock()

# ── Column normalization ──────────────────────────────────────────────────────

def _normalize(s: str) -> str:
    s = s.lower().strip()
    for k, v in {"é":"e","è":"e","ê":"e","ë":"e","à":"a","â":"a","ä":"a",
                 "ô":"o","ö":"o","ù":"u","û":"u","ü":"u","î":"i","ï":"i",
                 "ç":"c","'":"_","'":"_","-":"_"}.items():
        s = s.replace(k, v)
    return re.sub(r"[\s_]+", "_", s).strip("_")

_NOTION_NORMALIZED = {_normalize(k): k for k in NOTION_SCHEMA}

def match_column(col: str):
    return _NOTION_NORMALIZED.get(_normalize(col))

def normalize_email(email: str) -> str:
    """Lowercase + strip an email for dedup comparison."""
    return str(email).lower().strip() if email else ""

# ── Notion HTTP helpers ───────────────────────────────────────────────────────

def notion_headers(token: str) -> dict:
    return {
        "Authorization": f"Bearer {token}",
        "Notion-Version": NOTION_API_VERSION,
        "Content-Type": "application/json",
    }

def query_database_all(token: str, database_id: str) -> list:
    """Fetch all pages from a Notion database (handles pagination)."""
    pages = []
    url = f"{NOTION_BASE_URL}/databases/{database_id}/query"
    payload = {"page_size": 100}
    while True:
        resp = requests.post(url, headers=notion_headers(token), json=payload, timeout=20)
        resp.raise_for_status()
        data = resp.json()
        pages.extend(data.get("results", []))
        if not data.get("has_more"):
            break
        payload["start_cursor"] = data["next_cursor"]
    return pages

def archive_page(token: str, page_id: str):
    """Archive (soft-delete) a Notion page."""
    url = f"{NOTION_BASE_URL}/pages/{page_id}"
    resp = requests.patch(url, headers=notion_headers(token),
                          json={"archived": True}, timeout=15)
    resp.raise_for_status()

def get_members(token: str) -> list:
    """Return list of {name, url} from the Membres database."""
    pages = query_database_all(token, MEMBRES_DB_ID)
    members = []
    for p in pages:
        props = p.get("properties", {})
        nom_prop = props.get("Nom", {})
        title_arr = nom_prop.get("title", [])
        name = "".join(t.get("plain_text", "") for t in title_arr).strip()
        if name:
            members.append({"name": name, "url": p["url"], "id": p["id"]})
    return members

def extract_emails_from_notion(token: str) -> set:
    """
    Fetch all existing Prospection entries and return a set of
    normalized emails found in ANY text/email field.
    """
    pages = query_database_all(token, DATABASE_ID)
    emails = set()
    for p in pages:
        props = p.get("properties", {})
        for prop_name, prop_val in props.items():
            ptype = prop_val.get("type")
            raw = ""
            if ptype == "email":
                raw = prop_val.get("email") or ""
            elif ptype == "rich_text":
                raw = "".join(t.get("plain_text","") for t in prop_val.get("rich_text",[]))
            elif ptype == "title":
                raw = "".join(t.get("plain_text","") for t in prop_val.get("title",[]))
            # Simple email pattern check
            for token_part in re.findall(r"[\w.\-+]+@[\w.\-]+\.\w+", raw.lower()):
                emails.add(token_part.strip())
    return emails

# ── Notion property builders ──────────────────────────────────────────────────

def build_properties(notion_row: dict, member_url: str | None = None) -> dict:
    props = {}
    for col, value in notion_row.items():
        is_empty = (pd.isna(value) if not isinstance(value, str) else not str(value).strip())
        if is_empty:
            continue
        value_str = str(value).strip()
        col_type  = NOTION_SCHEMA[col]
        if col_type == "title":
            props[col] = {"title": [{"type": "text", "text": {"content": value_str}}]}
        elif col_type == "rich_text":
            props[col] = {"rich_text": [{"type": "text", "text": {"content": value_str}}]}
        elif col_type == "email":
            props[col] = {"email": value_str}
        elif col_type == "phone_number":
            props[col] = {"phone_number": value_str}
        elif col_type == "select":
            if col == "Statut du Partenariat" and value_str not in VALID_STATUT:
                continue
            if col == "Type d'entreprise" and value_str not in VALID_TYPE_ENT:
                continue
            props[col] = {"select": {"name": value_str}}
        elif col_type == "date":
            if hasattr(value, "strftime"):
                date_str = value.strftime("%Y-%m-%d")
            else:
                date_str = str(value)[:10]
            props[col] = {"date": {"start": date_str}}

    # Add "Qui a démarché" relation if a member URL was resolved
    if member_url:
        # Extract page ID from URL
        page_id = member_url.rstrip("/").split("/")[-1]
        # Remove any title slug (last 32 hex chars)
        page_id = re.sub(r"[^a-f0-9]", "", page_id)
        if len(page_id) == 32:
            formatted = f"{page_id[:8]}-{page_id[8:12]}-{page_id[12:16]}-{page_id[16:20]}-{page_id[20:]}"
            props["Qui a démarché"] = {"relation": [{"id": formatted}]}

    return props

def create_notion_page(token: str, properties: dict) -> dict:
    url = f"{NOTION_BASE_URL}/pages"
    payload = {"parent": {"database_id": DATABASE_ID}, "properties": properties}
    for attempt in range(4):
        resp = requests.post(url, headers=notion_headers(token), json=payload, timeout=15)
        if resp.status_code == 429:
            retry_after = float(resp.headers.get("Retry-After", 1.0))
            time.sleep(retry_after)
            continue
        resp.raise_for_status()
        return resp.json()
    resp.raise_for_status()

# ── File parsing ──────────────────────────────────────────────────────────────

def read_file(file_bytes: bytes, filename: str) -> pd.DataFrame:
    ext = filename.rsplit(".", 1)[-1].lower()
    if ext == "csv":
        for enc in ("utf-8", "latin-1", "utf-8-sig"):
            try:
                return pd.read_csv(io.BytesIO(file_bytes), encoding=enc)
            except UnicodeDecodeError:
                continue
        raise ValueError("Encodage CSV non supporté.")
    elif ext in ("xlsx", "xls"):
        return pd.read_excel(io.BytesIO(file_bytes))
    else:
        raise ValueError(f"Format non supporté: .{ext}")

# ── Background workers ────────────────────────────────────────────────────────

def push_event(job_id: str, event_type: str, data: dict):
    with jobs_lock:
        if job_id in jobs:
            jobs[job_id]["events"].append({"type": event_type, "data": data})

def is_cancelled(job_id: str) -> bool:
    with jobs_lock:
        return jobs.get(job_id, {}).get("cancelled", False)

def run_clear(job_id: str, token: str):
    """Archive all pages in the Prospection database."""
    try:
        push_event(job_id, "clear_start", {"message": "Récupération des entrées…"})
        pages = query_database_all(token, DATABASE_ID)
        total = len(pages)
        push_event(job_id, "clear_total", {"total": total})

        deleted = 0
        for p in pages:
            if is_cancelled(job_id):
                push_event(job_id, "cancelled", {"deleted": deleted})
                with jobs_lock: jobs[job_id]["done"] = True
                return
            try:
                archive_page(token, p["id"])
                deleted += 1
                push_event(job_id, "clear_row", {"deleted": deleted, "total": total})
                time.sleep(0.05)
            except Exception as e:
                push_event(job_id, "clear_row_err", {"id": p["id"], "error": str(e)})

        push_event(job_id, "clear_done", {"deleted": deleted, "total": total})
    except Exception as e:
        push_event(job_id, "error", {"message": str(e)})
    finally:
        with jobs_lock:
            jobs[job_id]["done"] = True

def run_import(job_id: str, file_bytes: bytes, filename: str, token: str,
               dry_run: bool, prenom: str):
    try:
        df = read_file(file_bytes, filename)
    except Exception as e:
        push_event(job_id, "error", {"message": str(e)})
        with jobs_lock: jobs[job_id]["done"] = True
        return

    # Map columns
    col_mapping, unmatched = {}, []
    for col in df.columns:
        notion_name = match_column(col)
        if notion_name: col_mapping[col] = notion_name
        else: unmatched.append(col)

    if "Nom de l'entreprise" not in col_mapping.values():
        push_event(job_id, "error", {"message": "Colonne 'Nom de l'entreprise' introuvable."})
        with jobs_lock: jobs[job_id]["done"] = True
        return

    # Resolve "Qui a démarché" member URL from prénom
    member_url = None
    if prenom and not dry_run:
        try:
            members = get_members(token)
            prenom_norm = _normalize(prenom)
            for m in members:
                if prenom_norm in _normalize(m["name"]):
                    member_url = m["url"]
                    break
            if member_url:
                push_event(job_id, "member_found", {"name": prenom, "url": member_url})
            else:
                push_event(job_id, "member_not_found", {"name": prenom})
        except Exception as e:
            push_event(job_id, "member_error", {"error": str(e)})

    # Fetch existing emails from Notion for dedup
    existing_emails = set()
    if not dry_run:
        try:
            push_event(job_id, "dedup_start", {"message": "Chargement des emails existants…"})
            existing_emails = extract_emails_from_notion(token)
            push_event(job_id, "dedup_ready", {"count": len(existing_emails)})
        except Exception as e:
            push_event(job_id, "dedup_error", {"error": str(e)})

    push_event(job_id, "columns", {
        "matched": list(col_mapping.values()),
        "unmatched": unmatched,
        "total_rows": len(df)
    })

    success, errors, skipped = 0, 0, 0
    for idx, raw_row in df.iterrows():
        # Skip entirely empty rows
        if raw_row.dropna().empty:
            continue
        # Skip row if the title field is empty
        title_csv_col = next((c for c, n in col_mapping.items() if n == "Nom de l'entreprise"), None)
        if title_csv_col and (pd.isna(raw_row[title_csv_col]) or not str(raw_row[title_csv_col]).strip()):
            continue

        if is_cancelled(job_id):
            push_event(job_id, "cancelled", {"success": success, "errors": errors, "skipped": skipped})
            with jobs_lock: jobs[job_id]["done"] = True
            return

        notion_row = {col_mapping[c]: raw_row[c] for c in col_mapping}
        entreprise = str(notion_row.get("Nom de l'entreprise", f"Ligne {idx+2}")).strip()

        # Dedup: extract email from this row (any column containing @)
        row_emails = set()
        for v in raw_row.values:
            s = str(v).lower().strip()
            for found in re.findall(r"[\w.\-+]+@[\w.\-]+\.\w+", s):
                row_emails.add(found)

        if not dry_run and row_emails and row_emails & existing_emails:
            skipped += 1
            push_event(job_id, "row_skip", {
                "index": idx + 2,
                "name": entreprise,
                "reason": f"Email déjà présent ({', '.join(row_emails & existing_emails)})"
            })
            continue

        try:
            props = build_properties(notion_row, member_url)
            if not dry_run:
                create_notion_page(token, props)
                # Add new emails to in-memory set to avoid dupes within same file
                existing_emails |= row_emails
                time.sleep(0.05)
            success += 1
            push_event(job_id, "row_ok", {"index": idx + 2, "name": entreprise})
        except requests.HTTPError as e:
            errors += 1
            try:    msg = e.response.json().get("message", e.response.text[:120])
            except: msg = e.response.text[:120]
            push_event(job_id, "row_err", {"index": idx + 2, "name": entreprise, "error": msg})
        except Exception as e:
            errors += 1
            push_event(job_id, "row_err", {"index": idx + 2, "name": entreprise, "error": str(e)})

    push_event(job_id, "done", {
        "success": success, "errors": errors, "skipped": skipped,
        "dry_run": dry_run,
        "notion_url": f"https://www.notion.so/{DATABASE_ID.replace('-', '')}"
    })
    with jobs_lock:
        jobs[job_id]["done"] = True

# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/members")
def members():
    token = request.args.get("token", "").strip()
    if not token:
        return jsonify({"error": "Token requis"}), 400
    try:
        result = get_members(token)
        return jsonify({"members": result})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/upload", methods=["POST"])
def upload():
    token   = request.form.get("token", "").strip()
    dry_run = request.form.get("dry_run", "false").lower() == "true"
    prenom  = request.form.get("prenom", "").strip()
    file    = request.files.get("file")

    if not file or not file.filename:
        return jsonify({"error": "Aucun fichier fourni."}), 400
    if not token and not dry_run:
        return jsonify({"error": "Token Notion requis."}), 400

    filename   = secure_filename(file.filename)
    file_bytes = file.read()
    job_id     = str(uuid.uuid4())

    with jobs_lock:
        jobs[job_id] = {"events": [], "done": False, "cancelled": False}

    thread = threading.Thread(
        target=run_import,
        args=(job_id, file_bytes, filename, token, dry_run, prenom),
        daemon=True
    )
    thread.start()
    return jsonify({"job_id": job_id})

@app.route("/clear", methods=["POST"])
def clear():
    data  = request.get_json(silent=True) or {}
    token = data.get("token", "").strip()
    if not token:
        return jsonify({"error": "Token requis."}), 400

    job_id = str(uuid.uuid4())
    with jobs_lock:
        jobs[job_id] = {"events": [], "done": False, "cancelled": False}

    thread = threading.Thread(target=run_clear, args=(job_id, token), daemon=True)
    thread.start()
    return jsonify({"job_id": job_id})

@app.route("/cancel/<job_id>", methods=["POST"])
def cancel(job_id: str):
    with jobs_lock:
        if job_id in jobs and not jobs[job_id]["done"]:
            jobs[job_id]["cancelled"] = True
            return jsonify({"status": "cancelling"})
    return jsonify({"status": "not_found"}), 404

@app.route("/stream/<job_id>")
def stream(job_id: str):
    def generate():
        pointer = 0
        timeout = 600
        start   = time.time()
        while time.time() - start < timeout:
            with jobs_lock:
                job = jobs.get(job_id)
                if job is None:
                    yield f"data: {json.dumps({'type': 'error', 'data': {'message': 'Job not found'}})}\n\n"
                    return
                events  = job["events"][pointer:]
                is_done = job["done"]

            for ev in events:
                yield f"data: {json.dumps(ev)}\n\n"
            pointer += len(events)

            if is_done and len(events) == 0:
                break
            if is_done:
                time.sleep(0.1)
                with jobs_lock:
                    remaining = jobs[job_id]["events"][pointer:]
                for ev in remaining:
                    yield f"data: {json.dumps(ev)}\n\n"
                break

            time.sleep(0.3)

        def cleanup():
            time.sleep(60)
            with jobs_lock: jobs.pop(job_id, None)
        threading.Thread(target=cleanup, daemon=True).start()

    return Response(
        stream_with_context(generate()),
        content_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}
    )

@app.route("/health")
def health():
    return jsonify({"status": "ok"})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
