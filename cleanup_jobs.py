# this code is also in harbour_scraper where certificates are present
#!/usr/bin/env python3
import os
import json
from datetime import datetime, timedelta, date
from collections import defaultdict
import traceback

import firebase_admin
from firebase_admin import credentials, firestore

# ===================== FIREBASE SETUP =====================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

FIREBASE_KEY_PATH = os.path.join(BASE_DIR, "harbour-final-firebase-private-key.json")
FIREBASE_KEY_JSON = os.environ.get("FIREBASE_KEY_JSON")

def log(msg: str):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}")

try:
    if FIREBASE_KEY_JSON:
        log("[FIREBASE] Using FIREBASE_KEY_JSON from environment.")
        service_account_info = json.loads(FIREBASE_KEY_JSON)
        cred = credentials.Certificate(service_account_info)
    else:
        log(f"[FIREBASE] Using key file at {FIREBASE_KEY_PATH}")
        cred = credentials.Certificate(FIREBASE_KEY_PATH)
except Exception as e:
    log(f"[FIREBASE] Failed to load credentials: {e}")
    raise

firebase_admin.initialize_app(cred)
db = firestore.client()

# ===================== CLEANUP LOGIC =====================

def parse_date_safe(s: str):
    s = (s or "").strip()
    if not s:
        return None
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except Exception:
        return None

def main():
    log("=== Cleanup script started ===")

    now = datetime.now().date()
    cutoff_recent = now - timedelta(days=2)  # last 2 days inclusive

    log(f"Cutoff for 'last two days' = {cutoff_recent.isoformat()}")

    docs = list(db.collection("Jobs").stream())
    log(f"Total Jobs docs fetched: {len(docs)}")

    # Collect info about docs
    info_list = []
    id_to_ref = {}

    for doc in docs:
        data = doc.to_dict() or {}
        dp_str = data.get("date-posted", "")
        more_info = (data.get("moreInfoLink") or "").strip()

        dp = parse_date_safe(dp_str)
        is_recent = dp is not None and dp >= cutoff_recent

        info = {
            "id": doc.id,
            "ref": doc.reference,
            "date": dp,
            "date_str": dp_str,
            "moreInfoLink": more_info,
            "is_recent": is_recent,
        }
        info_list.append(info)
        id_to_ref[doc.id] = doc.reference

    # 1) Delete jobs from last 2 days
    recent_delete_ids = {info["id"] for info in info_list if info["is_recent"]}
    log(f"Jobs marked for deletion (last 2 days): {len(recent_delete_ids)}")

    # 2) Delete duplicates by moreInfoLink (all time), keeping one
    link_to_infos = defaultdict(list)
    for info in info_list:
        # If already in "recent delete", we'll anyway delete it - no need to use it as "keeper"
        if info["id"] in recent_delete_ids:
            continue
        link = info["moreInfoLink"]
        if not link:
            continue
        link_to_infos[link].append(info)

    duplicate_delete_ids = set()

    for link, infos in link_to_infos.items():
        if len(infos) <= 1:
            continue  # no duplicates

        # Sort by date (oldest first); docs with no date go to the end
        def sort_key(i):
            if i["date"] is None:
                # Put unknown dates at the end
                return (date.max, i["id"])
            return (i["date"], i["id"])

        infos_sorted = sorted(infos, key=sort_key)

        keep = infos_sorted[0]
        to_delete = infos_sorted[1:]

        log(f"[DUP] moreInfoLink={link!r} has {len(infos)} docs -> keeping {keep['id']}, deleting {len(to_delete)} others")

        for i in to_delete:
            duplicate_delete_ids.add(i["id"])

    log(f"Jobs marked for deletion as duplicates (excluding recent): {len(duplicate_delete_ids)}")

    # Union of all deletions
    all_delete_ids = recent_delete_ids.union(duplicate_delete_ids)
    log(f"Total unique Jobs docs to delete: {len(all_delete_ids)}")

    # Actually delete
    failures = 0
    for doc_id in all_delete_ids:
        try:
            ref = id_to_ref.get(doc_id)
            if ref:
                ref.delete()
                log(f"Deleted doc {doc_id}")
            else:
                log(f"[WARN] No reference found for doc {doc_id}, skipping.")
        except Exception as e:
            failures += 1
            log(f"[ERROR] Failed to delete doc {doc_id}: {e}")
            log(traceback.format_exc())

    log(f"Cleanup completed. Deleted {len(all_delete_ids) - failures} docs, {failures} failures.")
    log("=== Cleanup script finished ===")

if __name__ == "__main__":
    main()
