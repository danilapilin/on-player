"""
Generate per-date JSON files for the online player.
Reads last N workdays from Google Sheets for all groups,
fetches API recordings, outputs data/group{N}/*.json.

Optimized: caches at (phone, api_date) level for cross-date/group reuse,
uses concurrent API requests.
"""

import asyncio
import json
import os
import re
import sys
import time
from collections import OrderedDict
from datetime import datetime, timedelta

import httpx
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

from api_client import get_recordings, get_recording_url
from config import API_BASE, API_KEY

SA_FILE = "google_sa.json"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
MAX_RECS = 10
LOOKBACK_DAYS = 4
NUM_DATES = 10
OUTPUT_DIR = "data"
CACHE_FILE = "recordings_cache.json"
CONCURRENCY = 3  # parallel API requests
SAVE_EVERY = 500  # save cache every N fetches

GROUPS = [
    {
        "num": 1,
        "sheet_id": "1KX4XQXzIj9mPU7mvST0Zy8Y0Aa9ZYLN_g-cmZwohdc8",
        "tab": "Группа №1",
        "operators": [
            {"name": "Светлана", "phone": "D", "status": "E", "company": "L"},
            {"name": "Елена",    "phone": "F", "status": "G", "company": "M"},
            {"name": "Диана",    "phone": "H", "status": "I", "company": "N"},
            {"name": "Юлия",     "phone": "J", "status": "K", "company": "O"},
        ],
    },
    {
        "num": 2,
        "sheet_id": "1zl09C82C73VhuUFRx_x9q6TQ9F9SUD6Qd7-0aKN7E04",
        "tab": "Группа №2",
        "operators": [
            {"name": "Милана",     "phone": "D", "status": "E", "company": "L"},
            {"name": "Кристина",   "phone": "F", "status": "G", "company": "M"},
            {"name": "Наталья",    "phone": "H", "status": "I", "company": "N"},
            {"name": "Анастасия",  "phone": "J", "status": "K", "company": "O"},
        ],
    },
    {
        "num": 3,
        "sheet_id": "1vs6s0124Tgn3ho6IsiFkzywA8G6SEEm3TZOqX0oGdXE",
        "tab": "Группа №3",
        "operators": [
            {"name": "Милана", "phone": "D", "status": "E", "company": "L"},
            {"name": "Елена",  "phone": "F", "status": "G", "company": "M"},
            {"name": "Лиза",   "phone": "H", "status": "I", "company": "N"},
            {"name": "Юлия",   "phone": "J", "status": "K", "company": "O"},
        ],
    },
    {
        "num": 4,
        "sheet_id": "1ACnZuBlfaR3fCWjNWW0Hbf253Wpa94-D8QxBS3HYBdM",
        "tab": "Группа №4",
        "operators": [
            {"name": "Мария",    "phone": "D", "status": "E", "company": "L"},
            {"name": "Марина",   "phone": "F", "status": "G", "company": "M"},
            {"name": "Алина",    "phone": "H", "status": "I", "company": "N"},
            {"name": "Светлана", "phone": "J", "status": "K", "company": "O"},
        ],
    },
]


def log(msg):
    print(msg, flush=True)


def get_sheet_service():
    creds = Credentials.from_service_account_file(SA_FILE, scopes=SCOPES)
    return build("sheets", "v4", credentials=creds)


def find_date_ranges(service, sheet_id, tab_name):
    """Find row ranges for each date in the sheet."""
    result = service.spreadsheets().values().get(
        spreadsheetId=sheet_id,
        range=f"'{tab_name}'!A1:A18000",
    ).execute()
    values = result.get("values", [])

    dates = OrderedDict()
    for i, row in enumerate(values):
        val = row[0].strip() if row and row[0] else ""
        if "." in val and len(val) <= 5:
            if val not in dates:
                dates[val] = {"start": i + 1, "end": i + 1}
            dates[val]["end"] = i + 1

    today = datetime.now()
    cutoff = today - timedelta(days=25)

    available = []
    for d_str, rows in dates.items():
        parts = d_str.split(".")
        if len(parts) != 2:
            continue
        day, month = parts
        try:
            dt = datetime(2026, int(month), int(day))
        except ValueError:
            continue
        if cutoff <= dt <= today:
            available.append({
                "date_str": d_str,
                "iso": dt.strftime("%Y-%m-%d"),
                "start": rows["start"],
                "end": rows["end"],
            })

    return available[-NUM_DATES:]


def read_date_data(service, date_info, sheet_id, tab_name, operators):
    """Read all operator data for a specific date."""
    start, end = date_info["start"], date_info["end"]

    ranges = [
        f"'{tab_name}'!B{start}:B{end}",
        f"'{tab_name}'!R{start}:R{end}",
        f"'{tab_name}'!S{start}:S{end}",
    ]
    for op in operators:
        ranges.append(f"'{tab_name}'!{op['phone']}{start}:{op['phone']}{end}")
        ranges.append(f"'{tab_name}'!{op['status']}{start}:{op['status']}{end}")
        ranges.append(f"'{tab_name}'!{op['company']}{start}:{op['company']}{end}")

    result = service.spreadsheets().values().batchGet(
        spreadsheetId=sheet_id, ranges=ranges,
    ).execute()

    def col(idx):
        vals = result["valueRanges"][idx].get("values", [])
        return [r[0].strip() if r and r[0] else "" for r in vals]

    sites = col(0)
    nishas = col(1)
    clients = col(2)

    all_data = {}
    for i, op in enumerate(operators):
        base = 3 + i * 3
        phones = col(base)
        statuses = col(base + 1)
        company_phones = col(base + 2)

        data = []
        for j in range(len(sites)):
            site = sites[j] if j < len(sites) else ""
            phone = phones[j] if j < len(phones) else ""
            if not site:
                continue
            data.append({
                "site": site,
                "phone": re.sub(r"[^\d]", "", phone) if phone else "",
                "status": statuses[j] if j < len(statuses) else "",
                "operator_phone": re.sub(r"[^\d]", "", company_phones[j]) if j < len(company_phones) and company_phones[j] else "",
                "nisha": nishas[j] if j < len(nishas) else "",
                "client": clients[j] if j < len(clients) else "",
            })
        all_data[op["name"]] = data

    return all_data


def parse_transcription(text):
    if not text:
        return 0.0, ""
    lines = text.strip().split("\n")
    try:
        conf = float(lines[0])
        txt = "\n".join(lines[1:]).strip()
    except (ValueError, IndexError):
        return 0.0, text.strip()
    return conf, txt


def is_silence(text):
    return text.strip().lower().startswith("silence")


def process_recs(recs):
    processed, silence = [], 0
    for rec in recs:
        text = rec.get("text", "")
        if is_silence(text):
            silence += 1
            continue
        conf, transcript = parse_transcription(text)
        if not transcript and conf == 0:
            silence += 1
            continue
        processed.append({
            "callerid": rec.get("callerid", ""),
            "conf": conf,
            "transcript": transcript[:500],
            "audio": rec.get("audio_url", ""),
        })
    processed.sort(key=lambda x: (-x["conf"], -len(x["transcript"])))
    return processed, silence


def calc_status(op_phone, processed):
    if not processed:
        return "Нет записей" if not op_phone else "Оператор нашёл, записей нет"
    callerids = [r["callerid"] for r in processed if r["callerid"]]
    if not op_phone:
        return "Оператор пусто, есть звонки" if callerids else "Нет данных"
    for cid in callerids:
        if op_phone in cid or cid in op_phone:
            return "Совпал"
    return "Не совпал"


async def fetch_all_needed(needed_pairs, cache):
    """Fetch all (phone, api_date) pairs with concurrency and periodic saves."""
    sem = asyncio.Semaphore(CONCURRENCY)
    done = 0
    total = len(needed_pairs)
    t0 = time.time()

    async def fetch_one(phone, api_date, client):
        nonlocal done
        key = f"{phone}:{api_date}"
        async with sem:
            try:
                r = await get_recordings(phone, api_date, client)
                for item in r:
                    if item.get("rec"):
                        item["audio_url"] = get_recording_url(item["rec"])
                cache[key] = r
            except Exception as e:
                cache[key] = []
            done += 1
            if done % 200 == 0 or done == total:
                elapsed = time.time() - t0
                rate = done / elapsed if elapsed > 0 else 0
                eta = (total - done) / rate if rate > 0 else 0
                log(f"  API: {done}/{total} ({rate:.1f}/s, ETA {eta:.0f}s)")

    async with httpx.AsyncClient() as client:
        for i in range(0, total, SAVE_EVERY):
            chunk = needed_pairs[i:i + SAVE_EVERY]
            tasks = [fetch_one(phone, api_date, client) for phone, api_date in chunk]
            await asyncio.gather(*tasks)
            with open(CACHE_FILE, "w") as f:
                json.dump(cache, f, ensure_ascii=False)
            log(f"  Cache checkpoint: {len(cache)} entries saved")

    return cache


def collect_needed_pairs(all_dates_data, cache):
    """Collect all unique (phone, api_date) pairs not in cache."""
    needed = set()
    for sheet_iso, all_data in all_dates_data.items():
        base = datetime.strptime(sheet_iso, "%Y-%m-%d")
        for items in all_data.values():
            for item in items:
                phone = item["phone"]
                if not phone:
                    continue
                for d in range(LOOKBACK_DAYS + 1):
                    api_date = (base + timedelta(days=d)).strftime("%Y-%m-%d")
                    key = f"{phone}:{api_date}"
                    if key not in cache:
                        needed.add((phone, api_date))
    return needed


def get_recs_for_phone(phone, sheet_iso, cache):
    """Get all recordings for a phone across the lookback window."""
    base = datetime.strptime(sheet_iso, "%Y-%m-%d")
    recs = []
    for d in range(LOOKBACK_DAYS + 1):
        api_date = (base + timedelta(days=d)).strftime("%Y-%m-%d")
        key = f"{phone}:{api_date}"
        recs.extend(cache.get(key, []))
    return recs


def generate_date_json(date_info, all_data, cache):
    """Generate player JSON for one date. Returns (data, stats, op_stats)."""
    sheet_iso = date_info["iso"]
    data = []
    stats = {"total": 0, "matched": 0, "mismatched": 0, "op_empty": 0, "no_recs": 0, "op_found": 0}
    op_stats = {}  # per-operator breakdown

    for op_name, items in all_data.items():
        ops = {"matched": 0, "mismatched": 0, "op_found": 0, "total": 0}
        for idx, item in enumerate(items):
            recs = get_recs_for_phone(item["phone"], sheet_iso, cache) if item["phone"] else []
            proc, sil = process_recs(recs)
            st = calc_status(item["operator_phone"], proc)

            stats["total"] += 1
            ops["total"] += 1
            if item["operator_phone"]:
                stats["op_found"] += 1
                ops["op_found"] += 1
            if st == "Совпал":
                stats["matched"] += 1
                ops["matched"] += 1
            elif st == "Не совпал":
                stats["mismatched"] += 1
                ops["mismatched"] += 1
            elif st == "Оператор пусто, есть звонки":
                stats["op_empty"] += 1
            elif "Нет записей" in st or st == "Оператор нашёл, записей нет":
                stats["no_recs"] += 1

            data.append({
                "idx": idx + 1,
                "operator": op_name,
                "site": item["site"],
                "phone": item["phone"],
                "operator_phone": item["operator_phone"],
                "nisha": item["nisha"],
                "client": item["client"],
                "status": st,
                "total_recs": len(recs),
                "silence": sil,
                "recs": [{"callerid": r["callerid"], "conf": f'{r["conf"]:.2f}',
                          "transcript": r["transcript"], "audio": r["audio"]}
                         for r in proc[:MAX_RECS]],
            })

        op_stats[op_name] = ops

    return data, stats, op_stats


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Load cache (phone:api_date level, shared across groups)
    try:
        with open(CACHE_FILE, "r") as f:
            cache = json.load(f)
        log(f"Cache loaded: {len(cache)} entries")
    except (FileNotFoundError, json.JSONDecodeError):
        cache = {}

    service = get_sheet_service()

    # Phase 1: Read sheet data for ALL groups
    log("\n=== Phase 1: Reading Google Sheets ===")
    groups_dates = {}   # {group_num: [date_ranges]}
    groups_data = {}    # {group_num: {iso: all_data}}

    for group in GROUPS:
        gn = group["num"]
        log(f"\n--- Группа {gn} ({group['tab']}) ---")

        date_ranges = find_date_ranges(service, group["sheet_id"], group["tab"])
        groups_dates[gn] = date_ranges
        log(f"Found {len(date_ranges)} dates")

        gdata = {}
        for di, date_info in enumerate(date_ranges):
            log(f"  [{di+1}/{len(date_ranges)}] {date_info['date_str']}...")
            all_data = read_date_data(
                service, date_info,
                group["sheet_id"], group["tab"], group["operators"],
            )
            total_rows = sum(len(v) for v in all_data.values())
            log(f"    {total_rows} rows")
            gdata[date_info["iso"]] = all_data
        groups_data[gn] = gdata

    # Phase 2: Collect ALL needed pairs across groups, fetch from API
    log("\n=== Phase 2: Fetching recordings from API ===")
    all_needed = set()
    for gn, gdata in groups_data.items():
        pairs = collect_needed_pairs(gdata, cache)
        all_needed.update(pairs)
    needed = list(all_needed)

    phones_set = set(p for p, d in needed)
    dates_set = set(d for p, d in needed)
    log(f"Need to fetch: {len(needed)} pairs ({len(phones_set)} phones x {len(dates_set)} dates)")

    if needed:
        asyncio.run(fetch_all_needed(needed, cache))

        with open(CACHE_FILE, "w") as f:
            json.dump(cache, f, ensure_ascii=False)
        log(f"Cache saved: {len(cache)} entries")
    else:
        log("All data in cache!")

    # Phase 3: Generate per-group JSONs
    log("\n=== Phase 3: Generating JSONs ===")
    for group in GROUPS:
        gn = group["num"]
        group_dir = os.path.join(OUTPUT_DIR, f"group{gn}")
        os.makedirs(group_dir, exist_ok=True)

        log(f"\n--- Группа {gn} ---")
        dates_index = []
        for date_info in groups_dates[gn]:
            iso = date_info["iso"]
            all_data = groups_data[gn][iso]

            data, stats, op_stats = generate_date_json(date_info, all_data, cache)
            out_file = os.path.join(group_dir, f"{iso}.json")
            with open(out_file, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False)

            recall = round(stats["matched"] / stats["op_found"] * 100) if stats["op_found"] else 0
            log(f"  {date_info['date_str']}: {len(data)} rows, matched={stats['matched']}, recall={recall}%")

            # per-operator recall for analytics
            operators = {}
            for name, s in op_stats.items():
                op_recall = round(s["matched"] / s["op_found"] * 100) if s["op_found"] else 0
                operators[name] = {
                    "matched": s["matched"],
                    "mismatched": s["mismatched"],
                    "total": s["total"],
                    "recall": op_recall,
                }

            dates_index.append({
                "date": iso,
                "label": date_info["date_str"],
                "sites": stats["total"],
                "matched": stats["matched"],
                "mismatched": stats["mismatched"],
                "op_empty": stats["op_empty"],
                "no_recs": stats["no_recs"],
                "recall": recall,
                "operators": operators,
            })

        with open(os.path.join(group_dir, "dates.json"), "w", encoding="utf-8") as f:
            json.dump(dates_index, f, ensure_ascii=False, indent=2)

        log(f"Группа {gn}: {len(dates_index)} dates")
        for d in dates_index:
            log(f"  {d['label']}: {d['sites']} sites, recall={d['recall']}%")

    log(f"\nDone! Generated data for {len(GROUPS)} groups")


if __name__ == "__main__":
    main()
