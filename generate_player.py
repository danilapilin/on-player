"""
Generate per-date JSON files for the online player.
Reads last N workdays from Google Sheet, fetches API recordings, outputs data/*.json.

Optimized: caches at (phone, api_date) level for cross-date reuse,
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
SOURCE_SHEET = "1KX4XQXzIj9mPU7mvST0Zy8Y0Aa9ZYLN_g-cmZwohdc8"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
MAX_RECS = 10
LOOKBACK_DAYS = 4
NUM_DATES = 10
OUTPUT_DIR = "data"
CACHE_FILE = "recordings_cache.json"
CONCURRENCY = 3  # parallel API requests
SAVE_EVERY = 500  # save cache every N fetches

OPERATORS = [
    {"name": "Светлана", "phone": "D", "status": "E", "company": "L"},
    {"name": "Елена",    "phone": "F", "status": "G", "company": "M"},
    {"name": "Диана",    "phone": "H", "status": "I", "company": "N"},
    {"name": "Юлия",     "phone": "J", "status": "K", "company": "O"},
]


def log(msg):
    print(msg, flush=True)


def get_sheet_service():
    creds = Credentials.from_service_account_file(SA_FILE, scopes=SCOPES)
    return build("sheets", "v4", credentials=creds)


def find_date_ranges(service):
    """Find row ranges for each date in the sheet."""
    result = service.spreadsheets().values().get(
        spreadsheetId=SOURCE_SHEET,
        range="'Группа №1'!A1:A18000",
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


def read_date_data(service, date_info):
    """Read all operator data for a specific date."""
    start, end = date_info["start"], date_info["end"]

    ranges = [
        f"'Группа №1'!B{start}:B{end}",
        f"'Группа №1'!R{start}:R{end}",
        f"'Группа №1'!S{start}:S{end}",
    ]
    for op in OPERATORS:
        ranges.append(f"'Группа №1'!{op['phone']}{start}:{op['phone']}{end}")
        ranges.append(f"'Группа №1'!{op['status']}{start}:{op['status']}{end}")
        ranges.append(f"'Группа №1'!{op['company']}{start}:{op['company']}{end}")

    result = service.spreadsheets().values().batchGet(
        spreadsheetId=SOURCE_SHEET, ranges=ranges,
    ).execute()

    def col(idx):
        vals = result["valueRanges"][idx].get("values", [])
        return [r[0].strip() if r and r[0] else "" for r in vals]

    sites = col(0)
    nishas = col(1)
    clients = col(2)

    all_data = {}
    for i, op in enumerate(OPERATORS):
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
    last_save = 0

    async def fetch_one(phone, api_date, client):
        nonlocal done, last_save
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
        # Process in chunks to save cache periodically
        for i in range(0, total, SAVE_EVERY):
            chunk = needed_pairs[i:i + SAVE_EVERY]
            tasks = [fetch_one(phone, api_date, client) for phone, api_date in chunk]
            await asyncio.gather(*tasks)
            # Save cache after each chunk
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
    return list(needed)


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
    """Generate player JSON for one date."""
    sheet_iso = date_info["iso"]
    data = []
    stats = {"total": 0, "matched": 0, "mismatched": 0, "op_empty": 0, "no_recs": 0, "op_found": 0}

    for op_name, items in all_data.items():
        for idx, item in enumerate(items):
            recs = get_recs_for_phone(item["phone"], sheet_iso, cache) if item["phone"] else []
            proc, sil = process_recs(recs)
            st = calc_status(item["operator_phone"], proc)

            stats["total"] += 1
            if item["operator_phone"]:
                stats["op_found"] += 1
            if st == "Совпал":
                stats["matched"] += 1
            elif st == "Не совпал":
                stats["mismatched"] += 1
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

    return data, stats


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Load cache (phone:api_date level)
    try:
        with open(CACHE_FILE, "r") as f:
            cache = json.load(f)
        log(f"Cache loaded: {len(cache)} entries")
    except (FileNotFoundError, json.JSONDecodeError):
        cache = {}

    service = get_sheet_service()

    log("Finding available dates...")
    date_ranges = find_date_ranges(service)
    log(f"Found {len(date_ranges)} dates:")
    for d in date_ranges:
        log(f"  {d['date_str']} ({d['iso']})")

    # Phase 1: Read ALL sheet data
    log("\n=== Phase 1: Reading Google Sheet ===")
    all_dates_data = {}
    for di, date_info in enumerate(date_ranges):
        log(f"  [{di+1}/{len(date_ranges)}] {date_info['date_str']}...")
        all_data = read_date_data(service, date_info)
        total_rows = sum(len(v) for v in all_data.values())
        log(f"    {total_rows} rows ({', '.join(f'{k}: {len(v)}' for k, v in all_data.items())})")
        all_dates_data[date_info["iso"]] = all_data

    # Phase 2: Collect needed (phone, api_date) pairs and fetch from API
    log("\n=== Phase 2: Fetching recordings from API ===")
    needed = collect_needed_pairs(all_dates_data, cache)

    # Count unique phones and dates
    phones_set = set(p for p, d in needed)
    dates_set = set(d for p, d in needed)
    log(f"Need to fetch: {len(needed)} pairs ({len(phones_set)} phones × {len(dates_set)} dates)")

    if needed:
        asyncio.run(fetch_all_needed(needed, cache))

        # Save cache
        with open(CACHE_FILE, "w") as f:
            json.dump(cache, f, ensure_ascii=False)
        log(f"Cache saved: {len(cache)} entries")
    else:
        log("All data in cache!")

    # Phase 3: Generate per-date JSONs
    log("\n=== Phase 3: Generating JSONs ===")
    dates_index = []
    for di, date_info in enumerate(date_ranges):
        iso = date_info["iso"]
        all_data = all_dates_data[iso]

        data, stats = generate_date_json(date_info, all_data, cache)
        out_file = os.path.join(OUTPUT_DIR, f"{iso}.json")
        with open(out_file, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)

        recall = round(stats["matched"] / stats["op_found"] * 100) if stats["op_found"] else 0
        log(f"  {date_info['date_str']}: {len(data)} rows, matched={stats['matched']}, recall={recall}%")

        dates_index.append({
            "date": iso,
            "label": date_info["date_str"],
            "sites": stats["total"],
            "matched": stats["matched"],
            "mismatched": stats["mismatched"],
            "op_empty": stats["op_empty"],
            "no_recs": stats["no_recs"],
            "recall": recall,
        })

    # Write dates index
    with open(os.path.join(OUTPUT_DIR, "dates.json"), "w", encoding="utf-8") as f:
        json.dump(dates_index, f, ensure_ascii=False, indent=2)

    log(f"\nDone! {len(dates_index)} dates in {OUTPUT_DIR}/")
    for d in dates_index:
        log(f"  {d['label']}: {d['sites']} sites, recall={d['recall']}%")


if __name__ == "__main__":
    main()
