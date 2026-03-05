"""
Generate analytics screenshot from the latest dates.json files.
Builds self-contained HTML (svodka + bars + table), renders with Playwright.
Output: data/analytics.png
"""

import json
import os
import sys

OUTPUT_DIR = "data"
SCREENSHOT_PATH = os.path.join(OUTPUT_DIR, "analytics.png")
NUM_GROUPS = 4


def load_groups_data():
    groups = {}
    for g in range(1, NUM_GROUPS + 1):
        path = os.path.join(OUTPUT_DIR, f"group{g}", "dates.json")
        if os.path.exists(path):
            with open(path) as f:
                groups[g] = json.load(f)
    return groups


def rc_color(v):
    return "#00b894" if v >= 90 else "#e17055" if v >= 70 else "#d63031"


def build_html(groups_data):
    all_dates = sorted({d["date"] for dates in groups_data.values() for d in dates})
    dl = {}
    for dates in groups_data.values():
        for d in dates:
            dl[d["date"]] = d["label"]

    # aggregate by date across all groups
    by_date = {}
    for date in all_dates:
        a = {"sites": 0, "matched": 0, "mismatched": 0, "op_empty": 0, "no_recs": 0, "uph": 0}
        for g in groups_data:
            d = next((x for x in groups_data[g] if x["date"] == date), None)
            if d:
                a["sites"] += d["sites"]
                a["matched"] += d["matched"]
                a["mismatched"] += d["mismatched"]
                a["op_empty"] += d["op_empty"]
                a["no_recs"] += d["no_recs"]
                a["uph"] += d.get("unique_phones", 0)
        of = a["matched"] + a["mismatched"]
        a["recall"] = round(a["matched"] / of * 100) if of else 0
        by_date[date] = a

    # report = previous day (not today's partial data)
    ri = len(all_dates) - 2 if len(all_dates) >= 2 else len(all_dates) - 1
    last = by_date[all_dates[ri]]
    last_lbl = dl[all_dates[ri]]
    prev = by_date[all_dates[ri - 1]] if ri > 0 else None

    def dlt(cur, prv, up=True):
        if prv is None:
            return ""
        d = cur - prv
        if d == 0:
            return ""
        ok = (d > 0) if up else (d < 0)
        color = "#00b894" if ok else "#d63031"
        arrow = "↑" if d > 0 else "↓"
        return f'<div style="font-size:12px;margin-top:2px;color:{color}">{arrow} {abs(d)}</div>'

    SC = {"m": "#00b894", "mis": "#d63031", "oe": "#fdcb6e", "nr": "#dfe6e9"}

    # stat boxes
    boxes = [
        ("", last["sites"], "Проверок", dlt(last["sites"], prev["sites"] if prev else None, True)),
        ("#00b894", last["matched"], "Совпал", dlt(last["matched"], prev["matched"] if prev else None, True)),
        ("#d63031", last["mismatched"], "Не совпал", dlt(last["mismatched"], prev["mismatched"] if prev else None, False)),
        ("#e17055", last["op_empty"], "Оп. пусто", dlt(last["op_empty"], prev["op_empty"] if prev else None, False)),
        ("", last["no_recs"], "Нет записей", ""),
        ("#00b894", f'{last["recall"]}%', "Recall", dlt(last["recall"], prev["recall"] if prev else None, True)),
        ("", last["uph"], "Уник. номеров", dlt(last["uph"], prev["uph"] if prev else None, True)),
    ]

    stat_html = ""
    for color, val, lbl, delta in boxes:
        c = color or "#0984e3"
        stat_html += f'<div style="background:#f8f9fa;border-radius:10px;padding:12px 8px;text-align:center">'
        stat_html += f'<div style="font-size:24px;font-weight:700;color:{c}">{val}</div>'
        stat_html += f'<div style="font-size:12px;color:#636e72;margin-top:4px">{lbl}</div>{delta}</div>'

    # stacked bars
    bars_html = ""
    for date in all_dates:
        d = by_date[date]
        t = d["sites"] or 1
        bars_html += f'''<div style="display:flex;align-items:center;gap:8px;margin-bottom:8px">
  <span style="width:60px;font-size:12px;color:#636e72;text-align:right;flex-shrink:0">{dl[date].split(" ")[0] if " " in dl[date] else dl[date]}</span>
  <div style="flex:1;height:28px;background:#f0f0f0;border-radius:4px;overflow:hidden">
    <div style="height:100%;display:flex;width:100%">
      <div style="width:{d["matched"]/t*100:.1f}%;background:{SC["m"]};height:100%"></div>
      <div style="width:{d["mismatched"]/t*100:.1f}%;background:{SC["mis"]};height:100%"></div>
      <div style="width:{d["op_empty"]/t*100:.1f}%;background:{SC["oe"]};height:100%"></div>
      <div style="width:{d["no_recs"]/t*100:.1f}%;background:{SC["nr"]};height:100%"></div>
    </div>
  </div>
  <span style="width:50px;font-size:12px;font-weight:600;color:#2d3436;flex-shrink:0">{d["sites"]}</span>
</div>'''

    # detail table rows
    rows = ""
    tS = tM = tMis = tOE = tNR = tUph = 0
    td = 'style="padding:8px 12px;border-bottom:1px solid #f0f0f0"'
    tdr = f'{td[:-1]};text-align:right"'
    for date in all_dates:
        d = by_date[date]
        tS += d["sites"]; tM += d["matched"]; tMis += d["mismatched"]
        tOE += d["op_empty"]; tNR += d["no_recs"]; tUph += d["uph"]
        rows += f'''<tr>
  <td {td}>{dl[date]}</td><td {tdr}>{d["sites"]}</td>
  <td {tdr[:-1]};color:#00b894;font-weight:600">{d["matched"]}</td>
  <td {tdr[:-1]};color:#d63031;font-weight:600">{d["mismatched"]}</td>
  <td {tdr[:-1]};color:#e17055;font-weight:600">{d["op_empty"]}</td>
  <td {tdr}>{d["no_recs"]}</td>
  <td {tdr[:-1]};font-weight:700;color:{rc_color(d["recall"])}">{d["recall"]}%</td>
  <td {tdr}>{d["uph"]}</td></tr>'''

    tRc = round(tM / (tM + tMis) * 100) if (tM + tMis) else 0
    rows += f'''<tr style="font-weight:700;border-top:2px solid #dfe6e9">
  <td style="padding:8px 12px">Итого</td><td {tdr}>{tS}</td>
  <td {tdr[:-1]};color:#00b894">{tM}</td><td {tdr[:-1]};color:#d63031">{tMis}</td>
  <td {tdr[:-1]};color:#e17055">{tOE}</td><td {tdr}>{tNR}</td>
  <td {tdr}>{tRc}%</td><td {tdr}>{tUph}</td></tr>'''

    return f'''<!DOCTYPE html>
<html><head><meta charset="UTF-8">
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
body{{font-family:-apple-system,'Segoe UI',Helvetica,sans-serif;background:#f5f6fa;color:#2d3436;padding:16px;width:1200px}}
.card{{background:#fff;border-radius:12px;padding:20px;margin-bottom:16px;box-shadow:0 1px 4px rgba(0,0,0,0.06)}}
.card h2{{font-size:16px;font-weight:600;margin-bottom:16px;color:#2d3436}}
.card h3{{font-size:14px;font-weight:600;margin:20px 0 8px;color:#636e72}}
table{{width:100%;border-collapse:collapse;font-size:13px}}
th{{text-align:left;padding:8px 12px;background:#f8f9fa;font-weight:600;color:#636e72;border-bottom:2px solid #dfe6e9}}
th.r{{text-align:right}}
</style></head><body>
<div class="card">
  <h2>Сводка за {last_lbl}</h2>
  <div style="display:grid;grid-template-columns:repeat(7,1fr);gap:10px">{stat_html}</div>
</div>
<div class="card">
  <h2>Распределение статусов по дням</h2>
  <div style="display:flex;gap:16px;margin:12px 0">
    <div style="display:flex;align-items:center;gap:4px;font-size:12px;color:#636e72"><div style="width:12px;height:12px;border-radius:3px;background:{SC["m"]}"></div>Совпал</div>
    <div style="display:flex;align-items:center;gap:4px;font-size:12px;color:#636e72"><div style="width:12px;height:12px;border-radius:3px;background:{SC["mis"]}"></div>Не совпал</div>
    <div style="display:flex;align-items:center;gap:4px;font-size:12px;color:#636e72"><div style="width:12px;height:12px;border-radius:3px;background:{SC["oe"]}"></div>Оп. пусто</div>
    <div style="display:flex;align-items:center;gap:4px;font-size:12px;color:#636e72"><div style="width:12px;height:12px;border-radius:3px;background:{SC["nr"]}"></div>Нет записей</div>
  </div>
  {bars_html}
  <h3>Детализация по дням</h3>
  <table>
    <tr><th>Дата</th><th class="r">Сайтов</th><th class="r">Совпал</th><th class="r">Не совпал</th><th class="r">Оп. пусто</th><th class="r">Нет записей</th><th class="r">Recall</th><th class="r">Уник. номеров</th></tr>
    {rows}
  </table>
</div>
</body></html>'''


def screenshot(html_content):
    from playwright.sync_api import sync_playwright

    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page(viewport={"width": 1240, "height": 800})
        page.set_content(html_content, wait_until="networkidle")
        page.wait_for_timeout(300)
        png = page.screenshot(full_page=True)
        browser.close()
        return png


def main():
    groups_data = load_groups_data()
    if not groups_data:
        print("No data found")
        sys.exit(1)

    html = build_html(groups_data)
    png = screenshot(html)

    with open(SCREENSHOT_PATH, "wb") as f:
        f.write(png)
    print(f"Screenshot saved: {SCREENSHOT_PATH} ({len(png)} bytes)")


if __name__ == "__main__":
    main()
