"""
AI Shadow Matching: анализ транскрипций звонков с помощью Claude.

Для каждой строки с записями AI определяет, какой callerid — обратный звонок
с сайта. Результат сравнивается с оператором (agree/disagree/ai_only/op_only).

Запуск: python ai_matching.py [--dry-run] [--date YYYY-MM-DD] [--reprocess]

Авторизация (в порядке приоритета):
  1. OAuth токен из Claude CLI keychain (подписка Max/Pro) — автоматически
  2. ANTHROPIC_API_KEY env var — для GitHub Actions
"""

import asyncio
import json
import logging
import os
import subprocess
import sys
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

OUTPUT_DIR = "data"
NUM_GROUPS = 4
BATCH_SIZE = 20
MAX_CONCURRENT = 10
MAX_RETRIES = 3

# модель для OAuth (через подписку) и для API key
MODEL_OAUTH = "claude-haiku-4-5-20251001"
MODEL_API = "claude-haiku-4-5-20251001"

# бета-флаги для OAuth авторизации через подписку (как в ims-aibot)
ANTHROPIC_BETA = ",".join([
    "claude-code-20250219",
    "oauth-2025-04-20",
    "fine-grained-tool-streaming-2025-05-14",
    "interleaved-thinking-2025-05-14",
])

SYSTEM_PROMPT = (
    "Ты анализируешь записи телефонных звонков. Клиент оставил номер в форме "
    "обратного звонка на сайте. Определи, какая из записей — обратный звонок "
    "с этого сайта.\n\n"
    "Признаки обратного звонка:\n"
    "- Упоминание компании, бренда или услуги, связанной с сайтом/нишей\n"
    "- Фразы: 'вы оставляли заявку', 'вы обращались', 'по вашей заявке с сайта'\n"
    "- Тематика разговора совпадает с нишей клиента\n\n"
    "Для каждого элемента верни JSON массив объектов:\n"
    '[{"id": <int>, "callerid": "<номер>" или "", "reasoning": "<1 предложение>"}]\n\n'
    "Если ни одна запись не подходит — верни callerid как пустую строку.\n"
    "Отвечай ТОЛЬКО JSON массивом, без markdown и пояснений."
)


def get_oauth_token():
    """Достаёт OAuth access token: из env (CI) или macOS keychain (локально).

    В CI приоритеты:
    1. Кэшированный refresh token из файла (ротированный при прошлом запуске)
    2. Refresh token из секрета (первоначальный)
    3. Готовый access token из env
    """

    # 1. CI: кэшированный refresh token из прошлого запуска (ротированный)
    cached_file = os.environ.get("CLAUDE_CACHED_REFRESH_TOKEN_FILE")
    if cached_file and Path(cached_file).exists():
        cached_rt = Path(cached_file).read_text().strip()
        if cached_rt:
            log.info("Using cached refresh token from previous run")
            result = _refresh_oauth_token(cached_rt)
            if result:
                return result
            log.warning("Cached refresh token failed, trying secret")

    # 2. CI: refresh token из секрета
    refresh_token = os.environ.get("CLAUDE_REFRESH_TOKEN")
    if refresh_token:
        result = _refresh_oauth_token(refresh_token)
        if result:
            return result

    # 3. CI: готовый access token в env
    access_token = os.environ.get("CLAUDE_ACCESS_TOKEN")
    if access_token:
        return access_token

    # 4. Локально: macOS keychain
    try:
        result = subprocess.run(
            ["security", "find-generic-password", "-s", "Claude Code-credentials", "-w"],
            capture_output=True, text=True, timeout=5,
        )
        if result.returncode != 0:
            return None
        data = json.loads(result.stdout.strip())
        return data.get("claudeAiOauth", {}).get("accessToken")
    except Exception:
        return None


OAUTH_CLIENT_ID = "9d1c250a-e61b-44d9-88ed-5944d1962f5e"


def _refresh_oauth_token(refresh_token):
    """Обменять refresh token на свежий access token через Anthropic OAuth.

    Returns (access_token, new_refresh_token) or (None, None).
    Refresh tokens are single-use — each call returns a rotated pair.
    """
    import httpx

    payload = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": OAUTH_CLIENT_ID,
    }

    # retry with both content types — some environments prefer form-urlencoded
    attempts = [
        ("json", {"json": payload}),
        ("form", {"data": payload, "headers": {"Content-Type": "application/x-www-form-urlencoded"}}),
    ]

    for label, kwargs in attempts:
        try:
            resp = httpx.post(
                "https://console.anthropic.com/v1/oauth/token",
                timeout=30,
                **kwargs,
            )
            log.info("OAuth refresh attempt (%s): HTTP %s", label, resp.status_code)
            if resp.status_code >= 400:
                log.warning("OAuth refresh body: %s", resp.text[:300])
                continue
            data = resp.json()
            access = data.get("access_token")
            new_refresh = data.get("refresh_token")
            if access:
                log.info("OAuth token refreshed successfully via %s", label)
                # save rotated refresh token for next run
                if new_refresh:
                    _save_new_refresh_token(new_refresh)
                return access
            log.error("No access_token in refresh response")
        except Exception as e:
            log.error("OAuth refresh failed (%s): %s", label, e)

    return None


def _save_new_refresh_token(new_token):
    """Сохранить новый refresh token в файл для последующего обновления секрета."""
    try:
        Path("new_refresh_token.txt").write_text(new_token)
        log.info("Saved rotated refresh token to new_refresh_token.txt")
    except Exception as e:
        log.error("Failed to save new refresh token: %s", e)


def make_client(oauth_token=None, api_key=None):
    """Создаёт Anthropic клиент — через OAuth (подписка) или API key."""
    import anthropic
    import httpx

    if oauth_token:
        return anthropic.AsyncAnthropic(
            api_key=None,
            auth_token=oauth_token,
            default_headers={
                "anthropic-beta": ANTHROPIC_BETA,
                "user-agent": "claude-cli/2.1.2 (external, cli)",
                "x-app": "cli",
            },
            timeout=httpx.Timeout(connect=5, read=120, write=30, pool=30),
        )

    return anthropic.AsyncAnthropic(api_key=api_key)


def load_dates_json(group_num):
    path = os.path.join(OUTPUT_DIR, f"group{group_num}", "dates.json")
    if not os.path.exists(path):
        return None
    with open(path) as f:
        return json.load(f)


def load_date_data(group_num, date_iso):
    path = os.path.join(OUTPUT_DIR, f"group{group_num}", f"{date_iso}.json")
    if not os.path.exists(path):
        return None
    with open(path) as f:
        return json.load(f)


def save_date_data(group_num, date_iso, data):
    path = os.path.join(OUTPUT_DIR, f"group{group_num}", f"{date_iso}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)


def save_dates_json(group_num, dates):
    path = os.path.join(OUTPUT_DIR, f"group{group_num}", "dates.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(dates, f, ensure_ascii=False, indent=2)


def get_target_date(dates):
    """Предпоследняя дата — вчерашние полные данные."""
    if len(dates) >= 2:
        return dates[-2]["date"]
    return dates[-1]["date"] if dates else None


def has_transcripts(row):
    """Есть ли у строки хотя бы одна запись с непустой транскрипцией."""
    if not row.get("recs"):
        return False
    return any(r.get("transcript", "").strip() for r in row["recs"])


def needs_processing(row, reprocess=False):
    """Строка подходит для AI: есть записи с транскрипциями и ещё не обработана."""
    if not has_transcripts(row):
        return False

    # reprocess — сбрасываем и перепрогоняем всё
    if reprocess:
        return True

    return row.get("ai_phone") is None


def build_batch_prompt(batch):
    """Формирует пользовательский промпт для батча строк."""
    items = []
    for i, row in enumerate(batch):
        recs = []
        for r in row["recs"]:
            if r.get("transcript", "").strip():
                recs.append({
                    "callerid": r["callerid"],
                    "transcript": r["transcript"][:300],
                })
        items.append({
            "id": i,
            "site": row["site"],
            "nisha": row.get("nisha", ""),
            "client": row.get("client", ""),
            "recs": recs,
        })
    return json.dumps(items, ensure_ascii=False)


def parse_ai_response(text, batch_size):
    """Парсит JSON ответ от Claude. Возвращает список {id, callerid, reasoning}."""
    text = text.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        text = "\n".join(lines[1:])
        if text.endswith("```"):
            text = text[:-3]
        text = text.strip()

    results = json.loads(text)

    if not isinstance(results, list):
        raise ValueError("Expected JSON array")

    parsed = []
    for item in results:
        idx = item.get("id")
        if idx is None or not (0 <= idx < batch_size):
            continue
        parsed.append({
            "id": idx,
            "callerid": str(item.get("callerid", "")).strip(),
            "reasoning": str(item.get("reasoning", "")).strip()[:200],
        })
    return parsed


def compute_ai_vs_op(row):
    """Сравнивает AI результат с оператором."""
    ai_phone = row.get("ai_phone", "")
    op_phone = row.get("operator_phone", "")

    if not ai_phone and not op_phone:
        return "both_empty"
    if not ai_phone and op_phone:
        return "op_only"
    if ai_phone and not op_phone:
        return "ai_only"

    # оба есть — сравниваем (частичное совпадение)
    if op_phone in ai_phone or ai_phone in op_phone:
        return "agree"
    return "disagree"


class FatalAPIError(Exception):
    """Неустранимая ошибка API — нет смысла ретраить."""
    pass


FATAL_MESSAGES = [
    "credit balance is too low",
    "billing",
    "account has been disabled",
]


def _is_fatal(error):
    """Проверяет, является ли ошибка фатальной (ретраи бессмысленны)."""
    msg = str(error).lower()
    return any(phrase in msg for phrase in FATAL_MESSAGES)


async def process_batch(client, batch, semaphore, model, dry_run=False):
    """Отправляет батч в Claude и возвращает распарсенные результаты."""
    prompt = build_batch_prompt(batch)

    if dry_run:
        log.info("DRY RUN batch (%d items), prompt length: %d", len(batch), len(prompt))
        return []

    async with semaphore:
        for attempt in range(MAX_RETRIES):
            try:
                response = await client.messages.create(
                    model=model,
                    max_tokens=2048,
                    system=SYSTEM_PROMPT,
                    messages=[{"role": "user", "content": prompt}],
                )
                text = response.content[0].text
                return parse_ai_response(text, len(batch))
            except json.JSONDecodeError as e:
                log.warning("Bad JSON from AI (attempt %d): %s", attempt + 1, e)
                if attempt == MAX_RETRIES - 1:
                    return []
            except Exception as e:
                if _is_fatal(e):
                    raise FatalAPIError(str(e))

                wait = 2 ** (attempt + 1)
                log.warning("API error (attempt %d): %s, retry in %ds", attempt + 1, e, wait)
                if attempt == MAX_RETRIES - 1:
                    return []
                await asyncio.sleep(wait)
    return []


async def process_group(client, group_num, target_date, model, dry_run=False, reprocess=False):
    """Обрабатывает все строки одной группы за целевую дату.

    Батчи запускаются параллельно (до MAX_CONCURRENT одновременно).
    Результаты сохраняются каждые 500 строк на случай падения.
    """
    data = load_date_data(group_num, target_date)
    if not data:
        log.warning("Group %d: no data for %s", group_num, target_date)
        return None

    # фильтруем строки для обработки
    to_process = [(i, row) for i, row in enumerate(data) if needs_processing(row, reprocess)]
    if not to_process:
        log.info("Group %d: nothing to process", group_num)
        return data

    log.info("Group %d: %d rows to process out of %d total", group_num, len(to_process), len(data))

    # нарезаем на батчи
    semaphore = asyncio.Semaphore(MAX_CONCURRENT)
    batches = []
    for i in range(0, len(to_process), BATCH_SIZE):
        batches.append(to_process[i:i + BATCH_SIZE])

    # обрабатываем чанками по MAX_CONCURRENT батчей параллельно
    processed = 0
    fatal = False
    SAVE_EVERY = 500

    for chunk_start in range(0, len(batches), MAX_CONCURRENT):
        chunk = batches[chunk_start:chunk_start + MAX_CONCURRENT]

        # запускаем батчи параллельно
        tasks = []
        for batch_items in chunk:
            rows = [row for _, row in batch_items]
            tasks.append(process_batch(client, rows, semaphore, model, dry_run))

        try:
            results_list = await asyncio.gather(*tasks, return_exceptions=True)
        except FatalAPIError as e:
            log.error("Group %d: fatal API error: %s", group_num, e)
            fatal = True
            break

        # применяем результаты из каждого батча
        for batch_items, results in zip(chunk, results_list):
            if isinstance(results, FatalAPIError):
                log.error("Group %d: fatal API error: %s", group_num, results)
                fatal = True
                break
            if isinstance(results, Exception):
                log.warning("Group %d: batch error: %s", group_num, results)
                results = []

            result_map = {r["id"]: r for r in results}
            for batch_idx, (data_idx, row) in enumerate(batch_items):
                ai = result_map.get(batch_idx)
                if ai:
                    data[data_idx]["ai_phone"] = ai["callerid"]
                    data[data_idx]["ai_reasoning"] = ai["reasoning"]
                else:
                    data[data_idx]["ai_phone"] = ""
                    data[data_idx]["ai_reasoning"] = ""
                data[data_idx]["ai_vs_op"] = compute_ai_vs_op(data[data_idx])

            processed += len(batch_items)

        if fatal:
            break

        # промежуточное сохранение каждые SAVE_EVERY строк
        if not dry_run and processed % SAVE_EVERY < MAX_CONCURRENT * BATCH_SIZE:
            save_date_data(group_num, target_date, data)

        if processed % 100 < MAX_CONCURRENT * BATCH_SIZE or processed == len(to_process):
            log.info("Group %d: %d/%d processed", group_num, processed, len(to_process))

    # помечаем строки без записей/транскрипций как skip
    for row in data:
        if "ai_vs_op" not in row:
            row["ai_phone"] = ""
            row["ai_reasoning"] = ""
            row["ai_vs_op"] = "skip"

    if not dry_run:
        save_date_data(group_num, target_date, data)
        log.info("Group %d: saved %s.json", group_num, target_date)

    if fatal:
        raise FatalAPIError("credit balance or billing issue")

    return data


def compute_ai_stats(data):
    """Считает AI агрегаты по обработанным данным."""
    stats = {"processed": 0, "agree": 0, "disagree": 0, "ai_only": 0, "op_only": 0}

    for row in data:
        vs = row.get("ai_vs_op", "skip")
        if vs == "skip" or vs == "both_empty":
            continue
        stats["processed"] += 1
        if vs in stats:
            stats[vs] += 1

    decided = stats["agree"] + stats["disagree"]
    stats["accuracy"] = round(stats["agree"] / decided * 100) if decided else 0

    return stats


def compute_ai_stats_by_operator(data):
    """AI агрегаты в разбивке по операторам."""
    by_op = {}

    for row in data:
        vs = row.get("ai_vs_op", "skip")
        if vs in ("skip", "both_empty"):
            continue

        op = row.get("operator", "?")
        if op not in by_op:
            by_op[op] = {"processed": 0, "agree": 0, "disagree": 0, "ai_only": 0, "op_only": 0}

        by_op[op]["processed"] += 1
        if vs in by_op[op]:
            by_op[op][vs] += 1

    for stats in by_op.values():
        decided = stats["agree"] + stats["disagree"]
        stats["accuracy"] = round(stats["agree"] / decided * 100) if decided else 0

    return by_op


def update_dates_json(group_num, target_date, ai_stats, ai_stats_by_op=None):
    """Добавляет AI статистику в dates.json."""
    dates = load_dates_json(group_num)
    if not dates:
        return

    for d in dates:
        if d["date"] == target_date:
            d["ai"] = ai_stats
            if ai_stats_by_op is not None:
                d["ai_operators"] = ai_stats_by_op
            break

    save_dates_json(group_num, dates)
    log.info("Group %d: updated dates.json with AI stats", group_num)


def backfill_stats():
    """Пересчитывает AI-статистику из существующих JSON без вызова Claude."""
    log.info("=== BACKFILL STATS MODE ===")

    for group_num in range(1, NUM_GROUPS + 1):
        dates = load_dates_json(group_num)
        if not dates:
            continue

        updated = False
        for d in dates:
            data = load_date_data(group_num, d["date"])
            if not data:
                continue

            # пропускаем даты без AI-обработки
            if not any(row.get("ai_vs_op") for row in data):
                continue

            ai_stats = compute_ai_stats(data)
            ai_by_op = compute_ai_stats_by_operator(data)

            d["ai"] = ai_stats
            d["ai_operators"] = ai_by_op
            updated = True

            log.info(
                "Group %d / %s: accuracy=%d%%, operators: %s",
                group_num, d["date"], ai_stats["accuracy"],
                ", ".join(f"{op}={s['accuracy']}%" for op, s in ai_by_op.items()),
            )

        if updated:
            save_dates_json(group_num, dates)
            log.info("Group %d: dates.json updated", group_num)


async def main():
    # --backfill-stats: пересчёт статистики без вызова Claude
    if "--backfill-stats" in sys.argv:
        backfill_stats()
        return

    dry_run = "--dry-run" in sys.argv
    reprocess = "--reprocess" in sys.argv

    # --date YYYY-MM-DD для указания конкретной даты
    explicit_date = None
    for i, arg in enumerate(sys.argv):
        if arg == "--date" and i + 1 < len(sys.argv):
            explicit_date = sys.argv[i + 1]

    if dry_run:
        log.info("=== DRY RUN MODE ===")
        client = None
        model = MODEL_API
    else:
        oauth_token = get_oauth_token()
        api_key = os.environ.get("ANTHROPIC_API_KEY")

        if oauth_token:
            client = make_client(oauth_token=oauth_token)
            model = MODEL_OAUTH
            log.info("Auth: OAuth (Claude subscription)")
        elif api_key:
            client = make_client(api_key=api_key)
            model = MODEL_API
            log.info("Auth: API key")
        else:
            log.warning("No auth available (no OAuth token, no ANTHROPIC_API_KEY)")
            return

    # определяем целевые даты
    dates = load_dates_json(1)
    if not dates:
        log.error("No dates.json found for group 1")
        return

    if explicit_date:
        target_dates = [explicit_date]
    else:
        # без --date обрабатываем предпоследнюю дату (вчера)
        td = get_target_date(dates)
        target_dates = [td] if td else []

    for target_date in target_dates:
        log.info("=== Processing date: %s %s===", target_date, "(reprocess) " if reprocess else "")

        total_stats = {"processed": 0, "agree": 0, "disagree": 0, "ai_only": 0, "op_only": 0}

        for group_num in range(1, NUM_GROUPS + 1):
            try:
                data = await process_group(
                    client, group_num, target_date, model, dry_run, reprocess,
                )
            except FatalAPIError as e:
                log.error("Fatal API error, stopping: %s", e)
                return

            if data is None:
                continue

            ai_stats = compute_ai_stats(data)
            ai_by_op = compute_ai_stats_by_operator(data)
            log.info(
                "Group %d: processed=%d, agree=%d, disagree=%d, "
                "ai_only=%d, op_only=%d, accuracy=%d%%",
                group_num, ai_stats["processed"], ai_stats["agree"],
                ai_stats["disagree"], ai_stats["ai_only"], ai_stats["op_only"],
                ai_stats["accuracy"],
            )

            if not dry_run:
                update_dates_json(group_num, target_date, ai_stats, ai_by_op)

            for k in total_stats:
                total_stats[k] += ai_stats.get(k, 0)

        decided = total_stats["agree"] + total_stats["disagree"]
        total_acc = round(total_stats["agree"] / decided * 100) if decided else 0
        log.info(
            "=== %s TOTAL: processed=%d, agree=%d, disagree=%d, accuracy=%d%% ===",
            target_date, total_stats["processed"], total_stats["agree"],
            total_stats["disagree"], total_acc,
        )


if __name__ == "__main__":
    asyncio.run(main())
