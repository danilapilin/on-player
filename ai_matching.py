"""
AI Shadow Matching: анализ транскрипций звонков с помощью Claude.

Для каждой строки с записями AI определяет, какой callerid — обратный звонок
с сайта. Результат сравнивается с оператором (agree/disagree/ai_only/op_only).

Запуск: python ai_matching.py [--dry-run] [--date YYYY-MM-DD]

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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

OUTPUT_DIR = "data"
NUM_GROUPS = 4
BATCH_SIZE = 10
MAX_CONCURRENT = 5
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
    """Достаёт OAuth токен из macOS keychain (Claude Code credentials)."""
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


def make_client(oauth_token=None, api_key=None):
    """Создаёт Anthropic клиент — через OAuth (подписка) или API key."""
    import anthropic
    import httpx

    if oauth_token:
        # OAuth через подписку — как в ims-aibot
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

    # API key — для GitHub Actions
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


def needs_processing(row):
    """Строка подходит для AI: есть записи с транскрипциями и ещё не обработана."""
    if row.get("ai_phone") is not None:
        return False
    if not row.get("recs"):
        return False
    # хотя бы одна запись с непустой транскрипцией
    return any(r.get("transcript", "").strip() for r in row["recs"])


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
    # убираем markdown обёртку если есть
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

    # проверяем что все id в диапазоне
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

    # оба есть — сравниваем (частичное совпадение, как в calc_status)
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
                # фатальные ошибки — прекращаем всю обработку
                if _is_fatal(e):
                    raise FatalAPIError(str(e))

                wait = 2 ** (attempt + 1)
                log.warning("API error (attempt %d): %s, retry in %ds", attempt + 1, e, wait)
                if attempt == MAX_RETRIES - 1:
                    return []
                await asyncio.sleep(wait)
    return []


async def process_group(client, group_num, target_date, model, dry_run=False):
    """Обрабатывает все строки одной группы за целевую дату."""
    data = load_date_data(group_num, target_date)
    if not data:
        log.warning("Group %d: no data for %s", group_num, target_date)
        return None

    # фильтруем строки для обработки
    to_process = [(i, row) for i, row in enumerate(data) if needs_processing(row)]
    if not to_process:
        log.info("Group %d: all rows already processed or no eligible rows", group_num)
        return data

    log.info("Group %d: %d rows to process out of %d total", group_num, len(to_process), len(data))

    # батчим
    semaphore = asyncio.Semaphore(MAX_CONCURRENT)
    batches = []
    for i in range(0, len(to_process), BATCH_SIZE):
        chunk = to_process[i:i + BATCH_SIZE]
        batches.append(chunk)

    processed = 0
    fatal = False
    for batch_items in batches:
        indices = [idx for idx, _ in batch_items]
        rows = [row for _, row in batch_items]

        try:
            results = await process_batch(client, rows, semaphore, model, dry_run)
        except FatalAPIError as e:
            log.error("Group %d: fatal API error, stopping: %s", group_num, e)
            fatal = True
            break

        # применяем результаты
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
        if processed % 100 == 0 or processed == len(to_process):
            log.info("Group %d: %d/%d processed", group_num, processed, len(to_process))

    # помечаем строки без записей как skip
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

    # accuracy: agree / (agree + disagree) — только по строкам где оба нашли
    decided = stats["agree"] + stats["disagree"]
    stats["accuracy"] = round(stats["agree"] / decided * 100) if decided else 0

    return stats


def update_dates_json(group_num, target_date, ai_stats):
    """Добавляет AI статистику в dates.json."""
    dates = load_dates_json(group_num)
    if not dates:
        return

    for d in dates:
        if d["date"] == target_date:
            d["ai"] = ai_stats
            break

    save_dates_json(group_num, dates)
    log.info("Group %d: updated dates.json with AI stats", group_num)


async def main():
    dry_run = "--dry-run" in sys.argv

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
        # пробуем OAuth из keychain (подписка), потом API key
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

    # определяем целевую дату (одна и та же для всех групп)
    dates = load_dates_json(1)
    if not dates:
        log.error("No dates.json found for group 1")
        return

    target_date = explicit_date or get_target_date(dates)
    log.info("Target date: %s", target_date)

    total_stats = {"processed": 0, "agree": 0, "disagree": 0, "ai_only": 0, "op_only": 0}

    for group_num in range(1, NUM_GROUPS + 1):
        try:
            data = await process_group(client, group_num, target_date, model, dry_run)
        except FatalAPIError as e:
            log.error("Fatal API error, stopping all groups: %s", e)
            break

        if data is None:
            continue

        ai_stats = compute_ai_stats(data)
        log.info(
            "Group %d AI stats: processed=%d, agree=%d, disagree=%d, "
            "ai_only=%d, op_only=%d, accuracy=%d%%",
            group_num, ai_stats["processed"], ai_stats["agree"],
            ai_stats["disagree"], ai_stats["ai_only"], ai_stats["op_only"],
            ai_stats["accuracy"],
        )

        if not dry_run:
            update_dates_json(group_num, target_date, ai_stats)

        for k in total_stats:
            total_stats[k] += ai_stats.get(k, 0)

    decided = total_stats["agree"] + total_stats["disagree"]
    total_acc = round(total_stats["agree"] / decided * 100) if decided else 0
    log.info(
        "=== TOTAL: processed=%d, agree=%d, disagree=%d, accuracy=%d%% ===",
        total_stats["processed"], total_stats["agree"],
        total_stats["disagree"], total_acc,
    )


if __name__ == "__main__":
    asyncio.run(main())
