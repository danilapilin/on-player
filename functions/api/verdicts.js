// Cloudflare Pages Function: centralized verdict storage via KV
// GET /api/verdicts?group=1&date=2026-03-06 → all verdicts for group+date
// PUT /api/verdicts → save single verdict { group, date, idx, verdict, comment }

const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, PUT, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type",
};

function json(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json", ...CORS },
  });
}

// KV key: verdicts_{group}_{date} → { "idx": { verdict, comment, ts } }

export async function onRequestOptions() {
  return new Response(null, { headers: CORS });
}

export async function onRequestGet(context) {
  const url = new URL(context.request.url);
  const group = url.searchParams.get("group");
  const date = url.searchParams.get("date");

  if (!group || !date) {
    return json({ error: "group and date required" }, 400);
  }

  const key = `verdicts_${group}_${date}`;
  const data = await context.env.VERDICTS.get(key, "json");
  return json(data || {});
}

export async function onRequestPut(context) {
  const body = await context.request.json();
  const { group, date, idx, verdict, comment } = body;

  if (!group || !date || idx === undefined || !verdict) {
    return json({ error: "group, date, idx, verdict required" }, 400);
  }

  const key = `verdicts_${group}_${date}`;
  const existing = (await context.env.VERDICTS.get(key, "json")) || {};

  existing[String(idx)] = { verdict, comment: comment || "", ts: new Date().toISOString() };

  await context.env.VERDICTS.put(key, JSON.stringify(existing));
  return json({ ok: true });
}

export async function onRequestDelete(context) {
  const url = new URL(context.request.url);
  const group = url.searchParams.get("group");
  const date = url.searchParams.get("date");
  const idx = url.searchParams.get("idx");

  if (!group || !date || !idx) {
    return json({ error: "group, date, idx required" }, 400);
  }

  const key = `verdicts_${group}_${date}`;
  const existing = (await context.env.VERDICTS.get(key, "json")) || {};

  delete existing[String(idx)];

  await context.env.VERDICTS.put(key, JSON.stringify(existing));
  return json({ ok: true });
}
