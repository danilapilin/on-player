// Cloudflare Pages Function: proxy audio requests to HTTP API
// /audio/path/to/file → http://assist.intmarksol.com/api/path/to/file
export async function onRequest(context) {
  const path = context.params.path ? context.params.path.join("/") : "";
  const target = `http://assist.intmarksol.com/api/${path}`;

  const resp = await fetch(target);

  // pass through with CORS headers so audio element can play it
  const headers = new Headers(resp.headers);
  headers.set("Access-Control-Allow-Origin", "*");

  return new Response(resp.body, {
    status: resp.status,
    headers,
  });
}
