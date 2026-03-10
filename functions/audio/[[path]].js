// Cloudflare Pages Function: proxy audio requests to HTTP API
// /audio/?action=getMP3&... → http://assist.intmarksol.com/api/?action=getMP3&...
export async function onRequest(context) {
  const url = new URL(context.request.url);
  const path = context.params.path ? context.params.path.join("/") : "";
  const target = `http://assist.intmarksol.com/api/${path}${url.search}`;

  const resp = await fetch(target);

  // pass through with CORS headers so audio element can play it
  const headers = new Headers(resp.headers);
  headers.set("Access-Control-Allow-Origin", "*");

  return new Response(resp.body, {
    status: resp.status,
    headers,
  });
}
