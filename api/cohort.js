// /api/cohort.js  — streaming + accumulate
// Why: non-streaming Anthropic calls for big SYSTEM_PROMPT + big output JSON often
// take 40-60+ s. Vercel proxies kill idle connections → 504. Streaming keeps the
// connection alive (first byte ~1-2s, chunks every few hundred ms), so no proxy
// ever sees an idle socket. We collect the chunks server-side and return the same
// { text } shape the frontend already expects → zero frontend rendering changes.

export const config = { maxDuration: 300 };

export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') { res.status(200).end(); return; }
  if (req.method !== 'POST')    { res.status(405).end(); return; }

  try {
    const { product, sp } = req.body || {};
    if (!product || !sp) { res.status(400).json({ error: 'Missing product or sp' }); return; }

    const upstream = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': process.env.ANTHROPIC_API_KEY,
        'anthropic-version': '2023-06-01',
        'anthropic-beta': 'prompt-caching-2024-07-31'
      },
      body: JSON.stringify({
        model: 'claude-sonnet-4-6',
        max_tokens: 8096,
        stream: true,                             // <-- the fix
        system: [{ type: 'text', text: sp, cache_control: { type: 'ephemeral' } }],
        messages: [{ role: 'user', content: 'Product: ' + product }]
      })
    });

    if (!upstream.ok || !upstream.body) {
      const raw = await upstream.text().catch(() => '');
      let errorMsg;
      try { errorMsg = JSON.parse(raw)?.error?.message || raw; }
      catch { errorMsg = raw || `Anthropic API error: ${upstream.status}`; }
      res.status(upstream.status || 502).json({ error: errorMsg });
      return;
    }

    const reader  = upstream.body.getReader();
    const decoder = new TextDecoder();
    let buffer    = '';
    let fullText  = '';
    let streamErr = null;

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });

      // Parse complete SSE events ("\n\n"-terminated)
      let idx;
      while ((idx = buffer.indexOf('\n\n')) !== -1) {
        const event = buffer.slice(0, idx);
        buffer = buffer.slice(idx + 2);
        for (const line of event.split('\n')) {
          if (!line.startsWith('data: ')) continue;
          const payload = line.slice(6).trim();
          if (!payload || payload === '[DONE]') continue;
          try {
            const ev = JSON.parse(payload);
            if (ev.type === 'content_block_delta' && ev.delta?.type === 'text_delta') {
              fullText += ev.delta.text || '';
            } else if (ev.type === 'error') {
              streamErr = ev.error?.message || 'Stream error';
            }
          } catch { /* malformed SSE chunk — skip */ }
        }
      }
    }

    if (streamErr) { res.status(502).json({ error: streamErr }); return; }
    if (!fullText) { res.status(502).json({ error: 'Empty response from model' }); return; }

    res.status(200).json({ text: fullText });
  } catch (e) {
    if (!res.headersSent) res.status(500).json({ error: e.message || 'Unknown server error' });
  }
}
