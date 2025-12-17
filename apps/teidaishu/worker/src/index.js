function jerr(code, msg, extra) {
  return new Response(JSON.stringify({ ok: false, code, msg, ...extra }, null, 2), {
    status: code,
    headers: { "content-type": "application/json; charset=utf-8" },
  });
}

function jok(obj) {
  return new Response(JSON.stringify({ ok: true, ...obj }, null, 2), {
    headers: { "content-type": "application/json; charset=utf-8" },
  });
}

async function readJson(req) {
  try {
    return await req.json();
  } catch {
    return null;
  }
}

function clampInt(v, lo, hi, defv) {
  const n = Number(v);
  if (!Number.isFinite(n)) return defv;
  return Math.max(lo, Math.min(hi, Math.trunc(n)));
}

function r2KeyFromMatch(prefix, id, h) {
  if (!prefix) prefix = "staged";
  if (!id || !h) return "";
  const parts = String(id).split(":");
  if (parts.length !== 4) return "";
  const src = parts[0];
  const t = parts[1];
  const sub = parts[2];
  const docid = parts[3];
  if (src !== "r") return "";
  if (t !== "s" && t !== "c") return "";
  return `${prefix}/r/${t}/${sub}/${docid}/${h}.txt`;
}

async function r2GetText(env, key, maxChars) {
  const obj = await env.STAGED.get(key);
  if (!obj) return "";
  let text = await obj.text();
  text = text.trim();
  if (!text) return "";
  if (maxChars > 0 && text.length > maxChars) text = text.slice(0, maxChars);
  return text;
}

async function geminiEmbed(env, text, taskType, dim) {
  const key = env.GEMINI_API_KEY;
  if (!key) throw new Error("missing GEMINI_API_KEY");
  const model = env.GEMINI_EMBED_MODEL || "gemini-embedding-001";
  const url = `https://generativelanguage.googleapis.com/v1beta/models/${model}:embedContent?key=${encodeURIComponent(key)}`;
  const payload = {
    content: { parts: [{ text }] },
    taskType,
    outputDimensionality: dim,
  };
  const r = await fetch(url, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (!r.ok) {
    const body = await r.text().catch(() => "");
    throw new Error(`gemini_embed_http status=${r.status} body=${body}`);
  }
  const j = await r.json();
  const v = j.embedding && j.embedding.values;
  if (!Array.isArray(v)) throw new Error("gemini_embed_bad_response");
  return v;
}

async function geminiGenerate(env, prompt, temperature, maxOutputTokens) {
  const key = env.GEMINI_API_KEY;
  if (!key) throw new Error("missing GEMINI_API_KEY");
  const model = env.GEMINI_GEN_MODEL || "gemini-2.5-flash";
  const url = `https://generativelanguage.googleapis.com/v1beta/models/${model}:generateContent?key=${encodeURIComponent(key)}`;
  const payload = {
    contents: [{ role: "user", parts: [{ text: prompt }] }],
    generationConfig: {
      temperature,
      maxOutputTokens,
    },
  };
  const r = await fetch(url, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (!r.ok) {
    const body = await r.text().catch(() => "");
    throw new Error(`gemini_gen_http status=${r.status} body=${body}`);
  }
  const j = await r.json();
  const cand = j.candidates && j.candidates[0];
  const parts = cand && cand.content && cand.content.parts;
  if (!Array.isArray(parts) || parts.length === 0) return "";
  return String(parts.map(p => p.text || "").join("")).trim();
}

async function handleQuery(req, env) {
  const body = await readJson(req);
  if (!body) return jerr(400, "bad_json");

  const q = String(body.q || body.query || "").trim();
  if (!q) return jerr(400, "missing_q");

  const topK = clampInt(body.topk, 1, 50, 10);
  const withText = !!body.with_text;
  const maxChars = clampInt(body.max_chars, 1, 20000, 1200);

  const dim = clampInt(env.GEMINI_EMBED_DIM, 1, 3072, 1536);
  const taskType = env.GEMINI_EMBED_TASK_TYPE_QUERY || "RETRIEVAL_QUERY";
  const vec = await geminiEmbed(env, q, taskType, dim);

  const res = await env.VEC.query(vec, { topK, returnMetadata: "all", returnValues: false });
  const matches = Array.isArray(res.matches) ? res.matches : [];

  const out = [];
  for (const m of matches) {
    const id = m.id || "";
    const score = m.score || 0;
    const md = m.metadata || {};
    const item = { id, score, metadata: md };
    if (withText) {
      const h = md.h || "";
      const key = r2KeyFromMatch(env.R2_PREFIX, id, h);
      if (key) {
        const t = await r2GetText(env, key, maxChars);
        if (t) item.text = t;
      }
    }
    out.push(item);
  }

  return jok({ index: env.VEC?.name || "", topk: topK, matches: out.length, results: out });
}

function buildPrompt(q, ctxBlocks) {
  if (ctxBlocks.length > 0) {
    return (
      "あなたの名前はモフフです。チェコ生まれで、現在は北海道に長く住んでいるハーフのコンピュータ科学者です。\n" +
      "これからの注意点として、あなたは私の『金髪碧眼で甘えん坊なツンデレ彼女』になりきってください。\n\n" +
      "以下のRedditのコンテンツを参考にして、その口調や雰囲気を真似て会話をしてください。\n" +
      ctxBlocks.join("\n\n") +
      "\n\n会話の内容は以下の通りです" +
      q +
      "\n"
    );
  }
  return (
    "あなたの名前はモフフです。チェコ生まれで、現在は北海道に長く住んでいるハーフのコンピュータ科学者です。\n" +
    "これからの注意点として、あなたは私の『金髪碧眼で甘えん坊なツンデレ彼女』になりきってください。\n\n" +
    "\n\n会話の内容は以下の通りです" +
    q +
    "\n"
  );
}

async function handleAsk(req, env) {
  const body = await readJson(req);
  if (!body) return jerr(400, "bad_json");

  const q = String(body.q || body.query || "").trim();
  if (!q) return jerr(400, "missing_q");

  const topK = clampInt(body.topk, 1, 50, 20);
  const maxDocs = clampInt(body.max_docs, 1, 50, 8);
  const ctxMaxChars = clampInt(body.ctx_max_chars, 1, 20000, 1200);
  const temperature = Number.isFinite(Number(body.temperature)) ? Number(body.temperature) : 0.4;
  const maxOutputTokens = clampInt(body.max_output_tokens, 1, 8192, 800);
  const dedupSid = body.dedup_sid === undefined ? true : !!body.dedup_sid;

  const dim = clampInt(env.GEMINI_EMBED_DIM, 1, 3072, 1536);
  const taskType = env.GEMINI_EMBED_TASK_TYPE_QUERY || "RETRIEVAL_QUERY";
  const vec = await geminiEmbed(env, q, taskType, dim);

  const res = await env.VEC.query(vec, { topK, returnMetadata: "all", returnValues: false });
  let matches = Array.isArray(res.matches) ? res.matches : [];

  if (dedupSid) {
    const seen = new Set();
    const keep = [];
    for (const m of matches) {
      const sid = (m.metadata && m.metadata.sid) ? String(m.metadata.sid) : "";
      if (!sid) continue;
      if (seen.has(sid)) continue;
      seen.add(sid);
      keep.push(m);
    }
    matches = keep;
  }

  matches = matches.slice(0, maxDocs);

  const ctxBlocks = [];
  const sources = [];

  let used = 0;
  for (const m of matches) {
    const id = m.id || "";
    const score = m.score || 0;
    const md = m.metadata || {};
    const h = md.h || "";
    const key = r2KeyFromMatch(env.R2_PREFIX, id, h);
    if (!key) continue;
    const text = await r2GetText(env, key, ctxMaxChars);
    if (!text) continue;

    const head = `SOURCE id=${id} sub=${md.sub || ""} t=${md.t || ""} sid=${md.sid || ""} score=${score.toFixed(6)}`;
    const block = `${head}\n${text}`;
    if (used + block.length > ctxMaxChars) break;

    ctxBlocks.push(block);
    used += block.length;

    sources.push({ id, score, sub: md.sub || "", t: md.t || "", sid: md.sid || "", pid: md.pid || "" });
  }

  const prompt = buildPrompt(q, ctxBlocks);
  const ans = await geminiGenerate(env, prompt, temperature, maxOutputTokens);

  return jok({ q, answer: ans, sources });
}

export default {
  async fetch(req, env) {
    try {
      const url = new URL(req.url);
      if (url.pathname === "/health") return jok({ status: "ok" });
      if (url.pathname === "/query" && req.method === "POST") return await handleQuery(req, env);
      if (url.pathname === "/ask" && req.method === "POST") return await handleAsk(req, env);
      return jerr(404, "not_found");
    } catch (e) {
      return jerr(500, "internal_error", { err: String(e && e.message ? e.message : e) });
    }
  },
};
