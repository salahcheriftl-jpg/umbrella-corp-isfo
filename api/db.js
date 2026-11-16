// Tiny shared store for the Umbrella intranet. One Vercel function + Vercel's built-in KV (Upstash Redis).
const crypto = require("crypto");
// works with any env prefix Vercel/Upstash picked (KV_, UPSTASH_REDIS_, STORAGE_, ...)
const pick = (re) => { const k = Object.keys(process.env).find((n) => re.test(n) && process.env[n]); return k ? process.env[k] : undefined; };
const RU = process.env.KV_REST_API_URL || process.env.UPSTASH_REDIS_REST_URL || pick(/(REST_API_URL|REST_URL)$/);
const RT = process.env.KV_REST_API_TOKEN || process.env.UPSTASH_REDIS_REST_TOKEN || pick(/(REST_API_TOKEN|REST_TOKEN)$/);
const FIXED = ["agents", "docs", "ch-lobby", "dms", "invites"];
const ID_RE = /^[A-Za-z0-9_|-]{1,90}$/, UID_RE = /^[A-Za-z0-9]{6,40}$/, TOK_RE = /^[A-Za-z0-9]{16,64}$/;
const AUTH = new Map(); // uid -> {th, until}  (only verified pairs are cached)

async function redis(cmds) {
  const r = await fetch(RU + "/pipeline", {
    method: "POST",
    headers: { Authorization: "Bearer " + RT, "Content-Type": "application/json" },
    body: JSON.stringify(cmds),
  });
  if (!r.ok) throw new Error("redis " + r.status);
  return (await r.json()).map((x) => { if (x.error) throw new Error(x.error); return x.result; });
}
const colOk = (c, uid) => FIXED.includes(c) || c === "data_users_" + uid + "_friends";
const isStr = (v, n) => typeof v === "string" && v.length <= n;
function pairs(flat) { const o = []; for (let i = 0; i < (flat || []).length; i += 2) { try { o.push({ id: flat[i], data: JSON.parse(flat[i + 1]) }); } catch (e) {} } return o; }
const bad = (m) => { const e = new Error(m); e.status = 403; return e; };

async function doWrite(w, uid) {
  const { op, col } = w, id = String(w.id);
  if (!["set", "update", "delete"].includes(op)) throw bad("op");
  if (!colOk(col, uid) || !ID_RE.test(id)) throw bad("col");
  const data = w.data && typeof w.data === "object" && !Array.isArray(w.data) ? w.data : null;
  if (op !== "delete" && (!data || JSON.stringify(data).length > 10000)) throw bad("data");
  const [raw] = await redis([["HGET", "umb:c:" + col, id]]);
  const ex = raw ? JSON.parse(raw) : null;

  if (col === "agents") { if (id !== uid || (op !== "delete" && data.uid !== uid)) throw bad("forbidden"); }
  else if (col === "docs") {
    if (op === "set") { if (ex || data.uid !== uid || !isStr(data.title, 120) || !isStr(data.body, 8000)) throw bad("forbidden"); }
    else if (op === "update" || !ex || ex.uid !== uid) { if (op === "delete" && !ex) return; throw bad("forbidden"); }
  } else if (col === "ch-lobby") {
    if (op !== "set" || ex || data.uid !== uid || !isStr(data.text, 2000)) throw bad("forbidden");
  } else if (col === "dms") {
    if (op !== "set" || ex || data.from !== uid || !isStr(data.text, 2000) || !String(data.pair || "").split("|").includes(uid)) throw bad("forbidden");
  } else if (col === "invites") {
    if (op === "set") { if (data.from !== uid || (ex && ex.from !== uid)) throw bad("forbidden"); }
    else if (op === "update") { if (!ex || ex.to !== uid) throw bad("forbidden"); }
    else { if (!ex) return; if (ex.to !== uid && ex.from !== uid) throw bad("forbidden"); }
  } // friends list: owner only (colOk)

  if (op === "delete") await redis([["HDEL", "umb:c:" + col, id], ["INCR", "umb:v:" + col]]);
  else {
    if (op === "update" && !ex) throw bad("notfound");
    const val = op === "update" ? Object.assign({}, ex, data) : data;
    await redis([["HSET", "umb:c:" + col, id, JSON.stringify(val)], ["INCR", "umb:v:" + col]]);
    if (col === "ch-lobby" || col === "dms") {
      const [n] = await redis([["HLEN", "umb:c:" + col]]);
      if (n > 900) {
        const [flat] = await redis([["HGETALL", "umb:c:" + col]]);
        const old = pairs(flat).sort((a, b) => (a.data.ts || 0) - (b.data.ts || 0)).slice(0, n - 600).map((r) => r.id);
        if (old.length) await redis([["HDEL", "umb:c:" + col, ...old]]);
      }
    }
  }
}

function readOk(r, uid) {
  const w = Array.isArray(r.where) ? r.where : [];
  const has = (f, fn) => w.some((x) => x && x[0] === f && x[1] === "==" && fn(String(x[2])));
  if (r.col === "dms") return has("to", (v) => v === uid) || has("pair", (v) => v.split("|").includes(uid));
  if (r.col === "invites") return has("to", (v) => v === uid) || has("from", (v) => v === uid);
  return true;
}

module.exports = async (req, res) => {
  res.setHeader("Cache-Control", "no-store");
  if (req.method !== "POST") return res.status(405).json({ ok: false, error: "method" });
  if (!RU || !RT) return res.status(200).json({ ok: false, error: "storage" });
  let b = req.body;
  if (typeof b === "string") { try { b = JSON.parse(b); } catch (e) { b = {}; } }
  b = b || {};
  if (b.ping) return res.status(200).json({ ok: true });
  const uid = String(b.uid || ""), tok = String(b.tok || "");
  if (!UID_RE.test(uid) || !TOK_RE.test(tok)) return res.status(400).json({ ok: false, error: "bad-id" });
  try {
    const th = crypto.createHash("sha256").update(tok).digest("hex");
    let authed = null;
    const auth = async () => {
      if (authed !== null) return authed;
      const c = AUTH.get(uid);
      if (c && c.th === th && c.until > Date.now()) return (authed = true);
      const key = "umb:tok:" + uid;
      let [cur] = await redis([["GET", key]]);
      if (cur === null) { await redis([["SET", key, th, "NX"]]); [cur] = await redis([["GET", key]]); }
      authed = cur === th;
      if (authed) AUTH.set(uid, { th, until: Date.now() + 600000 });
      return authed;
    };
    const writes = Array.isArray(b.writes) ? b.writes.slice(0, 10) : [];
    if (writes.length) {
      if (!(await auth())) return res.status(403).json({ ok: false, error: "auth" });
      for (const w of writes) await doWrite(w || {}, uid);
    }
    const reads = Array.isArray(b.reads) ? b.reads.slice(0, 12) : [];
    const out = { ok: true, vers: {}, results: {}, errs: {} };
    const cols = [...new Set(reads.map((r) => r.col))].filter((c) => colOk(c, uid));
    if (cols.length) {
      const [vs] = await redis([["MGET", ...cols.map((c) => "umb:v:" + c)]]);
      cols.forEach((c, i) => (out.vers[c] = String(vs[i] || "0")));
      const cv = b.vers || {};
      const changed = cols.filter((c) => b.full || cv[c] !== out.vers[c]);
      const todo = reads.filter((r) => changed.includes(r.col));
      if (todo.some((r) => ["dms", "invites"].includes(r.col)) && !(await auth())) return res.status(403).json({ ok: false, error: "auth" });
      const need = [...new Set(todo.map((r) => r.col))];
      const got = {};
      if (need.length) {
        const rs = await redis(need.map((c) => ["HGETALL", "umb:c:" + c]));
        need.forEach((c, i) => (got[c] = pairs(rs[i])));
      }
      for (const r of todo) {
        if (!readOk(r, uid)) { out.errs[r.k] = "forbidden"; continue; }
        const w = Array.isArray(r.where) ? r.where : [];
        out.results[r.k] = got[r.col].filter((x) => w.every((f) => f && f[1] === "==" && x.data[f[0]] === f[2])).slice(0, 1000);
      }
    }
    return res.status(200).json(out);
  } catch (e) {
    return res.status(e.status || 500).json({ ok: false, error: e.status ? "forbidden" : "server" });
  }
};
