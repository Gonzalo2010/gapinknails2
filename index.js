// index.js — Gapink Nails · v44.0.0
// “IA decide saludo/cita/info · 1 sola pregunta + mute 6h (auto y por '.') + búsqueda avanzada de logs”
// Reglas nuevas:
// - Preguntas más cercanas (tono natural).
// - Salón siempre con opciones explícitas: Torremolinos / La Luz.
// - Franja solo "mañana" o "tarde". Si el usuario dice "noche", re-pregunta (no se acepta noche).
// - Con todos los datos: SILENCIO 6h (sin resumen ni confirmación).
//
// ENV (opcionales):
//   PORT, BOT_DEBUG, HISTORY_HOURS, HISTORY_MAX_MSGS, HISTORY_TRUNC_EACH,
//   AI_PROVIDER (deepseek|openai), DEEPSEEK_API_KEY, DEEPSEEK_MODEL, OPENAI_API_KEY, OPENAI_MODEL,
//   AI_TIMEOUT_MS, AI_TEMPERATURE, AI_MAX_TOKENS, MUTE_HOURS

import express from "express"
import pino from "pino"
import qrcode from "qrcode"
import qrcodeTerminal from "qrcode-terminal"
import "dotenv/config"
import fs from "fs"
import Database from "better-sqlite3"
import dayjs from "dayjs"
import utc from "dayjs/plugin/utc.js"
import tz from "dayjs/plugin/timezone.js"
import "dayjs/locale/es.js"
import { webcrypto } from "crypto"
import { createRequire } from "module"

if (!globalThis.crypto) globalThis.crypto = webcrypto
dayjs.extend(utc); dayjs.extend(tz); dayjs.locale("es")
const EURO_TZ = "Europe/Madrid"
const nowEU = () => dayjs().tz(EURO_TZ)

// ===== Marca
const BRAND = "Gapink Nails"
const BOOKING_URL = "https://gapinknails.square.site/"

// ===== Config
const PORT = process.env.PORT || 8080
const BOT_DEBUG = /^true$/i.test(process.env.BOT_DEBUG || "")
const HISTORY_HOURS = Number(process.env.HISTORY_HOURS || 6)
const HISTORY_MAX_MSGS = Number(process.env.HISTORY_MAX_MSGS || 40)
const HISTORY_TRUNC_EACH = Number(process.env.HISTORY_TRUNC_EACH || 180)
const MUTE_HOURS = Number(process.env.MUTE_HOURS || 6)

// ===== IA
const AI_PROVIDER = (process.env.AI_PROVIDER || (process.env.DEEPSEEK_API_KEY? "deepseek" : process.env.OPENAI_API_KEY? "openai" : "none")).toLowerCase()
const DEEPSEEK_API_KEY = process.env.DEEPSEEK_API_KEY || ""
const DEEPSEEK_MODEL   = process.env.DEEPSEEK_MODEL   || "deepseek-chat"
const OPENAI_API_KEY   = process.env.OPENAI_API_KEY   || ""
const OPENAI_MODEL     = process.env.OPENAI_MODEL     || "gpt-4o-mini"
const AI_TIMEOUT_MS    = Number(process.env.AI_TIMEOUT_MS || 10000)
const AI_TEMPERATURE   = Number(process.env.AI_TEMPERATURE || 0.15)
const AI_MAX_TOKENS    = Number(process.env.AI_MAX_TOKENS || 160)

// ===== Utils
function truncate(s, n){ const x=String(s||""); return x.length<=n?x:x.slice(0,n-1)+"…" }
function safeJSONStringify(v){
  const seen = new WeakSet()
  try{
    return JSON.stringify(v, (_k, val)=>{
      if (typeof val === "bigint") return val.toString()
      if (typeof val === "object" && val !== null){
        if (seen.has(val)) return "[Circular]"
        seen.add(val)
      }
      return val
    })
  }catch{ try { return String(v) } catch { return "[Unserializable]" } }
}
const onlyDigits = s => String(s||"").replace(/\D+/g,"")
function normalizePhoneE164(raw){
  const d=onlyDigits(raw); if(!d) return null
  if (raw.startsWith("+") && d.length>=8 && d.length<=15) return `+${d}`
  if (d.startsWith("34") && d.length===11) return `+${d}`
  if (d.length===9) return `+34${d}`
  if (d.startsWith("00")) return `+${d.slice(2)}`
  return `+${d}`
}
function isJustDot(text){
  if (!text) return false
  return text.trim() === "."
}
function parseDateOrNull(v){
  if (!v) return null
  const d = dayjs(v).isValid() ? dayjs(v) : null
  return d ? d.toISOString() : null
}
function normalizePart(part){
  if (!part) return null
  const t = String(part).toLowerCase()
  if (t === "mañana") return "mañana"
  if (t === "tarde")  return "tarde"
  // "noche" u otras → no válido, forzamos re-pregunta
  return null
}

// ===== DB
const db = new Database("gapink_ai_classifier_v440.db"); db.pragma("journal_mode = WAL")
db.exec(`
CREATE TABLE IF NOT EXISTS logs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  phone TEXT,
  direction TEXT,        -- in|out|sys
  message TEXT,
  extra TEXT,
  ts TEXT
);
CREATE TABLE IF NOT EXISTS sessions (
  phone TEXT PRIMARY KEY,
  data_json TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_logs_ts ON logs(ts);
CREATE INDEX IF NOT EXISTS idx_logs_phone ON logs(phone);
CREATE INDEX IF NOT EXISTS idx_logs_direction ON logs(direction);
`)
let FTS_AVAILABLE = false
try {
  db.exec(`
    CREATE VIRTUAL TABLE IF NOT EXISTS logs_fts USING fts5(message, content='logs', content_rowid='id');
    CREATE TRIGGER IF NOT EXISTS logs_ai AFTER INSERT ON logs BEGIN
      INSERT INTO logs_fts(rowid, message) VALUES (new.id, new.message);
    END;
    CREATE TRIGGER IF NOT EXISTS logs_ad AFTER DELETE ON logs BEGIN
      INSERT INTO logs_fts(logs_fts, rowid, message) VALUES('delete', old.id, old.message);
    END;
    CREATE TRIGGER IF NOT EXISTS logs_au AFTER UPDATE ON logs BEGIN
      INSERT INTO logs_fts(logs_fts, rowid, message) VALUES('delete', old.id, old.message);
      INSERT INTO logs_fts(rowid, message) VALUES (new.id, new.message);
    END;
  `)
  db.prepare("SELECT count(*) AS n FROM logs_fts").get()
  FTS_AVAILABLE = true
} catch { FTS_AVAILABLE = false }

function logEvent({phone, direction, message, extra}){
  try{
    db.prepare("INSERT INTO logs (phone,direction,message,extra,ts) VALUES (@p,@d,@m,@e,@t)").run({
      p: phone || "unknown",
      d: direction || "sys",
      m: message || null,
      e: extra ? safeJSONStringify(extra) : null,
      t: new Date().toISOString()
    })
  }catch{}
  if (BOT_DEBUG){ try{ console.log(JSON.stringify({ phone:phone||"?", direction, message, extra })) }catch{} }
}
function getHistoryCompact(phone){
  const since = nowEU().subtract(HISTORY_HOURS,"hour").toISOString()
  const rows = db.prepare(`SELECT direction, message FROM logs
    WHERE phone=@p AND ts>=@since AND message IS NOT NULL
    ORDER BY id DESC LIMIT @limit`).all({p:phone, since, limit:HISTORY_MAX_MSGS})
  return rows.reverse().map(r=>{
    const tag = r.direction==="in" ? "U" : r.direction==="out" ? "A" : "S"
    return `${tag}:${truncate(r.message||"", HISTORY_TRUNC_EACH)}`
  })
}
function loadSession(phone){
  const row = db.prepare(`SELECT data_json FROM sessions WHERE phone=@p`).get({p:phone})
  const base = { phone, lang:"es", last_summary:null, mute_until:null, mute_reason:null }
  if (!row) return base
  try{
    const parsed = JSON.parse(row.data_json)
    return { ...base, ...parsed }
  }catch{
    return base
  }
}
function saveSession(phone, s){
  const j = JSON.stringify(s||{})
  const up = db.prepare(`UPDATE sessions SET data_json=@j, updated_at=@u WHERE phone=@p`).run({j, u:new Date().toISOString(), p:phone})
  if (up.changes===0){
    db.prepare(`INSERT INTO sessions (phone,data_json,updated_at) VALUES (@p,@j,@u)`).run({p:phone, j, u:new Date().toISOString()})
  }
}
function isMuted(session){
  if (!session?.mute_until) return false
  try{ return dayjs(session.mute_until).isAfter(nowEU()) }catch{ return false }
}
function setMute(session, hours, reason="manual"){
  const until = nowEU().add(hours,"hour").toISOString()
  session.mute_until = until
  session.mute_reason = reason
  return session
}

// ===== IA: clasificador compacto
async function aiClassify({brand, bookingURL, historyCompact, userText}){
  if (AI_PROVIDER==="none") return null
  const controller = new AbortController()
  const timeout = setTimeout(()=>controller.abort(), AI_TIMEOUT_MS)

  const sys =
`You are the WhatsApp assistant for a BEAUTY SALON called "${brand}".
Your ONLY task is to classify the conversation and extract info to avoid asking twice.
Return STRICT JSON and NOTHING else.

Schema:
{
  "lang": "es|en|fr|... (detected)",
  "is_greeting": true|false,
  "wants_appointment": true|false,
  "extracted": {
    "svc": string|null,
    "salon": "torremolinos"|"la_luz"|null,
    "staff_any": true|false|null,
    "staff": string|null,
    "day": string|null,
    "part": "mañana"|"tarde"|null   // ONLY morning or afternoon (no night)
  },
  "missing": ["svc"|"salon"|"staff_or_any"|"day"|"part", ...],
  "reply_hint": "brief, friendly cue to ask for the FIRST missing item (<=140 chars, 1 question, no menus)"
}

Rules:
- Consider ONLY the last ${HISTORY_HOURS} hours of chat (compact history provided).
- If the user already stated something (e.g., 'cejas', 'Torremolinos', 'me da igual quien'), include it in extracted.
- Appointment intent: phrases like "quiero cita", "appointment", etc.
- Greeting when the CURRENT message is greeting-like.
- NEVER include ${bookingURL} in reply_hint (UI adds it separately).
- IMPORTANT: time-of-day MUST be "mañana" or "tarde". If the user implies night/evening, leave "part" as null so we can re-ask.`

  const payload = {
    brand,
    now: nowEU().format("YYYY-MM-DD HH:mm"),
    history: historyCompact,
    user: userText
  }

  try{
    const url = AI_PROVIDER==="deepseek" ? "https://api.deepseek.com/chat/completions" : "https://api.openai.com/v1/chat/completions"
    const headers = { "Content-Type":"application/json", "Authorization":`Bearer ${AI_PROVIDER==="deepseek"?DEEPSEEK_API_KEY:OPENAI_API_KEY}` }
    const messages = [
      { role:"system", content: sys },
      { role:"user", content: JSON.stringify(payload) }
    ]
    const body = JSON.stringify({ model: AI_PROVIDER==="deepseek"?DEEPSEEK_MODEL:OPENAI_MODEL, temperature:AI_TEMPERATURE, max_tokens:AI_MAX_TOKENS, messages })
    const resp = await fetch(url,{ method:"POST", headers, body, signal: controller.signal })
    clearTimeout(timeout)
    if (!resp.ok) return null
    const data = await resp.json()
    const txt = data?.choices?.[0]?.message?.content || ""
    let s = String(txt).trim().replace(/```json/gi,"```")
    if (s.startsWith("```")) s = s.slice(3)
    if (s.endsWith("```")) s = s.slice(0,-3)
    const i = s.indexOf("{"), j = s.lastIndexOf("}")
    if (i>=0 && j>i) s = s.slice(i, j+1)
    return JSON.parse(s)
  }catch{
    clearTimeout(timeout); return null
  }
}

// ===== Mini web (estado, QR y endpoints)
const app = express()
let lastQR = null, conectado = false

app.get("/", (_req,res)=>{
  res.send(`<!doctype html><meta charset="utf-8"><style>
  body{font-family:system-ui;display:grid;place-items:center;min-height:100vh;background:#f6f7f9;margin:0}
  .card{max-width:1000px;padding:28px;border-radius:18px;background:#fff;box-shadow:0 10px 30px rgba(0,0,0,.08)}
  .row{display:flex;gap:10px;flex-wrap:wrap}
  .pill{padding:6px 10px;border-radius:999px;background:#eef1f4;font-size:13px}
  .ok{background:#d9f7e8;color:#0f5132}.bad{background:#fde2e1;color:#842029}
  .mt{margin-top:12px}
  a{color:#1f6feb;text-decoration:none}
  .foot{margin-top:8px;opacity:.7;font-size:12px}
  code{background:#f4f6f8;padding:2px 6px;border-radius:6px}
  </style>
  <div class="card">
    <h1>🩷 ${BRAND} — IA Clasificador v44.0.0</h1>
    <div class="row">
      <span class="pill ${conectado?"ok":"bad"}">WhatsApp: ${conectado?"Conectado ✅":"Desconectado ❌"}</span>
      <span class="pill">Historial IA ${HISTORY_HOURS}h · máx ${HISTORY_MAX_MSGS} msgs</span>
      <span class="pill">IA: ${AI_PROVIDER.toUpperCase()} · tokens=${AI_MAX_TOKENS}</span>
      <span class="pill">Mute default: ${MUTE_HOURS}h</span>
      <span class="pill">FTS: ${FTS_AVAILABLE?"ON":"OFF"}</span>
    </div>
    ${!conectado && lastQR ? `<div class="mt"><img src="/qr.png" width="280" style="border-radius:10px"/></div>`:""}
    <p class="mt">Reserva online: <a target="_blank" href="${BOOKING_URL}">${BOOKING_URL}</a></p>

    <h3 class="mt">🔎 Filtros rápidos de logs</h3>
    <p><code>/logs.json?phone=+34666...&q=cejas&dir=in&from=2025-09-09T00:00:00&to=2025-09-10&limit=200&offset=0&order=desc</code></p>
    <p><code>/logs.ndjson?... (mismos parámetros, salida por líneas)</code></p>
    <p><code>/sessions.json</code> · <code>/sessions.json?phone=+34...</code> · <code>/session/unmute?phone=+34...</code></p>

    <div class="foot">Desarrollado por <strong>Gonzalo García Aranda</strong></div>
  </div>`)
})
app.get("/qr.png", async (_req,res)=>{
  if(!lastQR) return res.status(404).send("No QR")
  const png = await qrcode.toBuffer(lastQR, { type:"png", width:512, margin:1 })
  res.set("Content-Type","image/png").send(png)
})

// --- Logs JSON con filtros ---
app.get("/logs.json", (req,res)=>{
  const phone = req.query.phone || null
  const q     = (req.query.q || "").toString().trim()
  const dir   = (req.query.dir || "").toString().trim().toLowerCase()
  const from  = parseDateOrNull(req.query.from)
  const to    = parseDateOrNull(req.query.to)
  const limit = Math.min(Number(req.query.limit || 200), 1000)
  const offset= Math.max(Number(req.query.offset || 0), 0)
  const order = (req.query.order||"desc").toString().toLowerCase()==="asc" ? "ASC" : "DESC"

  let params = {}
  let where = ["1=1"]

  if (phone){ where.push("phone=@phone"); params.phone = phone }
  if (dir && ["in","out","sys"].includes(dir)){ where.push("direction=@dir"); params.dir = dir }
  if (from){ where.push("ts>=@from"); params.from = from }
  if (to){ where.push("ts<=@to"); params.to = to }

  let sql
  if (q){
    if (FTS_AVAILABLE){
      sql = `
        SELECT l.id,l.phone,l.direction,l.message,l.extra,l.ts
        FROM logs l JOIN logs_fts f ON f.rowid = l.id
        WHERE (${where.join(" AND ")}) AND f.logs_fts MATCH @q
        ORDER BY l.id ${order}
        LIMIT @limit OFFSET @offset
      `
      params.q = q
    } else {
      sql = `
        SELECT id,phone,direction,message,extra,ts
        FROM logs
        WHERE (${where.join(" AND ")}) AND message LIKE @like
        ORDER BY id ${order}
        LIMIT @limit OFFSET @offset
      `
      params.like = `%${q}%`
    }
  } else {
    sql = `
      SELECT id,phone,direction,message,extra,ts
      FROM logs
      WHERE ${where.join(" AND ")}
      ORDER BY id ${order}
      LIMIT @limit OFFSET @offset
    `
  }

  params.limit = limit
  params.offset = offset

  const rows = db.prepare(sql).all(params).map(r => ({ ...r, extra: r.extra? JSON.parse(r.extra): null }))
  res.json({ fts: FTS_AVAILABLE, count: rows.length, items: rows })
})

// --- Logs NDJSON (stream) ---
app.get("/logs.ndjson", (req,res)=>{
  res.setHeader("Content-Type", "application/x-ndjson; charset=utf-8")

  const phone = req.query.phone || null
  const q     = (req.query.q || "").toString().trim()
  const dir   = (req.query.dir || "").toString().trim().toLowerCase()
  const from  = parseDateOrNull(req.query.from)
  const to    = parseDateOrNull(req.query.to)
  const limit = Math.min(Number(req.query.limit || 5000), 20000)
  const offset= Math.max(Number(req.query.offset || 0), 0)
  const order = (req.query.order||"desc").toString().toLowerCase()==="asc" ? "ASC" : "DESC"

  let params = {}
  let where = ["1=1"]

  if (phone){ where.push("phone=@phone"); params.phone = phone }
  if (dir && ["in","out","sys"].includes(dir)){ where.push("direction=@dir"); params.dir = dir }
  if (from){ where.push("ts>=@from"); params.from = from }
  if (to){ where.push("ts<=@to"); params.to = to }

  let sql
  if (q){
    if (FTS_AVAILABLE){
      sql = `
        SELECT l.id,l.phone,l.direction,l.message,l.extra,l.ts
        FROM logs l JOIN logs_fts f ON f.rowid=l.id
        WHERE (${where.join(" AND ")}) AND f.logs_fts MATCH @q
        ORDER BY l.id ${order}
        LIMIT @limit OFFSET @offset
      `
      params.q = q
    } else {
      sql = `
        SELECT id,phone,direction,message,extra,ts
        FROM logs
        WHERE (${where.join(" AND ")}) AND message LIKE @like
        ORDER BY id ${order}
        LIMIT @limit OFFSET @offset
      `
      params.like = `%${q}%`
    }
  } else {
    sql = `
      SELECT id,phone,direction,message,extra,ts
      FROM logs
      WHERE ${where.join(" AND ")}
      ORDER BY id ${order}
      LIMIT @limit OFFSET @offset
    `
  }

  params.limit = limit
  params.offset = offset

  const stmt = db.prepare(sql)
  const iter = stmt.iterate(params)
  for (const r of iter){
    const out = { ...r, extra: r.extra? JSON.parse(r.extra): null }
    res.write(JSON.stringify(out) + "\n")
  }
  res.end()
})

// --- Sessions helpers ---
app.get("/sessions.json", (req,res)=>{
  const phone = req.query.phone || null
  if (phone){
    const row = db.prepare(`SELECT data_json, updated_at FROM sessions WHERE phone=@p`).get({p:phone})
    if (!row) return res.json(null)
    let data = null; try{ data = JSON.parse(row.data_json) }catch{}
    return res.json({ phone, data, updated_at: row.updated_at })
  } else {
    const rows = db.prepare(`SELECT phone, data_json, updated_at FROM sessions ORDER BY updated_at DESC LIMIT 500`).all()
    const out = rows.map(r=>{
      let data = null; try{ data = JSON.parse(r.data_json) }catch{}
      return { phone:r.phone, data, updated_at:r.updated_at }
    })
    return res.json(out)
  }
})
app.get("/session/unmute", (req,res)=>{
  const phone = req.query.phone
  if (!phone) return res.status(400).json({ ok:false, error:"phone required" })
  const s = loadSession(phone)
  s.mute_until = null
  s.mute_reason = null
  saveSession(phone, s)
  logEvent({phone, direction:"sys", message:"manual_unmute", extra:{by:"http"}})
  res.json({ ok:true, phone, mute_until:s.mute_until })
})

// ===== WhatsApp (Baileys)
async function loadBaileys(){
  const require = createRequire(import.meta.url); let mod=null
  try{ mod=require("@whiskeysockets/baileys") }catch{}; if(!mod){ mod=await import("@whiskeysockets/baileys") }
  if(!mod) throw new Error("Baileys incompatible")
  const makeWASocket = mod.makeWASocket || mod.default?.makeWASocket || (typeof mod.default==="function"?mod.default:undefined)
  const useMultiFileAuthState = mod.useMultiFileAuthState || mod.default?.useMultiFileAuthState
  const fetchLatestBaileysVersion = mod.fetchLatestBaileysVersion || mod.default?.fetchLatestBaileysVersion || (async()=>({version:[2,3000,0]}))
  const Browsers = mod.Browsers || mod.default?.Browsers || {
    linux:(n=`${BRAND} Bot · Gonzalo García Aranda`)=>["Linux",n,"121.0.0"],
    macOS:(n=`${BRAND} Bot · Gonzalo García Aranda`)=>["MacOS",n,"121.0.0"],
    windows:(n=`${BRAND} Bot · Gonzalo García Aranda`)=>["Windows",n,"121.0.0"],
  }
  return { makeWASocket, useMultiFileAuthState, fetchLatestBaileysVersion, Browsers }
}

async function startBot(){
  try{
    const { makeWASocket, useMultiFileAuthState, fetchLatestBaileysVersion, Browsers } = await loadBaileys()
    if(!fs.existsSync("auth_info")) fs.mkdirSync("auth_info",{recursive:true})
    const { state, saveCreds } = await useMultiFileAuthState("auth_info")
    const { version } = await fetchLatestBaileysVersion().catch(()=>({version:[2,3000,0]}))
    const browserIdentity = (Browsers.linux ?? Browsers.macOS)(`${BRAND} Bot · Gonzalo García Aranda`)
    const sock = makeWASocket({ logger:pino({level:"silent"}), printQRInTerminal:false, auth:state, version, browser:browserIdentity, syncFullHistory:false })
    globalThis.sock = sock

    sock.ev.on("connection.update", ({connection,qr})=>{
      if (qr){ lastQR=qr; conectado=false; try{ qrcodeTerminal.generate(qr,{small:true}) }catch{} }
      if (connection==="open"){ lastQR=null; conectado=true }
      if (connection==="close"){ conectado=false; setTimeout(()=>{ startBot().catch(console.error) }, 3000) }
    })
    sock.ev.on("creds.update", saveCreds)

    if (!globalThis.__q) globalThis.__q = new Map()
    const QUEUE = globalThis.__q

    async function sendText(jid, phone, text){
      if (!text) return
      try{ await sock.sendMessage(jid, { text }) }catch{}
      logEvent({phone, direction:"out", message:text})
    }

    sock.ev.on("messages.upsert", async ({messages})=>{
      const m = messages?.[0]; if (!m?.message) return
      const jid = m.key.remoteJid
      const isFromMe = !!m.key.fromMe
      const phone = normalizePhoneE164((jid||"").split("@")[0]||"") || (jid||"").split("@")[0]
      const textRaw = (m.message.conversation || m.message.extendedTextMessage?.text || m.message?.imageMessage?.caption || "").trim()
      if (!textRaw) return

      const prev = QUEUE.get(phone)||Promise.resolve()
      const job = prev.then(async ()=>{
        try{
          // Sesión para mute, etc.
          let session = loadSession(phone)

          // 0) Silencio manual por "." — no responder
          if (isJustDot(textRaw)){
            session = setMute(session, MUTE_HOURS, isFromMe ? "manual-dot-staff" : "manual-dot-user")
            saveSession(phone, session)
            logEvent({phone, direction:"in", message:textRaw, extra:{fromMe:isFromMe, action:"mute", until:session.mute_until}})
            return
          }

          // Log de entrada
          logEvent({phone, direction:"in", message:textRaw, extra:{fromMe:isFromMe}})

          // Si es mensaje “fromMe” y no es ".", no activar bot
          if (isFromMe) return

          // 1) Si la conversación está muteada, no respondemos
          if (isMuted(session)){
            logEvent({phone, direction:"sys", message:"muted_skip", extra:{until:session.mute_until, reason:session.mute_reason}})
            return
          }

          // 2) Compacta historial para IA
          const hist = getHistoryCompact(phone)

          // 3) IA: clasificación + extracción
          const ai = await aiClassify({
            brand: BRAND,
            bookingURL: BOOKING_URL,
            historyCompact: hist,
            userText: textRaw
          })
          logEvent({phone, direction:"sys", message:"ai_json", extra: ai})

          // 4) Respuesta mínima (tono cercano + reglas nuevas)
          const lang = ai?.lang || "es"
          const exRaw = ai?.extracted || {}
          const ex = { ...exRaw, part: normalizePart(exRaw.part) }

          // Construimos lista de faltantes saneando la franja
          let missing = Array.isArray(ai?.missing) ? ai.missing.slice() : []
          if (exRaw.part && !ex.part){
            // El usuario dijo "noche" u otra cosa → aseguramos que se pida "part"
            if (!missing.includes("part")) missing.push("part")
          }

          // Sin IA -> saludo cercano + enlace
          if (!ai){
            const reply = `¡Hola! Soy la asistente de ${BRAND} 🩷 Reserva online si te viene mejor: ${BOOKING_URL}. ¿En qué te echo un cable?`
            await sendText(jid, phone, reply); return
          }

          // Saludo sin cita -> saludo cercano + enlace
          if (ai.is_greeting && !ai.wants_appointment){
            const reply = `¡Hola! Soy la asistente de ${BRAND} 🩷 También puedes reservar aquí: ${BOOKING_URL}. Dime, ¿en qué te ayudo?`
            await sendText(jid, phone, reply); return
          }

          // Quiere cita → preguntar SOLO lo que falte (con opciones claras)
          if (ai.wants_appointment){
            if (!missing.length){
              // Todos los datos → silencio 6h (sin confirmar)
              const salonTxt = ex.salon==="la_luz" ? "La Luz" : (ex.salon==="torremolinos" ? "Torremolinos" : "—")
              const staffTxt = (ex.staff_any===true) ? "cualquiera del equipo" : (ex.staff? ex.staff : "—")
              const resumenInterno = `Datos completos: ${ex.svc||"servicio"} · ${salonTxt} · ${staffTxt} · ${ex.day||"día?"} · ${ex.part||"franja?"}`
              session = setMute(session, MUTE_HOURS, "auto-after-data-complete_no-confirm")
              session.last_summary = resumenInterno
              saveSession(phone, session)
              logEvent({phone, direction:"sys", message:"auto_muted_after_complete_no_confirm", extra:{until:session.mute_until, data:ex}})
              return
            } else {
              const first = missing[0]
              let ask = null

              // Siempre opciones de salón claras
              if (first === "salon"){
                ask = "Genial. ¿Qué salón te viene mejor: Torremolinos o La Luz?"
              }
              // Franja solo mañana/tarde (si venía 'noche', aclaramos)
              else if (first === "part"){
                if (exRaw.part && !ex.part){
                  ask = "Solo trabajamos mañana o tarde. ¿Cuál te viene mejor?"
                } else {
                  ask = "¿Te viene mejor por la mañana o por la tarde?"
                }
              }
              else if (first === "svc"){
                ask = "Perfecto. ¿Qué te quieres hacer? (ej.: cejas, uñas, depilación...)"
              }
              else if (first === "staff_or_any"){
                ask = "¿Te da igual quién te atienda o prefieres a alguien en concreto?"
              }
              else if (first === "day"){
                ask = "¿Qué día te vendría bien pasar?"
              }

              // Fallback amigable por si la IA trae un hint
              if (!ask){
                const hint = (typeof ai?.reply_hint==="string" && ai.reply_hint.trim()) ? ai.reply_hint.trim() : null
                // Sanitizamos el hint si sugiere "noche"
                if (hint && /noche|night|evening/i.test(hint)){
                  ask = "Solo trabajamos mañana o tarde. ¿Cuál te viene mejor?"
                } else {
                  ask = hint || "Cuéntame lo que te falta y lo vemos."
                }
              }

              await sendText(jid, phone, ask); return
            }
          }

          // No saludo, no cita → respuesta mínima cercana
          const reply = "¡Anotado! Si quieres, te reservo cita. Dime salón (Torremolinos o La Luz), día y si prefieres mañana o tarde."
          await sendText(jid, phone, reply)

        }catch(err){
          logEvent({phone, direction:"sys", message:"handler_error", extra:{msg:err?.message, stack:err?.stack}})
          try{ await sendText(jid, phone, "Uff, me he liado un poco 😅 ¿Te pido la cita?") }catch{}
        }
      })
      QUEUE.set(phone, job.finally(()=>{ if (QUEUE.get(phone)===job) QUEUE.delete(phone) }))
    })
  }catch(e){
    console.error(e)
    setTimeout(()=>{ startBot().catch(console.error) }, 3000)
  }
}

// ===== Arranque
const appListen = app.listen(PORT, ()=>{ startBot().catch(console.error) })
process.on("uncaughtException", e=>{ console.error("uncaughtException:", e?.stack||e?.message||e) })
process.on("unhandledRejection", e=>{ console.error("unhandledRejection:", e) })
process.on("SIGTERM", ()=>{ try{ appListen.close(()=>process.exit(0)) }catch{ process.exit(0) } })
process.on("SIGINT",  ()=>{ try{ appListen.close(()=>process.exit(0)) }catch{ process.exit(0) } })
