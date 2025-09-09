// index.js — Gapink Nails · v37.0.0 “IA Compacta – 1-pass, low cost”
// Objetivo: Reducir coste de IA al mínimo, manteniendo TODO el flujo por IA (sin keywords/regex).
// - 1 sola llamada a IA por mensaje (sin segunda pasada).
// - Prompt MUY corto y esquema JSON con campos abreviados.
// - Historial compacto de 6h: últimos N mensajes (configurable) y truncados por caracteres.
// - Memoria de preferencias por teléfono (salón, último servicio, staff, franja, idioma).
// - Saludo inicial SIEMPRE (si no hubo mensajes del bot en 6h) e incluye el link de reservas: https://gapinknails.square.site/
// - Anti-bucle básico (si la IA repite el mismo texto > MAX_SAME_REPLY, forzamos cierre si ya tiene todos los campos).
// - Snooze con “.” (6h por defecto).
//
// NOTA: Sin listas, sin catálogos, sin Square aquí. Es un “intake” 100% IA + memoria.
// Los campos que la IA debe devolver en JSON (todo opcional, rellena lo que detecte):
// lang  : "es"|"en"|"fr" (detectar por conversación)
// intent: "appt"|"hi"|"other"
// upd   : { svc, salon ("torremolinos"|"la_luz"), staff_any (bool), staff (string), day, part ("mañana"|"tarde"|"noche") }
// reply : string (mensaje al usuario, en lang)
// final : bool (true si ya están todos los campos)
// indifferent: { day:bool, part:bool } (opcional; si true y hay memoria, la usamos)
//
// Campos que consideramos “completos” para cerrar intake: svc, salon, (staff_any==true o staff), day, part.

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
import isoWeek from "dayjs/plugin/isoWeek.js"
import "dayjs/locale/es.js"
import { webcrypto, randomUUID } from "crypto"
import { createRequire } from "module"

if (!globalThis.crypto) globalThis.crypto = webcrypto
dayjs.extend(utc); dayjs.extend(tz); dayjs.extend(isoWeek); dayjs.locale("es")
const EURO_TZ = "Europe/Madrid"
const nowEU = () => dayjs().tz(EURO_TZ)

// ===== Config
const PORT = process.env.PORT || 8080
const BOT_DEBUG = /^true$/i.test(process.env.BOT_DEBUG || "")
const SNOOZE_HOURS = Number(process.env.SNOOZE_HOURS || 6)
const SNOOZE_ON_DOT = !/^false$/i.test(process.env.SNOOZE_ON_DOT || "true")
const AUTO_UNSNOOZE_ON_USER = !/^false$/i.test(process.env.AUTO_UNSNOOZE_ON_USER || "true")

const HISTORY_HOURS = Number(process.env.HISTORY_HOURS || 6)
const HISTORY_MAX_MSGS = Number(process.env.HISTORY_MAX_MSGS || 24)        // menos contexto = menos coste
const HISTORY_TRUNC_EACH = Number(process.env.HISTORY_TRUNC_EACH || 140)   // recortar cada línea
const PROMPT_MAX_CHARS = Number(process.env.PROMPT_MAX_CHARS || 3200)      // límite duro del prompt

const MAX_SAME_REPLY = Number(process.env.MAX_SAME_REPLY || 2)
const FIELD_COOLDOWN_MS = Number(process.env.FIELD_COOLDOWN_MS || 15000)   // anti-spam de la misma pregunta

// ===== IA (compacta)
const AI_PROVIDER = (process.env.AI_PROVIDER || (process.env.DEEPSEEK_API_KEY? "deepseek" : process.env.OPENAI_API_KEY? "openai" : "none")).toLowerCase()
const DEEPSEEK_API_KEY = process.env.DEEPSEEK_API_KEY || ""
const DEEPSEEK_MODEL   = process.env.DEEPSEEK_MODEL   || "deepseek-chat"
const OPENAI_API_KEY   = process.env.OPENAI_API_KEY || ""
const OPENAI_MODEL     = process.env.OPENAI_MODEL   || "gpt-4o-mini"
const AI_TIMEOUT_MS    = Number(process.env.AI_TIMEOUT_MS || 12000)
const AI_TEMPERATURE   = Number(process.env.AI_TEMPERATURE || 0.2)
const AI_MAX_TOKENS    = Number(process.env.AI_MAX_TOKENS || 220)          // respuesta corta

// ===== Utils
const onlyDigits = s => String(s||"").replace(/\D+/g,"")
function normalizePhoneES(raw){
  const d=onlyDigits(raw); if(!d) return null
  if (raw.startsWith("+") && d.length>=8 && d.length<=15) return `+${d}`
  if (d.startsWith("34") && d.length===11) return `+${d}`
  if (d.length===9) return `+34${d}`
  if (d.startsWith("00")) return `+${d.slice(2)}`
  return `+${d}`
}
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
function deepClone(o){ return JSON.parse(JSON.stringify(o||{})) }
function truncate(s, n){ const x=String(s||""); return x.length<=n?x:x.slice(0,n-1)+"…" }

// ===== DB
const db = new Database("gapink_compact.db"); db.pragma("journal_mode = WAL")
db.exec(`
CREATE TABLE IF NOT EXISTS sessions (
  phone TEXT PRIMARY KEY,
  data_json TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS intakes (
  id TEXT PRIMARY KEY,
  phone TEXT NOT NULL,
  salon TEXT,
  svc TEXT,
  staff_any INTEGER,
  staff TEXT,
  day TEXT,
  part TEXT,
  created_at TEXT NOT NULL,
  last_msg TEXT
);
CREATE TABLE IF NOT EXISTS logs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  phone TEXT,
  direction TEXT, -- in|out|sys
  action TEXT,
  message TEXT,
  extra TEXT,
  ts TEXT
);
CREATE TABLE IF NOT EXISTS profiles (
  phone TEXT PRIMARY KEY,
  data_json TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
`)

function loadSession(phone){
  const row = db.prepare(`SELECT data_json FROM sessions WHERE phone=@phone`).get({phone})
  return row ? JSON.parse(row.data_json) : null
}
function saveSession(phone, s){
  const json = JSON.stringify(s || {})
  const upd = db.prepare(`UPDATE sessions SET data_json=@j, updated_at=@u WHERE phone=@p`)
    .run({ j: json, u: new Date().toISOString(), p: phone })
  if (upd.changes===0){
    db.prepare(`INSERT INTO sessions (phone, data_json, updated_at) VALUES (@p,@j,@u)`)
      .run({ p: phone, j: json, u: new Date().toISOString() })
  }
}
function loadProfile(phone){
  const row = db.prepare(`SELECT data_json FROM profiles WHERE phone=@phone`).get({phone})
  return row ? JSON.parse(row.data_json) : { phone, salon:null, svc:null, staff_any:null, staff:null, part:null, lang:"es" }
}
function saveProfile(phone, p){
  const json = JSON.stringify(p || {})
  const upd = db.prepare(`UPDATE profiles SET data_json=@j, updated_at=@u WHERE phone=@p`)
    .run({ j: json, u: new Date().toISOString(), p: phone })
  if (upd.changes===0){
    db.prepare(`INSERT INTO profiles (phone, data_json, updated_at) VALUES (@p,@j,@u)`)
      .run({ p: phone, j: json, u: new Date().toISOString() })
  }
}
function clearSession(phone){ db.prepare(`DELETE FROM sessions WHERE phone=@phone`).run({phone}) }

function logEvent({phone, direction, action, message, extra}){
  try{
    db.prepare(`INSERT INTO logs (phone, direction, action, message, extra, ts)
      VALUES (@p,@d,@a,@m,@e,@t)`).run({
      p: phone || "unknown",
      d: direction || "sys",
      a: action || "event",
      m: message || null,
      e: extra ? safeJSONStringify(extra) : null,
      t: new Date().toISOString()
    })
  }catch{}
  if (BOT_DEBUG){
    try{ console.log(JSON.stringify({ phone, direction, action, message, extra, ts:new Date().toISOString() })) }catch{}
  }
}

function getHistoryCompact(phone, maxMsgs=HISTORY_MAX_MSGS, trunc=HISTORY_TRUNC_EACH){
  const since = nowEU().subtract(HISTORY_HOURS,"hour").toISOString()
  const rows = db.prepare(`SELECT direction, message FROM logs
    WHERE phone=@p AND ts>=@since AND message IS NOT NULL
    ORDER BY id DESC LIMIT @limit`).all({p:phone, since, limit:maxMsgs})
  const ordered = rows.reverse()
  return ordered.map(r=>{
    const tag = r.direction==="in" ? "U" : r.direction==="out" ? "A" : "S"
    return `${tag}:${truncate(r.message||"", trunc)}`
  })
}
function hadAssistantLast6h(phone){
  const since = nowEU().subtract(HISTORY_HOURS,"hour").toISOString()
  const row = db.prepare(`SELECT 1 FROM logs WHERE phone=@p AND direction='out' AND ts>=@since LIMIT 1`).get({p:phone, since})
  return !!row
}

// ===== Reglas de completitud
function isComplete(s){
  return !!(s.svc && s.salon && (s.staff_any===true || s.staff) && s.day && s.part)
}
function nextFieldToAsk(s){
  const order = ["svc","salon","staff","day","part"]
  const missing = []
  if (!s.svc) missing.push("svc")
  if (!s.salon) missing.push("salon")
  if (!(s.staff_any===true || s.staff)) missing.push("staff")
  if (!s.day) missing.push("day")
  if (!s.part) missing.push("part")
  for (const f of order){ if (missing.includes(f)) return f }
  return null
}
function fieldAskedRecently(session, field){
  const t = Date.now()
  session.ask_at = session.ask_at || {}
  const last = session.ask_at[field] || 0
  return (t - last) < FIELD_COOLDOWN_MS
}
function noteAskedNow(session, field){
  session.ask_at = session.ask_at || {}
  session.ask_at[field] = Date.now()
}

// ===== IA compacta (1 sola llamada)
async function aiCallCompact({langHint, known, shouldGreet, bookingURL, historyCompact, userText}){
  if (AI_PROVIDER==="none") return { lang:"es", intent:"other", upd:{}, reply:"Hola 👋", final:false }
  const controller = new AbortController()
  const timeout = setTimeout(()=>controller.abort(), AI_TIMEOUT_MS)

  // Prompt mínimo + server hints + historial ya compactado
  const sys =
`You are a WhatsApp assistant for a beauty salon. Reply ONLY with JSON, no prose.
Short keys to reduce tokens.

Schema:
{"lang":"es|en|fr","intent":"appt|hi|other","upd":{"svc":?,"salon":"torremolinos|la_luz"?,"staff_any":true|false?,"staff":?,"day":?,"part":"mañana|tarde|noche"?},"reply": "...", "final": true|false, "indifferent":{"day":true|false?,"part":true|false?}}

Rules:
- Language = user's language. Keep consistent.
- Greet at start if server says so; include online booking link.
- Ask ONE short question at a time. No lists. Be warm.
- Use known fields; don't ask what is known.
- If indifferent and profile has a pref, you may rely on it (server will fill).
- Appointment complete when svc+salon+(staff_any or staff)+day+part.
- Always return valid JSON only.`

  const server = {
    langHint,
    known,                         // {svc,salon,staff_any,staff,day,part}
    shouldGreet,                   // boolean
    bookingURL,                    // string
    nextField: nextFieldToAsk(known),
    now: nowEU().format("YYYY-MM-DD HH:mm")
  }

  // Historial comprimido en una línea por msg
  const hist = historyCompact.join("\n")

  // Construcción del prompt final (recortar si hace falta)
  let prompt =
`[SERVER]\n${JSON.stringify(server)}\n[HISTORY]\n${hist}\n[USER]\n${userText}`
  // recorte duro por caracteres si hiciera falta
  const head = sys + "\n"
  if ((head.length + prompt.length) > PROMPT_MAX_CHARS){
    const overflow = (head.length + prompt.length) - PROMPT_MAX_CHARS
    // recorta al final del history/prompt
    prompt = truncate(prompt, Math.max(200, prompt.length - overflow - 50))
  }

  try{
    const url = AI_PROVIDER==="deepseek" ? "https://api.deepseek.com/chat/completions" : "https://api.openai.com/v1/chat/completions"
    const headers = { "Content-Type":"application/json", "Authorization":`Bearer ${AI_PROVIDER==="deepseek"?DEEPSEEK_API_KEY:OPENAI_API_KEY}` }
    const messages = [
      { role:"system", content: head.trim() },
      { role:"user", content: prompt }
    ]
    const body = JSON.stringify({ model: AI_PROVIDER==="deepseek"?DEEPSEEK_MODEL:OPENAI_MODEL, temperature:AI_TEMPERATURE, max_tokens:AI_MAX_TOKENS, messages })
    const resp = await fetch(url,{ method:"POST", headers, body, signal: controller.signal })
    clearTimeout(timeout)
    if (!resp.ok) return null
    const data = await resp.json()
    const text = data?.choices?.[0]?.message?.content || ""
    return text
  }catch{
    clearTimeout(timeout); return null
  }
}
function toJSONLoose(text){
  if (!text) return null
  let s = String(text).trim()
  s = s.replace(/```json/gi,"```")
  if (s.startsWith("```")) s = s.slice(3)
  if (s.endsWith("```")) s = s.slice(0,-3)
  const i = s.indexOf("{"), j = s.lastIndexOf("}")
  if (i>=0 && j>i) s = s.slice(i, j+1)
  try{ return JSON.parse(s) }catch{ return null }
}

// ===== Mini web
const app = express()
let lastQR = null, conectado = false
app.get("/", (_req,res)=>{
  const count = db.prepare(`SELECT COUNT(*) as c FROM intakes`).get()?.c || 0
  res.send(`<!doctype html><meta charset="utf-8"><style>
  body{font-family:system-ui;display:grid;place-items:center;min-height:100vh;background:#f6f7f9}
  .card{max-width:960px;padding:28px;border-radius:18px;background:#fff;box-shadow:0 10px 32px rgba(0,0,0,.08)}
  .row{display:flex;gap:12px;align-items:center;flex-wrap:wrap}
  .pill{padding:6px 10px;border-radius:999px;background:#eef1f4;font-size:13px}
  .ok{background:#d9f7e8;color:#0f5132}.bad{background:#fde2e1;color:#842029}.warn{background:#fff3cd;color:#664d03}
  .mt{margin-top:12px}
  .foot{margin-top:16px;font-size:12px;opacity:.7}
  a{color:#1f6feb;text-decoration:none}
  </style>
  <div class="card">
    <h1>🩷 Gapink Nails Bot v37.0.0 “IA Compacta – 1-pass”</h1>
    <div class="row">
      <div class="pill ${conectado ? "ok":"bad"}">WhatsApp: ${conectado?"Conectado ✅":"Desconectado ❌"}</div>
      <div class="pill warn">Historial ${HISTORY_HOURS}h · ${HISTORY_MAX_MSGS} msgs · trunc ${HISTORY_TRUNC_EACH}ch</div>
      <div class="pill">IA: ${AI_PROVIDER.toUpperCase()}</div>
      <div class="pill">Max tokens: ${AI_MAX_TOKENS}</div>
      <div class="pill">Intakes: ${count}</div>
    </div>
    ${!conectado && lastQR ? `<div class="mt"><img src="/qr.png" width="300" style="border-radius:8px"/></div>`:""}
    <p class="mt" style="opacity:.8">Reserva online: <a target="_blank" href="https://gapinknails.square.site/">https://gapinknails.square.site/</a> (también por WhatsApp)</p>
    <div class="foot">Desarrollado por <strong>Gonzalo García Aranda</strong></div>
  </div>`)
})
app.get("/qr.png", async (_req,res)=>{
  if(!lastQR) return res.status(404).send("No QR")
  const png = await qrcode.toBuffer(lastQR, { type:"png", width:512, margin:1 })
  res.set("Content-Type","image/png").send(png)
})
app.get("/logs.json", (req,res)=>{
  const phone = req.query.phone || null
  const limit = Number(req.query.limit || 500)
  const rows = phone
    ? db.prepare(`SELECT id,phone,direction,action,message,extra,ts FROM logs WHERE phone=@p ORDER BY id DESC LIMIT @limit`).all({p:phone, limit})
    : db.prepare(`SELECT id,phone,direction,action,message,extra,ts FROM logs ORDER BY id DESC LIMIT @limit`).all({limit})
  res.json(rows.map(r=>({ ...r, extra: r.extra? JSON.parse(r.extra): null })))
})
app.get("/intakes.json", (_req,res)=>{
  const rows = db.prepare(`SELECT * FROM intakes ORDER BY created_at DESC LIMIT 500`).all()
  res.json(rows)
})
app.get("/session.json", (req,res)=>{
  const phone = String(req.query.phone||"").trim()
  if (!phone) return res.status(400).json({error:"phone required"})
  const row = db.prepare(`SELECT data_json, updated_at FROM sessions WHERE phone=@p`).get({p:phone})
  res.json({ phone, session: row?.data_json ? JSON.parse(row.data_json) : null, updated_at: row?.updated_at || null })
})
app.get("/profile.json", (req,res)=>{
  const phone = String(req.query.phone||"").trim()
  if (!phone) return res.status(400).json({error:"phone required"})
  const prof = loadProfile(phone)
  res.json(prof)
})

// ===== Baileys
async function loadBaileys(){
  const require = createRequire(import.meta.url); let mod=null
  try{ mod=require("@whiskeysockets/baileys") }catch{}; if(!mod){ mod=await import("@whiskeysockets/baileys") }
  if(!mod) throw new Error("Baileys incompatible")
  const makeWASocket = mod.makeWASocket || mod.default?.makeWASocket || (typeof mod.default==="function"?mod.default:undefined)
  const useMultiFileAuthState = mod.useMultiFileAuthState || mod.default?.useMultiFileAuthState
  const fetchLatestBaileysVersion = mod.fetchLatestBaileysVersion || mod.default?.fetchLatestBaileysVersion || (async()=>({version:[2,3000,0]}))
  const Browsers = mod.Browsers || mod.default?.Browsers || {
    linux:(n="Gapink Bot · Gonzalo García Aranda")=>["Linux",n,"121.0.0"],
    macOS:(n="Gapink Bot · Gonzalo García Aranda")=>["MacOS",n,"121.0.0"],
    windows:(n="Gapink Bot · Gonzalo García Aranda")=>["Windows",n,"121.0.0"],
  }
  return { makeWASocket, useMultiFileAuthState, fetchLatestBaileysVersion, Browsers }
}

// ===== WhatsApp loop
async function startBot(){
  try{
    const { makeWASocket, useMultiFileAuthState, fetchLatestBaileysVersion, Browsers } = await loadBaileys()
    if(!fs.existsSync("auth_info")) fs.mkdirSync("auth_info",{recursive:true})
    const { state, saveCreds } = await useMultiFileAuthState("auth_info")
    const { version } = await fetchLatestBaileysVersion().catch(()=>({version:[2,3000,0]}))
    const browserIdentity = (Browsers.linux ?? Browsers.macOS)("Gapink Bot · Gonzalo García Aranda")
    const sock = makeWASocket({ logger:pino({level:"silent"}), printQRInTerminal:false, auth:state, version, browser:browserIdentity, syncFullHistory:false })
    globalThis.sock = sock

    let lastQRcache = null
    sock.ev.on("connection.update", ({connection,qr})=>{
      if (qr){ lastQR=qr; lastQRcache=qr; conectado=false; try{ qrcodeTerminal.generate(qr,{small:true}) }catch{} }
      if (connection==="open"){ lastQR=null; conectado=true }
      if (connection==="close"){ conectado=false; setTimeout(()=>{ startBot().catch(console.error) }, 3000) }
    })
    sock.ev.on("creds.update", saveCreds)

    if (!globalThis.__q) globalThis.__q = new Map()
    const QUEUE = globalThis.__q

    async function sendText(jid, phone, text){
      if (!text) return
      try{ await sock.sendMessage(jid, { text }) }catch(e){}
      logEvent({phone, direction:"out", action:"send", message:text})
    }

    sock.ev.on("messages.upsert", async ({messages})=>{
      const m = messages?.[0]; if (!m?.message) return
      const jid = m.key.remoteJid
      const isFromMe = !!m.key.fromMe
      const phone = normalizePhoneES((jid||"").split("@")[0]||"") || (jid||"").split("@")[0]
      const textRaw = (m.message.conversation || m.message.extendedTextMessage?.text || m.message?.imageMessage?.caption || "").trim()
      const pushName = (m.pushName || "").trim()
      if (!textRaw) return

      const prev = QUEUE.get(phone)||Promise.resolve()
      const job = prev.then(async ()=>{
        let s = loadSession(phone) || {
          phone,
          svc:null, salon:null, staff_any:null, staff:null, day:null, part:null,
          lang:null,
          snooze_until_ms:null,
          last_bot_reply:null, same_reply_count:0,
          ask_at:{}
        }
        let p = loadProfile(phone)

        try{
          // Silencio manual con "."
          if (isFromMe && textRaw.trim()==="." && SNOOZE_ON_DOT){
            s.snooze_until_ms = nowEU().add(SNOOZE_HOURS,"hour").valueOf()
            saveSession(phone, s)
            logEvent({phone, direction:"sys", action:"manual_snooze", message:"."})
            return
          }
          // Auto-unsnooze si habla el cliente
          if (!isFromMe && s.snooze_until_ms && nowEU().valueOf() < s.snooze_until_ms){
            if (AUTO_UNSNOOZE_ON_USER){
              s.snooze_until_ms = null
              saveSession(phone, s)
              logEvent({phone, direction:"sys", action:"auto_unsnooze_on_user", message:textRaw})
            } else {
              logEvent({phone, direction:"sys", action:"dropped_due_snooze", message:textRaw})
              return
            }
          }

          // Log entrada
          logEvent({phone, direction:"in", action:"message", message:textRaw})

          // Historial y saludo
          const histCompact = getHistoryCompact(phone)
          const shouldGreet = !hadAssistantLast6h(phone)

          // Compilar "known" para IA (usa memoria si está vacío tras cierre)
          const known = {
            svc:   s.svc || p.svc || null,
            salon: s.salon || p.salon || null,
            staff_any: (s.staff_any!=null) ? s.staff_any : (p.staff_any!=null ? p.staff_any : null),
            staff: s.staff || p.staff || null,
            day:   s.day || null,
            part:  s.part || p.part || null
          }
          const langHint = s.lang || p.lang || "es"

          // Anti-ask cooldown
          const nxt = nextFieldToAsk(s)
          if (nxt && fieldAskedRecently(s, nxt)){
            // intenta preguntar el siguiente que falte con cooldown distinto
            const order = ["svc","salon","staff","day","part"]
            for (const f of order){
              if (f===nxt) continue
              if (f==="staff" && (s.staff_any===true || s.staff)) continue
              if (f==="svc" && s.svc) continue
              if (f==="salon" && s.salon) continue
              if (f==="day" && s.day) continue
              if (f==="part" && s.part) continue
              noteAskedNow(s, f); break
            }
          } else if (nxt) {
            noteAskedNow(s, nxt)
          }

          // ===== Llamada IA compacta
          const bookingURL = "https://gapinknails.square.site/"
          const aiRaw = await aiCallCompact({
            langHint, known, shouldGreet, bookingURL,
            historyCompact: histCompact, userText: textRaw
          })
          logEvent({phone, direction:"sys", action:"ai_raw", message: truncate(String(aiRaw||""), 1000)})

          const aiJson = toJSONLoose(aiRaw) || {}
          logEvent({phone, direction:"sys", action:"ai_json", message: safeJSONStringify(aiJson)})

          // Aplicar updates (SIN keywords; solo lo que diga la IA)
          if (aiJson.lang) s.lang = aiJson.lang
          const up = aiJson.upd || {}
          if (typeof up.svc === "string" && up.svc.trim()) s.svc = up.svc.trim()
          if (up.salon === "torremolinos" || up.salon === "la_luz") s.salon = up.salon
          if (typeof up.staff_any === "boolean") s.staff_any = up.staff_any
          if (typeof up.staff === "string" && up.staff.trim()){ s.staff = up.staff.trim(); s.staff_any = false }
          if (typeof up.day === "string" && up.day.trim()) s.day = up.day.trim()
          if (up.part && ["mañana","tarde","noche"].includes(up.part)) s.part = up.part

          // Si marcó indiferente y hay memoria, completa desde perfil
          const ind = aiJson.indifferent || {}
          if (ind.day && !s.day) s.day = "cualquiera"
          if (ind.part && !s.part) s.part = p.part || "tarde"
          if (s.staff_any==null && (p.staff_any===true || p.staff)) {
            if (p.staff_any===true) s.staff_any = true
            else { s.staff = p.staff; s.staff_any = false }
          }

          // Mensaje IA
          let reply = (typeof aiJson.reply==="string"? aiJson.reply.trim() : "") || ""
          const finalizeFlag = !!aiJson.final
          const completed = isComplete(s)

          // Anti-bucle: si repite lo mismo
          if (reply && s.last_bot_reply && reply === s.last_bot_reply){
            s.same_reply_count = (s.same_reply_count||0) + 1
          } else {
            s.same_reply_count = 0
          }
          // Si ya está completo o hay bucle, cerramos
          if (completed || finalizeFlag || s.same_reply_count >= MAX_SAME_REPLY){
            // Guardar intake
            const id = `int_${randomUUID().slice(0,8)}_${Date.now().toString(36)}`
            db.prepare(`INSERT INTO intakes
              (id, phone, salon, svc, staff_any, staff, day, part, created_at, last_msg)
            VALUES
              (@id,@phone,@salon,@svc,@staff_any,@staff,@day,@part,@created,@raw)`).run({
              id, phone, salon:s.salon, svc:s.svc, staff_any: s.staff_any?1:0, staff:s.staff||null,
              day:s.day||null, part:s.part||null,
              created: new Date().toISOString(), raw: textRaw
            })

            // Actualizar memoria de perfil
            const p0 = deepClone(p)
            p.lang = s.lang || p.lang || "es"
            p.salon = s.salon || p.salon
            p.svc = s.svc || p.svc
            if (s.staff_any===true){ p.staff_any = true; p.staff = null }
            if (s.staff){ p.staff_any = false; p.staff = s.staff }
            p.part = s.part || p.part
            saveProfile(phone, p)

            // Mensaje de cierre si la IA no lo dio
            if (!reply){
              reply = (p.lang==="en")
                ? "All set! I’ve got everything noted. If you prefer, you can also book online: https://gapinknails.square.site/"
                : (p.lang==="fr")
                ? "Parfait, tout est noté. Tu peux aussi réserver en ligne : https://gapinknails.square.site/"
                : "¡Listo! Tengo todo apuntado. Si prefieres, también puedes reservar online: https://gapinknails.square.site/"
            }
            await sendText(jid, phone, reply)
            s.last_bot_reply = reply
            saveSession(phone, s)
            return
          }

          // Si IA no dio texto, pon una pregunta mínima (fallback raro)
          if (!reply){
            // Esta es la única rama donde podríamos “generar” algo sin IA para no quedarnos mudos
            const f = nextFieldToAsk(s)
            const lang = s.lang || p.lang || "es"
            if (f==="svc"){
              reply = (lang==="en")?"What service would you like?":"fr"===lang?"Tu veux quel service ?":"¿Qué servicio te gustaría?"
            } else if (f==="salon"){
              reply = (lang==="en")?"Torremolinos or La Luz?":"fr"===lang?"Torremolinos ou La Luz ?":"¿Torremolinos o La Luz?"
            } else if (f==="staff"){
              reply = (lang==="en")?"Any stylist or someone specific?":"fr"===lang?"N’importe qui ou quelqu’un en particulier ?":"¿Cualquiera del equipo o alguien en concreto?"
            } else if (f==="day"){
              reply = (lang==="en")?"What day works for you?":"fr"===lang?"Quel jour te convient ?":"¿Qué día te viene bien?"
            } else { // part
              reply = (lang==="en")?"Morning, afternoon or evening?":"fr"===lang?"Matin, après-midi ou soir ?":"¿Mañana, tarde o noche?"
            }
          }

          // Enviar respuesta IA
          await sendText(jid, phone, reply)
          s.last_bot_reply = reply
          saveSession(phone, s)

        }catch(err){
          logEvent({phone, direction:"sys", action:"handler_error", message: err?.message, extra:{stack: err?.stack}})
          const lang = s.lang || loadProfile(phone).lang || "es"
          const txt = (lang==="en")?"Oops, small glitch. Torremolinos or La Luz?"
                    : (lang==="fr")?"Oups, petit bug. Torremolinos ou La Luz ?"
                    : "Se me cruzó un cable 🤯. ¿Torremolinos o La Luz?"
          try{ await sock.sendMessage(jid, { text: txt }) }catch{}
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
console.log(`🩷 Gapink Nails Bot v37.0.0 “IA Compacta – 1-pass” · Historial ${HISTORY_HOURS}h/${HISTORY_MAX_MSGS} msgs · trunc ${HISTORY_TRUNC_EACH}ch · AI:${AI_PROVIDER.toUpperCase()} · maxTokens=${AI_MAX_TOKENS}`)
const server = app.listen(PORT, ()=>{ startBot().catch(console.error) })
process.on("uncaughtException", e=>{ console.error("uncaughtException:", e?.stack||e?.message||e) })
process.on("unhandledRejection", e=>{ console.error("unhandledRejection:", e) })
process.on("SIGTERM", ()=>{ try{ server.close(()=>process.exit(0)) }catch{ process.exit(0) } })
process.on("SIGINT",  ()=>{ try{ server.close(()=>process.exit(0)) }catch{ process.exit(0) } })
