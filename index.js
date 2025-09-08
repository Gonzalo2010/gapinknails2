// index.js — Gapink Nails · v35.2.0
// IA orquestadora + LOGS COMPLETOS + anti-bucle + no silencio accidental
// - Historial 6h a IA (usuario+bot)
// - Todos los mensajes los redacta la IA (tono cercano, idioma detectado)
// - Antibucle + defaults ("día cualquiera", "tarde", "equipo")
// - Snooze configurable (NO se calla tras finalizar por defecto). “.” del negocio silencia si quieres
// - LOGS completos con AI JSON, sesión before/after, errores + endpoints /logs, /logs.json, /logs.csv, /intakes.json, /session.json, /config.json

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
import { webcrypto, createHash } from "crypto"
import { createRequire } from "module"

if (!globalThis.crypto) globalThis.crypto = webcrypto
dayjs.extend(utc); dayjs.extend(tz); dayjs.extend(isoWeek); dayjs.locale("es")
const EURO_TZ = "Europe/Madrid"
const nowEU = () => dayjs().tz(EURO_TZ)

// ===== Config (ENV)
const PORT = process.env.PORT || 8080
const BOT_DEBUG = /^true$/i.test(process.env.BOT_DEBUG || "")
const SNOOZE_ENABLED = !/^false$/i.test(process.env.SNOOZE_ENABLED || "true")
const SNOOZE_ON_DOT = !/^false$/i.test(process.env.SNOOZE_ON_DOT || "true")
const SNOOZE_AFTER_FINALIZE = /^true$/i.test(process.env.SNOOZE_AFTER_FINALIZE || "false") // por defecto NO callar tras finalizar
const AUTO_UNSNOOZE_ON_USER = !/^false$/i.test(process.env.AUTO_UNSNOOZE_ON_USER || "true")
const SNOOZE_HOURS = Number(process.env.SNOOZE_HOURS || 6)
const HISTORY_HOURS = Number(process.env.HISTORY_HOURS || 6)
const HISTORY_MAX_MSGS = Number(process.env.HISTORY_MAX_MSGS || 80)
const MAX_SAME_REPLY = Number(process.env.MAX_SAME_REPLY || 2)

// ===== IA
const AI_PROVIDER = (process.env.AI_PROVIDER || (process.env.DEEPSEEK_API_KEY? "deepseek" : process.env.OPENAI_API_KEY? "openai" : "none")).toLowerCase()
const DEEPSEEK_API_KEY = process.env.DEEPSEEK_API_KEY || ""
const DEEPSEEK_MODEL   = process.env.DEEPSEEK_MODEL   || "deepseek-chat"
const OPENAI_API_KEY   = process.env.OPENAI_API_KEY || ""
const OPENAI_MODEL     = process.env.OPENAI_MODEL   || "gpt-4o-mini"
const AI_TIMEOUT_MS    = Number(process.env.AI_TIMEOUT_MS || 18000)

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
const titleCase = str => String(str||"").toLowerCase().replace(/\b([a-z])/g, m=>m.toUpperCase())
const norm = s => String(s||"").normalize("NFD").replace(/\p{Diacritic}/gu,"").toLowerCase().trim()
function safeJSONStringify(value){
  const seen = new WeakSet()
  try{
    return JSON.stringify(value, (_k, v)=>{
      if (typeof v === "bigint") return v.toString()
      if (typeof v === "object" && v !== null){
        if (seen.has(v)) return "[Circular]"
        seen.add(v)
      }
      return v
    })
  }catch{ try { return String(value) } catch { return "[Unserializable]" } }
}

// ===== DB
const db = new Database("gapink_aiorchestrator.db"); db.pragma("journal_mode = WAL")
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
  service_text TEXT,
  staff_any INTEGER,
  staff_name TEXT,
  day_text TEXT,
  part_of_day TEXT,
  created_at TEXT NOT NULL,
  raw_last_msg TEXT
);
CREATE TABLE IF NOT EXISTS logs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  phone TEXT,
  direction TEXT, -- "in" | "out" | "sys"
  action TEXT,
  message TEXT,
  extra TEXT,
  ts TEXT
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
function getHistory(phone, hours=HISTORY_HOURS, limit=HISTORY_MAX_MSGS){
  const since = nowEU().subtract(hours,"hour").toISOString()
  const rows = db.prepare(`SELECT direction, message, ts, extra FROM logs
                           WHERE phone=@p AND ts>=@since AND message IS NOT NULL
                           ORDER BY id DESC LIMIT @limit`).all({p:phone, since, limit})
  return rows.reverse().map(r=>{
    const role = r.direction === "in" ? "user" : (r.direction === "out" ? "assistant" : "system")
    return { role, text: r.message, ts: r.ts }
  })
}
function hadAssistantOutputLastHours(phone, hours=HISTORY_HOURS){
  const since = nowEU().subtract(hours,"hour").toISOString()
  const row = db.prepare(`SELECT 1 FROM logs WHERE phone=@p AND direction='out' AND ts>=@since LIMIT 1`).get({p:phone, since})
  return !!row
}

// ===== IA
async function aiChatRaw(messages, {temperature=0.4, max_tokens=700}={}){
  if (AI_PROVIDER==="none") return null
  const controller = new AbortController()
  const timeout = setTimeout(()=>controller.abort(), AI_TIMEOUT_MS)
  try{
    const url = AI_PROVIDER==="deepseek" ? "https://api.deepseek.com/chat/completions" : "https://api.openai.com/v1/chat/completions"
    const headers = { "Content-Type":"application/json", "Authorization":`Bearer ${AI_PROVIDER==="deepseek"?DEEPSEEK_API_KEY:OPENAI_API_KEY}` }
    const body = JSON.stringify({ model: AI_PROVIDER==="deepseek"?DEEPSEEK_MODEL:OPENAI_MODEL, temperature, max_tokens, messages })
    const resp = await fetch(url,{ method:"POST", headers, body, signal: controller.signal })
    clearTimeout(timeout)
    if (!resp.ok) return null
    const data = await resp.json()
    return data?.choices?.[0]?.message?.content || null
  }catch{ clearTimeout(timeout); return null }
}
function stripToJSON(text){
  if (!text) return null
  let s = String(text).trim().replace(/```json/gi,"```")
  if (s.startsWith("```")) s = s.slice(3)
  if (s.endsWith("```")) s = s.slice(0,-3)
  const i = s.indexOf("{"), j = s.lastIndexOf("}")
  if (i>=0 && j>i) s = s.slice(i, j+1)
  try{ return JSON.parse(s) }catch{ return null }
}
function buildOrchestratorSystemPrompt(){
  const now = nowEU().format("YYYY-MM-DD HH:mm")
  return `Eres el orquestador de WhatsApp de *Gapink Nails*. Lee el histórico (últimas 6h) y NO repitas lo ya preguntado.
- Detecta idioma y escribe con tono cercano, claro, natural (WhatsApp). Breve.
- Objetivo: "salon" ("torremolinos"|"la_luz"), "service_text" (libre), "staff_any"(bool) o "staff_name"(string), "day_text"(string), "part_of_day"("mañana"|"tarde"|"noche").
- Si el cliente dice "me da igual / cualquiera / lo que sea" sobre el DÍA → {"day_text":"cualquiera"}.
- Si también "le da igual" la FRANJA → {"part_of_day":"tarde"} por defecto.
- NO repitas la misma pregunta exacta. Si ves bloqueo, aplica esos defaults y continúa.
- No muestres listas. Personaliza con lo ya dicho.
- Al inicio si en 6h no hablaste, empieza con un saludo breve.
- Si ya están todos los datos, responde de cierre breve y pon "finalize": true.

Devuelve SOLO JSON:
{
  "language": "es" | "en" | "...",
  "intent": "appointment" | "greeting" | "other",
  "updates": {
    "salon": "torremolinos"|"la_luz"|null,
    "service_text": string|null,
    "staff_any": true|false|null,
    "staff_name": string|null,
    "day_text": string|null,
    "part_of_day": "mañana"|"tarde"|"noche"|null
  },
  "reply": "texto a enviar",
  "finalize": true|false
}

Fecha (Madrid): ${now}`
}
function formatHistoryForModel(history){
  const last = history.slice(-HISTORY_MAX_MSGS)
  return last.map(h=>{
    const role = h.role === "assistant" ? "assistant" : h.role === "user" ? "user" : "system"
    return { role, content: `${h.text}` }
  })
}
async function aiOrchestrate({session, history, userText}){
  const sys = buildOrchestratorSystemPrompt()
  const ctx = {
    known: {
      salon: session.salon || null,
      service_text: session.service_text || null,
      staff_any: session.staff_any ?? null,
      staff_name: session.staff_name || null,
      day_text: session.day_text || null,
      part_of_day: session.part_of_day || null
    },
    should_greet: !hadAssistantOutputLastHours(session.phone, HISTORY_HOURS)
  }
  const messages = [
    { role: "system", content: sys },
    { role: "system", content: `Contexto del servidor: ${JSON.stringify(ctx)}` },
    ...formatHistoryForModel(history),
    { role: "user", content: userText }
  ]
  const out = await aiChatRaw(messages, { temperature: 0.45, max_tokens: 800 })
  return { json: stripToJSON(out), raw: out }
}

// ===== Mini web + LOGS viewer
const app = express()
let lastQR = null, conectado = false
app.get("/", (_req,res)=>{
  const count = db.prepare(`SELECT COUNT(*) as c FROM intakes`).get()?.c || 0
  res.send(`<!doctype html><meta charset="utf-8"><style>
  body{font-family:system-ui;display:grid;place-items:center;min-height:100vh;background:#f6f7f9}
  .card{max-width:940px;padding:28px;border-radius:18px;background:#fff;box-shadow:0 10px 32px rgba(0,0,0,.08)}
  .row{display:flex;gap:12px;align-items:center;flex-wrap:wrap}
  .pill{padding:6px 10px;border-radius:999px;background:#eef1f4;font-size:13px}
  .ok{background:#d9f7e8;color:#0f5132}.bad{background:#fde2e1;color:#842029}.warn{background:#fff3cd;color:#664d03}
  .mt{margin-top:12px}
  table{border-collapse:collapse;width:100%;font-size:13px}
  th,td{border:1px solid #e9ecef;padding:6px 8px;text-align:left;vertical-align:top}
  .foot{margin-top:16px;font-size:12px;opacity:.7}
  </style>
  <div class="card">
    <h1>🩷 Gapink Nails Bot v35.2.0</h1>
    <div class="row">
      <div class="pill ${conectado ? "ok":"bad"}">WhatsApp: ${conectado?"Conectado ✅":"Desconectado ❌"}</div>
      <div class="pill warn">IA total · Historial ${HISTORY_HOURS}h · Snooze ${SNOOZE_HOURS}h</div>
      <div class="pill">IA: ${AI_PROVIDER.toUpperCase()}</div>
      <div class="pill">Intakes: ${count}</div>
    </div>
    ${!conectado && lastQR ? `<div class="mt"><img src="/qr.png" width="300" style="border-radius:8px"/></div>`:""}
    <p class="mt" style="opacity:.75">Endpoints: <code>/logs</code> · <code>/logs.json?phone=+34...</code> · <code>/logs.csv?phone=...</code> · <code>/intakes.json</code> · <code>/session.json?phone=...</code> · <code>/config.json</code></p>
    <div class="foot">Desarrollado por <strong>Gonzalo García Aranda</strong></div>
  </div>`)
})
app.get("/qr.png", async (_req,res)=>{
  if(!lastQR) return res.status(404).send("No QR")
  const png = await qrcode.toBuffer(lastQR, { type:"png", width:512, margin:1 })
  res.set("Content-Type","image/png").send(png)
})

// Logs API/visor
app.get("/config.json", (req,res)=>{
  res.json({
    SNOOZE_ENABLED, SNOOZE_ON_DOT, SNOOZE_AFTER_FINALIZE, AUTO_UNSNOOZE_ON_USER,
    SNOOZE_HOURS, HISTORY_HOURS, HISTORY_MAX_MSGS, MAX_SAME_REPLY, AI_PROVIDER
  })
})
app.get("/logs.json", (req,res)=>{
  const phone = req.query.phone || null
  const limit = Number(req.query.limit || 500)
  const rows = phone
    ? db.prepare(`SELECT id,phone,direction,action,message,extra,ts FROM logs WHERE phone=@p ORDER BY id DESC LIMIT @limit`).all({p:phone, limit})
    : db.prepare(`SELECT id,phone,direction,action,message,extra,ts FROM logs ORDER BY id DESC LIMIT @limit`).all({limit})
  res.json(rows.map(r=>({ ...r, extra: r.extra ? JSON.parse(r.extra) : null })))
})
app.get("/logs.csv", (req,res)=>{
  const phone = req.query.phone || null
  const limit = Number(req.query.limit || 1000)
  const rows = phone
    ? db.prepare(`SELECT id,phone,direction,action,message,extra,ts FROM logs WHERE phone=@p ORDER BY id DESC LIMIT @limit`).all({p:phone, limit})
    : db.prepare(`SELECT id,phone,direction,action,message,extra,ts FROM logs ORDER BY id DESC LIMIT @limit`).all({limit})
  const esc = v => `"${String(v??"").replaceAll('"','""')}"`
  const csv = ["id,phone,direction,action,message,extra,ts"].concat(rows.map(r=>
    [r.id,r.phone,r.direction,r.action,r.message, r.extra, r.ts].map(esc).join(",")
  )).join("\n")
  res.set("Content-Type","text/csv").send(csv)
})
app.get("/logs", (req,res)=>{
  const phone = req.query.phone || null
  const limit = Number(req.query.limit || 200)
  const rows = phone
    ? db.prepare(`SELECT id,phone,direction,action,message,extra,ts FROM logs WHERE phone=@p ORDER BY id DESC LIMIT @limit`).all({p:phone, limit})
    : db.prepare(`SELECT id,phone,direction,action,message,extra,ts FROM logs ORDER BY id DESC LIMIT @limit`).all({limit})
  res.send(`<!doctype html><meta charset="utf-8"><style>
    body{font-family:ui-sans-serif;max-width:1200px;margin:24px auto;padding:0 16px}
    table{border-collapse:collapse;width:100%;font-size:13px}
    th,td{border:1px solid #e9ecef;padding:6px 8px;text-align:left;vertical-align:top}
    .mono{font-family:ui-monospace,monospace;white-space:pre-wrap}
    </style>
    <h1>Logs (${rows.length})</h1>
    <table><thead><tr><th>ID</th><th>Phone</th><th>Dir</th><th>Action</th><th>Message</th><th>Extra (JSON)</th><th>ts</th></tr></thead>
    <tbody>
      ${rows.map(r=>`<tr>
        <td>${r.id}</td><td>${r.phone}</td><td>${r.direction}</td>
        <td>${r.action}</td><td class="mono">${(r.message||"").replaceAll("<","&lt;")}</td>
        <td class="mono">${(r.extra||"").replaceAll("<","&lt;")}</td>
        <td>${r.ts}</td>
      </tr>`).join("")}
    </tbody></table>`)
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
      try{ await sock.sendMessage(jid, { text }) }catch(e){}
      logEvent({phone, direction:"out", action:"send", message:text})
    }

    const NON_ANSWER_ANY = /\b(me da igual|cualquiera|como veas|lo que sea|me la pela|da igual|lo mismo|x|no se|no sé)\b/i

    sock.ev.on("messages.upsert", async ({messages})=>{
      const m = messages?.[0]; if (!m?.message) return
      const jid = m.key.remoteJid
      const isFromMe = !!m.key.fromMe
      const phone = normalizePhoneES((jid||"").split("@")[0]||"") || (jid||"").split("@")[0]
      const textRaw = (m.message.conversation || m.message.extendedTextMessage?.text || m.message?.imageMessage?.caption || "").trim()
      if (!textRaw) return

      const prev = QUEUE.get(phone)||Promise.resolve()
      const job = prev.then(async ()=>{
        const sessionBefore = loadSession(phone) || {
          phone,
          salon: null,
          service_text: null,
          staff_any: null,
          staff_name: null,
          day_text: null,
          part_of_day: null,
          snooze_until_ms: null,
          last_bot_reply: null,
          same_reply_count: 0
        }

        try{
          // Silencio manual por "." del negocio
          if (isFromMe && textRaw.trim()==="." && SNOOZE_ON_DOT){
            if (SNOOZE_ENABLED){
              sessionBefore.snooze_until_ms = nowEU().add(SNOOZE_HOURS,"hour").valueOf()
              saveSession(phone, sessionBefore)
            }
            logEvent({phone, direction:"sys", action:"manual_silence", message:textRaw, extra:{by:"business"}})
            return
          }

          // Entrada del cliente
          logEvent({phone, direction:"in", action:"message", message:textRaw})

          // Si está en snooze y AUTO_UNSNOOZE_ON_USER, despertamos
          if (!isFromMe && sessionBefore.snooze_until_ms && nowEU().valueOf() < sessionBefore.snooze_until_ms){
            if (AUTO_UNSNOOZE_ON_USER){
              sessionBefore.snooze_until_ms = null
              saveSession(phone, sessionBefore)
              logEvent({phone, direction:"sys", action:"auto_unsnooze_on_user", message:textRaw})
            } else if (SNOOZE_ENABLED){
              logEvent({phone, direction:"sys", action:"snoozed_drop", message:textRaw})
              return
            }
          }

          // Normaliza “me da igual”
          if (!sessionBefore.day_text && NON_ANSWER_ANY.test(textRaw)) sessionBefore.day_text = "cualquiera"
          if (!sessionBefore.part_of_day && NON_ANSWER_ANY.test(textRaw)) sessionBefore.part_of_day = sessionBefore.part_of_day || null

          // Historial 6h
          const history = getHistory(phone, HISTORY_HOURS, HISTORY_MAX_MSGS)

          // IA
          const aiRes = await aiOrchestrate({ session: sessionBefore, history, userText: textRaw })
          logEvent({phone, direction:"sys", action:"ai_raw", message: aiRes.raw || "(null)", extra:{ parsed: aiRes.json }})

          const ai = aiRes.json || {}
          const up = ai.updates || {}

          // Aplicar updates
          const s = { ...sessionBefore }
          if (up.salon && (up.salon==="torremolinos" || up.salon==="la_luz")) s.salon = up.salon
          if (typeof up.service_text === "string" && up.service_text.trim()) s.service_text = up.service_text.trim()
          if (typeof up.staff_any === "boolean") s.staff_any = up.staff_any
          if (typeof up.staff_name === "string" && up.staff_name.trim()){ s.staff_name = titleCase(up.staff_name.trim()); s.staff_any = false }
          if (typeof up.day_text === "string" && up.day_text.trim()) s.day_text = up.day_text.trim()
          if (up.part_of_day && ["mañana","tarde","noche"].includes(up.part_of_day)) s.part_of_day = up.part_of_day

          // Antibucle
          const replyText = (typeof ai.reply === "string" && ai.reply.trim()) ? ai.reply.trim() : null
          if (replyText && s.last_bot_reply && replyText === s.last_bot_reply){
            s.same_reply_count = (s.same_reply_count||0) + 1
          } else {
            s.same_reply_count = 0
          }

          // Compleción / defaults si bucle
          let haveAll = !!(s.salon && s.service_text && (s.staff_any===true || (s.staff_name && s.staff_name.length>0)) && s.day_text && s.part_of_day)
          let finalize = !!ai.finalize || haveAll
          if (!finalize && s.same_reply_count >= MAX_SAME_REPLY){
            if (!s.day_text) s.day_text = "cualquiera"
            if (!s.part_of_day) s.part_of_day = "tarde"
            if (s.staff_any==null && !s.staff_name) s.staff_any = true
            haveAll = !!(s.salon && s.service_text && (s.staff_any===true || (s.staff_name && s.staff_name.length>0)) && s.day_text && s.part_of_day)
            finalize = haveAll
          }

          // Guardar sesión (after)
          saveSession(phone, s)

          // Finalizar intake
          if (finalize){
            const intakeId = `int_${createHash("sha256").update(`${phone}|${Date.now()}`).digest("hex").slice(0,16)}`
            db.prepare(`INSERT INTO intakes
              (id, phone, salon, service_text, staff_any, staff_name, day_text, part_of_day, created_at, raw_last_msg)
            VALUES
              (@id,@phone,@salon,@service_text,@staff_any,@staff_name,@day_text,@part,@created,@raw)`).run({
              id: intakeId,
              phone,
              salon: s.salon,
              service_text: s.service_text,
              staff_any: s.staff_any?1:0,
              staff_name: s.staff_name || null,
              day_text: s.day_text || null,
              part: s.part_of_day || null,
              created: new Date().toISOString(),
              raw: textRaw
            })
            logEvent({phone, direction:"sys", action:"intake_saved", message:intakeId, extra:{ session_before: sessionBefore, session_after: s }})

            // Opcional: silencio tras finalizar
            if (SNOOZE_ENABLED && SNOOZE_AFTER_FINALIZE){
              s.snooze_until_ms = nowEU().add(SNOOZE_HOURS,"hour").valueOf()
              saveSession(phone, s)
              logEvent({phone, direction:"sys", action:"snooze_after_finalize", message:`${SNOOZE_HOURS}h`})
            }

            // Cerramos con un mensajito (seguimos cumpliendo “todos los mensajes con IA”)
            const closeMsg = replyText || "¡Listo! Tengo todo apuntado. Si quieres cambiar algo, dime 😉"
            await sendText(jid, phone, closeMsg)
            return
          }

          // Responder (romper bucle si hace falta)
          if (replyText){
            let toSend = replyText
            if (s.same_reply_count >= 1){
              if (!s.day_text && /día/i.test(replyText)){
                toSend = "Si te da igual el día, lo dejo en *cualquiera*. ¿Prefieres *mañana* o *tarde*?"
              } else if (!s.part_of_day && !/(mañana|tarde|noche)/i.test(replyText)) {
                toSend = "¿Prefieres *mañana* o *tarde*? Si te da igual, pongo *tarde*."
              }
            }
            await sendText(jid, phone, toSend)
            s.last_bot_reply = toSend
            saveSession(phone, s)
          } else {
            const fb = "¿Te viene mejor *Torremolinos* o *La Luz*? Y dime el *servicio* que quieres 🙌"
            await sendText(jid, phone, fb)
            s.last_bot_reply = fb
            saveSession(phone, s)
          }

        }catch(err){
          logEvent({phone, direction:"sys", action:"handler_error", message: err?.message, extra:{stack: err?.stack, session_before: sessionBefore}})
          try{ await sock.sendMessage(jid, { text: "Se me cruzó un cable 🤯. ¿Salón (*Torremolinos* o *La Luz*) y qué servicio quieres?" }) }catch{}
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
console.log(`🩷 Gapink Nails Bot v35.2.0 · IA total + logs completos · Historial ${HISTORY_HOURS}h · Snooze ${SNOOZE_HOURS}h (enabled=${SNOOZE_ENABLED}, afterFinalize=${SNOOZE_AFTER_FINALIZE}, autoUnsnooze=${AUTO_UNSNOOZE_ON_USER}) · IA:${AI_PROVIDER.toUpperCase()}`)
const server = app.listen(PORT, ()=>{ startBot().catch(console.error) })
process.on("uncaughtException", e=>{ console.error("uncaughtException:", e?.stack||e?.message||e) })
process.on("unhandledRejection", e=>{ console.error("unhandledRejection:", e) })
process.on("SIGTERM", ()=>{ try{ server.close(()=>process.exit(0)) }catch{ process.exit(0) } })
process.on("SIGINT",  ()=>{ try{ server.close(()=>process.exit(0)) }catch{ process.exit(0) } })
