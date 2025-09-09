// index.js — Gapink Nails · v41.0.0 “IA decide + saludo 24h con link + sin confirmación + . = silencio 6h”
//
// Qué hace:
// - TODO pasa por IA (clasificador): saludo, intención de cita, y extracción de datos ya mencionados.
// - El servidor SOLO pregunta por el primer dato que falte. Cero menús, cero repeticiones.
// - Saludo cercano SOLO 1 vez cada 24h y con el enlace a reservas.
// - Si el mensaje lo envías tú con un "." → el bot calla 6h (sin auto-unsnooze).
// - Cuando YA tenemos todos los datos (svc+salon+staff_any/staff+day+part) → mensaje de cierre cercano
//   y se AUTO-SNOOZEA el chat 6h para que lo coja una empleada. Sin "¿Lo confirmo?".
// - Logs completos en SQLite + /logs.json.
//
// ENV opcionales:
//   PORT, BOT_DEBUG,
//   GREET_WINDOW_HOURS=24, SNOOZE_HOURS=6, AUTO_SNOOZE_AFTER_COMPLETE_HOURS=6,
//   HISTORY_HOURS=6, HISTORY_MAX_MSGS=40, HISTORY_TRUNC_EACH=180,
//   AI_PROVIDER=deepseek|openai, DEEPSEEK_API_KEY, DEEPSEEK_MODEL, OPENAI_API_KEY, OPENAI_MODEL,
//   AI_TIMEOUT_MS=10000, AI_TEMPERATURE=0.15, AI_MAX_TOKENS=160

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

const GREET_WINDOW_HOURS = Number(process.env.GREET_WINDOW_HOURS || 24) // saludo 1×/24h
const SNOOZE_HOURS = Number(process.env.SNOOZE_HOURS || 6)              // "." = silencio 6h
const AUTO_SNOOZE_AFTER_COMPLETE_HOURS = Number(process.env.AUTO_SNOOZE_AFTER_COMPLETE_HOURS || 6)

const HISTORY_HOURS = Number(process.env.HISTORY_HOURS || 6)
const HISTORY_MAX_MSGS = Number(process.env.HISTORY_MAX_MSGS || 40)
const HISTORY_TRUNC_EACH = Number(process.env.HISTORY_TRUNC_EACH || 180)

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
  }catch{ try{ return String(v) }catch{ return "[Unserializable]" } }
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

// ===== DB
const db = new Database("gapink_ai_v410.db"); db.pragma("journal_mode = WAL")
db.exec(`
CREATE TABLE IF NOT EXISTS logs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  phone TEXT,
  direction TEXT,  -- in|out|sys
  message TEXT,
  extra TEXT,
  ts TEXT
);
CREATE TABLE IF NOT EXISTS sessions (
  phone TEXT PRIMARY KEY,
  data_json TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
`)
function logEvent({phone, direction, message, extra}){
  try{
    db.prepare(`INSERT INTO logs (phone,direction,message,extra,ts) VALUES (@p,@d,@m,@e,@t)`).run({
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
  return row ? JSON.parse(row.data_json) : {
    phone, lang:"es",
    last_greet_at_ms: null,
    snooze_until_ms: null,
    last_summary: null
  }
}
function saveSession(phone, s){
  const j = JSON.stringify(s||{})
  const up = db.prepare(`UPDATE sessions SET data_json=@j, updated_at=@u WHERE phone=@p`).run({j, u:new Date().toISOString(), p:phone})
  if (up.changes===0){
    db.prepare(`INSERT INTO sessions (phone,data_json,updated_at) VALUES (@p,@j,@u)`).run({p:phone, j, u:new Date().toISOString()})
  }
}

// ===== IA: clasificador compacto
async function aiClassify({brand, bookingURL, historyCompact, userText}){
  if (AI_PROVIDER==="none") return null
  const controller = new AbortController()
  const timeout = setTimeout(()=>controller.abort(), AI_TIMEOUT_MS)

  const sys =
`You are the WhatsApp assistant for a BEAUTY SALON called "${brand}".
Your ONLY job: detect greeting, appointment intent, and extract already-mentioned info so we never ask twice.
Return STRICT JSON, nothing else.

Schema:
{
  "lang": "es|en|fr|...",
  "is_greeting": true|false,
  "wants_appointment": true|false,
  "extracted": {
    "svc": string|null,
    "salon": "torremolinos"|"la_luz"|null,
    "staff_any": true|false|null,
    "staff": string|null,
    "day": string|null,
    "part": "mañana"|"tarde"|"noche"|null
  },
  "missing": ["svc"|"salon"|"staff_or_any"|"day"|"part", ...],
  "reply_hint": "friendly cue to ask the FIRST missing item (<=140 chars, 1 question, no menus)"
}

Rules:
- Consider ONLY the last 6 hours of compact history.
- If the user already said it earlier, include it in "extracted".
- Words like “cualquiera/me da igual” → staff_any=true.
- Do NOT include ${bookingURL} in reply_hint.
- Keep JSON tight, no prose.`

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
    let s = String(data?.choices?.[0]?.message?.content || "").trim().replace(/```json/gi,"```")
    if (s.startsWith("```")) s = s.slice(3)
    if (s.endsWith("```")) s = s.slice(0,-3)
    const i = s.indexOf("{"), j = s.lastIndexOf("}")
    if (i>=0 && j>i) s = s.slice(i, j+1)
    return JSON.parse(s)
  }catch{
    clearTimeout(timeout); return null
  }
}

// ===== Mini web (estado/QR/logs)
const app = express()
let lastQR = null, conectado = false
app.get("/", (_req,res)=>{
  res.send(`<!doctype html><meta charset="utf-8"><style>
  body{font-family:system-ui;display:grid;place-items:center;min-height:100vh;background:#f6f7f9}
  .card{max-width:900px;padding:28px;border-radius:18px;background:#fff;box-shadow:0 10px 30px rgba(0,0,0,.08)}
  .row{display:flex;gap:10px;flex-wrap:wrap}
  .pill{padding:6px 10px;border-radius:999px;background:#eef1f4;font-size:13px}
  .ok{background:#d9f7e8;color:#0f5132}.bad{background:#fde2e1;color:#842029}
  .mt{margin-top:12px}
  a{color:#1f6feb;text-decoration:none}
  .foot{margin-top:8px;opacity:.7;font-size:12px}
  </style>
  <div class="card">
    <h1>🩷 ${BRAND} — IA v41.0.0</h1>
    <div class="row">
      <span class="pill ${conectado?"ok":"bad"}">WhatsApp: ${conectado?"Conectado ✅":"Desconectado ❌"}</span>
      <span class="pill">Saludo cada ${GREET_WINDOW_HOURS}h · Silencio "." ${SNOOZE_HOURS}h</span>
      <span class="pill">Historial IA ${HISTORY_HOURS}h · IA:${AI_PROVIDER.toUpperCase()} · tokens=${AI_MAX_TOKENS}</span>
    </div>
    ${!conectado && lastQR ? `<div class="mt"><img src="/qr.png" width="280" style="border-radius:10px"/></div>`:""}
    <p class="mt">Reserva online: <a target="_blank" href="${BOOKING_URL}">${BOOKING_URL}</a></p>
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
    ? db.prepare(`SELECT id,phone,direction,message,extra,ts FROM logs WHERE phone=@p ORDER BY id DESC LIMIT @limit`).all({p:phone, limit})
    : db.prepare(`SELECT id,phone,direction,message,extra,ts FROM logs ORDER BY id DESC LIMIT @limit`).all({limit})
  res.json(rows.map(r=>({ ...r, extra: r.extra? JSON.parse(r.extra): null })))
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

    function shouldGreetOncePerWindow(session){
      if (!session.last_greet_at_ms) return true
      const diffH = (nowEU().valueOf() - session.last_greet_at_ms)/(1000*60*60)
      return diffH >= GREET_WINDOW_HOURS
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
        let s = loadSession(phone)

        try{
          // 0) "." desde tu lado → silencio 6h
          if (isFromMe && textRaw.trim()==="."){
            s.snooze_until_ms = nowEU().add(SNOOZE_HOURS,"hour").valueOf()
            saveSession(phone, s)
            logEvent({phone, direction:"sys", message:"manual_snooze_activated", extra:{hours:SNOOZE_HOURS}})
            return
          }

          // Silencio activo (no auto-desactivar)
          if (!isFromMe && s.snooze_until_ms && nowEU().valueOf() < s.snooze_until_ms){
            logEvent({phone, direction:"sys", message:"dropped_due_snooze"})
            return
          }

          // Log entrada
          logEvent({phone, direction:"in", message:textRaw})

          // 1) Historial compacto
          const hist = getHistoryCompact(phone)

          // 2) Clasificación IA
          const ai = await aiClassify({
            brand: BRAND,
            bookingURL: BOOKING_URL,
            historyCompact: hist,
            userText: textRaw
          })
          logEvent({phone, direction:"sys", message:"ai_json", extra: ai})

          const lang = ai?.lang || s.lang || "es"
          s.lang = lang

          // 3) Saludo (si aplica) — 1× cada 24h, con link
          const greetNow = !!ai?.is_greeting && shouldGreetOncePerWindow(s)
          if (greetNow){
            const hello =
              (lang==="en") ? `Hey! I'm the ${BRAND} assistant 💖 You can book here too: ${BOOKING_URL} — how can I help?`
            : (lang==="fr") ? `Coucou ! Je suis l’assistante de ${BRAND} 💖 Tu peux réserver ici aussi : ${BOOKING_URL} — dis-moi !`
            : /*es*/         `¡Hola! Soy la asistente de ${BRAND} 💖 Puedes reservar aquí también: ${BOOKING_URL}. ¿En qué te ayudo?`
            await sendText(jid, phone, hello)
            s.last_greet_at_ms = nowEU().valueOf()
            saveSession(phone, s)
            // NO return: si además quiere cita, seguimos con la pregunta (1 sola).
          }

          // 4) Si quiere cita, preguntar SOLO lo que falte
          const ex = ai?.extracted || {}
          const missing = Array.isArray(ai?.missing) ? ai.missing : []

          // Si está todo → cierre cercano + auto-silencio 6h
          const gotAll = !!(ex.svc && ex.salon && ((ex.staff_any===true)||ex.staff) && ex.day && ex.part)
          if (ai?.wants_appointment && gotAll){
            const closeMsg =
              (lang==="en") ? "Amazing ✨ I’ve noted everything and a teammate will review and confirm here. Thanks! 💕"
            : (lang==="fr") ? "Parfait ✨ Je note tout et une collègue vérifiera et confirmera ici. Merci ! 💕"
            : /*es*/         "¡Genial! ✨ Lo dejo todo listo y una compañera lo revisa y te confirma por aquí. ¡Gracias! 💕"
            await sendText(jid, phone, closeMsg)
            // Auto-silencio para no marear al cliente
            s.snooze_until_ms = nowEU().add(AUTO_SNOOZE_AFTER_COMPLETE_HOURS,"hour").valueOf()
            s.last_summary = { when: new Date().toISOString(), extracted: ex }
            saveSession(phone, s)
            return
          }

          // Si falta algo y hay intención de cita → 1 sola pregunta
          if (ai?.wants_appointment && missing.length){
            const first = missing[0]
            const hint = (typeof ai?.reply_hint==="string" && ai.reply_hint.trim()) ? ai.reply_hint.trim() : null
            // Pregunta cercana por campo faltante
            const ask =
              hint ? hint :
              (lang==="en") ? (
                first==="svc"          ? "Tell me what service you want 😊"
              : first==="salon"        ? "Which salon suits you better, Torremolinos or La Luz?"
              : first==="staff_or_any" ? "Any stylist is fine or someone in particular?"
              : first==="day"          ? "What day works for you?"
              :                          "Morning, afternoon or evening?"
              ) :
              (lang==="fr") ? (
                first==="svc"          ? "Quel service tu veux ? 😊"
              : first==="salon"        ? "Quel salon te va mieux, Torremolinos ou La Luz ?"
              : first==="staff_or_any" ? "Peu importe la personne ou quelqu’un en particulier ?"
              : first==="day"          ? "Quel jour te convient ?"
              :                          "Matin, après-midi ou soir ?"
              ) :
              /*es*/ (
                first==="svc"          ? "Cuéntame qué servicio quieres 😊"
              : first==="salon"        ? "¿Qué salón te viene mejor, Torremolinos o La Luz?"
              : first==="staff_or_any" ? "¿Te vale cualquiera del equipo o alguien en concreto?"
              : first==="day"          ? "¿Qué día te viene bien?"
              :                          "¿Prefieres por la mañana, por la tarde o por la noche?"
              )
            await sendText(jid, phone, ask)
            saveSession(phone, s)
            return
          }

          // Si no es saludo ni quiere cita → respuesta mínima cercana
          if (!ai?.wants_appointment){
            const base =
              (lang==="en") ? "Got it 💬 If you want to book, tell me the service and we’ll sort it out."
            : (lang==="fr") ? "Bien reçu 💬 Si tu veux réserver, dis-moi le service et on s’en occupe."
            : /*es*/         "¡Te leo! 💬 Si quieres reservar, dime el servicio y lo gestionamos."
            await sendText(jid, phone, base)
            saveSession(phone, s)
            return
          }

          // Fallback ultra defensivo
          await sendText(jid, phone, (lang==="es") ? "¿Te viene mejor Torremolinos o La Luz?" :
                                         (lang==="en") ? "Which salon works for you, Torremolinos or La Luz?" :
                                                         "Quel salon te convient, Torremolinos ou La Luz ?")
          saveSession(phone, s)

        }catch(err){
          logEvent({phone, direction:"sys", message:"handler_error", extra:{msg:err?.message, stack:err?.stack}})
          try{ await sendText(jid, phone, "Ups 😅 ¿Quieres reservar una cita?") }catch{}
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

