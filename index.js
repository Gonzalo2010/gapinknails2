// index.js — Gapink Nails · v41.2.0
// “Saludo 24h + ‘salón’ (nunca ‘sucursal’) + 1 sola pregunta + link SOLO en saludo + . silencia 6h (de cualquiera) + anti-repeticiones”
//
// Qué hace:
// - TODO por IA: detecta saludo, intención de cita y extrae lo ya dicho para no pedirlo dos veces.
// - Saludo cercano SOLO 1 vez cada 24h, con el enlace de reservas.
// - 1 sola pregunta a la vez. Anti-repetición: no repite la misma frase ni “Hola” fuera del saludo.
// - Cierra sin pedir confirmación: si ya hay svc+salon+staff_any/staff+day+part → mensaje de cierre cercano y silencio 6h.
// - El mensaje "." silencia 6h aunque lo envíe cualquiera (tú o el cliente).
// - Logs en SQLite y endpoint /logs.json
//
// ENV (opcionales):
//   PORT, BOT_DEBUG,
//   GREET_WINDOW_HOURS=24, SNOOZE_HOURS=6, AUTO_SNOOZE_AFTER_COMPLETE_HOURS=6,
//   HISTORY_HOURS=6, HISTORY_MAX_MSGS=40, HISTORY_TRUNC_EACH=180,
//   MAX_SAME_REPLY=2, ASK_COOLDOWN_MS=70000,
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

const GREET_WINDOW_HOURS = Number(process.env.GREET_WINDOW_HOURS || 24)
const SNOOZE_HOURS = Number(process.env.SNOOZE_HOURS || 6)
const AUTO_SNOOZE_AFTER_COMPLETE_HOURS = Number(process.env.AUTO_SNOOZE_AFTER_COMPLETE_HOURS || 6)

const HISTORY_HOURS = Number(process.env.HISTORY_HOURS || 6)
const HISTORY_MAX_MSGS = Number(process.env.HISTORY_MAX_MSGS || 40)
const HISTORY_TRUNC_EACH = Number(process.env.HISTORY_TRUNC_EACH || 180)

const MAX_SAME_REPLY = Number(process.env.MAX_SAME_REPLY || 2) // veces exacta antes de variar
const ASK_COOLDOWN_MS = Number(process.env.ASK_COOLDOWN_MS || 70000) // no re-preguntar mismo campo tan seguido

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
const onlyDigits = s => String(s||"").replace(/\D+/g,"")
function normalizePhoneE164(raw){
  const d=onlyDigits(raw); if(!d) return null
  if (raw.startsWith("+") && d.length>=8 && d.length<=15) return `+${d}`
  if (d.startsWith("34") && d.length===11) return `+${d}`
  if (d.length===9) return `+34${d}`
  if (d.startsWith("00")) return `+${d.slice(2)}`
  return `+${d}`
}
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

// ===== DB
const db = new Database("gapink_ai_v412.db"); db.pragma("journal_mode = WAL")
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
    // saludo & silencio
    last_greet_at_ms: null,
    snooze_until_ms: null,
    // anti-rep
    last_bot_reply: null,
    same_reply_count: 0,
    last_ask_field: null,
    last_ask_at_ms: 0,
    // guardar último “resumen”
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

// ===== Sanitizadores de respuesta
function sanitizeReplyLink(reply, isGreeting){
  const r = String(reply||"")
  return isGreeting ? r : r.replaceAll(BOOKING_URL,"").replace(/\s{2,}/g," ").trim()
}
function stripLeadingGreeting(reply, allowGreeting){
  if (allowGreeting) return reply
  // quita “hola/holi/buenas/hello/hi/hey” al principio si la IA lo coló por error
  const s = String(reply||"").trim()
  return s.replace(/^(hola+|holi+|buenas+|hello+|hi+|hey+)[,!.\s]+/i, "").trim()
}
function isSameReply(a,b){ return String(a||"").trim() === String(b||"").trim() }

// ===== IA: clasificador
async function aiClassify({brand, bookingURL, historyCompact, userText}){
  if (AI_PROVIDER==="none") return null
  const controller = new AbortController()
  const timeout = setTimeout(()=>controller.abort(), AI_TIMEOUT_MS)

  const sys =
`You are the WhatsApp assistant for a BEAUTY SALON called "${brand}" (never say "sucursal"; always say "salón").
Your ONLY job: detect greeting, appointment intent, and extract already-mentioned info so we NEVER ask twice.
Return STRICT JSON ONLY.

Schema:
{
  "lang": "es|en|fr|...",
  "is_greeting": true|false,
  "wants_appointment": true|false,
  "extracted": { "svc":string|null, "salon":"torremolinos"|"la_luz"|null, "staff_any":true|false|null, "staff":string|null, "day":string|null, "part":"mañana"|"tarde"|"noche"|null },
  "missing": ["svc"|"salon"|"staff_or_any"|"day"|"part", ...],
  "reply_hint": "friendly cue to ask the FIRST missing item (<=140 chars, 1 question, no menus, no greeting, no external links)"
}

Rules:
- Consider ONLY the last 6 hours of compact history (server provides).
- If the latest user message contradicts earlier info, prefer the LATEST user message.
- “me da igual/cualquiera” → staff_any=true.
- DO NOT include ${bookingURL} in reply_hint.
- No “hola/buenas/hello” in reply_hint unless explicitly told shouldGreet=true by the server.`

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
    <h1>🩷 ${BRAND} — IA v41.2.0</h1>
    <div class="row">
      <span class="pill ${conectado?"ok":"bad"}">WhatsApp: ${conectado?"Conectado ✅":"Desconectado ❌"}</span>
      <span class="pill">Saludo cada ${GREET_WINDOW_HOURS}h · "." silencia ${SNOOZE_HOURS}h (de cualquiera)</span>
      <span class="pill">Historial IA ${HISTORY_HOURS}h · Anti-rep activado</span>
      <span class="pill">IA:${AI_PROVIDER.toUpperCase()} · tokens=${AI_MAX_TOKENS}</span>
    </div>
    ${!conectado && lastQR ? `<div class="mt"><img src="/qr.png" width="280" style="border-radius:10px"/></div>`:""}
    <p class="mt">Reserva online (solo en saludo): <a target="_blank" href="${BOOKING_URL}">${BOOKING_URL}</a></p>
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
      const phone = normalizePhoneE164((jid||"").split("@")[0]||"") || (jid||"").split("@")[0]
      const isFromMe = !!m.key.fromMe
      const textRaw = (m.message.conversation || m.message.extendedTextMessage?.text || m.message?.imageMessage?.caption || "").trim()
      if (!textRaw) return

      const prev = QUEUE.get(phone)||Promise.resolve()
      const job = prev.then(async ()=>{
        let s = loadSession(phone)

        try{
          // 0) "." silencia 6h — por cualquiera (tú, clienta, etc.)
          if (textRaw.trim()==="."){
            s.snooze_until_ms = nowEU().add(SNOOZE_HOURS,"hour").valueOf()
            saveSession(phone, s)
            logEvent({phone, direction:"sys", message:"manual_snooze_activated", extra:{by:isFromMe?"owner":"user", hours:SNOOZE_HOURS}})
            return
          }

          // Silencio activo
          if (s.snooze_until_ms && nowEU().valueOf() < s.snooze_until_ms){
            logEvent({phone, direction:"sys", message:"dropped_due_snooze"})
            return
          }

          // Log entrada
          logEvent({phone, direction:"in", message:textRaw})

          // 1) Historial compacto
          const hist = getHistoryCompact(phone)

          // 2) IA clasifica
          const ai = await aiClassify({
            brand: BRAND,
            bookingURL: BOOKING_URL,
            historyCompact: hist,
            userText: textRaw
          })
          logEvent({phone, direction:"sys", message:"ai_json", extra: ai})

          const lang = ai?.lang || s.lang || "es"
          s.lang = lang

          // 3) Saludo: SOLO si greeting y no saludamos en 24h
          const greetNow = !!ai?.is_greeting && shouldGreetOncePerWindow(s)
          if (greetNow){
            const hello =
              (lang==="en") ? `Hey! I'm the ${BRAND} assistant 💖 You can also book here: ${BOOKING_URL}. How can I help?`
            : (lang==="fr") ? `Coucou ! Je suis l’assistante de ${BRAND} 💖 Tu peux réserver ici : ${BOOKING_URL}. Dis-moi !`
            : /*es*/         `¡Hola! Soy la asistente de ${BRAND} 💖 Puedes reservar aquí también: ${BOOKING_URL}. ¿En qué te ayudo?`
            await sendText(jid, phone, hello)
            s.last_greet_at_ms = nowEU().valueOf()
            saveSession(phone, s)
            // No hacemos return: seguimos si también quiere cita.
          }

          const ex = ai?.extracted || {}
          const missing = Array.isArray(ai?.missing) ? ai.missing : []
          const wants = !!ai?.wants_appointment

          // 4) Si está todo → cierre amigable y silencio 6h
          const gotAll = !!(ex.svc && ex.salon && ((ex.staff_any===true)||ex.staff) && ex.day && ex.part)
          if (wants && gotAll){
            const closeMsg =
              (lang==="en") ? "Amazing ✨ I’ve noted everything; a teammate will review and confirm here. Thanks! 💕"
            : (lang==="fr") ? "Parfait ✨ Je note tout; une collègue vérifiera et confirmera ici. Merci ! 💕"
            : /*es*/         "¡Genial! ✨ Lo dejo todo listo y una compañera lo revisa y te confirma por aquí. ¡Gracias! 💕"
            await sendText(jid, phone, closeMsg)
            s.snooze_until_ms = nowEU().add(AUTO_SNOOZE_AFTER_COMPLETE_HOURS,"hour").valueOf()
            s.last_summary = { when:new Date().toISOString(), extracted:ex }
            saveSession(phone, s)
            return
          }

          // 5) Si falta algo → 1 sola pregunta (anti-rep + sin “Hola”)
          if (wants && missing.length){
            const first = missing[0]
            let reply = (typeof ai?.reply_hint==="string" && ai.reply_hint.trim()) ? ai.reply_hint.trim() : ""

            if (!reply){
              reply =
                (lang==="en") ? (
                  first==="svc"          ? "Tell me what service you want 😊"
                : first==="salon"        ? "Which salon suits you better, Torremolinos or La Luz?"
                : first==="staff_or_any" ? "Any stylist is fine or someone in particular?"
                : first==="day"          ? "What day works for you?"
                :                          "Morning, afternoon or evening?"
                )
              : (lang==="fr") ? (
                  first==="svc"          ? "Quel service tu veux ? 😊"
                : first==="salon"        ? "Quel salon te va mieux, Torremolinos ou La Luz ?"
                : first==="staff_or_any" ? "Peu importe la personne ou quelqu’un en particulier ?"
                : first==="day"          ? "Quel jour te convient ?"
                :                          "Matin, après-midi ou soir ?"
                )
              : /*es*/ (
                  first==="svc"          ? "Cuéntame qué servicio quieres 😊"
                : first==="salon"        ? "¿Qué salón te viene mejor, Torremolinos o La Luz?"
                : first==="staff_or_any" ? "¿Te vale cualquiera del equipo o alguien en concreto?"
                : first==="day"          ? "¿Qué día te viene bien?"
                :                          "¿Prefieres por la mañana, por la tarde o por la noche?"
                )
            }

            // No meter “hola” si no estamos saludando
            reply = stripLeadingGreeting(reply, false)
            // Link solo en saludo
            reply = sanitizeReplyLink(reply, false)

            // Anti-repetición exacta
            if (isSameReply(reply, s.last_bot_reply)){
              s.same_reply_count = (s.same_reply_count||0) + 1
              if (s.same_reply_count >= MAX_SAME_REPLY){
                // Variación suave
                reply += (lang==="en") ? " (just the first thing I need 🫶)"
                       : (lang==="fr") ? " (juste la première info 🫶)"
                       : /*es*/         " (es lo primero que necesito 🫶)"
              }
            } else {
              s.same_reply_count = 0
            }

            // No re-preguntar el mismo campo demasiado seguido
            const nowMs = nowEU().valueOf()
            if (s.last_ask_field === first && (nowMs - (s.last_ask_at_ms||0)) < ASK_COOLDOWN_MS){
              // Si es exactamente igual que lo último, no molestamos más
              if (!isSameReply(reply, s.last_bot_reply)){
                await sendText(jid, phone, reply)
                s.last_bot_reply = reply
              }
              saveSession(phone, s)
              return
            }

            await sendText(jid, phone, reply)
            s.last_bot_reply = reply
            s.last_ask_field = first
            s.last_ask_at_ms = nowMs
            saveSession(phone, s)
            return
          }

          // 6) Si no quiere cita → respuesta mínima cercana (sin hola)
          if (!wants){
            let base =
              (lang==="en") ? "Got it 💬 If you want to book, tell me the service and we’ll sort it out."
            : (lang==="fr") ? "Bien reçu 💬 Si tu veux réserver, dis-moi le service et on s’en occupe."
            : /*es*/         "¡Te leo! 💬 Si quieres reservar, dime el servicio y lo gestionamos."
            base = stripLeadingGreeting(base, false)
            await sendText(jid, phone, base)
            s.last_bot_reply = base
            saveSession(phone, s)
            return
          }

          // 7) Fallback defensivo: pregunta por salón (sin “hola”)
          let fb =
            (lang==="en") ? "Which salon works for you, Torremolinos or La Luz?"
          : (lang==="fr") ? "Quel salon te convient, Torremolinos ou La Luz ?"
          : /*es*/         "¿Qué salón te viene mejor, Torremolinos o La Luz?"
          fb = stripLeadingGreeting(fb, false)
          await sendText(jid, phone, fb)
          s.last_bot_reply = fb
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
