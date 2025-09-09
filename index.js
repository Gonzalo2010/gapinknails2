// index.js — Gapink Nails · v41.0.0
// “IA decide saludo/cita/info · 1 sola pregunta + mute 6h (auto y por '.')”
//
// Novedades v41:
// - Silencio AUTO 6h cuando ya tenemos: servicio, salón, preferencia de día y franja (u hora), y preferencia de profesional (alguien en concreto o cualquiera).
// - Silencio MANUAL 6h por mensaje con un "." (punto) enviado por el cliente o por vosotros desde el mismo chat.
// - El saludo incluye el enlace de reserva: https://gapinknails.square.site/
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
  const t = text.trim()
  // Acepta solo un carácter punto típico
  return t === "."
}

// ===== DB
const db = new Database("gapink_ai_classifier_v410.db"); db.pragma("journal_mode = WAL")
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
`)
function logEvent({phone, direction, message, extra}){
  try{
    db.prepare(\`INSERT INTO logs (phone,direction,message,extra,ts) VALUES (@p,@d,@m,@e,@t)\`).run({
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
    return \`\${tag}:\${truncate(r.message||"", HISTORY_TRUNC_EACH)}\`
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
  try{
    return dayjs(session.mute_until).isAfter(nowEU())
  }catch{ return false }
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
    "part": "mañana"|"tarde"|"noche"|null
  },
  "missing": ["svc"|"salon"|"staff_or_any"|"day"|"part", ...],
  "reply_hint": "brief natural cue to ask for the FIRST missing item (<=140 chars, 1 question, no menus)"
}

Rules:
- Consider the LAST 6 hours of chat (provided as compact history).
- If the user already mentioned something before (e.g., 'cejas', 'Torremolinos', 'me da igual quien'), mark it in extracted to avoid re-asking.
- A message like "quiero cita" or "appointment" → wants_appointment=true.
- Greetings like "hola/hello/hi/buenas" → is_greeting=true (only if the current message is indeed a greeting tone).
- Never include ${bookingURL} in reply_hint. That's for the UI, not classification.
- DO NOT add extra fields. Keep JSON tight.`

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
    // Intentamos parseo robusto
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

// ===== Mini web (estado y QR)
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
    <h1>🩷 ${BRAND} — IA Clasificador v41.0.0</h1>
    <div class="row">
      <span class="pill ${conectado?"ok":"bad"}">WhatsApp: ${conectado?"Conectado ✅":"Desconectado ❌"}</span>
      <span class="pill">Historial IA ${HISTORY_HOURS}h · máx ${HISTORY_MAX_MSGS} msgs</span>
      <span class="pill">IA: ${AI_PROVIDER.toUpperCase()} · tokens=${AI_MAX_TOKENS}</span>
      <span class="pill">Mute default: ${MUTE_HOURS}h</span>
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
          // Cargamos sesión SIEMPRE (para poder mutear aunque sea "fromMe")
          let session = loadSession(phone)

          // 0) Silencio manual por '.' (cliente o staff). No respondemos nada.
          if (isJustDot(textRaw)){
            session = setMute(session, MUTE_HOURS, isFromMe ? "manual-dot-staff" : "manual-dot-user")
            saveSession(phone, session)
            logEvent({phone, direction: isFromMe?"in":"in", message:textRaw, extra:{fromMe:isFromMe, action:"mute", until:session.mute_until}})
            return
          }

          // Log de entrada (incluye fromMe, pero si es fromMe y no es '.', no hacemos nada más)
          logEvent({phone, direction:"in", message:textRaw, extra:{fromMe:isFromMe}})

          // Si es un mensaje enviado por nosotros (desde el número del negocio) y no es '.', no activar bot
          if (isFromMe) return

          // 1) Si la conversación está en silencio, no respondemos
          if (isMuted(session)){
            logEvent({phone, direction:"sys", message:"muted_skip", extra:{until:session.mute_until, reason:session.mute_reason}})
            return
          }

          // 2) Compacta historial (últimas 6h) para la IA
          const hist = getHistoryCompact(phone)

          // 3) IA: SOLO clasificación + extracción
          const ai = await aiClassify({
            brand: BRAND,
            bookingURL: BOOKING_URL,
            historyCompact: hist,
            userText: textRaw
          })
          logEvent({phone, direction:"sys", message:"ai_json", extra: ai})

          // 4) Respuesta mínima basada en IA
          let reply = ""
          const lang = ai?.lang || "es"
          const ex   = ai?.extracted || {}
          const missing = Array.isArray(ai?.missing) ? ai.missing : []

          // Si no hay IA o no clasificó -> una pregunta genérica (con enlace en saludo)
          if (!ai){
            reply = `¡Hola! Soy la asistente de ${BRAND} 💖 Puedes reservar aquí también: ${BOOKING_URL}. ¿Quieres reservar una cita?`
            await sendText(jid, phone, reply); return
          }

          // Saludo sin intención de cita -> saludo corto + enlace
          if (ai.is_greeting && !ai.wants_appointment){
            reply = (lang==="en")
              ? `Hi! I'm ${BRAND}'s assistant 💖 You can also book here: ${BOOKING_URL}. How can I help you?`
              : (lang==="fr")
                ? `Salut ! Je suis l’assistante de ${BRAND} 💖 Tu peux aussi réserver ici : ${BOOKING_URL}. Comment puis-je t’aider ?`
                : `¡Hola! Soy la asistente de ${BRAND} 💖 Puedes reservar aquí también: ${BOOKING_URL}. ¿En qué puedo ayudarte?`
            await sendText(jid, phone, reply); return
          }

          // Quiere cita: preguntamos SOLO lo que falte (primero de la lista)
          if (ai.wants_appointment){
            if (!missing.length){
              // Todo listo → resumen corto + confirmación (1 sola pregunta)
              const salonTxt = ex.salon==="la_luz" ? "La Luz" : (ex.salon==="torremolinos" ? "Torremolinos" : "—")
              const staffTxt = (ex.staff_any===true) ? "cualquiera del equipo" : (ex.staff? ex.staff : "—")
              const resumen = (lang==="en")
                ? `Great! ${ex.svc||"service"} in ${salonTxt}, ${staffTxt}, ${ex.day||"day to agree"} in the ${ex.part||"slot to agree"}. Do I confirm?`
                : (lang==="fr")
                  ? `Top ! ${ex.svc||"service"} à ${salonTxt}, ${staffTxt}, ${ex.day||"jour à convenir"} sur le ${ex.part||"créneau à convenir"}. Je confirme ?`
                  : `Perfecto: ${ex.svc||"servicio"} en ${salonTxt}, ${staffTxt}, ${ex.day||"día a convenir"} por la ${ex.part||"franja a convenir"}. ¿Lo confirmo?`

              await sendText(jid, phone, resumen)

              // ★ Auto-mute 6h tras tener toda la info (deja paso a gestión humana)
              session = setMute(session, MUTE_HOURS, "auto-after-data-complete")
              session.last_summary = resumen
              saveSession(phone, session)
              logEvent({phone, direction:"sys", message:"auto_muted_after_complete", extra:{until:session.mute_until}})

              return
            } else {
              const first = missing[0]
              const hint = (typeof ai.reply_hint==="string" && ai.reply_hint.trim()) ? ai.reply_hint.trim() : null
              const ask = hint || (
                (lang==="en") ? (
                  first==="svc"          ? "What service would you like?"
                : first==="salon"        ? "Which salon works for you, Torremolinos or La Luz?"
                : first==="staff_or_any" ? "Any stylist or someone specific?"
                : first==="day"          ? "What day works for you?"
                :                          "Morning, afternoon or evening?"
                )
                : (lang==="fr") ? (
                  first==="svc"          ? "Quel service veux-tu ?"
                : first==="salon"        ? "Quel salon te convient, Torremolinos ou La Luz ?"
                : first==="staff_or_any" ? "Peu importe la personne ou quelqu’un en particulier ?"
                : first==="day"          ? "Quel jour te convient ?"
                :                          "Matin, après-midi ou soir ?"
                )
                : (
                  first==="svc"          ? "¿Qué servicio te gustaría?"
                : first==="salon"        ? "¿Qué salón prefieres: Torremolinos o La Luz?"
                : first==="staff_or_any" ? "¿Cualquiera del equipo o alguien en concreto?"
                : first==="day"          ? "¿Qué día te viene bien?"
                :                          "¿Mañana, tarde o noche?"
                )
              )
              await sendText(jid, phone, ask); return
            }
          }

          // No saludo, no cita → respuesta mínima neutra (sin molestar)
          reply = (lang==="en") ? "Got it. Tell me if you want to book an appointment."
                : (lang==="fr") ? "Compris. Dis-moi si tu veux réserver."
                : "Entendido. Dime si quieres reservar una cita."
          await sendText(jid, phone, reply)

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
