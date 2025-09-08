// index.js — Gapink Nails · v35.0.0
// Modo: ORQUESTACIÓN TOTAL CON IA (DeepSeek/OpenAI)
// - La IA ve el historial de las últimas 6 h (usuario y bot) y rellena huecos sin pedir de nuevo.
// - Todos los mensajes al cliente los escribe la IA (tono cercano, idioma detectado).
// - Saludo automático al arrancar conversación (si no hemos escrito nada en 6 h).
// - Intake final (salón + servicio + staff_any/staff_name + día + parte) → guardado en SQLite + silencio 6 h.
// - “.” enviado por el negocio en ese chat → silencio 6 h.
// - Sin listas; preguntas directas y personalizadas según contexto.

// Requisitos de entorno: DEEPSEEK_API_KEY o OPENAI_API_KEY
// npm i express pino qrcode qrcode-terminal better-sqlite3 dayjs @whiskeysockets/baileys

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
const nowEU   = () => dayjs().tz(EURO_TZ)

// ===== Config
const PORT = process.env.PORT || 8080
const BOT_DEBUG = /^true$/i.test(process.env.BOT_DEBUG || "")
const SNOOZE_HOURS = Number(process.env.SNOOZE_HOURS || 6)
const HISTORY_HOURS = Number(process.env.HISTORY_HOURS || 6)
const HISTORY_MAX_MSGS = Number(process.env.HISTORY_MAX_MSGS || 60) // límite seguridad

// ===== IA
const AI_PROVIDER = (process.env.AI_PROVIDER || (process.env.DEEPSEEK_API_KEY? "deepseek" : process.env.OPENAI_API_KEY? "openai" : "none")).toLowerCase()
const DEEPSEEK_API_KEY = process.env.DEEPSEEK_API_KEY || ""
const DEEPSEEK_MODEL   = process.env.DEEPSEEK_MODEL   || "deepseek-chat"
const OPENAI_API_KEY   = process.env.OPENAI_API_KEY   || ""
const OPENAI_MODEL     = process.env.OPENAI_MODEL     || "gpt-4o-mini"
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
const norm = s => String(s||"").normalize("NFD").replace(/\p{Diacritic}/gu,"").toLowerCase().trim()
const titleCase = str => String(str||"").toLowerCase().replace(/\b([a-z])/g, m=>m.toUpperCase())

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
  if (!row) return null
  return JSON.parse(row.data_json)
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
    const line = { phone, direction, action, message, extra, ts: new Date().toISOString() }
    try{ console.log(JSON.stringify(line)) }catch{}
  }
}
function getHistory(phone, hours = HISTORY_HOURS, limit = HISTORY_MAX_MSGS){
  const since = nowEU().subtract(hours, "hour").toISOString()
  const rows = db.prepare(`SELECT direction, message, ts FROM logs
                           WHERE phone=@p AND ts>=@since AND message IS NOT NULL
                           ORDER BY id DESC LIMIT @limit`).all({p:phone, since, limit})
  // Devuelve cronológico ascendente
  return rows.reverse().map(r=>{
    const role = r.direction === "in" ? "user" : (r.direction === "out" ? "assistant" : "system")
    return { role, text: r.message, ts: r.ts }
  })
}
function hadAssistantOutputLastHours(phone, hours = HISTORY_HOURS){
  const since = nowEU().subtract(hours,"hour").toISOString()
  const row = db.prepare(`SELECT 1 FROM logs WHERE phone=@p AND direction='out' AND ts>=@since LIMIT 1`).get({p:phone, since})
  return !!row
}

// ===== IA
async function aiChatRaw(messages, {temperature=0.35, max_tokens=600}={}){
  if (AI_PROVIDER==="none") return null
  const controller = new AbortController()
  const timeout = setTimeout(()=>controller.abort(), AI_TIMEOUT_MS)
  try{
    const url = AI_PROVIDER==="deepseek" ? "https://api.deepseek.com/chat/completions" : "https://api.openai.com/v1/chat/completions"
    const headers = {
      "Content-Type":"application/json",
      "Authorization":`Bearer ${AI_PROVIDER==="deepseek" ? DEEPSEEK_API_KEY : OPENAI_API_KEY}`
    }
    const body = JSON.stringify({
      model: AI_PROVIDER==="deepseek" ? DEEPSEEK_MODEL : OPENAI_MODEL,
      temperature, max_tokens, messages
    })
    const resp = await fetch(url, { method:"POST", headers, body, signal: controller.signal })
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
  return `Eres el orquestador de WhatsApp de *Gapink Nails*. Tu trabajo:
- Leer TODA la conversación de las últimas 6 horas (usuario y bot) y **no repetir** lo ya preguntado.
- Detectar el idioma (ES por defecto). Mantén un tono **cercano, educado y natural** (estilo WhatsApp, sin emojis raros).
- Objetivo: obtener *salón* (Torremolinos o La Luz), *servicio* (texto libre como "cejas", "uñas semipermanente", etc.), preferencia de *profesional* (nombre o "equipo"), *día* y *franja* ("mañana"/"tarde"/"noche").
- **Nunca muestres listas largas.** Pregunta directo y personalizado.
- Si en el historial ya aparecen datos (p.ej., "cejas"), **no los pidas**.
- Al inicio de conversación (si en 6 h el bot no ha hablado), **saluda** (breve).
- Si ya están todos los datos (salón + servicio + [staff_any o staff_name] + día + parte), marca "finalize": true. El servidor guardará el intake y **no enviará más mensajes**.
- Mantén mensajes cortos, claros y con una sola pregunta a la vez cuando falten datos. Si falta más de uno, prioriza en este orden: salón → servicio → staff → día → parte.

Devuelve SOLO JSON con este formato:
{
  "language": "es" | "en" | "fr" | "...",
  "intent": "appointment" | "greeting" | "other",
  "updates": {
    "salon": "torremolinos" | "la_luz" | null,
    "service_text": string|null,
    "staff_any": true|false|null,
    "staff_name": string|null,
    "day_text": string|null,
    "part_of_day": "mañana"|"tarde"|"noche"|null
  },
  "reply": "texto del mensaje a enviar al cliente",
  "finalize": true|false
}

Fecha actual (Madrid): ${now}
Reglas extra:
- "salon": valores canónicos "torremolinos" o "la_luz".
- Si el usuario dice "me da igual/cualquiera/equipo", usa {"staff_any": true, "staff_name": null}.
- No inventes datos. Si dudas, deja el campo en null y formula una pregunta breve en "reply".
- Mantén el idioma elegido de forma consistente en "reply".
- No uses listas; personaliza con lo ya dicho.`
}

function formatHistoryForModel(history){
  // Recortamos a HISTORY_MAX_MSGS por cola
  const last = history.slice(-HISTORY_MAX_MSGS)
  // Convertimos a mensajes ChatML
  return last.map(h=>{
    const role = h.role === "assistant" ? "assistant"
               : h.role === "user" ? "user" : "system"
    const content = `${h.text}`
    return { role, content }
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
  const out = await aiChatRaw(messages, { temperature: 0.45, max_tokens: 700 })
  return stripToJSON(out)
}

// ===== Mini web
const app = express()
let lastQR = null, conectado = false
app.get("/", (_req,res)=>{
  const count = db.prepare(`SELECT COUNT(*) as c FROM intakes`).get()?.c || 0
  res.send(`<!doctype html><meta charset="utf-8"><style>
  body{font-family:system-ui;display:grid;place-items:center;min-height:100vh;background:#f6f7f9}
  .card{max-width:860px;padding:28px;border-radius:18px;background:#fff;box-shadow:0 10px 32px rgba(0,0,0,.08)}
  .row{display:flex;gap:12px;align-items:center;flex-wrap:wrap}
  .pill{padding:6px 10px;border-radius:999px;background:#eef1f4;font-size:13px}
  .ok{background:#d9f7e8;color:#0f5132}.bad{background:#fde2e1;color:#842029}.warn{background:#fff3cd;color:#664d03}
  .mt{margin-top:12px}
  .foot{margin-top:16px;font-size:12px;opacity:.7}
  </style>
  <div class="card">
    <h1>🩷 Gapink Nails Bot v35.0.0</h1>
    <div class="row">
      <div class="pill ${conectado ? "ok":"bad"}">WhatsApp: ${conectado?"Conectado ✅":"Desconectado ❌"}</div>
      <div class="pill warn">IA total · Historial ${HISTORY_HOURS}h · Silencio ${SNOOZE_HOURS}h</div>
      <div class="pill">Proveedor IA: ${AI_PROVIDER.toUpperCase()}</div>
      <div class="pill">Intakes: ${count}</div>
    </div>
    ${!conectado && lastQR ? `<div class="mt"><img src="/qr.png" width="300" style="border-radius:8px"/></div>`:""}
    <p class="mt" style="opacity:.75">Todos los mensajes los escribe la IA; detecta idioma, evita repeticiones y saluda al iniciar conversación.</p>
    <div class="foot">Desarrollado por <strong>Gonzalo García Aranda</strong></div>
  </div>`)
})
app.get("/qr.png", async (_req,res)=>{
  if(!lastQR) return res.status(404).send("No QR")
  const png = await qrcode.toBuffer(lastQR, { type:"png", width:512, margin:1 })
  res.set("Content-Type","image/png").send(png)
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

    // Cola por teléfono
    if (!globalThis.__q) globalThis.__q = new Map()
    const QUEUE = globalThis.__q

    // Helper: enviar y loguear
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
      if (!textRaw) return

      const prev = QUEUE.get(phone)||Promise.resolve()
      const job = prev.then(async ()=>{
        try{
          let s = loadSession(phone) || {
            phone,
            salon: null,
            service_text: null,
            staff_any: null,
            staff_name: null,
            day_text: null,
            part_of_day: null,
            snooze_until_ms: null
          }

          // Si TÚ (negocio) envías ".", silencio 6h a ese cliente
          if (isFromMe && textRaw.trim()==="."){
            s.snooze_until_ms = nowEU().add(SNOOZE_HOURS, "hour").valueOf()
            saveSession(phone, s)
            logEvent({phone, direction:"sys", action:"manual_silence_6h", message:"."})
            return
          }

          // Silencio activo
          const now = nowEU()
          if (s.snooze_until_ms && now.valueOf() < s.snooze_until_ms){
            // Logueamos pero no respondemos
            logEvent({phone, direction:"sys", action:"snoozed_drop", message:textRaw})
            return
          }

          // Loguea entrada
          logEvent({phone, direction:"in", action:"message", message:textRaw})

          // Construye historial 6h
          const history = getHistory(phone, HISTORY_HOURS, HISTORY_MAX_MSGS)

          // Orquestación IA
          const ai = await aiOrchestrate({ session: s, history, userText: textRaw })

          if (!ai){
            // Fallback mínimo (intento mantener tono, pero sin IA)
            await sendText(jid, phone, "Ahora mismo estoy procesando mucha info. ¿Me puedes repetir en qué salón y qué servicio quieres? 🙏")
            return
          }

          // Aplica updates
          const up = ai.updates || {}
          if (up.salon && (up.salon==="torremolinos" || up.salon==="la_luz")) s.salon = up.salon
          if (typeof up.service_text === "string" && up.service_text.trim()) s.service_text = up.service_text.trim()
          if (typeof up.staff_any === "boolean") s.staff_any = up.staff_any
          if (typeof up.staff_name === "string" && up.staff_name.trim()){
            s.staff_name = titleCase(up.staff_name.trim()); s.staff_any = false
          }
          if (typeof up.day_text === "string" && up.day_text.trim()) s.day_text = up.day_text.trim()
          if (up.part_of_day && ["mañana","tarde","noche"].includes(up.part_of_day)) s.part_of_day = up.part_of_day

          // ¿Finalizado?
          const haveAll = !!(s.salon && s.service_text && (s.staff_any===true || (s.staff_name && s.staff_name.length>0)) && s.day_text && s.part_of_day)
          const finalize = !!ai.finalize || haveAll

          // Guarda sesión
          saveSession(phone, s)

          if (finalize){
            // Inserta intake y silencia 6h (no respondemos más)
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
            logEvent({phone, direction:"sys", action:"intake_saved", message:intakeId, extra:{s}})
            s.snooze_until_ms = nowEU().add(SNOOZE_HOURS, "hour").valueOf()
            saveSession(phone, s)
            // Silencio: no enviamos respuesta (cumple tu requisito)
            return
          }

          // Responder con IA
          const reply = (typeof ai.reply === "string" && ai.reply.trim()) ? ai.reply.trim() : null
          if (reply){
            await sendText(jid, phone, reply)
          } else {
            // Fallback si el JSON vino sin reply
            await sendText(jid, phone, "¿Te viene mejor *Torremolinos* o *La Luz*? Y dime el *servicio* que quieres 🙌")
          }
        }catch(err){
          if (BOT_DEBUG) console.error(err)
          logEvent({phone, direction:"sys", action:"handler_error", message: err?.message, extra:{stack: err?.stack}})
          try{ await sock.sendMessage(jid, { text: "Se me ha cruzado un cable un segundo 🤯. ¿Me repites salón y servicio, porfa?" }) }catch{}
        }
      })
      QUEUE.set(phone, job.finally(()=>{ if (QUEUE.get(phone)===job) QUEUE.delete(phone) }))
    })
  }catch(e){
    console.error(e)
    setTimeout(()=>{ startBot().catch(console.error) }, 5000)
  }
}

// ===== Arranque
console.log(`🩷 Gapink Nails Bot v35.0.0 · IA total · Historial ${HISTORY_HOURS}h · Silencio ${SNOOZE_HOURS}h · IA:${AI_PROVIDER.toUpperCase()}`)
const server = app.listen(PORT, ()=>{ startBot().catch(console.error) })
process.on("uncaughtException", e=>{ console.error("uncaughtException:", e?.stack||e?.message||e) })
process.on("unhandledRejection", e=>{ console.error("unhandledRejection:", e) })
process.on("SIGTERM", ()=>{ try{ server.close(()=>process.exit(0)) }catch{ process.exit(0) } })
process.on("SIGINT",  ()=>{ try{ server.close(()=>process.exit(0)) }catch{ process.exit(0) } })
