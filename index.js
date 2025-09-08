// index.js — Gapink Nails · v35.1.0
// Modo: ORQUESTACIÓN TOTAL CON IA + ANTIBUCLES y VALORES POR DEFECTO
// - Historial 6h (usuario+bot) → IA no repite.
// - Saludo al inicio de conversación (si 6h sin hablar).
// - Si el cliente dice "me da igual" para el día → day_text="cualquiera".
// - Si también "le da igual" la franja → part_of_day="tarde" (defecto).
// - Antibucle: si la misma pregunta se repite ≥2 veces, el servidor asume por defecto y finaliza.
// - Intake completo (salon + servicio + [staff_any|staff_name] + day + part) ⇒ guardado + silencio 6h.
// - "." enviado por el negocio ⇒ silencio 6h.
// Requisitos: DEEPSEEK_API_KEY o OPENAI_API_KEY

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
const HISTORY_MAX_MSGS = Number(process.env.HISTORY_MAX_MSGS || 60)
const MAX_SAME_REPLY = Number(process.env.MAX_SAME_REPLY || 2) // tras 2 repeticiones: forzamos defaults

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
  direction TEXT,
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
  return `Eres el orquestador de WhatsApp de *Gapink Nails*. Lee el histórico (últimas 6h) y NO repitas lo ya preguntado.
- Detecta el idioma y escribe con tono cercano, claro y natural.
- Objetivo de datos: "salon" ("torremolinos"|"la_luz"), "service_text" (libre), "staff_any" (boolean) o "staff_name", "day_text", "part_of_day" ("mañana"|"tarde"|"noche").
- Si el cliente dice "me da igual / cualquiera / lo que sea" sobre el DÍA → usa {"day_text":"cualquiera"}.
- Si también "le da igual" la FRANJA → usa {"part_of_day":"tarde"} por defecto.
- NO preguntes más de una vez exactamente lo mismo. Si detectas bloqueo, aplica los valores por defecto anteriores y continúa.
- No muestres listas. Pregunta directo y personalizado.
- Cuando tengas todos los datos, responde breve de cierre y devuelve "finalize": true.

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

Fecha (Madrid): ${now}
Si ya saludaste hace poco, no vuelvas a saludar; si no, empieza con un saludo breve.`
}

function formatHistoryForModel(history){
  const last = history.slice(-HISTORY_MAX_MSGS)
  return last.map(h=>{
    const role = h.role === "assistant" ? "assistant"
               : h.role === "user" ? "user" : "system"
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
    <h1>🩷 Gapink Nails Bot v35.1.0</h1>
    <div class="row">
      <div class="pill ${conectado ? "ok":"bad"}">WhatsApp: ${conectado?"Conectado ✅":"Desconectado ❌"}</div>
      <div class="pill warn">IA total · Historial ${HISTORY_HOURS}h · Silencio ${SNOOZE_HOURS}h</div>
      <div class="pill">Proveedor IA: ${AI_PROVIDER.toUpperCase()}</div>
      <div class="pill">Intakes: ${count}</div>
    </div>
    ${!conectado && lastQR ? `<div class="mt"><img src="/qr.png" width="300" style="border-radius:8px"/></div>`:""}
    <p class="mt" style="opacity:.75">Antibucle: si el cliente dice “me da igual”, se asumen valores por defecto y no se martillea la misma pregunta.</p>
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

    if (!globalThis.__q) globalThis.__q = new Map()
    const QUEUE = globalThis.__q

    async function sendText(jid, phone, text){
      if (!text) return
      try{ await sock.sendMessage(jid, { text }) }catch(e){}
      logEvent({phone, direction:"out", action:"send", message:text})
    }

    // Helpers de interpretación de "me da igual"
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
        try{
          let s = loadSession(phone) || {
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

          // Silencio manual (negocio): "."
          if (isFromMe && textRaw.trim()==="."){
            s.snooze_until_ms = nowEU().add(SNOOZE_HOURS, "hour").valueOf()
            saveSession(phone, s)
            logEvent({phone, direction:"sys", action:"manual_silence_6h", message:"."})
            return
          }

          // Silencio activo
          const now = nowEU()
          if (s.snooze_until_ms && now.valueOf() < s.snooze_until_ms){
            logEvent({phone, direction:"sys", action:"snoozed_drop", message:textRaw})
            return
          }

          // Log entrada
          logEvent({phone, direction:"in", action:"message", message:textRaw})

          // Normalizamos “me da igual” para día/franja si faltan
          if (!s.day_text && NON_ANSWER_ANY.test(textRaw)) s.day_text = "cualquiera"
          if (!s.part_of_day && NON_ANSWER_ANY.test(textRaw)) s.part_of_day = s.part_of_day || null // lo decide IA; server pondrá "tarde" si se atasca

          // Historial 6h
          const history = getHistory(phone, HISTORY_HOURS, HISTORY_MAX_MSGS)

          // IA orquestadora
          const ai = await aiOrchestrate({ session: s, history, userText: textRaw })

          if (!ai){
            // Fallback mínimo
            await sendText(jid, phone, "¿Te viene mejor *Torremolinos* o *La Luz*? Y dime el *servicio* que quieres 🙌")
            return
          }

          // Aplicar updates IA
          const up = ai.updates || {}
          if (up.salon && (up.salon==="torremolinos" || up.salon==="la_luz")) s.salon = up.salon
          if (typeof up.service_text === "string" && up.service_text.trim()) s.service_text = up.service_text.trim()
          if (typeof up.staff_any === "boolean") s.staff_any = up.staff_any
          if (typeof up.staff_name === "string" && up.staff_name.trim()){
            s.staff_name = titleCase(up.staff_name.trim()); s.staff_any = false
          }
          if (typeof up.day_text === "string" && up.day_text.trim()) s.day_text = up.day_text.trim()
          if (up.part_of_day && ["mañana","tarde","noche"].includes(up.part_of_day)) s.part_of_day = up.part_of_day

          // Antibucle: si IA repite EXACTAMENTE el mismo reply varias veces
          const replyText = (typeof ai.reply === "string" && ai.reply.trim()) ? ai.reply.trim() : null
          if (replyText && s.last_bot_reply && replyText === s.last_bot_reply){
            s.same_reply_count = (s.same_reply_count||0) + 1
          } else {
            s.same_reply_count = 0
          }

          // ¿Tenemos todo?
          let haveAll = !!(s.salon && s.service_text && (s.staff_any===true || (s.staff_name && s.staff_name.length>0)) && s.day_text && s.part_of_day)
          let finalize = !!ai.finalize || haveAll

          // Si se repite la misma pregunta demasiadas veces, imponemos defaults y finalizamos
          if (!finalize && s.same_reply_count >= MAX_SAME_REPLY){
            if (!s.day_text) s.day_text = "cualquiera"
            if (!s.part_of_day) s.part_of_day = "tarde"
            // Si aún falta staff, y el usuario no especificó, asumimos equipo
            if (s.staff_any==null && !s.staff_name) s.staff_any = true
            haveAll = !!(s.salon && s.service_text && (s.staff_any===true || (s.staff_name && s.staff_name.length>0)) && s.day_text && s.part_of_day)
            finalize = haveAll
          }

          // Guardar sesión
          saveSession(phone, s)

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
            logEvent({phone, direction:"sys", action:"intake_saved", message:intakeId, extra:{s}})
            s.snooze_until_ms = nowEU().add(SNOOZE_HOURS, "hour").valueOf()
            saveSession(phone, s)
            // Silencio post-intake (no respondemos más)
            return
          }

          // Enviar respuesta IA (evita mandar la misma frase en bucle)
          if (replyText){
            // Si lleva repitiéndose, matizamos con pista distinta:
            let toSend = replyText
            if (s.same_reply_count >= 1){
              // Ajuste ligero para romper el bucle visual
              if (!s.day_text && /día/i.test(replyText)) {
                toSend = "Si te viene bien, lo dejamos en *cualquier día*. ¿Te va mejor *mañana* o *tarde*?"
              } else if (!s.part_of_day && /(mañana|tarde|noche)/i.test(replyText)===false) {
                toSend = "¿Prefieres *mañana* o *tarde*? Si te da igual, pongo *tarde* por defecto."
              }
            }
            await sendText(jid, phone, toSend)
            s.last_bot_reply = toSend
            saveSession(phone, s)
          } else {
            await sendText(jid, phone, "¿Te viene mejor *Torremolinos* o *La Luz*? Y dime el *servicio* que quieres 🙌")
            s.last_bot_reply = "¿Te viene mejor *Torremolinos* o *La Luz*? Y dime el *servicio* que quieres 🙌"
            saveSession(phone, s)
          }
        }catch(err){
          if (BOT_DEBUG) console.error(err)
          logEvent({phone, direction:"sys", action:"handler_error", message: err?.message, extra:{stack: err?.stack}})
          try{ await sock.sendMessage(jid, { text: "Se me cruzó un cable un segundo 🤯. Si te da igual el día, pongo *cualquier día*; ¿*mañana* o *tarde*?" }) }catch{}
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
console.log(`🩷 Gapink Nails Bot v35.1.0 · IA total + antibucle · Historial ${HISTORY_HOURS}h · Silencio ${SNOOZE_HOURS}h · IA:${AI_PROVIDER.toUpperCase()}`)
const server = app.listen(PORT, ()=>{ startBot().catch(console.error) })
process.on("uncaughtException", e=>{ console.error("uncaughtException:", e?.stack||e?.message||e) })
process.on("unhandledRejection", e=>{ console.error("unhandledRejection:", e) })
process.on("SIGTERM", ()=>{ try{ server.close(()=>process.exit(0)) }catch{ process.exit(0) } })
process.on("SIGINT",  ()=>{ try{ server.close(()=>process.exit(0)) }catch{ process.exit(0) } })
