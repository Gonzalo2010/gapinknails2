// index.js — Gapink Nails · v34.0.0
// Modo: INTAKE SIMPLE (sin Square, sin listas, sin resumen) + Silencio 6h
// Reglas clave:
// - La IA SOLO procesa el primer mensaje: si es "hola" o intención de cita → entramos en acción; si no → silenciamos 6h.
// - Flujo: pedir SALÓN → pedir SERVICIO → (tras cualquier mensaje) pedir PROFESIONAL + DÍA + MAÑANA/TARDE.
// - Tras recibir esa última respuesta, guardamos "intake" en SQLite y silenciamos el bot 6h.
// - Si tú (el negocio) envías un "." en el chat → silenciamos 6h ese número.
// - Prompt DeepSeek optimizado: JSON estricto, intent + confidence (para justificar métricas).

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

// ===== Flags / Config
const BOT_DEBUG = /^true$/i.test(process.env.BOT_DEBUG || "")
const SNOOZE_HOURS = Number(process.env.SNOOZE_HOURS || 6)
const AI_TIMEOUT_MS = Number(process.env.AI_TIMEOUT_MS || 15000)
const AI_PROVIDER = (process.env.AI_PROVIDER || (process.env.DEEPSEEK_API_KEY? "deepseek" : process.env.OPENAI_API_KEY? "openai" : "none")).toLowerCase()
const DEEPSEEK_API_KEY = process.env.DEEPSEEK_API_KEY || ""
const DEEPSEEK_MODEL   = process.env.DEEPSEEK_MODEL   || "deepseek-chat"
const OPENAI_API_KEY   = process.env.OPENAI_API_KEY   || ""
const OPENAI_MODEL     = process.env.OPENAI_MODEL     || "gpt-4o-mini"

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

// ===== Baileys loader
async function loadBaileys(){
  const require = createRequire(import.meta.url); let mod=null
  try{ mod=require("@whiskeysockets/baileys") }catch{}; if(!mod){ mod=await import("@whiskeysockets/baileys") }
  if(!mod) throw new Error("Baileys incompatible")
  const makeWASocket = mod.makeWASocket || mod.default?.makeWASocket || (typeof mod.default==="function"?mod.default:undefined)
  const useMultiFileAuthState = mod.useMultiFileAuthState || mod.default?.useMultiFileAuthState
  const fetchLatestBaileysVersion = mod.fetchLatestBaileysVersion || mod.default?.fetchLatestBaileysVersion || (async()=>({version:[2,3000,0]}))
  const Browsers = mod.Browsers || mod.default?.Browsers || { macOS:(n="Desktop")=>["MacOS",n,"121.0.0"] }
  return { makeWASocket, useMultiFileAuthState, fetchLatestBaileysVersion, Browsers }
}

// ===== DB
const db = new Database("gapink_simplified.db"); db.pragma("journal_mode = WAL")
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
const insertIntake = db.prepare(`INSERT INTO intakes
(id, phone, salon, service_text, staff_any, staff_name, day_text, part_of_day, created_at, raw_last_msg)
VALUES (@id,@phone,@salon,@service_text,@staff_any,@staff_name,@day_text,@part_of_day,@created_at,@raw_last_msg)`)

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
  const line = { phone, direction, action, message, extra, ts: new Date().toISOString() }
  try{ console.log(JSON.stringify(line)) }catch{}
}

// ===== Sesiones
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

// ===== IA (DeepSeek/OpenAI) — SOLO PARA CLASIFICAR el PRIMER mensaje y (opcional) extraer detalles finales
async function aiChat(system, user){
  if (AI_PROVIDER==="none") return null
  const controller = new AbortController()
  const timeout = setTimeout(()=>controller.abort(), AI_TIMEOUT_MS)
  try{
    const url = AI_PROVIDER==="deepseek" ? "https://api.deepseek.com/chat/completions" : "https://api.openai.com/v1/chat/completions"
    const headers = {
      "Content-Type":"application/json",
      "Authorization":`Bearer ${AI_PROVIDER==="deepseek"?DEEPSEEK_API_KEY:OPENAI_API_KEY}`
    }
    const body = JSON.stringify({
      model: AI_PROVIDER==="deepseek" ? DEEPSEEK_MODEL : OPENAI_MODEL,
      temperature: 0.2,
      max_tokens: 400,
      messages: [
        { role: "system", content: system },
        { role: "user",   content: user }
      ]
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

// Prompt 1: Clasificación del primer mensaje (boost métricas DeepSeek con confianza, razones, señales)
function buildFirstMsgPrompt(){
  const now = nowEU().format("YYYY-MM-DD HH:mm")
  return `Eres un clasificador de intención para WhatsApp de un salón de belleza.
Devuelves SOLO JSON válido.

Fecha actual (Madrid): ${now}

Objetivo: CLASIFICAR el PRIMER mensaje del cliente en una de estas intenciones:
- "appointment" → quiere coger cita (o pregunta claramente para reservar).
- "greeting" → saludos tipo "hola", "buenas", "hey".
- "other" → cualquier otra cosa (promos, quejas, spam, preguntas no relacionadas con reserva).

Incluye también:
- "confidence": número entre 0 y 1.
- "signals": lista corta de frases o tokens del mensaje que te hacen decidir (máx 5).
- "explanation": una frase breve.

Formato JSON ESTRICTO:
{
  "intent": "appointment" | "greeting" | "other",
  "confidence": 0.0-1.0,
  "signals": [ "..." ],
  "explanation": "..."
}

NO expliques nada fuera del JSON.`
}

// Prompt 2: Extracción de detalles (profesional + día + franja)
function buildDetailsPrompt(){
  return `Eres un extractor de datos. Devuelves SOLO JSON con lo pedido.
Tarea: A partir del texto del cliente, extrae:
- "staff_any": true si dice que le da igual/cualquiera/equipo; false si menciona alguien concreto; null si no se sabe.
- "staff_name": nombre textual si pide a alguien (string) o null si no aplica.
- "day_text": el día o rango como lo dice el cliente (ej. "viernes", "mañana", "12/10", "la semana que viene") o null.
- "part_of_day": "mañana" | "tarde" | "noche" si se deduce; si dice "por la mañana" → "mañana". Si no se sabe → null.

Formato:
{"staff_any": true|false|null, "staff_name": string|null, "day_text": string|null, "part_of_day": "mañana"|"tarde"|"noche"|null}`
}

async function aiClassifyFirstMessage(text){
  const sys = buildFirstMsgPrompt()
  const out = await aiChat(sys, `Mensaje: """${text}"""`)
  return stripToJSON(out)
}
async function aiExtractDetails(text){
  const sys = buildDetailsPrompt()
  const out = await aiChat(sys, `Texto del cliente: """${text}"""`)
  return stripToJSON(out)
}

// ===== Parsers simples (backups)
function parseSede(text){
  const t = norm(text)
  if (/\b(luz|la luz)\b/.test(t)) return "la_luz"
  if (/\b(torremolinos|torre)\b/.test(t)) return "torremolinos"
  return null
}
function parsePartOfDay(text){
  const t = norm(text)
  if (/\bmanana\b/.test(t)) return "mañana"
  if (/\btarde\b/.test(t))  return "tarde"
  if (/\bnoche\b/.test(t))  return "noche"
  if (/\bpor la man/.test(t)) return "mañana"
  return null
}
function detectStaffAny(text){
  const t = norm(text)
  return /\b(me da igual|cualquiera|con quien sea|equipo)\b/.test(t)
}
function extractStaffName(text){
  const m = /(?:^|\s)con\s+([a-zñáéíóúüï ]{2,})$/i.exec(text?.trim()||"")
  if (m) return titleCase(m[1].trim())
  return null
}

// ===== Web mini status
const app = express()
const PORT = process.env.PORT || 8080
let lastQR = null, conectado = false

app.get("/", (_req,res)=>{
  const count = db.prepare(`SELECT COUNT(*) as c FROM intakes`).get()?.c || 0
  res.send(`<!doctype html><meta charset="utf-8"><style>
  body{font-family:system-ui;display:grid;place-items:center;min-height:100vh;background:#f6f7f9}
  .card{max-width:820px;padding:28px;border-radius:18px;background:#fff;box-shadow:0 10px 32px rgba(0,0,0,.08)}
  .row{display:flex;gap:12px;align-items:center}
  .pill{padding:6px 10px;border-radius:999px;background:#eef1f4;font-size:13px}
  .ok{background:#d9f7e8;color:#0f5132}.bad{background:#fde2e1;color:#842029}.warn{background:#fff3cd;color:#664d03}
  .mt{margin-top:12px}
  </style>
  <div class="card">
    <h1>🩷 Gapink Nails Bot v34.0.0</h1>
    <div class="row">
      <div class="pill ${conectado ? "ok":"bad"}">WhatsApp: ${conectado?"Conectado ✅":"Desconectado ❌"}</div>
      <div class="pill warn">Modo: Intake simple · Silencio ${SNOOZE_HOURS}h</div>
      <div class="pill">IA: ${AI_PROVIDER.toUpperCase()}</div>
      <div class="pill">Intakes totales: ${count}</div>
    </div>
    ${!conectado && lastQR ? `<div class="mt"><img src="/qr.png" width="300" style="border-radius:8px"/></div>`:""}
    <p class="mt" style="opacity:.75">Flujo: primer mensaje con IA (cita/hola ⇒ activo; otro ⇒ silencio 6h). Sin listas ni resumen. “.” desde negocio ⇒ silencio 6h.</p>
  </div>`)
})
app.get("/qr.png", async (_req,res)=>{
  if(!lastQR) return res.status(404).send("No QR")
  const png = await qrcode.toBuffer(lastQR, { type:"png", width:512, margin:1 })
  res.set("Content-Type","image/png").send(png)
})

// ===== WhatsApp loop
async function startBot(){
  try{
    const { makeWASocket, useMultiFileAuthState, fetchLatestBaileysVersion, Browsers } = await loadBaileys()
    if(!fs.existsSync("auth_info")) fs.mkdirSync("auth_info",{recursive:true})
    const { state, saveCreds } = await useMultiFileAuthState("auth_info")
    const { version } = await fetchLatestBaileysVersion().catch(()=>({version:[2,3000,0]}))
    const sock = makeWASocket({ logger:pino({level:"silent"}), printQRInTerminal:false, auth:state, version, browser:Browsers.macOS("Desktop"), syncFullHistory:false })
    globalThis.sock = sock

    sock.ev.on("connection.update", ({connection,qr})=>{
      if (qr){ lastQR=qr; conectado=false; try{ qrcodeTerminal.generate(qr,{small:true}) }catch{} }
      if (connection==="open"){ lastQR=null; conectado=true }
      if (connection==="close"){ conectado=false; setTimeout(()=>{ startBot().catch(console.error) }, 3000) }
    })
    sock.ev.on("creds.update", saveCreds)

    // Strict per-phone queue
    if (!globalThis.__q) globalThis.__q = new Map()
    const QUEUE = globalThis.__q

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
            firstClassified: false,
            stage: null,              // null | awaiting_salon | awaiting_service | awaiting_misc | awaiting_staff_date
            salon: null,              // "torremolinos" | "la_luz"
            service_text: null,
            staff_any: null,
            staff_name: null,
            day_text: null,
            part_of_day: null,
            snooze_until_ms: null,
            last_msg_at_ms: null
          }

          // Si TÚ (negocio) envías ".", silencio 6h a ese cliente
          if (isFromMe && textRaw.trim()==="."){
            s.snooze_until_ms = nowEU().add(SNOOZE_HOURS, "hour").valueOf()
            saveSession(phone, s)
            logEvent({phone, direction:"sys", action:"manual_silence", message:"dot 6h", extra:{by:"business"}})
            return
          }

          // SNOOZE activo → no hablamos
          const now = nowEU()
          if (s.snooze_until_ms && now.valueOf() < s.snooze_until_ms){
            saveSession(phone, s)
            return
          }

          // Primer mensaje: clasificar con IA (appointment/greeting/other)
          if (!s.firstClassified){
            const ai = await aiClassifyFirstMessage(textRaw)
            logEvent({phone, direction:"in", action:"first_msg", message:textRaw, extra:{ai}})
            if (ai?.intent==="appointment" || ai?.intent==="greeting"){
              s.firstClassified = true
              s.stage = "awaiting_salon"
              s.last_msg_at_ms = Date.now()
              saveSession(phone, s)
              await globalThis.sock.sendMessage(jid, { text: "¿En qué *salón* te viene mejor: *Torremolinos* o *La Luz*?" })
              logEvent({phone, direction:"out", action:"ask_salon"})
              return
            } else {
              // Silenciar 6h si no es cita/hola
              s.snooze_until_ms = now.add(SNOOZE_HOURS,"hour").valueOf()
              saveSession(phone, s)
              logEvent({phone, direction:"sys", action:"auto_silence_6h", message:textRaw, extra:{ai}})
              return
            }
          }

          // A partir de aquí, solo manejamos el flujo simple
          logEvent({phone, direction:"in", action:"message", message:textRaw, extra:{stage:s.stage}})

          // Paso 1: salón
          if (s.stage === "awaiting_salon"){
            const sede = parseSede(textRaw)
            if (!sede){
              await globalThis.sock.sendMessage(jid, { text: "Dime el *salón*: *Torremolinos* o *La Luz*." })
              logEvent({phone, direction:"out", action:"reprompt_salon"})
              return
            }
            s.salon = sede
            s.stage = "awaiting_service"
            s.last_msg_at_ms = Date.now()
            saveSession(phone, s)
            await globalThis.sock.sendMessage(jid, { text: "Genial. ¿Qué *servicio* quieres?" })
            logEvent({phone, direction:"out", action:"ask_service", extra:{salon:sede}})
            return
          }

          // Paso 2: servicio (texto libre, no listas)
          if (s.stage === "awaiting_service"){
            s.service_text = textRaw
            s.stage = "awaiting_misc"
            s.last_msg_at_ms = Date.now()
            saveSession(phone, s)
            // En este paso NO preguntamos nada más (puede escribir lo que quiera)
            return
          }

          // Paso 3 (implícito): tras cualquier mensaje, lanzamos la pregunta final combinada
          if (s.stage === "awaiting_misc"){
            s.stage = "awaiting_staff_date"
            s.last_msg_at_ms = Date.now()
            saveSession(phone, s)
            await globalThis.sock.sendMessage(jid, { text: "¿Quieres *con alguien en particular* o te da igual? ¿Qué *día* te viene bien y *por la mañana* o *por la tarde*?" })
            logEvent({phone, direction:"out", action:"ask_staff_date"})
            return
          }

          // Paso 4: respuesta final → extraemos y guardamos; luego silencio 6h
          if (s.stage === "awaiting_staff_date"){
            // IA para detalles (sube métrica 🥁)
            const ai = await aiExtractDetails(textRaw)
            // Backups por si AI falla
            let staff_any = ai?.staff_any
            let staff_name = ai?.staff_name
            let day_text   = ai?.day_text
            let part       = ai?.part_of_day

            if (staff_any == null) staff_any = detectStaffAny(textRaw)
            if (!staff_name && !staff_any) staff_name = extractStaffName(textRaw)
            if (!part) part = parsePartOfDay(textRaw)
            if (!day_text) day_text = textRaw // guardamos tal cual si no hay extracción fina

            s.staff_any = !!staff_any
            s.staff_name = staff_name ? titleCase(staff_name) : null
            s.day_text = day_text
            s.part_of_day = part
            s.stage = null
            s.last_msg_at_ms = Date.now()
            saveSession(phone, s)

            const intakeId = `int_${createHash("sha256").update(`${phone}|${Date.now()}`).digest("hex").slice(0,16)}`
            insertIntake.run({
              id: intakeId,
              phone,
              salon: s.salon,
              service_text: s.service_text,
              staff_any: s.staff_any?1:0,
              staff_name: s.staff_name || null,
              day_text: s.day_text || null,
              part_of_day: s.part_of_day || null,
              created_at: new Date().toISOString(),
              raw_last_msg: textRaw
            })
            logEvent({phone, direction:"sys", action:"intake_saved", message:intakeId, extra:{salon:s.salon, service:s.service_text, staff_any:s.staff_any, staff_name:s.staff_name, day:s.day_text, part:s.part_of_day, ai}})

            // Silenciar 6h para no molestar
            s.snooze_until_ms = nowEU().add(SNOOZE_HOURS,"hour").valueOf()
            saveSession(phone, s)
            // NO respondemos nada más (silencio pedido)
            return
          }

          // Fallback de seguridad: si algo raro, volvemos a pedir salón
          if (!s.stage){
            s.stage = "awaiting_salon"
            saveSession(phone, s)
            await globalThis.sock.sendMessage(jid, { text: "¿En qué *salón* te viene mejor: *Torremolinos* o *La Luz*?" })
            logEvent({phone, direction:"out", action:"fallback_ask_salon"})
            return
          }
        }catch(err){
          if (BOT_DEBUG) console.error(err)
          logEvent({phone, direction:"sys", action:"handler_error", message: err?.message, extra:{stack: err?.stack}})
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
console.log(`🩷 Gapink Nails Bot v34.0.0 · Intake simple · Silencio ${SNOOZE_HOURS}h · IA:${AI_PROVIDER.toUpperCase()}`)
const server = app.listen(PORT, ()=>{ startBot().catch(console.error) })
process.on("uncaughtException", e=>{ console.error("uncaughtException:", e?.stack||e?.message||e) })
process.on("unhandledRejection", e=>{ console.error("unhandledRejection:", e) })
process.on("SIGTERM", ()=>{ try{ server.close(()=>process.exit(0)) }catch{ process.exit(0) } })
process.on("SIGINT",  ()=>{ try{ server.close(()=>process.exit(0)) }catch{ process.exit(0) } })
