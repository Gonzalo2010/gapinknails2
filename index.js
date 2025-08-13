// index.js — Bot WhatsApp Gapink Nails (DeepSeek + FSM + TZ + Confirmación fina)

import express from "express"
import baileys from "@whiskeysockets/baileys"
import pino from "pino"
import qrcode from "qrcode"
import qrcodeTerminal from "qrcode-terminal"
import "dotenv/config"
import fs from "fs"
import { webcrypto } from "crypto"
import Database from "better-sqlite3"
import dayjs from "dayjs"
import utc from "dayjs/plugin/utc.js"
import tz from "dayjs/plugin/timezone.js"
import "dayjs/locale/es.js"
import { Client, Environment } from "square"

if (!globalThis.crypto) globalThis.crypto = webcrypto

dayjs.extend(utc)
dayjs.extend(tz)
dayjs.locale("es")
const EURO_TZ = "Europe/Madrid"

// =========== Baileys
const { makeWASocket, useMultiFileAuthState, fetchLatestBaileysVersion, Browsers } = baileys

// =========== Config negocio
const WORK_DAYS = [1,2,3,4,5,6]  // 1=lun ... 6=sáb. (7=dom cerrado)
const OPEN_HOUR  = 10
const CLOSE_HOUR = 20
const SLOT_MIN   = 30

// Servicios
const SERVICES = { "uñas acrílicas": 90 }

// Map a Square
const SERVICE_VARIATIONS = {
  "uñas acrílicas": process.env.SQ_SV_UNAS_ACRILICAS || ""
}
const TEAM_MEMBER_IDS = (process.env.SQ_TEAM_IDS || "").split(",").map(s=>s.trim()).filter(Boolean)

// =========== DeepSeek
const DEEPSEEK_API_KEY = process.env.DEEPSEEK_API_KEY
const DEEPSEEK_API_URL = process.env.DEEPSEEK_API_URL || "https://api.deepseek.com/chat/completions"
const DEEPSEEK_MODEL   = process.env.DEEPSEEK_MODEL   || "deepseek-chat"

async function dsChat(messages, { temperature = 0.4 } = {}) {
  try {
    const resp = await fetch(DEEPSEEK_API_URL, {
      method: "POST",
      headers: { "Content-Type":"application/json", "Authorization":`Bearer ${DEEPSEEK_API_KEY}` },
      body: JSON.stringify({ model: DEEPSEEK_MODEL, messages, temperature })
    })
    if (!resp.ok) throw new Error(`DeepSeek HTTP ${resp.status} ${await resp.text()}`)
    const data = await resp.json()
    return (data?.choices?.[0]?.message?.content || "").trim()
  } catch (e) { console.error("DeepSeek error:", e?.message || e); return "" }
}

const SYSTEM_PROMPT = `Eres el asistente de WhatsApp de Gapink Nails. Sé natural y resolutivo.
- No hables de “IA”. Mensajes cortos, sin emojis.
- Si el cliente da día/hora, respétalo tal cual si hay hueco. Si no hay, ofrece el más cercano y pide confirmar.
- No ofrezcas elegir profesional. Pago siempre en persona.
- Si falta el contacto en alta nueva, pide SOLO nombre y email.
- Saludos: responde breve y pregunta si quiere pedir cita o ver disponibilidad.`

async function aiReply(userText) {
  return await dsChat([
    { role: "system", content: SYSTEM_PROMPT },
    { role: "user", content: userText || "" }
  ], { temperature: 0.3 })
}

async function classify(text) {
  const out = await dsChat([
    { role: "system", content:
`Devuelve SOLO:
GREETING -> saludo (hola, buenas...)
BOOKING  -> reservas/citas/disponibilidad/cambios/cancelaciones/horarios
OTHER    -> lo demás` },
    { role: "user", content: text || "" }
  ], { temperature: 0 })
  return (out || "OTHER").trim().toUpperCase()
}

// =========== Helpers texto/fecha
const onlyDigits = (s="") => (s||"").replace(/\D+/g,"")
const rmDiacritics = (s="") => s.normalize("NFD").replace(/\p{Diacritic}/gu,"")
const WEEKDAYS_ES = ["domingo","lunes","martes","miércoles","jueves","viernes","sábado"]
const MONTHS = {
  enero:1,febrero:2,marzo:3,abril:4,mayo:5,junio:6,julio:7,agosto:8,septiembre:9,setiembre:9,octubre:10,noviembre:11,diciembre:12,
  ene:1,feb:2,mar:3,abr:4,may:5,jun:6,jul:7,ago:8,sep:9,oct:10,nov:11,dic:12
}
const pad2 = n => String(n).padStart(2,"0")
const fmtES = d => `${WEEKDAYS_ES[d.tz(EURO_TZ).day()]} ${pad2(d.tz(EURO_TZ).date())}/${pad2(d.tz(EURO_TZ).month()+1)} ${pad2(d.tz(EURO_TZ).hour())}:${pad2(d.tz(EURO_TZ).minute())}`

function normalizePhoneES(raw) {
  const digits = onlyDigits(raw)
  if (!digits) return null
  if (raw.startsWith("+") && digits.length >= 8 && digits.length <= 15) return `+${digits}`
  if (digits.startsWith("34") && digits.length === 11) return `+${digits}`
  if (digits.length === 9) return `+34${digits}`
  if (digits.startsWith("00")) return `+${digits.slice(2)}`
  return `+${digits}`
}

function extractContact(text="") {
  const t = text.trim()
  const emailMatch = t.match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i)
  const email = emailMatch ? emailMatch[0] : null
  const nameHints = [/(?:^|\b)(?:me llamo|soy|mi nombre es)\s+([A-Za-zÁÉÍÓÚÜÑáéíóúüñ' -]{2,80})/i]
  let name = null
  for (const re of nameHints) { const m = t.match(re); if (m?.[1]) { name = m[1].trim(); break } }
  if (!name && email) {
    const idx = t.indexOf(email)
    const left = t.slice(0, idx).replace(/[,.;:]/g, " ")
    const ySplit = left.split(/\by\b/i)
    const candidate = (ySplit[0] || left).trim()
    if (candidate && candidate.length >= 2 && !/[?]/.test(candidate)) name = candidate.slice(-80)
  }
  if (name) name = name.replace(/\s+/g," ").replace(/^y\s+/i,"").trim()
  return { name: name || null, email }
}

function detectService(text="") {
  const low = rmDiacritics(text.toLowerCase())
  // Sinónimos y sin tildes
  const map = {
    "unas acrilicas":"uñas acrílicas",
    "unas acrylicas":"uñas acrílicas",
    "uñas acrilicas":"uñas acrílicas",
    "uñas acrílicas":"uñas acrílicas"
  }
  for (const k of Object.keys(map)) if (low.includes(rmDiacritics(k))) return map[k]
  for (const k of Object.keys(SERVICES)) if (low.includes(rmDiacritics(k))) return k
  return null
}

// Parser fecha/hora robusto (ES + am/pm + formatos comunes)
function parsePreference(text) {
  const t = rmDiacritics((text||"").toLowerCase())
  let day = null

  if (/\bhoy\b/.test(t)) day = dayjs().tz(EURO_TZ)
  else if (/\bmanana\b/.test(t)) day = dayjs().tz(EURO_TZ).add(1,"day")

  // 14 de agosto (opcional año)
  if (!day) {
    const m = t.match(/\b(\d{1,2})\s+de\s+(enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|setiembre|octubre|noviembre|diciembre|ene|feb|mar|abr|may|jun|jul|ago|sep|oct|nov|dic)\b(?:\s+de\s+(\d{4}))?/)
    if (m) {
      const dd = parseInt(m[1],10)
      const mm = MONTHS[m[2]] || null
      const yy = m[3] ? parseInt(m[3],10) : dayjs().tz(EURO_TZ).year()
      if (mm) day = dayjs.tz(`${yy}-${pad2(mm)}-${pad2(dd)} 00:00`, EURO_TZ)
    }
  }
  // 14/08[/2025] o 14-08
  if (!day) {
    const m = t.match(/\b(\d{1,2})[\/\-](\d{1,2})(?:[\/\-](\d{2,4}))?\b/)
    if (m) {
      const dd = parseInt(m[1],10), mm = parseInt(m[2],10)
      let yy = m[3] ? parseInt(m[3],10) : dayjs().tz(EURO_TZ).year()
      if (yy < 100) yy += 2000
      day = dayjs.tz(`${yy}-${pad2(mm)}-${pad2(dd)} 00:00`, EURO_TZ)
    }
  }
  // Día de la semana
  if (!day) {
    const m = t.match(/\b(domingo|lunes|martes|miercoles|jueves|viernes|sabado)\b/)
    if (m) {
      const target = ["domingo","lunes","martes","miercoles","jueves","viernes","sabado"].indexOf(m[1])
      const today = dayjs().tz(EURO_TZ)
      let diff = (target - today.day() + 7) % 7
      if (diff===0) diff = 7
      day = today.add(diff,"day").startOf("day")
    }
  }

  // Hora
  let hour = null, minute = 0
  const hm = t.match(/\b(\d{1,2})(?:[:h](\d{2}))?\s*(am|pm)?\b/)
  if (hm) {
    hour = parseInt(hm[1],10); minute = hm[2] ? parseInt(hm[2],10) : 0
    const ap = hm[3]
    if (ap === "pm" && hour < 12) hour += 12
    if (ap === "am" && hour === 12) hour = 0
  }
  if (!day && hour!==null) day = dayjs().tz(EURO_TZ)
  const hasTime = !!(day && hour!==null)

  if (!hasTime) return { hasTime:false }

  const start = day.hour(hour).minute(minute).second(0).millisecond(0)
  return { hasTime:true, day: start, hour, minute }
}

// Disponibilidad (slots)
function* slotsGenerator(fromDay, daysAhead = 10) {
  const start = fromDay.clone()
  for (let d=0; d<daysAhead; d++) {
    const day = start.add(d,"day")
    const dow = day.day()===0 ? 7 : day.day() // 1..7
    if (!WORK_DAYS.includes(dow)) continue
    let t = day.hour(OPEN_HOUR).minute(0).second(0).millisecond(0)
    const endOfDay = day.hour(CLOSE_HOUR).minute(0)
    while (t.add(SLOT_MIN,"minute").isSameOrBefore(endOfDay)) {
      yield t.clone()
    }
  }
}

const overlaps = (aStart,aEnd,bStart,bEnd)=> (aStart < bEnd) && (bStart < aEnd)

// =========== Square SDK
const square = new Client({
  accessToken: process.env.SQUARE_ACCESS_TOKEN,
  environment: process.env.SQUARE_ENV === "production" ? Environment.Production : Environment.Sandbox
})
const locationId = process.env.SQUARE_LOCATION_ID
let LOCATION_TZ = EURO_TZ

async function squareCheckCredentials() {
  try {
    const locs = await square.locationsApi.listLocations()
    const loc = (locs.result.locations || []).find(l => l.id === locationId) || (locs.result.locations || [])[0]
    if (loc?.timezone) LOCATION_TZ = loc.timezone
    console.log(`✅ Square OK. Location ${locationId} TZ=${LOCATION_TZ}`)
  } catch (e) {
    console.error("⛔ Square creds/location:", e?.message || e, e?.result?.errors || "")
  }
}

async function squareFindCustomerByPhone(phoneRaw) {
  try {
    const e164 = normalizePhoneES(phoneRaw)
    if (!e164 || !e164.startsWith("+") || e164.length < 8 || e164.length > 16) return null
    const resp = await square.customersApi.searchCustomers({ query: { filter: { phoneNumber: { exact: e164 } } } })
    return (resp?.result?.customers || [])[0] || null
  } catch (e) { console.error("Square search error:", e?.message || e, e?.result?.errors || ""); return null }
}

async function squareCreateCustomer({ givenName, emailAddress, phoneNumber }) {
  try {
    const phone = normalizePhoneES(phoneNumber)
    const resp = await square.customersApi.createCustomer({
      idempotencyKey: `cust_${Date.now()}_${Math.random().toString(36).slice(2,8)}`,
      givenName, emailAddress, phoneNumber: phone || undefined,
      note: "Creado desde bot WhatsApp Gapink Nails"
    })
    return resp?.result?.customer || null
  } catch (e) { console.error("Square create error:", e?.message || e, e?.result?.errors || ""); return null }
}

async function getServiceVariationVersion(serviceVariationId) {
  try {
    const resp = await square.catalogApi.retrieveCatalogObject(serviceVariationId, true)
    return resp?.result?.object?.version
  } catch (e) { console.error("getServiceVariationVersion error:", e?.message || e, e?.result?.errors || ""); return undefined }
}

async function createSquareBooking({ startEU, serviceKey, customerId, teamMemberId }) {
  try {
    const serviceVariationId = SERVICE_VARIATIONS[serviceKey]
    if (!serviceVariationId || !teamMemberId || !locationId) return null
    const version = await getServiceVariationVersion(serviceVariationId)
    if (!version) return null

    // Convertir hora Europe/Madrid → UTC ISO
    const startISO = startEU.tz("UTC").toISOString()

    const body = {
      idempotencyKey: `book_${Date.now()}_${Math.random().toString(36).slice(2,8)}`,
      booking: {
        locationId,
        startAt: startISO,
        customerId,
        appointmentSegments: [
          {
            teamMemberId,
            serviceVariationId,
            serviceVariationVersion: Number(version),
            durationMinutes: SERVICES[serviceKey] || 45
          }
        ]
      }
    }
    const resp = await square.bookingsApi.createBooking(body)
    return resp?.result?.booking || null
  } catch (e) { console.error("createSquareBooking error:", e?.message || e, e?.result?.errors || ""); return null }
}

// =========== DB
const db = new Database("gapink.db")
db.pragma("journal_mode = WAL")
db.exec(`
CREATE TABLE IF NOT EXISTS appointments (
  id TEXT PRIMARY KEY,
  customer_name TEXT,
  customer_phone TEXT,
  customer_square_id TEXT,
  service TEXT,
  duration_min INTEGER,
  start_iso TEXT,
  end_iso TEXT,
  staff_id TEXT,
  status TEXT,
  created_at TEXT,
  square_booking_id TEXT
);
CREATE TABLE IF NOT EXISTS sessions (
  phone TEXT PRIMARY KEY,
  state TEXT,
  data_json TEXT,
  updated_at TEXT,
  last_prompt_at TEXT
);
`)
const insertAppt = db.prepare(`INSERT INTO appointments
(id, customer_name, customer_phone, customer_square_id, service, duration_min, start_iso, end_iso, staff_id, status, created_at, square_booking_id)
VALUES (@id, @customer_name, @customer_phone, @customer_square_id, @service, @duration_min, @start_iso, @end_iso, @staff_id, @status, @created_at, @square_booking_id)`)
const listApptsBetween = db.prepare(`SELECT * FROM appointments WHERE status='confirmed' AND start_iso < @to AND end_iso > @from`)
const updateApptStatus   = db.prepare(`UPDATE appointments SET status=@status WHERE id=@id`)
const getUpcomingByPhone = db.prepare(`SELECT * FROM appointments WHERE customer_phone=@phone AND status='confirmed' AND start_iso > @now ORDER BY start_iso ASC LIMIT 1`)
const upsertSession = db.prepare(`
INSERT INTO sessions (phone, state, data_json, updated_at, last_prompt_at)
VALUES (@phone, @state, @data_json, @updated_at, @last_prompt_at)
ON CONFLICT(phone) DO UPDATE
SET state=excluded.state, data_json=excluded.data_json, updated_at=excluded.updated_at, last_prompt_at=excluded.last_prompt_at`)
const getSession   = db.prepare(`SELECT * FROM sessions WHERE phone=@phone`)
const clearSession = db.prepare(`DELETE FROM sessions WHERE phone=@phone`)

// =========== Disponibilidad
function getBookedIntervals(fromIso, toIso) {
  const appts = listApptsBetween.all({ from: fromIso, to: toIso })
  return appts.map(a => ({ start: dayjs(a.start_iso), end: dayjs(a.end_iso), staff_id: a.staff_id }))
}
function staffHasFree(intervals, start, end) {
  const staffIds = TEAM_MEMBER_IDS.length ? TEAM_MEMBER_IDS : ["any"]
  for (const st of staffIds) {
    const busy = intervals.filter(i => i.staff_id === st).some(i => overlaps(start,end,i.start,i.end))
    if (!busy) return true
  }
  return false
}

// Si usuario pide hora exacta, solo la damos si cabe; si no, sugerimos la más cercana sin reservar.
function findExactOrSuggest(serviceKey, durationMin, pref) {
  const now = dayjs().tz(EURO_TZ).second(0).millisecond(0)
  const endSearch = now.add(10,"day")
  const intervals = getBookedIntervals(now.utc().toISOString(), endSearch.utc().toISOString())

  if (pref?.day) {
    const start = pref.day.clone()
    const end   = start.clone().add(durationMin,"minute")
    // Validaciones negocio
    const dow = start.day()===0 ? 7 : start.day()
    if (!WORK_DAYS.includes(dow)) return { exact:null, suggestion:null }
    if (start.hour() < OPEN_HOUR || end.hour()>CLOSE_HOUR || (end.hour()===CLOSE_HOUR && end.minute()>0)) {
      // fuera de horario: sugerimos primer hueco libre ese día
      for (const s of slotsGenerator(start.startOf("day"), 1)) {
        const sEnd = s.clone().add(durationMin,"minute")
        if (s.isBefore(now.add(30,"minute"))) continue
        if (sEnd.hour()>CLOSE_HOUR || (sEnd.hour()===CLOSE_HOUR && sEnd.minute()>0)) continue
        if (staffHasFree(intervals, s, sEnd)) return { exact:null, suggestion:s }
      }
      return { exact:null, suggestion:null }
    }
    if (start.isBefore(now.add(30,"minute"))) {
      // no dar pasados/última hora: sugerencia
      for (const s of slotsGenerator(now, 3)) {
        const sEnd = s.clone().add(durationMin,"minute")
        if (staffHasFree(intervals, s, sEnd)) return { exact:null, suggestion:s }
      }
      return { exact:null, suggestion:null }
    }
    if (staffHasFree(intervals, start, end)) return { exact:start, suggestion:null }
    // Buscar el primer hueco cercano
    for (const s of slotsGenerator(start.startOf("day"), 1)) {
      const sEnd = s.clone().add(durationMin,"minute")
      if (s.isBefore(now.add(30,"minute"))) continue
      if (staffHasFree(intervals, s, sEnd)) return { exact:null, suggestion:s }
    }
    return { exact:null, suggestion:null }
  }

  // Sin hora → primera sugerencia libre
  for (const s of slotsGenerator(now, 10)) {
    const e = s.clone().add(durationMin,"minute")
    if (s.isBefore(now.add(30,"minute"))) continue
    if (e.hour()>CLOSE_HOUR || (e.hour()===CLOSE_HOUR && e.minute()>0)) continue
    if (staffHasFree(intervals, s, e)) return { exact:null, suggestion:s }
  }
  return { exact:null, suggestion:null }
}

// =========== Web mini (estado/QR)
const app = express()
const PORT = process.env.PORT || 8080
let lastQR = null, conectado = false

app.get("/", (_req, res) => {
  res.send(`<!doctype html><meta charset="utf-8"><style>body{font-family:system-ui;display:grid;place-items:center;min-height:100vh;background:linear-gradient(135deg,#fce4ec,#f8bbd0);color:#4a148c} .card{background:#fff;padding:24px;border-radius:16px;box-shadow:0 6px 24px rgba(0,0,0,.08);text-align:center;max-width:520px}</style><div class="card"><h1>Gapink Nails</h1><p>Estado: ${conectado?"✅ Conectado":"❌ Desconectado"}</p>${!conectado&&lastQR?`<img src="/qr.png" width="320" />`:``}<p><small>Desarrollado por Gonzalo</small></p></div>`)
})
app.get("/estado", (_req,res)=>res.json({ conectado, qr: !conectado && lastQR ? "/qr.png" : null }))
app.get("/qr.png", async (_req, res) => {
  try {
    if (!lastQR) return res.status(404).send("No hay QR activo")
    const png = await qrcode.toBuffer(lastQR, { type: "png", margin: 1, width: 512 })
    res.set("Content-Type", "image/png").send(png)
  } catch(e){ console.error("QR route error:", e); res.status(500).send("Error") }
})

app.listen(PORT, async () => {
  console.log(`🌐 Web en puerto ${PORT}`)
  await squareCheckCredentials()
  startBot().catch(e=>console.error("Fallo al iniciar bot:", e))
})

// =========== Bot
async function startBot() {
  console.log("🚀 Iniciando bot Gapink Nails (DeepSeek + TZ + Confirmación)...")
  try {
    if (!fs.existsSync("auth_info")) fs.mkdirSync("auth_info", { recursive: true })
    const { state, saveCreds } = await useMultiFileAuthState("auth_info")
    const { version } = await fetchLatestBaileysVersion()

    const sock = makeWASocket({
      logger: pino({ level: "silent" }),
      printQRInTerminal: false,
      auth: state,
      version,
      browser: Browsers.macOS("Desktop"),
      syncFullHistory: false,
      connectTimeoutMs: 30000
    })

    sock.ev.on("connection.update", ({ connection, lastDisconnect, qr }) => {
      if (qr) { lastQR = qr; conectado = false; try { qrcodeTerminal.generate(qr, { small:true }) } catch{} }
      if (connection === "open") { lastQR = null; conectado = true; console.log("✅ Conectado a WhatsApp") }
      if (connection === "close") {
        conectado = false
        console.log("❌ Conexión cerrada:", lastDisconnect?.error?.message || String(lastDisconnect?.error || ""))
        setTimeout(()=>startBot().catch(console.error), 3000)
      }
    })
    sock.ev.on("creds.update", saveCreds)

    const safeSend = async (jid, content) => { try { await sock.sendMessage(jid, content) } catch(e){ console.error("sendMessage error:", e) } }

    sock.ev.on("messages.upsert", async ({ messages }) => {
      try {
        const msg = messages?.[0]
        if (!msg?.message || msg.key.fromMe) return

        const from = msg.key.remoteJid
        const phoneRaw  = from?.split("@")[0] || ""
        const phoneE164 = normalizePhoneES(phoneRaw) || phoneRaw
        const body =
          msg.message.conversation ||
          msg.message.extendedTextMessage?.text ||
          msg.message?.imageMessage?.caption || ""
        let low = (body || "").trim()
        let lowNorm = rmDiacritics(low.toLowerCase())
        // limpiar menciones a pro
        lowNorm = lowNorm.replace(/\bcon\s+[a-záéíóúüñ]+/gi, "").trim()

        // Sesión
        const sess = getSession.get({ phone: phoneE164 })
        const data = sess?.data_json ? JSON.parse(sess.data_json) : {}

        // Intent
        const intent = await classify(low)
        if (intent === "GREETING" && !sess?.state) {
          await safeSend(from, { text: "Hola. ¿Quieres pedir cita o ver disponibilidad?" })
          upsertSession.run({ phone: phoneE164, state: "awaiting_service", data_json: JSON.stringify(data), updated_at: new Date().toISOString(), last_prompt_at: new Date().toISOString() })
          return
        }

        // Detectar piezas del mensaje actual
        const svcInMsg = detectService(low)
        const prefInMsg = parsePreference(low)

        if (svcInMsg) data.service = svcInMsg
        if (prefInMsg?.hasTime) data.timeParsed = prefInMsg

        // ===== CONFIRM STATE =====
        const saidYes = /\b(si|sí|ok|vale|confirmo|perfecto|de acuerdo)\b/i.test(low)
        const saidNo  = /\b(no|cambia|otra|mejor|mas tarde|más tarde|no confirmo)\b/i.test(low)

        // Si estando en confirm, el usuario manda nueva hora → actualizar propuesta
        if (sess?.state === "awaiting_confirm" && prefInMsg?.hasTime) {
          data.timeParsed = prefInMsg
        }

        if (sess?.state === "awaiting_confirm") {
          if (saidNo) {
            upsertSession.run({ phone: phoneE164, state: "awaiting_datetime", data_json: JSON.stringify(data), updated_at: new Date().toISOString(), last_prompt_at: new Date().toISOString() })
            await safeSend(from, { text: "Sin problema. Dime otra hora o día." })
            return
          }
          if (!saidYes) {
            await safeSend(from, { text: "¿Confirmo la cita? Responde “sí” para cerrar, o “no” para cambiar hora." })
            return
          }
          // Confirmado: si falta contacto y no existe cliente, lo pedimos UNA vez, pero guardamos que ya confirmó
          let customer = await squareFindCustomerByPhone(phoneE164)
          if (!customer && (!data.name || !data.email)) {
            data.confirmApproved = true
            await safeSend(from, { text: "Genial. Para cerrar, dime tu nombre y email (ej: “Ana Pérez, ana@correo.com”)." })
            upsertSession.run({ phone: phoneE164, state: "awaiting_contact", data_json: JSON.stringify(data), updated_at: new Date().toISOString(), last_prompt_at: new Date().toISOString() })
            return
          }
          // Tenemos todo → reservar
          await finalizeBooking(from, phoneE164, data)
          return
        }

        // ===== CONTACT STATE =====
        if (sess?.state === "awaiting_contact") {
          const { name: nm, email: em } = extractContact(low)
          if (nm) data.name = nm
          if (em) data.email = em
          if (!data.name || !data.email) {
            const need = !data.name ? "tu nombre y apellidos" : "tu email"
            await safeSend(from, { text: `Me falta ${need}.` })
            upsertSession.run({ phone: phoneE164, state: "awaiting_contact", data_json: JSON.stringify(data), updated_at: new Date().toISOString(), last_prompt_at: new Date().toISOString() })
            return
          }
          // Si ya había confirmado antes, no preguntar otra vez
          if (data.confirmApproved) {
            await finalizeBooking(from, phoneE164, data)
            return
          }
          // Pedir confirmación
          const when = data.timeParsed?.day ? fmtES(data.timeParsed.day) : "esa hora"
          await safeSend(from, { text: `Perfecto, ${data.name}. ¿Confirmo ${when} para ${data.service}?` })
          upsertSession.run({ phone: phoneE164, state: "awaiting_confirm", data_json: JSON.stringify(data), updated_at: new Date().toISOString(), last_prompt_at: new Date().toISOString() })
          return
        }

        // ===== SERVICE & DATE STATES =====
        if (!data.service && /(cita|reserv|disponibil|hueco|hora)/i.test(low)) {
          const txt = Object.keys(SERVICES).map(s=>`• ${s} (${SERVICES[s]} min)`).join("\n")
          await safeSend(from, { text: `¿Qué servicio necesitas?\n${txt}\n\nEjemplo:\n- "uñas acrílicas el 14 de agosto a las 10"` })
          upsertSession.run({ phone: phoneE164, state: "awaiting_service", data_json: JSON.stringify(data), updated_at: new Date().toISOString(), last_prompt_at: new Date().toISOString() })
          return
        }

        if (sess?.state === "awaiting_service" || !sess?.state) {
          if (!data.service && !svcInMsg) {
            await safeSend(from, { text: "Dime el servicio (p. ej., “uñas acrílicas”)." })
            upsertSession.run({ phone: phoneE164, state: "awaiting_service", data_json: JSON.stringify(data), updated_at: new Date().toISOString(), last_prompt_at: new Date().toISOString() })
            return
          }
          await safeSend(from, { text: "¿Qué día y hora te viene bien? Ej: “14 de agosto a las 10” o “mañana a las 10”." })
          upsertSession.run({ phone: phoneE164, state: "awaiting_datetime", data_json: JSON.stringify(data), updated_at: new Date().toISOString(), last_prompt_at: new Date().toISOString() })
          return
        }

        if (sess?.state === "awaiting_datetime" || (!data.timeParsed && data.service)) {
          if (!prefInMsg?.hasTime) {
            await safeSend(from, { text: "Necesito una hora concreta. Ej: “14/08 10:00” o “viernes 18:30”." })
            upsertSession.run({ phone: phoneE164, state: "awaiting_datetime", data_json: JSON.stringify(data), updated_at: new Date().toISOString(), last_prompt_at: new Date().toISOString() })
            return
          }
          data.timeParsed = prefInMsg
          // Calculamos exacto o sugerimos
          const durationMin = SERVICES[data.service]
          const { exact, suggestion } = findExactOrSuggest(data.service, durationMin, { day: data.timeParsed.day })
          if (exact) {
            await safeSend(from, { text: `Tengo libre ${fmtES(exact)} para ${data.service}. ¿Confirmo la cita?` })
            data.timeParsed.day = exact
            upsertSession.run({ phone: phoneE164, state: "awaiting_confirm", data_json: JSON.stringify(data), updated_at: new Date().toISOString(), last_prompt_at: new Date().toISOString() })
            return
          }
          if (suggestion) {
            await safeSend(from, { text: `No tengo ese hueco exacto. Te puedo ofrecer ${fmtES(suggestion)}. ¿Confirmo?` })
            data.timeParsed.day = suggestion
            upsertSession.run({ phone: phoneE164, state: "awaiting_confirm", data_json: JSON.stringify(data), updated_at: new Date().toISOString(), last_prompt_at: new Date().toISOString() })
            return
          }
          await safeSend(from, { text: "No veo hueco en esa franja. Dime otra hora o día." })
          upsertSession.run({ phone: phoneE164, state: "awaiting_datetime", data_json: JSON.stringify(data), updated_at: new Date().toISOString(), last_prompt_at: new Date().toISOString() })
          return
        }

        // ===== Comandos rápidos =====
        if (/(cancel(ar)? cita)/i.test(lowNorm)) {
          const upcoming = getUpcomingByPhone.get({ phone: phoneE164, now: new Date().toISOString() })
          if (!upcoming) return
          updateApptStatus.run({ id: upcoming.id, status: "cancelled" })
          await safeSend(from, { text: "Cita cancelada. Si quieres, pide una nueva con “cita uñas acrílicas”." })
          return
        }

        if (/(cambiar cita|mover cita|reprogramar)/i.test(lowNorm)) {
          const upcoming = getUpcomingByPhone.get({ phone: phoneE164, now: new Date().toISOString() })
          if (!upcoming) return
          const p = parsePreference(low)
          if (!p?.hasTime) { await safeSend(from, { text: "Dime nueva fecha y hora, porfa." }); return }
          const durationMin = upcoming.duration_min || SERVICES[upcoming.service] || 60
          const { exact, suggestion } = findExactOrSuggest(upcoming.service, durationMin, { day: p.day })
          const pick = exact || suggestion
          if (!pick) { await safeSend(from, { text: "No veo hueco en esa franja. Dime otra hora." }); return }
          db.prepare(`UPDATE appointments SET start_iso=@s, end_iso=@e WHERE id=@id`).run({
            id: upcoming.id,
            s: pick.tz("UTC").toISOString(),
            e: pick.clone().add(durationMin,"minute").tz("UTC").toISOString()
          })
          await safeSend(from, { text: `He movido tu cita a:\n${fmtES(pick)}\nPago en persona.` })
          return
        }

        // ===== Fallback humano corto
        const reply = await aiReply(low)
        if (reply) await safeSend(from, { text: reply })

      } catch (e) {
        console.error("messages.upsert error:", e)
      }
    })
  } catch (e) {
    console.error("startBot error:", e)
  }
}

// =========== Reserva final
async function finalizeBooking(from, phoneE164, data) {
  try {
    // Cliente
    let customer = await squareFindCustomerByPhone(phoneE164)
    if (!customer) {
      customer = await squareCreateCustomer({
        givenName: data.name || "Cliente",
        emailAddress: data.email || undefined,
        phoneNumber: phoneE164
      })
    }
    if (!customer) { await safeSend(from, { text: "Ahora mismo no puedo crear tu ficha. Prueba en unos minutos." }); return }

    const durationMin = SERVICES[data.service]
    const startEU = data.timeParsed.day.clone()  // Europe/Madrid
    const teamMemberId = TEAM_MEMBER_IDS[0] || null
    const sqBooking = await createSquareBooking({
      startEU, serviceKey: data.service, customerId: customer.id, teamMemberId
    })

    const aptId = `apt_${Math.random().toString(36).slice(2,8)}${Date.now().toString(36).slice(-4)}`
    insertAppt.run({
      id: aptId,
      customer_name: data.name || customer?.givenName || null,
      customer_phone: phoneE164,
      customer_square_id: customer.id,
      service: data.service,
      duration_min: durationMin,
      start_iso: startEU.tz("UTC").toISOString(),
      end_iso: startEU.clone().add(durationMin,"minute").tz("UTC").toISOString(),
      staff_id: teamMemberId,
      status: "confirmed",
      created_at: new Date().toISOString(),
      square_booking_id: sqBooking?.id || null
    })

    clearSession.run({ phone: phoneE164 })

    // Mensaje confirmación
    const txt = `Reserva confirmada.
Servicio: ${data.service}
Fecha: ${fmtES(startEU)}
Duración: ${durationMin} min
Pago en persona.`
    await globalSafeSend(from, { text: txt })
  } catch (e) {
    console.error("finalizeBooking error:", e)
    await globalSafeSend(from, { text: "No he podido cerrar la reserva ahora mismo. ¿Probamos en un momento?" })
  }
}

// safeSend accesible desde finalizeBooking
let globalSafeSend = async () => {}
// Hook para asignar safeSend real al arrancar socket
(function attachSafeSendLater(){
  Object.defineProperty(global, "setSafeSend", {
    value: fn => { globalSafeSend = fn },
    writable: false
  })
})()
