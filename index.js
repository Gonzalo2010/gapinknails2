import express from "express"
import baileys from "@whiskeysockets/baileys"
import pino from "pino"
import qrcode from "qrcode"
import qrcodeTerminal from "qrcode-terminal"
import "dotenv/config"
import OpenAI from "openai"
import fs from "fs"
import { webcrypto } from "crypto"
import Database from "better-sqlite3"
import dayjs from "dayjs"
import { Client, Environment } from "square"

if (!globalThis.crypto) globalThis.crypto = webcrypto

const {
  makeWASocket,
  useMultiFileAuthState,
  fetchLatestBaileysVersion,
  Browsers
} = baileys

// =================== CONFIG NEGOCIO ===================
const WORK_DAYS = [1,2,3,4,5,6]         // 1=lun ... 6=sáb (7=dom cerrado)
const OPEN_HOUR  = 10
const CLOSE_HOUR = 20
const SLOT_MIN   = 30

// Servicios y duraciones
const SERVICES = {
  "uñas acrílicas": 90,
  // si luego añades más:
  // "manicura": 45,
  // "pedicura": 60,
  // "relleno": 75,
  // "semipermanente": 45
}

// Mapear servicios → service_variation_id (solo acrílicas ahora)
const SERVICE_VARIATIONS = {
  "uñas acrílicas": process.env.SQ_SV_UNAS_ACRILICAS || ""
}

// Técnicas (IDs reales de Square). No mostramos nombres.
const TEAM_MEMBER_IDS = (process.env.SQ_TEAM_IDS || "")
  .split(",").map(s => s.trim()).filter(Boolean)

// =================== OPENAI ===================
const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY })

const SYSTEM_PROMPT = `Eres el asistente de WhatsApp de Gapink Nails. Tu objetivo es sonar humano (natural y cercano), no robótico.
Reglas:
- Nunca reveles el nombre de la técnica ni ofrezcas elegir profesional.
- Prioriza rellenar huecos cercanos de la semana, salvo que el cliente pida hora/día concreto (intenta respetarlo).
- Mensajes cortos y claros, sin emojis.
- Pago siempre en persona.
- Si faltan datos para alta nueva, pide solo nombre y email.
- Si el mensaje NO trata de reservas/cambios/cancelaciones/disponibilidad, no respondas.
- En confirmaciones incluye servicio, fecha/hora y que el pago será en persona.
- Español de España. Nunca hables de que eres IA.`

async function isBookingIntent(text) {
  try {
    const r = await openai.chat.completions.create({
      model: "gpt-4o-mini",
      messages: [
        { role: "system", content:
`Devuelve SOLO:
BOOKING -> si el mensaje trata de reservar/cita/disponibilidad/cambiar/cancelar.
IGNORE  -> en cualquier otro caso.` },
        { role: "user", content: text || "" }
      ],
      temperature: 0
    })
    return (r.choices[0].message.content || "").trim().toUpperCase().startsWith("BOOKING")
  } catch (e) { console.error("Classifier error:", e); return false }
}

async function aiReply(text) {
  try {
    const r = await openai.chat.completions.create({
      model: "gpt-4o-mini",
      messages: [
        { role: "system", content: SYSTEM_PROMPT },
        { role: "user", content: text }
      ],
      temperature: 0.4
    })
    return r.choices[0].message.content?.trim() || ""
  } catch (e) { console.error("AI reply error:", e); return "" }
}

// =================== HELPERS TELÉFONO ===================
const onlyDigits = (s="") => (s || "").replace(/\D+/g, "")
function normalizePhoneES(raw) {
  const digits = onlyDigits(raw)
  if (!digits) return null
  if (digits.startsWith("34") && digits.length === 11) return `+${digits}`
  if (digits.length === 9) return `+34${digits}`
  if (digits.startsWith("00")) return `+${digits.slice(2)}`
  if (raw.startsWith("+")) return raw
  return `+${digits}`
}

// =================== SQUARE SDK ===================
const square = new Client({
  accessToken: process.env.SQUARE_ACCESS_TOKEN,
  environment: process.env.SQUARE_ENV === "production" ? Environment.Production : Environment.Sandbox
})
const locationId = process.env.SQUARE_LOCATION_ID
let LOCATION_TZ = "Europe/Madrid" // solo para logs

async function squareCheckCredentials() {
  try {
    const locs = await square.locationsApi.listLocations()
    const loc = (locs.result.locations || []).find(l => l.id === locationId) || (locs.result.locations || [])[0]
    if (loc?.timezone) LOCATION_TZ = loc.timezone
    console.log(`✅ Square listo. Location ${locationId}, TZ: ${LOCATION_TZ}`)
  } catch (e) { console.error("⛔ Square creds/location:", e?.message || e) }
}

async function squareFindCustomerByPhone(phoneRaw) {
  try {
    const e164 = normalizePhoneES(phoneRaw)
    const candidates = Array.from(new Set([ e164, (e164||"").replace("+",""), onlyDigits(phoneRaw) ])).filter(Boolean)
    for (const ph of candidates) {
      const resp = await square.customersApi.searchCustomers({ query: { filter: { phoneNumber: { exact: ph } } } })
      const list = resp?.result?.customers || []
      if (list[0]) return list[0]
    }
    return null
  } catch (e) { console.error("Square search error:", e?.message || e); return null }
}

async function squareCreateCustomer({ givenName, emailAddress, phoneNumber }) {
  try {
    const phone = normalizePhoneES(phoneNumber)
    const resp = await square.customersApi.createCustomer({
      idempotencyKey: `cust_${Date.now()}_${Math.random().toString(36).slice(2,8)}`,
      givenName, emailAddress,
      phoneNumber: phone || undefined,
      note: "Creado desde bot WhatsApp Gapink Nails"
    })
    return resp?.result?.customer || null
  } catch (e) { console.error("Square create error:", e?.message || e); return null }
}

// Obtener versión de variación de servicio (para Bookings)
async function getServiceVariationVersion(serviceVariationId) {
  try {
    const resp = await square.catalogApi.retrieveCatalogObject(serviceVariationId, true)
    return resp?.result?.object?.version
  } catch (e) { console.error("getServiceVariationVersion error:", e?.message || e); return undefined }
}

// Crear booking real en Square
async function createSquareBooking({ startISO, serviceKey, customerId, teamMemberId }) {
  try {
    const serviceVariationId = SERVICE_VARIATIONS[serviceKey]
    if (!serviceVariationId || !teamMemberId || !locationId) return null
    const version = await getServiceVariationVersion(serviceVariationId)
    if (!version) return null

    const body = {
      idempotencyKey: `book_${Date.now()}_${Math.random().toString(36).slice(2,8)}`,
      booking: {
        locationId,
        startAt: startISO, // UTC
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
  } catch (e) { console.error("createSquareBooking error:", e?.message || e); return null }
}

// =================== DB ===================
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
  updated_at TEXT
);
`)

const insertAppt = db.prepare(`INSERT INTO appointments
(id, customer_name, customer_phone, customer_square_id, service, duration_min, start_iso, end_iso, staff_id, status, created_at, square_booking_id)
VALUES (@id, @customer_name, @customer_phone, @customer_square_id, @service, @duration_min, @start_iso, @end_iso, @staff_id, @status, @created_at, @square_booking_id)`)

const listApptsBetween = db.prepare(`SELECT * FROM appointments WHERE status='confirmed' AND start_iso < @to AND end_iso > @from`)
const updateApptStatus   = db.prepare(`UPDATE appointments SET status=@status WHERE id=@id`)
const getUpcomingByPhone = db.prepare(`SELECT * FROM appointments WHERE customer_phone=@phone AND status='confirmed' AND start_iso > @now ORDER BY start_iso ASC LIMIT 1`)

const upsertSession = db.prepare(`
INSERT INTO sessions (phone, state, data_json, updated_at) VALUES (@phone, @state, @data_json, @updated_at)
ON CONFLICT(phone) DO UPDATE SET state=excluded.state, data_json=excluded.data_json, updated_at=excluded.updated_at`)
const getSession   = db.prepare(`SELECT * FROM sessions WHERE phone=@phone`)
const clearSession = db.prepare(`DELETE FROM sessions WHERE phone=@phone`)

// =================== SLOTS & PARSEO DE PREFERENCIAS ===================
function* slotsGenerator(fromDay, daysAhead = 10) {
  const start = dayjs(fromDay)
  for (let d=0; d<daysAhead; d++) {
    const day = start.add(d, "day")
    const dow = (day.day()+6)%7 + 1
    if (!WORK_DAYS.includes(dow)) continue
    let t = day.hour(OPEN_HOUR).minute(0).second(0).millisecond(0)
    const endOfDay = day.hour(CLOSE_HOUR).minute(0)
    while (t.add(SLOT_MIN, "minute").isBefore(endOfDay) || t.add(SLOT_MIN, "minute").isSame(endOfDay)) {
      yield t
      t = t.add(SLOT_MIN, "minute")
    }
  }
}
const overlaps = (aStart,aEnd,bStart,bEnd)=> (aStart < bEnd) && (bStart < aEnd)
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

// Parsea “hoy/mañana/lunes… a las 20/20:30”
const WEEKDAYS = ["domingo","lunes","martes","miércoles","miercoles","jueves","viernes","sábado","sabado"]
function parsePreference(text) {
  const t = (text || "").toLowerCase()
  let day = null
  if (/hoy\b/.test(t)) day = dayjs()
  else if (/mañana|manana/.test(t)) day = dayjs().add(1,"day")
  else {
    for (let i=0;i<WEEKDAYS.length;i++){
      const name = WEEKDAYS[i]
      if (t.includes(name)) {
        const today = dayjs()
        let targetDow = i===0?7:(i)
        let dowToday = (today.day()+6)%7 + 1
        let diff = (targetDow - dowToday + 7) % 7
        if (diff===0) diff = 7
        day = today.add(diff,"day")
        break
      }
    }
  }
  let hour = null, minute = 0
  const m1 = t.match(/(?:a las |a la |a )?(\d{1,2})(?:[:h](\d{2}))?\b/)
  if (m1) {
    hour = parseInt(m1[1],10)
    if (m1[2]) minute = parseInt(m1[2],10)
  }
  if (!day && (hour!==null)) day = dayjs()
  return { day, hour, minute }
}

// Encuentra hueco: respeta preferencia si cabe; si no, earliest
function findBestSlot(serviceKey, durationMin, preference) {
  const now = dayjs().second(0).millisecond(0)
  const endOfWeek = now.day() === 0 ? now.add(6,"day") : now.day(6)
  const intervalsWeek = getBookedIntervals(now.toISOString(), endOfWeek.endOf("day").toISOString())

  if (preference?.day && preference.hour !== null) {
    let start = preference.day.hour(preference.hour).minute(preference.minute || 0).second(0).millisecond(0)
    const latestStart = preference.day.hour(CLOSE_HOUR).minute(0).subtract(durationMin,"minute")
    if (start.isBefore(preference.day.hour(OPEN_HOUR))) start = preference.day.hour(OPEN_HOUR)
    if (start.isAfter(latestStart)) start = latestStart
    if (start.isBefore(now.add(30,"minute"))) start = now.add(30,"minute")
    const end = start.add(durationMin,"minute")
    const dow = (start.day()+6)%7 + 1
    if (WORK_DAYS.includes(dow) && end.hour()<=CLOSE_HOUR && staffHasFree(intervalsWeek, start, end)) {
      return { start, end }
    }
    const endOfDay = preference.day.hour(CLOSE_HOUR).minute(0)
    let probe = start
    while (probe.add(SLOT_MIN,"minute").isSameOrBefore(endOfDay)) {
      const s = probe, e = probe.add(durationMin,"minute")
      if (e.hour()<=CLOSE_HOUR && staffHasFree(intervalsWeek, s, e)) return { start:s, end:e }
      probe = probe.add(SLOT_MIN,"minute")
    }
  }

  for (const s of slotsGenerator(now, 7)) {
    if (s.isAfter(endOfWeek.endOf("day"))) break
    const start = s, end = s.add(durationMin,"minute")
    if (start.isBefore(now.add(30,"minute"))) continue
    if (end.hour()>CLOSE_HOUR || (end.hour()===CLOSE_HOUR && end.minute()>0)) continue
    if (staffHasFree(intervalsWeek, start, end)) return { start, end }
  }

  const intervals10 = getBookedIntervals(now.toISOString(), now.add(10,"day").toISOString())
  for (const s of slotsGenerator(now, 10)) {
    const start = s, end = s.add(durationMin,"minute")
    if (start.isBefore(now.add(30,"minute"))) continue
    if (end.hour()>CLOSE_HOUR || (end.hour()===CLOSE_HOUR && end.minute()>0)) continue
    if (staffHasFree(intervals10, start, end)) return { start, end }
  }
  return null
}

const randomId = (p="apt") => `${p}_${Math.random().toString(36).slice(2,8)}${Date.now().toString(36).slice(-4)}`

// =================== WEB ESTADO/QR ===================
const app = express()
const PORT = process.env.PORT || 8080

let lastQR = null
let conectado = false

app.get("/", (_req, res) => {
  res.send(`<!doctype html><meta charset="utf-8"><style>body{font-family:system-ui;display:grid;place-items:center;min-height:100vh;background:linear-gradient(135deg,#fce4ec,#f8bbd0);color:#4a148c} .card{background:#fff;padding:24px;border-radius:16px;box-shadow:0 6px 24px rgba(0,0,0,.08);text-align:center;max-width:520px}</style><div class="card"><h1>Gapink Nails</h1><p>Estado: ${conectado?"✅ Conectado":"❌ Desconectado"}</p>${!conectado&&lastQR?`<img src="/qr.png" width="320" />`:``}<p><small>Desarrollado por <a href="https://gonzalog.co" target="_blank">Gonzalo García Aranda</a></small></p></div>`)
})
app.get("/estado", (_req,res)=>res.json({ conectado, qr: !conectado && lastQR ? "/qr.png" : null }))
app.get("/qr.png", async (_req, res) => {
  try {
    if (!lastQR) return res.status(404).send("No hay QR activo")
    const png = await qrcode.toBuffer(lastQR, { type: "png", margin: 1, width: 512 })
    res.set("Content-Type", "image/png").send(png)
  } catch(e){ console.error("QR route error:", e); res.status(500).send("Error") }
})

// =================== ARRANQUE ===================
app.listen(PORT, async () => {
  console.log(`🌐 Web en puerto ${PORT}`)
  await squareCheckCredentials()
  startBot().catch(e=>console.error("Fallo al iniciar bot:", e))
})

// =================== BOT ===================
async function startBot() {
  console.log("🚀 Iniciando bot Gapink Nails...")
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
        const low = (body || "").trim().toLowerCase()

        // Sesión (alta de cliente)
        const sess = getSession.get({ phone: phoneE164 })
        if (sess?.state === "ask_name") {
          const name = (body || "").trim().replace(/\s+/g," ").slice(0,80)
          const data = JSON.parse(sess.data_json || "{}"); data.name = name
          upsertSession.run({ phone: phoneE164, state: "ask_email", data_json: JSON.stringify(data), updated_at: new Date().toISOString() })
          await safeSend(from, { text: "¿Cuál es tu email? (para enviarte la confirmación)" })
          return
        }
        if (sess?.state === "ask_email") {
          const email = (body || "").trim()
          const data = JSON.parse(sess.data_json || "{}"); data.email = email

          let sq = null
          try { sq = await squareCreateCustomer({ givenName: data.name || "Cliente", emailAddress: email || undefined, phoneNumber: phoneE164 }) } catch(e){ console.error(e) }
          if (!sq) { clearSession.run({ phone: phoneE164 }); return }

          const dur = data.durationMin, svc = data.service
          const pref = parsePreference(data.preferenceText || "")
          const slot = findBestSlot(svc, dur, pref)
          if (!slot) { clearSession.run({ phone: phoneE164 }); return }

          const teamMemberId = TEAM_MEMBER_IDS[0] || null
          let squareBooking = null
          try {
            squareBooking = await createSquareBooking({
              startISO: slot.start.toISOString(),
              serviceKey: svc,
              customerId: sq.id,
              teamMemberId
            })
          } catch (e) { console.error("square booking (new) error:", e) }

          const aptId = randomId("apt")
          insertAppt.run({
            id: aptId,
            customer_name: data.name,
            customer_phone: phoneE164,
            customer_square_id: sq.id,
            service: svc,
            duration_min: dur,
            start_iso: slot.start.toISOString(),
            end_iso: slot.end.toISOString(),
            staff_id: teamMemberId,
            status: "confirmed",
            created_at: new Date().toISOString(),
            square_booking_id: squareBooking?.id || null
          })

          clearSession.run({ phone: phoneE164 })
          await safeSend(from, { text:
`Reserva confirmada.
Servicio: ${svc}
Fecha: ${slot.start.format("dddd DD/MM HH:mm")}
Duración: ${dur} min
Pago en persona.` })
          return
        }

        // Clasificador: si no es de cita, no respondemos
        const seemsBooking = await isBookingIntent(body || "")
        if (!seemsBooking) return

        // Cancelar
        if (/(cancel(ar)? cita)/i.test(low)) {
          const upcoming = getUpcomingByPhone.get({ phone: phoneE164, now: new Date().toISOString() })
          if (!upcoming) return
          updateApptStatus.run({ id: upcoming.id, status: "cancelled" })
          await safeSend(from, { text: "Cita cancelada. Si quieres, pide una nueva con “cita uñas acrílicas”." })
          return
        }

        // Cambiar
        if (/(cambiar cita|mover cita|reprogramar)/i.test(low)) {
          const upcoming = getUpcomingByPhone.get({ phone: phoneE164, now: new Date().toISOString() })
          if (!upcoming) return
          const pref = parsePreference(body)
          const slot = findBestSlot(upcoming.service, upcoming.duration_min, pref)
          if (!slot) return
          db.prepare(`UPDATE appointments SET start_iso=@s, end_iso=@e WHERE id=@id`).run({
            id: upcoming.id, s: slot.start.toISOString(), e: slot.end.toISOString()
          })
          await safeSend(from, { text:
`He movido tu cita a:
${slot.start.format("dddd DD/MM HH:mm")}
Pago en persona.` })
          return
        }

        // Pedir cita (solo “uñas acrílicas” de momento)
        const svc = Object.keys(SERVICES).find(s => low.includes(s))
        if (/(reserva|reservar|cita|disponible|disponibilidad|pedir hora|hueco)/i.test(low) && svc) {
          const durationMin = SERVICES[svc]
          let customer = null
          try { customer = await squareFindCustomerByPhone(phoneE164) } catch(e){ console.error(e) }

          const pref = parsePreference(body)
          const slot = findBestSlot(svc, durationMin, pref)
          if (!slot) return

          const teamMemberId = TEAM_MEMBER_IDS[0] || null
          let squareBooking = null
          if (customer) {
            try {
              squareBooking = await createSquareBooking({
                startISO: slot.start.toISOString(),
                serviceKey: svc,
                customerId: customer.id,
                teamMemberId
              })
            } catch (e) { console.error("square booking (existing) error:", e) }
          }

          const aptId = randomId("apt")
          insertAppt.run({
            id: aptId,
            customer_name: customer?.givenName || null,
            customer_phone: phoneE164,
            customer_square_id: customer?.id || null,
            service: svc,
            duration_min: durationMin,
            start_iso: slot.start.toISOString(),
            end_iso: slot.end.toISOString(),
            staff_id: teamMemberId,
            status: "confirmed",
            created_at: new Date().toISOString(),
            square_booking_id: squareBooking?.id || null
          })

          await safeSend(from, { text:
`Reserva confirmada.
Servicio: ${svc}
Fecha: ${slot.start.format("dddd DD/MM HH:mm")}
Duración: ${durationMin} min
Pago en persona.` })
          return
        }

        // Si pregunta disponibilidad pero sin decir servicio → pedirlo
        if (/(reserva|reservar|cita|disponible|disponibilidad|pedir hora|hueco)/i.test(low) && !Object.keys(SERVICES).some(s=>low.includes(s))) {
          const txt = Object.keys(SERVICES).map(s=>`• ${s} (${SERVICES[s]} min)`).join("\n")
          await safeSend(from, { text:
`¿Qué servicio necesitas?
${txt}

Ejemplo:
- "cita uñas acrílicas hoy a las 19"` })
          return
        }

        // Fallback corto (solo booking)
        const reply = await aiReply(body)
        if (reply) await safeSend(from, { text: reply })
      } catch (e) {
        console.error("messages.upsert error:", e) // nada al usuario
      }
    })
  } catch (e) {
    console.error("startBot error:", e)
  }
}
