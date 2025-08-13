import express from "express"
import baileys from "@whiskeysockets/baileys"
import pino from "pino"
import qrcode from "qrcode"
import qrcodeTerminal from "qrcode-terminal"
import "dotenv/config"
import fs from "fs"
import { webcrypto } from "crypto"
import fetch from "node-fetch"
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
const WORK_DAYS = [1,2,3,4,5,6]   // 1=lun ... 6=sáb (7=dom cerrado)
const OPEN_HOUR  = 10
const CLOSE_HOUR = 20
const SLOT_MIN   = 30

// Servicios disponibles ahora mismo
const SERVICES = {
  "uñas acrílicas": 90,
}
const SERVICE_ALIASES = [
  { key: "uñas acrílicas", re: /\buñas?\s+acr[ií]licas?\b/i },
]

const SERVICE_VARIATIONS = {
  "uñas acrílicas": process.env.SQ_SV_UNAS_ACRILICAS || ""
}

const TEAM_MEMBER_IDS = (process.env.SQ_TEAM_IDS || "")
  .split(",").map(s => s.trim()).filter(Boolean)

// =================== DEEPSEEK ===================
async function dsChat(system, user, temperature=0.4) {
  try {
    const res = await fetch(process.env.DEEPSEEK_API_URL, {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${process.env.DEEPSEEK_API_KEY}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        model: process.env.DEEPSEEK_MODEL || "deepseek-chat",
        messages: [
          { role: "system", content: system },
          { role: "user", content: user }
        ],
        temperature
      })
    })
    if (!res.ok) {
      console.error("DeepSeek error:", res.status, await res.text())
      return ""
    }
    const data = await res.json()
    return data?.choices?.[0]?.message?.content?.trim() || ""
  } catch (e) {
    console.error("DeepSeek fetch error:", e)
    return ""
  }
}

// Clasificador muy estable (no free style). Devuelve BOOKING | GREETING | IGNORE.
async function classifyIntent(text) {
  const sys = `Clasifica el mensaje del usuario en EXACTAMENTE una palabra:
BOOKING si habla de reservar/cita/cambiar/cancelar/disponibilidad/horarios.
GREETING si solo saluda (hola, buenas, qué tal) o abre conversación.
IGNORE en cualquier otro caso.
Responde SOLO la palabra (BOOKING/GREETING/IGNORE).`
  const out = (await dsChat(sys, text || "", 0)).toUpperCase()
  if (out.startsWith("BOOKING")) return "BOOKING"
  if (out.startsWith("GREETING")) return "GREETING"
  return "IGNORE"
}

// Respuesta breve y humana para saludos o dudas no crí­ticas
async function smallTalk(text) {
  const sys = `Eres el asistente de Gapink Nails. Tono humano, breve y claro.
Guía a reservar: pregunta servicio y disponibilidad si procede. No uses emojis.`
  return await dsChat(sys, text || "", 0.6) || "¿Quieres pedir cita o consultar disponibilidad?"
}

// =================== HELPERS TELÉFONO & CONTACTO ===================
const onlyDigits = (s="") => (s || "").replace(/\D+/g, "")
function normalizePhoneES(raw) {
  const digits = onlyDigits(raw)
  if (!digits) return null
  if (raw.startsWith("+") && digits.length >= 8 && digits.length <= 15) return `+${digits}`
  if (digits.startsWith("34") && digits.length === 11) return `+${digits}`
  if (digits.length === 9) return `+34${digits}`
  if (digits.startsWith("00")) return `+${digits.slice(2)}`
  return `+${digits}`
}

// Extrae nombre/email si vienen juntos tipo “Pepe García y pepe@mail.com”
function extractContact(text="") {
  const t = text.trim()
  const emailMatch = t.match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i)
  const email = emailMatch ? emailMatch[0] : null

  const nameHints = [
    /(?:^|\b)(?:me llamo|soy|mi nombre es)\s+([A-Za-zÁÉÍÓÚÜÑáéíóúüñ' -]{2,80})/i,
  ]
  let name = null
  for (const re of nameHints) {
    const m = t.match(re)
    if (m && m[1]) { name = m[1].trim(); break }
  }
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

// =================== SQUARE SDK ===================
const square = new Client({
  accessToken: process.env.SQUARE_ACCESS_TOKEN,
  environment: process.env.SQUARE_ENV === "production" ? Environment.Production : Environment.Sandbox
})
const locationId = process.env.SQUARE_LOCATION_ID
let LOCATION_TZ = "Europe/Madrid"

async function squareCheckCredentials() {
  try {
    const locs = await square.locationsApi.listLocations()
    const loc = (locs.result.locations || []).find(l => l.id === locationId) || (locs.result.locations || [])[0]
    if (loc?.timezone) LOCATION_TZ = loc.timezone
    console.log(`✅ Square listo. Location ${locationId}, TZ: ${LOCATION_TZ}`)
  } catch (e) {
    console.error("⛔ Square creds/location:", e?.message || e, e?.result?.errors || "")
  }
}

async function squareFindCustomerByPhone(phoneRaw) {
  try {
    const e164 = normalizePhoneES(phoneRaw)
    if (!e164 || !e164.startsWith("+")) return null
    const resp = await square.customersApi.searchCustomers({
      query: { filter: { phoneNumber: { exact: e164 } } }
    })
    return (resp?.result?.customers || [])[0] || null
  } catch (e) {
    console.error("Square search error:", e?.message || e, e?.result?.errors || "")
    return null
  }
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
  } catch (e) {
    console.error("Square create error:", e?.message || e, e?.result?.errors || "")
    return null
  }
}

async function getServiceVariationVersion(serviceVariationId) {
  try {
    const resp = await square.catalogApi.retrieveCatalogObject(serviceVariationId, true)
    return resp?.result?.object?.version
  } catch (e) {
    console.error("getServiceVariationVersion error:", e?.message || e, e?.result?.errors || "")
    return undefined
  }
}

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
  } catch (e) {
    console.error("createSquareBooking error:", e?.message || e, e?.result?.errors || "")
    return null
  }
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
  state TEXT,              -- idle | ask_name | ask_email
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

// =================== SLOTS ===================
const isSameOrBefore = (a,b)=> a.isBefore(b) || a.isSame(b)

function* slotsGenerator(fromDay, daysAhead = 10) {
  const start = dayjs(fromDay)
  for (let d=0; d<daysAhead; d++) {
    const day = start.add(d, "day")
    const dow = (day.day()+6)%7 + 1
    if (!WORK_DAYS.includes(dow)) continue
    let t = day.hour(OPEN_HOUR).minute(0).second(0).millisecond(0)
    const endOfDay = day.hour(CLOSE_HOUR).minute(0)
    while (isSameOrBefore(t.add(SLOT_MIN,"minute"), endOfDay)) {
      yield t
      t = t.add(SLOT_MIN,"minute")
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

// Día/hora desde texto (“hoy/mañana/lunes… a las 19”)
const WEEKDAYS = ["domingo","lunes","martes","miércoles","miercoles","jueves","viernes","sábado","sabado"]
function parsePreference(text) {
  const t = (text || "").toLowerCase()
  let day = null
  if (/\bhoy\b/.test(t)) day = dayjs()
  else if (/\bmañana|\bmanana/.test(t)) day = dayjs().add(1,"day")
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
  if (m1) { hour = +m1[1]; if (m1[2]) minute = +m1[2] }
  if (!day && (hour!==null)) day = dayjs()
  return { day, hour, minute }
}

function findBestSlot(serviceKey, durationMin, preference) {
  const now = dayjs().second(0).millisecond(0)
  const endOfWeek = now.day() === 0 ? now.add(6,"day") : now.day(6)
  const intervalsWeek = getBookedIntervals(now.toISOString(), endOfWeek.endOf("day").toISOString())

  // Intento respetar preferencia
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
    while (isSameOrBefore(probe.add(SLOT_MIN,"minute"), endOfDay)) {
      const s = probe, e = probe.add(durationMin,"minute")
      if (e.hour()<=CLOSE_HOUR && staffHasFree(intervalsWeek, s, e)) return { start:s, end:e }
      probe = probe.add(SLOT_MIN,"minute")
    }
  }

  // Primer hueco de la semana
  for (const s of slotsGenerator(now, 7)) {
    if (s.isAfter(endOfWeek.endOf("day"))) break
    const start = s, end = s.add(durationMin,"minute")
    if (start.isBefore(now.add(30,"minute"))) continue
    if (end.hour()>CLOSE_HOUR || (end.hour()===CLOSE_HOUR && end.minute()>0)) continue
    if (staffHasFree(intervalsWeek, start, end)) return { start, end }
  }

  // Si no, hasta 10 días
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

// =================== BOT (Baileys) ===================
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
        let low = (body || "").trim().toLowerCase()

        // Quitar “con <nombre>”
        low = low.replace(/\bcon\s+[a-záéíóúüñ]+/gi,"").trim()

        // Sesión actual
        const sess = getSession.get({ phone: phoneE164 })
        const st = sess?.state || "idle"
        const data = sess?.data_json ? JSON.parse(sess.data_json) : {}

        // Si llega nombre/email en un mensaje, guárdalo
        const inline = extractContact(body)
        if (inline.name) data.name = inline.name
        if (inline.email) data.email = inline.email

        // Detectar servicio en este mensaje si aún no hay
        if (!data.service) {
          for (const a of SERVICE_ALIASES) {
            if (a.re.test(body)) { data.service = a.key; data.durationMin = SERVICES[a.key]; break }
          }
        }

        // Guardar preferencia horaria si llega
        if (/(hoy|mañana|manana|lunes|martes|miércoles|miercoles|jueves|viernes|sábado|sabado|\ba las?\b|\d{1,2}[:h]?\d{0,2})/i.test(body)) {
          data.preferenceText = (data.preferenceText || "") + " " + body
        }

        // Pipeline de intents
        const intent = await classifyIntent(body)

        // ========= PASO DE ALTA PENDIENTE =========
        if (st === "ask_name" || st === "ask_email") {
          if (!data.name || !data.email) {
            // pedir solo lo que falta, sin spam
            const need = !data.name ? "tu nombre y apellidos" : "tu email"
            const now = Date.now(), last = Date.parse(sess?.last_prompt_at || 0)
            if (now - last > 60_000) {
              upsertSession.run({
                phone: phoneE164,
                state: !data.name ? "ask_name" : "ask_email",
                data_json: JSON.stringify(data),
                updated_at: new Date().toISOString(),
                last_prompt_at: new Date().toISOString()
              })
              await safeSend(from, { text: `Para confirmarte la reserva necesito ${need}.` })
            }
            return
          }
          // ya tenemos ambos → crear cliente + reservar
          await flowCreateCustomerAndBook(from, phoneE164, data, safeSend)
          return
        }

        // ========= INTENT BOOKING =========
        if (intent === "BOOKING") {
          // Si no dijo servicio → pedirlo una vez
          if (!data.service) {
            await safeSend(from, { text: "¿Qué servicio necesitas? • uñas acrílicas (90 min)\nEjemplo: \"cita uñas acrílicas mañana a las 10\"" })
            upsertSession.run({
              phone: phoneE164, state: "idle",
              data_json: JSON.stringify(data),
              updated_at: new Date().toISOString(),
              last_prompt_at: new Date().toISOString()
            })
            return
          }

          // Si el cliente ya existe en Square → reservar directo
          let customer = await squareFindCustomerByPhone(phoneE164)
          const pref = parsePreference(data.preferenceText || body)
          const slot = findBestSlot(data.service, data.durationMin, pref)
          if (!slot) { await safeSend(from, { text: "Ahora mismo no veo huecos. Te aviso si se libera alguno cercano." }); return }

          const teamMemberId = TEAM_MEMBER_IDS[0] || null

          if (customer) {
            // agenda directo
            const squareBooking = await createSquareBooking({
              startISO: slot.start.toISOString(),
              serviceKey: data.service,
              customerId: customer.id,
              teamMemberId
            })
            const aptId = randomId("apt")
            insertAppt.run({
              id: aptId,
              customer_name: customer?.givenName || null,
              customer_phone: phoneE164,
              customer_square_id: customer?.id || null,
              service: data.service,
              duration_min: data.durationMin,
              start_iso: slot.start.toISOString(),
              end_iso: slot.end.toISOString(),
              staff_id: teamMemberId,
              status: "confirmed",
              created_at: new Date().toISOString(),
              square_booking_id: squareBooking?.id || null
            })
            await safeSend(from, { text:
`Reserva confirmada.
Servicio: ${data.service}
Fecha: ${slot.start.format("dddd DD/MM HH:mm")}
Duración: ${data.durationMin} min
Pago en persona.` })
            clearSession.run({ phone: phoneE164 })
            return
          }

          // no existe → intentar extraer nombre/email del mismo mensaje
          if (!data.name || !data.email) {
            const need = !data.name ? "tu nombre y apellidos" : "tu email"
            await safeSend(from, { text: `Para confirmar la reserva necesito ${need}.` })
            upsertSession.run({
              phone: phoneE164,
              state: !data.name ? "ask_name" : "ask_email",
              data_json: JSON.stringify(data),
              updated_at: new Date().toISOString(),
              last_prompt_at: new Date().toISOString()
            })
            return
          }

          // ya tenemos ambos → crear cliente + reservar
          await flowCreateCustomerAndBook(from, phoneE164, data, safeSend)
          return
        }

        // ========= INTENT GREETING =========
        if (intent === "GREETING") {
          const reply = await smallTalk(body)
          await safeSend(from, { text: reply || "¿Quieres pedir cita o consultar disponibilidad?" })
          return
        }

        // ========= OTROS: silencio o respuesta corta =========
        // Puedes dejarlo en silencio; o:
        // const reply = await smallTalk(body); if (reply) await safeSend(from, { text: reply })

      } catch (e) {
        console.error("messages.upsert error:", e)
      }
    })
  } catch (e) {
    console.error("startBot error:", e)
  }
}

// =================== SUB-FLUJO: crear cliente + reservar ===================
async function flowCreateCustomerAndBook(from, phoneE164, data, safeSend) {
  try {
    const sq = await squareCreateCustomer({
      givenName: data.name || "Cliente",
      emailAddress: data.email || undefined,
      phoneNumber: phoneE164
    })
    if (!sq) return

    const pref = parsePreference(data.preferenceText || "")
    const slot = findBestSlot(data.service, data.durationMin, pref)
    if (!slot) return

    const teamMemberId = TEAM_MEMBER_IDS[0] || null
    const squareBooking = await createSquareBooking({
      startISO: slot.start.toISOString(),
      serviceKey: data.service,
      customerId: sq.id,
      teamMemberId
    })

    const aptId = randomId("apt")
    insertAppt.run({
      id: aptId,
      customer_name: data.name,
      customer_phone: phoneE164,
      customer_square_id: sq.id,
      service: data.service,
      duration_min: data.durationMin,
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
Servicio: ${data.service}
Fecha: ${slot.start.format("dddd DD/MM HH:mm")}
Duración: ${data.durationMin} min
Pago en persona.` })
  } catch (e) {
    console.error("flowCreateCustomerAndBook error:", e)
  }
}
