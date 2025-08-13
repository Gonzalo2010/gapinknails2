// index.js — Gapink Nails WhatsApp Bot (DeepSeek + extracción JSON + tono humano)

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
dayjs.extend(utc); dayjs.extend(tz); dayjs.locale("es")
const EURO_TZ = "Europe/Madrid"

// ===== Baileys
const { makeWASocket, useMultiFileAuthState, fetchLatestBaileysVersion, Browsers } = baileys

// ===== Negocio
const WORK_DAYS = [1,2,3,4,5,6] // 1=lun...6=sáb (7=dom cerrado)
const OPEN_HOUR  = 10
const CLOSE_HOUR = 20
const SLOT_MIN   = 30

const SERVICES = { "uñas acrílicas": 90 }
const SERVICE_VARIATIONS = { "uñas acrílicas": process.env.SQ_SV_UNAS_ACRILICAS || "" }
const TEAM_MEMBER_IDS = (process.env.SQ_TEAM_IDS || "").split(",").map(s=>s.trim()).filter(Boolean)

// ===== DeepSeek
const DEEPSEEK_API_KEY = process.env.DEEPSEEK_API_KEY
const DEEPSEEK_API_URL = process.env.DEEPSEEK_API_URL || "https://api.deepseek.com/chat/completions"
const DEEPSEEK_MODEL   = process.env.DEEPSEEK_MODEL   || "deepseek-chat"

async function dsChat(messages, opts={ temperature:0.4 }) {
  try {
    const r = await fetch(DEEPSEEK_API_URL, {
      method: "POST",
      headers: {
        "Content-Type":"application/json",
        "Authorization":`Bearer ${DEEPSEEK_API_KEY}`
      },
      body: JSON.stringify({ model: DEEPSEEK_MODEL, messages, temperature: opts.temperature })
    })
    if (!r.ok) throw new Error(`DeepSeek ${r.status} ${await r.text()}`)
    const j = await r.json()
    return (j?.choices?.[0]?.message?.content || "").trim()
  } catch (e) { console.error("DeepSeek error:", e?.message || e); return "" }
}

// Sistema: tono humano, nada de “soy IA”
const SYS_TONE = `Eres el asistente de WhatsApp de un salón de uñas (Gapink Nails, España).
Habla natural, breve y claro (sin emojis). Nunca digas que eres IA.
Si el horario pedido no cuadra, ofrece el hueco más cercano y pide confirmación.
No des nombres del personal. Pago siempre en persona.`

// 1) EXTRACCIÓN JSON (la IA “piensa” y devuelve estructura; nosotros no la encorsetamos)
async function extractFromText(userText) {
  const schema = `
Devuelve SOLO un JSON válido con estas claves (puedes omitir las que no veas):
{
  "intent": "greeting|booking|cancel|reschedule|other",
  "service": "uñas acrílicas|…",
  "datetime_text": "texto tal cual que indique fecha/hora si lo hay",
  "confirm": "yes|no|unknown",
  "name": "Nombre si lo ves",
  "email": "email si lo ves",
  "polite_reply": "redacción breve y natural que avance la conversación"
}`
  const content = await dsChat([
    { role:"system", content: `${SYS_TONE}\n${schema}\nHabla y entiende español de España.` },
    { role:"user", content: userText || "" }
  ], { temperature: 0.2 })
  try {
    const jsonStr = content.trim().replace(/^```(json)?/i,"").replace(/```$/,"")
    return JSON.parse(jsonStr)
  } catch {
    // Fallback mínimo
    return { intent:"other", polite_reply:"" }
  }
}

// 2) RESPUESTA NATURAL (la IA escribe, pero tú le pasas el contexto)
async function aiSay(contextSummary) {
  return await dsChat([
    { role:"system", content: SYS_TONE },
    { role:"user", content: contextSummary }
  ], { temperature: 0.4 })
}

// ===== Utilidades texto/fecha
const onlyDigits = (s="") => (s||"").replace(/\D+/g,"")
const rmDiacritics = (s="") => s.normalize("NFD").replace(/\p{Diacritic}/gu,"")
const YES_RE = /\b(si|sí|ok|vale|confirmo|confirmar|de acuerdo|perfecto)\b/i
const NO_RE  = /\b(no|mejor otra|cambia|no confirmo)\b/i

function normalizePhoneES(raw) {
  const digits = onlyDigits(raw)
  if (!digits) return null
  if (raw.startsWith("+") && digits.length >= 8 && digits.length <= 15) return `+${digits}`
  if (digits.startsWith("34") && digits.length === 11) return `+${digits}`
  if (digits.length === 9) return `+34${digits}`
  if (digits.startsWith("00")) return `+${digits.slice(2)}`
  return `+${digits}`
}

function detectServiceFree(text="") {
  const low = rmDiacritics(text.toLowerCase())
  const map = { "unas acrilicas":"uñas acrílicas", "uñas acrilicas":"uñas acrílicas", "uñas acrílicas":"uñas acrílicas" }
  for (const k of Object.keys(map)) if (low.includes(rmDiacritics(k))) return map[k]
  for (const k of Object.keys(SERVICES)) if (low.includes(rmDiacritics(k))) return k
  return null
}

// Parse ES fechas/horas con varios formatos y am/pm
function parseDateTimeES(dtText) {
  if (!dtText) return null
  const t = rmDiacritics(dtText.toLowerCase())
  let baseDay = null
  if (/\bhoy\b/.test(t)) baseDay = dayjs().tz(EURO_TZ)
  else if (/\bmanana\b/.test(t)) baseDay = dayjs().tz(EURO_TZ).add(1,"day")
  // 14 de agosto (opcional año)
  if (!baseDay) {
    const MONTHS = {enero:1,febrero:2,marzo:3,abril:4,mayo:5,junio:6,julio:7,agosto:8,septiembre:9,setiembre:9,octubre:10,noviembre:11,diciembre:12,ene:1,feb:2,mar:3,abr:4,may:5,jun:6,jul:7,ago:8,sep:9,oct:10,nov:11,dic:12}
    const m = t.match(/\b(\d{1,2})\s+de\s+(enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|setiembre|octubre|noviembre|diciembre|ene|feb|mar|abr|may|jun|jul|ago|sep|oct|nov|dic)\b(?:\s+de\s+(\d{4}))?/)
    if (m) {
      const dd = +m[1], mm = MONTHS[m[2]], yy = m[3] ? +m[3] : dayjs().tz(EURO_TZ).year()
      baseDay = dayjs.tz(`${yy}-${String(mm).padStart(2,"0")}-${String(dd).padStart(2,"0")} 00:00`, EURO_TZ)
    }
  }
  // 14/08[/2025]
  if (!baseDay) {
    const m = t.match(/\b(\d{1,2})[\/\-](\d{1,2})(?:[\/\-](\d{2,4}))?\b/)
    if (m) {
      let yy = m[3] ? +m[3] : dayjs().tz(EURO_TZ).year()
      if (yy < 100) yy += 2000
      baseDay = dayjs.tz(`${yy}-${String(+m[2]).padStart(2,"0")}-${String(+m[1]).padStart(2,"0")} 00:00`, EURO_TZ)
    }
  }
  // si solo hay hora, usar hoy
  if (!baseDay) baseDay = dayjs().tz(EURO_TZ)

  // hora
  let hour = null, minute = 0
  const hm = t.match(/(\d{1,2})(?::|h)?(\d{2})?\s*(am|pm)?\b/)
  if (hm) {
    hour = +hm[1]; minute = hm[2] ? +hm[2] : 0
    const ap = hm[3]
    if (ap === "pm" && hour < 12) hour += 12
    if (ap === "am" && hour === 12) hour = 0
  }
  if (hour===null) return null
  return baseDay.hour(hour).minute(minute).second(0).millisecond(0)
}

const fmtES = (d)=> {
  const t = d.tz(EURO_TZ)
  const dias = ["domingo","lunes","martes","miércoles","jueves","viernes","sábado"]
  const DD = String(t.date()).padStart(2,"0"), MM = String(t.month()+1).padStart(2,"0")
  const HH = String(t.hour()).padStart(2,"0"), mm = String(t.minute()).padStart(2,"0")
  return `${dias[t.day()]} ${DD}/${MM} ${HH}:${mm}`
}

// ===== Square
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
    console.log(`✅ Square listo. Location ${locationId}, TZ=${LOCATION_TZ}`)
  } catch (e) { console.error("⛔ Square:", e?.message || e) }
}
async function squareFindCustomerByPhone(phoneRaw) {
  try {
    const e164 = normalizePhoneES(phoneRaw)
    if (!e164 || !e164.startsWith("+") || e164.length < 8 || e164.length > 16) return null
    const resp = await square.customersApi.searchCustomers({ query: { filter: { phoneNumber: { exact: e164 } } } })
    return (resp?.result?.customers || [])[0] || null
  } catch (e) { console.error("Square search:", e?.message || e); return null }
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
  } catch (e) { console.error("Square create:", e?.message || e); return null }
}
async function getServiceVariationVersion(serviceVariationId) {
  try {
    const resp = await square.catalogApi.retrieveCatalogObject(serviceVariationId, true)
    return resp?.result?.object?.version
  } catch (e) { console.error("getServiceVariationVersion:", e?.message || e); return undefined }
}
async function createSquareBooking({ startEU, serviceKey, customerId, teamMemberId }) {
  try {
    const serviceVariationId = SERVICE_VARIATIONS[serviceKey]
    if (!serviceVariationId || !teamMemberId || !locationId) return null
    const version = await getServiceVariationVersion(serviceVariationId)
    if (!version) return null
    const startISO = startEU.tz("UTC").toISOString()
    const endISO = startEU.clone().add(SERVICES[serviceKey],"minute").tz("UTC").toISOString()
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
            durationMinutes: SERVICES[serviceKey]
          }
        ]
      }
    }
    const resp = await square.bookingsApi.createBooking(body)
    return { booking: resp?.result?.booking || null, startISO, endISO }
  } catch (e) { console.error("createSquareBooking:", e?.message || e); return null }
}

// ===== DB
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
  data_json TEXT,
  updated_at TEXT
);
`)
const insertAppt = db.prepare(`INSERT INTO appointments
(id, customer_name, customer_phone, customer_square_id, service, duration_min, start_iso, end_iso, staff_id, status, created_at, square_booking_id)
VALUES (@id, @customer_name, @customer_phone, @customer_square_id, @service, @duration_min, @start_iso, @end_iso, @staff_id, @status, @created_at, @square_booking_id)`)
const getSession = db.prepare(`SELECT * FROM sessions WHERE phone=@phone`)
const upsertSession = db.prepare(`
INSERT INTO sessions (phone, data_json, updated_at) VALUES (@phone, @data_json, @updated_at)
ON CONFLICT(phone) DO UPDATE SET data_json=excluded.data_json, updated_at=excluded.updated_at`)
const clearSession = db.prepare(`DELETE FROM sessions WHERE phone=@phone`)

// ===== Disponibilidad local
function getBookedIntervals(fromIso, toIso) {
  const rows = db.prepare(`SELECT start_iso, end_iso, staff_id FROM appointments WHERE status='confirmed' AND start_iso < @to AND end_iso > @from`).all({
    from: fromIso, to: toIso
  })
  return rows.map(r => ({ start: dayjs(r.start_iso), end: dayjs(r.end_iso), staff_id: r.staff_id }))
}
function staffHasFree(intervals, start, end) {
  const ids = TEAM_MEMBER_IDS.length ? TEAM_MEMBER_IDS : ["any"]
  for (const id of ids) {
    const busy = intervals.filter(i => i.staff_id === id).some(i => (start < i.end) && (i.start < end))
    if (!busy) return true
  }
  return false
}
function suggestOrExact(startEU, durationMin) {
  const now = dayjs().tz(EURO_TZ).add(30,"minute").second(0).millisecond(0)
  const from = now.tz("UTC").toISOString()
  const to   = now.add(14,"day").tz("UTC").toISOString()
  const intervals = getBookedIntervals(from, to)

  // Validar horario/diario
  const endEU = startEU.clone().add(durationMin,"minute")
  const dow = startEU.day()===0 ? 7 : startEU.day()
  const insideHours = startEU.hour()>=OPEN_HOUR && (endEU.hour()<CLOSE_HOUR || (endEU.hour()===CLOSE_HOUR && endEU.minute()===0))
  if (dow===7 || !WORK_DAYS.includes(dow) || !insideHours || startEU.isBefore(now)) {
    // sugerir: primer slot libre ese día
    const dayStart = startEU.clone().hour(OPEN_HOUR).minute(0).second(0).millisecond(0)
    const dayEnd   = startEU.clone().hour(CLOSE_HOUR).minute(0).second(0)
    for (let t = dayStart.clone(); !t.isAfter(dayEnd); t = t.add(SLOT_MIN,"minute")) {
      const e = t.clone().add(durationMin,"minute")
      if (t.isBefore(now)) continue
      if (staffHasFree(intervals, t.tz("UTC"), e.tz("UTC"))) return { exact:null, suggestion:t }
    }
    return { exact:null, suggestion:null }
  }
  // Exacto libre?
  if (staffHasFree(intervals, startEU.tz("UTC"), endEU.tz("UTC"))) return { exact:startEU, suggestion:null }

  // buscar lo más cercano el mismo día
  const dayStart = startEU.clone().hour(OPEN_HOUR).minute(0).second(0).millisecond(0)
  const dayEnd   = startEU.clone().hour(CLOSE_HOUR).minute(0).second(0)
  for (let t = dayStart.clone(); !t.isAfter(dayEnd); t = t.add(SLOT_MIN,"minute")) {
    const e = t.clone().add(durationMin,"minute")
    if (t.isBefore(now)) continue
    if (staffHasFree(intervals, t.tz("UTC"), e.tz("UTC"))) return { exact:null, suggestion:t }
  }
  return { exact:null, suggestion:null }
}

// ===== Web mínimo
const app = express()
const PORT = process.env.PORT || 8080
let lastQR = null, conectado = false

app.get("/", (_req,res)=>{
  res.send(`<!doctype html><meta charset="utf-8"><style>body{font-family:system-ui;display:grid;place-items:center;min-height:100vh;background:linear-gradient(135deg,#fce4ec,#f8bbd0);color:#4a148c} .card{background:#fff;padding:24px;border-radius:16px;box-shadow:0 6px 24px rgba(0,0,0,.08);text-align:center;max-width:520px}</style><div class="card"><h1>Gapink Nails</h1><p>Estado: ${conectado?"✅ Conectado":"❌ Desconectado"}</p>${!conectado&&lastQR?`<img src="/qr.png" width="320" />`:``}<p><small>Desarrollado por Gonzalo</small></p></div>`)
})
app.get("/qr.png", async (_req,res)=>{
  if (!lastQR) return res.status(404).send("No hay QR")
  const png = await qrcode.toBuffer(lastQR, { type:"png", width: 512, margin:1 })
  res.set("Content-Type","image/png").send(png)
})

app.listen(PORT, async ()=>{
  console.log(`🌐 Web en puerto ${PORT}`)
  await squareCheckCredentials()
  startBot().catch(console.error)
})

// ===== Bot
async function startBot() {
  console.log("🚀 Bot arrancando…")
  if (!fs.existsSync("auth_info")) fs.mkdirSync("auth_info",{recursive:true})
  const { state, saveCreds } = await useMultiFileAuthState("auth_info")
  const { version } = await fetchLatestBaileysVersion()
  const sock = makeWASocket({
    logger: pino({ level:"silent" }),
    printQRInTerminal: false,
    auth: state,
    version,
    browser: Browsers.macOS("Desktop"),
    syncFullHistory: false,
    connectTimeoutMs: 30000
  })
  sock.ev.on("connection.update", ({ connection, lastDisconnect, qr })=>{
    if (qr){ lastQR = qr; conectado=false; try{ qrcodeTerminal.generate(qr,{small:true}) }catch{} }
    if (connection==="open"){ lastQR=null; conectado=true; console.log("✅ Conectado a WhatsApp") }
    if (connection==="close"){ conectado=false; console.log("❌ Cerrado:", lastDisconnect?.error?.message || ""); setTimeout(()=>startBot().catch(console.error), 3000) }
  })
  sock.ev.on("creds.update", saveCreds)

  const safeSend = async (jid, content)=>{ try{ await sock.sendMessage(jid, content) } catch(e){ console.error("sendMessage:", e) } }

  sock.ev.on("messages.upsert", async ({ messages })=>{
    const m = messages?.[0]; if (!m?.message || m.key.fromMe) return
    const from = m.key.remoteJid
    const phone = normalizePhoneES((from||"").split("@")[0] || "") || (from||"").split("@")[0] || ""
    const body =
      m.message.conversation ||
      m.message.extendedTextMessage?.text ||
      m.message?.imageMessage?.caption || ""
    const textRaw = (body || "").trim()

    // Cargar sesión
    const sessRow = getSession.get({ phone })
    const data = sessRow?.data_json ? JSON.parse(sessRow.data_json) : {
      // campos que vamos rellenando
      service: null,
      startEU: null,
      durationMin: null,
      name: null,
      email: null,
      confirmAsked: false,
      confirmApproved: false
    }

    // 1) Extrae intención y datos con IA
    const extra = await extractFromText(textRaw)
    // aplica al contexto (sin forzar)
    if (!data.service) data.service = extra.service || detectServiceFree(textRaw) || data.service
    if (!data.name && extra.name) data.name = extra.name
    if (!data.email && extra.email) data.email = extra.email

    // confirmación libre
    const saidYes = YES_RE.test(textRaw)
    const saidNo  = NO_RE.test(textRaw)
    if (extra.confirm === "yes") data.confirmApproved = true
    if (saidYes) data.confirmApproved = true
    if (saidNo) { data.confirmApproved = false; data.confirmAsked = false }

    // fecha/hora
    const whenText = extra.datetime_text || textRaw
    const parsed = parseDateTimeES(whenText)
    if (parsed) data.startEU = parsed

    // si tenemos servicio, fija duración
    if (data.service && !data.durationMin) data.durationMin = SERVICES[data.service] || 60

    // 2) Lógica de decisión
    // ¿Quiere cancelar?
    if ((extra.intent==="cancel") || /cancel(ar)? cita/i.test(textRaw)) {
      // (mantén tu lógica de cancelación aquí si ya usas DB de citas futuras)
      await safeSend(from,{ text: "He anotado la cancelación. Si quieres, dime otra fecha y te doy hueco." })
      clearSession.run({ phone }); return
    }

    // ¿Tenemos servicio + hora? → comprobar exacto/sugerir
    if (data.service && data.startEU && data.durationMin) {
      const { exact, suggestion } = suggestOrExact(data.startEU, data.durationMin)
      if (exact) {
        data.startEU = exact
        // Si ya confirmó, reservar directo
        if (data.confirmApproved) {
          await finalizeBooking({ from, phone, data, safeSend })
          return
        }
        // Pedir confirmación (solo una vez por propuesta)
        data.confirmAsked = true
        upsertSession.run({ phone, data_json: JSON.stringify(data), updated_at: new Date().toISOString() })
        await safeSend(from,{ text: `Tengo libre ${fmtES(data.startEU)} para ${data.service}. ¿Confirmo la cita?` })
        return
      }
      if (suggestion) {
        data.startEU = suggestion
        data.confirmAsked = true
        upsertSession.run({ phone, data_json: JSON.stringify(data), updated_at: new Date().toISOString() })
        await safeSend(from,{ text: `No tengo ese hueco exacto. Te puedo ofrecer ${fmtES(data.startEU)}. ¿Confirmo?` })
        return
      }
      // Sin hueco ese día
      data.confirmAsked = false
      upsertSession.run({ phone, data_json: JSON.stringify(data), updated_at: new Date().toISOString() })
      await safeSend(from,{ text: "No veo hueco en esa franja. Dime otra hora o día y te digo." })
      return
    }

    // Si dijo “sí” pero falta nombre/email → pedirlos y reservar directo al recibirlos
    if (data.confirmApproved && (!data.name || !data.email)) {
      upsertSession.run({ phone, data_json: JSON.stringify(data), updated_at: new Date().toISOString() })
      await safeSend(from,{ text: "Para cerrar, dime tu nombre y email (ej: “Ana Pérez, ana@correo.com”)." })
      return
    }
    if (data.confirmApproved && data.name && data.email && data.service && data.startEU) {
      await finalizeBooking({ from, phone, data, safeSend })
      return
    }

    // 3) Si falta algo, deja que la IA hable natural y pida lo que falte
    const missing = []
    if (!data.service) missing.push("servicio")
    if (!data.startEU) missing.push("día y hora")
    if (!data.name || !data.email) missing.push("nombre y email (si eres nuevo)")

    let prompt = `Contexto:
- Servicio: ${data.service || "?"}
- Fecha/Hora: ${data.startEU ? fmtES(data.startEU) : "?"}
- Nombre: ${data.name || "?"}
- Email: ${data.email || "?"}
Necesito que escribas un único mensaje breve y humano para el cliente que avance la reserva.
Si faltan datos (${missing.join(", ")}), pídelos amablemente y con ejemplo.
Si ya tenemos todo menos confirmación, pregunta si confirmamos.
Mensaje del cliente: "${textRaw}"`

    const say = await aiSay(prompt)
    upsertSession.run({ phone, data_json: JSON.stringify(data), updated_at: new Date().toISOString() })
    await safeSend(from,{ text: say || "¿Qué servicio necesitas y para cuándo?" })
  })
}

// ===== Reserva final
async function finalizeBooking({ from, phone, data, safeSend }) {
  try {
    // Cliente Square
    let customer = await squareFindCustomerByPhone(phone)
    if (!customer) {
      if (!data.name || !data.email) {
        await safeSend(from,{ text: "Me falta tu nombre y email para crear la reserva." })
        return
      }
      customer = await squareCreateCustomer({ givenName: data.name, emailAddress: data.email, phoneNumber: phone })
    }
    if (!customer) { await safeSend(from,{ text: "No pude crear la ficha ahora mismo. Probamos en un minuto." }); return }

    const teamMemberId = TEAM_MEMBER_IDS[0] || null
    const booking = await createSquareBooking({
      startEU: data.startEU,
      serviceKey: data.service,
      customerId: customer.id,
      teamMemberId
    })
    if (!booking?.booking) { await safeSend(from,{ text: "No pude cerrar la cita ahora mismo. ¿Probamos de nuevo?" }); return }

    const id = `apt_${Math.random().toString(36).slice(2,8)}${Date.now().toString(36).slice(-4)}`
    insertAppt.run({
      id,
      customer_name: data.name || customer?.givenName || null,
      customer_phone: phone,
      customer_square_id: customer.id,
      service: data.service,
      duration_min: SERVICES[data.service],
      start_iso: booking.startISO,
      end_iso: dayjs(booking.startISO).add(SERVICES[data.service], "minute").toISOString(),
      staff_id: teamMemberId,
      status: "confirmed",
      created_at: new Date().toISOString(),
      square_booking_id: booking.booking.id || null
    })

    clearSession.run({ phone })
    await safeSend(from,{ text:
`Reserva confirmada.
Servicio: ${data.service}
Fecha: ${fmtES(data.startEU)}
Duración: ${SERVICES[data.service]} min
Pago en persona.` })
  } catch (e) {
    console.error("finalizeBooking:", e)
    await safeSend(from,{ text: "Ha fallado el cierre de la reserva. Lo miramos y te aviso, ¿vale?" })
  }
}
