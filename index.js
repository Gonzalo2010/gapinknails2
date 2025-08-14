// index.js — Gapink Nails WhatsApp Bot (v2 fallback)
// Novedades clave:
// - Modo sin Square (ENV SQUARE_BOOKINGS=off): confirma en SQLite y listo.
// - Servicios por ENV (SERVICES_FALLBACK='{"uñas acrílicas":90,"manicura":60}') si Square no tiene ninguno.
// - Mapeo de staff por texto ("con Gonzalo") -> ENV SQ_STAFF_MAP='gonzalo:TMEMB1,maría:TMEMB2'
// - Detección de staff preferido y respeto de TEAM_MEMBER_IDS
// - Manejo robusto cuando Square devuelve 404 en retrieveCatalogObject

import express from "express"
import baileys from "@whiskeysockets/baileys"
import pino from "pino"
import qrcode from "qrcode"
import qrcodeTerminal from "qrcode-terminal"
import "dotenv/config"
import fs from "fs"
import { webcrypto, createHash } from "crypto"
import Database from "better-sqlite3"
import dayjs from "dayjs"
import utc from "dayjs/plugin/utc.js"
import tz from "dayjs/plugin/timezone.js"
import "dayjs/locale/es.js"
import { Client, Environment } from "square"

// ===== Config global
if (!globalThis.crypto) globalThis.crypto = webcrypto
dayjs.extend(utc); dayjs.extend(tz); dayjs.locale("es")
const EURO_TZ = "Europe/Madrid"

const { makeWASocket, useMultiFileAuthState, fetchLatestBaileysVersion, Browsers } = baileys

// ===== Horarios negocio
const WORK_DAYS = [1,2,3,4,5,6] // Lunes a sábado
const OPEN_HOUR  = 10
const CLOSE_HOUR = 20
const SLOT_MIN   = 30

// ===== Feature flags por ENV
const SQUARE_BOOKINGS = String(process.env.SQUARE_BOOKINGS || "on").toLowerCase() !== "off"

// ===== Servicios dinámicos
let SERVICES = {}              // {nombre: duración_min}
let SERVICE_VARIATIONS = {}    // {nombre: square_variation_id}
let SERVICE_KEYWORDS = {}      // {nombre: [palabras_clave]}

// TEAM MEMBERS permitidos (para disponibilidad local)
const TEAM_MEMBER_IDS = (process.env.SQ_TEAM_IDS || "")
  .split(",").map(s=>s.trim()).filter(Boolean)

// Mapa nombre->team_member_id para frases tipo "con Gonzalo"
const STAFF_NAME_MAP = (() => {
  const raw = (process.env.SQ_STAFF_MAP || "").trim() // "gonzalo:TM1,maría:TM2"
  const map = {}
  raw.split(",").map(s=>s.trim()).filter(Boolean).forEach(pair=>{
    const [name,id] = pair.split(":").map(x=>x?.trim())
    if (name && id) map[name.toLowerCase()] = id
  })
  return map
})()

// ===== DeepSeek
const DEEPSEEK_API_KEY = process.env.DEEPSEEK_API_KEY
const DEEPSEEK_API_URL = process.env.DEEPSEEK_API_URL || "https://api.deepseek.com/chat/completions"
const DEEPSEEK_MODEL   = process.env.DEEPSEEK_MODEL   || "deepseek-chat"

// ===== Square
const square = new Client({ 
  accessToken: process.env.SQUARE_ACCESS_TOKEN, 
  environment: process.env.SQUARE_ENV === "production" ? Environment.Production : Environment.Sandbox 
})
const locationId = process.env.SQUARE_LOCATION_ID
let LOCATION_TZ = EURO_TZ

// ===== Utilidades
const onlyDigits = (s="") => (s||"").replace(/\D+/g,"")
const rmDiacritics = (s="") => s.normalize("NFD").replace(/\p{Diacritic}/gu,"")
const YES_RE = /\b(si|sí|ok|vale|confirmo|confirmar|de acuerdo|perfecto)\b/i
const NO_RE  = /\b(no|otra|cambia|no confirmo|mejor mas tarde|mejor más tarde)\b/i

function normalizePhoneES(raw) {
  const d = onlyDigits(raw)
  if (!d) return null
  if (raw.startsWith("+") && d.length >= 8 && d.length <= 15) return `+${d}`
  if (d.startsWith("34") && d.length === 11) return `+${d}`
  if (d.length === 9) return `+34${d}`
  if (d.startsWith("00")) return `+${d.slice(2)}`
  return `+${d}`
}

function generateServiceKeywords(serviceName) {
  const keywords = [serviceName]
  const words = serviceName.toLowerCase().split(/[\s\-_]+/)
  keywords.push(...words)
  const lower = serviceName.toLowerCase()
  if (lower.includes("uñas") || lower.includes("nail")) {
    keywords.push("uñas","nails","manicura","pedicura")
  }
  if (lower.includes("acrílicas") || lower.includes("acrilicas")) {
    keywords.push("acrílicas","acrilicas","acrylic")
  }
  if (lower.includes("gel")) keywords.push("gel","gelish")
  if (lower.includes("semipermanente")) keywords.push("semipermanente","semi")
  if (lower.includes("extensión") || lower.includes("extension")) keywords.push("extensión","extension","alargamiento")
  return [...new Set(keywords.filter(k=>k && k.length>2))]
}

function detectServiceFree(text="") {
  const low = rmDiacritics(text.toLowerCase())
  for (const [serviceName, keywords] of Object.entries(SERVICE_KEYWORDS)) {
    for (const keyword of keywords) {
      if (low.includes(rmDiacritics(keyword.toLowerCase()))) return serviceName
    }
  }
  for (const serviceName of Object.keys(SERVICES)) {
    if (low.includes(rmDiacritics(serviceName.toLowerCase()))) return serviceName
  }
  return null
}

function detectPreferredStaff(text="") {
  const low = rmDiacritics(text.toLowerCase())
  for (const name in STAFF_NAME_MAP) {
    if (low.includes(rmDiacritics(name))) return STAFF_NAME_MAP[name]
  }
  return null
}

function parseDateTimeES(dtText) {
  if (!dtText) return null
  const t = rmDiacritics(dtText.toLowerCase())
  let base = null
  if (/\bhoy\b/.test(t)) base = dayjs().tz(EURO_TZ)
  else if (/\bmanana\b/.test(t)) base = dayjs().tz(EURO_TZ).add(1,"day")

  if (!base) {
    const M = {enero:1,febrero:2,marzo:3,abril:4,mayo:5,junio:6,julio:7,agosto:8,septiembre:9,setiembre:9,octubre:10,noviembre:11,diciembre:12,ene:1,feb:2,mar:3,abr:4,may:5,jun:6,jul:7,ago:8,sep:9,oct:10,nov:11,dic:12}
    const m = t.match(/\b(\d{1,2})\s+de\s+(enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|setiembre|octubre|noviembre|diciembre|ene|feb|mar|abr|may|jun|jul|ago|sep|oct|nov|dic)\b(?:\s+de\s+(\d{4}))?/)
    if (m) {
      const dd = +m[1], mm = M[m[2]], yy = m[3] ? +m[3] : dayjs().tz(EURO_TZ).year()
      base = dayjs.tz(`${yy}-${String(mm).padStart(2,"0")}-${String(dd).padStart(2,"0")} 00:00`, EURO_TZ)
    }
  }
  if (!base) {
    const m = t.match(/\b(\d{1,2})[\/\-](\d{1,2})(?:[\/\-](\d{2,4}))?\b/)
    if (m) {
      let yy = m[3] ? +m[3] : dayjs().tz(EURO_TZ).year()
      if (yy < 100) yy += 2000
      base = dayjs.tz(`${yy}-${String(+m[2]).padStart(2,"0")}-${String(+m[1]).padStart(2,"0")} 00:00`, EURO_TZ)
    }
  }
  if (!base) base = dayjs().tz(EURO_TZ)
  let hour = null, minute = 0
  const hm = t.match(/(\d{1,2})(?::|h)?(\d{2})?\s*(am|pm)?\b/)
  if (hm) {
    hour = +hm[1]; minute = hm[2] ? +hm[2] : 0
    const ap = hm[3]
    if (ap === "pm" && hour < 12) hour += 12
    if (ap === "am" && hour === 12) hour = 0
  }
  if (hour === null) return null
  return base.hour(hour).minute(minute).second(0).millisecond(0)
}

const fmtES = (d) => {
  const t = d.tz(EURO_TZ)
  const dias = ["domingo","lunes","martes","miércoles","jueves","viernes","sábado"]
  const DD = String(t.date()).padStart(2,"0")
  const MM = String(t.month()+1).padStart(2,"0") 
  const HH = String(t.hour()).padStart(2,"0")
  const mm = String(t.minute()).padStart(2,"0")
  return `${dias[t.day()]} ${DD}/${MM} ${HH}:${mm}`
}

// ===== DeepSeek calls
async function dsChat(messages, { temperature=0.4 } = {}) {
  try {
    const controller = new AbortController()
    const timeout = setTimeout(() => controller.abort(), 15000)
    const r = await fetch(DEEPSEEK_API_URL, {
      method: "POST",
      headers: { "Content-Type":"application/json", "Authorization":`Bearer ${DEEPSEEK_API_KEY}` },
      body: JSON.stringify({ model: DEEPSEEK_MODEL, messages, temperature }),
      signal: controller.signal
    })
    clearTimeout(timeout)
    if (!r.ok) {
      const errorText = await r.text()
      throw new Error(`DeepSeek ${r.status}: ${errorText}`)
    }
    const j = await r.json()
    return (j?.choices?.[0]?.message?.content || "").trim()
  } catch (e) { 
    console.error("DeepSeek error:", e?.message || e)
    return "" 
  }
}
function buildSysTone() {
  const serviceNames = Object.keys(SERVICES)
  const serviceList = serviceNames.map(name => `"${name}"`).join(", ")
  return `Eres el asistente de WhatsApp de Gapink Nails (España).
Habla natural, breve y sin emojis. No digas que eres IA.
Servicios disponibles: ${serviceList}
Si la hora pedida está libre, ofrécela tal cual; si no, propone la más cercana y pide confirmación.
No ofrezcas elegir profesional. Pago siempre en persona.`
}
async function extractFromText(userText="") {
  const serviceNames = Object.keys(SERVICES)
  const serviceOptions = serviceNames.map(name => `"${name}"`).join("|")
  const schema = `
Devuelve SOLO un JSON válido (omite claves que no apliquen):
{
  "intent": "greeting|booking|cancel|reschedule|other",
  "service": "${serviceOptions}",
  "datetime_text": "texto con fecha/hora si lo hay",
  "confirm": "yes|no|unknown",
  "name": "si aparece",
  "email": "si aparece",
  "polite_reply": "respuesta breve y natural para avanzar"
}`
  const content = await dsChat([
    { role: "system", content: `${buildSysTone()}\n${schema}\nUsa español de España.` },
    { role: "user", content: userText }
  ], { temperature: 0.2 })
  try {
    const jsonStr = content.trim().replace(/^```(json)?/i,"").replace(/```$/,"")
    return JSON.parse(jsonStr)
  } catch { 
    return { intent:"other", polite_reply:"" } 
  }
}
async function aiSay(contextSummary) {
  return await dsChat([
    { role:"system", content: buildSysTone() },
    { role:"user", content: contextSummary }
  ], { temperature: 0.35 })
}

// ===== Square helpers (opcionales)
async function squareCheckCredentials() {
  try {
    const locs = await square.locationsApi.listLocations()
    const loc = (locs.result.locations||[]).find(l=>l.id===locationId) || (locs.result.locations||[])[0]
    if (loc?.timezone) LOCATION_TZ = loc.timezone
    console.log(`✅ Square listo. Location ${locationId}, TZ=${LOCATION_TZ}`)
    await loadServicesFromSquare()
  } catch(e) {
    console.error("⛔ Square:",e?.message||e)
  }
}
async function loadServicesFromSquare() {
  try {
    console.log("🔄 Cargando servicios desde Square...")
    const catalogResp = await square.catalogApi.listCatalog(undefined, "ITEM_VARIATION")
    const variations = catalogResp?.result?.objects || []
    const serviceVariations = variations.filter(obj => 
      obj.type === "ITEM_VARIATION" &&
      obj.itemVariationData?.itemId &&
      obj.itemVariationData?.serviceVariationData
    )
    console.log(`📋 Encontradas ${serviceVariations.length} variaciones de servicio`)
    SERVICES = {}; SERVICE_VARIATIONS = {}; SERVICE_KEYWORDS = {}
    for (const variation of serviceVariations) {
      const name = variation.itemVariationData?.name || "Servicio sin nombre"
      const serviceData = variation.itemVariationData?.serviceVariationData
      const durationMin = Number(serviceData?.durationMinutes || 60)
      const teamMemberIds = serviceData?.teamMemberIds || []
      const hasValidTeamMember = teamMemberIds.some(id => TEAM_MEMBER_IDS.includes(id)) || TEAM_MEMBER_IDS.length === 0
      if (hasValidTeamMember) {
        SERVICES[name] = durationMin
        SERVICE_VARIATIONS[name] = variation.id
        SERVICE_KEYWORDS[name] = generateServiceKeywords(name)
        console.log(`✅ Servicio cargado: ${name} (${durationMin}min) - ID: ${variation.id}`)
      } else {
        console.log(`⏭️ Omitido (sin team members válidos): ${name}`)
      }
    }

    // Si Square no tiene ninguno → intentar ENV SERVICES_FALLBACK
    if (Object.keys(SERVICES).length === 0) {
      console.warn("⚠️ No se encontraron servicios válidos en Square.")
      const envJson = process.env.SERVICES_FALLBACK || ""
      if (envJson) {
        try {
          const parsed = JSON.parse(envJson) // {"uñas acrílicas":90,"manicura":60}
          for (const [n, mins] of Object.entries(parsed)) {
            SERVICES[n] = Number(mins) || 60
            SERVICE_VARIATIONS[n] = null // no hay en Square
            SERVICE_KEYWORDS[n] = generateServiceKeywords(n)
          }
          console.log(`🪄 Cargados ${Object.keys(SERVICES).length} servicios desde SERVICES_FALLBACK`)
        } catch(e) {
          console.error("❌ SERVICES_FALLBACK inválido:", e?.message || e)
        }
      }
      if (Object.keys(SERVICES).length === 0) {
        // último recurso
        SERVICES = { "uñas acrílicas": 90 }
        SERVICE_VARIATIONS = { "uñas acrílicas": null }
        SERVICE_KEYWORDS = { "uñas acrílicas": ["uñas","acrílicas","acrilicas","nails","manicura"] }
        console.log("✨ Usando configuración por defecto local.")
      }
    }
  } catch (error) {
    console.error("❌ Error cargando servicios desde Square:", error?.message || error)
    // Fallback duro
    SERVICES = { "uñas acrílicas": 90 }
    SERVICE_VARIATIONS = { "uñas acrílicas": null }
    SERVICE_KEYWORDS = { "uñas acrílicas": ["uñas","acrílicas","acrilicas","nails","manicura"] }
  }
}
async function squareFindCustomerByPhone(phoneRaw) {
  try {
    const e164 = normalizePhoneES(phoneRaw)
    if (!e164 || !e164.startsWith("+") || e164.length < 8 || e164.length > 16) return null
    const resp = await square.customersApi.searchCustomers({
      query: { filter: { phoneNumber: { exact: e164 } } }
    })
    return (resp?.result?.customers||[])[0] || null
  } catch(e) {
    console.error("Square search:",e?.message||e)
    return null
  }
}
async function squareCreateCustomer({givenName, emailAddress, phoneNumber}) {
  try {
    const phone = normalizePhoneES(phoneNumber)
    const resp = await square.customersApi.createCustomer({
      idempotencyKey: `cust_${Date.now()}_${Math.random().toString(36).slice(2,8)}`,
      givenName, emailAddress, phoneNumber: phone || undefined,
      note: "Creado desde bot WhatsApp Gapink Nails"
    })
    return resp?.result?.customer || null
  } catch(e) {
    console.error("Square create:",e?.message||e)
    return null
  }
}
async function getServiceVariationVersion(id) {
  try {
    if (!id) return undefined
    const resp = await square.catalogApi.retrieveCatalogObject(id, true)
    return resp?.result?.object?.version
  } catch(e) {
    console.error("getServiceVariationVersion:", e?.message || e)
    return undefined
  }
}
function stableKey({locationId, serviceVariationId, startISO, customerId}) {
  const raw = `${locationId}|${serviceVariationId}|${startISO}|${customerId}`
  return createHash("sha256").update(raw).digest("hex").slice(0,48)
}
async function createSquareBooking({startEU, serviceKey, customerId, teamMemberId}) {
  if (!SQUARE_BOOKINGS) return { id: `local_${Date.now()}` } // modo local
  try {
    const serviceVariationId = SERVICE_VARIATIONS[serviceKey]
    if (!serviceVariationId || !teamMemberId || !locationId) return null
    const version = await getServiceVariationVersion(serviceVariationId)
    if (!version) return null
    const startISO = startEU.tz("UTC").toISOString()
    const body = {
      idempotencyKey: stableKey({locationId, serviceVariationId, startISO, customerId}),
      booking: {
        locationId,
        startAt: startISO,
        customerId,
        appointmentSegments: [{
          teamMemberId,
          serviceVariationId,
          serviceVariationVersion: Number(version),
          durationMinutes: SERVICES[serviceKey]
        }]
      }
    }
    const resp = await square.bookingsApi.createBooking(body)
    return resp?.result?.booking || null
  } catch(e) {
    console.error("createSquareBooking:", e?.message || e)
    return null
  }
}

// ===== DB local (SQLite)
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
  status TEXT,            -- pending|confirmed|cancelled
  created_at TEXT,
  square_booking_id TEXT
);
CREATE TABLE IF NOT EXISTS sessions (
  phone TEXT PRIMARY KEY,
  data_json TEXT,
  updated_at TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_slot
ON appointments(staff_id, start_iso)
WHERE status IN ('pending','confirmed');
CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_customer_slot
ON appointments(customer_phone, start_iso)
WHERE status IN ('pending','confirmed');
`)
const insertAppt = db.prepare(`INSERT INTO appointments
(id, customer_name, customer_phone, customer_square_id, service, duration_min, start_iso, end_iso, staff_id, status, created_at, square_booking_id)
VALUES (@id, @customer_name, @customer_phone, @customer_square_id, @service, @duration_min, @start_iso, @end_iso, @staff_id, @status, @created_at, @square_booking_id)`)
const updateAppt = db.prepare(`UPDATE appointments SET status=@status, square_booking_id=@square_booking_id WHERE id=@id`)
const deleteAppt = db.prepare(`DELETE FROM appointments WHERE id=@id`)
const getSessionRow = db.prepare(`SELECT * FROM sessions WHERE phone=@phone`)
const upsertSession = db.prepare(`INSERT INTO sessions (phone, data_json, updated_at)
VALUES (@phone, @data_json, @updated_at)
ON CONFLICT(phone) DO UPDATE SET data_json=excluded.data_json, updated_at=excluded.updated_at`)
const clearSession = db.prepare(`DELETE FROM sessions WHERE phone=@phone`)

function loadSession(phone) {
  const row = getSessionRow.get({phone})
  if (!row?.data_json) return null
  const raw = JSON.parse(row.data_json)
  const data = {...raw}
  if (raw.startEUISO) data.startEU = dayjs.tz(raw.startEUISO, EURO_TZ)
  return data
}
function saveSession(phone, data) {
  const s = {...data}
  s.startEUISO = data.startEU?.toISOString ? data.startEU.toISOString() : (data.startEUISO || null)
  delete s.startEU
  upsertSession.run({ phone, data_json: JSON.stringify(s), updated_at: new Date().toISOString() })
}

// ===== Disponibilidad local
function getBookedIntervals(fromIso, toIso) {
  const rows = db.prepare(`SELECT start_iso, end_iso, staff_id, customer_phone FROM appointments 
    WHERE status IN ('pending','confirmed') AND start_iso < @to AND end_iso > @from`)
    .all({from: fromIso, to: toIso})
  return rows.map(r => ({
    start: dayjs(r.start_iso),
    end: dayjs(r.end_iso),
    staff_id: r.staff_id,
    customer_phone: r.customer_phone
  }))
}
function staffHasFree(intervals, start, end, customerPhone = null, preferredStaffId = null) {
  const idsBase = TEAM_MEMBER_IDS.length ? TEAM_MEMBER_IDS : ["any"]
  const ids = preferredStaffId ? [preferredStaffId] : idsBase
  for (const id of ids) {
    const staffBusy = intervals.filter(i => i.staff_id === id)
      .some(i => (start < i.end) && (i.start < end))
    const customerBusy = customerPhone ? intervals
      .filter(i => i.customer_phone === customerPhone)
      .some(i => (start < i.end) && (i.start < end)) : false
    if (!staffBusy && !customerBusy) return id
  }
  return null
}
function suggestOrExact(startEU, durationMin, customerPhone = null, preferredStaffId = null) {
  const now = dayjs().tz(EURO_TZ).add(30,"minute").second(0).millisecond(0)
  const from = now.tz("UTC").toISOString()
  const to = now.add(14,"day").tz("UTC").toISOString()
  const intervals = getBookedIntervals(from, to)

  const endEU = startEU.clone().add(durationMin,"minute")
  const dow = startEU.day() === 0 ? 7 : startEU.day()
  const insideHours = startEU.hour() >= OPEN_HOUR && 
    (endEU.hour() < CLOSE_HOUR || (endEU.hour() === CLOSE_HOUR && endEU.minute() === 0))
  if (dow === 7 || !WORK_DAYS.includes(dow) || !insideHours || startEU.isBefore(now)) {
    const dayStart = startEU.clone().hour(OPEN_HOUR).minute(0).second(0)
    const dayEnd = startEU.clone().hour(CLOSE_HOUR).minute(0).second(0)
    for (let t = dayStart.clone(); !t.isAfter(dayEnd); t = t.add(SLOT_MIN,"minute")) {
      const e = t.clone().add(durationMin,"minute")
      if (t.isBefore(now)) continue
      const freeId = staffHasFree(intervals, t.tz("UTC"), e.tz("UTC"), customerPhone, preferredStaffId)
      if (freeId) return {exact: null, suggestion: t, staffId: freeId}
    }
    return {exact: null, suggestion: null, staffId: null}
  }
  const freeExactId = staffHasFree(intervals, startEU.tz("UTC"), endEU.tz("UTC"), customerPhone, preferredStaffId)
  if (freeExactId) return {exact: startEU, suggestion: null, staffId: freeExactId}
  const dayStart = startEU.clone().hour(OPEN_HOUR).minute(0).second(0)
  const dayEnd = startEU.clone().hour(CLOSE_HOUR).minute(0).second(0)
  for (let t = dayStart.clone(); !t.isAfter(dayEnd); t = t.add(SLOT_MIN,"minute")) {
    const e = t.clone().add(durationMin,"minute")
    if (t.isBefore(now)) continue
    const freeId = staffHasFree(intervals, t.tz("UTC"), e.tz("UTC"), customerPhone, preferredStaffId)
    if (freeId) return {exact: null, suggestion: t, staffId: freeId}
  }
  return {exact: null, suggestion: null, staffId: null}
}

// ===== Web mínima
const app = express()
const PORT = process.env.PORT || 8080
let lastQR = null, conectado = false

app.get("/", (_req, res) => {
  const servicesList = Object.keys(SERVICES).map(name => 
    `<li>${name} (${SERVICES[name]}min)</li>`
  ).join('')
  res.send(`<!doctype html>
<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<style>
body{font-family:system-ui;display:grid;place-items:center;min-height:100vh;background:linear-gradient(135deg,#fce4ec,#f8bbd0);color:#4a148c;margin:0}
.card{background:#fff;padding:24px;border-radius:16px;box-shadow:0 6px 24px rgba(0,0,0,.08);text-align:center;max-width:520px;margin:20px}
.status{padding:8px 16px;border-radius:8px;font-weight:bold;margin:16px 0}
.connected{background:#e8f5e8;color:#2e7d32}.disconnected{background:#ffebee;color:#c62828}
.services{text-align:left;margin:16px 0}.services ul{padding-left:20px}
img{max-width:100%;height:auto}
.badge{display:inline-block;margin:4px 0;padding:4px 8px;border-radius:8px;background:#eee;color:#333;font-size:12px}
</style>
<div class="card">
  <h1>🌸 Gapink Nails Bot</h1>
  <div class="status ${conectado ? 'connected' : 'disconnected'}">
    ${conectado ? "✅ Conectado" : "❌ Desconectado"}
  </div>
  <div>
    <span class="badge">Square bookings: ${SQUARE_BOOKINGS ? "ON" : "OFF"}</span>
  </div>
  <div class="services">
    <h3>📋 Servicios Disponibles:</h3>
    <ul>${servicesList}</ul>
  </div>
  ${!conectado && lastQR ? `<div><img src="/qr.png" width="300" alt="QR Code" /></div>` : ""}
  <p><small>Bot de reservas WhatsApp</small></p>
  <p><small>Desarrollado por Gonzalo</small></p>
</div>`)
})
app.get("/qr.png", async(_req, res) => {
  if (!lastQR) return res.status(404).send("No hay QR disponible")
  try {
    const png = await qrcode.toBuffer(lastQR, { type: "png", width: 512, margin: 1 })
    res.set("Content-Type", "image/png").send(png)
  } catch(e) { res.status(500).send("Error generando QR") }
})
app.get("/health", (_req, res) => {
  res.json({ 
    status: "ok", connected: conectado,
    services: Object.keys(SERVICES).length,
    teamMembers: TEAM_MEMBER_IDS.length,
    squareBookings: SQUARE_BOOKINGS
  })
})
app.get("/api/services", (_req, res) => {
  res.json({ services: SERVICES, variations: SERVICE_VARIATIONS, keywords: SERVICE_KEYWORDS, teamMembers: TEAM_MEMBER_IDS })
})

// ===== Cola de mensajes
const wait = (ms) => new Promise(r => setTimeout(r, ms))

// ===== Reserva
async function finalizeBooking({ from, phone, data, safeSend }) {
  try {
    if (data.bookingInFlight) { console.log("Booking in flight:", phone); return }
    data.bookingInFlight = true; saveSession(phone, data)

    if (!SERVICES[data.service]) {
      await safeSend(from, { text: `El servicio "${data.service}" no está disponible. Disponibles: ${Object.keys(SERVICES).join(', ')}` })
      data.bookingInFlight = false; saveSession(phone, data); return
    }

    // Cliente Square (solo si SQUARE_BOOKINGS)
    let customer = null
    if (SQUARE_BOOKINGS) {
      customer = await squareFindCustomerByPhone(phone)
      if (!customer) {
        if (!data.name || !data.email) { 
          await safeSend(from, { text: "Me falta tu nombre y email para crear la reserva." })
          data.bookingInFlight = false; saveSession(phone, data); return 
        }
        customer = await squareCreateCustomer({ givenName: data.name, emailAddress: data.email, phoneNumber: phone })
      }
      if (!customer) {
        await safeSend(from, { text: "Ahora mismo no puedo crear tu ficha en Square. Probamos en un minuto o lo registro localmente." })
        data.bookingInFlight = false; saveSession(phone, data); return 
      }
    }

    const durationMin = SERVICES[data.service]
    const startUTC = data.startEU.tz("UTC")
    const endUTC = startUTC.clone().add(durationMin,"minute")
    const aptId = `apt_${Math.random().toString(36).slice(2,8)}${Date.now().toString(36).slice(-4)}`
    const teamMemberId = data.staffId || TEAM_MEMBER_IDS[0] || "any"

    // Anti-dobles
    const intervals = getBookedIntervals(startUTC.toISOString(), endUTC.toISOString())
    const freeId = staffHasFree(intervals, startUTC, endUTC, phone, teamMemberId)
    if (!freeId) {
      await safeSend(from, { text: "Ese horario se acaba de ocupar. Dime otra hora y te busco el siguiente disponible." })
      data.bookingInFlight = false; saveSession(phone, data); return
    }

    // Insertar pendiente
    try {
      insertAppt.run({ 
        id: aptId, 
        customer_name: data.name || customer?.givenName || null, 
        customer_phone: phone, 
        customer_square_id: customer?.id || null, 
        service: data.service, 
        duration_min: durationMin, 
        start_iso: startUTC.toISOString(), 
        end_iso: endUTC.toISOString(), 
        staff_id: freeId, 
        status: "pending", 
        created_at: new Date().toISOString(), 
        square_booking_id: null 
      })
    } catch (e) {
      if (String(e?.message||"").includes("UNIQUE")) {
        await safeSend(from, { text: "Ese hueco ya está cogido por ti o por otra persona. Dime otra hora." })
        data.bookingInFlight = false; saveSession(phone, data); return
      }
      throw e
    }

    // Crear reserva en Square (si ON). Si falla, seguimos local pero marcamos msg.
    let sq = null
    if (SQUARE_BOOKINGS) {
      sq = await createSquareBooking({ startEU: data.startEU, serviceKey: data.service, customerId: customer.id, teamMemberId: freeId })
      if (!sq) {
        // seguimos confirmando localmente
        console.warn("⚠️ No se pudo crear booking en Square; se deja confirmada local.")
      }
    }

    updateAppt.run({ id: aptId, status: "confirmed", square_booking_id: sq?.id || null })
    clearSession.run({ phone })
    
    await safeSend(from, { text:
`✅ Reserva confirmada

📅 ${fmtES(data.startEU)}
💅 ${data.service}
⏱️ Duración: ${durationMin} min
👤 ${data.name || "Cliente"}

${SQUARE_BOOKINGS && sq ? "🔗 Registrada en Square" : "🗂️ Registrada localmente"}
📍 Gapink Nails — Pago en persona

¡Te esperamos!` })

  } catch (e) {
    console.error("finalizeBooking error:", e)
  } finally {
    data.bookingInFlight = false
    try { saveSession(phone, data) } catch(e) { console.error("Save session error:", e) }
  }
}

// ===== Bot WhatsApp
async function startBot() {
  console.log("🚀 Bot arrancando…")
  try {
    if (!fs.existsSync("auth_info")) fs.mkdirSync("auth_info", {recursive: true})
    const { state, saveCreds } = await useMultiFileAuthState("auth_info")
    const { version } = await fetchLatestBaileysVersion()
    let isOpen = false, reconnecting = false
    const sock = makeWASocket({
      logger: pino({level: "silent"}),
      printQRInTerminal: false,
      auth: state,
      version,
      browser: Browsers.macOS("Desktop"),
      syncFullHistory: false,
      connectTimeoutMs: 30000,
      defaultQueryTimeoutMs: 20000
    })
    const outbox = []; let sending = false
    const __SAFE_SEND__ = (jid, content) => new Promise((resolve, reject) => {
      outbox.push({jid, content, resolve, reject}); processOutbox().catch(console.error)
    })
    async function processOutbox() {
      if (sending) return; sending = true
      while (outbox.length) {
        const {jid, content, resolve, reject} = outbox.shift()
        let guard = 0
        while (!isOpen && guard < 60) { await wait(1000); guard++ }
        if (!isOpen) { reject(new Error("WA not connected")); continue }
        let ok = false, err = null
        for (let attempt = 1; attempt <= 4; attempt++) {
          try { await sock.sendMessage(jid, content); ok = true; break }
          catch(e) {
            err = e; const msg = e?.data?.stack || e?.message || String(e)
            if (/Timed Out/i.test(msg) || /Boom/i.test(msg)) { await wait(500 * attempt); continue }
            await wait(400)
          }
        }
        if (ok) resolve(true)
        else { console.error("sendMessage failed:", err?.message || err); reject(err); }
      }
      sending = false
    }
    sock.ev.on("connection.update", async({connection, lastDisconnect, qr}) => {
      if (qr) {
        lastQR = qr; conectado = false
        try { qrcodeTerminal.generate(qr, {small: true}); console.log("📱 Escanea el QR para conectar WhatsApp") }
        catch(e) { console.error("Error showing QR:", e) }
      }
      if (connection === "open") { lastQR=null; conectado=true; isOpen=true; console.log("✅ Conectado a WhatsApp"); processOutbox().catch(console.error) }
      if (connection === "close") {
        conectado=false; isOpen=false
        const reason = lastDisconnect?.error?.message || String(lastDisconnect?.error || "")
        console.log("❌ Conexión cerrada:", reason)
        if (!reconnecting) {
          reconnecting=true; await wait(2000)
          try { await startBot() } catch(e) { console.error("Error restarting bot:", e) } finally { reconnecting=false }
        }
      }
    })
    sock.ev.on("creds.update", saveCreds)

    sock.ev.on("messages.upsert", async({messages}) => {
      try {
        const m = messages?.[0]
        if (!m?.message || m.key.fromMe) return
        const from = m.key.remoteJid
        const phone = normalizePhoneES((from||"").split("@")[0]||"") || (from||"").split("@")[0] || ""
        const body = m.message.conversation || m.message.extendedTextMessage?.text || m.message?.imageMessage?.caption || ""
        const textRaw = (body||"").trim()
        if (!textRaw) return

        console.log(`📱 ${phone}: ${textRaw}`)

        let data = loadSession(phone) || {
          service: null, startEU: null, durationMin: null,
          name: null, email: null, confirmApproved: false, confirmAsked: false,
          bookingInFlight: false, lastUserDtText: null, lastService: null, staffId: null
        }

        const extra = await extractFromText(textRaw)
        const incomingService = extra.service || detectServiceFree(textRaw)
        const incomingDt = extra.datetime_text || null
        const preferredStaff = detectPreferredStaff(textRaw)
        if (preferredStaff) data.staffId = preferredStaff

        if ((incomingService && incomingService !== data.lastService) || 
            (incomingDt && incomingDt !== data.lastUserDtText)) {
          data.confirmApproved = false; data.confirmAsked = false
        }
        if (!data.service) data.service = incomingService || data.service
        data.lastService = data.service || data.lastService

        if (!data.name && extra.name) data.name = extra.name
        if (!data.email && extra.email) data.email = extra.email

        if (YES_RE.test(textRaw) || extra.confirm === "yes") data.confirmApproved = true
        if (NO_RE.test(textRaw)  || extra.confirm === "no") { data.confirmApproved = false; data.confirmAsked = false }

        const whenText = extra.datetime_text || textRaw
        if (whenText) data.lastUserDtText = extra.datetime_text || data.lastUserDtText
        const parsed = parseDateTimeES(whenText)
        if (parsed) data.startEU = parsed

        if (data.service && !data.durationMin) data.durationMin = SERVICES[data.service] || 60
        saveSession(phone, data)

        // Si ya pidió confirmación y dice "sí"
        if (data.confirmAsked && data.confirmApproved && data.service && data.startEU && data.durationMin) {
          await finalizeBooking({ from, phone, data, safeSend: __SAFE_SEND__ })
          return
        }

        // Lista servicios
        if (/\b(servicios|que servicios|qué servicios|lista servicios)\b/i.test(textRaw)) {
          const serviceList = Object.keys(SERVICES).map(name => `• ${name} (${SERVICES[name]} min)`).join('\n')
          await __SAFE_SEND__(from, { text: `📋 Nuestros servicios:\n\n${serviceList}\n\n¿Cuál te interesa y para cuándo?` })
          return
        }

        // Cancelación
        if ((extra.intent === "cancel") || /cancel(ar)?\s+cita/i.test(textRaw)) {
          await __SAFE_SEND__(from, { text: "Cancelación anotada. Si quieres, dime otra fecha y te busco hueco." })
          clearSession.run({ phone }); return
        }

        // Comprobar disponibilidad
        if (data.service && data.startEU && data.durationMin) {
          if (!SERVICES[data.service]) {
            data.service = null; data.durationMin = null; saveSession(phone, data)
            const availableServices = Object.keys(SERVICES).join(', ')
            await __SAFE_SEND__(from, { text: `Ese servicio no está disponible. Tenemos: ${availableServices}. ¿Cuál prefieres?` })
            return
          }
          const { exact, suggestion, staffId } = suggestOrExact(data.startEU, data.durationMin, phone, data.staffId)
          if (exact) {
            data.startEU = exact; if (staffId) data.staffId = staffId
            if (data.confirmApproved) { saveSession(phone, data); await finalizeBooking({ from, phone, data, safeSend: __SAFE_SEND__ }); return }
            data.confirmAsked = true; saveSession(phone, data)
            await __SAFE_SEND__(from, { text: `Tengo libre ${fmtES(data.startEU)} para ${data.service}${data.staffId && data.staffId!=="any" ? " (personal asignado)" : ""}. ¿Confirmo la cita?` })
            return
          }
          if (suggestion) {
            data.startEU = suggestion; if (staffId) data.staffId = staffId
            data.confirmAsked = true; data.confirmApproved = false; saveSession(phone, data)
            await __SAFE_SEND__(from, { text: `No tengo ese hueco exacto. Te puedo ofrecer ${fmtES(data.startEU)}. ¿Te viene bien? Si es sí, responde "confirmo".` })
            return
          }
          data.confirmAsked = false; saveSession(phone, data)
          await __SAFE_SEND__(from, { text: "No veo hueco en esa franja. Dime otra hora o día y te digo." })
          return
        }

        // Falta nombre/email tras "sí"
        if (data.confirmApproved && (!data.name || !data.email) && SQUARE_BOOKINGS) {
          saveSession(phone, data)
          await __SAFE_SEND__(from, { text: "Para cerrar en Square, dime tu nombre y email (ej: \"Ana Pérez, ana@correo.com\")." })
          return
        }
        if (data.confirmApproved && (!SQUARE_BOOKINGS) && data.service && data.startEU) {
          await finalizeBooking({ from, phone, data, safeSend: __SAFE_SEND__ })
          return
        }
        if (data.confirmApproved && data.name && data.email && data.service && data.startEU) {
          await finalizeBooking({ from, phone, data, safeSend: __SAFE_SEND__ })
          return
        }

        // Faltan datos → IA
        const missing = []
        if (!data.service) missing.push("servicio")
        if (!data.startEU) missing.push("día y hora")
        if (SQUARE_BOOKINGS && (!data.name || !data.email)) missing.push("nombre y email (si eres nuevo)")
        const availableServices = Object.keys(SERVICES).join(', ')
        const prompt = `Contexto:
- Servicios disponibles: ${availableServices}
- Servicio elegido: ${data.service || "?"}
- Fecha/Hora: ${data.startEU ? fmtES(data.startEU) : "?"}
- Nombre: ${data.name || "?"}
- Email: ${data.email || "?"}
- Staff: ${data.staffId || "cualquiera"}

Escribe un único mensaje corto y humano que avance la reserva, sin emojis.
Si faltan datos (${missing.join(", ")}), pídelo con ejemplo.
Mensaje del cliente: "${textRaw}"`
        const say = await aiSay(prompt)
        saveSession(phone, data)
        await __SAFE_SEND__(from, { text: say || `Hola. Nuestros servicios son: ${availableServices}. ¿Cuál te interesa y para cuándo?` })
      } catch(e) {
        console.error("messages.upsert error:", e)
      }
    })
    
  } catch(e) {
    console.error("startBot error:", e)
    setTimeout(() => startBot().catch(console.error), 5000)
  }
}

// ===== Inicio app
app.listen(PORT, async() => {
  console.log(`🌐 Web servidor iniciado en puerto ${PORT}`)
  const requiredEnvs = ['DEEPSEEK_API_KEY']
  if (SQUARE_BOOKINGS) requiredEnvs.push('SQUARE_ACCESS_TOKEN','SQUARE_LOCATION_ID')
  const missing = requiredEnvs.filter(env => !process.env[env])
  if (missing.length > 0) {
    console.error('⛔ Faltan variables de entorno:', missing.join(', '))
    process.exit(1)
  }
  console.log(`✅ Configuración validada`)
  console.log(`📍 Square Location: ${process.env.SQUARE_LOCATION_ID || "(modo local)"}`)
  console.log(`👥 Team IDs: ${TEAM_MEMBER_IDS.length}`)
  console.log(`🧰 Square bookings: ${SQUARE_BOOKINGS ? "ON" : "OFF"}`)

  if (SQUARE_BOOKINGS) await squareCheckCredentials()
  else await loadServicesFromSquare() // esto cargará SERVICES_FALLBACK / default

  startBot().catch(console.error)
})

// Señales
process.on('uncaughtException', (error) => { console.error('Uncaught Exception:', error) })
process.on('unhandledRejection', (reason, promise) => { console.error('Unhandled Rejection at:', promise, 'reason:', reason) })
process.on('SIGTERM', () => { console.log('SIGTERM recibido, cerrando…'); db.close(); process.exit(0) })
process.on('SIGINT',  () => { console.log('SIGINT recibido, cerrando…');  db.close(); process.exit(0) })
