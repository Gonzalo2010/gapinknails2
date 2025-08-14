// index.js — Gapink Nails WhatsApp Bot
// DeepSeek + extracción JSON + TZ Europe/Madrid
// Confirmación fina (auto-reserva solo en exactos; sugeridos piden "¿confirmo?")
// FIX bucle: si hay propuesta previa y dice "sí/confirmo", reservar directo SIN recalcular
// Anti-dobles: idempotencia estable + lock pending + índice único (staff_id,start_iso)
// Baileys: cola con reintentos/backoff para "Timed Out"
// Servicios dinámicos desde Square + mejor detección IA
// Optimizado para Railway con mejor manejo de errores y conexiones

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

// Configuración global
if (!globalThis.crypto) globalThis.crypto = webcrypto
dayjs.extend(utc); dayjs.extend(tz); dayjs.locale("es")
const EURO_TZ = "Europe/Madrid"

const { makeWASocket, useMultiFileAuthState, fetchLatestBaileysVersion, Browsers } = baileys

// ===== Configuración de negocio
const WORK_DAYS = [1,2,3,4,5,6] // Lunes a sábado
const OPEN_HOUR  = 10
const CLOSE_HOUR = 20
const SLOT_MIN   = 30

// Variables globales para servicios dinámicos
let SERVICES = {} // {nombre: duración_minutos}
let SERVICE_VARIATIONS = {} // {nombre: square_variation_id}
let SERVICE_KEYWORDS = {} // {nombre: [palabras_clave]}
const TEAM_MEMBER_IDS = (process.env.SQ_TEAM_IDS || "").split(",").map(s=>s.trim()).filter(Boolean)

// ===== Configuración DeepSeek
const DEEPSEEK_API_KEY = process.env.DEEPSEEK_API_KEY
const DEEPSEEK_API_URL = process.env.DEEPSEEK_API_URL || "https://api.deepseek.com/chat/completions"
const DEEPSEEK_MODEL   = process.env.DEEPSEEK_MODEL   || "deepseek-chat"

// ===== Funciones DeepSeek
async function dsChat(messages, { temperature=0.4 } = {}) {
  try {
    const controller = new AbortController()
    const timeout = setTimeout(() => controller.abort(), 15000) // 15s timeout
    
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

// Sistema de prompts mejorado para servicios dinámicos
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

// Función mejorada de detección de servicios
function detectServiceFree(text="") {
  const low = rmDiacritics(text.toLowerCase())
  
  // Buscar por palabras clave específicas de cada servicio
  for (const [serviceName, keywords] of Object.entries(SERVICE_KEYWORDS)) {
    for (const keyword of keywords) {
      if (low.includes(rmDiacritics(keyword.toLowerCase()))) {
        return serviceName
      }
    }
  }
  
  // Buscar por nombre exacto del servicio
  for (const serviceName of Object.keys(SERVICES)) {
    if (low.includes(rmDiacritics(serviceName.toLowerCase()))) {
      return serviceName
    }
  }
  
  return null
}

function parseDateTimeES(dtText) {
  if (!dtText) return null
  
  const t = rmDiacritics(dtText.toLowerCase())
  let base = null
  
  // Manejo de "hoy" y "mañana"
  if (/\bhoy\b/.test(t)) {
    base = dayjs().tz(EURO_TZ)
  } else if (/\bmanana\b/.test(t)) {
    base = dayjs().tz(EURO_TZ).add(1,"day")
  }
  
  // Formato "14 de agosto"
  if (!base) {
    const M = {
      enero:1, febrero:2, marzo:3, abril:4, mayo:5, junio:6,
      julio:7, agosto:8, septiembre:9, setiembre:9, octubre:10, 
      noviembre:11, diciembre:12, ene:1, feb:2, mar:3, abr:4, 
      may:5, jun:6, jul:7, ago:8, sep:9, oct:10, nov:11, dic:12
    }
    
    const m = t.match(/\b(\d{1,2})\s+de\s+(enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|setiembre|octubre|noviembre|diciembre|ene|feb|mar|abr|may|jun|jul|ago|sep|oct|nov|dic)\b(?:\s+de\s+(\d{4}))?/)
    
    if (m) {
      const dd = +m[1]
      const mm = M[m[2]]
      const yy = m[3] ? +m[3] : dayjs().tz(EURO_TZ).year()
      base = dayjs.tz(`${yy}-${String(mm).padStart(2,"0")}-${String(dd).padStart(2,"0")} 00:00`, EURO_TZ)
    }
  }
  
  // Formato dd/mm o dd-mm
  if (!base) {
    const m = t.match(/\b(\d{1,2})[\/\-](\d{1,2})(?:[\/\-](\d{2,4}))?\b/)
    if (m) {
      let yy = m[3] ? +m[3] : dayjs().tz(EURO_TZ).year()
      if (yy < 100) yy += 2000
      base = dayjs.tz(`${yy}-${String(+m[2]).padStart(2,"0")}-${String(+m[1]).padStart(2,"0")} 00:00`, EURO_TZ)
    }
  }
  
  if (!base) base = dayjs().tz(EURO_TZ)
  
  // Extraer hora
  let hour = null, minute = 0
  const hm = t.match(/(\d{1,2})(?::|h)?(\d{2})?\s*(am|pm)?\b/)
  
  if (hm) {
    hour = +hm[1]
    minute = hm[2] ? +hm[2] : 0
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

// ===== Square API
const square = new Client({ 
  accessToken: process.env.SQUARE_ACCESS_TOKEN, 
  environment: process.env.SQUARE_ENV === "production" ? Environment.Production : Environment.Sandbox 
})

const locationId = process.env.SQUARE_LOCATION_ID
let LOCATION_TZ = EURO_TZ

// Función para cargar servicios dinámicamente desde Square
async function loadServicesFromSquare() {
  try {
    console.log("🔄 Cargando servicios desde Square...")
    
    // Obtener todos los objetos del catálogo
    const catalogResp = await square.catalogApi.listCatalog(undefined, "ITEM_VARIATION")
    const variations = catalogResp?.result?.objects || []
    
    // Filtrar solo las variaciones de servicio que están en nuestros team member IDs
    const serviceVariations = variations.filter(obj => 
      obj.type === "ITEM_VARIATION" &&
      obj.itemVariationData?.itemId &&
      obj.itemVariationData?.serviceVariationData
    )
    
    console.log(`📋 Encontradas ${serviceVariations.length} variaciones de servicio`)
    
    // Reset de configuración
    SERVICES = {}
    SERVICE_VARIATIONS = {}
    SERVICE_KEYWORDS = {}
    
    for (const variation of serviceVariations) {
      const name = variation.itemVariationData?.name || "Servicio sin nombre"
      const serviceData = variation.itemVariationData?.serviceVariationData
      const durationMs = serviceData?.durationMinutes || 60
      
      // Solo incluir servicios que tienen team members asignados
      const teamMemberIds = serviceData?.teamMemberIds || []
      const hasValidTeamMember = teamMemberIds.some(id => TEAM_MEMBER_IDS.includes(id)) || TEAM_MEMBER_IDS.length === 0
      
      if (hasValidTeamMember) {
        SERVICES[name] = durationMs
        SERVICE_VARIATIONS[name] = variation.id
        
        // Generar palabras clave para cada servicio
        SERVICE_KEYWORDS[name] = generateServiceKeywords(name)
        
        console.log(`✅ Servicio cargado: ${name} (${durationMs}min) - ID: ${variation.id}`)
      } else {
        console.log(`⏭️ Servicio omitido (sin team members válidos): ${name}`)
      }
    }
    
    console.log(`🎯 Total servicios activos: ${Object.keys(SERVICES).length}`)
    
    if (Object.keys(SERVICES).length === 0) {
      console.warn("⚠️ No se encontraron servicios válidos. Usando configuración por defecto.")
      SERVICES = { "servicio general": 60 }
      SERVICE_VARIATIONS = { "servicio general": "default" }
      SERVICE_KEYWORDS = { "servicio general": ["servicio", "cita", "reserva"] }
    }
    
  } catch (error) {
    console.error("❌ Error cargando servicios desde Square:", error?.message || error)
    
    // Fallback a configuración por defecto
    SERVICES = { "uñas acrílicas": 90 }
    SERVICE_VARIATIONS = { "uñas acrílicas": process.env.SQ_SV_UNAS_ACRILICAS || "" }
    SERVICE_KEYWORDS = { "uñas acrílicas": ["uñas", "acrilicas", "acrílicas", "nails"] }
  }
}

// Genera palabras clave para un servicio basándose en su nombre
function generateServiceKeywords(serviceName) {
  const keywords = [serviceName]
  const words = serviceName.toLowerCase().split(/[\s\-_]+/)
  
  // Añadir cada palabra individual
  keywords.push(...words)
  
  // Añadir combinaciones específicas según el tipo de servicio
  const lowerName = serviceName.toLowerCase()
  
  if (lowerName.includes("uñas") || lowerName.includes("nail")) {
    keywords.push("uñas", "nails", "manicura", "pedicura")
  }
  
  if (lowerName.includes("acrílicas") || lowerName.includes("acrilicas")) {
    keywords.push("acrílicas", "acrilicas", "acrylic")
  }
  
  if (lowerName.includes("gel")) {
    keywords.push("gel", "gelish")
  }
  
  if (lowerName.includes("semipermanente")) {
    keywords.push("semipermanente", "semi")
  }
  
  if (lowerName.includes("extensión") || lowerName.includes("extension")) {
    keywords.push("extensión", "extension", "alargamiento")
  }
  
  // Eliminar duplicados y elementos vacíos
  return [...new Set(keywords.filter(k => k && k.length > 2))]
}

async function squareCheckCredentials() {
  try {
    const locs = await square.locationsApi.listLocations()
    const loc = (locs.result.locations||[]).find(l=>l.id===locationId) || (locs.result.locations||[])[0]
    
    if (loc?.timezone) LOCATION_TZ = loc.timezone
    console.log(`✅ Square listo. Location ${locationId}, TZ=${LOCATION_TZ}`)
    
    // Cargar servicios después de verificar credenciales
    await loadServicesFromSquare()
    
  } catch(e) {
    console.error("⛔ Square:",e?.message||e)
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
      givenName,
      emailAddress,
      phoneNumber: phone || undefined,
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
    const resp = await square.catalogApi.retrieveCatalogObject(id, true)
    return resp?.result?.object?.version
  } catch(e) {
    console.error("getServiceVariationVersion:",e?.message||e)
    return undefined
  }
}

function stableKey({locationId, serviceVariationId, startISO, customerId}) {
  const raw = `${locationId}|${serviceVariationId}|${startISO}|${customerId}`
  return createHash("sha256").update(raw).digest("hex").slice(0,48)
}

async function createSquareBooking({startEU, serviceKey, customerId, teamMemberId}) {
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
    console.error("createSquareBooking:",e?.message||e)
    return null
  }
}

// ===== Base de datos y sesiones
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

-- Añadir índice para prevenir citas duplicadas por teléfono en el mismo horario
CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_customer_slot
ON appointments(customer_phone, start_iso)
WHERE status IN ('pending','confirmed');
`)

// Prepared statements
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
  
  if (raw.startEUISO) {
    data.startEU = dayjs.tz(raw.startEUISO, EURO_TZ)
  }
  
  return data
}

function saveSession(phone, data) {
  const s = {...data}
  s.startEUISO = data.startEU?.toISOString ? data.startEU.toISOString() : (data.startEUISO || null)
  delete s.startEU
  
  upsertSession.run({
    phone,
    data_json: JSON.stringify(s),
    updated_at: new Date().toISOString()
  })
}

// ===== Sistema de disponibilidad mejorado
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

function staffHasFree(intervals, start, end, customerPhone = null) {
  const ids = TEAM_MEMBER_IDS.length ? TEAM_MEMBER_IDS : ["any"]
  
  for (const id of ids) {
    // Verificar conflictos de staff
    const staffBusy = intervals.filter(i => i.staff_id === id)
      .some(i => (start < i.end) && (i.start < end))
    
    // Verificar si el mismo cliente ya tiene cita en ese horario
    const customerBusy = customerPhone ? intervals
      .filter(i => i.customer_phone === customerPhone)
      .some(i => (start < i.end) && (i.start < end)) : false
    
    if (!staffBusy && !customerBusy) return true
  }
  
  return false
}

function suggestOrExact(startEU, durationMin, customerPhone = null) {
  const now = dayjs().tz(EURO_TZ).add(30,"minute").second(0).millisecond(0)
  const from = now.tz("UTC").toISOString()
  const to = now.add(14,"day").tz("UTC").toISOString()
  const intervals = getBookedIntervals(from, to)
  
  const endEU = startEU.clone().add(durationMin,"minute")
  const dow = startEU.day() === 0 ? 7 : startEU.day()
  
  const insideHours = startEU.hour() >= OPEN_HOUR && 
    (endEU.hour() < CLOSE_HOUR || (endEU.hour() === CLOSE_HOUR && endEU.minute() === 0))
  
  // Si es domingo, fuera de horario o en el pasado, buscar alternativa
  if (dow === 7 || !WORK_DAYS.includes(dow) || !insideHours || startEU.isBefore(now)) {
    const dayStart = startEU.clone().hour(OPEN_HOUR).minute(0).second(0)
    const dayEnd = startEU.clone().hour(CLOSE_HOUR).minute(0).second(0)
    
    for (let t = dayStart.clone(); !t.isAfter(dayEnd); t = t.add(SLOT_MIN,"minute")) {
      const e = t.clone().add(durationMin,"minute")
      if (t.isBefore(now)) continue
      if (staffHasFree(intervals, t.tz("UTC"), e.tz("UTC"), customerPhone)) {
        return {exact: null, suggestion: t}
      }
    }
    
    return {exact: null, suggestion: null}
  }
  
  // Verificar si la hora exacta está libre
  if (staffHasFree(intervals, startEU.tz("UTC"), endEU.tz("UTC"), customerPhone)) {
    return {exact: startEU, suggestion: null}
  }
  
  // Buscar alternativa en el mismo día
  const dayStart = startEU.clone().hour(OPEN_HOUR).minute(0).second(0)
  const dayEnd = startEU.clone().hour(CLOSE_HOUR).minute(0).second(0)
  
  for (let t = dayStart.clone(); !t.isAfter(dayEnd); t = t.add(SLOT_MIN,"minute")) {
    const e = t.clone().add(durationMin,"minute")
    if (t.isBefore(now)) continue
    if (staffHasFree(intervals, t.tz("UTC"), e.tz("UTC"), customerPhone)) {
      return {exact: null, suggestion: t}
    }
  }
  
  return {exact: null, suggestion: null}
}

// ===== Web interface
const app = express()
const PORT = process.env.PORT || 8080
let lastQR = null, conectado = false

app.get("/", (_req, res) => {
  const servicesList = Object.keys(SERVICES).map(name => 
    `<li>${name} (${SERVICES[name]}min)</li>`
  ).join('')
  
  res.send(`<!doctype html>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
body {
  font-family: system-ui;
  display: grid;
  place-items: center;
  min-height: 100vh;
  background: linear-gradient(135deg, #fce4ec, #f8bbd0);
  color: #4a148c;
  margin: 0;
}
.card {
  background: #fff;
  padding: 24px;
  border-radius: 16px;
  box-shadow: 0 6px 24px rgba(0,0,0,.08);
  text-align: center;
  max-width: 520px;
  margin: 20px;
}
.status {
  padding: 8px 16px;
  border-radius: 8px;
  font-weight: bold;
  margin: 16px 0;
}
.connected { background: #e8f5e8; color: #2e7d32; }
.disconnected { background: #ffebee; color: #c62828; }
.services { text-align: left; margin: 16px 0; }
.services ul { padding-left: 20px; }
img { max-width: 100%; height: auto; }
</style>
<div class="card">
  <h1>🌸 Gapink Nails Bot</h1>
  <div class="status ${conectado ? 'connected' : 'disconnected'}">
    ${conectado ? "✅ Conectado" : "❌ Desconectado"}
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
    const png = await qrcode.toBuffer(lastQR, {
      type: "png",
      width: 512,
      margin: 1
    })
    res.set("Content-Type", "image/png").send(png)
  } catch(e) {
    res.status(500).send("Error generando QR")
  }
})

app.get("/health", (_req, res) => {
  res.json({ 
    status: "ok", 
    connected: conectado,
    services: Object.keys(SERVICES).length,
    teamMembers: TEAM_MEMBER_IDS.length
  })
})

app.get("/api/services", (_req, res) => {
  res.json({
    services: SERVICES,
    variations: SERVICE_VARIATIONS,
    keywords: SERVICE_KEYWORDS,
    teamMembers: TEAM_MEMBER_IDS
  })
})

// ===== Sistema de cola de mensajes
const wait = (ms) => new Promise(r => setTimeout(r, ms))

// ===== Función principal de reserva mejorada
async function finalizeBooking({ from, phone, data, safeSend }) {
  try {
    if (data.bookingInFlight) {
      console.log("Booking already in flight for", phone)
      return
    }
    
    data.bookingInFlight = true
    saveSession(phone, data)

    // Validar que el servicio existe
    if (!SERVICES[data.service]) {
      await safeSend(from, { text: `El servicio "${data.service}" no está disponible. Los servicios disponibles son: ${Object.keys(SERVICES).join(', ')}` })
      data.bookingInFlight = false
      saveSession(phone, data)
      return
    }

    // Buscar o crear cliente
    let customer = await squareFindCustomerByPhone(phone)
    if (!customer) {
      if (!data.name || !data.email) { 
        await safeSend(from, { text: "Me falta tu nombre y email para crear la reserva." })
        data.bookingInFlight = false
        saveSession(phone, data)
        return 
      }
      customer = await squareCreateCustomer({ 
        givenName: data.name, 
        emailAddress: data.email, 
        phoneNumber: phone 
      })
    }
    
    if (!customer) { 
      await safeSend(from, { text: "Ahora mismo no puedo crear tu ficha. Probamos en un minuto." })
      data.bookingInFlight = false
      saveSession(phone, data)
      return 
    }

    // Seleccionar team member (el primero disponible)
    const teamMemberId = TEAM_MEMBER_IDS[0] || "any"
    const durationMin = SERVICES[data.service]
    const startUTC = data.startEU.tz("UTC")
    const endUTC = startUTC.clone().add(durationMin,"minute")
    const aptId = `apt_${Math.random().toString(36).slice(2,8)}${Date.now().toString(36).slice(-4)}`
    
    // Verificar disponibilidad una vez más antes de crear
    const intervals = getBookedIntervals(startUTC.toISOString(), endUTC.toISOString())
    if (!staffHasFree(intervals, startUTC, endUTC, phone)) {
      await safeSend(from, { text: "Ese horario se acaba de ocupar. Dime otra hora y te busco el siguiente disponible." })
      data.bookingInFlight = false
      saveSession(phone, data)
      return
    }
    
    // Insertar cita pendiente (candado anti-dobles)
    try {
      insertAppt.run({ 
        id: aptId, 
        customer_name: data.name || customer?.givenName || null, 
        customer_phone: phone, 
        customer_square_id: customer.id, 
        service: data.service, 
        duration_min: durationMin, 
        start_iso: startUTC.toISOString(), 
        end_iso: endUTC.toISOString(), 
        staff_id: teamMemberId, 
        status: "pending", 
        created_at: new Date().toISOString(), 
        square_booking_id: null 
      })
    } catch (e) {
      if (String(e?.message||"").includes("UNIQUE")) {
        await safeSend(from, { text: "Ese hueco se acaba de ocupar o ya tienes una cita en ese horario. Dime otra hora y te digo el primero disponible." })
        data.bookingInFlight = false
        saveSession(phone, data)
        return
      }
      throw e
    }

    // Crear reserva en Square
    const sq = await createSquareBooking({ 
      startEU: data.startEU, 
      serviceKey: data.service, 
      customerId: customer.id, 
      teamMemberId 
    })
    
    if (!sq) {
      deleteAppt.run({ id: aptId })
      await safeSend(from, { text: "No he podido cerrar la cita ahora mismo. ¿Probamos otra vez?" })
      data.bookingInFlight = false
      saveSession(phone, data)
      return
    }

    // Confirmar reserva
    updateAppt.run({ id: aptId, status: "confirmed", square_booking_id: sq.id || null })
    clearSession.run({ phone })
    
    await safeSend(from, { text:
`✅ Reserva confirmada

📅 ${fmtES(data.startEU)}
💅 ${data.service}
⏱️ Duración: ${durationMin} min

💰 Pago en persona
📍 Gapink Nails

¡Te esperamos!` })

  } catch (e) {
    console.error("finalizeBooking error:", e)
    await safeSend(from, { text: "Ha fallado el cierre de la reserva. Lo reviso y te aviso." })
  } finally {
    data.bookingInFlight = false
    try { saveSession(phone, data) } catch(e) { console.error("Save session error:", e) }
  }
}

// ===== Bot principal
async function startBot() {
  console.log("🚀 Bot arrancando…")
  
  try {
    // Crear directorio de auth si no existe
    if (!fs.existsSync("auth_info")) {
      fs.mkdirSync("auth_info", {recursive: true})
    }
    
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

    // Sistema de cola de mensajes
    const outbox = []
    let sending = false
    
    const __SAFE_SEND__ = (jid, content) => new Promise((resolve, reject) => {
      outbox.push({jid, content, resolve, reject})
      processOutbox().catch(console.error)
    })
    
    async function processOutbox() {
      if (sending) return
      sending = true
      
      while (outbox.length) {
        const {jid, content, resolve, reject} = outbox.shift()
        
        // Esperar conexión con timeout
        let guard = 0
        while (!isOpen && guard < 60) {
          await wait(1000)
          guard++
        }
        
        if (!isOpen) {
          reject(new Error("WA not connected"))
          continue
        }
        
        let ok = false, err = null
        
        // Reintentos con backoff
        for (let attempt = 1; attempt <= 4; attempt++) {
          try {
            await sock.sendMessage(jid, content)
            ok = true
            break
          } catch(e) {
            err = e
            const msg = e?.data?.stack || e?.message || String(e)
            
            if (/Timed Out/i.test(msg) || /Boom/i.test(msg)) {
              console.log(`Attempt ${attempt}/4 failed: ${msg}`)
              await wait(500 * attempt)
              continue
            }
            
            await wait(400)
          }
        }
        
        if (ok) {
          resolve(true)
        } else {
          console.error("sendMessage failed:", err?.message || err)
          reject(err)
          
          // Forzar reconexión en caso de timeout
          if (/Timed Out/i.test(err?.message || "")) {
            try {
              await sock.ws.close()
            } catch(e) {
              console.error("Error closing socket:", e)
            }
          }
        }
      }
      
      sending = false
    }

    // Eventos de conexión
    sock.ev.on("connection.update", async({connection, lastDisconnect, qr}) => {
      if (qr) {
        lastQR = qr
        conectado = false
        
        try {
          qrcodeTerminal.generate(qr, {small: true})
          console.log("📱 Escanea el QR para conectar WhatsApp")
        } catch(e) {
          console.error("Error showing QR:", e)
        }
      }
      
      if (connection === "open") {
        lastQR = null
        conectado = true
        isOpen = true
        console.log("✅ Conectado a WhatsApp")
        processOutbox().catch(console.error)
      }
      
      if (connection === "close") {
        conectado = false
        isOpen = false
        
        const reason = lastDisconnect?.error?.message || String(lastDisconnect?.error || "")
        console.log("❌ Conexión cerrada:", reason)
        
        if (!reconnecting) {
          reconnecting = true
          await wait(2000)
          
          try {
            await startBot()
          } catch(e) {
            console.error("Error restarting bot:", e)
          } finally {
            reconnecting = false
          }
        }
      }
    })

    sock.ev.on("creds.update", saveCreds)

    // ===== Procesamiento de mensajes mejorado
    sock.ev.on("messages.upsert", async({messages}) => {
      try {
        const m = messages?.[0]
        if (!m?.message || m.key.fromMe) return
        
        const from = m.key.remoteJid
        const phone = normalizePhoneES((from||"").split("@")[0]||"") || (from||"").split("@")[0] || ""
        const body = m.message.conversation || 
                    m.message.extendedTextMessage?.text || 
                    m.message?.imageMessage?.caption || ""
        const textRaw = (body||"").trim()
        
        if (!textRaw) return
        
        console.log(`📱 ${phone}: ${textRaw}`)

        // Cargar sesión
        let data = loadSession(phone) || {
          service: null,
          startEU: null,
          durationMin: null,
          name: null,
          email: null,
          confirmApproved: false,
          confirmAsked: false,
          bookingInFlight: false,
          lastUserDtText: null,
          lastService: null
        }

        // Extracción con IA mejorada
        const extra = await extractFromText(textRaw)
        const incomingService = extra.service || detectServiceFree(textRaw)
        const incomingDt = extra.datetime_text || null

        // Reset confirmación si cambió servicio o fecha/hora
        if ((incomingService && incomingService !== data.lastService) || 
            (incomingDt && incomingDt !== data.lastUserDtText)) {
          data.confirmApproved = false
          data.confirmAsked = false
        }
        
        // Actualizar datos
        if (!data.service) data.service = incomingService || data.service
        data.lastService = data.service || data.lastService

        if (!data.name && extra.name) data.name = extra.name
        if (!data.email && extra.email) data.email = extra.email

        if (YES_RE.test(textRaw) || extra.confirm === "yes") data.confirmApproved = true
        if (NO_RE.test(textRaw) || extra.confirm === "no") { 
          data.confirmApproved = false
          data.confirmAsked = false 
        }

        const whenText = extra.datetime_text || textRaw
        if (whenText) data.lastUserDtText = extra.datetime_text || data.lastUserDtText
        const parsed = parseDateTimeES(whenText)
        if (parsed) data.startEU = parsed

        if (data.service && !data.durationMin) {
          data.durationMin = SERVICES[data.service] || 60
        }
        
        saveSession(phone, data)

        // === FIX BUCLE: si ya había propuesta y el usuario dice "sí"/"confirmo", reservar inmediatamente ===
        if (data.confirmAsked && data.confirmApproved && data.service && data.startEU && data.durationMin) {
          await finalizeBooking({ from, phone, data, safeSend: __SAFE_SEND__ })
          return
        }

        // Comando para listar servicios
        if (/\b(servicios|que servicios|qué servicios|lista servicios)\b/i.test(textRaw)) {
          const serviceList = Object.keys(SERVICES).map(name => 
            `• ${name} (${SERVICES[name]} min)`
          ).join('\n')
          
          await __SAFE_SEND__(from, { 
            text: `📋 Nuestros servicios:\n\n${serviceList}\n\n¿Cuál te interesa y para cuándo?` 
          })
          return
        }

        // Cancelación
        if ((extra.intent === "cancel") || /cancel(ar)?\s+cita/i.test(textRaw)) {
          await __SAFE_SEND__(from, { text: "Cancelación anotada. Si quieres, dime otra fecha y te busco hueco." })
          clearSession.run({ phone })
          return
        }

        // DISPONIBILIDAD mejorada
        if (data.service && data.startEU && data.durationMin) {
          // Validar que el servicio existe
          if (!SERVICES[data.service]) {
            data.service = null
            data.durationMin = null
            saveSession(phone, data)
            
            const availableServices = Object.keys(SERVICES).join(', ')
            await __SAFE_SEND__(from, { 
              text: `Ese servicio no está disponible. Los servicios que tenemos son: ${availableServices}. ¿Cuál prefieres?` 
            })
            return
          }

          const { exact, suggestion } = suggestOrExact(data.startEU, data.durationMin, phone)
          
          if (exact) {
            data.startEU = exact
            
            if (data.confirmApproved) { 
              saveSession(phone, data)
              await finalizeBooking({ from, phone, data, safeSend: __SAFE_SEND__ })
              return 
            }
            
            data.confirmAsked = true
            saveSession(phone, data)
            await __SAFE_SEND__(from, { 
              text: `Tengo libre ${fmtES(data.startEU)} para ${data.service}. ¿Confirmo la cita?` 
            })
            return
          }
          
          if (suggestion) {
            // Siempre pedir confirmación para sugeridos
            data.startEU = suggestion
            data.confirmAsked = true
            data.confirmApproved = false
            saveSession(phone, data)
            
            await __SAFE_SEND__(from, { 
              text: `No tengo ese hueco exacto. Te puedo ofrecer ${fmtES(data.startEU)}. ¿Te viene bien? Si es sí, responde "confirmo".` 
            })
            return
          }
          
          data.confirmAsked = false
          saveSession(phone, data)
          await __SAFE_SEND__(from, { 
            text: "No veo hueco en esa franja. Dime otra hora o día y te digo." 
          })
          return
        }

        // Falta nombre/email tras "sí"
        if (data.confirmApproved && (!data.name || !data.email)) {
          saveSession(phone, data)
          await __SAFE_SEND__(from, { 
            text: "Para cerrar, dime tu nombre y email (ej: \"Ana Pérez, ana@correo.com\")." 
          })
          return
        }
        
        if (data.confirmApproved && data.name && data.email && data.service && data.startEU) {
          await finalizeBooking({ from, phone, data, safeSend: __SAFE_SEND__ })
          return
        }

        // Faltan datos → IA redacta con servicios dinámicos
        const missing = []
        if (!data.service) missing.push("servicio")
        if (!data.startEU) missing.push("día y hora")
        if (!data.name || !data.email) missing.push("nombre y email (si eres nuevo)")
        
        const availableServices = Object.keys(SERVICES).join(', ')
        const prompt = `Contexto:
- Servicios disponibles: ${availableServices}
- Servicio elegido: ${data.service || "?"}
- Fecha/Hora: ${data.startEU ? fmtES(data.startEU) : "?"}
- Nombre: ${data.name || "?"}
- Email: ${data.email || "?"}

Escribe un único mensaje corto y humano que avance la reserva, sin emojis.
Si faltan datos (${missing.join(", ")}), pídelo amablemente con ejemplo.
Si no reconoces el servicio, menciona los disponibles.
Mensaje del cliente: "${textRaw}"`

        const say = await aiSay(prompt)
        saveSession(phone, data)
        
        await __SAFE_SEND__(from, { 
          text: say || `Hola. Nuestros servicios son: ${availableServices}. ¿Cuál te interesa y para cuándo?` 
        })
        
      } catch(e) {
        console.error("messages.upsert error:", e)
      }
    })
    
  } catch(e) {
    console.error("startBot error:", e)
    setTimeout(() => startBot().catch(console.error), 5000)
  }
}

// ===== Inicio de la aplicación
app.listen(PORT, async() => {
  console.log(`🌐 Web servidor iniciado en puerto ${PORT}`)
  
  // Verificar variables de entorno críticas
  const requiredEnvs = [
    'DEEPSEEK_API_KEY',
    'SQUARE_ACCESS_TOKEN', 
    'SQUARE_LOCATION_ID',
    'SQ_TEAM_IDS'
  ]
  
  const missing = requiredEnvs.filter(env => !process.env[env])
  if (missing.length > 0) {
    console.error('⛔ Faltan variables de entorno:', missing.join(', '))
    process.exit(1)
  }
  
  console.log(`✅ Configuración validada`)
  console.log(`📍 Square Location: ${process.env.SQUARE_LOCATION_ID}`)
  console.log(`👥 Team IDs: ${TEAM_MEMBER_IDS.length}`)
  
  await squareCheckCredentials()
  startBot().catch(console.error)
})

// Manejo de errores globales
process.on('uncaughtException', (error) => {
  console.error('Uncaught Exception:', error)
})

process.on('unhandledRejection', (reason, promise) => {
  console.error('Unhandled Rejection at:', promise, 'reason:', reason)
})

// Graceful shutdown
process.on('SIGTERM', () => {
  console.log('SIGTERM recibido, cerrando aplicación...')
  db.close()
  process.exit(0)
})

process.on('SIGINT', () => {
  console.log('SIGINT recibido, cerrando aplicación...')
  db.close()
  process.exit(0)
})
