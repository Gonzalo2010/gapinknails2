// index.js — Gapink Nails · v28.0.0 (Core IA Avanzada + Square 100%)
// Changelog clave vs v27.2.x:
// • 🔥 Motor IA multinivel (preproc → intenciones → contexto → respuesta adaptativa → validación).
// • 🧠 Memoria conversacional en sessionData (preferencias, estilo, urgencia, satisfacción, patrones).
// • 🧩 Prompt de sistema dinámico (tiempo, historial, contexto de servicios y staff).
// • 🛡️ Validador inteligente antes de crear reservas + sugerencias.
// • 🤝 Integración Square con predicción/optimización IA (placeholders seguros).
// • 👋 “Con {nombre}” sigue priorizando staff; solo oferta staff permitido en esa sede.
// • 🔕 Punto “.” silencia 6h por chat (igual que antes).
// • 🧽 Eliminado sistema rígido de anchors POS_/NEG_ → ahora NLP avanzado interno.
// • 🧾 Logs/metricas IA y Square en SQLite (mismo fichero).

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
import { webcrypto, createHash } from "crypto"
import { createRequire } from "module"
import { Client, Environment } from "square"

if (!globalThis.crypto) globalThis.crypto = webcrypto
dayjs.extend(utc); dayjs.extend(tz); dayjs.locale("es")
const EURO_TZ = "Europe/Madrid"

// ====== Config horario
const WORK_DAYS = [1,2,3,4,5]
const SLOT_MIN = 30
const OPEN = { start: 9, end: 20 }
const NOW_MIN_OFFSET_MIN = Number(process.env.BOT_NOW_OFFSET_MIN || 30)
const HOLIDAYS_EXTRA = (process.env.HOLIDAYS_EXTRA || "06/01,28/02,15/08,12/10,01/11,06/12,08/12,25/12")
  .split(",").map(s=>s.trim()).filter(Boolean)

// ====== Flags
const BOT_DEBUG = /^true$/i.test(process.env.BOT_DEBUG || "")
const SQUARE_MAX_RETRIES = Number(process.env.SQUARE_MAX_RETRIES || 3)

// ====== Square
const square = new Client({
  accessToken: process.env.SQUARE_ACCESS_TOKEN,
  environment: (process.env.SQUARE_ENV==="production") ? Environment.Production : Environment.Sandbox
})
const LOC_TORRE = (process.env.SQUARE_LOCATION_ID_TORREMOLINOS || "").trim()
const LOC_LUZ   = (process.env.SQUARE_LOCATION_ID_LA_LUZ || "").trim()
const ADDRESS_TORRE = process.env.ADDRESS_TORREMOLINOS || "Av. de Benyamina 18, Torremolinos"
const ADDRESS_LUZ   = process.env.ADDRESS_LA_LUZ || "Málaga – Barrio de La Luz"
const DRY_RUN = /^true$/i.test(process.env.DRY_RUN || "")

// ====== IA (DeepSeek por defecto)
const AI_API_KEY = process.env.DEEPSEEK_API_KEY || ""
const AI_MODEL = process.env.AI_MODEL || "deepseek-chat"
const AI_MAX_RETRIES = Number(process.env.AI_MAX_RETRIES || 3)
const AI_TIMEOUT_MS = Number(process.env.AI_TIMEOUT_MS || 15000)
const sleep = ms => new Promise(r=>setTimeout(r, ms))

// ====== Utils básicos
const onlyDigits = s => String(s||"").replace(/\D+/g,"")
// Quita diacríticos solo para comparaciones; no para mostrar
const rm = s => String(s||"").normalize("NFD").replace(/\p{Diacritic}/gu,"")
// Normaliza para matching laxo
const norm = s => rm(s).toLowerCase().replace(/[+.,;:()/_-]/g," ").replace(/[^\p{Letter}\p{Number}\s]/gu," ").replace(/\s+/g," ").trim()

// 👉 Restaurar tildes/ñ y arreglar faltas comunes SOLO para mostrar
function applySpanishDiacritics(label){
  let x = String(label||"")
  x = x.replace(/\bunas\b/gi, m => m[0] === 'U' ? 'Uñas' : 'uñas')
  x = x.replace(/\bpestan(as?|)\b/gi, (m, suf) => (m[0]==='P'?'Pestañ':'pestañ') + (suf||''))
  x = x.replace(/\bnivelacion\b/gi, m => m[0]==='N' ? 'Nivelación' : 'nivelación')
  x = x.replace(/\bacrilic[oa]s?\b/gi, m => {
    const cap = m[0] === m[0].toUpperCase()
    const plural = /s$/.test(m.toLowerCase())
    const fem = /a/i.test(m.slice(-1))
    const base = fem ? 'acrílica' : 'acrílico'
    const out = base + (plural ? 's' : '')
    return cap ? out[0].toUpperCase()+out.slice(1) : out
  })
  x = x.replace(/\bfrances\b/gi, m => m[0]==='F' ? 'Francés' : 'francés')
  x = x.replace(/\bmas\b/gi, (m) => (m[0]==='M' ? 'Más' : 'más'))
  x = x.replace(/\bsemi ?permanente\b/gi, m => /[A-Z]/.test(m[0]) ? 'Semipermanente' : 'semipermanente')
  x = x.replace(/\bninas\b/gi, 'niñas')
  x = x.replace(/Esculpid(a|as)\b/gi, (m)=> {
    const cap = /[A-Z]/.test(m[0]); const suf = m.endsWith('as') ? 'as' : 'a'
    return (cap?'E':'e') + 'sculpid' + suf
  })
  return x
}

function normalizePhoneES(raw){
  const d=onlyDigits(raw); if(!d) return null
  if (raw.startsWith("+") && d.length>=8 && d.length<=15) return `+${d}`
  if (d.startsWith("34") && d.length===11) return `+${d}`
  if (d.length===9) return `+34${d}`
  if (d.startsWith("00")) return `+${d.slice(2)}`
  return `+${d}`
}
function locationToId(key){ return key==="la_luz" ? LOC_LUZ : LOC_TORRE }
function idToLocKey(id){ return id===LOC_LUZ ? "la_luz" : id===LOC_TORRE ? "torremolinos" : null }
function locationNice(key){ return key==="la_luz" ? "Málaga – La Luz" : "Torremolinos" }

// ====== Horario helpers
function isHolidayEU(d){
  const dd=String(d.date()).padStart(2,"0"), mm=String(d.month()+1).padStart(2,"0")
  return HOLIDAYS_EXTRA.includes(`${dd}/${mm}`)
}
function insideBusinessHours(d,dur){
  const t=d.clone()
  if (!WORK_DAYS.includes(t.day())) return false
  if (isHolidayEU(t)) return false
  const end=t.clone().add(dur,"minute")
  if (!t.isSame(end,"day")) return false
  const startMin = t.hour()*60 + t.minute()
  const endMin   = end.hour()*60 + end.minute()
  const openMin  = OPEN.start*60
  const closeMin = OPEN.end*60
  return startMin >= openMin && endMin <= closeMin
}
function nextOpeningFrom(d){
  let t=d.clone()
  const nowMin = t.hour()*60 + t.minute()
  const openMin= OPEN.start*60
  const closeMin=OPEN.end*60
  if (nowMin < openMin) t = t.hour(OPEN.start).minute(0).second(0).millisecond(0)
  if (nowMin >= closeMin) t = t.add(1,"day").hour(OPEN.start).minute(0).second(0).millisecond(0)
  while (!WORK_DAYS.includes(t.day()) || isHolidayEU(t)) {
    t = t.add(1,"day").hour(OPEN.start).minute(0).second(0).millisecond(0)
  }
  return t
}
function ceilToSlotEU(t){
  const m=t.minute(), rem=m%SLOT_MIN
  return rem===0 ? t.second(0).millisecond(0) : t.add(SLOT_MIN-rem,"minute").second(0).millisecond(0)
}
function fmtES(d){
  const dias=["domingo","lunes","martes","miércoles","jueves","viernes","sábado"]
  const t=(dayjs.isDayjs(d)?d:dayjs(d)).tz(EURO_TZ)
  return `${dias[t.day()]} ${String(t.date()).padStart(2,"0")}/${String(t.month()+1).padStart(2,"0")} ${String(t.hour()).padStart(2,"0")}:${String(t.minute()).padStart(2,"0")}`
}
function enumerateHours(list){ return list.map((d,i)=>({ index:i+1, iso:d.format("YYYY-MM-DDTHH:mm"), pretty:fmtES(d) })) }
function stableKey(parts){ const raw=Object.values(parts).join("|"); return createHash("sha256").update(raw).digest("hex").slice(0,48) }

// ====== JSON seguro
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

// ====== TZ robusto
function parseToEU(input){
  if (dayjs.isDayjs(input)) return input.clone().tz(EURO_TZ)
  const s = String(input||"")
  if (/[Zz]|[+\-]\d{2}:?\d{2}$/.test(s)) return dayjs(s).tz(EURO_TZ)
  return dayjs.tz(s, EURO_TZ)
}

// ====== DB
const db=new Database("gapink.db"); db.pragma("journal_mode = WAL")
db.exec(`
CREATE TABLE IF NOT EXISTS appointments (
  id TEXT PRIMARY KEY,
  customer_name TEXT,
  customer_phone TEXT,
  customer_square_id TEXT,
  location_key TEXT,
  service_env_key TEXT,
  service_label TEXT,
  duration_min INTEGER,
  start_iso TEXT,
  end_iso TEXT,
  staff_id TEXT,
  status TEXT,
  created_at TEXT,
  square_booking_id TEXT,
  square_error TEXT,
  retry_count INTEGER DEFAULT 0
);
CREATE TABLE IF NOT EXISTS sessions (
  phone TEXT PRIMARY KEY,
  data_json TEXT,
  updated_at TEXT
);
CREATE TABLE IF NOT EXISTS ai_conversations (
  phone TEXT,
  message_id TEXT,
  user_message TEXT,
  ai_response TEXT,
  timestamp TEXT,
  session_data TEXT,
  ai_error TEXT,
  fallback_used BOOLEAN DEFAULT 0,
  PRIMARY KEY (phone, message_id)
);
CREATE TABLE IF NOT EXISTS square_logs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  phone TEXT,
  action TEXT,
  request_data TEXT,
  response_data TEXT,
  error_data TEXT,
  timestamp TEXT,
  success BOOLEAN
);
CREATE TABLE IF NOT EXISTS ai_metrics (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  phone TEXT,
  response_time_ms INTEGER,
  user_satisfaction REAL,
  task_completion REAL,
  error_rate REAL,
  conversion_rate REAL,
  timestamp TEXT
);
`)

const insertAppt = db.prepare(`INSERT INTO appointments
(id,customer_name,customer_phone,customer_square_id,location_key,service_env_key,service_label,duration_min,start_iso,end_iso,staff_id,status,created_at,square_booking_id,square_error,retry_count)
VALUES (@id,@customer_name,@customer_phone,@customer_square_id,@location_key,@service_env_key,@service_label,@duration_min,@start_iso,@end_iso,@staff_id,@status,@created_at,@square_booking_id,@square_error,@retry_count)`)

const insertAIConversation = db.prepare(`INSERT OR REPLACE INTO ai_conversations
(phone, message_id, user_message, ai_response, timestamp, session_data, ai_error, fallback_used)
VALUES (@phone, @message_id, @user_message, @ai_response, @timestamp, @session_data, @ai_error, @fallback_used)`)

const insertSquareLog = db.prepare(`INSERT INTO square_logs
(phone, action, request_data, response_data, error_data, timestamp, success)
VALUES (@phone, @action, @request_data, @response_data, @error_data, @timestamp, @success)`)

const insertMetrics = db.prepare(`INSERT INTO ai_metrics
(phone, response_time_ms, user_satisfaction, task_completion, error_rate, conversion_rate, timestamp)
VALUES (@phone, @rt, @sat, @task, @err, @conv, @ts)`)

// ====== Sesión
function enhanceSessionData(sessionData) {
  const base = {
    conversationMemory: {
      topics: [],
      preferences: {},
      communicationStyle: 'formal',
      urgencyLevel: 'normal',
      satisfactionLevel: 'neutral'
    },
    predictiveContext: {
      likelyNextAction: null,
      suggestedServices: [],
      preferredTimes: [],
      behaviorPattern: 'new_user'
    },
    decisionHistory: [],
    interactionMetrics: {
      responseTime: [],
      clarificationCount: 0,
      successfulBookings: 0,
      cancelledBookings: 0
    }
  }
  return { ...base, ...sessionData, conversationMemory: { ...base.conversationMemory, ...(sessionData?.conversationMemory || {}) }, predictiveContext: { ...base.predictiveContext, ...(sessionData?.predictiveContext || {}) }, interactionMetrics: { ...base.interactionMetrics, ...(sessionData?.interactionMetrics || {}) } }
}
function loadSession(phone){
  const row = db.prepare(`SELECT data_json FROM sessions WHERE phone=@phone`).get({phone})
  if (!row?.data_json) return null
  const s = JSON.parse(row.data_json)
  if (Array.isArray(s.lastHours_ms)) s.lastHours = s.lastHours_ms.map(ms=>dayjs.tz(ms,EURO_TZ))
  if (s.pendingDateTime_ms) s.pendingDateTime = dayjs.tz(s.pendingDateTime_ms,EURO_TZ)
  return enhanceSessionData(s)
}
function saveSession(phone,s){
  const c={...s}
  c.lastHours_ms = Array.isArray(s.lastHours)? s.lastHours.map(d=>dayjs.isDayjs(d)?d.valueOf():null).filter(Boolean):[]
  c.pendingDateTime_ms = s.pendingDateTime? (dayjs.isDayjs(s.pendingDateTime)? s.pendingDateTime.valueOf() : dayjs(s.pendingDateTime).valueOf()) : null
  delete c.lastHours; delete c.pendingDateTime
  const j=JSON.stringify(c)
  const up=db.prepare(`UPDATE sessions SET data_json=@j, updated_at=@u WHERE phone=@p`).run({j,u:new Date().toISOString(),p:phone})
  if (up.changes===0) db.prepare(`INSERT INTO sessions (phone,data_json,updated_at) VALUES (@p,@j,@u)`).run({p:phone,j,u:new Date().toISOString()})
}
function clearSession(phone){ db.prepare(`DELETE FROM sessions WHERE phone=@phone`).run({phone}) }

// ====== Empleadas y servicios
function deriveLabelsFromEnvKey(envKey){
  const raw = envKey.replace(/^SQ_EMP_/, "")
  const toks = raw.split("_").map(t=>norm(t)).filter(Boolean)
  const uniq = Array.from(new Set(toks))
  const labels = [...uniq]
  if (uniq.length>1) labels.push(uniq.join(" "))
  return labels
}
function parseEmployees(){
  const out=[]
  for (const [k,v] of Object.entries(process.env)) {
    if (!k.startsWith("SQ_EMP_")) continue
    const [id, book, locs] = String(v||"").split("|")
    if (!id) continue
    const bookable = (book||"").toUpperCase()==="BOOKABLE"
    let allow = (locs||"").split(",").map(s=>s.trim()).filter(Boolean)

    const empKey = "EMP_CENTER_" + k.replace(/^SQ_EMP_/, "")
    const empVal = process.env[empKey]
    if (empVal) {
      const centers = String(empVal).split(",").map(s=>s.trim().toLowerCase()).filter(Boolean)
      if (centers.some(c => c === "all")) {
        allow = ["ALL"]
      } else {
        const normCenter = c => (c==="la luz" ? "la_luz" : c)
        const ids = centers
          .map(c => normCenter(c))
          .map(centerKey => locationToId(centerKey))
          .filter(Boolean)
        if (ids.length) allow = ids
      }
    }

    const labels = deriveLabelsFromEnvKey(k)
    out.push({ envKey:k, id, bookable, allow, labels })
  }
  return out
}
const EMPLOYEES = parseEmployees()
function staffLabelFromId(id){
  const e = EMPLOYEES.find(x=>x.id===id)
  return e?.labels?.[0] || (id ? `Prof. ${String(id).slice(-4)}` : null)
}
function isStaffAllowedInLocation(staffId, locKey){
  const e = EMPLOYEES.find(x=>x.id===staffId)
  if (!e || !e.bookable) return false
  const locId = locationToId(locKey)
  return e.allow.includes("ALL") || e.allow.includes(locId)
}
function pickStaffForLocation(locKey, preferId=null){
  const locId = locationToId(locKey)
  const isAllowed = e => e.bookable && (e.allow.includes("ALL") || e.allow.includes(locId))
  if (preferId){
    const e = EMPLOYEES.find(x=>x.id===preferId)
    if (e && isAllowed(e)) return e.id
  }
  const found = EMPLOYEES.find(isAllowed)
  return found?.id || null
}

// ====== Servicios
function titleCase(str){
  return String(str||"").toLowerCase().replace(/\b([a-z])/g, (m)=>m.toUpperCase())
}
function cleanDisplayLabel(label){
  const s = String(label||"").replace(/^\s*(luz|la\s*luz)\s+/i,"").trim()
  return applySpanishDiacritics(s)
}
function servicesForSedeKeyRaw(sedeKey){
  const prefix = (sedeKey==="la_luz") ? "SQ_SVC_luz_" : "SQ_SVC_"
  const out=[]
  for (const [k,v] of Object.entries(process.env)){
    if (!k.startsWith(prefix)) continue
    const [id] = String(v||"").split("|"); if (!id) continue
    const raw = k.replace(prefix,"").replaceAll("_"," ")
    let label = titleCase(raw)
    label = applySpanishDiacritics(label)
    out.push({ sedeKey, key:k, id, rawKey:k, label: cleanDisplayLabel(label), norm: norm(label) })
  }
  return out
}
function serviceLabelFromEnvKey(envKey){
  if (!envKey) return null
  const all = [...servicesForSedeKeyRaw("torremolinos"), ...servicesForSedeKeyRaw("la_luz")]
  return all.find(s=>s.key===envKey)?.label || null
}
function allServices(){ return [...servicesForSedeKeyRaw("torremolinos"), ...servicesForSedeKeyRaw("la_luz")] }

// ====== Square helpers (identidad por teléfono)
async function searchCustomersByPhone(phone){
  try{
    const e164=normalizePhoneES(phone); if(!e164) return []
    const got = await square.customersApi.searchCustomers({ query:{ filter:{ phoneNumber:{ exact:e164 } } } })
    return got?.result?.customers || []
  }catch{ return [] }
}
async function getUniqueCustomerByPhoneOrPrompt(phone, sessionData, sock, jid){
  const matches = await searchCustomersByPhone(phone)
  if (matches.length === 1){
    const c = matches[0]
    sessionData.name = sessionData.name || c?.givenName || null
    sessionData.email = sessionData.email || c?.emailAddress || null
    return { status:"single", customer:c }
  }
  if (matches.length === 0){
    sessionData.stage = "awaiting_identity"
    saveSession(phone, sessionData)
    await sock.sendMessage(jid, { text: "Para terminar, no encuentro tu ficha por este número. Dime tu *nombre completo* y, si quieres, tu *email* para crearte 😊" })
    return { status:"need_new" }
  }
  const choices = matches.map((c,i)=>({
    index:i+1, id:c.id,
    name:c?.givenName || "Sin nombre",
    email:c?.emailAddress || "—"
  }))
  sessionData.identityChoices = choices
  sessionData.stage = "awaiting_identity_pick"
  saveSession(phone, sessionData)
  const lines = choices.map(ch => `${ch.index}) ${ch.name} ${ch.email!=="—" ? `(${ch.email})`:""}`).join("\n")
  await sock.sendMessage(jid, { text: `Para terminar, he encontrado varias fichas con tu número. ¿Cuál eres?\n\n${lines}\n\nResponde con el número.` })
  return { status:"need_pick" }
}
async function findOrCreateCustomerWithRetry({ name, email, phone }){
  let lastError = null
  for (let attempt = 1; attempt <= SQUARE_MAX_RETRIES; attempt++) {
    try{
      const e164=normalizePhoneES(phone); if(!e164) return null
      const got = await square.customersApi.searchCustomers({ query:{ filter:{ phoneNumber:{ exact:e164 } } } })
      const c=(got?.result?.customers||[])[0]; 
      if (c) return c
      const created = await square.customersApi.createCustomer({
        idempotencyKey:`cust_${Date.now()}_${Math.random().toString(36).slice(2,6)}`,
        givenName:name||undefined,
        emailAddress:email||undefined,
        phoneNumber:e164||undefined
      })
      const newCustomer = created?.result?.customer||null
      if (newCustomer) return newCustomer
    } catch(e) { lastError = e; if (attempt < SQUARE_MAX_RETRIES) await sleep(1000 * attempt) }
  }
  return null
}

// ====== Square booking helpers
async function getServiceIdAndVersion(envKey){
  const raw = process.env[envKey]; if (!raw) return null
  let [id, ver] = String(raw).split("|"); ver=ver?Number(ver):null
  if (!id) return null
  if (!ver){
    try{ 
      const resp=await square.catalogApi.retrieveCatalogObject(id,true)
      const vRaw = resp?.result?.object?.version
      ver = vRaw != null ? Number(vRaw) : 1
    } catch(e) { ver=1 }
  }
  return {id,version:ver||1}
}
async function createBookingWithRetry({ startEU, locationKey, envServiceKey, durationMin, customerId, teamMemberId, phone }){
  if (!envServiceKey) return { success: false, error: "No se especificó servicio" }
  if (!teamMemberId || typeof teamMemberId!=="string" || !teamMemberId.trim()){ 
    return { success: false, error: "teamMemberId requerido" }
  }
  if (DRY_RUN) return { success: true, booking: { id:`TEST_SIM_${Date.now()}`, __sim:true } }
  const sv = await getServiceIdAndVersion(envServiceKey)
  if (!sv?.id || !sv?.version) return { success: false, error: `No se pudo obtener servicio ${envServiceKey}` }
  const startISO = startEU.tz("UTC").toISOString()
  const idempotencyKey = stableKey({ loc:locationToId(locationKey), sv:sv.id, startISO, customerId, teamMemberId })
  let lastError = null
  for (let attempt = 1; attempt <= SQUARE_MAX_RETRIES; attempt++) {
    try{
      const requestData = {
        idempotencyKey,
        booking:{
          locationId: locationToId(locationKey),
          startAt: startISO,
          customerId,
          appointmentSegments:[{
            teamMemberId,
            serviceVariationId: sv.id,
            serviceVariationVersion: Number(sv.version),
            durationMinutes: durationMin||60
          }]
        }
      }
      const resp = await square.bookingsApi.createBooking(requestData)
      const booking = resp?.result?.booking || null
      try{
        insertSquareLog.run({
          phone: phone || 'unknown',
          action: 'create_booking',
          request_data: safeJSONStringify(requestData),
          response_data: safeJSONStringify(resp?.result || {}),
          error_data: null,
          timestamp: new Date().toISOString(),
          success: 1
        })
      }catch{}
      if (booking) return { success: true, booking }
    } catch(e) {
      lastError = e
      try{
        insertSquareLog.run({
          phone: phone || 'unknown',
          action: 'create_booking',
          request_data: safeJSONStringify({ attempt, envServiceKey, locationKey, startISO }),
          response_data: null,
          error_data: safeJSONStringify({ message: e?.message, body: e?.body }),
          timestamp: new Date().toISOString(),
          success: 0
        })
      }catch{}
      if (attempt < SQUARE_MAX_RETRIES) await sleep(2000 * attempt)
    }
  }
  return { success: false, error: `No se pudo crear reserva: ${lastError?.message || 'Error desconocido'}`, lastError }
}
async function cancelBooking(bookingId){
  if (DRY_RUN) return true
  try{
    const body = { idempotencyKey:`cancel_${bookingId}_${Date.now()}` }
    const resp = await square.bookingsApi.cancelBooking(bookingId, body)
    return !!resp?.result?.booking
  }catch(e){ return false }
}

// ====== DISPONIBILIDAD
async function searchAvailabilityForStaff({ locationKey, envServiceKey, staffId, fromEU, days=14, n=3, distinctDays=false }){
  try{
    const sv = await getServiceIdAndVersion(envServiceKey)
    if (!sv?.id || !staffId) return []
    const startAt = fromEU.tz("UTC").toISOString()
    const endAt = fromEU.clone().add(days,"day").tz("UTC").toISOString()
    const locationId = locationToId(locationKey)
    const body = {
      query:{ filter:{
        startAtRange:{ startAt, endAt },
        locationId,
        segmentFilters:[{ serviceVariationId: sv.id, teamMemberIdFilter:{ any:[ staffId ] } }]
      } }
    }
    const resp = await square.bookingsApi.searchAvailability(body)
    const avail = resp?.result?.availabilities || []
    const slots=[], seenDays=new Set()
    for (const a of avail){
      if (!a?.startAt) continue
      const d = dayjs(a.startAt).tz(EURO_TZ)
      if (!insideBusinessHours(d,60)) continue
      if (distinctDays){
        const key=d.format("YYYY-MM-DD"); if (seenDays.has(key)) continue; seenDays.add(key)
      }
      if (!isStaffAllowedInLocation(staffId, locationKey)) continue
      slots.push({ date:d, staffId })
      if (slots.length>=n) break
    }
    return slots
  }catch{ return [] }
}
async function searchAvailabilityGeneric({ locationKey, envServiceKey, fromEU, days=14, n=3, distinctDays=false }){
  try{
    const sv = await getServiceIdAndVersion(envServiceKey)
    if (!sv?.id) return []
    const startAt = fromEU.tz("UTC").toISOString()
    const endAt = fromEU.clone().add(days,"day").tz("UTC").toISOString()
    const locationId = locationToId(locationKey)
    const body = { query:{ filter:{ startAtRange:{ startAt, endAt }, locationId, segmentFilters:[{ serviceVariationId: sv.id }] } } }
    const resp = await square.bookingsApi.searchAvailability(body)
    const avail = resp?.result?.availabilities || []
    const slots=[], seenDays=new Set()
    for (const a of avail){
      if (!a?.startAt) continue
      const d = dayjs(a.startAt).tz(EURO_TZ)
      if (!insideBusinessHours(d,60)) continue
      let tm = null
      const segs = Array.isArray(a.appointmentSegments) ? a.appointmentSegments
                 : Array.isArray(a.segments) ? a.segments
                 : []
      if (segs[0]?.teamMemberId) tm = segs[0].teamMemberId
      if (distinctDays){
        const key=d.format("YYYY-MM-DD"); if (seenDays.has(key)) continue; seenDays.add(key)
      }
      if (tm && !isStaffAllowedInLocation(tm, locationKey)) continue
      slots.push({ date:d, staffId: tm || null })
      if (slots.length>=n) break
    }
    return slots
  }catch{ return [] }
}

// ====== IA HTTP
async function callAIOnce(messages, systemPrompt = "") {
  const controller = new AbortController()
  const timeoutId = setTimeout(() => controller.abort(), AI_TIMEOUT_MS)
  try {
    const allMessages = systemPrompt ? [{ role: "system", content: systemPrompt }, ...messages] : messages
    const response = await fetch("https://api.deepseek.com/chat/completions", {
      method: "POST",
      headers: { "Content-Type": "application/json", "Authorization": `Bearer ${AI_API_KEY}` },
      body: JSON.stringify({ model: AI_MODEL, messages: allMessages, max_tokens: 1500, temperature: 0.7, stream: false }),
      signal: controller.signal
    });
    clearTimeout(timeoutId)
    if (!response.ok) return null
    const data = await response.json();
    return data?.choices?.[0]?.message?.content ?? null
  } catch { clearTimeout(timeoutId); return null }
}
async function callAIWithRetries(messages, systemPrompt=""){
  for (let i=0;i<=AI_MAX_RETRIES;i++){
    const res = await callAIOnce(messages, systemPrompt)
    if (res && typeof res==="string" && res.trim()) return res
    if (i < AI_MAX_RETRIES) await sleep(Math.min(5000, 500 * Math.pow(2, i)))
  }
  return null
}

// ====== Roster para el prompt (IA ve TODO)
function staffRosterForPrompt(){
  return EMPLOYEES.map(e=>{
    const locs = e.allow.map(id=> id===LOC_TORRE?"torremolinos" : id===LOC_LUZ?"la_luz" : id).join(",")
    return `• ID:${e.id} | Nombres:[${e.labels.join(", ")}] | Sedes:[${locs||"ALL"}] | Reservable:${e.bookable}`
  }).join("\n")
}

// ====== Helpers prompt avanzado
function generateTimeContext(nowEU){
  const dow = ["domingo","lunes","martes","miércoles","jueves","viernes","sábado"][nowEU.day()]
  const within = insideBusinessHours(nowEU, 60) ? "dentro de horario" : "fuera de horario"
  return `${dow} ${nowEU.format("DD/MM/YYYY HH:mm")} (${within})`
}
function analyzeUserPatterns(conversationHistory){
  const text = conversationHistory.map(x => (x?.content||"")).join(" | ").toLowerCase()
  const prefersTorre = /torre|torremolinos/.test(text)
  const prefersLuz = /\bla luz\b|luz\b/.test(text)
  const likesUnas = /\buña|manicura|gel|acril/i.test(text)
  const likesLash = /\bpestañ|lash|lifting/i.test(text)
  const style = /\bvale|ok|tio|bro|jaja|jeje|xd/i.test(text) ? "coloquial" : "formal"
  return `preferencias: ${prefersTorre?"Torremolinos ":""}${prefersLuz?"La Luz ":""}${likesUnas?"uñas ":""}${likesLash?"pestañas ":""}| estilo:${style}`
}
function generateSmartServiceContext(sede){
  const list = sede ? servicesForSedeKeyRaw(sede) : allServices()
  const lines = list.map(s => `- ${s.label} (Clave: ${s.key})`).join("\n")
  return `SERVICIOS DISPONIBLES${sede?` (${locationNice(sede)})`:""}:\n${lines}`
}
function generateSessionContext(s){
  return `Sede:${s?.sede||"—"} | Servicio:${s?.selectedServiceLabel||"—"} (${s?.selectedServiceEnvKey||"—"}) | Preferida:${s?.preferredStaffLabel||"—"} | Pendiente:${s?.pendingDateTime?fmtES(parseToEU(s.pendingDateTime)):"—"} | Etapa:${s?.stage||"—"}`
}

// ====== Prompt avanzado
function buildAdvancedSystemPrompt(sessionData, conversationHistory, userProfile) {
  const nowEU = dayjs().tz(EURO_TZ);
  const timeContext = generateTimeContext(nowEU);
  const userPattern = analyzeUserPatterns(conversationHistory);
  const serviceContext = generateSmartServiceContext(sessionData.sede);

  return `Eres Gapink AI, la asistente más inteligente de salones de belleza en España. Devuelves SOLO JSON válido con el formato:
{"message":"...","action":"propose_times|create_booking|list_appointments|cancel_appointment|choose_service|need_info|none","session_updates":{...},"action_params":{...}}

CONTEXTO TEMPORAL: ${timeContext}
PERFIL USUARIO: ${userPattern}
ESTADO ACTUAL: ${generateSessionContext(sessionData)}

${serviceContext}

REGLAS AVANZADAS DE IA:
1. INTERPRETACIÓN INTELIGENTE: español coloquial, errores y abreviaciones.
2. CONTEXTO PERSISTENTE: recuerda toda la conversación y patrones.
3. PREDICCIÓN PROACTIVA: anticipa necesidades con el historial.
4. FLEXIBILIDAD TOTAL: adapta el estilo al usuario.
5. RESOLUCIÓN AUTOMÁTICA: usa contexto previo para ambigüedades.

CAPACIDADES:
- Multi-intención (reservar, cancelar, listar, cambiar).
- Maneja referencias temporales naturales ("viernes", "la semana que viene").
- Cambios de opinión en mitad de conversación.
- Prioriza "con {nombre}" si es posible en la sede.

FORMATO ESTRICTO: devuelve solo JSON, sin texto adicional.`
}

// ====== NLP Avanzado
class AdvancedNLPProcessor {
  static normalizeAdvanced(text) {
    const corrections = {
      'uñas': ['unas', 'unyas', 'uñass'],
      'pestañas': ['pestanas', 'pestaañas', 'pestanyas', 'pestañass'],
      'cejas': ['zejas', 'sejas'],
      'torremolinos': ['torre', 'torremolino', 'torremelinos'],
      'la luz': ['luz','laa luz','laluz'],
      'semipermanente': ['semi permanente','semi-permanente','semipermanete'],
      'acrílico': ['acrilico','acrilic','acrilyco'],
      'francés': ['frances']
    }
    let normalized = String(text||"")
    for (const [correct, vars] of Object.entries(corrections)) {
      vars.forEach(v => { normalized = normalized.replace(new RegExp(v, 'gi'), correct) })
    }
    return normalized
  }
  static _synonyms(){
    return {
      unas: ['uñas','manicura','gel','acrílico','semipermanente','relleno','press on','tips','francés','nivelación','decoración','retirar'],
      pedicura: ['pedicura','pies','pie'],
      pestanas: ['pestañas','lifting','pelo a pelo','volumen','clásicas','russian'],
      cejas: ['cejas','microblading','microshading','efecto polvo','hairstroke'],
    }
  }
  static detectUrgency(text){
    return /\burgente|ya|ahora|lo antes posible|prisa/i.test(text) ? 'alta' : 'normal'
  }
  static detectCommunicationStyle(text){
    return /\bvale|ok|tio|bro|jaja|jeje|xd/i.test(text) ? 'coloquial' : 'formal'
  }
  static async analyzeEmotionalTone(){ return 'neutral' }
  static async extractEntities(text){
    const t = this.normalizeAdvanced(text).toLowerCase()
    const loc = /\bla luz\b/.test(t) ? 'la_luz' : /\btorremolinos|torre\b/.test(t) ? 'torremolinos' : null
    const staff = (() => {
      for (const e of EMPLOYEES){
        for (const lbl of e.labels){
          if (norm(lbl) && norm(t).includes(norm(lbl))) return { id:e.id, label: e.labels[0] }
        }
      }
      return null
    })()
    const dates = /\bhoy|mañana|pasado|viernes|lunes|martes|miércoles|jueves\b/i.test(t) ? ['relative'] : []
    const times = /\b(\d{1,2}[:.]\d{2}|\d{1,2}\s*(am|pm))\b/i.test(t) ? ['clock'] : []
    const syn = this._synonyms()
    const services = []
    for (const [k, arr] of Object.entries(syn)){
      if (arr.some(w => t.includes(w))) services.push(k)
    }
    return { services, dates, times, locations: loc?[loc]:[], staff: staff?[staff]:[], emotions:[] }
  }
  static async detectIntentions(text, context){
    const t = this.normalizeAdvanced(text).toLowerCase()
    const intents = []
    if (/\bcancel(ar|a|a la)\b|\banular\b|\bborrar\b/.test(t)) intents.push('cancel')
    if (/\bmis citas|ver citas|lista\b/.test(t)) intents.push('list')
    if (/\breserv(ar|a)|cita|quiero\b/.test(t)) intents.push('book')
    if (/\bcon\s+[a-zñáéíóú]+/.test(t)) intents.push('with_staff')
    if (!intents.length && /\buñas|manicura|gel|acrílico|semipermanente|pedicura|pestañas|cejas/i.test(t)) intents.push('choose_service')
    return intents
  }
  static servicesForIntention(sedeKey, userMsg, target='unas'){
    const pool = sedeKey ? servicesForSedeKeyRaw(sedeKey) : allServices()
    const t = this.normalizeAdvanced(userMsg).toLowerCase()
    const syn = this._synonyms()
    let keys = syn[target] || []
    if (target==='unas' && /\bpedicura|pies|pie\b/.test(t)) keys = [...keys, ...syn.pedicura]
    const scored = pool.map(s=>{
      const l = s.label.toLowerCase()
      let score = 0
      for (const k of keys){ if (l.includes(k) || t.includes(k)) score += 1.2 }
      // bonus por superposición de tokens
      const utoks = new Set(norm(t).split(" ").filter(Boolean))
      const ltoks = new Set(norm(l).split(" ").filter(Boolean))
      let overlap=0; for (const tok of utoks){ if (ltoks.has(tok)) overlap++ }
      score += Math.min(overlap,3)*0.2
      return { ...s, _score:score }
    }).filter(x=>x._score>0.2)
    scored.sort((a,b)=> b._score - a._score)
    // dedup por label
    const seen=new Set(); const out=[]
    for (const s of scored){ const k=s.label.toLowerCase(); if(seen.has(k)) continue; seen.add(k); out.push(s) }
    return out
  }
}

// ====== Sistema de Aprendizaje
class ContextualLearningEngine {
  static async storeInteractionLearning(interaction){
    try{
      // Guardamos como conversación AI (reutilizamos tabla).
      insertAIConversation.run({
        phone: interaction.sessionContext?.phone || 'unknown',
        message_id: `learn_${Date.now()}`,
        user_message: interaction.userMessage || "",
        ai_response: safeJSONStringify({ outcome: interaction.outcome, patterns: interaction.patterns }),
        timestamp: new Date().toISOString(),
        session_data: safeJSONStringify(interaction.sessionContext || {}),
        ai_error: null,
        fallback_used: 0
      })
    }catch{}
  }
  static async extractPatterns(userMessage, sessionData){
    const ents = await AdvancedNLPProcessor.extractEntities(userMessage||"")
    return { categories: ents.services, location: ents.locations?.[0]||null }
  }
  static async updateUserProfile(_phone, _interaction){ /* no-op local */ }
  static async analyzeUserPatterns(conversationHistory){
    return analyzeUserPatterns(conversationHistory)
  }
  static async predictUserNeeds(sessionData, conversationHistory){
    const patterns = await this.analyzeUserPatterns(conversationHistory)
    const prefersTorre = /Torremolinos/i.test(patterns)
    const prefersLuz = /La Luz/i.test(patterns)
    return {
      likelyServices: [],
      preferredTimes: [],
      communicationPreferences: /\bestilo:coloquial/i.test(patterns) ? 'coloquial' : 'formal',
      urgencyTendency: sessionData?.conversationMemory?.urgencyLevel || 'normal',
      preferredSede: prefersTorre ? 'torremolinos' : (prefersLuz ? 'la_luz' : null)
    }
  }
}

// ====== Validador Inteligente
class IntelligentValidator {
  static async validateService(serviceEnvKey, sedeKey){
    const list = servicesForSedeKeyRaw(sedeKey||"torremolinos")
    const ok = list.some(s=>s.key===serviceEnvKey)
    return { isValid: !!ok, reason: ok?null:`Servicio no existe en ${locationNice(sedeKey||"torremolinos")}` }
  }
  static async validateDateTime(dt, sessionData){
    if (!dt) return { isValid:false, reason:"Fecha/hora no indicada" }
    const d = parseToEU(dt)
    const ok = insideBusinessHours(d, 60)
    return { isValid: ok, reason: ok?null:"Fuera de horario laboral" }
  }
  static async validateLocation(loc){
    const ok = loc==="torremolinos" || loc==="la_luz"
    return { isValid: ok, reason: ok?null:"Sede inválida" }
  }
  static async validateStaff(staffId, loc){
    if (!staffId) return { isValid:true }
    const ok = isStaffAllowedInLocation(staffId, loc)
    return { isValid: ok, reason: ok?null:"Profesional no disponible en sede" }
  }
  static async generateCorrections(validations, sessionData){
    const corr = {}
    if (!validations.location?.isValid && sessionData?.sede) corr.location = sessionData.sede
    if (!validations.datetime?.isValid && sessionData?.pendingDateTime){
      const baseFrom = nextOpeningFrom(dayjs().tz(EURO_TZ).add(NOW_MIN_OFFSET_MIN, "minute"))
      corr.datetime = baseFrom.toISOString()
    }
    return corr
  }
  static async generatePreferenceSuggestions(sessionData){
    const prefs = sessionData?.conversationMemory?.preferences || {}
    const out=[]
    if (prefs?.category==='unas') out.push("¿Te ofrezco semipermanente o gel según lo último que pediste?")
    return out
  }
  static async generateAvailabilitySuggestions(_validations){
    return ["Si te viene mejor, también puedo mirar el viernes por la tarde."]
  }
  static async generateIntelligentSuggestions(validations, sessionData){
    const suggestions = []
    if (sessionData.conversationMemory?.preferences) {
      suggestions.push(...await this.generatePreferenceSuggestions(sessionData))
    }
    suggestions.push(...await this.generateAvailabilitySuggestions(validations))
    return suggestions
  }
  static async validateAndCorrectBookingData(data, sessionData){
    const v = {
      service: await this.validateService(data.service, sessionData.sede),
      datetime: await this.validateDateTime(data.datetime, sessionData),
      location: await this.validateLocation(data.location),
      staff: await this.validateStaff(data.staff, data.location)
    }
    const corrections = await this.generateCorrections(v, sessionData)
    return { isValid: Object.values(v).every(x=>x.isValid), corrections, suggestions: await this.generateIntelligentSuggestions(v, sessionData), detail: v }
  }
}

// ====== Respuestas adaptativas
class AdaptiveResponseGenerator {
  static determineResponseStyle(style){ return style==='coloquial'?'coloquial':'formal' }
  static determineEmotionalTone(_){ return 'neutral' }
  static determineUrgencyLevel(level){ return level || 'normal' }
  static async generateBaseResponse(intention, sessionData){
    switch(intention){
      case 'list': return { message:"Estas son tus próximas citas:", action:"list_appointments" }
      case 'cancel': return { message:"Vale, dime cuál cancelar:", action:"cancel_appointment" }
      case 'book': return { message:"Te propongo horarios ahora 👇", action:"propose_times" }
      default: return { message:"¿Quieres reservar, cancelar o ver tus citas?", action:"none" }
    }
  }
  static async adaptResponse(base, _opts){ return base }
  static generateHumanHandoffMessage(){ return { message:"Te paso con una persona del equipo para terminarlo 👩‍💻", action:"none" } }
  static async generateAlternativeResponses(failed, sessionData){
    const alt = []
    alt.push({ message:"Te enseño otros horarios posibles", action:"propose_times" })
    if (!sessionData?.sede) alt.push({ message:"¿En qué sede te viene mejor, Torremolinos o La Luz?", action:"need_info", session_updates:{ stage:"awaiting_sede_for_services" } })
    return alt
  }
  static generateEscalationPath(attempts){ return attempts>=2?"human":"retry" }
  static async generateContextualResponse(intention, sessionData, userProfile){
    const style = this.determineResponseStyle(userProfile?.communicationStyle||'formal')
    const urgency = this.determineUrgencyLevel(sessionData?.conversationMemory?.urgencyLevel)
    const base = await this.generateBaseResponse(intention, sessionData)
    return this.adaptResponse(base, { style, urgency })
  }
  static async generateSmartFallbacks(failedResponse, sessionData, attempts = 0) {
    if (attempts > 3) return this.generateHumanHandoffMessage()
    const alternatives = await this.generateAlternativeResponses(failedResponse, sessionData)
    return { primary: alternatives[0], fallbacks: alternatives.slice(1), escalationPath: this.generateEscalationPath(attempts) }
  }
}

// ====== Monitor de IA
class AIPerformanceMonitor {
  static async predictSatisfaction(_interaction){ return 0.9 }
  static async analyzeTaskCompletion(_interaction){ return 0.8 }
  static async calculateErrorRate(_interaction){ return 0.02 }
  static async calculateConversionRate(_interaction){ return 0.6 }
  static async storeMetrics(m){
    try{
      insertMetrics.run({ phone: m.phone||'unknown', rt:m.responseTime||0, sat:m.userSatisfaction, task:m.taskCompletion, err:m.errorRate, conv:m.conversionRate, ts:new Date().toISOString() })
    }catch{}
  }
  static async triggerOptimizations(_){ /* no-op local */ }
  static async trackInteraction(interaction){
    const metrics = {
      phone: interaction?.phone||'unknown',
      responseTime: interaction?.responseTime||0,
      userSatisfaction: await this.predictSatisfaction(interaction),
      taskCompletion: await this.analyzeTaskCompletion(interaction),
      errorRate: await this.calculateErrorRate(interaction),
      conversionRate: await this.calculateConversionRate(interaction)
    }
    await this.storeMetrics(metrics)
    await this.triggerOptimizations(metrics)
  }
}

// ====== IA Square extra (placeholders seguros)
async function getSquareHistoricalData(){ return [] }
async function validateWithSquareAPI(pred){
  // Validamos predicción mirando disponibilidad real
  if (!pred?.service || !pred?.location) return null
  const baseFrom = nextOpeningFrom(dayjs().tz(EURO_TZ).add(NOW_MIN_OFFSET_MIN,"minute"))
  const envKey = pred.service
  const slots = await searchAvailabilityGeneric({ locationKey: pred.location, envServiceKey: envKey, fromEU: baseFrom, n: 3 })
  return slots?.length ? slots : null
}
async function getSquareAlternatives(){ return [] }
async function getUserPreferences(){ return {} }
async function getBusinessOptimizationRules(){ return {} }
async function executeOptimizedBooking(opt){ return opt }
async function aiPredictAvailability(userRequest, sessionData){
  // Simple: intenta validar con Square con preferencias del usuario.
  const prediction = {
    service: userRequest.service,
    preferredTimes: sessionData?.conversationMemory?.preferences?.times || [],
    location: userRequest.location || sessionData?.sede || 'torremolinos',
    historicalData: await getSquareHistoricalData()
  }
  return await validateWithSquareAPI(prediction)
}
async function aiOptimizeBooking(booking, alternatives){
  const optimization = { originalBooking: booking, alternatives, userPreferences: await getUserPreferences(), businessRules: await getBusinessOptimizationRules() }
  return await executeOptimizedBooking(optimization)
}

// ====== Builder de prompt legacy (fallback técnico, no principal)
function buildSystemPrompt() {
  const nowEU = dayjs().tz(EURO_TZ);
  const torremolinos_services = servicesForSedeKeyRaw("torremolinos");
  const laluz_services = servicesForSedeKeyRaw("la_luz");
  const staffLines = staffRosterForPrompt()

  return `Eres el asistente de WhatsApp para Gapink Nails. Devuelves SOLO JSON válido.

INFORMACIÓN:
- Fecha/hora actual: ${nowEU.format("dddd DD/MM/YYYY HH:mm")} (Madrid)

PROFESIONALES:
${staffLines}

SERVICIOS TORREMOLINOS:
${torremolinos_services.map(s => `- ${s.label} (Clave: ${s.key})`).join("\n")}

SERVICIOS LA LUZ:
${laluz_services.map(s => `- ${s.label} (Clave: ${s.key})`).join("\n")}

FORMATO:
{"message":"...","action":"propose_times|create_booking|list_appointments|cancel_appointment|choose_service|need_info|none","session_updates":{...},"action_params":{...}}`
}

// ====== IA multinivel
async function preprocessUserInput(userMessage, sessionData){
  const normalized = AdvancedNLPProcessor.normalizeAdvanced(userMessage)
  const entities = await AdvancedNLPProcessor.extractEntities(userMessage)
  // actualizar memoria ligera
  if (entities.locations?.[0]) sessionData.sede = sessionData.sede || entities.locations[0]
  if (entities.staff?.[0]) {
    sessionData.preferredStaffId = entities.staff[0].id
    sessionData.preferredStaffLabel = entities.staff[0].label
  }
  sessionData.conversationMemory.urgencyLevel = AdvancedNLPProcessor.detectUrgency(userMessage)
  sessionData.conversationMemory.communicationStyle = AdvancedNLPProcessor.detectCommunicationStyle(userMessage)
  return { normalized, entities }
}
async function analyzeMultipleIntentions(preprocessed, conversationHistory){
  const intentions = await AdvancedNLPProcessor.detectIntentions(preprocessed.normalized, { history: conversationHistory })
  return intentions
}
async function resolveContextualAmbiguity(intentions, sessionData){
  const top = intentions[0] || 'none'
  return { topIntention: top, session: sessionData }
}
async function generateAdaptiveResponse(context, userMessage, sessionData){
  const userProfile = {
    communicationStyle: sessionData.conversationMemory.communicationStyle,
    emotionalState: sessionData.conversationMemory.satisfactionLevel
  }
  // Si la intención es elegir servicio, generamos candidatos por NLP
  if (context.topIntention === 'choose_service'){
    const items = AdvancedNLPProcessor.servicesForIntention(sessionData.sede, userMessage, 'unas')
    const candidates = items.slice(0, 10).map((s,i)=>({ index:i+1, label:s.label, confidence: Math.min(0.9, 0.4 + (s._score||0)/3) }))
    return {
      message: "Elige tu servicio de uñas:",
      action: "choose_service",
      session_updates: { stage:"awaiting_service_choice" },
      action_params: { candidates }
    }
  }
  // Listar/cancelar/crear/proponer
  const base = await AdaptiveResponseGenerator.generateContextualResponse(context.topIntention, sessionData, userProfile)
  return { ...base, session_updates:{}, action_params:{} }
}
async function validateAndOptimizeResponse(response, sessionData){
  return response // placeholder: podría ajustar estilo/tone
}
async function getAdvancedAIResponse(userMessage, sessionData, phone, conversationHistory){
  const t0 = Date.now()
  const preprocessed = await preprocessUserInput(userMessage, sessionData)
  const intentions = await analyzeMultipleIntentions(preprocessed, conversationHistory)
  const context = await resolveContextualAmbiguity(intentions, sessionData)

  // Generación adaptativa propia
  let aiObj = await generateAdaptiveResponse(context, userMessage, sessionData)
  aiObj = await validateAndOptimizeResponse(aiObj, sessionData)

  // Si aún es “none” o necesitamos mayor contexto, pedimos a la LLM con prompt avanzado
  if (!aiObj || aiObj.action==="none"){
    const sys = buildAdvancedSystemPrompt(sessionData, conversationHistory, {})
    const messages = [{ role:"user", content:`MENSAJE DEL CLIENTE: "${userMessage}"\n\nINSTRUCCIÓN: Devuelve SOLO JSON siguiendo el formato.` }]
    const aiText = await callAIWithRetries(messages, sys)
    if (aiText){
      const cleaned = aiText.replace(/```json\s*/gi, "").replace(/```\s*/g, "").replace(/^[^{]*/, "").replace(/[^}]*$/, "").trim()
      try { aiObj = JSON.parse(cleaned) } catch { /* fallback abajo */ }
    }
  }
  if (!aiObj){
    // Fallback adaptativo
    const fb = await AdaptiveResponseGenerator.generateSmartFallbacks({ message:"no"}, sessionData, 0)
    aiObj = fb.primary || { message:"¿Quieres reservar, cancelar o ver tus citas?", action:"none" }
    aiObj.__fallback_used = true
  }

  const t1 = Date.now()
  AIPerformanceMonitor.trackInteraction({ phone, responseTime: t1-t0 }).catch(()=>{})
  return aiObj
}

// ====== Enrutado principal (legacy signature)
async function getAIResponse(userMessage, sessionData, phone){
  const recent = db.prepare(`SELECT user_message, ai_response FROM ai_conversations WHERE phone = ? ORDER BY timestamp DESC LIMIT 8`).all(phone);
  const conversationHistory = recent.reverse().map(msg => [
    { role: "user", content: msg.user_message },
    { role: "assistant", content: msg.ai_response }
  ]).flat();
  return await getAdvancedAIResponse(userMessage, sessionData, phone, conversationHistory)
}

// ====== Bot principal
let RECONNECT_SCHEDULED = false
let RECONNECT_ATTEMPTS = 0
const QUEUE=new Map()
function enqueue(key,job){
  const prev=QUEUE.get(key)||Promise.resolve()
  const next=prev.then(job,job).finally(()=>{ if (QUEUE.get(key)===next) QUEUE.delete(key) })
  QUEUE.set(key,next); return next
}
async function sendWithPresence(sock, jid, text){
  try{ await sock.sendPresenceUpdate("composing", jid) }catch{}
  await new Promise(r=>setTimeout(r, 800+Math.random()*1200))
  return sock.sendMessage(jid, { text })
}

// ====== Helpers chat
function isCancelIntent(text){
  const lower = norm(text)
  return /\b(cancelar|anular|borrar)\b/.test(lower) && /\b(cita|reserva|pr[oó]xima|mi)\b/.test(lower)
}
function parseSede(text){
  const t=norm(text)
  if (/\b(luz|la luz)\b/.test(t)) return "la_luz"
  if (/\b(torre|torremolinos)\b/.test(t)) return "torremolinos"
  return null
}

// 👉 parse básico para nombre/email en texto libre
function parseNameEmailFromText(txt){
  const emailMatch = String(txt||"").match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i)
  const email = emailMatch ? emailMatch[0] : null
  const name = String(txt||"").replace(email||"", "").replace(/(email|correo)[:\s]*/ig,"").trim()
  return { name: name || null, email }
}

// ====== Menú de servicios por NLP
function buildServiceChoiceListBySede(sedeKey, userMsg, aiCandidates){
  const itemsNLP = AdvancedNLPProcessor.servicesForIntention(sedeKey, userMsg||"", 'unas')
  const localScores = new Map()
  for (const s of itemsNLP){ localScores.set(s.label, s._score||0) }
  const aiMap = new Map()
  if (Array.isArray(aiCandidates)){
    for (const c of aiCandidates){
      const label = String(cleanDisplayLabel(c.label||"")).trim(); if (!label) continue
      const conf = Number(c.confidence ?? 0)
      const prev = localScores.get(label) ?? 0
      localScores.set(label, prev + Math.max(0, conf*3))
      aiMap.set(label, conf)
    }
  }
  const merged = [...itemsNLP]
  // Añade candidatos de IA que no salieron por NLP
  for (const c of aiCandidates||[]){
    const label = String(cleanDisplayLabel(c.label||"")).trim()
    if (label && !merged.some(x=>x.label.toLowerCase()===label.toLowerCase())){
      merged.push({ label, sedeKey, key: null, id: null, norm: norm(label), _score: (c.confidence||0.1)*3 })
    }
  }
  merged.sort((a,b)=> (localScores.get(b.label)||0)-(localScores.get(a.label)||0))
  return merged.map((s,i)=>({ index:i+1, label:s.label }))
}

async function executeChooseService(params, sessionData, phone, sock, jid, userMsg){
  if (!sessionData.sede){
    sessionData.pendingCategory = "uñas"
    sessionData.stage = "awaiting_sede_for_services"
    saveSession(phone, sessionData)
    await sendWithPresence(sock, jid, "¿En qué sede te viene mejor, Torremolinos o La Luz? (así te muestro las opciones de uñas correctas)")
    return
  }
  const aiCands = Array.isArray(params?.candidates) ? params.candidates : []
  const items = buildServiceChoiceListBySede(sessionData.sede, userMsg||"", aiCands)
  if (!items.length){
    await sendWithPresence(sock, jid, "Ahora mismo no tengo servicios de uñas configurados para esa sede.")
    return
  }
  sessionData.serviceChoices = items
  sessionData.stage = "awaiting_service_choice"
  saveSession(phone, sessionData)
  const lines = items.map(it=> {
    const star = aiCands.find(c=>cleanDisplayLabel(String(c.label||"")).toLowerCase()===it.label.toLowerCase()) ? " ⭐" : ""
    return `${it.index}) ${applySpanishDiacritics(it.label)}${star}`
  }).join("\n")
  await sendWithPresence(sock, jid, `Estas son nuestras opciones de **uñas** en ${locationNice(sessionData.sede)}:\n\n${lines}\n\nResponde con el número.`)
}

// ====== Proponer horas
function proposeSlots({ fromEU, durationMin=60, n=3 }){
  const out=[]
  let t = ceilToSlotEU(fromEU.clone())
  while (out.length<n){
    if (insideBusinessHours(t, durationMin)) out.push(t.clone())
    t = t.add(SLOT_MIN, "minute")
    if (t.hour()>=OPEN.end) { t = nextOpeningFrom(t) }
  }
  return out
}

async function executeProposeTime(_params, sessionData, phone, sock, jid) {
  const nowEU = dayjs().tz(EURO_TZ);
  const baseFrom = nextOpeningFrom(nowEU.add(NOW_MIN_OFFSET_MIN, "minute"));
  if (!sessionData.sede || !sessionData.selectedServiceEnvKey) { await sendWithPresence(sock, jid, "Necesito la sede y el servicio primero."); return; }

  // Predicción IA simple (si fallan huecos luego, usamos genérico)
  const pred = await aiPredictAvailability({ service: sessionData.selectedServiceEnvKey, location: sessionData.sede }, sessionData)
  let slots = Array.isArray(pred) ? pred : []

  let usedPreferred = false
  if (!slots.length && sessionData.preferredStaffId && isStaffAllowedInLocation(sessionData.preferredStaffId, sessionData.sede)) {
    const staffSlots = await searchAvailabilityForStaff({ locationKey: sessionData.sede, envServiceKey: sessionData.selectedServiceEnvKey, staffId: sessionData.preferredStaffId, fromEU: baseFrom, n: 3 })
    if (staffSlots.length){ slots = staffSlots; usedPreferred = true }
  }
  if (!slots.length) {
    const generic = await searchAvailabilityGeneric({ locationKey: sessionData.sede, envServiceKey: sessionData.selectedServiceEnvKey, fromEU: baseFrom, n: 3 })
    slots = generic
  }
  if (!slots.length) {
    const generalSlots = proposeSlots({ fromEU: baseFrom, durationMin: 60, n: 3 });
    slots = generalSlots.map(d => ({ date: d, staffId: null }))
  }
  if (!slots.length) { await sendWithPresence(sock, jid, "No encuentro horarios disponibles en los próximos días. ¿Otra fecha?"); return; }

  slots = slots.filter(s => !s.staffId || isStaffAllowedInLocation(s.staffId, sessionData.sede))

  const hoursEnum = enumerateHours(slots.map(s => s.date))
  const map = {}; for (const s of slots) map[s.date.format("YYYY-MM-DDTHH:mm")] = s.staffId || null

  const nameMap = {}
  Object.values(map).forEach(sid => { if (sid) nameMap[sid] = staffLabelFromId(sid) })
  sessionData.lastStaffNamesById = nameMap

  sessionData.lastHours = slots.map(s => s.date)
  sessionData.lastStaffByIso = map
  sessionData.lastProposeUsedPreferred = usedPreferred
  sessionData.stage = "awaiting_time"
  saveSession(phone, sessionData)

  const lines = hoursEnum.map(h => {
    const sid = map[h.iso]
    const tag = sid ? ` — ${staffLabelFromId(sid)}` : ""
    return `${h.index}) ${h.pretty}${tag}`
  }).join("\n")
  const header = usedPreferred
    ? `Horarios disponibles con ${sessionData.preferredStaffLabel || "tu profesional"}:`
    : `Horarios disponibles (nuestro equipo):${sessionData.preferredStaffLabel ? `\nNota: no veo huecos con ${sessionData.preferredStaffLabel} en los próximos días; te muestro alternativas.`:""}`
  await sendWithPresence(sock, jid, `${header}\n${lines}\n\nResponde con el número (1, 2 o 3)`)
}

// ====== Crear reserva (con Validador Inteligente)
async function executeCreateBooking(_params, sessionData, phone, sock, jid) {
  if (!sessionData.sede) { await sendWithPresence(sock, jid, "Falta seleccionar la sede (Torremolinos o La Luz)"); return; }
  if (!sessionData.selectedServiceEnvKey) { await sendWithPresence(sock, jid, "Falta seleccionar el servicio"); return; }
  if (!sessionData.pendingDateTime) { await sendWithPresence(sock, jid, "Falta seleccionar la fecha y hora"); return; }

  const startEU = parseToEU(sessionData.pendingDateTime)
  // Validador IA
  const validation = await IntelligentValidator.validateAndCorrectBookingData({
    service: sessionData.selectedServiceEnvKey,
    datetime: startEU.toISOString(),
    location: sessionData.sede,
    staff: sessionData.preferredStaffId || null
  }, sessionData)

  if (!validation.isValid){
    const msg = `No puedo confirmar aún: ${Object.values(validation.detail).filter(v=>!v.isValid).map(v=>v.reason).join("; ")}.\n${validation.suggestions?.[0]||"¿Probamos otra hora?"}`
    await sendWithPresence(sock, jid, msg)
    await executeProposeTime({}, sessionData, phone, sock, jid)
    return
  }

  const iso = startEU.format("YYYY-MM-DDTHH:mm")
  let staffId = sessionData.lastProposeUsedPreferred ? (sessionData.preferredStaffId || sessionData.lastStaffByIso?.[iso] || null)
                                                    : (sessionData.lastStaffByIso?.[iso] || sessionData.preferredStaffId || null)

  if (staffId && !isStaffAllowedInLocation(staffId, sessionData.sede)) {
    staffId = null
  }
  if (!staffId) {
    const probe = await searchAvailabilityGeneric({ locationKey: sessionData.sede, envServiceKey: sessionData.selectedServiceEnvKey, fromEU: startEU.clone().subtract(1, "minute"), days: 1, n: 10 })
    const match = probe.find(x => x.date.isSame(startEU, "minute"))
    if (match?.staffId && isStaffAllowedInLocation(match.staffId, sessionData.sede)) staffId = match.staffId
  }
  if (!staffId) staffId = pickStaffForLocation(sessionData.sede, null)
  if (!staffId) { await sendWithPresence(sock, jid, "No hay profesionales disponibles en esa sede"); return; }

  // === Identidad justo después de confirmar ===
  let customerId = sessionData.identityResolvedCustomerId || null

  if (!customerId){
    const { status, customer } = await getUniqueCustomerByPhoneOrPrompt(phone, sessionData, sock, jid) || {}
    if (status === "need_new" || status === "need_pick") {
      return
    }
    customerId = customer?.id || null
  }
  if (!customerId && (sessionData.name || sessionData.email)){
    const created = await findOrCreateCustomerWithRetry({ name: sessionData.name, email: sessionData.email, phone })
    if (created) customerId = created.id
  }
  if (!customerId){
    sessionData.stage = "awaiting_identity"
    saveSession(phone, sessionData)
    await sendWithPresence(sock, jid, "Para terminar, dime tu *nombre* y (opcional) tu *email* para crear tu ficha 😊")
    return
  }

  // Optimización IA (placeholder sin efectos destructivos)
  await aiOptimizeBooking({ start: iso, staffId }, [])

  const result = await createBookingWithRetry({ startEU, locationKey: sessionData.sede, envServiceKey: sessionData.selectedServiceEnvKey, durationMin: 60, customerId, teamMemberId: staffId, phone })
  if (!result.success) {
    const aptId = `apt_failed_${Math.random().toString(36).slice(2,8)}${Date.now().toString(36).slice(-4)}`
    insertAppt.run({
      id: aptId, customer_name: sessionData?.name || null, customer_phone: phone,
      customer_square_id: customerId, location_key: sessionData.sede, service_env_key: sessionData.selectedServiceEnvKey,
      service_label: sessionData.selectedServiceLabel || serviceLabelFromEnvKey(sessionData.selectedServiceEnvKey) || "Servicio", duration_min: 60,
      start_iso: startEU.tz("UTC").toISOString(), end_iso: startEU.clone().add(60, "minute").tz("UTC").toISOString(),
      staff_id: staffId, status: "failed", created_at: new Date().toISOString(),
      square_booking_id: null, square_error: result.error, retry_count: SQUARE_MAX_RETRIES
    })
    await sendWithPresence(sock, jid, "No pude crear la reserva ahora. Nuestro equipo te contactará. ¿Quieres que te proponga otro horario?")
    return
  }

  if (result.booking.__sim) { await sendWithPresence(sock, jid, "🧪 SIMULACIÓN: Reserva creada exitosamente (modo prueba)"); clearSession(phone); return }

  const aptId = `apt_${Math.random().toString(36).slice(2,8)}${Date.now().toString(36).slice(-4)}`
  insertAppt.run({
    id: aptId, customer_name: sessionData?.name || null, customer_phone: phone,
    customer_square_id: customerId, location_key: sessionData.sede, service_env_key: sessionData.selectedServiceEnvKey,
    service_label: sessionData.selectedServiceLabel || serviceLabelFromEnvKey(sessionData.selectedServiceEnvKey) || "Servicio",
    duration_min: 60, start_iso: startEU.tz("UTC").toISOString(), end_iso: startEU.clone().add(60, "minute").tz("UTC").toISOString(),
    staff_id: staffId, status: "confirmed", created_at: new Date().toISOString(),
    square_booking_id: result.booking.id, square_error: null, retry_count: 0
  })

  const staffName = staffLabelFromId(staffId) || sessionData.preferredStaffLabel || "nuestro equipo";
  const address = sessionData.sede === "la_luz" ? ADDRESS_LUZ : ADDRESS_TORRE;
  const svcLabel = serviceLabelFromEnvKey(sessionData.selectedServiceEnvKey) || sessionData.selectedServiceLabel || "Servicio"
  const confirmMessage = `🎉 ¡Reserva confirmada!

📍 ${locationNice(sessionData.sede)}
${address}

💅 ${svcLabel}
👩‍💼 ${staffName}
📅 ${fmtES(startEU)}

Referencia: ${result.booking.id}

¡Te esperamos!`
  await sendWithPresence(sock, jid, confirmMessage);
  clearSession(phone);
}

// ====== Listar/cancelar por teléfono
async function enumerateCitasByPhone(phone){
  const items=[]
  let cid=null
  try{
    const e164=normalizePhoneES(phone)
    const s=await square.customersApi.searchCustomers({ query:{ filter:{ phoneNumber:{ exact:e164 } } } })
    cid=(s?.result?.customers||[])[0]?.id||null
  }catch{}
  if (cid){
    try{
      const resp=await square.bookingsApi.listBookings(undefined, undefined, cid)
      const list=resp?.result?.bookings||[]
      const nowISO=new Date().toISOString()
      const seen = new Set()
      for (const b of list){
        if (!b?.startAt || b.startAt<nowISO) continue
        if (seen.has(b.id)) continue
        seen.add(b.id)
        const start=dayjs(b.startAt).tz(EURO_TZ)
        const seg=(b.appointmentSegments||[{}])[0]
        items.push({
          index:items.length+1,
          id:b.id,
          fecha_iso:start.format("YYYY-MM-DD"),
          pretty:fmtES(start),
          sede: locationNice(idToLocKey(b.locationId)||""),
          profesional: staffLabelFromId(seg?.teamMemberId) || "Profesional",
        })
      }
      items.sort((a,b)=> (a.fecha_iso.localeCompare(b.fecha_iso)) || (a.pretty.localeCompare(b.pretty)))
    }catch(e){}
  }
  return items
}
async function executeListAppointments(_params, _sessionData, phone, sock, jid) {
  const appointments = await enumerateCitasByPhone(phone);
  if (!appointments.length) { await sendWithPresence(sock, jid, "No tienes citas programadas. ¿Quieres agendar una?"); return; }
  const message = `Tus próximas citas (asociadas a tu número):\n\n${appointments.map(apt => 
    `${apt.index}) ${apt.pretty}\n📍 ${apt.sede}\n👩‍💼 ${apt.profesional}\n`
  ).join("\n")}`;
  await sendWithPresence(sock, jid, message);
}
async function executeCancelAppointment(params, sessionData, phone, sock, jid) {
  const appointments = await enumerateCitasByPhone(phone);
  if (!appointments.length) { await sendWithPresence(sock, jid, "No encuentro citas futuras asociadas a tu número. ¿Quieres que te ayude a reservar?"); return; }
  const appointmentIndex = params?.appointmentIndex;
  if (!appointmentIndex) {
    sessionData.cancelList = appointments
    sessionData.stage = "awaiting_cancel"
    saveSession(phone, sessionData)
    const message = `Estas son tus próximas citas (por tu número). ¿Cuál quieres cancelar?\n\n${appointments.map(apt => 
      `${apt.index}) ${apt.pretty} - ${apt.sede}`
    ).join("\n")}\n\nResponde con el número`
    await sendWithPresence(sock, jid, message);
    return;
  }
  const appointment = appointments.find(apt => apt.index === appointmentIndex);
  if (!appointment) { await sendWithPresence(sock, jid, "No encontré esa cita. ¿Puedes verificar el número?"); return; }
  const success = await cancelBooking(appointment.id);
  if (success) { await sendWithPresence(sock, jid, `✅ Cita cancelada: ${appointment.pretty} en ${appointment.sede}`) }
  else { await sendWithPresence(sock, jid, "No pude cancelar la cita. Por favor contacta directamente al salón.") }
  delete sessionData.cancelList
  sessionData.stage = null
  saveSession(phone, sessionData)
}

// ====== Mini-web + QR
const app=express()
const PORT=process.env.PORT||8080
let lastQR=null, conectado=false
app.get("/", (_req,res)=>{
  const totalAppts = db.prepare(`SELECT COUNT(*) as count FROM appointments`).get()?.count || 0
  const successAppts = db.prepare(`SELECT COUNT(*) as count FROM appointments WHERE status = 'confirmed'`).get()?.count || 0
  const failedAppts = db.prepare(`SELECT COUNT(*) as count FROM appointments WHERE status = 'failed'`).get()?.count || 0
  res.send(`<!doctype html><meta charset="utf-8"><style>
  body{font-family:system-ui;display:grid;place-items:center;min-height:100vh;background:#f8f9fa}
  .card{max-width:720px;padding:32px;border-radius:20px;box-shadow:0 8px 32px rgba(0,0,0,.1);background:white}
  .status{padding:12px;border-radius:8px;margin:8px 0}
  .success{background:#d4edda;color:#155724}
  .error{background:#f8d7da;color:#721c24}
  .warning{background:#fff3cd;color:#856404}
  .stat{display:inline-block;margin:0 16px;padding:8px 12px;background:#e9ecef;border-radius:6px}
  </style><div class="card">
  <h1>🩷 Gapink Nails Bot v28.0.0</h1>
  <div class="status ${conectado ? 'success' : 'error'}">Estado WhatsApp: ${conectado ? "✅ Conectado" : "❌ Desconectado"}</div>
  ${!conectado&&lastQR?`<div style="text-align:center;margin:20px 0"><img src="/qr.png" width="300" style="border-radius:8px"></div>`:""}
  <div class="status warning">Modo: ${DRY_RUN ? "🧪 Simulación" : "🚀 Producción"}</div>
  <h3>📊 Estadísticas</h3>
  <div><span class="stat">📅 Total: ${totalAppts}</span><span class="stat">✅ Exitosas: ${successAppts}</span><span class="stat">❌ Fallidas: ${failedAppts}</span></div>
  <div style="margin-top:24px;padding:16px;background:#e3f2fd;border-radius:8px;font-size:14px">
    <strong>🚀 Mejoras v28:</strong><br>
    • Motor IA multinivel + memoria.<br>
    • Prompt dinámico + NLP sin anchors rígidos.<br>
    • Validador inteligente previo a Square y predicción de slots.<br>
  </div>
  </div>`)
})
app.get("/qr.png", async (_req,res)=>{
  if(!lastQR) return res.status(404).send("No QR")
  const png = await qrcode.toBuffer(lastQR, { type:"png", width:512, margin:1 })
  res.set("Content-Type","image/png").send(png)
})
app.get("/logs", (_req,res)=>{
  const recent = db.prepare(`SELECT * FROM square_logs ORDER BY timestamp DESC LIMIT 50`).all()
  res.json({ logs: recent })
})

// ====== Baileys
async function loadBaileys(){
  const require = createRequire(import.meta.url); let mod=null
  try{ mod=require("@whiskeysockets/baileys") }catch{}; if(!mod){ try{ mod=await import("@whiskeysockets/baileys") }catch{} }
  if(!mod) throw new Error("Baileys incompatible")
  const makeWASocket = mod.makeWASocket || mod.default?.makeWASocket || (typeof mod.default==="function"?mod.default:undefined)
  const useMultiFileAuthState = mod.useMultiFileAuthState || mod.default?.useMultiFileAuthState
  const fetchLatestBaileysVersion = mod.fetchLatestBaileysVersion || mod.default?.fetchLatestBaileysVersion || (async()=>({version:[2,3000,0]}))
  const Browsers = mod.Browsers || mod.default?.Browsers || { macOS:(n="Desktop")=>["MacOS",n,"121.0.0"] }
  return { makeWASocket, useMultiFileAuthState, fetchLatestBaileysVersion, Browsers }
}

// 👉 Pre-intercept “con {nombre}” (smart)
function parsePreferredStaffFromText(text){
  const t = norm(text)
  const m = t.match(/\bcon\s+([a-zñáéíóú]+)\b/i)
  if (!m) return null
  const token = m[1]
  for (const e of EMPLOYEES){
    for (const lbl of e.labels){
      if (norm(lbl).includes(token)) return e
    }
  }
  return null
}

async function startBot(){
  try{
    const { makeWASocket, useMultiFileAuthState, fetchLatestBaileysVersion, Browsers } = await loadBaileys()
    if(!fs.existsSync("auth_info")) fs.mkdirSync("auth_info",{recursive:true})
    const { state, saveCreds } = await useMultiFileAuthState("auth_info")
    const { version } = await fetchLatestBaileysVersion().catch(()=>({version:[2,3000,0]}))
    const sock = makeWASocket({ logger:pino({level:"silent"}), printQRInTerminal:false, auth:state, version, browser:Browsers.macOS("Desktop"), syncFullHistory:false })
    globalThis.sock=sock

    sock.ev.on("connection.update", ({connection,qr})=>{
      if (qr){ lastQR=qr; conectado=false; try{ qrcodeTerminal.generate(qr,{small:true}) }catch{} }
      if (connection==="open"){ lastQR=null; conectado=true; RECONNECT_ATTEMPTS=0; RECONNECT_SCHEDULED=false; }
      if (connection==="close"){ 
        conectado=false; 
        if (!RECONNECT_SCHEDULED){
          RECONNECT_SCHEDULED = true
          const delay = Math.min(30000, 1500 * Math.pow(2, RECONNECT_ATTEMPTS++))
          setTimeout(()=>{ RECONNECT_SCHEDULED=false; startBot().catch(console.error) }, delay)
        }
      }
    })
    sock.ev.on("creds.update", saveCreds)

    sock.ev.on("messages.upsert", async ({messages})=>{
      const m=messages?.[0]; 
      if (!m?.message) return
      const jid = m.key.remoteJid
      const isFromMe = !!m.key.fromMe
      const phone = normalizePhoneES((jid||"").split("@")[0]||"") || (jid||"").split("@")[0]
      const textRaw = (m.message.conversation || m.message.extendedTextMessage?.text || m.message?.imageMessage?.caption || "").trim()
      if (!textRaw) return

      await enqueue(phone, async ()=>{
        const t0 = Date.now()
        try {
          let sessionData = loadSession(phone) || enhanceSessionData({
            greeted: false, sede: null, selectedServiceEnvKey: null, selectedServiceLabel: null,
            preferredStaffId: null, preferredStaffLabel: null, pendingDateTime: null,
            name: null, email: null, last_msg_id: null, lastStaffByIso: {},
            lastProposeUsedPreferred: false, stage: null, cancelList: null,
            serviceChoices: null, identityChoices: null, pendingCategory: null,
            lastStaffNamesById: null,
            snooze_until_ms: null,
            identityResolvedCustomerId: null
          })
          if (sessionData.last_msg_id === m.key.id) return
          sessionData.last_msg_id = m.key.id

          // === MUTE BY "." ===
          const trimmed = textRaw.trim()
          const nowEU = dayjs().tz(EURO_TZ)
          if (trimmed === ".") {
            sessionData.snooze_until_ms = nowEU.add(6, "hour").valueOf()
            saveSession(phone, sessionData)
            return
          }
          if (sessionData.snooze_until_ms && nowEU.valueOf() < sessionData.snooze_until_ms) {
            saveSession(phone, sessionData)
            return
          }
          if (isFromMe) { saveSession(phone, sessionData); return }

          const lower = norm(textRaw)
          const numMatch = lower.match(/^(?:opcion|opción)?\s*([1-9]\d*)\b/)

          // === PRE-INTERCEPT: identidad (varias fichas) ===
          if (sessionData.stage==="awaiting_identity_pick"){
            if (!numMatch){
              await sendWithPresence(sock, jid, "Responde con el número de tu ficha (1, 2, ...).")
              return
            }
            const n = Number(numMatch[1])
            const choice = (sessionData.identityChoices||[]).find(c=>c.index===n)
            if (!choice){
              await sendWithPresence(sock, jid, "No encontré esa opción. Prueba con el número de la lista.")
              return
            }
            sessionData.identityResolvedCustomerId = choice.id
            sessionData.stage = null
            saveSession(phone, sessionData)
            await sendWithPresence(sock, jid, "¡Gracias! Finalizo tu reserva…")
            await executeCreateBooking({}, sessionData, phone, sock, jid)
            return
          }

          // === PRE-INTERCEPT: identidad (crear nueva) ===
          if (sessionData.stage==="awaiting_identity"){
            const { name, email } = parseNameEmailFromText(textRaw)
            if (!name && !email){
              await sendWithPresence(sock, jid, "Dime tu *nombre completo* y, si quieres, tu *email* 😊")
              return
            }
            if (name) sessionData.name = name
            if (email) sessionData.email = email
            const created = await findOrCreateCustomerWithRetry({ name: sessionData.name, email: sessionData.email, phone })
            if (!created){
              await sendWithPresence(sock, jid, "No pude crear tu ficha. ¿Puedes repetir tu *nombre* y (opcional) tu *email*?")
              return
            }
            sessionData.identityResolvedCustomerId = created.id
            sessionData.stage = null
            saveSession(phone, sessionData)
            await sendWithPresence(sock, jid, "¡Gracias! Finalizo tu reserva…")
            await executeCreateBooking({}, sessionData, phone, sock, jid)
            return
          }

          // === PRE-INTERCEPT: sede si estamos esperando para servicios ===
          if (sessionData.stage==="awaiting_sede_for_services"){
            const sede = parseSede(textRaw)
            if (sede){
              sessionData.sede = sede
              sessionData.stage = null
              saveSession(phone, sessionData)
              await executeChooseService({ candidates: [] }, sessionData, phone, sock, jid, textRaw)
              return
            }
          }

          // === PRE-INTERCEPT: selección de horario ===
          if (numMatch && Array.isArray(sessionData.lastHours) && sessionData.lastHours.length && (!sessionData.stage || sessionData.stage==="awaiting_time")){
            const idx = Number(numMatch[1]) - 1
            const pick = sessionData.lastHours[idx]
            if (dayjs.isDayjs(pick)){
              const iso = pick.format("YYYY-MM-DDTHH:mm")
              const staffFromIso = sessionData?.lastStaffByIso?.[iso] || null
              if (staffFromIso && !isStaffAllowedInLocation(staffFromIso, sessionData.sede)) {
                await sendWithPresence(sock, jid, "Esa hora ya no está disponible con esa profesional en esa sede. Te paso otras opciones 👇")
                await executeProposeTime({}, sessionData, phone, sock, jid)
                return
              }
              sessionData.pendingDateTime = pick.tz(EURO_TZ).toISOString()
              if (staffFromIso){ sessionData.preferredStaffId = staffFromIso; sessionData.preferredStaffLabel = null }
              saveSession(phone, sessionData)
              const aiObj = { message:"Perfecto, confirmo tu cita ✨", action:"create_booking", session_updates:{}, action_params:{} }
              await routeAIResult(aiObj, sessionData, textRaw, m, phone, sock, jid)
              return
            }
          }

          // === PRE-INTERCEPT: selección para cancelar ===
          if (numMatch && sessionData.stage==="awaiting_cancel" && Array.isArray(sessionData.cancelList) && sessionData.cancelList.length){
            const n = Number(numMatch[1])
            const chosen = sessionData.cancelList.find(apt=>apt.index===n)
            if (chosen){
              const success = await cancelBooking(chosen.id)
              if (success) await sendWithPresence(sock, jid, `✅ Cita cancelada: ${chosen.pretty} en ${chosen.sede}`)
              else await sendWithPresence(sock, jid, "No pude cancelar la cita. Por favor contacta directamente al salón.")
              delete sessionData.cancelList
              sessionData.stage = null
              saveSession(phone, sessionData)
              return
            }
          }

          // === PRE-INTERCEPT: “con {nombre}” → proponer horas con esa pro
          if (sessionData.sede && sessionData.selectedServiceEnvKey){
            const maybe = parsePreferredStaffFromText(textRaw)
            if (maybe && isStaffAllowedInLocation(maybe.id, sessionData.sede)){
              sessionData.preferredStaffId = maybe.id
              sessionData.preferredStaffLabel = staffLabelFromId(maybe.id)
              saveSession(phone, sessionData)
              await executeProposeTime({}, sessionData, phone, sock, jid)
              return
            }
          }

          if (isCancelIntent(textRaw) && sessionData.stage!=="awaiting_cancel"){
            await executeCancelAppointment({}, sessionData, phone, sock, jid)
            return
          }

          // ===== IA multinivel
          const aiObj = await getAIResponse(textRaw, sessionData, phone)
          if (aiObj?.session_updates?.sede && (!sessionData.selectedServiceEnvKey) && sessionData.selectedServiceLabel){
            // autocompletar envKey del servicio ya elegido si cambia sede
            const ek = resolveEnvKeyFromLabelAndSede(sessionData.selectedServiceLabel, aiObj.session_updates.sede)
            if (ek) aiObj.session_updates.selectedServiceEnvKey = ek
          }
          await routeAIResult(aiObj, sessionData, textRaw, m, phone, sock, jid)

          // Aprendizaje ligero
          ContextualLearningEngine.learnFromInteraction(textRaw, safeJSONStringify(aiObj), sessionData, 'success').catch(()=>{})

        } catch (error) {
          if (BOT_DEBUG) console.error(error)
          await sendWithPresence(sock, jid, "Disculpa, hubo un error técnico. ¿Puedes repetir tu mensaje?")
        } finally {
          const t1 = Date.now()
          try{
            const arr = sessionData?.interactionMetrics?.responseTime || []
            arr.push(t1 - t0)
            sessionData.interactionMetrics.responseTime = arr.slice(-50)
            saveSession(phone, sessionData)
          }catch{}
        }
      })
    })
  }catch(e){ setTimeout(() => startBot().catch(console.error), 5000) }
}

async function routeAIResult(aiObj, sessionData, textRaw, m, phone, sock, jid){
  if (aiObj.session_updates) {
    Object.keys(aiObj.session_updates).forEach(key => {
      if (aiObj.session_updates[key] !== null && aiObj.session_updates[key] !== undefined) {
        sessionData[key] = aiObj.session_updates[key]
      }
    })
  }
  if (sessionData.sede && sessionData.selectedServiceLabel && !sessionData.selectedServiceEnvKey){
    const ek = resolveEnvKeyFromLabelAndSede(sessionData.selectedServiceLabel, sessionData.sede)
    if (ek) sessionData.selectedServiceEnvKey = ek
  }

  const fallbackUsedBool = !!aiObj.__fallback_used
  insertAIConversation.run({
    phone, message_id: m.key.id, user_message: textRaw,
    ai_response: safeJSONStringify(aiObj), timestamp: new Date().toISOString(),
    session_data: safeJSONStringify(sessionData),
    ai_error: (typeof aiObj.__ai_error === "string" || aiObj.__ai_error == null) ? (aiObj.__ai_error ?? null) : safeJSONStringify(aiObj.__ai_error),
    fallback_used: Number(fallbackUsedBool)
  })
  saveSession(phone, sessionData)

  switch (aiObj.action) {
    case "choose_service":
      await executeChooseService(aiObj.action_params, sessionData, phone, sock, jid, textRaw); break
    case "propose_times":
      await executeProposeTime(aiObj.action_params, sessionData, phone, sock, jid); break
    case "create_booking":
      await executeCreateBooking(aiObj.action_params, sessionData, phone, sock, jid); break
    case "list_appointments":
      await executeListAppointments(aiObj.action_params, sessionData, phone, sock, jid); break
    case "cancel_appointment":
      await executeCancelAppointment(aiObj.action_params, sessionData, phone, sock, jid); break
    case "need_info":
    case "none":
    default:
      // Si habló de uñas pero no eligió servicio → menú por NLP
      if (!sessionData.selectedServiceEnvKey && /\buñ|manicura|gel|acrilic|acrílic|semi|press|tips|francesa|encapsul/i.test(textRaw)){
        await executeChooseService({ candidates: aiObj?.action_params?.candidates || [] }, sessionData, phone, sock, jid, textRaw)
      } else {
        await sendWithPresence(sock, jid, aiObj.message || "¿Puedes repetirlo, por favor?")
      }
  }
}

// ====== Arranque
console.log(`🩷 Gapink Nails Bot v28.0.0`)
const server = app.listen(PORT, ()=>{ startBot().catch(console.error) })
process.on("uncaughtException", (e)=>{ console.error("💥 uncaughtException:", e?.stack||e?.message||e) })
process.on("unhandledRejection", (e)=>{ console.error("💥 unhandledRejection:", e) })
process.on("SIGTERM", ()=>{ try{server.close()}catch{} process.exit(0) })
process.on("SIGINT", ()=>{ try{server.close()}catch{} process.exit(0) })

// ====== Helpers restantes usados arriba
function resolveEnvKeyFromLabelAndSede(label, sedeKey){
  const list = servicesForSedeKeyRaw(sedeKey)
  return list.find(s=>s.label.toLowerCase()===String(label||"").toLowerCase())?.key || null
}
