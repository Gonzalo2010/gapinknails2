// index.js — Gapink Nails · v29.0.0
// Cambios clave v29:
// • IA gobierna todo: categoría → servicio → sede → proponer horas → crear reserva.
// • Comprensión natural de fechas: “viernes”, “mañana”, “por la tarde”, “próxima semana”.
// • Staff por centro: si “con {nombre}” pero no atiende en la sede, se avisa y se sugieren alternativas válidas.
// • Propuesta de horas filtrando por día/franja y respetando el teamMemberId de cada slot.
// • Elección de servicio por número si stage = awaiting_service_choice (sin reiniciar el flujo).
// • Fix SQL (16 columnas), fix paréntesis y no hay funciones duplicadas.
// • Silencio con "." por 6h y DRY_RUN opcional.

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

// Node 18+ trae fetch global
if (!globalThis.crypto) globalThis.crypto = webcrypto
dayjs.extend(utc); dayjs.extend(tz); dayjs.locale("es")
const EURO_TZ = "Europe/Madrid"

// ====== Config horario
const WORK_DAYS = [1,2,3,4,5] // L-V
const SLOT_MIN = 30
const OPEN = { start: 9, end: 20 } // 09:00–20:00
const NOW_MIN_OFFSET_MIN = Number(process.env.BOT_NOW_OFFSET_MIN || 30)
const HOLIDAYS_EXTRA = (process.env.HOLIDAYS_EXTRA || "06/01,28/02,15/08,12/10,01/11,06/12,08/12,25/12")
  .split(",").map(s=>s.trim()).filter(Boolean)

// ====== Flags
const BOT_DEBUG = /^true$/i.test(process.env.BOT_DEBUG || "")
const SQUARE_MAX_RETRIES = Number(process.env.SQUARE_MAX_RETRIES || 3)
const DRY_RUN = /^true$/i.test(process.env.DRY_RUN || "")

// ====== Square
const square = new Client({
  accessToken: process.env.SQUARE_ACCESS_TOKEN,
  environment: (process.env.SQUARE_ENV==="production") ? Environment.Production : Environment.Sandbox
})
const LOC_TORRE = (process.env.SQUARE_LOCATION_ID_TORREMOLINOS || "").trim()
const LOC_LUZ   = (process.env.SQUARE_LOCATION_ID_LA_LUZ || "").trim()
const ADDRESS_TORRE = process.env.ADDRESS_TORREMOLINOS || "Av. de Benyamina 18, Torremolinos"
const ADDRESS_LUZ   = process.env.ADDRESS_LA_LUZ || "Málaga – Barrio de La Luz"

// ====== IA
const AI_API_KEY = process.env.DEEPSEEK_API_KEY || process.env.OPENAI_API_KEY || ""
const AI_MODEL = process.env.AI_MODEL || process.env.DEEPSEEK_MODEL || "deepseek-chat"
const AI_MAX_RETRIES = Number(process.env.AI_MAX_RETRIES || 3)
const AI_TIMEOUT_MS = Number(process.env.AI_TIMEOUT_MS || 15000)
const sleep = ms => new Promise(r=>setTimeout(r, ms))

// ====== Utils
const onlyDigits = s => String(s||"").replace(/\D+/g,"")
const rm = s => String(s||"").normalize("NFD").replace(/\p{Diacritic}/gu,"")
const norm = s => rm(s).toLowerCase().replace(/[+.,;:()/_-]/g," ").replace(/[^\p{Letter}\p{Number}\s]/gu," ").replace(/\s+/g," ").trim()
function applySpanishDiacritics(label){
  let x = String(label||"")
  x = x.replace(/\bunas\b/gi, m => m[0] === 'U' ? 'Uñas' : 'uñas')
  x = x.replace(/\bpestan(as?|)\b/gi, (m, suf) => (m[0]==='P'?'Pestañ':'pestañ') + (suf||''))
  x = x.replace(/\bnivelacion\b/gi, m => m[0]==='N' ? 'Nivelación' : 'nivelación')
  x = x.replace(/\bfrances\b/gi, m => m[0]==='F' ? 'Francés' : 'francés')
  x = x.replace(/\bmas\b/gi, m => m[0]==='M' ? 'Más' : 'más')
  x = x.replace(/\bsemi ?permanente\b/gi, m => /[A-Z]/.test(m[0]) ? 'Semipermanente' : 'semipermanente')
  x = x.replace(/\bninas\b/gi, 'niñas')
  x = x.replace(/\bdiseno\b/gi, m => m[0]==='D' ? 'Diseño' : 'diseño')
  x = x.replace(/\bfotodepilacion\b/gi, m => m[0]==='F' ? 'Fotodepilación' : 'fotodepilación')
  x = x.replace(/\bdepilacion\b/gi, m => m[0]==='D' ? 'Depilación' : 'depilación')
  x = x.replace(/\blimpieza\b/gi, m => m)
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

function fmtES(d){
  const dias=["domingo","lunes","martes","miércoles","jueves","viernes","sábado"]
  const t=(dayjs.isDayjs(d)?d:dayjs(d)).tz(EURO_TZ)
  return `${dias[t.day()]} ${String(t.date()).padStart(2,"0")}/${String(t.month()+1).padStart(2,"0")} ${String(t.hour()).padStart(2,"0")}:${String(t.minute()).padStart(2,"0")}`
}
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
function parseToEU(input){
  if (dayjs.isDayjs(input)) return input.clone().tz(EURO_TZ)
  const s = String(input||"")
  if (/[Zz]|[+\-]\d{2}:?\d{2}$/.test(s)) return dayjs(s).tz(EURO_TZ)
  return dayjs.tz(s, EURO_TZ)
}
function stableKey(parts){ const raw=Object.values(parts).join("|"); return createHash("sha256").update(raw).digest("hex").slice(0,48) }

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
function enumerateHours(list){ return list.map((d,i)=>({ index:i+1, iso:d.format("YYYY-MM-DDTHH:mm"), pretty:fmtES(d) })) }

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
`)

// Statements
const insertAppt = db.prepare(`INSERT INTO appointments
(id,customer_name,customer_phone,customer_square_id,location_key,service_env_key,service_label,duration_min,start_iso,end_iso,staff_id,status,created_at,square_booking_id,square_error,retry_count)
VALUES (@id,@customer_name,@customer_phone,@customer_square_id,@location_key,@service_env_key,@service_label,@duration_min,@start_iso,@end_iso,@staff_id,@status,@created_at,@square_booking_id,@square_error,@retry_count)`)

const insertAIConversation = db.prepare(`INSERT OR REPLACE INTO ai_conversations
(phone, message_id, user_message, ai_response, timestamp, session_data, ai_error, fallback_used)
VALUES (@phone, @message_id, @user_message, @ai_response, @timestamp, @session_data, @ai_error, @fallback_used)`)

const insertSquareLog = db.prepare(`INSERT INTO square_logs
(phone, action, request_data, response_data, error_data, timestamp, success)
VALUES (@phone, @action, @request_data, @response_data, @error_data, @timestamp, @success)`)

// ====== Sesión
function loadSession(phone){
  const row = db.prepare(`SELECT data_json FROM sessions WHERE phone=@phone`).get({phone})
  if (!row?.data_json) return null
  const s = JSON.parse(row.data_json)
  if (Array.isArray(s.lastHours_ms)) s.lastHours = s.lastHours_ms.map(ms=>dayjs.tz(ms,EURO_TZ))
  if (s.pendingDateTime_ms) s.pendingDateTime = dayjs.tz(s.pendingDateTime_ms,EURO_TZ)
  return s
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

// ====== Empleadas (desde env + alias)
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
    const [id, book, _locs] = String(v||"").split("|")
    if (!id) continue
    const bookable = (book||"").toUpperCase()!=="NO_BOOKABLE"
    let allow = []
    // Prefer EMP_CENTER_* si existe
    const centerKey = "EMP_CENTER_" + k.replace(/^SQ_EMP_/, "")
    const empVal = process.env[centerKey]
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
        allow = ids.length ? ids : allow
      }
    } else {
      // fallback al tercer campo |locs| si viene con ids
      const locs = (_locs||"").split(",").map(s=>s.trim()).filter(Boolean)
      allow = locs.length ? locs : allow
    }
    const labels = deriveLabelsFromEnvKey(k)
    // Aliases comunes
    const friendly = labels.map(l=>applySpanishDiacritics(l))
    const base = friendly.join(" ")
    const low = norm(base)
    const aliases = new Set(friendly)
    if (/patric/i.test(base)) { aliases.add("patri"); aliases.add("patricia") }
    if (/crist/i.test(base)) { aliases.add("cristi"); aliases.add("cristina"); aliases.add("cristy") }
    if (/rocio.*chica|chica.*rocio/i.test(base)) { aliases.add("rocio chica"); aliases.add("rocío chica") }
    if (/carmen.*belen|bel[eé]n.*carmen/i.test(base)) { aliases.add("carmen"); aliases.add("carmen belén"); }
    out.push({ envKey:k, id, bookable, allow, labels: friendly, aliases:[...aliases].map(a=>norm(a)) })
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
function allAllowedStaffForLocation(locKey){
  const locId = locationToId(locKey)
  return EMPLOYEES.filter(e => e.bookable && (e.allow.includes("ALL") || e.allow.includes(locId)))
}
function findEmployeeByNameLike(name, locKey=null){
  const token = norm(name||"")
  const list = EMPLOYEES.filter(e => e.bookable)
  const filtered = list.filter(e=>{
    const okLoc = locKey ? (e.allow.includes("ALL") || e.allow.includes(locationToId(locKey))) : true
    const hay = e.aliases?.some(a=>a.includes(token)) || e.labels?.some(l=>norm(l).includes(token))
    return okLoc && hay
  })
  return filtered[0] || null
}
function parsePreferredStaffFromText(text){
  const t = norm(text)
  // Busca "con X"
  const m = t.match(/\bcon\s+([a-zñáéíóú\s]{2,})$/i)
  const pickToken = m ? m[1] : t // también permitimos "quiero con cristina" o "con carmen por la tarde"
  const e = findEmployeeByNameLike(pickToken)
  return e
}

// ====== Servicios
function titleCase(str){ return String(str||"").toLowerCase().replace(/\b([a-záéíóúñ])/g, (m)=>m.toUpperCase()) }
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

// ====== Categorías
const CATS = ["uñas","depilación","micropigmentación","faciales","pestañas"]
function detectCategoryFromMessage(msg){
  const u = norm(msg)
  if (/\buñ|manicura|acrilic|gel|semipermanente|pedicur/.test(u)) return "uñas"
  if (/\bdepil|fotodepil|hilo|axil|pierna|brazo|labio|pubis|ingles|fosas|cejas\b/.test(u)) return "depilación"
  if (/\bmicroblading|microshading|polvo|aquarela|eyeliner|hairstroke\b/.test(u)) return "micropigmentación"
  if (/\bfacial|limpieza|dermapen|vitamina|manchas|hidra|hydra|oro|diamante|tratamiento\b/.test(u)) return "faciales"
  if (/\bpestañ|lifting|tinte|extensi/.test(u)) return "pestañas"
  return null
}
function isServiceInCategory(labelNorm, category){
  const l = labelNorm
  switch(category){
    case "uñas":
      if (/\bpestañ|ceja/.test(l)) return false
      return /\buñ|manicura|gel|acril|semi|press|tips|frances|nivelaci|pedicur/.test(l)
    case "depilación":
      return /\bdepilaci|fotodepil|hilo|axil|pierna|brazo|labio|pubis|ingl[eé]s|fosas|ceja|l[aá]ser/.test(l)
    case "micropigmentación":
      return /\bmicroblading|microshading|polvo|hairstroke|aquarela|eyeliner/.test(l)
    case "faciales":
      return /\b facial|limpieza|dermapen|hydra|vitamina|manchas|jade|colageno|oro|acn[eé]/.test(l)
    case "pestañas":
      return /\bpestañ|lifting|tinte|extensi/.test(l)
    default: return false
  }
}
function servicesByCategoryForSede(sedeKey, category){
  const list = servicesForSedeKeyRaw(sedeKey)
  return list.filter(s=>isServiceInCategory(s.norm, category)).sort((a,b)=> a.label.localeCompare(b.label))
}
function resolveEnvKeyFromLabelAndSede(label, sedeKey){
  const list = servicesForSedeKeyRaw(sedeKey)
  return list.find(s=>s.label.toLowerCase()===String(label||"").toLowerCase())?.key || null
}

// ====== Relevancia
function scoreServiceRelevance(userMsg, label){
  const u = norm(userMsg), l = norm(label); let score = 0
  const toks = ["natural","francesa","frances","decoracion","diseño","extra","exprés","express","completa","nivelacion","nivelación","axilas","piernas","brazos","pubis","labio","cejas","ingles"]
  for (const t of toks){ if (u.includes(norm(t)) && l.includes(norm(t))) score += 0.4 }
  if (/\bdepil/.test(u) && /\bdepil/.test(l)) score += 3
  if (/\buñ|manicura|pedicur/.test(u) && /\buñ|manicura|pedicur/.test(l)) score += 3
  if (/\b(pestañ|extension)/.test(u) && /\bpestañ|extensi/.test(l)) score += 2.5
  let overlap=0; const utoks=new Set(u.split(" ").filter(Boolean)); const ltoks=new Set(l.split(" ").filter(Boolean))
  for (const t of utoks){ if (ltoks.has(t)) overlap++ }
  score += Math.min(overlap,3)*0.25
  return score
}

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
async function searchAvailabilityForStaff({ locationKey, envServiceKey, staffId, fromEU, days=14, n=20, dayOfWeek=null, partOfDay=null }){
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
    const slots=[]
    for (const a of avail){
      if (!a?.startAt) continue
      const d = dayjs(a.startAt).tz(EURO_TZ)
      if (!insideBusinessHours(d,60)) continue
      if (dayOfWeek!=null && d.day()!==dayOfWeek) continue
      if (partOfDay==="am" && d.hour()>=14) continue
      if (partOfDay==="pm" && d.hour()<14) continue
      const segs = Array.isArray(a.appointmentSegments) ? a.appointmentSegments
                   : Array.isArray(a.segments) ? a.segments : []
      let tm = segs[0]?.teamMemberId || staffId
      if (!isStaffAllowedInLocation(tm, locationKey)) continue
      slots.push({ date:d, staffId: tm })
      if (slots.length>=n) break
    }
    return slots
  }catch{ return [] }
}
async function searchAvailabilityGeneric({ locationKey, envServiceKey, fromEU, days=14, n=20, dayOfWeek=null, partOfDay=null }){
  try{
    const sv = await getServiceIdAndVersion(envServiceKey)
    if (!sv?.id) return []
    const startAt = fromEU.tz("UTC").toISOString()
    const endAt = fromEU.clone().add(days,"day").tz("UTC").toISOString()
    const locationId = locationToId(locationKey)
    const body = { query:{ filter:{ startAtRange:{ startAt, endAt }, locationId, segmentFilters:[{ serviceVariationId: sv.id }] } } }
    const resp = await square.bookingsApi.searchAvailability(body)
    const avail = resp?.result?.availabilities || []
    const slots=[]
    for (const a of avail){
      if (!a?.startAt) continue
      const d = dayjs(a.startAt).tz(EURO_TZ)
      if (!insideBusinessHours(d,60)) continue
      if (dayOfWeek!=null && d.day()!==dayOfWeek) continue
      if (partOfDay==="am" && d.hour()>=14) continue
      if (partOfDay==="pm" && d.hour()<14) continue
      const segs = Array.isArray(a.appointmentSegments) ? a.appointmentSegments
                   : Array.isArray(a.segments) ? a.segments : []
      let tm = segs[0]?.teamMemberId || null
      if (tm && !isStaffAllowedInLocation(tm, locationKey)) continue
      slots.push({ date:d, staffId: tm })
      if (slots.length>=n) break
    }
    return slots
  }catch{ return [] }
}

// ====== IA (prompts con reglas)
async function callAIOnce(messages, systemPrompt = "") {
  const controller = new AbortController()
  const timeoutId = setTimeout(() => controller.abort(), AI_TIMEOUT_MS)
  try {
    const allMessages = systemPrompt ? [{ role: "system", content: systemPrompt }, ...messages] : messages
    const apiURL = process.env.DEEPSEEK_API_URL || process.env.OPENAI_API_URL || "https://api.deepseek.com/v1/chat/completions"
    const response = await fetch(apiURL, {
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

function staffRosterForPrompt(){
  return EMPLOYEES.map(e=>{
    const locs = e.allow.map(id=> id===LOC_TORRE?"torremolinos" : id===LOC_LUZ?"la_luz" : id).join(",")
    return `• ID:${e.id} | Nombres:[${(e.labels||[]).join(", ")}] | Aliases:[${(e.aliases||[]).join(", ")}] | Sedes:[${locs||"ALL"}] | Reservable:${e.bookable}`
  }).join("\n")
}

function buildSystemPrompt() {
  const nowEU = dayjs().tz(EURO_TZ);
  const staffLines = staffRosterForPrompt()

  const sT = servicesForSedeKeyRaw("torremolinos")
  const sL = servicesForSedeKeyRaw("la_luz")

  return `Eres el asistente de WhatsApp para Gapink Nails. Devuelves SOLO JSON válido.

AHORA: ${nowEU.format("dddd DD/MM/YYYY HH:mm")} (Madrid)
SEDES:
- Torremolinos: ${ADDRESS_TORRE}
- Málaga – La Luz: ${ADDRESS_LUZ}

HORARIO: L-V 09:00-20:00; S/D cerrado; Festivos: ${HOLIDAYS_EXTRA.join(", ")}

PROFESIONALES:
${staffLines}

SERVICIOS TORREMOLINOS:
${sT.map(s => `- ${s.label} (Clave: ${s.key})`).join("\n")}

SERVICIOS LA LUZ:
${sL.map(s => `- ${s.label} (Clave: ${s.key})`).join("\n")}

REGLAS:
1) Nunca propongas ni reserves con una profesional no permitida en esa sede.
2) Si el cliente dice “con {nombre}”, intenta mapear a staffId y fija preferredStaffId si es válido. Si no lo es en esa sede, dilo y sugiere alternativas válidas en la sede.
3) Antes de listar servicios, asegúrate de tener la CATEGORÍA (Uñas, Depilación, Micropigmentación, Faciales o Pestañas) y la SEDE.
4) Al elegir una hora por número (1/2/3), conserva el teamMemberId del slot.
5) Identidad: usa el teléfono. Si 0 o 2+, pide y, tras responder, FINALIZA la reserva.
6) Responde con: {"message":"...","action":"propose_times|create_booking|list_appointments|cancel_appointment|choose_service|need_info|none","session_updates":{...},"action_params":{...}}`
}

async function getAIResponse(userMessage, sessionData, phone) {
  const systemPrompt = buildSystemPrompt();

  const recent = db.prepare(`SELECT user_message, ai_response FROM ai_conversations WHERE phone = ? ORDER BY timestamp DESC LIMIT 6`).all(phone);
  const conversationHistory = recent.reverse().map(msg => [
    { role: "user", content: msg.user_message },
    { role: "assistant", content: msg.ai_response }
  ]).flat();

  const sessionContext = `
ESTADO:
- Sede: ${sessionData?.sede || 'no'}
- Categoría: ${sessionData?.category || 'no'}
- Servicio: ${sessionData?.selectedServiceLabel || 'no'} (${sessionData?.selectedServiceEnvKey || 'no_key'})
- Profesional pref.: ${sessionData?.preferredStaffLabel || 'ninguna'} (${sessionData?.preferredStaffId||'—'})
- Fecha/hora pendiente: ${sessionData?.pendingDateTime ? fmtES(parseToEU(sessionData.pendingDateTime)) : 'no'}
- Etapa: ${sessionData?.stage || 'inicial'}
- Últimas horas: ${Array.isArray(sessionData?.lastHours) ? sessionData.lastHours.length + ' opciones' : 'ninguna'}
- Filtros tiempo: ${sessionData?.timeFilterDay ?? '—'}, ${sessionData?.timeFilterPart ?? '—'}
`;

  const messages = [
    ...conversationHistory,
    { role: "user", content: `MENSAJE DEL CLIENTE: "${userMessage}"\n\n${sessionContext}\n\nINSTRUCCIÓN: Devuelve SOLO JSON siguiendo las reglas.` }
  ];

  const aiText = await callAIWithRetries(messages, systemPrompt)
  if (!aiText || /^error de conexión/i.test(aiText.trim())) return buildLocalFallback(userMessage, sessionData)

  const cleaned = aiText.replace(/```json\s*/gi, "").replace(/```\s*/g, "").replace(/^[^{]*/, "").replace(/[^}]*$/, "").trim()
  try { return JSON.parse(cleaned) } catch { return buildLocalFallback(userMessage, sessionData) }
}

// ====== Fallback local mínimo
function buildLocalFallback(userMessage, sessionData){
  const msg = String(userMessage||"").trim()
  const lower = norm(msg)
  const numMatch = lower.match(/^(?:opcion|opción)?\s*([1-9]\d*)\b/)
  const yesMatch = /\b(si|sí|ok|vale|confirmo|de\ acuerdo)\b/i.test(msg)
  const cancelMatch = /\b(cancelar|anular|borra|elimina)\b/i.test(lower)
  const listMatch = /\b(mis citas|lista|ver citas)\b/i.test(lower)

  const hasCore = (s)=> s?.sede && s?.selectedServiceEnvKey && s?.pendingDateTime

  if (numMatch && Array.isArray(sessionData?.lastHours) && sessionData.lastHours.length){
    const idx = Number(numMatch[1]) - 1
    const pick = sessionData.lastHours[idx]
    if (dayjs.isDayjs(pick)){
      const iso = pick.format("YYYY-MM-DDTHH:mm")
      const staffFromIso = sessionData?.lastStaffByIso?.[iso] || null
      const updates = { pendingDateTime: pick.tz(EURO_TZ).toISOString() }
      if (staffFromIso) { updates.preferredStaffId = staffFromIso; updates.preferredStaffLabel = null }
      const okToCreate = hasCore({...sessionData, ...updates})
      return { message: okToCreate ? "Perfecto, voy a confirmar esa hora 👍" : "Genial. Me falta algún dato.", action: okToCreate ? "create_booking" : "need_info", session_updates: updates, action_params: {} }
    }
  }
  if (yesMatch){
    if (hasCore(sessionData)){
      return { message:"¡Voy a crear la reserva! ✨", action:"create_booking", session_updates:{}, action_params:{} }
    } else {
      const faltan=[]
      if (!sessionData?.sede) faltan.push("sede (Torremolinos o La Luz)")
      if (!sessionData?.selectedServiceEnvKey) faltan.push("servicio")
      if (!sessionData?.pendingDateTime) faltan.push("fecha y hora")
      return { message:`Para terminar necesito: ${faltan.join(", ")}.`, action:"need_info", session_updates:{}, action_params:{} }
    }
  }
  if (cancelMatch && !/^awaiting_/.test(sessionData?.stage||"")) return { message:"Vale, te enseño tus citas para cancelar:", action:"cancel_appointment", session_updates:{}, action_params:{} }
  if (listMatch) return { message:"Estas son tus próximas citas:", action:"list_appointments", session_updates:{}, action_params:{} }

  const cat = detectCategoryFromMessage(msg)
  if (!sessionData?.category && cat){
    return { message:`Categoría detectada: ${cat}.`, action:"choose_service", session_updates:{ category:cat, stage:"awaiting_service_choice" }, action_params:{ candidates:[] } }
  }
  return { message:"¿Quieres reservar, cancelar o ver tus citas? Dime *categoría* (Uñas/Depilación/Micropigmentación/Faciales/Pestañas) y *sede*.", action:"none", session_updates:{}, action_params:{} }
}

// ====== Helpers UI y cola
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

// ====== Texto → sede
function parseSede(text){
  const t=norm(text)
  if (/\b(luz|la luz)\b/.test(t)) return "la_luz"
  if (/\b(torre|torremolinos)\b/.test(t)) return "torremolinos"
  return null
}

// ====== Texto → filtros de tiempo
function parseTemporalPreference(txt){
  const u = norm(txt)
  const daysMap = { "lunes":1,"martes":2,"miercoles":3,"miércoles":3,"jueves":4,"viernes":5,"sabado":6,"sábado":6,"domingo":0,"hoy":"hoy","manana":"mañana","mañana":"mañana" }
  let dayOfWeek=null, partOfDay=null, fromEU = dayjs().tz(EURO_TZ)
  if (/\b(tarde)\b/.test(u)) partOfDay="pm"
  if (/\b(ma[nñ]ana)\b/.test(u) && !/\bsemana\b/.test(u)) partOfDay=partOfDay||"am"
  if (/\bmediod/i.test(u)) { partOfDay=null }
  if (/\bproxima semana|semana que viene\b/.test(u)) { fromEU = fromEU.add(7,"day").startOf("week").add(1,"day").hour(OPEN.start) }
  for (const k of Object.keys(daysMap)){
    if (u.includes(norm(k))){
      if (k==="hoy"){ /* same */ }
      else if (k==="mañana" || k==="manana"){ fromEU = fromEU.add(1,"day").hour(OPEN.start) }
      else { dayOfWeek = daysMap[k]; }
      break
    }
  }
  return { dayOfWeek, partOfDay, fromEU: ceilToSlotEU(nextOpeningFrom(fromEU)) }
}

// ====== Construir listas de servicios
function buildServiceChoiceListBySedeAndCategory(sedeKey, category, userMsg, aiCandidates){
  const all = servicesByCategoryForSede(sedeKey, category)
  const localScores = new Map()
  for (const s of all){ localScores.set(s.label, scoreServiceRelevance(userMsg||"", s.label)) }
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
  const inAI = all.filter(s=>aiMap.has(s.label)).sort((a,b)=> (aiMap.get(b.label)-aiMap.get(a.label)) || ((localScores.get(b.label)||0)-(localScores.get(a.label)||0)))
  const rest = all.filter(s=>!aiMap.has(s.label)).sort((a,b)=> (localScores.get(b.label)||0)-(localScores.get(a.label)||0))
  const final = [...inAI, ...rest]
  return final.map((s,i)=>({ index:i+1, label:s.label }))
}

// ====== Acciones
async function executeChooseService(params, sessionData, phone, sock, jid, userMsg){
  if (!sessionData.category){
    sessionData.stage = "awaiting_category"
    saveSession(phone, sessionData)
    await sendWithPresence(sock, jid, "¿Qué categoría necesitas? *Uñas*, *Depilación*, *Micropigmentación*, *Faciales* o *Pestañas*.")
    return
  }
  if (!sessionData.sede){
    sessionData.stage = "awaiting_sede_for_services"
    saveSession(phone, sessionData)
    await sendWithPresence(sock, jid, "¿En qué sede? Torremolinos o La Luz.")
    return
  }
  const aiCands = Array.isArray(params?.candidates) ? params.candidates : []
  const items = buildServiceChoiceListBySedeAndCategory(sessionData.sede, sessionData.category, userMsg||"", aiCands)
  if (!items.length){
    await sendWithPresence(sock, jid, `Ahora mismo no tengo servicios de *${sessionData.category}* configurados para ${locationNice(sessionData.sede)}.`)
    return
  }
  sessionData.serviceChoices = items
  sessionData.stage = "awaiting_service_choice"
  saveSession(phone, sessionData)
  const lines = items.map(it=> {
    const star = aiCands.find(c=>cleanDisplayLabel(String(c.label||"")).toLowerCase()===it.label.toLowerCase()) ? " ⭐" : ""
    return `${it.index}) ${applySpanishDiacritics(it.label)}${star}`
  }).join("\n")
  await sendWithPresence(sock, jid, `Opciones de *${sessionData.category}* en ${locationNice(sessionData.sede)}:\n\n${lines}\n\nResponde con el número.`)
}

async function executeProposeTime(params, sessionData, phone, sock, jid) {
  const nowEU = dayjs().tz(EURO_TZ);
  let baseFrom = nextOpeningFrom(nowEU.add(NOW_MIN_OFFSET_MIN, "minute"));
  if (sessionData.timeFilterFrom) baseFrom = parseToEU(sessionData.timeFilterFrom)

  const dayOfWeek = (params?.dayOfWeek ?? sessionData.timeFilterDay ?? null)
  const partOfDay = (params?.partOfDay ?? sessionData.timeFilterPart ?? null)

  if (!sessionData.sede || !sessionData.selectedServiceEnvKey) { await sendWithPresence(sock, jid, "Necesito la sede y el servicio primero."); return; }

  let slots = []
  let usedPreferred = false

  if (sessionData.preferredStaffId && isStaffAllowedInLocation(sessionData.preferredStaffId, sessionData.sede)) {
    const staffSlots = await searchAvailabilityForStaff({ locationKey: sessionData.sede, envServiceKey: sessionData.selectedServiceEnvKey, staffId: sessionData.preferredStaffId, fromEU: baseFrom, n: 6, dayOfWeek, partOfDay })
    if (staffSlots.length){ slots = staffSlots; usedPreferred = true }
  }
  if (!slots.length) {
    const generic = await searchAvailabilityGeneric({ locationKey: sessionData.sede, envServiceKey: sessionData.selectedServiceEnvKey, fromEU: baseFrom, n: 6, dayOfWeek, partOfDay })
    slots = generic
  }
  if (!slots.length) {
    // deterministic fallback
    const gen=[]
    let t = ceilToSlotEU(baseFrom.clone())
    while (gen.length<6){
      if (insideBusinessHours(t,60)){
        if ((dayOfWeek==null || t.day()===dayOfWeek) && (!partOfDay || (partOfDay==="am"?t.hour()<14:t.hour()>=14))){
          gen.push({date:t.clone(), staffId:null})
        }
      }
      t = t.add(SLOT_MIN, "minute")
      if (t.hour()>=OPEN.end) { t = nextOpeningFrom(t) }
      if (gen.length>=6) break
    }
    slots = gen
  }
  if (!slots.length) { await sendWithPresence(sock, jid, "No encuentro horarios disponibles en los próximos días. ¿Otra fecha o franja (mañana/tarde)?"); return; }

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
    const tag = sid ? ` — ${applySpanishDiacritics(staffLabelFromId(sid))}` : ""
    return `${h.index}) ${h.pretty}${tag}`
  }).join("\n")
  const header = usedPreferred
    ? `Horarios disponibles con ${applySpanishDiacritics(sessionData.preferredStaffLabel || "tu profesional")}:`
    : `Horarios disponibles (nuestro equipo):${sessionData.preferredStaffLabel ? `\nNota: no veo huecos con ${applySpanishDiacritics(sessionData.preferredStaffLabel)} en los próximos días; te muestro alternativas.`:""}`
  await sendWithPresence(sock, jid, `${header}\n${lines}\n\nResponde con el número (1-${hoursEnum.length})`)
}

async function executeCreateBooking(_params, sessionData, phone, sock, jid) {
  if (!sessionData.sede) { await sendWithPresence(sock, jid, "Falta seleccionar la sede (Torremolinos o La Luz)"); return; }
  if (!sessionData.selectedServiceEnvKey) { await sendWithPresence(sock, jid, "Falta seleccionar el servicio"); return; }
  if (!sessionData.pendingDateTime) { await sendWithPresence(sock, jid, "Falta seleccionar la fecha y hora"); return; }

  const startEU = parseToEU(sessionData.pendingDateTime)
  if (!insideBusinessHours(startEU, 60)) { await sendWithPresence(sock, jid, "Esa hora está fuera del horario (L-V 09:00–20:00)"); return; }

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
  if (!staffId) staffId = (allAllowedStaffForLocation(sessionData.sede)[0]?.id || null)
  if (!staffId) { await sendWithPresence(sock, jid, "No hay profesionales disponibles en esa sede"); return; }

  // Identidad
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
    await sendWithPresence(sock, jid, "No pude crear la reserva ahora. ¿Quieres que te proponga otro horario?")
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

🧾 ${applySpanishDiacritics(svcLabel)}
👩‍💼 ${applySpanishDiacritics(staffName)}
📅 ${fmtES(startEU)}

Ref: ${result.booking.id}

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
    `${apt.index}) ${apt.pretty}\n📍 ${apt.sede}\n👩‍💼 ${applySpanishDiacritics(apt.profesional)}\n`
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
  .card{max-width:640px;padding:32px;border-radius:20px;box-shadow:0 8px 32px rgba(0,0,0,.1);background:white}
  .status{padding:12px;border-radius:8px;margin:8px 0}
  .success{background:#d4edda;color:#155724}
  .error{background:#f8d7da;color:#721c24}
  .warning{background:#fff3cd;color:#856404}
  .stat{display:inline-block;margin:0 16px;padding:8px 12px;background:#e9ecef;border-radius:6px}
  </style><div class="card">
  <h1>🩷 Gapink Nails Bot v29.0.0</h1>
  <div class="status ${conectado ? 'success' : 'error'}">Estado WhatsApp: ${conectado ? "✅ Conectado" : "❌ Desconectado"}</div>
  ${!conectado&&lastQR?`<div style="text-align:center;margin:20px 0"><img src="/qr.png" width="300" style="border-radius:8px"></div>`:""}
  <div class="status warning">Modo: ${DRY_RUN ? "🧪 Simulación" : "🚀 Producción"}</div>
  <h3>📊 Estadísticas</h3>
  <div><span class="stat">📅 Total: ${totalAppts}</span><span class="stat">✅ Exitosas: ${successAppts}</span><span class="stat">❌ Fallidas: ${failedAppts}</span></div>
  <div style="margin-top:24px;padding:16px;background:#e3f2fd;border-radius:8px;font-size:14px">
    <strong>🚀 Mejoras:</strong><br>
    • Categorías + IA en todo el flujo<br>
    • Huecos por día/franja y profesional deseada<br>
    • Staff por centro validado<br>
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

// ====== Bot principal
let RECONNECT_SCHEDULED = false
let RECONNECT_ATTEMPTS = 0

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
        try {
          let sessionData = loadSession(phone) || {
            greeted: false,
            category: null,
            sede: null,
            selectedServiceEnvKey: null,
            selectedServiceLabel: null,
            preferredStaffId: null,
            preferredStaffLabel: null,
            pendingDateTime: null,
            name: null,
            email: null,
            last_msg_id: null,
            lastStaffByIso: {},
            lastProposeUsedPreferred: false,
            stage: null,
            cancelList: null,
            serviceChoices: null,
            identityChoices: null,
            lastStaffNamesById: null,
            snooze_until_ms: null,
            identityResolvedCustomerId: null,
            timeFilterDay: null,
            timeFilterPart: null,
            timeFilterFrom: null
          }
          if (sessionData.last_msg_id === m.key.id) return
          sessionData.last_msg_id = m.key.id

          // Silencio por "."
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
          const temporal = parseTemporalPreference(textRaw)

          // === PRE: identidad (varias fichas)
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

          // === PRE: identidad (crear nueva)
          if (sessionData.stage==="awaiting_identity"){
            const emailMatch = String(textRaw||"").match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i)
            const email = emailMatch ? emailMatch[0] : null
            const name = String(textRaw||"").replace(email||"", "").replace(/(email|correo)[:\s]*/ig,"").trim() || null
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

          // === PRE: elegir categoría si se estaba esperando
          if (sessionData.stage === "awaiting_category") {
            const cat = detectCategoryFromMessage(textRaw);
            if (cat) {
              sessionData.category = cat;
              sessionData.stage = null;
              saveSession(phone, sessionData);
              if (!sessionData.sede) {
                sessionData.stage = "awaiting_sede_for_services";
                saveSession(phone, sessionData);
                await sock.sendMessage(jid, { text: "¿En qué sede? Torremolinos o La Luz." });
              } else {
                await executeChooseService({}, sessionData, phone, sock, jid, textRaw);
              }
              return;
            } else {
              await sock.sendMessage(jid, { text: "Dime una categoría válida: *Uñas*, *Depilación*, *Micropigmentación*, *Faciales* o *Pestañas*." });
              return;
            }
          }

          // === PRE: elegir sede si estamos esperando para servicios
          if (sessionData.stage==="awaiting_sede_for_services"){
            const sede = parseSede(textRaw)
            if (sede){
              sessionData.sede = sede
              sessionData.stage = null

              // si había preferencia de staff, validarla contra la sede
              if (sessionData.preferredStaffId && !isStaffAllowedInLocation(sessionData.preferredStaffId, sessionData.sede)) {
                const badName = sessionData.preferredStaffLabel || staffLabelFromId(sessionData.preferredStaffId) || "esa profesional";
                const alts = allAllowedStaffForLocation(sessionData.sede).slice(0, 6).map(e => applySpanishDiacritics(e.labels[0])).join(", ");
                await sock.sendMessage(jid, { text: `${applySpanishDiacritics(badName)} no atiende en ${locationNice(sessionData.sede)}. Disponibles: ${alts}.` });
              }

              saveSession(phone, sessionData)
              await executeChooseService({ candidates: [] }, sessionData, phone, sock, jid, textRaw)
              return
            }
          }

          // === PRE: selección de servicio por número
          if (numMatch && sessionData.stage==="awaiting_service_choice" && Array.isArray(sessionData.serviceChoices) && sessionData.serviceChoices.length){
            const idx = Number(numMatch[1]) - 1
            const choice = sessionData.serviceChoices[idx]
            if (!choice){ await sendWithPresence(sock, jid, "No encontré esa opción. Responde con el número que aparece en la lista."); return }
            const envKey = resolveEnvKeyFromLabelAndSede(choice.label, sessionData.sede)
            if (!envKey){ await sendWithPresence(sock, jid, "Hubo un problema con el servicio. Prueba a elegir de nuevo."); return }
            sessionData.selectedServiceEnvKey = envKey
            sessionData.selectedServiceLabel = choice.label
            sessionData.stage = null
            saveSession(phone, sessionData)
            // Si el usuario mencionó “con {nombre}”
            const maybe = parsePreferredStaffFromText(textRaw)
            if (maybe){
              sessionData.preferredStaffId = maybe.id
              sessionData.preferredStaffLabel = staffLabelFromId(maybe.id)
              // Validar sede
              if (!isStaffAllowedInLocation(maybe.id, sessionData.sede)){
                const alts = allAllowedStaffForLocation(sessionData.sede).slice(0, 6).map(e => applySpanishDiacritics(e.labels[0])).join(", ")
                await sendWithPresence(sock, jid, `${applySpanishDiacritics(sessionData.preferredStaffLabel)} no atiende en ${locationNice(sessionData.sede)}. Disponibles: ${alts}.`)
                sessionData.preferredStaffId = null
                sessionData.preferredStaffLabel = null
                saveSession(phone, sessionData)
              }
            }
            // Filtros temporales si el mensaje los trae
            if (temporal){
              sessionData.timeFilterDay = temporal.dayOfWeek
              sessionData.timeFilterPart = temporal.partOfDay
              sessionData.timeFilterFrom = temporal.fromEU.toISOString()
              saveSession(phone, sessionData)
            }
            await executeProposeTime({}, sessionData, phone, sock, jid)
            return
          }

          // === PRE: si el usuario pide “viernes”, “mañana”, “tarde” y estamos esperando hora
          if (sessionData.stage==="awaiting_time"){
            if (/\b(hoy|mañana|manana|viernes|jueves|miercoles|miércoles|martes|lunes|tarde|mañana|manana)\b/i.test(textRaw)){
              if (temporal){
                sessionData.timeFilterDay = temporal.dayOfWeek
                sessionData.timeFilterPart = temporal.partOfDay
                sessionData.timeFilterFrom = temporal.fromEU.toISOString()
                saveSession(phone, sessionData)
                await executeProposeTime({}, sessionData, phone, sock, jid)
                return
              }
            }
          }

          // === PRE: selección de horario (1/2/3…)
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
              if (staffFromIso){ sessionData.preferredStaffId = staffFromIso; sessionData.preferredStaffLabel = staffLabelFromId(staffFromIso) }
              saveSession(phone, sessionData)
              const aiObj = { message:"Perfecto, confirmo tu cita ✨", action:"create_booking", session_updates:{}, action_params:{} }
              await routeAIResult(aiObj, sessionData, textRaw, m, phone, sock, jid)
              return
            }
          }

          // === PRE: pedir staff “con {nombre}” tras haber elegido servicio
          if (sessionData.sede && sessionData.selectedServiceEnvKey){
            const maybe = parsePreferredStaffFromText(textRaw)
            if (maybe){
              if (isStaffAllowedInLocation(maybe.id, sessionData.sede)){
                sessionData.preferredStaffId = maybe.id
                sessionData.preferredStaffLabel = staffLabelFromId(maybe.id)
                // si además pide una fecha/franja en el mismo mensaje
                if (temporal){
                  sessionData.timeFilterDay = temporal.dayOfWeek
                  sessionData.timeFilterPart = temporal.partOfDay
                  sessionData.timeFilterFrom = temporal.fromEU.toISOString()
                }
                saveSession(phone, sessionData)
                await executeProposeTime({}, sessionData, phone, sock, jid)
                return
              } else {
                const alts = allAllowedStaffForLocation(sessionData.sede).slice(0, 6).map(e => applySpanishDiacritics(e.labels[0])).join(", ")
                await sendWithPresence(sock, jid, `${applySpanishDiacritics(staffLabelFromId(maybe.id) || "Esa profesional")} no atiende en ${locationNice(sessionData.sede)}. Disponibles: ${alts}.`)
                return
              }
            }
          }

          // === IA normal
          const aiObj = await getAIResponse(textRaw, sessionData, phone)

          // Si IA fijó sede pero aún no hay envKey y ya tenemos etiqueta previa, resolver
          if (aiObj?.session_updates?.sede && (!sessionData.selectedServiceEnvKey) && sessionData.selectedServiceLabel){
            const ek = resolveEnvKeyFromLabelAndSede(sessionData.selectedServiceLabel, aiObj.session_updates.sede)
            if (ek) aiObj.session_updates.selectedServiceEnvKey = ek
          }

          // Si IA detecta categoría desde texto suelto
          if (!sessionData.category){
            const cat = detectCategoryFromMessage(textRaw)
            if (cat) {
              aiObj.session_updates = { ...(aiObj.session_updates||{}), category: cat }
            }
          }

          // Aplicar filtros de tiempo si el mensaje trae “viernes/tarde…”
          if (temporal){
            aiObj.session_updates = { ...(aiObj.session_updates||{}),
              timeFilterDay: temporal.dayOfWeek, timeFilterPart: temporal.partOfDay, timeFilterFrom: temporal.fromEU.toISOString()
            }
          }

          await routeAIResult(aiObj, sessionData, textRaw, m, phone, sock, jid)

        } catch (error) {
          if (BOT_DEBUG) console.error(error)
          await sendWithPresence(sock, jid, "Disculpa, hubo un error técnico. ¿Puedes repetir tu mensaje?")
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
      // Si aún no hay servicio pero ya hay categoría → mostrar servicios de esa categoría
      if (!sessionData?.selectedServiceEnvKey && sessionData?.category){
        await executeChooseService({ candidates: aiObj?.action_params?.candidates || [] }, sessionData, phone, sock, jid, textRaw)
      } else {
        await sendWithPresence(sock, jid, aiObj.message || "¿Puedes repetirlo, por favor?")
      }
  }
}

// ====== Arranque
console.log(`🩷 Gapink Nails Bot v29.0.0`)
app.listen(PORT, ()=>{ startBot().catch(console.error) })
process.on("uncaughtException", (e)=>{ console.error("💥 uncaughtException:", e?.stack||e?.message||e) })
process.on("unhandledRejection", (e)=>{ console.error("💥 unhandledRejection:", e) })
process.on("SIGTERM", ()=>{ process.exit(0) })
process.on("SIGINT", ()=>{ process.exit(0) })
