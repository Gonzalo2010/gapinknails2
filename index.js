// index.js — Gapink Nails · v29.2.1 — Full
// IA end-to-end: categoría → sede → servicio → profesional → día/parte del día → hora → Square.
// Fixes clave v29.2.x:
// - "con {profesional}" ahora *también* entiende día/franja del mismo mensaje (p.ej. "viernes por la tarde").
// - Si la profesional no atiende en la sede elegida, no mostramos "no hay huecos"; avisamos sede incorrecta y proponemos alternativas.
// - La nota "no veo huecos con {X}" solo se muestra si realmente se buscó con {X} en esa sede y *no hubo* horarios.
// - Persistencia de día/franja en contexto para preguntas subsiguientes ("¿y por la tarde?").
// - Dedupe de servicios, SQL OK, manejo de errores y logging.

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
const DRY_RUN = /^true$/i.test(process.env.DRY_RUN || "")

// ====== Square
const square = new Client({
  accessToken: process.env.SQUARE_ACCESS_TOKEN,
  environment: (process.env.SQUARE_ENV==="production") ? Environment.Production : Environment.Sandbox
})
const LOC_TORRE = (process.env.SQUARE_LOCATION_ID_TORREMOLINOS || process.env.SQUARE_LOCATION_ID_PLAYAMAR || "").trim()
const LOC_LUZ   = (process.env.SQUARE_LOCATION_ID_LA_LUZ || "").trim()
const ADDRESS_TORRE = process.env.ADDRESS_TORREMOLINOS || "Av. de Benyamina 18, Torremolinos"
const ADDRESS_LUZ   = process.env.ADDRESS_LA_LUZ || "Málaga – Barrio de La Luz"

// ====== IA
const AI_API_KEY = process.env.DEEPSEEK_API_KEY || process.env.OPENAI_API_KEY || ""
const AI_MODEL = process.env.DEEPSEEK_MODEL || process.env.OPENAI_MODEL || "deepseek-chat"
const AI_MAX_RETRIES = Number(process.env.AI_MAX_RETRIES || 3)
const AI_TIMEOUT_MS = Number(process.env.AI_TIMEOUT_MS || 15000)
const sleep = ms => new Promise(r=>setTimeout(r, ms))

// ====== Utils
const onlyDigits = s => String(s||"").replace(/\D+/g,"")
const rm = s => String(s||"").normalize("NFD").replace(/\p{Diacritic}/gu,"")
const norm = s => rm(s).toLowerCase().replace(/[+.,;:()/_-]/g," ").replace(/[^\p{Letter}\p{Number}\s]/gu," ").replace(/\s+/g," ").trim()
function stableKey(parts){ const raw=Object.values(parts).join("|"); return createHash("sha256").update(raw).digest("hex").slice(0,48) }
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
function parseToEU(input){
  if (dayjs.isDayjs(input)) return input.clone().tz(EURO_TZ)
  const s = String(input||"")
  if (/[Zz]|[+\-]\d{2}:?\d{2}$/.test(s)) return dayjs(s).tz(EURO_TZ)
  return dayjs.tz(s, EURO_TZ)
}
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

// ====== Day-part helpers
const DAYPARTS = { morning:{start:9,end:13}, afternoon:{start:15,end:19}, evening:{start:18,end:20} }
function parseDaypart(text){
  const t = norm(text)
  if (/\b(ma[nñ]ana)\b/.test(t)) return "morning"
  if (/\b(tarde)\b/.test(t)) return "afternoon"
  if (/\b(noche|tardi[ta])\b/.test(t)) return "evening"
  return null
}
function filterByDaypart(d, daypart){
  if (!daypart) return true
  const { start, end } = DAYPARTS[daypart] || DAYPARTS.afternoon
  const h = d.hour() + d.minute()/60
  return h >= start && h < end
}
function parseRequestedDayFromText(text, baseDay){
  const t = norm(text)
  const base = (dayjs.isDayjs(baseDay)?baseDay:dayjs().tz(EURO_TZ)).clone().startOf("day")
  if (/\bhoy\b/.test(t)) return base
  if (/\bma[nñ]ana\b/.test(t)) return base.add(1,"day")
  if (/\bpasado\s+ma[nñ]ana\b/.test(t)) return base.add(2,"day")
  const days = ["domingo","lunes","martes","miercoles","miércoles","jueves","viernes","sabado","sábado"]
  for (let i=0;i<days.length;i++){
    if (new RegExp(`\\b${days[i]}\\b`, "i").test(t)){
      let target = i%7
      const now = base.day()
      let delta = (target - now + 7) % 7
      if (delta===0 && /proximo|siguiente/i.test(t)) delta = 7
      return base.add(delta, "day")
    }
  }
  const m = t.match(/\b(\d{1,2})(?:[\/\-\.](\d{1,2}))?\b/)
  if (m){
    const dd = Number(m[1]); const mm = m[2]?Number(m[2]):(base.month()+1)
    const y = base.year()
    const cand = dayjs.tz(`${y}-${String(mm).padStart(2,"0")}-${String(dd).padStart(2,"0")}T09:00`, EURO_TZ)
    if (cand.isValid()) return cand.startOf("day")
  }
  return null
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

// ====== Sesión
function loadSession(phone){
  const row = db.prepare(`SELECT data_json FROM sessions WHERE phone=@phone`).get({phone})
  if (!row?.data_json) return null
  const s = JSON.parse(row.data_json)
  if (Array.isArray(s.lastHours_ms)) s.lastHours = s.lastHours_ms.map(ms=>dayjs.tz(ms,EURO_TZ))
  if (s.pendingDateTime_ms) s.pendingDateTime = dayjs.tz(s.pendingDateTime_ms,EURO_TZ)
  if (s.dateContextISO_ms) s.dateContextISO = dayjs.tz(s.dateContextISO_ms, EURO_TZ).toISOString()
  return s
}
function saveSession(phone,s){
  const c={...s}
  c.lastHours_ms = Array.isArray(s.lastHours)? s.lastHours.map(d=>dayjs.isDayjs(d)?d.valueOf():null).filter(Boolean):[]
  c.pendingDateTime_ms = s.pendingDateTime? (dayjs.isDayjs(s.pendingDateTime)? s.pendingDateTime.valueOf() : dayjs(s.pendingDateTime).valueOf()) : null
  c.dateContextISO_ms = s.dateContextISO ? dayjs(s.dateContextISO).valueOf() : null
  delete c.lastHours; delete c.pendingDateTime
  const j=JSON.stringify(c)
  const up=db.prepare(`UPDATE sessions SET data_json=@j, updated_at=@u WHERE phone=@p`).run({j,u:new Date().toISOString(),p:phone})
  if (up.changes===0) db.prepare(`INSERT INTO sessions (phone,data_json,updated_at) VALUES (@p,@j,@u)`).run({p:phone,j,u:new Date().toISOString()})
}
function clearSession(phone){ db.prepare(`DELETE FROM sessions WHERE phone=@phone`).run({phone}) }

// ====== Empleadas
function deriveLabelsFromEnvKey(envKey){
  const raw = envKey.replace(/^SQ_EMP_/, "")
  const toks = raw.split("_").map(t=>norm(t)).filter(Boolean)
  const uniq = Array.from(new Set(toks))
  const labels = [...uniq]
  if (uniq.length>1) labels.push(uniq.join(" "))
  return labels
}
const FALLBACK_STAFF_CENTERS = {
  "rocio":"la_luz",
  "rocio chica":"both",
  "carmen belen":"both",      // <- permitir ambos si el env no trae locs
  "patri":"la_luz",
  "ganna":"la_luz",
  "maria":"la_luz",
  "anaira":"la_luz",
  "cristi":"both",
  "ginna":"torremolinos",
  "daniela":"torremolinos",
  "desi":"torremolinos",
  "jamaica":"torremolinos",
  "johana":"torremolinos",
  "edurne":"torremolinos",
  "sudemis":"torremolinos",
  "tania":"torremolinos",
  "chabely":"torremolinos",
  "elisabeth":"torremolinos"
}
function centerNameToId(center){
  if (!center) return []
  const c = center.toLowerCase()
  if (c==="both" || c==="all") return [LOC_TORRE, LOC_LUZ].filter(Boolean)
  if (c==="la_luz" || c.includes("luz")) return [LOC_LUZ].filter(Boolean)
  if (c==="torremolinos" || c.includes("torre")) return [LOC_TORRE].filter(Boolean)
  return []
}
function parseEmployees(){
  const out=[]
  const envKeys = Object.keys(process.env).filter(k=>k.startsWith("SQ_EMP_"))
  for (const k of envKeys){
    const v = String(process.env[k]||"")
    const [id, book, locs] = v.split("|")
    if (!id) continue
    const bookable = (book||"").toUpperCase()==="BOOKABLE"
    let allow = []
    if (locs && locs !== "NO_LOCS"){
      const tokens = locs.split(",").map(s=>s.trim()).filter(Boolean)
      for (const t of tokens) allow.push(t)
    }
    const labels = deriveLabelsFromEnvKey(k)
    out.push({ envKey:k, id, bookable, allow, labels })
  }
  for (const e of out){
    const bestLabel = (e.labels?.[0] || "").toLowerCase()
    if (!e.allow?.length){
      const fbCenter = FALLBACK_STAFF_CENTERS[bestLabel] || null
      if (fbCenter){
        e.allow = centerNameToId(fbCenter)
      } else {
        e.allow = ["ALL"]
      }
    } else {
      if (e.allow.includes("ALL")) e.allow = ["ALL"]
    }
  }
  return out
}
const EMPLOYEES = parseEmployees()
function staffLabelFromId(id){
  const e = EMPLOYEES.find(x=>x.id===id)
  return e?.labels?.[0] || (id ? `Profesional ${String(id).slice(-4)}` : null)
}
function isStaffAllowedInLocation(staffId, locKey){
  const e = EMPLOYEES.find(x=>x.id===staffId)
  if (!e || !e.bookable) return false
  const locId = locationToId(locKey)
  return e.allow.includes("ALL") || e.allow.includes(locId)
}
function allowedLocKeysForStaff(staffId){
  const e = EMPLOYEES.find(x=>x.id===staffId)
  if (!e || !e.bookable) return []
  const out=[]
  for (const id of e.allow){
    if (id===LOC_TORRE) out.push("torremolinos")
    if (id===LOC_LUZ) out.push("la_luz")
  }
  return out
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
function parsePreferredStaffFromText(text){
  const t = norm(text)
  const m = t.match(/\bcon\s+([a-zñáéíóú]+(?:\s+[a-zñáéíóú]+)?)\b/i)
  if (!m) return null
  const token = norm(m[1])
  let best = null
  for (const e of EMPLOYEES){
    for (const lbl of e.labels){
      if (norm(lbl).includes(token)) { best = e; break }
    }
    if (best) break
  }
  return best
}

// ====== Servicios y categorías
function titleCase(str){ return String(str||"").toLowerCase().replace(/\b([a-z])/g, (m)=>m.toUpperCase()) }
function applySpanishDiacritics(label){
  let x = String(label||"")
  x = x.replace(/\bunas\b/gi, m => m[0] === 'U' ? 'Uñas' : 'uñas')
  x = x.replace(/\bpestan(as?|)\b/gi, (m, suf) => (m[0]==='P'?'Pestañ':'pestañ') + (suf||''))
  x = x.replace(/\bnivelacion\b/gi, m => m[0]==='N' ? 'Nivelación' : 'nivelación')
  x = x.replace(/\bfrances\b/gi, m => m[0]==='F' ? 'Francés' : 'francés')
  x = x.replace(/\bsemi ?permanente\b/gi, m => /[A-Z]/.test(m[0]) ? 'Semipermanente' : 'semipermanente')
  x = x.replace(/\bninas\b/gi, 'niñas')
  return x
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
function categorizeServiceLabel(labelNorm){
  const s = " " + labelNorm + " "
  if (/\b(u[nñ]a|uñas|manicura|pedicura|acril|gel|semiperman|press|tips|nivelacion|nivelación)\b/.test(s)) return "unas"
  if (/\b(depila|fotodepila|axilas|ingles|labio|ceja|fosas|piernas|pubis)\b/.test(s)) return "depilacion"
  if (/\b(microblading|microshading|labios|eyeliner|aquarela|cejas)\b/.test(s)) return "micropigmentacion"
  if (/\b(facial|hidra|limpieza|dermapen|vitamina|diamante|jade|endosphere|masaje)\b/.test(s)) return "facial_corporal"
  return "otros"
}
function allServices(){ return [...servicesForSedeKeyRaw("torremolinos"), ...servicesForSedeKeyRaw("la_luz")] }
function servicesByCategory(sedeKey, category){
  const list = servicesForSedeKeyRaw(sedeKey)
  return list.filter(s=>categorizeServiceLabel(s.norm)===category)
}
function resolveEnvKeyFromLabelAndSede(label, sedeKey){
  const list = servicesForSedeKeyRaw(sedeKey)
  return list.find(s=>s.label.toLowerCase()===String(label||"").toLowerCase())?.key || null
}

// ====== Square helpers (clientes)
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

// ====== Square service id+version
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

// ====== DISPONIBILIDAD
async function searchAvailabilityForStaff({ locationKey, envServiceKey, staffId, fromEU, days=14, n=3, exactDayOnly=false, daypart=null }){
  try{
    const sv = await getServiceIdAndVersion(envServiceKey)
    if (!sv?.id || !staffId) return []
    let startAt = fromEU.tz("UTC").toISOString()
    let endAt = fromEU.clone().add(days,"day").tz("UTC").toISOString()
    if (exactDayOnly){
      startAt = fromEU.clone().startOf("day").tz("UTC").toISOString()
      endAt   = fromEU.clone().endOf("day").tz("UTC").toISOString()
    }
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
    const seen = new Set()
    for (const a of avail){
      const start = a?.startAt; if (!start) continue
      const d = dayjs(start).tz(EURO_TZ)
      if (!insideBusinessHours(d,60)) continue
      if (!filterByDaypart(d, daypart)) continue
      if (!isStaffAllowedInLocation(staffId, locationKey)) continue
      const key=d.format("YYYY-MM-DDTHH:mm")
      if (seen.has(key)) continue
      seen.add(key)
      slots.push({ date:d, staffId })
      if (slots.length>=n) break
    }
    return slots
  }catch{ return [] }
}
async function searchAvailabilityGeneric({ locationKey, envServiceKey, fromEU, days=14, n=3, exactDayOnly=false, daypart=null }){
  try{
    const sv = await getServiceIdAndVersion(envServiceKey)
    if (!sv?.id) return []
    let startAt = fromEU.tz("UTC").toISOString()
    let endAt = fromEU.clone().add(days,"day").tz("UTC").toISOString()
    if (exactDayOnly){
      startAt = fromEU.clone().startOf("day").tz("UTC").toISOString()
      endAt   = fromEU.clone().endOf("day").tz("UTC").toISOString()
    }
    const locationId = locationToId(locationKey)
    const body = { query:{ filter:{ startAtRange:{ startAt, endAt }, locationId, segmentFilters:[{ serviceVariationId: sv.id }] } } }
    const resp = await square.bookingsApi.searchAvailability(body)
    const avail = resp?.result?.availabilities || []
    const slots=[]
    const seen=new Set()
    for (const a of avail){
      const start = a?.startAt; if (!start) continue
      const d = dayjs(start).tz(EURO_TZ)
      if (!insideBusinessHours(d,60)) continue
      if (!filterByDaypart(d, daypart)) continue
      let tm = null
      const segs = Array.isArray(a.appointmentSegments) ? a.appointmentSegments
                 : Array.isArray(a.segments) ? a.segments : []
      if (segs[0]?.teamMemberId) tm = segs[0].teamMemberId
      if (tm && !isStaffAllowedInLocation(tm, locationKey)) continue
      const key=d.format("YYYY-MM-DDTHH:mm")
      if (seen.has(key)) continue
      seen.add(key)
      slots.push({ date:d, staffId: tm || null })
      if (slots.length>=n) break
    }
    return slots
  }catch{ return [] }
}

// ====== IA (DeepSeek/OpenAI)
async function callAIOnce(messages, systemPrompt = "") {
  const controller = new AbortController()
  const timeoutId = setTimeout(() => controller.abort(), AI_TIMEOUT_MS)
  try {
    const allMessages = systemPrompt ? [{ role: "system", content: systemPrompt }, ...messages] : messages
    const url = process.env.DEEPSEEK_API_URL || process.env.OPENAI_API_URL || "https://api.deepseek.com/v1/chat/completions"
    const headers = { "Content-Type": "application/json", "Authorization": `Bearer ${AI_API_KEY}` }
    const body = JSON.stringify({ model: AI_MODEL, messages: allMessages, max_tokens: 1500, temperature: 0.6, stream: false })
    const response = await fetch(url, { method:"POST", headers, body, signal: controller.signal })
    clearTimeout(timeoutId)
    if (!response.ok) return null
    const data = await response.json()
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
    return `• ID:${e.id} | Nombres:[${e.labels.join(", ")}] | Sedes:[${locs||"ALL"}] | Reservable:${e.bookable}`
  }).join("\n")
}
function buildSystemPrompt(sessionData){
  const nowEU = dayjs().tz(EURO_TZ);
  const staffLines = staffRosterForPrompt()
  const cat = sessionData?.category || "desconocida"
  return `Eres el asistente de WhatsApp de Gapink Nails. Devuelves SOLO JSON válido.

Fecha/hora: ${nowEU.format("dddd DD/MM/YYYY HH:mm")} Europe/Madrid

SEDES:
- Torremolinos: ${ADDRESS_TORRE}
- Málaga – La Luz: ${ADDRESS_LUZ}

HORARIO:
- L-V 09:00-20:00; Festivos: ${HOLIDAYS_EXTRA.join(", ")}

PROFESIONALES:
${staffLines}

CATEGORÍAS: "unas", "depilacion", "micropigmentacion", "facial_corporal".
REGLAS:
- Si el usuario dice “con {nombre}”, intenta mapear a una profesional y valida sede.
- No listar servicios sin tener categoría y sede.
- Si pide “otro día/por la tarde/mañana”, usa el día en contexto (sessionData.dateContextISO si existe).
- Para reservar: sede + servicio + fecha/hora. Identidad por teléfono.

FORMATO:
{"message":"...","action":"propose_times|create_booking|list_appointments|cancel_appointment|choose_category|choose_service|set_sede|need_info|none","session_updates":{...},"action_params":{...}}

Contexto actual: ${safeJSONStringify({
  sede: sessionData?.sede || null,
  category: cat,
  service: sessionData?.selectedServiceLabel || null,
  preferredStaff: sessionData?.preferredStaffLabel || null
})}`
}
async function getAIResponse(userMessage, sessionData, phone) {
  const systemPrompt = buildSystemPrompt(sessionData);

  const recent = db.prepare(`SELECT user_message, ai_response FROM ai_conversations WHERE phone = ? ORDER BY timestamp DESC LIMIT 6`).all(phone);
  const conversationHistory = recent.reverse().map(msg => [
    { role: "user", content: msg.user_message },
    { role: "assistant", content: msg.ai_response }
  ]).flat();

  const sessionContext = `
ESTADO:
- Sede: ${sessionData?.sede || 'no seleccionada'}
- Categoría: ${sessionData?.category || 'no seleccionada'}
- Servicio: ${sessionData?.selectedServiceLabel || 'no seleccionado'} (${sessionData?.selectedServiceEnvKey || 'no_key'})
- Profesional preferida: ${sessionData?.preferredStaffLabel || 'ninguna'} (explícita: ${sessionData?.preferredExplicit ? "sí":"no"})
- Fecha/hora pendiente: ${sessionData?.pendingDateTime ? fmtES(parseToEU(sessionData.pendingDateTime)) : 'no seleccionada'}
- Últimas horas propuestas: ${Array.isArray(sessionData?.lastHours) ? sessionData.lastHours.length + ' opciones' : 'ninguna'}
- Día de contexto: ${sessionData?.dateContextISO ? fmtES(dayjs(sessionData.dateContextISO)) : '—'}
`;

  const messages = [
    ...conversationHistory,
    { role: "user", content: `MENSAJE: "${userMessage}"\n\n${sessionContext}\n\nINSTRUCCIÓN: Devuelve SOLO JSON siguiendo reglas.` }
  ];

  const aiText = await callAIWithRetries(messages, systemPrompt)
  if (!aiText) return { message:"¿Quieres reservar, cancelar o ver tus citas? Dime la *categoría* (uñas, depilación, micropigmentación, facial) y la *sede*.", action:"need_info", session_updates:{}, action_params:{} }

  const cleaned = aiText.replace(/```json\s*/gi, "").replace(/```\s*/g, "").replace(/^[^{]*/, "").replace(/[^}]*$/, "").trim()
  try { return JSON.parse(cleaned) } catch { 
    return { message:"¿Quieres reservar, cancelar o ver tus citas? Dime la *categoría* (uñas, depilación, micropigmentación, facial) y la *sede*.", action:"need_info", session_updates:{}, action_params:{} }
  }
}

// ====== Chat helpers
async function sendWithPresence(sock, jid, text){
  try{ await sock.sendPresenceUpdate("composing", jid) }catch{}
  await new Promise(r=>setTimeout(r, 500+Math.random()*700))
  return sock.sendMessage(jid, { text })
}
function parseSede(text){
  const t=norm(text)
  if (/\b(luz|la luz)\b/.test(t)) return "la_luz"
  if (/\b(torre|torremolinos)\b/.test(t)) return "torremolinos"
  return null
}
function parseCategory(text){
  const t = norm(text)
  if (/\b(u[nñ]as|manicura|pedicura)\b/.test(t)) return "unas"
  if (/\b(depila|fotodepila|axilas|ingles|labio|ceja|piernas|pubis|fosas)\b/.test(t)) return "depilacion"
  if (/\b(microblading|microshading|labios|aquarela|eyeliner|cejas)\b/.test(t)) return "micropigmentacion"
  if (/\b(facial|hidra|limpieza|dermapen|vitamina|diamante|jade|masaje|endosphere)\b/.test(t)) return "facial_corporal"
  return null
}
function parseNameEmailFromText(txt){
  const emailMatch = String(txt||"").match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i)
  const email = emailMatch ? emailMatch[0] : null
  const name = String(txt||"").replace(email||"", "").replace(/(email|correo)[:\s]*/ig,"").trim()
  return { name: name || null, email }
}

// ====== Construir lista de servicios por categoría (sin duplicados)
function buildServiceChoiceListByCategory(sedeKey, category){
  const list = servicesByCategory(sedeKey, category)
  const seen = new Set()
  const out=[]
  for (const s of list){
    const key = s.label.toLowerCase()
    if (seen.has(key)) continue
    seen.add(key)
    out.push({ index: out.length+1, label: s.label })
  }
  return out
}

// ====== Proponer horas
function proposeSlots({ fromEU, durationMin=60, n=3, daypart=null }){
  const out=[]
  let t = ceilToSlotEU(fromEU.clone())
  const endOfSearch = fromEU.clone().endOf("day")
  while (out.length<n && t.isBefore(endOfSearch.add(1,"minute"))){
    if (insideBusinessHours(t, durationMin) && filterByDaypart(t, daypart)) out.push(t.clone())
    t = t.add(SLOT_MIN, "minute")
    if (t.hour()>=OPEN.end) break
  }
  while (out.length<n){
    const nx = nextOpeningFrom(endOfSearch.add(1,"minute"))
    if (!nx) break
    const more = ceilToSlotEU(nx.clone())
    if (insideBusinessHours(more, durationMin) && filterByDaypart(more, daypart)) out.push(more.clone())
    if (out.length>=n) break
    endOfSearch.add(1,"day")
  }
  return out.slice(0,n)
}

async function executeChooseService(params, sessionData, phone, sock, jid, userMsg){
  if (!sessionData.category){
    const inferred = parseCategory(userMsg||"")
    if (inferred){ sessionData.category = inferred; saveSession(phone, sessionData) }
  }
  if (!sessionData.category){
    await sendWithPresence(sock, jid, "¿Qué categoría necesitas? *uñas*, *depilación*, *micropigmentación* o *facial*.")
    return
  }
  if (!sessionData.sede){
    await sendWithPresence(sock, jid, "¿En qué sede te viene mejor? *Torremolinos* o *La Luz*.")
    return
  }
  const items = buildServiceChoiceListByCategory(sessionData.sede, sessionData.category)
  if (!items.length){
    await sendWithPresence(sock, jid, `Ahora mismo no tengo servicios de ${sessionData.category} configurados en ${locationNice(sessionData.sede)}.`)
    return
  }
  sessionData.serviceChoices = items
  sessionData.stage = "awaiting_service_choice"
  saveSession(phone, sessionData)
  const lines = items.map(it=> `${it.index}) ${applySpanishDiacritics(it.label)}`).join("\n")
  await sendWithPresence(sock, jid, `Opciones de *${sessionData.category.replace("_"," ")}* en ${locationNice(sessionData.sede)}:\n\n${lines}\n\nResponde con el número.`)
}

async function executeProposeTime(params, sessionData, phone, sock, jid) {
  const nowEU = dayjs().tz(EURO_TZ);
  const baseTextDay = params?.fromISO ? parseToEU(params.fromISO) : null;
  const baseFrom = baseTextDay ? baseTextDay : nextOpeningFrom(nowEU.add(NOW_MIN_OFFSET_MIN, "minute"));
  const exactDayOnly = !!params?.exactDayOnly;
  const daypart = params?.daypart || null

  if (!sessionData.sede || !sessionData.selectedServiceEnvKey) { await sendWithPresence(sock, jid, "Necesito la sede y el servicio primero."); return; }

  // Guarda el día en contexto para mensajes como "por la tarde" a continuación
  sessionData.dateContextISO = baseFrom.clone().startOf("day").toISOString();

  let slots = []
  let usedPreferred = false
  let attemptedPreferred = false

  // Si se pidió alguien concreto y NO atiende en la sede, avisamos y salimos:
  if (sessionData.preferredStaffId && !isStaffAllowedInLocation(sessionData.preferredStaffId, sessionData.sede)) {
    const name = sessionData.preferredStaffLabel || "esa profesional"
    const allowedKeys = allowedLocKeysForStaff(sessionData.preferredStaffId)
    const allowedNice = allowedKeys.map(locationNice).join(" o ")
    await sendWithPresence(sock, jid, `${name} no atiende en ${locationNice(sessionData.sede)}. Puede atender en ${allowedNice}. ¿Prefieres cambiar de sede o elegir otra profesional en ${locationNice(sessionData.sede)}?`)
    saveSession(phone, sessionData)
    return
  }

  if (sessionData.preferredStaffId) {
    attemptedPreferred = true
    const staffSlots = await searchAvailabilityForStaff({
      locationKey: sessionData.sede,
      envServiceKey: sessionData.selectedServiceEnvKey,
      staffId: sessionData.preferredStaffId,
      fromEU: baseFrom,
      n: 3,
      exactDayOnly,
      daypart
    })
    if (staffSlots.length){ slots = staffSlots; usedPreferred = true }
  }

  if (!slots.length) {
    const generic = await searchAvailabilityGeneric({
      locationKey: sessionData.sede,
      envServiceKey: sessionData.selectedServiceEnvKey,
      fromEU: baseFrom,
      n: 3,
      exactDayOnly,
      daypart
    })
    slots = generic
  }
  if (!slots.length) {
    const generalSlots = proposeSlots({ fromEU: baseFrom, durationMin: 60, n: 3, daypart });
    slots = generalSlots.map(d => ({ date: d, staffId: null }))
  }
  if (!slots.length) { await sendWithPresence(sock, jid, "No encuentro horarios disponibles. ¿Otra fecha?"); return; }

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

  let header = `Horarios disponibles (nuestro equipo):`
  if (usedPreferred) {
    header = `Horarios disponibles con ${sessionData.preferredStaffLabel || "tu profesional"}:`
  } else if (attemptedPreferred && sessionData.preferredExplicit) {
    header = `Horarios disponibles (nuestro equipo):\nNota: no veo huecos con ${sessionData.preferredStaffLabel} en ese rango; te muestro alternativas.`
  }

  await sendWithPresence(sock, jid, `${header}\n${lines}\n\nResponde con el número (1, 2 o 3)`)
}

// ====== Crear reserva
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
    const probe = await searchAvailabilityGeneric({ locationKey: sessionData.sede, envServiceKey: sessionData.selectedServiceEnvKey, fromEU: startEU.clone().subtract(1, "minute"), days: 1, n: 10, exactDayOnly:true })
    const match = probe.find(x => x.date.isSame(startEU, "minute"))
    if (match?.staffId && isStaffAllowedInLocation(match.staffId, sessionData.sede)) staffId = match.staffId
  }
  if (!staffId) staffId = pickStaffForLocation(sessionData.sede, null)
  if (!staffId) { await sendWithPresence(sock, jid, "No hay profesionales disponibles en esa sede"); return; }

  let customerId = sessionData.identityResolvedCustomerId || null
  if (!customerId){
    const { status, customer } = await getUniqueCustomerByPhoneOrPrompt(phone, sessionData, sock, jid) || {}
    if (status === "need_new" || status === "need_pick") return
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
      service_label: sessionData.selectedServiceLabel || "Servicio", duration_min: 60,
      start_iso: startEU.tz("UTC").toISOString(), end_iso: startEU.clone().add(60, "minute").tz("UTC").toISOString(),
      staff_id: staffId, status: "failed", created_at: new Date().toISOString(),
      square_booking_id: null, square_error: result.error, retry_count: SQUARE_MAX_RETRIES
    })
    await sendWithPresence(sock, jid, "No pude crear la reserva ahora. ¿Quieres que te proponga otro horario?")
    return
  }

  if (result.booking.__sim) { await sendWithPresence(sock, jid, "🧪 SIMULACIÓN: Reserva creada (modo prueba)"); clearSession(phone); return }

  const aptId = `apt_${Math.random().toString(36).slice(2,8)}${Date.now().toString(36).slice(-4)}`
  insertAppt.run({
    id: aptId, customer_name: sessionData?.name || null, customer_phone: phone,
    customer_square_id: customerId, location_key: sessionData.sede, service_env_key: sessionData.selectedServiceEnvKey,
    service_label: sessionData.selectedServiceLabel || "Servicio",
    duration_min: 60, start_iso: startEU.tz("UTC").toISOString(), end_iso: startEU.clone().add(60, "minute").tz("UTC").toISOString(),
    staff_id: staffId, status: "confirmed", created_at: new Date().toISOString(),
    square_booking_id: result.booking.id, square_error: null, retry_count: 0
  })

  const staffName = staffLabelFromId(staffId) || sessionData.preferredStaffLabel || "nuestro equipo";
  const address = sessionData.sede === "la_luz" ? ADDRESS_LUZ : ADDRESS_TORRE;
  const svcLabel = sessionData.selectedServiceLabel || "Servicio"
  const confirmMessage = `🎉 ¡Reserva confirmada!

📍 ${locationNice(sessionData.sede)}
${address}

🧾 ${svcLabel}
👩‍💼 ${staffName}
📅 ${fmtES(startEU)}

Ref: ${result.booking.id}

¡Te esperamos!`
  await sendWithPresence(sock, jid, confirmMessage);
  clearSession(phone);
}

// ====== Listar / Cancelar
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
  const message = `Tus próximas citas:\n\n${appointments.map(apt => 
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
    const message = `Estas son tus próximas citas. ¿Cuál quieres cancelar?\n\n${appointments.map(apt => 
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
async function cancelBooking(bookingId){
  if (DRY_RUN) return true
  try{
    const body = { idempotencyKey:`cancel_${bookingId}_${Date.now()}` }
    const resp = await square.bookingsApi.cancelBooking(bookingId, body)
    return !!resp?.result?.booking
  }catch(e){ return false }
}

// ====== Mini web + QR
const app=express()
const PORT=process.env.PORT||8080
let lastQR=null, conectado=false
app.get("/", (_req,res)=>{
  const totalAppts = db.prepare(`SELECT COUNT(*) as count FROM appointments`).get()?.count || 0
  const successAppts = db.prepare(`SELECT COUNT(*) as count FROM appointments WHERE status = 'confirmed'`).get()?.count || 0
  const failedAppts = db.prepare(`SELECT COUNT(*) as count FROM appointments WHERE status = 'failed'`).get()?.count || 0
  res.send(`<!doctype html><meta charset="utf-8"><style>
  body{font-family:system-ui;display:grid;place-items:center;min-height:100vh;background:#f8f9fa}
  .card{max-width:680px;padding:32px;border-radius:20px;box-shadow:0 8px 32px rgba(0,0,0,.1);background:white}
  .status{padding:12px;border-radius:8px;margin:8px 0}
  .success{background:#d4edda;color:#155724}
  .error{background:#f8d7da;color:#721c24}
  .warning{background:#fff3cd;color:#856404}
  .stat{display:inline-block;margin:0 16px;padding:8px 12px;background:#e9ecef;border-radius:6px}
  </style><div class="card">
  <h1>🩷 Gapink Nails Bot v29.2.1</h1>
  <div class="status ${conectado ? 'success' : 'error'}">WhatsApp: ${conectado ? "✅ Conectado" : "❌ Desconectado"}</div>
  ${!conectado&&lastQR?`<div style="text-align:center;margin:20px 0"><img src="/qr.png" width="300" style="border-radius:8px"></div>`:""}
  <div class="status warning">Modo: ${DRY_RUN ? "🧪 Simulación" : "🚀 Producción"}</div>
  <h3>📊 Estadísticas</h3>
  <div><span class="stat">📅 Total: ${totalAppts}</span><span class="stat">✅ Exitosas: ${successAppts}</span><span class="stat">❌ Fallidas: ${failedAppts}</span></div>
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
const QUEUE=new Map()
function enqueue(key,job){
  const prev=QUEUE.get(key)||Promise.resolve()
  const next=prev.then(job,job).finally(()=>{ if (QUEUE.get(key)===next) QUEUE.delete(key) })
  QUEUE.set(key,next); return next
}

function isCancelIntent(text){
  const lower = norm(text)
  return /\b(cancelar|anular|borrar)\b/.test(lower) && /\b(cita|reserva|pr[oó]xima|mi)\b/.test(lower)
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
        try {
          let sessionData = loadSession(phone) || {
            greeted: false, sede: null, category: null,
            selectedServiceEnvKey: null, selectedServiceLabel: null,
            preferredStaffId: null, preferredStaffLabel: null, preferredExplicit: false,
            pendingDateTime: null, name: null, email: null, last_msg_id: null,
            lastStaffByIso: {}, lastProposeUsedPreferred: false, stage: null,
            cancelList: null, serviceChoices: null, identityChoices: null,
            pendingCategory: null, lastStaffNamesById: null, dateContextISO: null,
            snooze_until_ms: null, identityResolvedCustomerId: null
          }
          if (sessionData.last_msg_id === m.key.id) return
          sessionData.last_msg_id = m.key.id

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

          // === Identidad pick
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

          // === Identidad crear
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

          // === Selección de hora (1/2/3)
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
              await executeCreateBooking({}, sessionData, phone, sock, jid)
              return
            }
          }

          // === Cancelar
          if (isCancelIntent(textRaw) && sessionData.stage!=="awaiting_cancel"){
            await executeCancelAppointment({}, sessionData, phone, sock, jid)
            return
          }

          // === “con {nombre}” (ahora entiende *en el mismo mensaje* día y franja)
          const maybeStaff = parsePreferredStaffFromText(textRaw)
          if (maybeStaff){
            const reqDay = parseRequestedDayFromText(textRaw, dayjs().tz(EURO_TZ)) // <-- viernes, 25/08, etc.
            const reqPart = parseDaypart(textRaw) // <-- por la tarde, mañana...
            sessionData.preferredStaffId = maybeStaff.id
            sessionData.preferredStaffLabel = staffLabelFromId(maybeStaff.id)
            sessionData.preferredExplicit = true
            saveSession(phone, sessionData)

            if (sessionData.sede && sessionData.selectedServiceEnvKey){
              await executeProposeTime({
                fromISO: reqDay ? reqDay.toISOString() : undefined,
                exactDayOnly: !!reqDay,
                daypart: reqPart || null
              }, sessionData, phone, sock, jid)
              return
            }
          }

          // === “otro día/por la tarde/mañana” con día en contexto
          {
            const base = sessionData?.dateContextISO 
              ? dayjs(sessionData.dateContextISO).tz(EURO_TZ)
              : dayjs().tz(EURO_TZ);
            const reqDay = parseRequestedDayFromText(textRaw, base);
            const reqPart = parseDaypart(textRaw)
            if (reqDay && sessionData.sede && sessionData.selectedServiceEnvKey){
              await executeProposeTime({ fromISO: reqDay.toISOString(), exactDayOnly:true, daypart:reqPart }, sessionData, phone, sock, jid)
              return
            }
            if (reqPart && sessionData.sede && sessionData.selectedServiceEnvKey && sessionData.dateContextISO){
              const sameDay = dayjs(sessionData.dateContextISO).tz(EURO_TZ)
              await executeProposeTime({ fromISO: sameDay.toISOString(), exactDayOnly:true, daypart:reqPart }, sessionData, phone, sock, jid)
              return
            }
          }

          // === Elección de servicio por número
          if (numMatch && sessionData.stage==="awaiting_service_choice" && Array.isArray(sessionData.serviceChoices)){
            const n = Number(numMatch[1])
            const choice = sessionData.serviceChoices.find(x=>x.index===n)
            if (!choice){ await sendWithPresence(sock, jid, "No encontré esa opción. Prueba con el número de la lista."); return }
            sessionData.selectedServiceLabel = choice.label
            const ek = resolveEnvKeyFromLabelAndSede(choice.label, sessionData.sede)
            sessionData.selectedServiceEnvKey = ek
            sessionData.stage = null
            saveSession(phone, sessionData)
            const prefDay = parseRequestedDayFromText(textRaw, dayjs().tz(EURO_TZ))
            const part = parseDaypart(textRaw)
            if (prefDay) {
              sessionData.dateContextISO = prefDay.clone().startOf("day").toISOString();
              saveSession(phone, sessionData);
            }
            await executeProposeTime({
              fromISO: prefDay ? prefDay.toISOString() : undefined,
              exactDayOnly: !!prefDay,
              daypart: part || null
            }, sessionData, phone, sock, jid)
            return
          }

          // === IA principal
          const aiObj = await getAIResponse(textRaw, sessionData, phone)

          // Propaga updates de IA
          if (aiObj.session_updates) {
            Object.keys(aiObj.session_updates).forEach(key => {
              if (aiObj.session_updates[key] !== null && aiObj.session_updates[key] !== undefined) {
                sessionData[key] = aiObj.session_updates[key]
              }
            })
          }

          // Derivar envKey si hay label+sede
          if (sessionData.sede && sessionData.selectedServiceLabel && !sessionData.selectedServiceEnvKey){
            const ek = resolveEnvKeyFromLabelAndSede(sessionData.selectedServiceLabel, sessionData.sede)
            if (ek) sessionData.selectedServiceEnvKey = ek
          }

          insertAIConversation.run({
            phone, message_id: m.key.id, user_message: textRaw,
            ai_response: safeJSONStringify(aiObj), timestamp: new Date().toISOString(),
            session_data: safeJSONStringify(sessionData),
            ai_error: null,
            fallback_used: 0
          })
          saveSession(phone, sessionData)

          // Routing por acción IA
          switch (aiObj.action) {
            case "choose_category":
              sessionData.category = sessionData.category || parseCategory(textRaw) || null
              saveSession(phone, sessionData)
              await sendWithPresence(sock, jid, "¿Qué categoría necesitas? *uñas*, *depilación*, *micropigmentación* o *facial*.")
              break
            case "set_sede":
              if (!sessionData.sede){
                const s = parseSede(textRaw)
                if (s){ sessionData.sede = s; saveSession(phone, sessionData) }
                await sendWithPresence(sock, jid, "¿En qué sede te viene mejor? *Torremolinos* o *La Luz*.")
              } else {
                await sendWithPresence(sock, jid, `Usaré ${locationNice(sessionData.sede)}.`)
              }
              break
            case "choose_service":
              await executeChooseService(aiObj.action_params, sessionData, phone, sock, jid, textRaw); 
              break
            case "propose_times": {
              const reqDay = parseRequestedDayFromText(textRaw, dayjs().tz(EURO_TZ))
              const part = parseDaypart(textRaw)
              await executeProposeTime({ fromISO: reqDay?reqDay.toISOString():undefined, exactDayOnly:!!reqDay, daypart:part||null }, sessionData, phone, sock, jid)
              break
            }
            case "create_booking":
              await executeCreateBooking(aiObj.action_params, sessionData, phone, sock, jid); 
              break
            case "list_appointments":
              await executeListAppointments(aiObj.action_params, sessionData, phone, sock, jid); 
              break
            case "cancel_appointment":
              await executeCancelAppointment(aiObj.action_params, sessionData, phone, sock, jid); 
              break
            case "need_info":
            case "none":
            default: {
              if (!sessionData.category){
                const cat = parseCategory(textRaw)
                if (cat){ sessionData.category=cat; saveSession(phone, sessionData) }
                else { await sendWithPresence(sock, jid, "Dime la *categoría* (uñas, depilación, micropigmentación, facial)."); break }
              }
              if (!sessionData.sede){
                const s = parseSede(textRaw)
                if (s){ sessionData.sede=s; saveSession(phone, sessionData) }
                else { await sendWithPresence(sock, jid, "¿Sede? *Torremolinos* o *La Luz*."); break }
              }
              if (!sessionData.selectedServiceEnvKey){
                await executeChooseService({}, sessionData, phone, sock, jid, textRaw)
              } else {
                await executeProposeTime({}, sessionData, phone, sock, jid)
              }
            }
          }

        } catch (error) {
          if (BOT_DEBUG) console.error(error)
          await sendWithPresence(sock, jid, "Disculpa, hubo un error técnico. ¿Puedes repetir tu mensaje?")
        }
      })
    })
  }catch(e){ setTimeout(() => startBot().catch(console.error), 5000) }
}

// ====== Arranque
console.log(`🩷 Gapink Nails Bot v29.2.1`)
const appInstance = app.listen(PORT, ()=>{ startBot().catch(console.error) })
process.on("uncaughtException", (e)=>{ console.error("💥 uncaughtException:", e?.stack||e?.message||e) })
process.on("unhandledRejection", (e)=>{ console.error("💥 unhandledRejection:", e) })
process.on("SIGTERM", ()=>{ try{ appInstance.close(()=>process.exit(0)) }catch{ process.exit(0) } })
process.on("SIGINT", ()=>{ try{ appInstance.close(()=>process.exit(0)) }catch{ process.exit(0) } })
