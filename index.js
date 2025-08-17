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

// ====== Pausas por intervención manual (con ".")
const INTERVENTION_PAUSE_HOURS = Number(process.env.INTERVENTION_PAUSE_HOURS || 6)

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
const AI_API_KEY = process.env.DEEPSEEK_API_KEY || ""
const AI_MODEL = process.env.AI_MODEL || "deepseek-chat"
const AI_MAX_RETRIES = Number(process.env.AI_MAX_RETRIES || 3)
const AI_TIMEOUT_MS = Number(process.env.AI_TIMEOUT_MS || 15000)
const sleep = ms => new Promise(r=>setTimeout(r, ms))

// ====== Mensajes fijos
const SELF_SERVICE_LINK = "https://gapinknails.square.site/?source=qr-code"
const WELCOME_MSG =
`Gracias por comunicarte con Gapink Nails. Por favor, haznos saber cómo podemos ayudarte.

Solo atenderemos por WhatsApp y llamadas en horario de lunes a viernes de 10 a 14:00 y de 16:00 a 20:00 

Si quieres reservar una cita puedes hacerlo a través de este link:

${SELF_SERVICE_LINK}

Y si quieres modificarla puedes hacerlo a través del link del sms que llega con su cita! 

Para cualquier otra consulta, déjenos saber y en el horario establecido le responderemos.
Gracias 😘`

const CANCEL_MODIFY_MSG =
`Para *cancelar*, *reagendar* o *editar* tu cita:
• Usa el enlace que recibiste por *SMS* o *email* junto a tu reserva.
• Para *reservar una nueva* cita: ${SELF_SERVICE_LINK}

Si necesitas cualquier otra cosa, dime y te ayudo dentro del horario 🩷`

const VIEW_APPTS_MSG =
`Puedes ver tus *citas* en el enlace de confirmación que recibiste por *SMS* o *email*.
Desde ese enlace podrás ver los detalles, cambiar o cancelar tu reserva. 🩷`

// ====== Utils
const onlyDigits = s => String(s||"").replace(/\D+/g,"")
const rm = s => String(s||"").normalize("NFD").replace(/\p{Diacritic}/gu,"")
const norm = s => rm(s).toLowerCase().replace(/[+.,;:()/_-]/g," ").replace(/[^\p{Letter}\p{Number}\s]/gu," ").replace(/\s+/g," ").trim()

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
function parseToEU(input){
  if (dayjs.isDayjs(input)) return input.clone().tz(EURO_TZ)
  const s = String(input||"")
  if (/[Zz]|[+\-]\d{2}:?\d{2}$/.test(s)) return dayjs(s).tz(EURO_TZ)
  return dayjs.tz(s, EURO_TZ)
}
function fmtES(d){
  const dias=["domingo","lunes","martes","miércoles","jueves","viernes","sábado"]
  const t=(dayjs.isDayjs(d)?d:dayjs(d)).tz(EURO_TZ)
  return `${dias[t.day()]} ${String(t.date()).padStart(2,"0")}/${String(t.month()+1).padStart(2,"0")} ${String(t.hour()).padStart(2,"0")}:${String(t.minute()).padStart(2,"0")}`
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
function stableKey(parts){ const raw=Object.values(parts).join("|"); return createHash("sha256").update(raw).digest("hex").slice(0,48) }
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

// ====== Estado miniweb/QR
let lastQR = null
let conectado = false

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
  if (s.pauseUntil_ms) s.pauseUntil = dayjs.tz(s.pauseUntil_ms, EURO_TZ)
  return s
}
function saveSession(phone,s){
  const c={...s}
  c.lastHours_ms = Array.isArray(s.lastHours)? s.lastHours.map(d=>dayjs.isDayjs(d)?d.valueOf():null).filter(Boolean):[]
  c.pendingDateTime_ms = s.pendingDateTime? dayjs(s.pendingDateTime).valueOf(): null
  c.pauseUntil_ms = s.pauseUntil? dayjs(s.pauseUntil).valueOf(): null
  delete c.lastHours; delete c.pendingDateTime; delete c.pauseUntil
  const j=JSON.stringify(c)
  const up=db.prepare(`UPDATE sessions SET data_json=@j, updated_at=@u WHERE phone=@p`).run({j,u:new Date().toISOString(),p:phone})
  if (up.changes===0) db.prepare(`INSERT INTO sessions (phone,data_json,updated_at) VALUES (@p,@j,@u)`).run({p:phone,j,u:new Date().toISOString()})
}
function clearSession(phone){ db.prepare(`DELETE FROM sessions WHERE phone=@phone`).run({phone}) }
function setPause(phone, hours=INTERVENTION_PAUSE_HOURS){
  const s = loadSession(phone) || { __phone: phone }
  s.pauseUntil = dayjs().tz(EURO_TZ).add(hours, "hour")
  saveSession(phone, s)
}
function isPaused(s){
  if (!s?.pauseUntil) return false
  return dayjs().tz(EURO_TZ).isBefore(s.pauseUntil)
}

// ====== Empleadas/servicios
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
    const allow = (locs||"").split(",").map(s=>s.trim()).filter(Boolean)
    const labels = deriveLabelsFromEnvKey(k)

    // Centro base opcional: EMP_CENTER_<SUFIJO de SQ_EMP_...> = la_luz|torremolinos
    const centerKey = `EMP_CENTER_${k.replace(/^SQ_EMP_/,"")}`
    const rawCenter = String(process.env[centerKey]||"").trim().toLowerCase()
    const baseCenter = rawCenter==="la_luz" ? "la_luz" : rawCenter==="torremolinos" ? "torremolinos" : null

    out.push({ envKey:k, id, bookable, allow, labels, baseCenter })
  }
  return out
}
const EMPLOYEES = parseEmployees()
function staffLabelFromId(id){
  const e = EMPLOYEES.find(x=>x.id===id)
  return e?.labels?.[0] || (id ? `Profesional ${String(id).slice(-4)}` : null)
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

// ====== Acentos/ñ + TitleCase unicode
function toTitleUnicode(s){
  return String(s||"").toLowerCase().replace(/\p{L}[\p{L}\p{M}]*/gu, w => w[0].toUpperCase()+w.slice(1))
}
function replaceWordKeepCase(s, plain, accented){
  const re = new RegExp(`\\b${plain}\\b`, "gi")
  return s.replace(re, (m)=>{
    const up = m===m.toUpperCase(), cap = m[0]===m[0].toUpperCase()
    if (up) return accented.toUpperCase()
    if (cap) return accented[0].toUpperCase()+accented.slice(1)
    return accented
  })
}
function fixDisplayAccents(label){
  let out = String(label||"")
  out = replaceWordKeepCase(out, "unas", "uñas")
  out = replaceWordKeepCase(out, "una", "uña")
  out = replaceWordKeepCase(out, "pestanas", "pestañas")
  out = replaceWordKeepCase(out, "pestana", "pestaña")
  out = replaceWordKeepCase(out, "semipermanete", "semipermanente")
  out = replaceWordKeepCase(out, "nivelacion", "nivelación")
  out = replaceWordKeepCase(out, "mas", "más")
  return out
}
function titleCaseFromEnvKey(raw){ return toTitleUnicode(raw) }
function cleanDisplayLabel(label){ return String(label||"").replace(/^\s*(luz|la\s*luz)\s+/i,"").trim() }
function servicesForSedeKeyRaw(salonKey){
  const prefix = (salonKey==="la_luz") ? "SQ_SVC_luz_" : "SQ_SVC_"
  const out=[]
  for (const [k,v] of Object.entries(process.env)){
    if (!k.startsWith(prefix)) continue
    const [id] = String(v||"").split("|"); if (!id) continue
    const raw = k.replace(prefix,"").replaceAll("_"," ")
    let label = titleCaseFromEnvKey(raw)
    label = cleanDisplayLabel(label)
    label = fixDisplayAccents(label)
    out.push({ salonKey, key:k, id, rawKey:k, label, norm: norm(label) })
  }
  return out
}
function allServices(){ return [...servicesForSedeKeyRaw("torremolinos"), ...servicesForSedeKeyRaw("la_luz")] }
function serviceLabelFromEnvKey(envKey){
  if (!envKey) return null
  const all = allServices()
  return all.find(s=>s.key===envKey)?.label || null
}

// ====== Categorías
const CATEGORY_PRETTY = { unas:"uñas", pestanas:"pestañas", cejas:"cejas", pedicura:"pedicura", manicura:"manicura" }
function detectCategory(text){
  const t = norm(text)
  if (/\b(ceja|cejas|depila(r|cion)\s*cejas?|dise(n|ñ)o\s*de\s*cejas?)\b/.test(t)) return "cejas"
  if (/\b(pesta(n|ñ)a(s)?|2d|3d|pelo a pelo|lifting|lash|eyelash)\b/.test(t)) return "pestanas"
  if (/\b(pedicur|pies|pie)\b/.test(t)) return "pedicura"
  if (/\b(manicur|semi|semipermanente|esmalte|u(n|ñ)a(s)?|esculpidas?)\b/.test(t)) return "manicura"
  if (/\b(u(n|ñ)a(s)?|acril|gel|tips)\b/.test(t)) return "unas"
  return null
}
function filterServicesByCategory(list, category){
  const L = list
  const has = (s, rex) => rex.test(s.norm)
  switch(category){
    case "cejas":
      return L.filter(s => has(s, /\b(ceja|cejas)\b/) && !has(s, /\b(pesta(n|ñ)a(s)?|mani|pedi|u(n|ñ)a(s)?)\b/))
    case "pestanas":
      return L.filter(s => has(s, /\b(pesta(n|ñ)a(s)?|2d|3d|pelo a pelo|lifting)\b/))
    case "pedicura":
      return L.filter(s => has(s, /\b(pedicur|pies|pie)\b/))
    case "manicura":
      return L.filter(s => has(s, /\b(manicur|u(n|ñ)a(s)?|esculpidas?|semipermanente|esmalte)\b/) && !has(s, /\b(pedicur|pies|pie)\b/))
    case "unas":
    default:
      return L.filter(s => has(s, /\b(u(n|ñ)a(s)?|manicur|pedicur|esculpidas?|semipermanente|esmalte)\b/))
  }
}
function servicesForCategory(salonKey, category){
  const base = servicesForSedeKeyRaw(salonKey)
  const list = filterServicesByCategory(base, category||"unas")
  const seen=new Set(), fin=[]
  for (const s of list){ const k=s.label.toLowerCase(); if (seen.has(k)) continue; seen.add(k); fin.push(s) }
  return fin
}
function isLabelInCategory(salonKey, label, category){
  if (!label || !category) return true
  const list = servicesForCategory(salonKey, category)
  return list.some(s=>s.label.toLowerCase()===String(label).toLowerCase())
}
function resolveEnvKeyFromLabelAndSede(label, salonKey){
  if (!label || !salonKey) return null
  const list = servicesForSedeKeyRaw(salonKey)
  return list.find(s=>s.label.toLowerCase()===String(label||"").toLowerCase())?.key || null
}

// ====== Matching servicio
function generalServiceScore(userText,label){
  const u=norm(userText), l=norm(label); let score=0
  const keys = [
    ["uñas","uñas"],["uña","uña"],["manicura","manicura"],["pedicura","pedicura"],["pies","pedicura"],
    ["semipermanente","semipermanente"],["spa","spa"],["rusa","rusa"],["relleno","relleno"],
    ["francesa","francesa"],["baby","baby"],["boomer","boomer"],["encapsulado","encapsulado"],
    ["nivelacion","nivelación"],["gel","gel"],["acril","acrílico"],
    ["pestañas","pestañas"],["lifting","lifting"],["2d","2d"],["3d","3d"],["pelo a pelo","pelo a pelo"],
    ["cejas","cejas"],["depilar","depilar"],["depilacion","depilación"],["diseño","diseño"]
  ]
  for (const [t,alias] of keys){ if (u.includes(norm(t)) && l.includes(norm(alias))) score+=2 }
  const utoks=new Set(u.split(" ").filter(Boolean)); const ltoks=new Set(l.split(" ").filter(Boolean))
  let overlap=0; for (const t of utoks){ if (ltoks.has(t)) overlap++ }
  score += Math.min(overlap,5)*0.4
  if (u===l) score += 10
  if (l.includes(u) || u.includes(l)) score += 5
  return score
}
function fuzzyFindBestService(salonKey, text, categoryHint=null){
  const pool = categoryHint ? servicesForCategory(salonKey, categoryHint) : servicesForSedeKeyRaw(salonKey)
  let best=null, bestScore=0
  for (const s of pool){
    const sc = generalServiceScore(text, s.label)
    if (sc>bestScore){ best=s; bestScore=sc }
  }
  return (bestScore>=2.5) ? best : null
}

// ====== IA Quick Extract (SIN bandera de intervención manual)
async function aiQuickExtract(userText){
  if (!AI_API_KEY) return null
  const controller = new AbortController()
  const to = setTimeout(()=>controller.abort(), AI_TIMEOUT_MS)
  try{
    const promptSys = `Eres un extractor experto. Devuelve SOLO JSON:
{"intent":"book|cancel|modify|list|info|other","sede":"torremolinos|la_luz|null","category":"unas|pestanas|cejas|pedicura|manicura|null","serviceLabel":"cadena o null","staffName":"cadena o null","staffIntent":"pick|suggest|any|none","urgency":"asap|specific|none"}`
    const body = { model: AI_MODEL, messages:[
      {role:"system", content: promptSys},
      {role:"user", content: `Texto: "${userText}"\nResponde SOLO el JSON.`}
    ], max_tokens: 200, temperature: 0 }
    const resp = await fetch("https://api.deepseek.com/chat/completions",{
      method:"POST",
      headers:{ "Content-Type":"application/json", "Authorization":`Bearer ${AI_API_KEY}` },
      body: JSON.stringify(body),
      signal: controller.signal
    })
    clearTimeout(to)
    if (!resp.ok) return null
    const data = await resp.json()
    let out = data?.choices?.[0]?.message?.content || ""
    out = out.replace(/```json|```/g,"").trim()
    try{ return JSON.parse(out) }catch{ return null }
  }catch{ clearTimeout(to); return null }
}

// ====== Square helpers
async function searchCustomersByPhone(phone){
  try{
    const e164=normalizePhoneES(phone); if(!e164) return []
    const got = await square.customersApi.searchCustomers({ query:{ filter:{ phoneNumber:{ exact:e164 } } } })
    return got?.result?.customers || []
  }catch{ return [] }
}
async function getUniqueCustomerByPhoneOrPrompt(phone, sessionData, sock, jid){
  // Se llama SOLO después de tener hora elegida (evita lío de doble ficha antes de tiempo)
  if (sessionData.customerId) return { status:"single", customer: { id: sessionData.customerId } }
  const matches = await searchCustomersByPhone(phone)
  if (matches.length === 1){
    const c = matches[0]
    sessionData.name = sessionData.name || c?.givenName || null
    sessionData.email = sessionData.email || c?.emailAddress || null
    sessionData.customerId = c.id
    return { status:"single", customer:c }
  }
  if (matches.length === 0){
    sessionData.stage = "awaiting_identity"
    saveSession(sessionData.__phone, sessionData)
    await sock.sendMessage(jid, { text: "No encuentro tu ficha por este número. Dime tu *nombre completo* y, si quieres, tu *email* para crearte 😊" })
    return { status:"need_new" }
  }
  const choices = matches.map((c,i)=>({ index:i+1, id:c.id, name:c?.givenName || "Sin nombre", email:c?.emailAddress || "—" }))
  sessionData.identityChoices = choices
  sessionData.stage = "awaiting_identity_pick"
  saveSession(sessionData.__phone, sessionData)
  const lines = choices.map(ch => `${ch.index}) ${ch.name} ${ch.email!=="—" ? `(${ch.email})`:""}`).join("\n")
  await sock.sendMessage(jid, { text: `He encontrado varias fichas con tu número. ¿Cuál eres?\n\n${lines}\n\nResponde con el número.` })
  return { status:"need_pick" }
}
async function findOrCreateCustomerWithRetry({ name, email, phone }){
  let lastError = null
  for (let attempt = 1; attempt <= SQUARE_MAX_RETRIES; attempt++) {
    try{
      const e164=normalizePhoneES(phone); if(!e164) return null
      const got = await square.customersApi.searchCustomers({ query:{ filter:{ phoneNumber:{ exact:e164 } } } })
      const c=(got?.result?.customers||[])[0]; if (c) return c
      const created = await square.customersApi.createCustomer({
        idempotencyKey:`cust_${Date.now()}_${Math.random().toString(36).slice(2,6)}`,
        givenName:name||undefined, emailAddress:email||undefined, phoneNumber:e164||undefined
      })
      const newCustomer = created?.result?.customer||null
      if (newCustomer) return newCustomer
    } catch(e) { lastError = e; if (attempt < SQUARE_MAX_RETRIES) await sleep(1000 * attempt) }
  }
  return null
}
async function getServiceIdAndVersion(envKey){
  const raw = process.env[envKey]; if (!raw) return null
  let [id, ver] = String(raw).split("|"); ver=ver?Number(ver):null
  if (!id) return null
  if (!ver){
    try{ 
      const resp=await square.catalogApi.retrieveCatalogObject(id,true)
      const vRaw = resp?.result?.object?.version
      ver = vRaw != null ? Number(vRaw) : 1
    } catch { ver=1 }
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
        booking:{ locationId: locationToId(locationKey), startAt: startISO, customerId,
          appointmentSegments:[{ teamMemberId, serviceVariationId: sv.id, serviceVariationVersion: Number(sv.version), durationMinutes: durationMin||60 }] }
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
async function cancelBooking(_bookingId){ return false } // autoservicio

// ====== Citas por teléfono (desactivado: redirige a SMS/email)
async function enumerateCitasByPhone(_phone){
  return [] // ahora no enumeramos
}

// ====== DISPONIBILIDAD
async function searchAvailabilityForStaff({ locationKey, envServiceKey, staffId, fromEU, days=14, n=3, distinctDays=false }){
  try{
    const sv = await getServiceIdAndVersion(envServiceKey)
    if (!sv?.id || !staffId) return []
    const startAt = fromEU.tz("UTC").toISOString()
    const endAt = fromEU.clone().add(days,"day").tz("UTC").toISOString()
    const locationId = locationToId(locationKey)
    const body = { query:{ filter:{ startAtRange:{ startAt, endAt }, locationId,
      segmentFilters:[{ serviceVariationId: sv.id, teamMemberIdFilter:{ any:[ staffId ] } }] } } }
    const resp = await square.bookingsApi.searchAvailability(body)
    const avail = resp?.result?.availabilities || []
    const slots=[], seenDays=new Set()
    for (const a of avail){
      if (!a?.startAt) continue
      const d = dayjs(a.startAt).tz(EURO_TZ)
      if (!insideBusinessHours(d,60)) continue
      if (distinctDays){ const key=d.format("YYYY-MM-DD"); if (seenDays.has(key)) continue; seenDays.add(key) }
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
      if (distinctDays){ const key=d.format("YYYY-MM-DD"); if (seenDays.has(key)) continue; seenDays.add(key) }
      slots.push({ date:d, staffId: tm })
      if (slots.length>=n) break
    }
    return slots
  }catch{ return [] }
}

// ====== IA conversacional core
async function callAIOnce(messages, systemPrompt = "") {
  const controller = new AbortController()
  const timeoutId = setTimeout(() => controller.abort(), AI_TIMEOUT_MS)
  try {
    const allMessages = systemPrompt ? [{ role: "system", content: systemPrompt }, ...messages] : messages
    const response = await fetch("https://api.deepseek.com/chat/completions", {
      method: "POST",
      headers: { "Content-Type": "application/json", "Authorization": `Bearer ${AI_API_KEY}` },
      body: JSON.stringify({ model: AI_MODEL, messages: allMessages, max_tokens: 1500, temperature: 0.6, stream: false }),
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
    const base = e.baseCenter ? (e.baseCenter==="la_luz"?"la_luz":"torremolinos") : "—"
    return `• ID:${e.id} | Nombres:[${e.labels.join(", ")}] | Sedes:[${locs||"ALL"}] | Base:${base} | Reservable:${e.bookable}`
  }).join("\n")
}
function buildSystemPrompt() {
  const nowEU = dayjs().tz(EURO_TZ);
  const torremolinos_services = servicesForSedeKeyRaw("torremolinos");
  const laluz_services = servicesForSedeKeyRaw("la_luz");
  const staffLines = staffRosterForPrompt()
  return `Eres el asistente de WhatsApp para Gapink Nails. Devuelves SOLO JSON válido.

INFO:
- Fecha/hora: ${nowEU.format("dddd DD/MM/YYYY HH:mm")} (Madrid)

ROSTER:
${staffLines}

SERVICIOS TORREMOLINOS:
${torremolinos_services.map(s => "- "+s.label+" (Clave: "+s.key+")").join("\n")}
SERVICIOS LA LUZ:
${laluz_services.map(s => "- "+s.label+" (Clave: "+s.key+")").join("\n")}

REGLAS CLAVE:
1) Cancelar/cambiar/editar → autoservicio (SMS/email).
2) Tildes y ñ SIEMPRE. Usa “salón”.
3) Coherencia con slots/roster. Valida staff por salón.
4) Para reservar: salón + servicio + fecha/hora. Si falta *empleada*, elige (IA) o sugiere lista ordenada por disponibilidad.
5) Si el servicio no está claro, lista por *categoría* detectada.
6) Si el usuario dice “me da igual” o “elige tú”, selecciona la profesional con *próxima disponibilidad*.
7) Si el cliente quiere *ver sus citas*, NO las enumeres → acción "list_appointments" (el bot enviará instrucciones para verlas por SMS/email).
8) Si el usuario pide "lo antes posible/asap/primera hora", marca urgency=asap y procede a confirmar el primer hueco disponible automáticamente.

FORMATO:
{"message":"...","action":"propose_times|create_booking|list_appointments|cancel_appointment|choose_service|choose_staff|need_info|none","session_updates":{"sede": "...","selectedServiceLabel":"...","selectedServiceEnvKey":"...","preferredStaffId":"...","preferredStaffLabel":"...","asapWanted":true|false},"action_params":{"category":"unas|pestanas|cejas|pedicura|manicura","candidates":[{"label":"...","confidence":0-1}],"staffCandidates":[{"name":"...","confidence":0-1}]}}`
}

// ====== Heurísticas staff
function _normName(s){ return norm(String(s||"")).replace(/\s+/g," ").trim() }
function parseSede(text){
  const t=norm(text)
  if (/\b(luz|la luz)\b/.test(t)) return "la_luz"
  if (/\b(torre|torremolinos)\b/.test(t)) return "torremolinos"
  return null
}
// Levenshtein
function levenshtein(a,b){
  a=_normName(a); b=_normName(b)
  const m=a.length, n=b.length
  if (m===0) return n
  if (n===0) return m
  const dp=new Array(n+1)
  for (let j=0;j<=n;j++) dp[j]=j
  for (let i=1;i<=m;i++){
    let prev=dp[0]; dp[0]=i
    for (let j=1;j<=n;j++){
      const temp=dp[j]
      const cost = a[i-1]===b[j-1] ? 0 : 1
      dp[j] = Math.min(dp[j]+1, dp[j-1]+1, prev+cost)
      prev=temp
    }
  }
  return dp[n]
}
function nameSim(a,b){
  const A=_normName(a), B=_normName(b)
  const dist = levenshtein(A,B)
  const maxLen = Math.max(A.length,B.length)
  return maxLen ? (1 - dist/maxLen) : 0
}
function suggestClosestStaff(input, locKey){
  const locId = locKey ? locationToId(locKey) : null
  let best=null, bestScore=0
  for (const e of EMPLOYEES){
    if (!e.bookable) continue
    if (locId && !(e.allow.includes("ALL") || e.allow.includes(locId))) continue
    for (const label of e.labels){
      const s=nameSim(input,label)
      if (s>bestScore){ best={...e, matchLabel:label}, bestScore=s }
    }
  }
  if (best && bestScore>=0.72) return best
  return null
}
function findStaffByName(inputName, locKey=null){
  const q = _normName(inputName||""); if (!q) return null
  const locId = locKey ? locationToId(locKey) : null
  for (const e of EMPLOYEES){
    if (!e.bookable) continue
    if (locId){
      const allowed = (e.allow.includes("ALL") || e.allow.includes(locId))
      if (!allowed) continue
    }
    for (const l of e.labels){
      const L = _normName(l); if (!L) continue
      if (L===q || L.includes(q) || q.includes(L)) return e
    }
    const pretty = _normName(staffLabelFromId(e.id))
    if (pretty && (pretty===q || pretty.includes(q) || q.includes(pretty))) return e
  }
  return null
}
function findStaffAnySalon(inputName){
  const q=_normName(inputName||""); if(!q) return null
  for (const e of EMPLOYEES){
    if (!e.bookable) continue
    for (const l of e.labels){
      const L=_normName(l)
      if (L===q || L.includes(q) || q.includes(L)) return e
    }
    const pretty=_normName(staffLabelFromId(e.id))
    if (pretty && (pretty===q || pretty.includes(q) || q.includes(pretty))) return e
  }
  return null
}
function findStaffElsewhere(inputName, currentSalonKey){
  const match = findStaffAnySalon(inputName)
  if (!match) return null
  const sedes = match.allow.includes("ALL")
    ? ["torremolinos","la_luz"]
    : match.allow.map(id => idToLocKey(id)).filter(Boolean)
  if (!sedes.length) return null
  if (currentSalonKey && sedes.includes(currentSalonKey)) return null
  return { ...match, sedes }
}

function extractStaffAsk(text){
  const m = text.match(/\bcon\s+([a-záéíóúñü]+(?:\s+[a-záéíóúñü]+){0,2})\b/i)
  return m ? m[1].trim() : null
}
function isAffirmative(text){ return /\b(s[ií]|si|vale|ok|claro|de acuerdo|perfecto|sí)\b/i.test(text) }
function isNegative(text){ return /\b(no|nah|nop)\b/i.test(text) }
function isIndifferent(text){ return /\b(me da igual|cualquiera|quien sea|elige t[uú]|como veas)\b/i.test(norm(text)) }
function isASAP(text){ return /\b(antes posible|lo mas pronto|lo más pronto|asap|cuanto antes|primera hora|lo primero que tengas)\b/i.test(norm(text)) }

// ====== Ranking de staff por disponibilidad
async function rankStaffForService(salonKey, envServiceKey, fromEU){
  const locId = locationToId(salonKey)
  const allowed = EMPLOYEES.filter(e=> e.bookable && (e.allow.includes("ALL") || e.allow.includes(locId)))
  const rows=[]
  for (const e of allowed){
    const slots = await searchAvailabilityForStaff({ locationKey: salonKey, envServiceKey, staffId: e.id, fromEU, n: 1 })
    const next = slots[0]?.date || null
    rows.push({ staff:e, next })
  }
  rows.sort((a,b)=>{
    if (a.next && b.next) return a.next.valueOf() - b.next.valueOf()
    if (a.next && !b.next) return -1
    if (!a.next && b.next) return 1
    return 0
  })
  return rows
}

// ====== Híbrido IA + Heurística
async function ensureCoreFromText(sessionData, userText){
  let changed=false
  const extracted = await aiQuickExtract(userText)
  if (extracted){
    sessionData.__last_intent = extracted.intent || null

    if (!sessionData.sede && (extracted.sede==="torremolinos" || extracted.sede==="la_luz")){
      sessionData.sede = extracted.sede; changed=true
    }
    if (!sessionData.pendingCategory && extracted.category){
      sessionData.pendingCategory = extracted.category; changed=true
    } else if (!sessionData.pendingCategory){
      const catHeu = detectCategory(userText)
      if (catHeu){ sessionData.pendingCategory = catHeu; changed=true }
    }
    if (!sessionData.selectedServiceEnvKey && sessionData.sede){
      const best = (extracted.serviceLabel && fuzzyFindBestService(sessionData.sede, extracted.serviceLabel, sessionData.pendingCategory))
               || fuzzyFindBestService(sessionData.sede, userText, sessionData.pendingCategory)
      if (best){
        sessionData.selectedServiceLabel = best.label
        sessionData.selectedServiceEnvKey = best.key
        changed=true
      }
    }
    // Staff
    if (!sessionData.preferredStaffId && extracted.staffName){
      // si no hay salón aún, usa centro base si existe
      let staffAny = findStaffAnySalon(extracted.staffName) || suggestClosestStaff(extracted.staffName, null)
      if (staffAny && !sessionData.sede && staffAny.baseCenter){
        sessionData.sede = staffAny.baseCenter
        changed = true
      }
      const staff = findStaffByName(extracted.staffName, sessionData.sede) || suggestClosestStaff(extracted.staffName, sessionData.sede)
      if (staff){
        sessionData.preferredStaffId = staff.id
        sessionData.preferredStaffLabel = staff.labels?.[0] || staffLabelFromId(staff.id)
        changed=true
      } else {
        const elsewhere = findStaffElsewhere(extracted.staffName, sessionData.sede)
        if (elsewhere){
          sessionData.stage = "confirm_switch_salon"
          sessionData.switchTargetSalon = elsewhere.sedes.includes("torremolinos") ? "torremolinos" : "la_luz"
          changed=true
        }
      }
    }
    // ASAP
    if (extracted.urgency === "asap" || isASAP(userText)){
      sessionData.asapWanted = true
      changed = true
    }
    if (!sessionData.preferredStaffId && extracted.staffIntent==="any" && sessionData.sede && sessionData.selectedServiceEnvKey){
      const baseFrom = nextOpeningFrom(dayjs().tz(EURO_TZ).add(NOW_MIN_OFFSET_MIN, "minute"))
      const ranked = await rankStaffForService(sessionData.sede, sessionData.selectedServiceEnvKey, baseFrom)
      const top = ranked.find(r=>!!r.next)
      if (top){
        sessionData.preferredStaffId = top.staff.id
        sessionData.preferredStaffLabel = top.staff.labels?.[0] || staffLabelFromId(top.staff.id)
        changed=true
      }
    }
  } else {
    if (!sessionData.sede){
      const sede = parseSede(userText); if (sede){ sessionData.sede=sede; changed=true }
    }
    if (!sessionData.pendingCategory){
      const cat = detectCategory(userText); if (cat){ sessionData.pendingCategory = cat; changed=true }
    }
    if (!sessionData.selectedServiceEnvKey && sessionData.sede){
      const best = fuzzyFindBestService(sessionData.sede, userText, sessionData.pendingCategory)
      if (best){
        sessionData.selectedServiceLabel = best.label
        sessionData.selectedServiceEnvKey = best.key
        changed=true
      }
    }
    if (!sessionData.preferredStaffId && isIndifferent(userText) && sessionData.sede && sessionData.selectedServiceEnvKey){
      const baseFrom = nextOpeningFrom(dayjs().tz(EURO_TZ).add(NOW_MIN_OFFSET_MIN,"minute"))
      const ranked = await rankStaffForService(sessionData.sede, sessionData.selectedServiceEnvKey, baseFrom)
      const top = ranked.find(r=>!!r.next)
      if (top){
        sessionData.preferredStaffId = top.staff.id
        sessionData.preferredStaffLabel = top.staff.labels?.[0] || staffLabelFromId(top.staff.id)
        changed=true
      }
    }
    if (isASAP(userText)){ sessionData.asapWanted = true; changed = true }
  }
  if (sessionData.sede && sessionData.selectedServiceLabel && sessionData.pendingCategory){
    if (!isLabelInCategory(sessionData.sede, sessionData.selectedServiceLabel, sessionData.pendingCategory)){
      sessionData.selectedServiceLabel = null
      sessionData.selectedServiceEnvKey = null
      changed = true
    }
  }
  // Si definimos staff preferida y aún no hay sede, usa su centro base
  if (!sessionData.sede && sessionData.preferredStaffId){
    const e = EMPLOYEES.find(x=>x.id===sessionData.preferredStaffId)
    if (e?.baseCenter){ sessionData.sede = e.baseCenter; changed = true }
  }
  return changed
}

// ====== Scoring y listas
function scoreServiceRelevance(userMsg, label){
  const u = norm(userMsg), l = norm(label); let score = 0
  if (/\b(uñas|manicura|pedicura|pestañas|cejas)\b/.test(u) && /\b(uñas|manicura|pedicura|pestañas|cejas)\b/.test(l)) score += 3
  if (/\bsemipermanente\b/.test(u) && l.includes("semipermanente")) score += 2.5
  if (/\bspa\b/.test(u) && l.includes("spa")) score += 1.5
  if (/\brusa\b/.test(u) && l.includes("rusa")) score += 1.2
  if (/\bceja|cejas|depil\b/.test(u) && /\bceja|cejas|depil\b/.test(l)) score += 3
  if (/\bpesta(n|ñ)a|2d|3d|pelo a pelo|lifting\b/.test(u) && /\bpesta(n|ñ)a|2d|3d|pelo a pelo|lifting\b/.test(l)) score += 3
  const utoks = new Set(u.split(" ").filter(Boolean))
  const ltoks = new Set(l.split(" ").filter(Boolean))
  let overlap=0; for (const t of utoks){ if (ltoks.has(t)) overlap++ }
  score += Math.min(overlap,3)*0.25
  return score
}
function buildServiceChoiceListBySedeCategory(salonKey, userMsg, category, aiCandidates){
  const list = servicesForCategory(salonKey, category)
  if (!list.length) return []
  const localScores = new Map()
  for (const s of list){ localScores.set(s.label, scoreServiceRelevance(userMsg, s.label)) }
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
  const inAI = list.filter(s=>aiMap.has(s.label)).sort((a,b)=> (aiMap.get(b.label)-aiMap.get(a.label)) || ((localScores.get(b.label)||0)-(localScores.get(a.label)||0)))
  const rest = list.filter(s=>!aiMap.has(s.label)).sort((a,b)=> (localScores.get(b.label)||0)-(localScores.get(a.label)||0))
  const final = [...inAI, ...rest]
  return final.map((s,i)=>({ index:i+1, label:s.label }))
}

// ====== Acciones
async function executeChooseService(params, sessionData, phone, sock, jid, userMsg){
  const category = params?.category || sessionData.pendingCategory || detectCategory(userMsg||"") || "unas"
  sessionData.pendingCategory = category
  if (!sessionData.sede){
    sessionData.stage = "awaiting_salon_for_services"
    saveSession(phone, sessionData)
    await sendWithPresence(sock, jid, `¿En qué *salón* te viene mejor, Torremolinos o La Luz? (así te muestro las opciones de ${CATEGORY_PRETTY[category]})`)
    return
  }
  const aiCands = Array.isArray(params?.candidates) ? params.candidates : []
  const items = buildServiceChoiceListBySedeCategory(sessionData.sede, userMsg||"", category, aiCands)
  if (!items.length){ await sendWithPresence(sock, jid, `Ahora mismo no tengo servicios de ${CATEGORY_PRETTY[category]} en ese salón.`); return }
  sessionData.serviceChoices = items
  sessionData.stage = "awaiting_service_choice"
  saveSession(phone, sessionData)
  const lines = items.map(it=> {
    const star = aiCands.find(c=>cleanDisplayLabel(String(c.label||"")).toLowerCase()===it.label.toLowerCase()) ? " ⭐" : ""
    return `${it.index}) ${it.label}${star}`
  }).join("\n")
  await sendWithPresence(sock, jid, `Estas son nuestras opciones de **${CATEGORY_PRETTY[category]}** en ${locationNice(sessionData.sede)}:\n\n${lines}\n\nResponde con el número.`)
}

async function executeChooseStaff(params, sessionData, phone, sock, jid){
  if (!sessionData.sede || !sessionData.selectedServiceEnvKey){
    await sendWithPresence(sock, jid, "Primero dime el *salón* y el *servicio* para proponerte profesionales.")
    return
  }
  const baseFrom = nextOpeningFrom(dayjs().tz(EURO_TZ).add(NOW_MIN_OFFSET_MIN,"minute"))
  const ranked = await rankStaffForService(sessionData.sede, sessionData.selectedServiceEnvKey, baseFrom)
  const aiCands = Array.isArray(params?.staffCandidates) ? params.staffCandidates : []
  const aiMap = new Map()
  aiCands.forEach(c=> aiMap.set(_normName(c.name||""), Number(c.confidence||0)))
  const rows = ranked.filter(r=>r.staff).map((r,i)=>({
    index: i+1,
    id: r.staff.id,
    name: r.staff.labels?.[0] || staffLabelFromId(r.staff.id) || `Profesional ${String(r.staff.id).slice(-4)}`,
    next: r.next ? fmtES(r.next) : null,
    ai: aiMap.get(_normName(r.staff.labels?.[0]||"")) || 0
  }))
  rows.sort((a,b)=>{
    if (b.ai!==a.ai) return b.ai - a.ai
    if (a.next && b.next) return dayjs(a.next,"dddd DD/MM HH:mm") - dayjs(b.next,"dddd DD/MM HH:mm")
    if (a.next && !b.next) return -1
    if (!a.next && b.next) return 1
    return 0
  })
  const top = rows.slice(0,5)
  sessionData.staffChoices = top
  sessionData.stage = "awaiting_staff_choice"
  saveSession(phone, sessionData)
  const lines = top.map(r => `${r.index}) ${r.name}${r.next?` — siguiente: ${r.next}`:""}`).join("\n")
  await sendWithPresence(sock, jid, `¿Con quién prefieres?\n\n${lines}\n\nResponde con el número.`)
}

async function executeProposeTime(_params, sessionData, phone, sock, jid) {
  const nowEU = dayjs().tz(EURO_TZ);
  const baseFrom = nextOpeningFrom(nowEU.add(NOW_MIN_OFFSET_MIN, "minute"));
  if (!sessionData.sede || !sessionData.selectedServiceEnvKey) {
    const cat = sessionData.pendingCategory ? ` de ${CATEGORY_PRETTY[sessionData.pendingCategory]}` : ""
    await sendWithPresence(sock, jid, `Para proponerte horas necesito el *salón* y el *servicio*${cat}. Por ejemplo: *“en Torremolinos, ${sessionData.pendingCategory==="cejas"?"depilar cejas":"manicura semipermanente"}”* 😉`);
    return;
  }

  // Valida preferida en salón
  if (sessionData.preferredStaffId){
    const e = EMPLOYEES.find(x=>x.id===sessionData.preferredStaffId)
    const locId = locationToId(sessionData.sede)
    if (!e || !(e.allow.includes("ALL") || e.allow.includes(locId))){
      sessionData.preferredStaffId = null
      sessionData.preferredStaffLabel = null
    }
  }

  let slots = []
  let usedPreferred = false
  if (sessionData.preferredStaffId) {
    const staffSlots = await searchAvailabilityForStaff({ locationKey: sessionData.sede, envServiceKey: sessionData.selectedServiceEnvKey, staffId: sessionData.preferredStaffId, fromEU: baseFrom, n: 3 })
    if (staffSlots.length){ slots = staffSlots; usedPreferred = true }
  }
  if (!slots.length) {
    const generic = await searchAvailabilityGeneric({ locationKey: sessionData.sede, envServiceKey: sessionData.selectedServiceEnvKey, fromEU: baseFrom, n: 3 })
    slots = generic
  }
  if (!slots.length) {
    const generalSlots = [baseFrom, baseFrom.add(1,"hour"), baseFrom.add(2,"hour")].map(t=>ceilToSlotEU(t))
    slots = generalSlots.map(d => ({ date: d, staffId: null }))
  }
  if (!slots.length) { await sendWithPresence(sock, jid, "No encuentro horarios disponibles en los próximos días. ¿Otra fecha?"); return; }

  // ASAP → confirmamos primer hueco automático
  if (sessionData.asapWanted === true){
    const first = slots[0]
    sessionData.pendingDateTime = first.date.toISOString()
    sessionData.preferredStaffId = first.staffId || sessionData.preferredStaffId || null
    sessionData.preferredStaffLabel = first.staffId ? (staffLabelFromId(first.staffId) || null) : sessionData.preferredStaffLabel || null
    saveSession(phone, sessionData)
    await executeCreateBooking({}, sessionData, phone, sock, jid)
    return
  }

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

async function executeCreateBooking(_params, sessionData, phone, sock, jid) {
  if (!sessionData.sede) { await sendWithPresence(sock, jid, "Falta el *salón* (Torremolinos o La Luz)"); return; }
  if (sessionData.pendingCategory && sessionData.selectedServiceLabel && !isLabelInCategory(sessionData.sede, sessionData.selectedServiceLabel, sessionData.pendingCategory)){
    await executeChooseService({ category: sessionData.pendingCategory, candidates: [] }, sessionData, phone, sock, jid, sessionData.__last_user_text || "")
    return
  }
  if (!sessionData.selectedServiceEnvKey) {
    await executeChooseService({ category: sessionData.pendingCategory || "unas", candidates: [] }, sessionData, phone, sock, jid, sessionData.__last_user_text || "")
    return
  }
  if (!sessionData.pendingDateTime) { await sendWithPresence(sock, jid, "Dime la *hora* (elige 1/2/3 o escribe una)."); return; }

  const startEU = parseToEU(sessionData.pendingDateTime)
  if (!insideBusinessHours(startEU, 60)) { await sendWithPresence(sock, jid, "Esa hora está fuera del horario (L-V 09:00–20:00)"); return; }

  const iso = startEU.format("YYYY-MM-DDTHH:mm")
  let staffId = sessionData.lastProposeUsedPreferred ? (sessionData.preferredStaffId || sessionData.lastStaffByIso?.[iso] || null)
                                                    : (sessionData.lastStaffByIso?.[iso] || sessionData.preferredStaffId || null)
  if (!staffId) {
    const probe = await searchAvailabilityGeneric({ locationKey: sessionData.sede, envServiceKey: sessionData.selectedServiceEnvKey, fromEU: startEU.clone().subtract(1, "minute"), days: 1, n: 10 })
    const match = probe.find(x => x.date.isSame(startEU, "minute"))
    if (match?.staffId) staffId = match.staffId
  }
  if (!staffId) staffId = pickStaffForLocation(sessionData.sede, null)
  if (!staffId) { await sendWithPresence(sock, jid, "No hay profesionales disponibles en ese salón"); return; }

  let customerId = sessionData.customerId || null

  // Resolver ficha solo ahora (ya tenemos hora)
  if (!customerId){
    sessionData.__phone = phone
    const got = await getUniqueCustomerByPhoneOrPrompt(phone, sessionData, sock, jid)
    if (got?.status === "need_new" || got?.status === "need_pick") return
    customerId = got?.customer?.id || null
  }

  if (!customerId && (sessionData.name || sessionData.email)){
    const created = await findOrCreateCustomerWithRetry({ name: sessionData.name, email: sessionData.email, phone })
    if (!created){ await sendWithPresence(sock, jid, "No pude crear tu ficha de cliente. Intenta de nuevo o contacta al salón."); return }
    customerId = created.id
    sessionData.customerId = customerId
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
    await sendWithPresence(sock, jid, "No pude crear la reserva ahora. Nuestro equipo te contactará. ¿Quieres otro horario?")
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
⏱️ 60 minutos

¡Te esperamos!`
  await sendWithPresence(sock, jid, confirmMessage);
  clearSession(phone);
}

async function executeListAppointments(_params, _sessionData, _phone, sock, jid) {
  await sendWithPresence(sock, jid, VIEW_APPTS_MSG)
}
async function executeCancelAppointment(_params, _sessionData, _phone, sock, jid) {
  await sendWithPresence(sock, jid, CANCEL_MODIFY_MSG)
}

// ====== Bot/WhatsApp infra
let RECONNECT_SCHEDULED = false
let RECONNECT_ATTEMPTS = 0
const QUEUE=new Map()
function enqueue(key,job){
  const prev=QUEUE.get(key)||Promise.resolve()
  const next=prev.then(job,job).finally(()=>{ if (QUEUE.get(key)===next) QUEUE.delete(key) })
  QUEUE.set(key,next); return next
}

// === Rastreo de mensajes enviados por el bot para no confundirlos con intervención humana
const BOT_SENT = new Map() // phone -> Set<msgId>
function markBotSent(phone, id){
  if (!phone || !id) return
  let s = BOT_SENT.get(phone)
  if (!s){ s = new Set(); BOT_SENT.set(phone, s) }
  s.add(id)
  // prune
  if (s.size > 200){
    const it = s.values()
    for (let i=0;i<50;i++){ const v = it.next().value; if (v) s.delete(v) }
  }
}
function wasBotSent(phone, id){
  const s = BOT_SENT.get(phone)
  if (!s) return false
  if (s.has(id)){ s.delete(id); return true }
  return false
}

async function sendWithPresence(sock, jid, text){
  try{ await sock.sendPresenceUpdate("composing", jid) }catch{}
  await new Promise(r=>setTimeout(r, 800+Math.random()*1200))
  const msg = await sock.sendMessage(jid, { text })
  try{
    const phone = normalizePhoneES((jid||"").split("@")[0]||"") || (jid||"").split("@")[0]
    if (msg?.key?.id) markBotSent(phone, msg.key.id)
  }catch{}
  return msg
}

function isCancelIntent(text){
  const t = norm(text)
  const cancelWords = /\b(cancelar|anular|borrar|dar de baja)\b/
  const modifyWords = /\b(cambiar|modificar|editar|mover|reprogramar|reagendar)\b/
  return cancelWords.test(t) || modifyWords.test(t)
}

// ====== IA principal
function buildSystemPromptMain() {
  const nowEU = dayjs().tz(EURO_TZ);
  const torremolinos_services = servicesForSedeKeyRaw("torremolinos");
  const laluz_services = servicesForSedeKeyRaw("la_luz");
  return `Eres el asistente de WhatsApp para Gapink Nails. Devuelves SOLO JSON válido.
Fecha: ${nowEU.format("dddd DD/MM/YYYY HH:mm")} (Madrid)
Reglas: usa "salón"; tildes/ñ; coherencia con roster/slots; autoservicio cancelar/cambiar; acciones: propose_times|create_booking|list_appointments|cancel_appointment|choose_service|choose_staff|need_info|none; "ver mis citas" -> list_appointments; ASAP -> confirmar primer hueco.

SERVICIOS TORREMOLINOS:
${torremolinos_services.map(s => "- "+s.label+" (Clave: "+s.key+")").join("\n")}
SERVICIOS LA LUZ:
${laluz_services.map(s => "- "+s.label+" (Clave: "+s.key+")").join("\n")}`
}

// ====== Baileys + Bot
const { makeWASocket, useMultiFileAuthState, fetchLatestBaileysVersion, Browsers } = await (async function loadBaileys(){
  const require = createRequire(import.meta.url); let mod=null
  try{ mod=require("@whiskeysockets/baileys") }catch{}; if(!mod){ try{ mod=await import("@whiskeysockets/baileys") }catch{} }
  if(!mod) throw new Error("Baileys incompatible")
  const _makeWASocket = mod.makeWASocket || mod.default?.makeWASocket || (typeof mod.default==="function"?mod.default:undefined)
  const _useMultiFileAuthState = mod.useMultiFileAuthState || mod.default?.useMultiFileAuthState
  const _fetchLatestBaileysVersion = mod.fetchLatestBaileysVersion || mod.default?.fetchLatestBaileysVersion || (async()=>({version:[2,3000,0]}))
  const _Browsers = mod.Browsers || mod.default?.Browsers || { macOS:(n="Desktop")=>["MacOS",n,"121.0.0"] }
  return { makeWASocket:_makeWASocket, useMultiFileAuthState:_useMultiFileAuthState, fetchLatestBaileysVersion:_fetchLatestBaileysVersion, Browsers:_Browsers }
})()

async function startBot(){
  try{
    if(!fs.existsSync("auth_info")) fs.mkdirSync("auth_info",{recursive:true})
    const { state, saveCreds } = await useMultiFileAuthState("auth_info")
    const { version } = await fetchLatestBaileysVersion().catch(()=>({version:[2,3000,0]}))
    const sock = makeWASocket({ 
      logger:pino({level:"silent"}), 
      printQRInTerminal:false, 
      auth:state, 
      version, 
      browser:Browsers.macOS("Desktop"), 
      syncFullHistory:false 
    })
    globalThis.sock=sock

    sock.ev.on("connection.update", ({connection,qr})=>{
      if (qr){ 
        lastQR=qr; conectado=false; 
        try{ qrcodeTerminal.generate(qr,{small:true}) }catch{} 
      }
      if (connection==="open"){ 
        lastQR=null; conectado=true; RECONNECT_ATTEMPTS=0; RECONNECT_SCHEDULED=false; 
      }
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
      const phone = normalizePhoneES((jid||"").split("@")[0]||"") || (jid||"").split("@")[0]

      // Intervención manual SOLO si el mensaje fromMe es exactamente "."
      if (m.key.fromMe){
        if (wasBotSent(phone, m.key.id)) return
        const textFromMe = (m.message.conversation || m.message.extendedTextMessage?.text || m.message?.imageMessage?.caption || "").trim()
        if (textFromMe === "."){
          if (phone) setPause(phone, INTERVENTION_PAUSE_HOURS)
        }
        return
      }

      const textRaw = (m.message.conversation || m.message.extendedTextMessage?.text || m.message?.imageMessage?.caption || "").trim()
      if (!textRaw) return

      await enqueue(phone, async ()=>{
        try {
          let sessionData = loadSession(phone) || {
            greeted: false,
            sede: null,
            selectedServiceEnvKey: null,
            selectedServiceLabel: null,
            preferredStaffId: null,
            preferredStaffLabel: null,
            pendingDateTime: null,
            name: null,
            email: null,
            customerId: null,
            last_msg_id: null,
            lastStaffByIso: {},
            lastProposeUsedPreferred: false,
            stage: null,
            cancelList: null,
            serviceChoices: null,
            identityChoices: null,
            staffChoices: null,
            pendingCategory: null,
            lastStaffNamesById: null,
            __last_user_text: null,
            __last_intent: null,
            __phone: phone,
            switchTargetSalon: null,
            asapWanted: false,
            pauseUntil: null
          }

          // Pausa vigente → no contestamos
          if (isPaused(sessionData)) return
          
          if (sessionData.last_msg_id === m.key.id) return
          sessionData.last_msg_id = m.key.id
          sessionData.__last_user_text = textRaw

          // Bienvenida solo una vez (y cortamos este turno)
          if (!sessionData.greeted){
            sessionData.greeted = true
            saveSession(phone, sessionData)
            await sendWithPresence(sock, jid, WELCOME_MSG)
            return
          }

          // Autoservicio cancelar/modificar
          if (isCancelIntent(textRaw)){
            await sendWithPresence(sock, jid, CANCEL_MODIFY_MSG)
            return
          }

          // Enriquecer estado (IA + heurística)
          await ensureCoreFromText(sessionData, textRaw)
          saveSession(phone, sessionData)

          const lower = norm(textRaw)
          const numMatch = lower.match(/^(?:opcion|opción)?\s*([1-9]\d*)\b/)

          // Confirmación de cambio de salón
          if (sessionData.stage==="confirm_switch_salon"){
            if (isAffirmative(textRaw)){
              sessionData.sede = sessionData.switchTargetSalon
              sessionData.stage = null
              sessionData.switchTargetSalon = null
              saveSession(phone, sessionData)
              await executeProposeTime({}, sessionData, phone, sock, jid)
              return
            }
            if (isNegative(textRaw)){
              sessionData.stage = null
              sessionData.switchTargetSalon = null
              saveSession(phone, sessionData)
              await sendWithPresence(sock, jid, "Sin problema. Te muestro huecos con nuestro equipo del salón actual.")
              await executeProposeTime({}, sessionData, phone, sock, jid)
              return
            }
          }

          // Salón pendiente para listar servicios
          if (sessionData.stage==="awaiting_salon_for_services"){
            const sede = parseSede(textRaw)
            if (sede){
              sessionData.sede = sede
              sessionData.stage = null
              saveSession(phone, sessionData)
              await executeChooseService({ category: sessionData.pendingCategory || "unas", candidates: [] }, sessionData, phone, sock, jid, textRaw)
              return
            }
          }

          // Elección de staff
          if (numMatch && sessionData.stage==="awaiting_staff_choice" && Array.isArray(sessionData.staffChoices) && sessionData.staffChoices.length){
            const idx = Number(numMatch[1]) - 1
            const pick = sessionData.staffChoices[idx]
            if (pick){
              sessionData.preferredStaffId = pick.id
              sessionData.preferredStaffLabel = pick.name
              sessionData.stage = "awaiting_time"
              saveSession(phone, sessionData)
              await executeProposeTime({}, sessionData, phone, sock, jid)
              return
            }
          }

          // “con {nombre}”
          const staffAsk = extractStaffAsk(textRaw)
          if (staffAsk){
            if (!sessionData.sede){
              const eAny = findStaffAnySalon(staffAsk) || suggestClosestStaff(staffAsk, null)
              if (eAny?.baseCenter){ sessionData.sede = eAny.baseCenter }
            }
            if (sessionData.sede){
              let staff = findStaffByName(staffAsk, sessionData.sede) || suggestClosestStaff(staffAsk, sessionData.sede)
              if (staff){
                sessionData.preferredStaffId = staff.id
                sessionData.preferredStaffLabel = staff.labels?.[0] || staffAsk
                saveSession(phone, sessionData)
                await executeProposeTime({}, sessionData, phone, sock, jid)
                return
              }
              const elsewhere = findStaffElsewhere(staffAsk, sessionData.sede)
              if (elsewhere){
                const target = elsewhere.sedes.includes("torremolinos") ? "torremolinos" : "la_luz"
                sessionData.stage = "confirm_switch_salon"
                sessionData.switchTargetSalon = target
                saveSession(phone, sessionData)
                await sendWithPresence(sock, jid, `${staffAsk} trabaja en ${locationNice(target)}, no en ${locationNice(sessionData.sede)}. ¿Cambio el *salón* a ${locationNice(target)} y te paso horas? (sí/no)`)
                return
              }
            }
            await executeChooseStaff({ staffCandidates: [] }, sessionData, phone, sock, jid)
            return
          }

          // Elección de servicio
          if (numMatch && sessionData.stage==="awaiting_service_choice" && Array.isArray(sessionData.serviceChoices)){
            const idx = Number(numMatch[1]) - 1
            const pick = sessionData.serviceChoices[idx]
            if (pick && sessionData.sede){
              sessionData.selectedServiceLabel = pick.label
              sessionData.selectedServiceEnvKey = resolveEnvKeyFromLabelAndSede(pick.label, sessionData.sede)
              sessionData.stage = null
              saveSession(phone, sessionData)
              if (/quien|quién|recomiendas|profesional/i.test(textRaw) || isIndifferent(textRaw)){
                await executeChooseStaff({ staffCandidates: [] }, sessionData, phone, sock, jid)
              } else {
                await executeProposeTime({}, sessionData, phone, sock, jid)
              }
              return
            }
          }

          // Elección de horario
          if (numMatch && Array.isArray(sessionData.lastHours) && sessionData.lastHours.length && (!sessionData.stage || sessionData.stage==="awaiting_time")){
            const idx = Number(numMatch[1]) - 1
            const pick = sessionData.lastHours[idx]
            if (dayjs.isDayjs(pick)){
              const iso = pick.format("YYYY-MM-DDTHH:mm")
              const staffFromIso = sessionData?.lastStaffByIso?.[iso] || null
              sessionData.pendingDateTime = pick.tz(EURO_TZ).toISOString()
              if (staffFromIso){ sessionData.preferredStaffId = staffFromIso; sessionData.preferredStaffLabel = null }
              saveSession(phone, sessionData)
              await routeAIResult({ message:"Perfecto, confirmo tu cita ✨", action:"create_booking", session_updates:{}, action_params:{} }, sessionData, textRaw, m, phone, sock, jid)
              return
            }
          }

          // IA principal
          const aiObj = await (async ()=>{
            const systemPrompt = buildSystemPrompt()
            const recent = db.prepare(`SELECT user_message, ai_response FROM ai_conversations WHERE phone = ? ORDER BY timestamp DESC LIMIT 6`).all(phone);
            const conversationHistory = recent.reverse().map(msg => [
              { role: "user", content: msg.user_message },
              { role: "assistant", content: msg.ai_response }
            ]).flat();
            const messages = [
              ...conversationHistory,
              { role: "user", content: `MENSAJE DEL CLIENTE: "${textRaw}"\nESTADO:\n- Salón: ${sessionData?.sede || '—'}\n- Categoría: ${sessionData?.pendingCategory || '—'}\n- Servicio: ${sessionData?.selectedServiceLabel || '—'} (${sessionData?.selectedServiceEnvKey || 'no_key'})\n- Profesional: ${sessionData?.preferredStaffLabel || '—'}\n- Fecha/hora: ${sessionData?.pendingDateTime ? fmtES(parseToEU(sessionData.pendingDateTime)) : '—'}\nINSTRUCCIÓN: Devuelve SOLO JSON siguiendo las reglas.` }
            ];
            const aiText = await callAIWithRetries(messages, systemPrompt)
            if (!aiText) return null
            const cleaned = aiText.replace(/```json\s*/gi, "").replace(/```\s*/g, "").replace(/^[^{]*/, "").replace(/[^}]*$/, "").trim()
            try { return JSON.parse(cleaned) } catch { return null }
          })()

          if (aiObj?.action==="choose_staff"){
            await executeChooseStaff(aiObj.action_params, sessionData, phone, sock, jid)
            return
          }

          await routeAIResult(aiObj||{action:"none",message:null,session_updates:{},action_params:{}}, sessionData, textRaw, m, phone, sock, jid)

        } catch (error) {
          if (BOT_DEBUG) console.error("Handler error:", error)
          await sendWithPresence(sock, jid, "He tenido un pequeño contratiempo, pero seguimos 🙌. Dime el *salón* (Torremolinos o La Luz) y el *servicio* (por ejemplo “depilar cejas”), y te propongo horas al momento.")
        }
      })
    })
  }catch{ setTimeout(() => startBot().catch(console.error), 5000) }
}

// ====== Mini-web (rosa suave) + QR + crédito
const app = express()
const PORT = process.env.PORT || 8080

app.get("/", (_req,res)=>{
  res.send(`<!doctype html><meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <style>
    :root{
      --pink-bg:#fff1f6; --pink-accent:#ff69b4; --pink-soft:#ffcce0;
      --text:#1f2937; --muted:#6b7280; --card:#ffffff; --shadow:0 12px 40px rgba(255,105,180,.18); --radius:22px;
    }
    *{box-sizing:border-box}
    body{ margin:0; font-family:system-ui,-apple-system,Segoe UI,Roboto,Ubuntu,"Helvetica Neue",Arial;
      min-height:100vh; display:grid; place-items:center; background: radial-gradient(1200px 800px at 20% -10%, var(--pink-bg), #fff), #fff; color:var(--text);
    }
    .wrap{padding:24px; width:100%; max-width:860px}
    .card{ background:var(--card); border-radius:var(--radius); padding:36px 28px; box-shadow:var(--shadow); position:relative; overflow:hidden; }
    .brand{ display:flex; align-items:center; gap:12px; margin:0 0 12px 0; }
    .logo{ width:44px; height:44px; border-radius:12px; background: linear-gradient(135deg, var(--pink-accent), var(--pink-soft)); box-shadow: 0 6px 22px rgba(255,105,180,.35); }
    h1{font-size:28px; margin:0; letter-spacing:.2px}
    .status{ display:inline-flex; align-items:center; gap:10px; padding:10px 14px; border-radius:12px; font-weight:600; margin:8px 0 16px; background:#ffe7f0; color:#a81b66; border:1px solid #ffd3e3; }
    .status.success{ background:#eafaf0; color:#0f7b3b; border-color:#c8f1d8 }
    .status.error{ background:#fde8e8; color:#b91c1c; border-color:#fbd5d5 }
    .qr{ text-align:center; margin:18px 0 6px }
    .qr img{ width:300px; max-width:80%; border-radius:16px; box-shadow:0 10px 28px rgba(0,0,0,.12) }
    .mode{ margin-top:10px; font-size:14px; color:var(--muted) }
    .footer{ margin-top:20px; text-align:center; font-size:14px; color:var(--muted) }
    .footer a{ color:var(--pink-accent); text-decoration:none; font-weight:600; }
    .footer a:hover{ text-decoration:underline }
  </style>
  <div class="wrap">
    <div class="card">
      <div class="brand">
        <div class="logo"></div>
        <h1>Gapink Nails</h1>
      </div>
      <div class="status ${conectado ? 'success' : 'error'}">
        Estado WhatsApp: ${conectado ? "Conectado" : "Desconectado"}
      </div>
      ${!conectado&&lastQR?`<div class="qr"><img src="/qr.png" alt="QR de WhatsApp"></div>`:""}
      <div class="mode">${DRY_RUN ? "🧪 Simulación (no toca Square)" : "🚀 Producción"}</div>
    </div>
    <div class="footer">Hecho por <a href="https://gonzalog.co" target="_blank" rel="noopener">Gonzalo García Aranda</a></div>
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

console.log(`🩷 Gapink Nails Bot v30.3`)
app.listen(PORT, ()=>{ startBot().catch(console.error) })

process.on("uncaughtException", (e)=>{ console.error("💥 uncaughtException:", e?.stack||e?.message||e) })
process.on("unhandledRejection", (e)=>{ console.error("💥 unhandledRejection:", e) })
process.on("SIGTERM", ()=>{ process.exit(0) })
process.on("SIGINT", ()=>{ process.exit(0) })
