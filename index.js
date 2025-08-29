// index.js — Gapink Nails · v31.6.0
// Cambios clave:
// - Lock duro de staff del slot elegido (sin fallback a preferred).
// - Probe a Square en el mismo minuto si falta teamMemberId.
// - Parser de sede más robusto + no re-preguntar si ya está fijada.
// - Nota automática cuando no hay huecos con la preferida y se muestran alternativas.
// - Hash del menú para evitar desincronizaciones.

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
import isoWeek from "dayjs/plugin/isoWeek.js"
import "dayjs/locale/es.js"
import { webcrypto, createHash } from "crypto"
import { createRequire } from "module"
import { Client, Environment } from "square"

if (!globalThis.crypto) globalThis.crypto = webcrypto
dayjs.extend(utc); dayjs.extend(tz); dayjs.extend(isoWeek); dayjs.locale("es")
const EURO_TZ = "Europe/Madrid"

// ====== Config horario
const WORK_DAYS = [1,2,3,4,5]
const SLOT_MIN = 30
const OPEN = { start: 9, end: 20 }
const NOW_MIN_OFFSET_MIN = Number(process.env.BOT_NOW_OFFSET_MIN || 30)
const SEARCH_WINDOW_DAYS = Number(process.env.BOT_SEARCH_WINDOW_DAYS || 30)
const HOLIDAYS_EXTRA = (process.env.HOLIDAYS_EXTRA || "06/01,28/02,15/08,12/10,01/11,06/12,08/12,25/12")
  .split(",").map(s=>s.trim()).filter(Boolean)
const SHOW_TOP_N = Number(process.env.SHOW_TOP_N || 5)

// ====== Flags
const BOT_DEBUG = /^true$/i.test(process.env.BOT_DEBUG || "")
const DRY_RUN = /^true$/i.test(process.env.DRY_RUN || "")
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

// ====== IA
const AI_PROVIDER = (process.env.AI_PROVIDER || (process.env.DEEPSEEK_API_KEY? "deepseek" : process.env.OPENAI_API_KEY? "openai" : "none")).toLowerCase()
const DEEPSEEK_API_KEY = process.env.DEEPSEEK_API_KEY || ""
const DEEPSEEK_MODEL = process.env.DEEPSEEK_MODEL || "deepseek-chat"
const OPENAI_API_KEY = process.env.OPENAI_API_KEY || ""
const OPENAI_MODEL = process.env.OPENAI_MODEL || "gpt-4o-mini"
const AI_TIMEOUT_MS = Number(process.env.AI_TIMEOUT_MS || 15000)
const sleep = ms => new Promise(r=>setTimeout(r, ms))

async function aiChat(system, user, extraMsgs=[]){
  if (AI_PROVIDER==="none") return null
  const controller = new AbortController()
  const timeout = setTimeout(()=>controller.abort(), AI_TIMEOUT_MS)
  try{
    const messages = [
      system ? { role:"system", content: system } : null,
      ...extraMsgs,
      { role:"user", content: user }
    ].filter(Boolean)
    const url = AI_PROVIDER==="deepseek" ? "https://api.deepseek.com/chat/completions" : "https://api.openai.com/v1/chat/completions"
    const headers = { "Content-Type":"application/json", "Authorization":`Bearer ${AI_PROVIDER==="deepseek"?DEEPSEEK_API_KEY:OPENAI_API_KEY}` }
    const body = JSON.stringify({ model: AI_PROVIDER==="deepseek"?DEEPSEEK_MODEL:OPENAI_MODEL, messages, temperature:0.2, max_tokens:1000 })
    const resp = await fetch(url,{ method:"POST", headers, body, signal: controller.signal })
    clearTimeout(timeout)
    if (!resp.ok) return null
    const data = await resp.json()
    return data?.choices?.[0]?.message?.content || null
  }catch{ clearTimeout(timeout); return null }
}
function stripToJSON(text){
  if (!text) return null
  let s = text.trim().replace(/```json/gi,"```")
  if (s.startsWith("```")) s = s.slice(3)
  if (s.endsWith("```")) s = s.slice(0,-3)
  s = s.trim()
  const i = s.indexOf("{"), j = s.lastIndexOf("}")
  if (i>=0 && j>i) s = s.slice(i, j+1)
  try{ return JSON.parse(s) }catch{ return null }
}

// ====== Utils
const onlyDigits = s => String(s||"").replace(/\D+/g,"")
const rm = s => String(s||"").normalize("NFD").replace(/\p{Diacritic}/gu,"")
const norm = s => rm(s).toLowerCase().replace(/[+.,;:()/_-]/g," ").replace(/[^\p{Letter}\p{Number}\s]/gu," ").replace(/\s+/g," ").trim()
function stableKey(parts){ const raw=Object.values(parts).join("|"); return createHash("sha256").update(raw).digest("hex").slice(0,48) }
function titleCase(str){ return String(str||"").toLowerCase().replace(/\b([a-z])/g, (m)=>m.toUpperCase()) }
function cleanDisplayLabel(label){
  const s = String(label||"").replace(/^\s*(luz|la\s*luz)\s+/i,"").trim()
  return s
}
function normalizePhoneES(raw){
  const d=onlyDigits(raw); if(!d) return null
  if (raw.startsWith("+") && d.length>=8 && d.length<=15) return `+${d}`
  if (d.startsWith("34") && d.length===11) return `+${d}`
  if (d.length===9) return `+34${d}`
  if (d.startsWith("00")) return `+${d.slice(2)}`
  return `+${d}`
}
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
function fmtDay(d){
  const dias=["domingo","lunes","martes","miércoles","jueves","viernes","sábado"]
  const t=(dayjs.isDayjs(d)?d:dayjs(d)).tz(EURO_TZ)
  return `${dias[t.day()]} ${String(t.date()).padStart(2,"0")}/${String(t.month()+1).padStart(2,"0")}`
}
function fmtHour(d){ const t=(dayjs.isDayjs(d)?d:dayjs(d)).tz(EURO_TZ); return `${String(t.hour()).padStart(2,"0")}:${String(t.minute()).padStart(2,"0")}` }
function enumerateHours(list){ return list.map((d,i)=>({ index:i+1, iso:d.format("YYYY-MM-DDTHH:mm"), pretty:fmtES(d) })) }
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
`)
const insertAppt = db.prepare(`INSERT INTO appointments
(id,customer_name,customer_phone,customer_square_id,location_key,service_env_key,service_label,duration_min,start_iso,end_iso,staff_id,status,created_at,square_booking_id,square_error,retry_count)
VALUES (@id,@customer_name,@customer_phone,@customer_square_id,@location_key,@service_env_key,@service_label,@duration_min,@start_iso,@end_iso,@staff_id,@status,@created_at,@square_booking_id,@square_error,@retry_count)`)

function saveSession(phone,s){
  const c={...s}
  c.lastHours_ms = Array.isArray(s.lastHours)? s.lastHours.map(d=>dayjs.isDayjs(d)?d.valueOf():null).filter(Boolean):[]
  c.pendingDateTime_ms = s.pendingDateTime? (dayjs.isDayjs(s.pendingDateTime)? s.pendingDateTime.valueOf() : dayjs(s.pendingDateTime).valueOf()) : null
  delete c.lastHours; delete c.pendingDateTime
  const j=JSON.stringify(c)
  const up=db.prepare(`UPDATE sessions SET data_json=@j, updated_at=@u WHERE phone=@p`).run({j,u:new Date().toISOString(),p:phone})
  if (up.changes===0) db.prepare(`INSERT INTO sessions (phone,data_json,updated_at) VALUES (@p,@j,@u)`).run({p:phone,j,u:new Date().toISOString()})
}
function loadSession(phone){
  const row = db.prepare(`SELECT data_json FROM sessions WHERE phone=@phone`).get({phone})
  if (!row?.data_json) return null
  const s = JSON.parse(row.data_json)
  if (Array.isArray(s.lastHours_ms)) s.lastHours = s.lastHours_ms.map(ms=>dayjs.tz(ms,EURO_TZ))
  if (s.pendingDateTime_ms) s.pendingDateTime = dayjs.tz(s.pendingDateTime_ms,EURO_TZ)
  return s
}
function clearSession(phone){ db.prepare(`DELETE FROM sessions WHERE phone=@phone`).run({phone}) }

// ====== Empleadas (global)
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
    const parts = String(v||"").split("|").map(s=>s.trim())
    const id = parts[0]
    if (!id) continue
    const bookTag = (parts[1]||"BOOKABLE").toUpperCase()
    const bookable = ["BOOKABLE","TRUE","YES","1"].includes(bookTag)
    const labels = deriveLabelsFromEnvKey(k)
    out.push({ envKey:k, id, bookable, labels })
  }
  return out
}
let EMPLOYEES = parseEmployees()
function staffLabelFromId(id){
  const e = EMPLOYEES.find(x=>x.id===id)
  return e?.labels?.[0] ? titleCase(e.labels[0]) : (id ? `Prof. ${String(id).slice(-4)}` : null)
}

// ====== Aliases staff
const NAME_ALIASES = [
  ["patri","patricia"],["patricia","patri"],
  ["cristi","cristina","cristy"],
  ["rocio chica","rociochica","rocio  chica","rocio c","rocio chica"],["rocio","rosio"],
  ["carmen belen","carmen","belen"],["tania","tani"],["johana","joana","yohana"],["ganna","gana"],
  ["ginna","gina"],["chabely","chabeli","chabelí"],["elisabeth","elisabet","elis"],
  ["desi","desiree","desirée"],["daniela","dani"],["jamaica","jahmaica"],["edurne","edur"],
  ["sudemis","sude"],["maria","maría"],["anaira","an aira"],["thalia","thalía","talia","talía"]
]
function fuzzyStaffFromText(text){
  const tnorm = norm(text)
  if (/\b(con el equipo|me da igual|cualquiera|con quien sea|lo que haya)\b/i.test(tnorm)) return { anyTeam:true }
  const t = " " + tnorm + " "
  const m = t.match(/\scon\s+([a-zñáéíóú ]{2,})\b/i)
  let token = m ? norm(m[1]).trim() : null
  if (!token){
    const nm = t.match(/\b(patri|patricia|cristi|cristina|rocio chica|rocio|carmen belen|carmen|belen|ganna|maria|anaira|ginna|daniela|desi|jamaica|johana|edurne|sudemis|tania|chabely|elisabeth|thalia|thalía|talia|talía)\b/i)
    if (nm) token = norm(nm[0])
  }
  if (!token) return null
  for (const arr of NAME_ALIASES){ if (arr.some(a=>token.includes(a))) { token = arr[0]; break } }
  for (const e of EMPLOYEES){ for (const lbl of e.labels){ if (norm(lbl).includes(token)) return e } }
  return null
}
function staffRosterForPrompt(){
  return EMPLOYEES.map(e=>`• ID:${e.id} | Nombres:[${e.labels.join(", ")}] | Reservable:${true}`).join("\n")
}

// ====== Servicios
function servicesForSedeKeyRaw(sedeKey){
  const prefix = (sedeKey==="la_luz") ? "SQ_SVC_luz_" : "SQ_SVC_"
  const out=[]
  for (const [k,v] of Object.entries(process.env)){
    if (!k.startsWith(prefix)) continue
    const [id] = String(v||"").split("|"); if (!id) continue
    const raw = k.replace(prefix,"").replaceAll("_"," ")
    let label = titleCase(raw)
    out.push({ sedeKey, key:k, id, rawKey:k, label: cleanDisplayLabel(label), norm: norm(label) })
  }
  return out
}
function allServices(){ return [...servicesForSedeKeyRaw("torremolinos"), ...servicesForSedeKeyRaw("la_luz")] }
function resolveEnvKeyFromLabelAndSede(label, sedeKey){
  const list = servicesForSedeKeyRaw(sedeKey)
  return list.find(s=>s.label.toLowerCase()===String(label||"").toLowerCase())?.key || null
}
function serviceLabelFromEnvKey(envKey){
  if (!envKey) return null
  const all = allServices()
  return all.find(s=>s.key===envKey)?.label || null
}

// ====== Categorías
const CATS = {
  "uñas": (s,u)=> {
    const NEG = /\b(pesta|ceja|facial|labios|eyeliner|micro|blading|laser|endosphere|madero|masaje|vitamina|limpieza|tratamiento)\b/i
    if (NEG.test(s.label)) return false
    const POS = /\b(uñ|manicura|gel|acril|nivel|semiperman|press|tips|francés|frances|pedicura|pies)\b/i
    const pediUser = /\b(pedicur|pie|pies)\b/i.test(norm(u||""))
    const isPediLabel = /\b(pedicur|pie|pies)\b/i.test(norm(s.label))
    if (!pediUser && isPediLabel) return false
    return POS.test(s.label)
  },
  "depilación": (s,_u)=> /\b(depil|fotodepil|axilas|ingles|inglés|labio|fosas|nasales)\b/i.test(s.label),
  "micropigmentación": (s,_u)=> /\b(microblading|microshading|efecto polvo|aquarela|eyeliner|retoque|labios|cejas)\b/i.test(s.label),
  "faciales": (s,_u)=> /\b(limpieza|facial|dermapen|carbon|peel|vitamina|hidra|piedras|oro|acne|manchas|colageno|colágeno)\b/i.test(s.label),
  "pestañas": (s,_u)=> /\b(pestañ|pestanas|lifting|extensiones|relleno pesta)\b/i.test(s.label)
}
const CAT_ALIASES = { "unas":"uñas","unias":"uñas","unyas":"uñas","depilacion":"depilación","depilacion laser":"depilación","micro":"micropigmentación","micropigmentacion":"micropigmentación","facial":"faciales","pestanas":"pestañas" }
function parseCategory(text){
  const t = norm(text)
  if (/\buñ|manicura|pedicur|acril|gel|semi|tips|frances/i.test(t)) return "uñas"
  if (/\bdepil|fotodepil|axilas|ingles|labio|fosas/i.test(t)) return "depilación"
  if (/\bmicroblading|microshading|aquarela|eyeliner|retoque|efecto polvo|cejas\b/i.test(t)) return "micropigmentación"
  if (/\blimpieza|facial|dermapen|carbon|peel|vitamina|hidra\b/i.test(t)) return "faciales"
  if (/\bpestañ|pestanas|lifting|extensiones\b/i.test(t)) return "pestañas"
  for (const [k,v] of Object.entries(CAT_ALIASES)){ if (t.includes(k)) return v }
  return null
}
function listServicesByCategory(sedeKey, category, userMsg){
  const all = servicesForSedeKeyRaw(sedeKey)
  const fn = CATS[category]; if (!fn) return []
  const filtered = all.filter(s=>fn(s,userMsg))
  const seen=new Set(); const out=[]
  for (const s of filtered){
    const key = s.label.toLowerCase()
    if (seen.has(key)) continue
    seen.add(key); out.push({ label:s.label, key:s.key, id:s.id })
  }
  return out
}

// ====== Square helpers
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
async function searchCustomersByPhone(phone){
  try{
    const e164=normalizePhoneES(phone); if(!e164) return []
    const got = await square.customersApi.searchCustomers({ query:{ filter:{ phoneNumber:{ exact:e164 } } } })
    return got?.result?.customers || []
  }catch{ return [] }
}
async function getUniqueCustomerByPhoneOrPrompt(phone, session, sock, jid){
  const matches = await searchCustomersByPhone(phone)
  if (matches.length === 1) return { status:"single", customer:matches[0] }
  if (matches.length === 0){
    session.stage = "awaiting_identity"
    saveSession(phone, session)
    await sock.sendMessage(jid, { text: "Para terminar, no encuentro tu ficha por este número. Dime tu *nombre completo* y, si quieres, tu *email* para crearte 😊" })
    return { status:"need_new" }
  }
  const choices = matches.map((c,i)=>({ index:i+1, id:c.id, name:c?.givenName||"Sin nombre", email:c?.emailAddress||"—" }))
  session.identityChoices = choices
  session.stage = "awaiting_identity_pick"
  saveSession(phone, session)
  const lines = choices.map(ch => `${ch.index}) ${ch.name} ${ch.email!=="—" ? `(${ch.email})`:""}`).join("\n")
  await sock.sendMessage(jid, { text: `Para terminar, he encontrado varias fichas con tu número. ¿Cuál eres?\n\n${lines}\n\nResponde con el número.` })
  return { status:"need_pick" }
}
async function findOrCreateCustomerWithRetry({ name, email, phone }){
  for (let attempt = 1; attempt <= SQUARE_MAX_RETRIES; attempt++) {
    try{
      const e164=normalizePhoneES(phone); if(!e164) return null
      const got = await square.customersApi.searchCustomers({ query:{ filter:{ phoneNumber:{ exact:e164 } } } })
      const c=(got?.result?.customers||[])[0]; 
      if (c) return c
      const created = await square.customersApi.createCustomer({
        idempotencyKey:`cust_${Date.now()}_${Math.random().toString(36).slice(2,6)}`,
        givenName:name||undefined, emailAddress:email||undefined, phoneNumber:e164||undefined
      })
      const newCustomer = created?.result?.customer||null
      if (newCustomer) return newCustomer
    } catch(e) { if (attempt < SQUARE_MAX_RETRIES) await sleep(1000 * attempt) }
  }
  return null
}
async function createBookingWithRetry({ startEU, locationKey, envServiceKey, durationMin, customerId, teamMemberId }){
  if (DRY_RUN) return { success: true, booking: { id:`TEST_SIM_${Date.now()}`, __sim:true } }
  const sv = await getServiceIdAndVersion(envServiceKey)
  if (!sv?.id || !sv?.version) return { success: false, error: `Servicio no válido` }
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
      if (booking) return { success: true, booking }
    } catch(e) { lastError = e; if (attempt < SQUARE_MAX_RETRIES) await sleep(2000 * attempt) }
  }
  return { success: false, error: `No se pudo crear reserva: ${lastError?.message || 'Error'}`, lastError }
}

// ====== Disponibilidad
function partOfDayWindow(dateEU, part){
  let start=dateEU.clone().hour(OPEN.start).minute(0).second(0).millisecond(0)
  let end  =dateEU.clone().hour(OPEN.end).minute(0).second(0).millisecond(0)
  if (part==="mañana") end = dateEU.clone().hour(13).minute(0)
  if (part==="tarde") { start = dateEU.clone().hour(15).minute(0) }
  if (part==="noche") { start = dateEU.clone().hour(18).minute(0) }
  return { start, end }
}
function parseTemporalPreference(text){
  const t = norm(text)
  const now = dayjs().tz(EURO_TZ)
  const mapDia = { "lunes":1,"martes":2,"miercoles":3,"miércoles":3,"jueves":4,"viernes":5,"sabado":6,"sábado":6,"domingo":0 }
  let targetDay=null; for (const k of Object.keys(mapDia)){ if (t.includes(k)) { targetDay = mapDia[k]; break } }
  let when = null
  if (/\bhoy\b/.test(t)) when = now
  else if (/\bmanana\b/.test(t)) when = now.add(1,"day")
  else if (/\bpasado\b/.test(t)) when = now.add(2,"day")
  if (targetDay!=null){ let d = now.clone(); while (d.day() !== targetDay) d = d.add(1,"day"); when = d }
  let part = null
  if (/\bpor la manana\b/.test(t) || (/\bmanana\b/.test(t) && !when)) part="mañana"
  if (/\btarde\b/.test(t)) part="tarde"
  if (/\bnoche\b/.test(t)) part="noche"
  const nextWeek = /\b(pr[oó]xima\s+semana|semana\s+que\s+viene)\b/i.test(t)
  return { when, part, nextWeek }
}
async function searchAvailWindow({ locationKey, envServiceKey, startEU, endEU, limit=200, part=null }){
  const sv = await getServiceIdAndVersion(envServiceKey)
  if (!sv?.id) return []
  const body = {
    query:{ filter:{
      startAtRange:{ startAt: startEU.tz("UTC").toISOString(), endAt: endEU.tz("UTC").toISOString() },
      locationId: locationToId(locationKey),
      segmentFilters: [{ serviceVariationId: sv.id }]
    } }
  }
  let avail=[]
  try{
    const resp = await square.bookingsApi.searchAvailability(body)
    avail = resp?.result?.availabilities || []
  }catch{}
  const out=[]
  for (const a of avail){
    if (!a?.startAt) continue
    const d = dayjs(a.startAt).tz(EURO_TZ)
    if (!insideBusinessHours(d,60)) continue
    let tm = null
    const segs = Array.isArray(a.appointmentSegments) ? a.appointmentSegments
                 : Array.isArray(a.segments) ? a.segments
                 : []
    if (segs[0]?.teamMemberId) tm = segs[0].teamMemberId
    if (part){
      const { start, end } = partOfDayWindow(d, part)
      if (!(d.isSame(start,"day") && d.isAfter(start.subtract(1,"minute")) && d.isBefore(end.add(1,"minute")))) continue
    }
    out.push({ date:d, staffId: tm || null })
    if (out.length>=limit) break
  }
  return out
}

// ====== Conversación / IA
function parseSede(text){
  const t=norm(text)
  if (/\b(la\s*luz|luz)\b/.test(t)) return "la_luz"
  if (/\b(torre|torre-mo?lin|torremolinos|benyamina|beniamina|torre molin)\b/.test(t)) return "torremolinos"
  return null
}
function parseNameEmailFromText(txt){
  const emailMatch = String(txt||"").match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i)
  const email = emailMatch ? emailMatch[0] : null
  const name = String(txt||"").replace(email||"", "").replace(/(email|correo)[:\s]*/ig,"").trim()
  return { name: name || null, email }
}
function enumerateLines(slots, mapIsoToStaff){
  const hoursEnum = enumerateHours(slots.map(s=>s.date))
  const lines = hoursEnum.map(h => {
    const sid = mapIsoToStaff[h.iso] || null
    const tag = sid ? ` — ${staffLabelFromId(sid)}` : ""
    return `${h.index}) ${h.pretty}${tag}`
  }).join("\n")
  return { lines, hoursEnum }
}
function buildGreeting(){
  return `¡Hola! Soy el asistente de Gapink Nails 💅
Para reservar, dime *salón* (Torremolinos o La Luz) y *categoría* (Uñas / Depilación / Micropigmentación / Faciales / Pestañas).
Ej.: “depilación en Torremolinos con Patri el viernes por la tarde”.
También puedo mostrarte *horarios* (“esta semana”, “próxima semana con Cristina”).`
}
function buildSystemPrompt(session){
  const nowEU = dayjs().tz(EURO_TZ)
  const tsv = servicesForSedeKeyRaw("torremolinos")
  const luz = servicesForSedeKeyRaw("la_luz")
  return `Eres el asistente de WhatsApp de Gapink Nails. Devuelves SOLO JSON válido.
Ahora: ${nowEU.format("dddd DD/MM/YYYY HH:mm")} (Madrid)
Salones: Torremolinos (${ADDRESS_TORRE}) y Málaga – La Luz (${ADDRESS_LUZ})

REGLAS:
- Si el cliente escribe números, no intervengas.
- Identifica *categoría* y *salón* antes de listar.
- "horario/esta semana/próxima semana" => {action:"weekly_schedule", next_week:boolean, staff_name?:string}
- "viernes tarde..." => {action:"propose_times", date_hint, part_of_day}
- Acciones: set_salon|set_sede|set_category|set_staff|choose_service_label|propose_times|weekly_schedule|create_booking|list_appointments|cancel_appointment|none.

Servicios TORREMOLINOS:
${tsv.map(s => `- ${s.label} (Clave: ${s.key})`).join("\n")}
Servicios LA LUZ:
${luz.map(s => `- ${s.label} (Clave: ${s.key})`).join("\n")}

FORMATO:
{"message":"...","action":"...","params":{...}}`
}
async function aiInterpret(textRaw, session){
  if (AI_PROVIDER==="none") return null
  const sys = buildSystemPrompt(session)
  const ctx = `Estado:
- Salón: ${session.sede||"—"}
- Categoría: ${session.category||"—"}
- Servicio: ${session.selectedServiceLabel||"—"}
- Profesional: ${session.preferredStaffLabel||"—"}`
  const out = await aiChat(sys, `Mensaje: "${textRaw}"\n${ctx}\nDevuelve SOLO JSON.`)
  return stripToJSON(out)
}

// ====== Hash de menú
function computeMenuHash(options){
  const payload = options.map(o=>`${o.iso}|${o.staffId||"_"}`).join("~")
  return createHash("sha256").update(payload).digest("hex").slice(0,16)
}

// ====== Proponer horas
async function proposeTimes(session, phone, sock, jid, opts={}){
  const nowEU = dayjs().tz(EURO_TZ);
  const baseFrom = nextOpeningFrom(nowEU.add(NOW_MIN_OFFSET_MIN, "minute"))
  const days = SEARCH_WINDOW_DAYS

  let when=null, part=null
  if (opts.date_hint || opts.part_of_day){
    const p = parseTemporalPreference(`${opts.date_hint||""} ${opts.part_of_day||""}`)
    when = p.when; part = p.part || opts.part_of_day || null
  } else if (opts.text){
    const p = parseTemporalPreference(opts.text)
    when = p.when; part = p.part
  }

  let startEU = when ? when.clone().hour(OPEN.start).minute(0) : baseFrom.clone()
  let endEU   = when ? when.clone().hour(OPEN.end).minute(0)   : baseFrom.clone().add(days,"day")

  if (!session.sede || !session.selectedServiceEnvKey){
    if (!session.sede && !session._askedSedeOnce){
      session._askedSedeOnce = true
      saveSession(phone, session)
      await sock.sendMessage(jid, { text: "¿En qué *salón* te viene mejor? *Torremolinos* o *La Luz*." })
    } else if (!session.selectedServiceEnvKey){
      await sock.sendMessage(jid, { text: "Dime el *servicio* para pasarte horas 💖" })
    }
    return
  }

  const rawSlots = await searchAvailWindow({
    locationKey: session.sede,
    envServiceKey: session.selectedServiceEnvKey,
    startEU, endEU, limit: 200, part
  })

  let slots = rawSlots
  let usedPreferred = false
  if (session.preferredStaffId){
    const onlyPreferred = rawSlots.filter(s => s.staffId === session.preferredStaffId)
    if (onlyPreferred.length){ slots = onlyPreferred; usedPreferred = true }
    else if (rawSlots.length){ slots = rawSlots; usedPreferred = false }
  }

  // Fallback próxima semana
  if (!slots.length){
    const startNext = startEU.clone().add(7, "day")
    const endNext   = endEU.clone().add(7, "day")
    const rawNext = await searchAvailWindow({
      locationKey: session.sede,
      envServiceKey: session.selectedServiceEnvKey,
      startEU: startNext, endEU: endNext, limit: 200, part
    })
    let nextSlots = rawNext
    let nextUsedPreferred = false
    if (session.preferredStaffId){
      const onlyPrefNext = rawNext.filter(s => s.staffId === session.preferredStaffId)
      if (onlyPrefNext.length){ nextSlots = onlyPrefNext; nextUsedPreferred = true }
    }
    if (nextSlots.length){
      const shown = nextSlots.slice(0, SHOW_TOP_N)
      const mapN={}; for (const s of shown) mapN[s.date.format("YYYY-MM-DDTHH:mm")] = s.staffId || null
      const { lines } = enumerateLines(shown, mapN)

      const options = shown.map(s=>({ iso:s.date.format("YYYY-MM-DDTHH:mm"), staffId:s.staffId||null }))
      session.lastHours = shown.map(s => s.date)
      session.lastStaffByIso = mapN
      session.lastOptions = options
      session.lastMenuHash = computeMenuHash(options)
      session.lastMenuAt = Date.now()
      session.lastProposeUsedPreferred = nextUsedPreferred
      session.stage = "awaiting_time"
      saveSession(phone, session)

      const note = (session.preferredStaffLabel && !nextUsedPreferred)
        ? `\nNota: no veo huecos con *${session.preferredStaffLabel}* en ese rango; te muestro alternativas del *equipo* la próxima semana.`
        : ""
      await sock.sendMessage(jid, { text: `No había huecos en los próximos ${days} días. *La próxima semana* sí hay (primeras ${SHOW_TOP_N}):\n${lines}\n${note}\n\nResponde con el número.` })
      return
    }
  }

  if (!slots.length){
    await sock.sendMessage(jid, { text: when
      ? `No veo huecos para ese día${part?` por la ${part}`:""}. ¿Otra fecha o franja?`
      : `No encuentro huecos en los próximos ${days} días. ¿Otra fecha/franja (ej. “viernes por la tarde”)?`
    })
    return
  }

  const shown = slots.slice(0, SHOW_TOP_N)
  const map = {}; for (const s of shown) map[s.date.format("YYYY-MM-DDTHH:mm")] = s.staffId || null
  const { lines } = enumerateLines(shown, map)

  const options = shown.map(s=>({ iso:s.date.format("YYYY-MM-DDTHH:mm"), staffId:s.staffId||null }))
  session.lastHours = shown.map(s => s.date)
  session.lastStaffByIso = map
  session.lastOptions = options
  session.lastMenuHash = computeMenuHash(options)
  session.lastMenuAt = Date.now()
  session.lastProposeUsedPreferred = usedPreferred
  session.stage = "awaiting_time"
  saveSession(phone, session)

  const header = usedPreferred
    ? `Horarios disponibles con ${session.preferredStaffLabel || "tu profesional"} (primeras ${SHOW_TOP_N}):`
    : `Horarios disponibles (equipo) — primeras ${SHOW_TOP_N}:${session.preferredStaffLabel ? `\nNota: no veo huecos con *${session.preferredStaffLabel}* en este rango; te muestro alternativas del *equipo*.`:""}`
  await sock.sendMessage(jid, { text: `${header}\n${lines}\n\nResponde con el número.` })
}

// ====== Horario semanal
function nextMondayEU(base){ return base.clone().add(1,"week").isoWeekday(1).hour(OPEN.start).minute(0).second(0).millisecond(0) }
async function weeklySchedule(session, phone, sock, jid, opts={}){
  if (!session.sede){
    if (!session._askedSedeOnce){
      session._askedSedeOnce = true; saveSession(phone, session)
      await sock.sendMessage(jid,{ text:"¿En qué *salón* te viene mejor? *Torremolinos* o *La Luz*." })
    }
    return
  }
  if (!session.selectedServiceEnvKey){
    await sock.sendMessage(jid,{ text:"Dime el *servicio* y te muestro el horario semanal." })
    return
  }
  const nowEU = dayjs().tz(EURO_TZ)
  let startEU = nextOpeningFrom(nowEU.add(NOW_MIN_OFFSET_MIN,"minute"))
  if (opts.nextWeek){ startEU = nextMondayEU(nowEU) }
  const endEU = startEU.clone().add(7,"day").hour(OPEN.end).minute(0)
  const rawSlots = await searchAvailWindow({ locationKey: session.sede, envServiceKey: session.selectedServiceEnvKey, startEU, endEU, limit: 500 })
  if (!rawSlots.length){ await sock.sendMessage(jid,{ text:`No encuentro huecos en ese rango. ¿Quieres que mire otra semana?` }); return }
  let staffIdFilter = null
  if (opts.staffName){
    const fz = fuzzyStaffFromText("con " + opts.staffName)
    if (fz && !fz.anyTeam) staffIdFilter = fz.id
  } else if (opts.usePreferred && session.preferredStaffId){
    staffIdFilter = session.preferredStaffId
  }
  let slots = rawSlots
  if (staffIdFilter){
    const own = rawSlots.filter(s => s.staffId === staffIdFilter)
    if (own.length){ slots = own }
    else { await sock.sendMessage(jid,{ text:`No veo huecos con ${staffLabelFromId(staffIdFilter)} en ese rango. Te muestro el *equipo*:` }); slots = rawSlots }
  }
  slots.sort((a,b)=>a.date.valueOf()-b.date.valueOf())
  const limited = slots.slice(0, SHOW_TOP_N)
  const byDay = new Map()
  for (const s of limited){
    const key = s.date.format("YYYY-MM-DD")
    if (!byDay.has(key)) byDay.set(key, [])
    byDay.get(key).push(s)
  }
  const dayKeys = Array.from(byDay.keys()).sort()
  const lines = []
  const enumerated = []
  let idx=1
  for (const dk of dayKeys){
    const list = byDay.get(dk).sort((a,b)=>a.date.valueOf()-b.date.valueOf())
    lines.push(`\n📅 ${fmtDay(list[0].date)}`)
    for (const s of list){
      const iso = s.date.format("YYYY-MM-DDTHH:mm")
      const tag = s.staffId ? ` — ${staffLabelFromId(s.staffId)}` : ""
      lines.push(`${idx}) ${fmtHour(s.date)}${tag}`)
      enumerated.push({ index:idx, date:s.date, iso, staffId:s.staffId||null })
      idx++
    }
  }
  const map={}; const arr=[]; const options=[]
  for (const e of enumerated){ map[e.iso]=e.staffId; arr.push(e.date); options.push({iso:e.iso, staffId:e.staffId||null}) }
  session.lastHours = arr
  session.lastStaffByIso = map
  session.lastOptions = options
  session.lastMenuHash = computeMenuHash(options)
  session.lastMenuAt = Date.now()
  session.lastProposeUsedPreferred = !!staffIdFilter
  session.stage = "awaiting_time"
  saveSession(phone, session)
  const header = `🗓️ Horario ${opts.nextWeek? "próxima semana":"7 días"} — ${locationNice(session.sede)}\n${serviceLabelFromEnvKey(session.selectedServiceEnvKey) || session.selectedServiceLabel || "Servicio"}${staffIdFilter? ` · con ${staffLabelFromId(staffIdFilter)}`:""}\n`
  await sock.sendMessage(jid,{ text: `${header}${lines.join("\n")}\n\nResponde con el *número* para reservar ese hueco.` })
}

// ====== Crear reserva (lock estricto del staff del slot)
async function executeCreateBooking(session, phone, sock, jid){
  if (!session.sede){ await sock.sendMessage(jid,{text:"Falta el *salón*"}); return }
  if (!session.selectedServiceEnvKey){ await sock.sendMessage(jid,{text:"Falta el *servicio*"}); return }
  if (!session.pendingDateTime){ await sock.sendMessage(jid,{text:"Falta la *fecha y hora*"}); return }

  const startEU = parseToEU(session.pendingDateTime)
  if (!insideBusinessHours(startEU, 60)){ await sock.sendMessage(jid,{text:"Esa hora está fuera de horario (L–V 09:00–20:00)"}); return }

  const iso = startEU.format("YYYY-MM-DDTHH:mm")
  let staffId = session.lockedStaffIdForPending || session.pendingSlotStaffId || null

  // Si por lo que sea no tenemos staff, hacemos probe exacto ±5 min
  if (!staffId){
    const probe = await searchAvailWindow({
      locationKey: session.sede,
      envServiceKey: session.selectedServiceEnvKey,
      startEU: startEU.clone().subtract(5,"minute"),
      endEU: startEU.clone().add(5,"minute"),
      limit: 5
    })
    const match = probe.find(x => x.date.isSame(startEU, "minute"))
    if (match?.staffId) staffId = match.staffId
  }

  // IMPORTANTE: NO usar preferredStaffId como fallback (evita “cambio” indeseado)
  if (!staffId){
    await sock.sendMessage(jid,{text:"Ese hueco ya no está disponible. Te paso opciones actualizadas:"})
    await proposeTimes(session, phone, sock, jid, { text:"" })
    return
  }

  // Identidad
  let customerId = session.identityResolvedCustomerId || null
  if (!customerId){
    const { status, customer } = await getUniqueCustomerByPhoneOrPrompt(phone, session, sock, jid) || {}
    if (status === "need_new" || status === "need_pick") return
    customerId = customer?.id || null
  }

  const result = await createBookingWithRetry({
    startEU, locationKey: session.sede, envServiceKey: session.selectedServiceEnvKey,
    durationMin: 60, customerId, teamMemberId: staffId
  })
  if (!result.success) {
    await sock.sendMessage(jid,{text:"No pude crear la reserva ahora. ¿Quieres que te proponga otro horario?"})
    return
  }

  const aptId = `apt_${Math.random().toString(36).slice(2,8)}${Date.now().toString(36).slice(-4)}`
  insertAppt.run({
    id: aptId, customer_name: session?.name || null, customer_phone: phone,
    customer_square_id: customerId, location_key: session.sede, service_env_key: session.selectedServiceEnvKey,
    service_label: serviceLabelFromEnvKey(session.selectedServiceEnvKey) || session.selectedServiceLabel || "Servicio",
    duration_min: 60, start_iso: startEU.tz("UTC").toISOString(), end_iso: startEU.clone().add(60, "minute").tz("UTC").toISOString(),
    staff_id: staffId, status: "confirmed", created_at: new Date().toISOString(),
    square_booking_id: result.booking.id, square_error: null, retry_count: 0
  })

  const staffName = staffLabelFromId(staffId) || "nuestro equipo";
  const address = session.sede === "la_luz" ? ADDRESS_LUZ : ADDRESS_TORRE;
  const svcLabel = serviceLabelFromEnvKey(session.selectedServiceEnvKey) || session.selectedServiceLabel || "Servicio"
  const confirmMessage = `🎉 ¡Reserva confirmada!

📍 ${locationNice(session.sede)}
${address}

🧾 ${svcLabel}
👩‍💼 ${staffName}
📅 ${fmtES(startEU)}

¡Te esperamos!`
  await sock.sendMessage(jid, { text: confirmMessage })

  clearSession(phone);
}

// ====== Mini-web + Baileys
const app=express()
const PORT=process.env.PORT||8080
let lastQR=null, conectado=false
app.get("/", (_req,res)=>{
  res.send(`<!doctype html><meta charset="utf-8"><style>
  body{font-family:system-ui;display:grid;place-items:center;min-height:100vh;background:#f8f9fa}
  .card{max-width:720px;padding:32px;border-radius:20px;box-shadow:0 8px 32px rgba(0,0,0,.1);background:white}
  .status{padding:12px;border-radius:8px;margin:8px 0}
  .success{background:#d4edda;color:#155724}
  .error{background:#f8d7da;color:#721c24}
  .warning{background:#fff3cd;color:#856404}
  </style><div class="card">
  <h1>🩷 Gapink Nails Bot v31.6.0 — Top ${SHOW_TOP_N}</h1>
  <div class="status ${conectado ? 'success' : 'error'}">WhatsApp: ${conectado ? "✅ Conectado" : "❌ Desconectado"}</div>
  ${!conectado&&lastQR?`<div style="text-align:center;margin:20px 0"><img src="/qr.png" width="300" style="border-radius:8px"></div>`:""}
  <div class="status warning">Modo: ${DRY_RUN ? "🧪 Simulación" : "🚀 Producción"} | IA: ${AI_PROVIDER.toUpperCase()}</div>
  <p>Staff global. Square ya filtra por salón. Lock estricto del staff del slot elegido.</p>
  </div>`)
})
app.get("/qr.png", async (_req,res)=>{
  if(!lastQR) return res.status(404).send("No QR")
  const png = await qrcode.toBuffer(lastQR, { type:"png", width:512, margin:1 })
  res.set("Content-Type","image/png").send(png)
})

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
      if (connection==="open"){ lastQR=null; conectado=true; }
      if (connection==="close"){ conectado=false; setTimeout(()=>{ startBot().catch(console.error) }, 3000) }
    })
    sock.ev.on("creds.update", saveCreds)

    sock.ev.on("messages.upsert", async ({messages})=>{
      const m=messages?.[0]; if (!m?.message) return
      const jid = m.key.remoteJid
      const isFromMe = !!m.key.fromMe
      const phone = normalizePhoneES((jid||"").split("@")[0]||"") || (jid||"").split("@")[0]
      const textRaw = (m.message.conversation || m.message.extendedTextMessage?.text || m.message?.imageMessage?.caption || "").trim()
      if (!textRaw) return

      // Cola por teléfono
      if (!globalThis.__q) globalThis.__q = new Map()
      const QUEUE = globalThis.__q
      const prev=QUEUE.get(phone)||Promise.resolve()
      const job=prev.then(async ()=>{
        try{
          let session = loadSession(phone) || {
            greeted:false, sede:null, _askedSedeOnce:false, category:null,
            selectedServiceEnvKey:null, selectedServiceLabel:null,
            preferredStaffId:null, preferredStaffLabel:null,
            pendingDateTime:null, lastHours:null, lastStaffByIso:{},
            lastOptions:[], lastMenuHash:null, lastMenuAt:null,
            lastProposeUsedPreferred:false, stage:null,
            identityChoices:null, identityResolvedCustomerId:null,
            name:null, email:null
          }
          if (isFromMe) { saveSession(phone, session); return }

          // Saludo primera vez
          if (!session.greeted){
            await sock.sendMessage(jid,{ text: buildGreeting() })
            session.greeted = true
            saveSession(phone, session)
          }

          const t = norm(textRaw)
          const numMatch = t.match(/^\s*([1-9]\d*)\b/)
          const sedeMention = parseSede(textRaw)
          const catMention = parseCategory(textRaw)
          const temporal = parseTemporalPreference(textRaw)

          // Setear sede si la menciona (y no re-preguntar)
          if (sedeMention && !session.sede){ session.sede = sedeMention; saveSession(phone, session) }
          if (catMention && !session.category){ session.category = catMention; saveSession(phone, session) }

          // Equipo explícito
          if (/\b(con el equipo|me da igual|cualquiera|con quien sea|lo que haya)\b/i.test(t)){
            session.preferredStaffId = null
            session.preferredStaffLabel = null
            saveSession(phone, session)
            if (session.sede && session.selectedServiceEnvKey){
              await proposeTimes(session, phone, sock, jid, { text:textRaw })
            } else {
              const faltan=[]; if(!session.sede) faltan.push("salón"); if(!session.category) faltan.push("categoría"); if(!session.selectedServiceEnvKey) faltan.push("servicio")
              await sock.sendMessage(jid,{text:`Perfecto, te propongo huecos del equipo en cuanto me digas ${faltan.join(", ")}.`})
            }
            return
          }

          // ====== NÚMEROS ======
          if (session.stage==="awaiting_identity_pick" && numMatch){
            const n = Number(numMatch[1])
            const choice = (session.identityChoices||[]).find(c=>c.index===n)
            if (!choice){ await sock.sendMessage(jid,{text:"No encontré esa opción. Responde con el número de tu ficha."}); return }
            session.identityResolvedCustomerId = choice.id
            session.stage = null
            saveSession(phone, session)
            await sock.sendMessage(jid,{text:"¡Gracias! Finalizo tu reserva…"})
            await executeCreateBooking(session, phone, sock, jid)
            return
          }

          if (session.stage==="awaiting_identity"){
            const {name,email} = parseNameEmailFromText(textRaw)
            if (!name && !email){ await sock.sendMessage(jid,{text:"Dime tu *nombre completo* y opcionalmente tu *email* 😊"}); return }
            if (name) session.name = name
            if (email) session.email = email
            const created = await findOrCreateCustomerWithRetry({ name: session.name, email: session.email, phone })
            if (!created){ await sock.sendMessage(jid,{text:"No pude crear tu ficha. ¿Puedes repetir nombre y (opcional) email?"}); return }
            session.identityResolvedCustomerId = created.id
            session.stage = null
            saveSession(phone, session)
            await sock.sendMessage(jid,{text:"¡Gracias! Finalizo tu reserva…"})
            await executeCreateBooking(session, phone, sock, jid)
            return
          }

          if (session.stage==="awaiting_service_choice" && numMatch && Array.isArray(session.serviceChoices) && session.serviceChoices.length){
            const n = Number(numMatch[1])
            const choice = session.serviceChoices.find(it=>it.index===n)
            if (!choice){ await sock.sendMessage(jid,{text:"No encontré esa opción. Responde con el número de la lista."}); return }
            session.selectedServiceEnvKey = choice.key
            session.selectedServiceLabel = choice.label
            session.stage = null
            saveSession(phone, session)
            if (session.preferredStaffId){
              await proposeTimes(session, phone, sock, jid, { text:"" })
            } else {
              await sock.sendMessage(jid,{text:`Perfecto, ${choice.label} en ${locationNice(session.sede)}. ¿Lo quieres *con alguna profesional*? (por ejemplo “con Patri”). Si no, te paso huecos del equipo.`})
            }
            return
          }

          if ((!session.stage || session.stage==="awaiting_time") && numMatch && Array.isArray(session.lastHours) && session.lastHours.length){
            const n = Number(numMatch[1])
            const idx = n - 1
            if (idx<0 || idx>= (session.lastOptions||[]).length){
              await sock.sendMessage(jid,{text:"No encontré esa opción. Responde con un número válido."}); return
            }
            const currentHash = computeMenuHash(session.lastOptions||[])
            if (session.lastMenuHash && currentHash !== session.lastMenuHash){
              await sock.sendMessage(jid,{text:"El listado cambió. Te paso opciones actualizadas:"})
              await proposeTimes(session, phone, sock, jid, { text:"" })
              return
            }
            const pickDate = session.lastHours[idx]
            const pickOpt = (session.lastOptions||[])[idx] || null
            const iso = pickDate.format("YYYY-MM-DDTHH:mm")
            const staffFromIso = pickOpt?.staffId ?? session?.lastStaffByIso?.[iso] ?? null

            session.pendingDateTime = pickDate.tz(EURO_TZ).toISOString()
            session.lockedStaffIdForPending = staffFromIso || null
            session.pendingSlotIso = iso
            session.pendingSlotStaffId = staffFromIso || null

            if (staffFromIso){
              session.preferredStaffId = staffFromIso
              session.preferredStaffLabel = staffLabelFromId(staffFromIso)
            }
            saveSession(phone, session)
            await sock.sendMessage(jid,{text:"¡Perfecto! Creo la reserva…"})
            await executeCreateBooking(session, phone, sock, jid)
            return
          }
          // ====== FIN NÚMEROS ======

          // Staff fuzzy
          const fuzzy = fuzzyStaffFromText(textRaw)
          if (fuzzy){
            if (fuzzy.anyTeam){
              session.preferredStaffId = null; session.preferredStaffLabel = null; saveSession(phone, session)
              if (session.sede && session.selectedServiceEnvKey){ await proposeTimes(session, phone, sock, jid, { text:textRaw }) }
              else {
                const faltan=[]; if(!session.sede) faltan.push("salón"); if(!session.category) faltan.push("categoría"); if(!session.selectedServiceEnvKey) faltan.push("servicio")
                await sock.sendMessage(jid,{text:`Perfecto, te paso huecos del equipo en cuanto me digas ${faltan.join(", ")}.`})
              }
              return
            }
            session.preferredStaffId = fuzzy.id
            session.preferredStaffLabel = staffLabelFromId(fuzzy.id)
            saveSession(phone, session)
          }

          // “horario…”
          if (/\b(horario|agenda|est[áa]\s+semana|esta\s+semana|pr[oó]xima\s+semana|semana\s+que\s+viene|7\s+d[ií]as|siete\s+d[ií]as)\b/i.test(t)){
            if (!session.selectedServiceEnvKey){
              if (!session.category){ await sock.sendMessage(jid,{ text:"Antes del horario, dime *categoría* (Uñas / Depilación / Micropigmentación / Faciales / Pestañas)." }); return }
              if (!session.sede){ if(!session._askedSedeOnce){ session._askedSedeOnce=true; saveSession(phone, session) }; await sock.sendMessage(jid,{ text:"¿En qué *salón* te viene mejor? *Torremolinos* o *La Luz*." }); return }
              const itemsRaw = listServicesByCategory(session.sede, session.category, textRaw)
              if (!itemsRaw.length){ await sock.sendMessage(jid,{text:`No tengo servicios de *${session.category}* en ${locationNice(session.sede)}.`}); return }
              const list = itemsRaw.slice(0,22).map((s,i)=>({ index:i+1, key:s.key, label:s.label }))
              session.serviceChoices = list
              session.stage = "awaiting_service_choice"
              saveSession(phone, session)
              const lines = list.map(it=> `${it.index}) ${it.label}`).join("\n")
              await sock.sendMessage(jid,{text:`Elige el *servicio* para mostrarte el horario semanal en ${locationNice(session.sede)}:\n\n${lines}\n\nResponde con el número.`})
              return
            }
            await weeklySchedule(session, phone, sock, jid, {
              nextWeek: temporal.nextWeek,
              staffName: null,
              usePreferred: true
            })
            return
          }

          // Si ya hay salón+servicio y menciona día/franja → proponer
          if (session.sede && session.selectedServiceEnvKey && /\botro dia\b|\botro día\b|\bhoy\b|\bmanana\b|\bpasado\b|\blunes\b|\bmartes\b|\bmiercoles\b|\bjueves\b|\bviernes\b|\btarde\b|\bpor la manana\b|\bnoche\b/i.test(t)){
            await proposeTimes(session, phone, sock, jid, { text:textRaw })
            return
          }

          // IA genérica
          const aiObj = await aiInterpret(textRaw, session)
          if (aiObj && typeof aiObj==="object"){
            const action = aiObj.action
            const p = aiObj.params || {}

            if ((action==="set_salon" || action==="set_sede") && p.sede){
              const lk = parseSede(String(p.sede))
              if (lk){ session.sede = lk; saveSession(phone, session) }
              if (!session.category){ await sock.sendMessage(jid,{text:"¿Qué *categoría* necesitas? *Uñas*, *Depilación*, *Micropigmentación*, *Faciales* o *Pestañas*."}); return }
            }
            if (action==="set_category" && p.category){
              const cm = parseCategory(String(p.category))
              if (cm){ session.category = cm; saveSession(phone, session) }
              if (!session.sede){ if(!session._askedSedeOnce){ session._askedSedeOnce=true; saveSession(phone, session) } ; await sock.sendMessage(jid,{text:"¿En qué *salón* te viene mejor? *Torremolinos* o *La Luz*."}); return }
            }
            if (action==="choose_service_label" && p.label && session.sede){
              const ek = resolveEnvKeyFromLabelAndSede(p.label, session.sede)
              if (ek){
                session.selectedServiceEnvKey = ek
                session.selectedServiceLabel = p.label
                session.stage=null; saveSession(phone, session)
                await sock.sendMessage(jid,{text:`Perfecto, ${p.label} en ${locationNice(session.sede)}.`})
                await proposeTimes(session, phone, sock, jid, { text:textRaw })
                return
              }
            }
            if (action==="weekly_schedule"){
              if (!session.selectedServiceEnvKey){ await sock.sendMessage(jid,{ text:"Dime el *servicio* y te muestro el horario semanal." }); return }
              await weeklySchedule(session, phone, sock, jid, {
                nextWeek: !!p.next_week,
                staffName: p.staff_name || null,
                usePreferred: !p.staff_name
              })
              return
            }
            if (action==="propose_times"){
              if (!session.sede){ if(!session._askedSedeOnce){ session._askedSedeOnce=true; saveSession(phone, session) } ; await sock.sendMessage(jid,{text:"¿En qué *salón* te viene mejor? *Torremolinos* o *La Luz*."}); return }
              if (!session.category){ await sock.sendMessage(jid,{text:"¿Qué *categoría* necesitas? *Uñas*, *Depilación*, *Micropigmentación*, *Faciales* o *Pestañas*."}); return }
              if (!session.selectedServiceEnvKey){
                const itemsRaw = listServicesByCategory(session.sede, session.category, textRaw)
                if (!itemsRaw.length){ await sock.sendMessage(jid,{text:`No tengo servicios de *${session.category}* en ${locationNice(session.sede)}.`}); return }
                const list = itemsRaw.slice(0,22).map((s,i)=>({ index:i+1, key:s.key, label:s.label }))
                session.serviceChoices = list
                session.stage = "awaiting_service_choice"
                saveSession(phone, session)
                const lines = list.map(it=> `${it.index}) ${it.label}`).join("\n")
                await sock.sendMessage(jid,{text:`Opciones de *${session.category}* en ${locationNice(session.sede)}:\n\n${lines}\n\nResponde con el número.`})
                return
              }
              await proposeTimes(session, phone, sock, jid, { date_hint:p.date_hint, part_of_day:p.part_of_day, text:textRaw })
              return
            }
          }

          // Guía por faltantes
          if (!session.sede){ if(!session._askedSedeOnce){ session._askedSedeOnce=true; saveSession(phone, session) } ; await sock.sendMessage(jid,{text:"¿En qué *salón* te viene mejor? *Torremolinos* o *La Luz*."}); return }
          if (!session.category){ await sock.sendMessage(jid,{text:"¿Qué *categoría* necesitas? *Uñas*, *Depilación*, *Micropigmentación*, *Faciales* o *Pestañas*."}); return }
          if (!session.selectedServiceEnvKey){
            const itemsRaw = listServicesByCategory(session.sede, session.category, textRaw)
            if (!itemsRaw.length){ await sock.sendMessage(jid,{text:`No tengo servicios de *${session.category}* en ${locationNice(session.sede)}.`}); return }
            const list = itemsRaw.slice(0,22).map((s,i)=>({ index:i+1, key:s.key, label:s.label }))
            session.serviceChoices = list
            session.stage = "awaiting_service_choice"
            saveSession(phone, session)
            const lines = list.map(it=> `${it.index}) ${it.label}`).join("\n")
            await sock.sendMessage(jid,{text:`Opciones de *${session.category}* en ${locationNice(session.sede)}:\n\n${lines}\n\nResponde con el número.`})
            return
          }
          if (/\botro dia\b|\botro día\b|\bhoy\b|\bmanana\b|\bpasado\b|\blunes\b|\bmartes\b|\bmiercoles\b|\bjueves\b|\bviernes\b|\btarde\b|\bpor la manana\b|\bnoche\b/i.test(t)){
            await proposeTimes(session, phone, sock, jid, { text:textRaw })
            return
          }
          await sock.sendMessage(jid,{text:buildGreeting()})
        }catch(err){
          if (BOT_DEBUG) console.error(err)
          await sock.sendMessage(jid,{text:"Ups, error técnico. ¿Puedes repetirlo, porfa?"})
        }
      })
      QUEUE.set(phone, job.finally(()=>{ if (QUEUE.get(phone)===job) QUEUE.delete(phone) }))
    })
  }catch(e){ setTimeout(() => startBot().catch(console.error), 5000) }
}

// ====== Arranque
console.log(`🩷 Gapink Nails Bot v31.6.0 — Top ${SHOW_TOP_N} (L–V)`)
const appListen = app.listen(PORT, ()=>{ startBot().catch(console.error) })
process.on("uncaughtException", (e)=>{ console.error("💥 uncaughtException:", e?.stack||e?.message||e) })
process.on("unhandledRejection", (e)=>{ console.error("💥 unhandledRejection:", e) })
process.on("SIGTERM", ()=>{ try{ appListen.close(()=>process.exit(0)) }catch{ process.exit(0) } })
process.on("SIGINT", ()=>{ try{ appListen.close(()=>process.exit(0)) }catch{ process.exit(0) } })
