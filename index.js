// index.js — Gapink Nails · v27.6.0
// Cambios clave en 27.6.0:
// • Auto-selección de servicio: si el usuario dice algo como "acrílicas" y hay match claro, no mostramos lista.
// • La lista de servicios solo aparece si la IA/heurística NO está segura.
// • Nuevo stage handler: awaiting_service_choice → si responde con un número, se fija ese servicio y se proponen horas.
// • Mantiene separación de hilos, mini-web rosa glass, identidad por teléfono, y búsqueda de disponibilidad genérica.

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

// ====== Hilos / segmentación
const THREAD_TIMEOUT_MIN = Number(process.env.THREAD_TIMEOUT_MIN || 180) // 3h

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

// ====== IA
const AI_API_KEY = process.env.DEEPSEEK_API_KEY || ""
const AI_MODEL = process.env.AI_MODEL || "deepseek-chat"
const AI_MAX_RETRIES = Number(process.env.AI_MAX_RETRIES || 3)
const AI_TIMEOUT_MS = Number(process.env.AI_TIMEOUT_MS || 15000)
const sleep = ms => new Promise(r=>setTimeout(r, ms))

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
function fmtES(d){
  const dias=["domingo","lunes","martes","miércoles","jueves","viernes","sábado"]
  const t=(dayjs.isDayjs(d)?d:dayjs(d)).tz(EURO_TZ)
  return `${dias[t.day()]} ${String(t.date()).padStart(2,"0")}/${String(t.month()+1).padStart(2,"0")} ${String(t.hour()).padStart(2,"0")}:${String(t.minute()).padStart(2,"0")}`
}
function ceilToSlotEU(t){ const m=t.minute(), rem=m%SLOT_MIN; return rem===0 ? t.second(0).millisecond(0) : t.add(SLOT_MIN-rem,"minute").second(0).millisecond(0) }
function isHolidayEU(d){ const dd=String(d.date()).padStart(2,"0"), mm=String(d.month()+1).padStart(2,"0"); return HOLIDAYS_EXTRA.includes(`${dd}/${mm}`) }
function insideBusinessHours(d,dur){
  const t=d.clone()
  if (!WORK_DAYS.includes(t.day())) return false
  if (isHolidayEU(t)) return false
  const end=t.clone().add(dur,"minute")
  if (!t.isSame(end,"day")) return false
  const startMin=t.hour()*60+t.minute()
  const endMin=end.hour()*60+end.minute()
  const openMin=OPEN.start*60, closeMin=OPEN.end*60
  return startMin>=openMin && endMin<=closeMin
}
function nextOpeningFrom(d){
  let t=d.clone()
  const nowMin=t.hour()*60+t.minute(), openMin=OPEN.start*60, closeMin=OPEN.end*60
  if (nowMin < openMin) t=t.hour(OPEN.start).minute(0).second(0).millisecond(0)
  if (nowMin >= closeMin) t=t.add(1,"day").hour(OPEN.start).minute(0).second(0).millisecond(0)
  while (!WORK_DAYS.includes(t.day()) || isHolidayEU(t)) {
    t=t.add(1,"day").hour(OPEN.start).minute(0).second(0).millisecond(0)
  }
  return t
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
  thread_id TEXT,
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
try{ db.exec(`ALTER TABLE ai_conversations ADD COLUMN thread_id TEXT`) }catch{}
try{
  const tmp = JSON.parse(db.prepare(`SELECT data_json FROM sessions LIMIT 1`).get()?.data_json||"{}")
  if (!("thread_id" in tmp)) {
    const rows = db.prepare(`SELECT phone,data_json FROM sessions`).all()
    const upd = db.prepare(`UPDATE sessions SET data_json=@j, updated_at=@u WHERE phone=@p`).run.bind(db.prepare(`UPDATE sessions SET data_json=@j, updated_at=@u WHERE phone=@p`))
    for (const r of rows){
      const js = JSON.parse(r.data_json||"{}"); js.thread_id=null; js.thread_started_at=null; js.thread_last_at=null
      db.prepare(`UPDATE sessions SET data_json=@j, updated_at=@u WHERE phone=@p`).run({ j:JSON.stringify(js), u:new Date().toISOString(), p:r.phone })
    }
  }
}catch{}

// Prepared statements
const insertAppt = db.prepare(`INSERT INTO appointments
(id,customer_name,customer_phone,customer_square_id,location_key,service_env_key,service_label,duration_min,start_iso,end_iso,staff_id,status,created_at,square_booking_id,square_error,retry_count)
VALUES (@id,@customer_name,@customer_phone,@customer_square_id,@location_key,@service_env_key,@service_label,@duration_min,@start_iso,@end_iso,@staff_id,@status,@created_at,@square_booking_id,@square_error,@retry_count)`)

const insertAIConversation = db.prepare(`INSERT OR REPLACE INTO ai_conversations
(phone, message_id, user_message, ai_response, timestamp, session_data, ai_error, fallback_used, thread_id)
VALUES (@phone, @message_id, @user_message, @ai_response, @timestamp, @session_data, @ai_error, @fallback_used, @thread_id)`)

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
  const j=JSON.stringify(c)
  const up=db.prepare(`UPDATE sessions SET data_json=@j, updated_at=@u WHERE phone=@p`).run({j,u:new Date().toISOString(),p:phone})
  if (up.changes===0) db.prepare(`INSERT INTO sessions (phone,data_json,updated_at) VALUES (@p,@j,@u)`).run({p:phone,j,u:new Date().toISOString()})
}
function clearSession(phone){ db.prepare(`DELETE FROM sessions WHERE phone=@phone`).run({phone}) }

// ====== Empleadas / servicios
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
    out.push({ envKey:k, id, bookable, allow, labels })
  }
  return out
}
const EMPLOYEES = parseEmployees()
function staffLabelFromId(id){
  const e = EMPLOYEES.find(x=>x.id===id)
  return e?.labels?.[0] || (id ? `Prof. ${String(id).slice(-4)}` : null)
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
function staffForSede(sedeKey){
  const locId = locationToId(sedeKey)
  return EMPLOYEES.filter(e => e.bookable && (e.allow.includes("ALL") || e.allow.includes(locId)))
}
function staffSedeRosterForPrompt(sedeKey){
  const list = staffForSede(sedeKey)
  if (!list.length) return "(sin profesionales configuradas para esta sede)"
  return list.map(e=>{
    const locTags = e.allow.map(id=> id===LOC_TORRE? "torremolinos" : id===LOC_LUZ? "la_luz" : id).join(",")
    return "• ID:" + e.id + " | Nombres:[" + e.labels.join(", ") + "] | Sedes:[" + (locTags||"ALL") + "]"
  }).join("\n")
}

// ====== Servicios + uñas helpers
function cleanDisplayLabel(label){ return String(label||"").replace(/^\s*(luz|la\s*luz)\s+/i,"").trim() }
function servicesForSedeKeyRaw(sedeKey){
  const prefix = (sedeKey==="la_luz") ? "SQ_SVC_luz_" : "SQ_SVC_"
  const out=[]
  for (const [k,v] of Object.entries(process.env)){
    if (!k.startsWith(prefix)) continue
    const [id] = String(v||"").split("|"); if (!id) continue
    const raw = k.replace(prefix,"").replaceAll("_"," ")
    const label = raw.replace(/\b([a-z])/g,m=>m.toUpperCase()).replace("Pestan","Pestañ")
    out.push({ sedeKey, key:k, id, rawKey:k, label: cleanDisplayLabel(label), norm: norm(label) })
  }
  return out
}
function serviceLabelFromEnvKey(envKey){
  if (!envKey) return null
  const all = [...servicesForSedeKeyRaw("torremolinos"), ...servicesForSedeKeyRaw("la_luz")]
  return all.find(s=>s.key===envKey)?.label || null
}
const POS_NAIL_ANCHORS = ["uña","unas","uñas","manicura","gel","acrilic","acrilico","acrílico","acrilicas","acrílicas","acrylic","semi","semipermanente","esculpida","esculpidas","press on","press-on","tips","francesa","frances","baby boomer","encapsulado","encapsulados","nivelacion","nivelación","esmaltado","esmalte"]
const NEG_NOT_NAILS = ["pesta","pestañ","ceja","cejas","ojos","pelo a pelo","eyelash"]
function shouldIncludePedicure(userMsg){ return /\b(pedicur|pies|pie)\b/i.test(String(userMsg||"")) }
function isNailsLabel(labelNorm, allowPedicure){
  if (NEG_NOT_NAILS.some(n=>labelNorm.includes(norm(n)))) return false
  const hasPos = POS_NAIL_ANCHORS.some(p=>labelNorm.includes(norm(p))); if (!hasPos) return false
  const isPedi = /\b(pedicur|pies|pie)\b/.test(labelNorm); if (isPedi && !allowPedicure) return false
  return true
}
function uniqueByLabel(arr){ const seen=new Set(); const out=[]; for (const s of arr){ const key=s.label.toLowerCase(); if (seen.has(key)) continue; seen.add(key); out.push(s) } return out }

function nailsServicesForSede(sedeKey, userMsg){
  const allowPedi = shouldIncludePedicure(userMsg)
  const list = servicesForSedeKeyRaw(sedeKey)
  const filtered = list.filter(s=>isNailsLabel(s.norm, allowPedi))
  return uniqueByLabel(filtered)
}
function scoreServiceRelevance(userMsg, label){
  const u = norm(userMsg), l = norm(label); let score = 0
  // señales fuertes
  if (/\b(uñas|unas)\b/.test(u) && /\b(uñas|unas|manicura)\b/.test(l)) score += 3
  if (/\bmanicura\b/.test(u) && /\bmanicura\b/.test(l)) score += 3
  if (/\b(acrilic|acrilico|acrílico|acrilicas|acrílicas|acrylic)\b/.test(u) && (l.includes("acril") || l.includes("esculp"))) score += 3.2 // acrílicas ≈ esculpidas
  if (/\bgel\b/.test(u) && l.includes("gel")) score += 2.5
  if (/\bsemi|semipermanente\b/.test(u) && l.includes("semi")) score += 2
  if (/\brelleno\b/.test(u) && (l.includes("uña") || l.includes("manicura") || l.includes("gel") || l.includes("acril"))) score += 2
  if (/\bretir(ar|o)\b/.test(u) && (l.includes("quitar")||l.includes("retiro")||l.includes("retir"))) score += 1.8
  // tokens blandos
  const tokens = ["natural","francesa","frances","decoracion","diseño","extra","exprés","express","completa","nivelacion","nivelación","baby boomer","encapsulado","tips","press"]
  for (const t of tokens){ if (u.includes(norm(t)) && l.includes(norm(t))) score += 0.4 }
  // solapamiento
  const utoks = new Set(u.split(" ").filter(Boolean))
  const ltoks = new Set(l.split(" ").filter(Boolean))
  let overlap=0; for (const t of utoks){ if (ltoks.has(t)) overlap++ }
  score += Math.min(overlap,3)*0.25
  return score
}
function resolveEnvKeyFromLabelAndSede(label, sedeKey){
  const list = servicesForSedeKeyRaw(sedeKey)
  return list.find(s=>s.label.toLowerCase()===String(label||"").toLowerCase())?.key || null
}

// === Auto-selección de servicio si hay match claro
function tryAutoSelectService(sedeKey, userMsg, aiCandidates){
  if (!sedeKey) return null
  const services = nailsServicesForSede(sedeKey, userMsg)
  if (!services.length) return null

  // 1) puntuación local
  const scores = services.map(s => ({ ...s, score: scoreServiceRelevance(userMsg, s.label) }))
  scores.sort((a,b)=> b.score - a.score)

  // 2) candidatos IA (si hay), subimos score por confianza
  const candMap = new Map()
  if (Array.isArray(aiCandidates)){
    for (const c of aiCandidates){
      const label = cleanDisplayLabel(String(c.label||"")).trim()
      const conf = Number(c.confidence ?? 0)
      if (!label) continue
      candMap.set(label.toLowerCase(), Math.max(conf, candMap.get(label.toLowerCase())||0))
    }
    for (const s of scores){
      const conf = candMap.get(s.label.toLowerCase())
      if (conf != null) s.score += (conf * 3.5)
    }
  }

  // 3) decisiones: umbral + separación con el segundo
  const top = scores[0], second = scores[1]
  if (!top) return null
  const strong = top.score >= 5.0
  const separated = !second || (top.score - second.score >= 1.6)
  if (strong && separated) return top

  // Si la IA marcó uno con confianza muy alta, acéptalo aunque la separación sea menor
  if (candMap.size){
    const bestAI = scores.find(s => (candMap.get(s.label.toLowerCase())||0) >= 0.82)
    if (bestAI) return bestAI
  }

  return null
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
    await sock.sendMessage(jid, { text: "No encuentro tu ficha por este número. Dime tu *nombre completo* y, si quieres, tu *email* para crearte 😊" })
    return { status:"need_new" }
  }
  const choices = matches.map((c,i)=>({ index:i+1, id:c.id, name:c?.givenName || "Sin nombre", email:c?.emailAddress || "—" }))
  sessionData.identityChoices = choices
  sessionData.stage = "awaiting_identity_pick"
  saveSession(phone, sessionData)
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

// ====== Booking helpers
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

// ====== Disponibilidad genérica + filtrado
async function searchAvailabilityGeneric({ locationKey, envServiceKey, fromEU, days=14, max=50, distinctDays=false }){
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
      slots.push({ date:d, staffId: tm })
      if (slots.length>=max) break
    }
    return slots
  }catch{ return [] }
}
function prioritizeSlotsFromGeneric(slots, preferredStaffId, n=3){
  const byPref = preferredStaffId ? slots.filter(s => s.staffId === preferredStaffId) : []
  const rest = preferredStaffId ? slots.filter(s => s.staffId !== preferredStaffId) : slots.slice()
  const chosen = [...byPref.slice(0,n)]
  for (const s of rest){ if (chosen.length>=n) break; chosen.push(s) }
  return { chosen, usedPreferred: !!(preferredStaffId && byPref.length) }
}

// ====== IA core
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
    })
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

// ====== Segmentación de hilos con DeepSeek
function makeThreadId(){ return `thr_${Date.now().toString(36)}_${Math.random().toString(36).slice(2,6)}` }
async function decideNewThreadDeepSeek({ recentTurns, newMessage, minutesSinceLast }){
  const system = `Eres un segmentador de conversaciones de WhatsApp. Devuelves SOLO JSON válido.
Tarea: decidir si el mensaje nuevo inicia una conversación distinta a la última o si continúa el mismo hilo.
Criterios:
- Cambio claro de tema => nuevo hilo. - Retoma directo de lo anterior => mismo hilo.
- Inactividad larga (minutos_since_last) => inclínate por hilo nuevo salvo referencia explícita.
Formato:
{"is_new_conversation": true|false, "confidence": 0..1, "reason": "texto corto", "carry_over_fields": []}`
  const msg = { role:"user", content:
`ULTIMOS_TURNOS:
${recentTurns.map(t => `- [${t.ts}] ${t.role.toUpperCase()}: ${t.text}`).join("\n")}
MINUTES_SINCE_LAST: ${minutesSinceLast}
MENSAJE_NUEVO: "${newMessage}"
INSTRUCCION: Devuelve SOLO el JSON indicado.` }
  const raw = await callAIWithRetries([msg], system)
  if (!raw) return { is_new_conversation: minutesSinceLast > THREAD_TIMEOUT_MIN }
  const cleaned = raw.replace(/```json\s*/gi,"").replace(/```\s*/g,"").replace(/^[^{]*/,"").replace(/[^}]*$/,"").trim()
  try{ return JSON.parse(cleaned) }catch{ return { is_new_conversation: minutesSinceLast > THREAD_TIMEOUT_MIN } }
}

// ====== Prompts de negocio
function staffRosterForPrompt(){
  return EMPLOYEES.map(e=>{
    const locs = e.allow.map(id=> id===LOC_TORRE?"torremolinos" : id===LOC_LUZ?"la_luz" : id).join(",")
    return "• ID:" + e.id + " | Nombres:[" + e.labels.join(", ") + "] | Sedes:[" + (locs||"ALL") + "] | Reservable:" + e.bookable
  }).join("\n")
}
function buildSystemPrompt() {
  const nowEU = dayjs().tz(EURO_TZ);
  const torremolinos_services = servicesForSedeKeyRaw("torremolinos");
  const laluz_services = servicesForSedeKeyRaw("la_luz");
  const staffLines = staffRosterForPrompt()

  return (
`Eres el asistente de WhatsApp para Gapink Nails. Devuelves SOLO JSON válido.

INFORMACIÓN:
- Fecha/hora actual: ${nowEU.format("dddd DD/MM/YYYY HH:mm")} (Madrid)
- Estado: PRODUCCIÓN

SEDES:
- Torremolinos: ${ADDRESS_TORRE}
- Málaga – La Luz: ${ADDRESS_LUZ}

HORARIOS:
- L-V 09:00-20:00; S/D cerrado; Festivos: ${HOLIDAYS_EXTRA.join(", ")}

PROFESIONALES (con aliases y sedes):
${staffLines}

REGLAS:
1) Identidad: NO pidas nombre/email si el número existe (match único). Solo si no existe o hay duplicados.
2) "Uñas" ambiguo → acción "choose_service". Lista SOLO uñas (pedicura solo si se menciona). Orden: candidatos IA arriba.
3) Si hay match claro con un servicio (ej: el usuario dice "acrílicas") → NO lista: selecciona servicio y pasa a proponer horas.
4) Sede requerida para listar/proponer.
5) 1/2/3 selecciona hora si hay lastHours.
6) Cancelar: usa número del chat para listar y cancelar.

FORMATO:
{"message":"...","action":"propose_times|create_booking|list_appointments|cancel_appointment|choose_service|need_info|none","session_updates":{...},"action_params":{...}}`
  )
}

async function getAIResponse(userMessage, sessionData, phone) {
  const systemPrompt = buildSystemPrompt();
  const threadId = sessionData?.thread_id || null

  const recent = db.prepare(`
    SELECT user_message, ai_response 
    FROM ai_conversations 
    WHERE phone = ? AND thread_id = ?
    ORDER BY timestamp DESC 
    LIMIT 6
  `).all(phone, threadId || "__none__");

  const conversationHistory = recent.reverse().map(msg => [
    { role: "user", content: msg.user_message },
    { role: "assistant", content: msg.ai_response }
  ]).flat();

  let dynamicContext = ""
  if (sessionData?.sede){
    const sedeRoster = staffSedeRosterForPrompt(sessionData.sede)
    dynamicContext += "STAFF_SEDE_ACTUAL:\n" + sedeRoster + "\n\n"
  }
  if (sessionData?.lastStaffNamesById && Object.keys(sessionData.lastStaffNamesById).length){
    const lines = Object.entries(sessionData.lastStaffNamesById).map(([id,name]) => "• " + name + " (ID:" + id + ")").join("\n")
    dynamicContext += "RECENT_STAFF_SHOWN:\n" + lines + "\n\n"
  }

  const sessionContext =
"ESTADO:\n" +
"- Sede: " + (sessionData?.sede || 'no seleccionada') + "\n" +
"- Servicio: " + (sessionData?.selectedServiceLabel || 'no seleccionado') + " (" + (sessionData?.selectedServiceEnvKey || 'no_key') + ")\n" +
"- Profesional preferida: " + (sessionData?.preferredStaffLabel || 'ninguna') + "\n" +
"- Fecha/hora pendiente: " + (sessionData?.pendingDateTime ? fmtES(parseToEU(sessionData.pendingDateTime)) : 'no seleccionada') + "\n" +
"- Etapa: " + (sessionData?.stage || 'inicial') + "\n" +
"- Últimas horas propuestas: " + (Array.isArray(sessionData?.lastHours) ? (sessionData.lastHours.length + ' opciones') : 'ninguna') + "\n\n" +
dynamicContext

  const messages = [
    ...conversationHistory,
    { role: "user", content: `MENSAJE DEL CLIENTE: "${userMessage}"\n\n${sessionContext}INSTRUCCIÓN: Devuelve SOLO JSON siguiendo las reglas.` }
  ];

  const aiText = await callAIWithRetries(messages, systemPrompt)
  if (!aiText || /^error de conexión/i.test(aiText.trim())) return buildLocalFallback(userMessage, sessionData)

  const cleaned = aiText.replace(/```json\s*/gi, "").replace(/```\s*/g, "").replace(/^[^{]*/, "").replace(/[^}]*$/, "").trim()
  try { return JSON.parse(cleaned) } catch { return buildLocalFallback(userMessage, sessionData) }
}

// ====== Fallback local (resumen)
function buildLocalFallback(userMessage, sessionData){
  const lower = norm(String(userMessage||""))
  const numMatch = lower.match(/^(?:opcion|opción)?\s*([1-9]\d*)\b/)
  const yesMatch = /\b(si|sí|ok|vale|confirmo|de\ acuerdo)\b/i.test(userMessage||"")
  const cancelMatch = /\b(cancelar|anular|borra|elimina)\b/i.test(lower)
  const listMatch = /\b(mis citas|lista|ver citas)\b/i.test(lower)
  const bookMatch = /\b(reservar|cita|quiero.*(cita|reservar))\b/i.test(lower)
  const hasCore = (s)=> s?.sede && s?.selectedServiceEnvKey && s?.pendingDateTime

  if (numMatch && Array.isArray(sessionData?.lastHours) && sessionData.lastHours.length){
    const idx = Number(numMatch[1]) - 1
    const pick = sessionData.lastHours[idx]
    if (dayjs.isDayjs(pick)){
      const updates = { pendingDateTime: pick.tz(EURO_TZ).toISOString(), stage: null }
      const okToCreate = hasCore({...sessionData, ...updates})
      return { message: okToCreate ? "Perfecto, voy a confirmar esa hora 👍" : "Genial. Dime la sede y el servicio para terminar.", action: okToCreate ? "create_booking" : "need_info", session_updates: updates, action_params: {} }
    }
  }
  if (yesMatch){
    if (hasCore(sessionData)){
      return { message:"¡Voy a crear la reserva! ✨", action:"create_booking", session_updates:{}, action_params:{} }
    } else {
      const faltan=[]
      if (!sessionData?.sede) faltan.push("sede")
      if (!sessionData?.selectedServiceEnvKey) faltan.push("servicio")
      if (!sessionData?.pendingDateTime) faltan.push("fecha y hora")
      return { message:`Me faltan: ${faltan.join(", ")}.`, action:"need_info", session_updates:{}, action_params:{} }
    }
  }
  if (cancelMatch) return { message:"Vale, dime qué cita quieres cancelar o responde con el número cuando te la liste.", action:"cancel_appointment", session_updates:{}, action_params:{} }
  if (listMatch) return { message:"Estas son tus próximas citas:", action:"list_appointments", session_updates:{}, action_params:{} }
  if (bookMatch){
    if (sessionData?.sede && sessionData?.selectedServiceEnvKey){
      return { message:"Te propongo horas disponibles:", action:"propose_times", session_updates:{ stage:"awaiting_time" }, action_params:{} }
    } else {
      const faltan=[]
      if (!sessionData?.sede) faltan.push("sede (Torremolinos o La Luz)")
      if (!sessionData?.selectedServiceEnvKey) faltan.push("servicio")
      return { message:`Para proponerte horas dime: ${faltan.join(" y ")}.`, action:"need_info", session_updates:{}, action_params:{} }
    }
  }
  return { message:"¿Quieres reservar, cancelar o ver tus citas? Si es para reservar, dime sede y servicio.", action:"none", session_updates:{}, action_params:{} }
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

// ====== Cancel intent + sede parse
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

// ====== Elección/auto-selección de servicios
function buildServiceChoiceListBySede(sedeKey, userMsg, aiCandidates){
  const nails = nailsServicesForSede(sedeKey, userMsg)
  const localScores = new Map()
  for (const s of nails){ localScores.set(s.label, scoreServiceRelevance(userMsg, s.label)) }
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
  const inAI = nails.filter(s=>aiMap.has(s.label)).sort((a,b)=> (aiMap.get(b.label)-aiMap.get(a.label)) || ((localScores.get(b.label)||0)-(localScores.get(a.label)||0)))
  const rest = nails.filter(s=>!aiMap.has(s.label)).sort((a,b)=> (localScores.get(b.label)||0)-(localScores.get(a.label)||0))
  const final = [...inAI, ...rest]
  return final.map((s,i)=>({ index:i+1, label:s.label }))
}
async function executeChooseService(params, sessionData, phone, sock, jid, userMsg){
  if (!sessionData.sede){
    sessionData.pendingCategory = "unas"
    sessionData.stage = "awaiting_sede_for_services"
    saveSession(phone, sessionData)
    await sendWithPresence(sock, jid, "¿En qué sede te viene mejor, Torremolinos o La Luz? (así te muestro las opciones de uñas correctas)")
    return
  }

  // 🔥 Auto-selección si hay match claro
  const auto = tryAutoSelectService(sessionData.sede, userMsg||"", params?.candidates || [])
  if (auto){
    sessionData.selectedServiceLabel = auto.label
    sessionData.selectedServiceEnvKey = auto.key
    sessionData.stage = null
    saveSession(phone, sessionData)
    await sendWithPresence(sock, jid, `Perfecto, te reservo para *${auto.label}*. Te paso horas disponibles 👇`)
    await executeProposeTime({}, sessionData, phone, sock, jid)
    return
  }

  // Si no hay match claro, lista
  const aiCands = Array.isArray(params?.candidates) ? params.candidates : []
  const items = buildServiceChoiceListBySede(sessionData.sede, userMsg||"", aiCands)
  if (!items.length){ await sendWithPresence(sock, jid, "Ahora mismo no tengo servicios de uñas configurados para esa sede."); return }

  sessionData.serviceChoices = items
  sessionData.stage = "awaiting_service_choice"
  saveSession(phone, sessionData)
  const lines = items.map(it=> {
    const star = aiCands.find(c=>cleanDisplayLabel(String(c.label||"")).toLowerCase()===it.label.toLowerCase()) ? " ⭐" : ""
    return `${it.index}) ${it.label}${star}`
  }).join("\n")
  await sendWithPresence(sock, jid, `Estas son nuestras opciones de *uñas* en ${locationNice(sessionData.sede)}:\n\n${lines}\n\nResponde con el número.`)
}

// ====== Proponer horas
async function executeProposeTime(_params, sessionData, phone, sock, jid) {
  const nowEU = dayjs().tz(EURO_TZ);
  const baseFrom = nextOpeningFrom(nowEU.add(NOW_MIN_OFFSET_MIN, "minute"));
  if (!sessionData.sede || !sessionData.selectedServiceEnvKey) { await sendWithPresence(sock, jid, "Necesito la sede y el servicio primero."); return; }

  const generic = await searchAvailabilityGeneric({
    locationKey: sessionData.sede,
    envServiceKey: sessionData.selectedServiceEnvKey,
    fromEU: baseFrom, days: 14, max: 50, distinctDays: false
  })

  let chosen=[], usedPreferred=false
  if (generic.length){
    const out = prioritizeSlotsFromGeneric(generic, sessionData.preferredStaffId, 3)
    chosen = out.chosen; usedPreferred = out.usedPreferred
  }
  if (!chosen.length){
    const generalSlots=(function({fromEU,durationMin=60,n=3}){
      const out=[]; let t=ceilToSlotEU(fromEU.clone()); t=nextOpeningFrom(t)
      while (out.length<n){
        if (insideBusinessHours(t,durationMin)){ out.push(t.clone()); t=t.add(SLOT_MIN,"minute") }
        else {
          const nowMin=t.hour()*60+t.minute(), closeMin=OPEN.end*60
          if (nowMin >= closeMin) t = t.add(1,"day").hour(OPEN.start).minute(0).second(0).millisecond(0)
          else t = t.add(SLOT_MIN,"minute")
          while (!WORK_DAYS.includes(t.day()) || isHolidayEU(t)) { t = t.add(1,"day").hour(OPEN.start).minute(0).second(0).millisecond(0) }
        }
      }
      return out
    })({fromEU:baseFrom,n:3})
    chosen = generalSlots.map(d => ({ date:d, staffId:null }))
  }

  const hoursEnum = enumerateHours(chosen.map(s => s.date))
  const map = {}; const nameMap={}
  for (const s of chosen){
    const iso = s.date.format("YYYY-MM-DDTHH:mm"); map[iso] = s.staffId || null
    if (s.staffId) nameMap[s.staffId] = staffLabelFromId(s.staffId)
  }
  sessionData.lastHours = chosen.map(s => s.date)
  sessionData.lastStaffByIso = map
  sessionData.lastStaffNamesById = nameMap
  sessionData.lastProposeUsedPreferred = usedPreferred
  sessionData.stage = "awaiting_time"
  saveSession(phone, sessionData)

  const lines = hoursEnum.map(h => {
    const sid = map[h.iso]; const tag = sid ? ` — ${staffLabelFromId(sid)}` : ""
    return `${h.index}) ${h.pretty}${tag}`
  }).join("\n")
  const header = usedPreferred
    ? `Horarios disponibles con ${sessionData.preferredStaffLabel || "tu profesional"}:`
    : `Horarios disponibles (nuestro equipo):${sessionData.preferredStaffLabel ? `\nNota: no veo huecos con ${sessionData.preferredStaffLabel} en los próximos días; te muestro alternativas.`:""}`
  await sendWithPresence(sock, jid, `${header}\n${lines}\n\nResponde con el número (1, 2 o 3)`)
}

// ====== Crear reserva
async function createBookingWithRetry({ startEU, locationKey, envServiceKey, durationMin, customerId, teamMemberId, phone }){
  if (!envServiceKey) return { success: false, error: "No se especificó servicio" }
  if (!teamMemberId || typeof teamMemberId!=="string" || !teamMemberId.trim()) return { success: false, error: "teamMemberId requerido" }
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
  let staffId = sessionData.lastStaffByIso?.[iso] || sessionData.preferredStaffId || null
  if (!staffId) {
    const probe = await searchAvailabilityGeneric({
      locationKey: sessionData.sede,
      envServiceKey: sessionData.selectedServiceEnvKey,
      fromEU: startEU.clone().subtract(1, "minute"),
      days: 1, max: 10
    })
    const match = probe.find(x => x.date.isSame(startEU, "minute"))
    if (match?.staffId) staffId = match.staffId
  }
  if (!staffId) staffId = pickStaffForLocation(sessionData.sede, null)
  if (!staffId) { await sendWithPresence(sock, jid, "No hay profesionales disponibles en esa sede"); return; }

  const { status, customer } = await getUniqueCustomerByPhoneOrPrompt(phone, sessionData, sock, jid) || {}
  if (status === "need_new" || status === "need_pick") return

  let customerId = customer?.id
  if (!customerId && (sessionData.name || sessionData.email)){
    const created = await findOrCreateCustomerWithRetry({ name: sessionData.name, email: sessionData.email, phone })
    if (!created){ await sendWithPresence(sock, jid, "No pude crear tu ficha de cliente. Intenta de nuevo o contacta al salón."); return }
    customerId = created.id
  }
  if (!customerId){
    sessionData.stage = "awaiting_identity"
    saveSession(phone, sessionData)
    await sendWithPresence(sock, jid, "Para terminar, dime tu *nombre* y (opcional) tu *email* para crear tu ficha 😊")
    return
  }

  const result = await createBookingWithRetry({
    startEU, locationKey: sessionData.sede, envServiceKey: sessionData.selectedServiceEnvKey,
    durationMin: 60, customerId, teamMemberId: staffId, phone
  })
  if (!result.success) {
    const aptId = `apt_failed_${Math.random().toString(36).slice(2,8)}${Date.now().toString(36).slice(-4)}`
    insertAppt.run({
      id: aptId, customer_name: sessionData?.name || null, customer_phone: phone,
      customer_square_id: customerId, location_key: sessionData.sede, service_env_key: sessionData.selectedServiceEnvKey,
      service_label: serviceLabelFromEnvKey(sessionData.selectedServiceEnvKey) || sessionData.selectedServiceLabel || "Servicio", duration_min: 60,
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
    service_label: serviceLabelFromEnvKey(sessionData.selectedServiceEnvKey) || sessionData.selectedServiceLabel || "Servicio",
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

Referencia: ${result.booking.id}

¡Te esperamos!`
  await sendWithPresence(sock, jid, confirmMessage);
  clearSession(phone);
}

// ====== Listar / cancelar por teléfono
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
  try{
    const body = { idempotencyKey:`cancel_${appointment.id}_${Date.now()}` }
    const resp = await square.bookingsApi.cancelBooking(appointment.id, body)
    if (resp?.result?.booking) await sendWithPresence(sock, jid, `✅ Cita cancelada: ${appointment.pretty} en ${appointment.sede}`)
    else await sendWithPresence(sock, jid, "No pude cancelar la cita. Por favor contacta directamente al salón.")
  }catch{ await sendWithPresence(sock, jid, "No pude cancelar la cita. Por favor contacta directamente al salón.") }
  delete sessionData.cancelList
  sessionData.stage = null
  saveSession(phone, sessionData)
}

// ====== Mini-web (rosa glass + crédito centrado)
const app=express()
const PORT=process.env.PORT||8080
let lastQR=null, conectado=false

app.get("/", (_req,res)=>{
  const statusText = conectado ? "✅ Conectado" : "❌ Desconectado"
  const statusClass = conectado ? "ok" : "bad"

  res.send(`<!doctype html>
<html lang="es">
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Gapink Nails — Bot</title>
<style>
  :root{
    --pink-a:#ffd9ec; --pink-b:#fbb6d9; --pink-c:#f79cc9;
    --glass: rgba(255,255,255,.18); --brd: rgba(255,255,255,.35); --txt:#32172a;
  }
  *{box-sizing:border-box}
  body{
    margin:0; min-height:100vh; color:var(--txt);
    display:grid; place-items:center;
    background:
      radial-gradient(1200px 800px at 10% 10%, var(--pink-a), transparent 60%),
      radial-gradient(1000px 700px at 90% 15%, var(--pink-b), transparent 60%),
      radial-gradient(900px 900px at 50% 100%, var(--pink-c), transparent 60%),
      linear-gradient(135deg, #fff6fb, #ffdff0);
  }
  .wrap{ width:min(820px, 92vw); display:flex; flex-direction:column; gap:18px; padding:28px; align-items:center; }
  .logo{
    font-family: ui-rounded, system-ui, -apple-system, "Segoe UI", Roboto, "Helvetica Neue", Arial;
    font-weight:800; letter-spacing:0.5px; text-transform:uppercase;
    font-size: clamp(28px, 5.5vw, 46px); color:#fff;
    text-shadow: 0 8px 30px rgba(247,156,201,.35), 0 2px 6px rgba(0,0,0,.18);
    padding:12px 18px; border-radius:20px;
    background: linear-gradient(135deg, #ff8fc4, #ffa6d1, #ffc3e1);
    box-shadow: 0 8px 32px rgba(247,156,201,.35);
    text-align:center;
  }
  .glass{ background: var(--glass); border: 1px solid var(--brd); border-radius: 22px; padding: 22px; backdrop-filter: blur(16px) saturate(1.2); -webkit-backdrop-filter: blur(16px) saturate(1.2); box-shadow: 0 10px 40px rgba(50,23,42,.12), inset 0 1px 0 rgba(255,255,255,.25); width:100%; max-width:680px; }
  .status{ display:flex; justify-content:center; align-items:center; gap:12px; font-size:18px; font-weight:700; padding:14px 16px; border-radius:14px; background: rgba(255,255,255,.22); border:1px solid rgba(255,255,255,.35); }
  .dot{width:12px; height:12px; border-radius:50%;}
  .ok .dot{ background: #22c55e; box-shadow: 0 0 18px rgba(34,197,94,.5);}
  .bad .dot{ background: #ef4444; box-shadow: 0 0 18px rgba(239,68,68,.5);}
  .qr{ display:flex; flex-direction:column; align-items:center; gap:12px; margin-top:10px; }
  .qr img{ width:min(320px, 70vw); border-radius:16px; border:1px solid rgba(255,255,255,.45); box-shadow: 0 12px 40px rgba(50,23,42,.18); background:#fff; }
  .credit{ margin-top:8px; text-align:center; font-weight:700; }
  .credit a{ color:#711b42; text-decoration:none; border-bottom:1px dashed rgba(113,27,66,.35); padding-bottom:1px; }
</style>
<body>
  <div class="wrap">
    <div class="logo">Gapink Nails</div>
    <div class="glass">
      <div class="status ${statusClass}">
        <div class="dot"></div>
        <div>Estado WhatsApp: <span>${statusText}</span></div>
      </div>
      ${!conectado ? `
      <div class="qr">
        <img src="/qr.png" alt="Escanea para conectar WhatsApp" />
        <div class="credit">Hecho por <a href="https://gonzalog.co" target="_blank" rel="noopener noreferrer">Gonzalo García Aranda</a></div>
      </div>` : `
      <div class="credit">Hecho por <a href="https://gonzalog.co" target="_blank" rel="noopener noreferrer">Gonzalo García Aranda</a></div>
      `}
    </div>
  </div>
</body>
</html>`)
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

// ====== Arranque / hilo por mensaje
async function startBot(){
  try{
    const { makeWASocket, useMultiFileAuthState, fetchLatestBaileysVersion, Browsers } = await loadBaileys()
    if(!fs.existsSync("auth_info")) fs.mkdirSync("auth_info",{recursive:true})
    const { state, saveCreds } = await useMultiFileAuthState("auth_info")
    const { version } = await fetchLatestBaileysVersion().catch(()=>({version:[2,3000,0]}))
    const sock = makeWASocket({ 
      logger:pino({level:"silent"}), printQRInTerminal:false, auth:state, version,
      browser:Browsers.macOS("Desktop"), syncFullHistory:false 
    })
    globalThis.sock=sock

    sock.ev.on("connection.update", ({connection,qr})=>{
      if (qr){ lastQR=qr; conectado=false; try{ qrcodeTerminal.generate(qr,{small:true}) }catch{} }
      if (connection==="open"){ lastQR=null; conectado=true; RECONNECT_ATTEMPTS=0; RECONNECT_SCHEDULED=false; }
      if (connection==="close"){ 
        conectado=false
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
      if (!m?.message || m.key.fromMe) return
      
      const jid = m.key.remoteJid
      const phone = normalizePhoneES((jid||"").split("@")[0]||"") || (jid||"").split("@")[0]
      const textRaw = (m.message.conversation || m.message.extendedTextMessage?.text || m.message?.imageMessage?.caption || "").trim()
      if (!textRaw) return

      await enqueue(phone, async ()=>{
        try {
          let sessionData = loadSession(phone) || {
            greeted: false, sede: null, selectedServiceEnvKey: null, selectedServiceLabel: null,
            preferredStaffId: null, preferredStaffLabel: null, pendingDateTime: null,
            name: null, email: null, last_msg_id: null, lastStaffByIso: {},
            lastProposeUsedPreferred: false, stage: null, cancelList: null,
            serviceChoices: null, identityChoices: null, pendingCategory: null,
            lastStaffNamesById: null,
            thread_id: null, thread_started_at: null, thread_last_at: null
          }

          // ==== Segmentación de hilo
          const lastTs = sessionData.thread_last_at ? dayjs(sessionData.thread_last_at) : null
          const minutesSinceLast = lastTs ? Math.max(0, dayjs().diff(lastTs, "minute")) : 999999
          const recentTurnsRaw = db.prepare(`
            SELECT user_message, ai_response, timestamp 
            FROM ai_conversations 
            WHERE phone = ? 
            ORDER BY timestamp DESC 
            LIMIT 12
          `).all(phone).reverse()
          const recentTurns = []
          for (const r of recentTurnsRaw){
            if (r.user_message) recentTurns.push({ role:"user", text:r.user_message, ts:r.timestamp })
            if (r.ai_response)  recentTurns.push({ role:"assistant", text:r.ai_response, ts:r.timestamp })
          }
          let needNewThread = minutesSinceLast > THREAD_TIMEOUT_MIN
          if (!needNewThread){
            const decision = await decideNewThreadDeepSeek({ recentTurns, newMessage: textRaw, minutesSinceLast })
            needNewThread = !!decision?.is_new_conversation
          }
          if (!sessionData.thread_id){
            sessionData.thread_id = makeThreadId()
            sessionData.thread_started_at = new Date().toISOString()
          } else if (needNewThread){
            // reset suave
            sessionData = {
              greeted:false, sede:null, selectedServiceEnvKey:null, selectedServiceLabel:null,
              preferredStaffId:null, preferredStaffLabel:null, pendingDateTime:null,
              name: sessionData?.name || null, email: sessionData?.email || null,
              last_msg_id:null, lastStaffByIso:{}, lastProposeUsedPreferred:false, stage:null,
              cancelList:null, serviceChoices:null, identityChoices:null, pendingCategory:null,
              lastStaffNamesById:null,
              thread_id: makeThreadId(), thread_started_at: new Date().toISOString(), thread_last_at: new Date().toISOString()
            }
          }
          sessionData.thread_last_at = new Date().toISOString()
          saveSession(phone, sessionData)

          // Evitar reprocesar
          if (sessionData.last_msg_id === m.key.id) return
          sessionData.last_msg_id = m.key.id
          saveSession(phone, sessionData)

          const lower = norm(textRaw)
          const numMatch = lower.match(/^(?:opcion|opción)?\s*([1-9]\d*)\b/)

          // 0) Resolver sede si el user la dice al vuelo
          const parsedSede = parseSede(textRaw); if (parsedSede && !sessionData.sede){ sessionData.sede = parsedSede; saveSession(phone, sessionData) }

          // A) Elección de servicio tras mostrar lista
          if (sessionData.stage === "awaiting_service_choice" && Array.isArray(sessionData.serviceChoices) && sessionData.serviceChoices.length && numMatch){
            const n = Number(numMatch[1]); const chosen = sessionData.serviceChoices.find(it=>it.index===n)
            if (chosen){
              const ek = resolveEnvKeyFromLabelAndSede(chosen.label, sessionData.sede)
              if (ek){
                sessionData.selectedServiceLabel = chosen.label
                sessionData.selectedServiceEnvKey = ek
                sessionData.stage = null
                saveSession(phone, sessionData)
                await sendWithPresence(sock, jid, `Perfecto, te reservo para *${chosen.label}*. Te paso horas 👇`)
                await executeProposeTime({}, sessionData, phone, sock, jid)
                return
              }
            }
          }

          // B) Selección de hora
          if (numMatch && Array.isArray(sessionData.lastHours) && sessionData.lastHours.length && (!sessionData.stage || sessionData.stage==="awaiting_time")){
            const idx = Number(numMatch[1]) - 1
            const pick = sessionData.lastHours[idx]
            if (dayjs.isDayjs(pick)){
              const iso = pick.format("YYYY-MM-DDTHH:mm")
              const staffFromIso = sessionData?.lastStaffByIso?.[iso] || null
              sessionData.pendingDateTime = pick.tz(EURO_TZ).toISOString()
              if (staffFromIso){ sessionData.preferredStaffId = staffFromIso; sessionData.preferredStaffLabel = null }
              saveSession(phone, sessionData)
              const aiObj = { message:"Perfecto, confirmo tu cita ✨", action:"create_booking", session_updates:{}, action_params:{} }
              await routeAIResult(aiObj, sessionData, textRaw, m, phone, sock, jid)
              return
            }
          }

          // C) Cancelación
          if (numMatch && sessionData.stage==="awaiting_cancel" && Array.isArray(sessionData.cancelList) && sessionData.cancelList.length){
            const n = Number(numMatch[1])
            const chosen = sessionData.cancelList.find(apt=>apt.index===n)
            if (chosen){
              try{
                const body = { idempotencyKey:`cancel_${chosen.id}_${Date.now()}` }
                const resp = await square.bookingsApi.cancelBooking(chosen.id, body)
                if (resp?.result?.booking) await sendWithPresence(sock, jid, `✅ Cita cancelada: ${chosen.pretty} en ${chosen.sede}`)
                else await sendWithPresence(sock, jid, "No pude cancelar la cita. Por favor contacta directamente al salón.")
              }catch{ await sendWithPresence(sock, jid, "No pude cancelar la cita. Por favor contacta directamente al salón.") }
              delete sessionData.cancelList
              sessionData.stage = null
              saveSession(phone, sessionData)
              return
            }
          }
          if (isCancelIntent(textRaw) && sessionData.stage!=="awaiting_cancel"){
            await executeCancelAppointment({}, sessionData, phone, sock, jid)
            return
          }

          // === IA negocio (solo historial del hilo actual)
          const aiObj = await getAIResponse(textRaw, sessionData, phone)

          // D) Overwrite: si la IA quería lista, pero tenemos match claro → saltamos lista
          if ((!sessionData.selectedServiceEnvKey) && /\buñ|unas|manicura|gel|acril/i.test(textRaw)){
            const auto = tryAutoSelectService(sessionData.sede || parseSede(textRaw), textRaw, aiObj?.action_params?.candidates || [])
            if (auto){
              sessionData.sede = sessionData.sede || parseSede(textRaw) || sessionData.sede
              sessionData.selectedServiceLabel = auto.label
              sessionData.selectedServiceEnvKey = auto.key
              sessionData.stage = null
              saveSession(phone, sessionData)
              await sendWithPresence(sock, jid, `Perfecto, te reservo para *${auto.label}*. Te paso horas 👇`)
              await executeProposeTime({}, sessionData, phone, sock, jid)
              // guardamos el AI turn igualmente
              await routeAIResult({ message:"", action:"none", session_updates:{}, action_params:{} }, sessionData, textRaw, m, phone, sock, jid)
              return
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
  if (aiObj?.session_updates) {
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

  insertAIConversation.run({
    phone, message_id: m.key.id, user_message: textRaw,
    ai_response: safeJSONStringify(aiObj), timestamp: new Date().toISOString(),
    session_data: safeJSONStringify(sessionData), ai_error: null,
    fallback_used: Number(!!aiObj.__fallback_used), thread_id: sessionData.thread_id || null
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
      // Si habla de uñas y aún no hay servicio, mostramos lista (solo si no hubo match claro anteriormente)
      if (!sessionData.selectedServiceEnvKey && /\buñ|unas|manicura|gel|acril/i.test(textRaw)){
        await executeChooseService({ candidates: aiObj?.action_params?.candidates || [] }, sessionData, phone, sock, jid, textRaw)
      } else {
        if (aiObj.message) await sendWithPresence(sock, jid, aiObj.message)
      }
  }
}

// ====== Arranque
console.log(`🩷 Gapink Nails Bot v27.6.0`)
const appServer = app.listen(PORT, ()=>{ 
  console.log(`🌐 Mini-web en puerto ${PORT}`)
  console.log(`📱 Iniciando bot de WhatsApp...`)
  startBot().catch(console.error) 
})
process.on("uncaughtException", (e)=>{ console.error("💥 uncaughtException:", e?.stack||e?.message||e) })
process.on("unhandledRejection", (e)=>{ console.error("💥 unhandledRejection:", e) })
process.on("SIGTERM", ()=>{ try{ appServer?.close?.() }catch{} process.exit(0) })
process.on("SIGINT", ()=>{ try{ appServer?.close?.() }catch{} process.exit(0) })
