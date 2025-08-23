// index.js — Gapink Bot · v33.1.0 (simple & robusto)
// Requisitos: Node 20+
// ENV necesarios: los que ya tienes (Square, DeepSeek, Sedes, SQ_SVC_*, SQ_DUR_*)

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

// ============== NODE 20 SETUP ==============
if (!globalThis.crypto) globalThis.crypto = webcrypto

dayjs.extend(utc); dayjs.extend(tz); dayjs.locale("es")
const EURO_TZ = "Europe/Madrid"

// ============== CONFIG BÁSICA ==============
const WORK_DAYS = [1,2,3,4,5] // lun-vie
const OPEN = { start: 9, end: 20 } // 09:00–20:00
const MORNING = { start: 9, end: 14 }
const AFTERNOON = { start: 15, end: 20 }
const SLOT_MIN = 30

const NOW_MIN_OFFSET_MIN = Number(process.env.BOT_NOW_OFFSET_MIN || 30)
const SAME_DAY_MAX_DEVIATION_MIN = Number(process.env.BOT_MAX_SAME_DAY_DEVIATION_MIN || 60)
const SEARCH_WINDOW_DAYS = Number(process.env.BOT_SEARCH_WINDOW_DAYS || 14)

const HOLIDAYS_EXTRA = (process.env.HOLIDAYS_EXTRA || "06/01,28/02,15/08,12/10,01/11,06/12,08/12,25/12")
  .split(",").map(s=>s.trim()).filter(Boolean)

const BOT_DEBUG = /^true$/i.test(process.env.BOT_DEBUG || "")
const DRY_RUN   = /^true$/i.test(process.env.DRY_RUN || "")

// ============== SEDES Y DIRECCIONES ==============
const LOC_TORRE = (process.env.SQUARE_LOCATION_ID_TORREMOLINOS || "").trim()
const LOC_LUZ   = (process.env.SQUARE_LOCATION_ID_LA_LUZ || "").trim()
const ADDRESS_TORRE = process.env.ADDRESS_TORREMOLINOS || "Av. de Benyamina 18, Torremolinos"
const ADDRESS_LUZ   = process.env.ADDRESS_LA_LUZ || "Málaga – Barrio de La Luz"

// ============== SQUARE ==============
const square = new Client({
  accessToken: process.env.SQUARE_ACCESS_TOKEN,
  environment: (process.env.SQUARE_ENV==="production") ? Environment.Production : Environment.Sandbox
})

// ============== IA (DeepSeek) ==============
const AI_API_KEY = process.env.DEEPSEEK_API_KEY || ""
const AI_MODEL   = process.env.DEEPSEEK_MODEL || process.env.AI_MODEL || "deepseek-chat"
const AI_URL     = process.env.DEEPSEEK_API_URL || "https://api.deepseek.com/v1/chat/completions"
const AI_TIMEOUT_MS = Number(process.env.AI_TIMEOUT_MS || 8000)

function tryParseJSONLoose(text){
  try { return JSON.parse(text) } catch {}
  const m = String(text||"").match(/\{[\s\S]*\}|\[[\s\S]*\]/)
  if (m) { try { return JSON.parse(m[0]) } catch {} }
  return null
}
async function deepseekJSON(system, user){
  if (!AI_API_KEY) return null
  const controller = new AbortController()
  const to = setTimeout(()=>controller.abort(), AI_TIMEOUT_MS)
  try{
    const body = {
      model: AI_MODEL,
      temperature: 0.2,
      max_tokens: 400,
      messages: [
        { role:"system", content: system },
        { role:"user", content: user }
      ]
    }
    const res = await fetch(AI_URL, {
      method:"POST",
      headers:{ "Content-Type":"application/json", "Authorization":`Bearer ${AI_API_KEY}` },
      body: JSON.stringify(body),
      signal: controller.signal
    })
    if (!res.ok) throw new Error(`AI_HTTP_${res.status}`)
    const data = await res.json()
    const txt = data?.choices?.[0]?.message?.content || ""
    return tryParseJSONLoose(txt)
  } catch(e){
    if (BOT_DEBUG) console.error("DeepSeek fail:", e?.message||e)
    return null
  } finally { clearTimeout(to) }
}

// ============== UTILS ==============
const onlyDigits = s => String(s||"").replace(/\D+/g,"")
const rm = s => String(s||"").normalize("NFD").replace(/\p{Diacritic}/gu,"")
const norm = s => rm(s).toLowerCase().replace(/[+.,;:()/_-]/g," ").replace(/[^\p{Letter}\p{Number}\s]/gu," ").replace(/\s+/g," ").trim()
function normalizePhoneES(raw){
  const d = onlyDigits(raw)
  if (!d) return null
  if (raw.startsWith("+") && d.length >= 8 && d.length <= 15) return `+${d}`
  if (d.startsWith("34") && d.length === 11) return `+${d}`
  if (d.length === 9) return `+34${d}`
  if (d.startsWith("00")) return `+${d.slice(2)}`
  return `+${d}`
}
function stableKey(parts){
  const raw = Object.values(parts).join("|")
  return createHash("sha256").update(raw).digest("hex").slice(0,48)
}
function fmtES(d){
  const dias=["domingo","lunes","martes","miércoles","jueves","viernes","sábado"]
  const t=(dayjs.isDayjs(d)?d:dayjs(d)).tz(EURO_TZ)
  return `${dias[t.day()]} ${String(t.date()).padStart(2,"0")}/${String(t.month()+1).padStart(2,"0")} ${String(t.hour()).padStart(2,"0")}:${String(t.minute()).padStart(2,"0")}`
}
function locationToId(key){ return key==="la_luz" ? LOC_LUZ : LOC_TORRE }
function locationNice(key){ return key==="la_luz" ? "Málaga – La Luz" : "Torremolinos" }
function idToLocKey(id){ return id===LOC_LUZ ? "la_luz" : id===LOC_TORRE ? "torremolinos" : null }

function isHolidayEU(d){
  const dd = String(d.date()).padStart(2,"0");
  const mm = String(d.month()+1).padStart(2,"0");
  return HOLIDAYS_EXTRA.includes(`${dd}/${mm}`);
}
function insideBusinessHours(d, durMin){
  const t=d.clone()
  if (!WORK_DAYS.includes(t.day())) return false
  if (isHolidayEU(t)) return false
  const end=t.clone().add(durMin,"minute")
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
function parseSede(text){
  const t=norm(text||"")
  if (/\b(la luz|luz|malaga|málaga)\b/.test(t)) return "la_luz"
  if (/\b(torre|torremolinos|playamar)\b/.test(t)) return "torremolinos"
  return null
}
function parsePartOfDay(text){
  const t=norm(text)
  if (/\b(mañana|manana|por la manana|por la mañana|primeras horas|temprano)\b/.test(t)) return "morning"
  if (/\b(tarde|por la tarde|después de comer|despues de comer|ultima hora|más tarde|mas tarde)\b/.test(t)) return "afternoon"
  return null
}
function parseWeekTarget(text){
  const t=norm(text)
  if (/\b(semana que viene|proxima semana|pr[oó]xima semana|la proxima|la próxima)\b/.test(t)) return "next"
  if (/\b(esta semana|esta|hoy|manana|mañana|asap|ahora|ya|cuando antes|cuanto antes)\b/.test(t)) return "this"
  return null
}
function parseWeekday(text){
  const t=norm(text)
  const map = { "lunes":1,"martes":2,"miercoles":3,"miércoles":3,"jueves":4,"viernes":5 }
  for (const [k,v] of Object.entries(map)) if (new RegExp(`\\b${k}\\b`).test(t)) return v
  return null
}
function parsePreferredStaffFromText(text){
  // busca “con {nombre}”
  const t = norm(text||"")
  const m = t.match(/\bcon\s+([a-zñáéíóú]+)\b/i)
  if (!m) return null
  const token = norm(m[1])
  return findStaffByAliasToken(token)
}
function enumerateHours(list){ return list.map((d,i)=>({ index:i+1, iso:d.format("YYYY-MM-DDTHH:mm"), pretty:fmtES(d) })) }

// ============== DB ==============
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

function loadSession(phone){
  const row = db.prepare(`SELECT data_json FROM sessions WHERE phone=@phone`).get({phone})
  if (!row?.data_json) return null
  const s = JSON.parse(row.data_json)
  if (Array.isArray(s.lastHours_ms)) s.lastHours = s.lastHours_ms.map(ms=>dayjs.tz(ms,EURO_TZ))
  return s
}
function saveSession(phone,s){
  const c={...s}
  c.lastHours_ms = Array.isArray(s.lastHours)? s.lastHours.map(d=>dayjs.isDayjs(d)?d.valueOf():null).filter(Boolean):[]
  delete c.lastHours
  const j=JSON.stringify(c)
  const up=db.prepare(`UPDATE sessions SET data_json=@j, updated_at=@u WHERE phone=@p`).run({j,u:new Date().toISOString(),p:phone})
  if (up.changes===0) db.prepare(`INSERT INTO sessions (phone,data_json,updated_at) VALUES (@p,@j,@u)`).run({p:phone,j,u:new Date().toISOString()})
}
function clearSession(phone){ db.prepare(`DELETE FROM sessions WHERE phone=@phone`).run({phone}) }

// ============== STAFF (Todos los profesionales) ==============
function deriveLabelsFromEnvKey(envKey){
  const raw = envKey.replace(/^SQ_EMP_/, "")
  // ejemplos: SQ_EMP_ROCIO_CHICA_ROCIO -> ["ROCIO","CHICA","ROCIO"]
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

    // override con EMP_CENTER_*
    const empKey = "EMP_CENTER_" + k.replace(/^SQ_EMP_/, "")
    const empVal = process.env[empKey]
    if (empVal) {
      const centers = String(empVal).split(",").map(s=>s.trim().toLowerCase()).filter(Boolean)
      if (centers.some(c => c === "all")) {
        allow = ["ALL"]
      } else {
        const normCenter = c => (c==="la luz" ? "la_luz" : c)
        const ids = centers.map(c => locationToId(normCenter(c))).filter(Boolean)
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
  return e?.labels?.[0] || (id ? `Profesional ${String(id).slice(-4)}` : null)
}
function isStaffAllowedInLocation(staffId, locKey){
  const e = EMPLOYEES.find(x=>x.id===staffId)
  if (!e || !e.bookable) return false
  const locId = locationToId(locKey)
  return e.allow.includes("ALL") || e.allow.includes(locId)
}
const STAFF_ALIAS_MAP = new Map() // opcional si quisieras mapear alias manuales
function findStaffByAliasToken(tokenNorm){
  const directId = STAFF_ALIAS_MAP.get(tokenNorm)
  if (directId){
    const e = EMPLOYEES.find(x=>x.id===directId)
    if (e) return e
  }
  for (const e of EMPLOYEES){
    for (const lbl of e.labels){
      if (norm(lbl).includes(tokenNorm)) return e
    }
  }
  return null
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

// ============== SERVICIOS (listas completas por sede) ==============
function titleCase(str){ return String(str||"").toLowerCase().replace(/\b([a-z])/g, (m)=>m.toUpperCase()) }
function cleanDisplayLabel(s){ return String(s||"").replace(/^\s*(luz|la\s*luz)\s+/i,"").trim() }
function servicesForSedeKeyRaw(sedeKey){
  const prefix = (sedeKey==="la_luz") ? "SQ_SVC_luz_" : "SQ_SVC_"
  const out=[]
  for (const [k,v] of Object.entries(process.env)){
    if (!k.startsWith(prefix)) continue
    const [id] = String(v||"").split("|"); if (!id) continue
    const raw = k.replace(prefix,"").replaceAll("_"," ")
    let label = titleCase(raw)
    out.push({ sedeKey, key:k, id, label: cleanDisplayLabel(label), norm: norm(label) })
  }
  return out
}
function getDurationForEnvKey(envKey, fallback=60){
  if (!envKey) return fallback
  const durKey = envKey.replace(/^SQ_SVC_/,"SQ_DUR_")
  const v = process.env[durKey]
  const n = v!=null ? Number(String(v).trim()) : NaN
  return Number.isFinite(n) && n>=0 ? n : fallback
}
function serviceLabelFromEnvKey(envKey){
  if (!envKey) return null
  const all = [...servicesForSedeKeyRaw("torremolinos"), ...servicesForSedeKeyRaw("la_luz")]
  return all.find(s=>s.key===envKey)?.label || null
}
function findServiceFuzzy(labelLike, sedeKey){
  const list = servicesForSedeKeyRaw(sedeKey)
  const L = norm(labelLike)
  // contiene todas las palabras
  const words = L.split(" ").filter(Boolean)
  const hit = list.find(s => words.every(w => s.norm.includes(w)))
  return hit || null
}
function listServicesChunked(sedeKey, start=0, count=25){
  const all = servicesForSedeKeyRaw(sedeKey)
  const chunk = all.slice(start, start+count).map((s,i)=>({ index:start+i+1, label:s.label }))
  return { total: all.length, items: chunk }
}

// Categorías (para IA/heurística)
const CAT_PATTERNS = {
  "uñas": ["uñas","unas","manicura","gel","acril","semipermanente","nivelacion","esculpida","press on","tips","francesa","baby boomer","encapsulado"],
  "pestañas": ["pestaña","pestañas","eyelash","lifting","tinte","rizado","volumen","2d","3d","mega"],
  "cejas": ["ceja","cejas","brow","henna","laminado","perfilado","microblading","microshading","ombre","powder","polvo","hairstroke","micropigment"],
  "depilación": ["depil","cera","hilo","fotodepil","laser","láser","ipl","axila","pierna","ingles","pubis","facial","labio"],
  "pedicura": ["pedicura","pies"],
  "tratamiento facial": ["facial","higiene","limpieza","peeling","radiofrecuencia","hidra"],
  "tratamiento corporal": ["corporal","maderoterapia","drenaje","endosphere","reafirmante"],
  "dental": ["dental","blanqueamiento"]
}
function detectCategory(text){
  const t = norm(text||"")
  for (const [cat, list] of Object.entries(CAT_PATTERNS)){
    if (list.some(w => t.includes(norm(w)))) return cat
  }
  return null
}

// ============== SQUARE HELPERS ==============
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
async function searchAvailability({ locationKey, envServiceKey, fromEU, days=SEARCH_WINDOW_DAYS, staffId=null, limit=30 }){
  try{
    const sv = await getServiceIdAndVersion(envServiceKey)
    if (!sv?.id) return []
    const startAt = fromEU.tz("UTC").toISOString()
    const endAt = fromEU.clone().add(days,"day").tz("UTC").toISOString()
    const locationId = locationToId(locationKey)
    const filter = { serviceVariationId: sv.id }
    if (staffId) filter.teamMemberIdFilter = { any: [ staffId ] }
    const body = { query:{ filter:{ startAtRange:{ startAt, endAt }, locationId, segmentFilters:[filter] } } }
    const resp = await square.bookingsApi.searchAvailability(body)
    const avail = resp?.result?.availabilities || []
    const out=[]
    const durMin = getDurationForEnvKey(envServiceKey, 60)
    for (const a of avail){
      if (!a?.startAt) continue
      const d = dayjs(a.startAt).tz(EURO_TZ)
      if (!insideBusinessHours(d, durMin)) continue
      let tm = null
      const segs = Array.isArray(a.appointmentSegments) ? a.appointmentSegments
                 : Array.isArray(a.segments) ? a.segments : []
      if (segs[0]?.teamMemberId) tm = segs[0].teamMemberId
      if (staffId && tm && tm !== staffId) continue
      // filtra por sede
      if (tm && !isStaffAllowedInLocation(tm, locationKey)) continue
      out.push({ date:d, staffId: tm || staffId || null })
      if (out.length>=limit) break
    }
    return out
  }catch(e){ return [] }
}
async function createBooking({ startEU, locationKey, envServiceKey, durationMin, customerId, teamMemberId }){
  if (DRY_RUN) return { success:true, booking:{ id:`TEST_${Date.now()}`, __sim:true } }
  const sv = await getServiceIdAndVersion(envServiceKey)
  if (!sv?.id) return { success:false, error:"Servicio inválido" }
  const startISO = startEU.tz("UTC").toISOString()
  const idempotencyKey = stableKey({ loc:locationToId(locationKey), sv:sv.id, startISO, customerId, teamMemberId })
  try{
    const resp = await square.bookingsApi.createBooking({
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
    })
    const booking = resp?.result?.booking
    if (booking) return { success:true, booking }
    return { success:false, error:"Sin booking" }
  }catch(e){
    return { success:false, error:e?.message || "Error creando booking" }
  }
}
async function cancelBooking(bookingId){
  if (DRY_RUN) return true
  try{
    const resp = await square.bookingsApi.cancelBooking(bookingId, { idempotencyKey:`cancel_${bookingId}_${Date.now()}` })
    return !!resp?.result?.booking
  }catch(e){ return false }
}
async function searchCustomersByPhone(phone){
  try{
    const e164=normalizePhoneES(phone); if(!e164) return []
    const got = await square.customersApi.searchCustomers({ query:{ filter:{ phoneNumber:{ exact:e164 } } } })
    return got?.result?.customers || []
  }catch{ return [] }
}
async function findOrCreateCustomer({ name, email, phone }){
  const matches = await searchCustomersByPhone(phone)
  if (matches[0]) return matches[0]
  try{
    const created = await square.customersApi.createCustomer({
      idempotencyKey:`cust_${Date.now()}_${Math.random().toString(36).slice(2,6)}`,
      givenName:name||undefined,
      emailAddress:email||undefined,
      phoneNumber:normalizePhoneES(phone)||undefined
    })
    return created?.result?.customer || null
  }catch{ return null }
}

// ============== IA: CLASIFICADOR SIMPLE + DeepSeek ==============
async function classifyIntent(userText, sessionData){
  const t = norm(userText)

  // Heurística directa
  const intent = /\bcancel(ar|a|ación|acion)\b/.test(t) ? "cancel"
              : /\b(mis citas|ver citas|lista|listar)\b/.test(t) ? "list"
              : "book"

  const sede = parseSede(userText)
  const part = parsePartOfDay(userText)
  const week = parseWeekTarget(userText)
  const wday = parseWeekday(userText)
  const staffMaybe = parsePreferredStaffFromText(userText)
  const category = detectCategory(userText)

  // Si ya tenemos bastante, no llamamos IA
  if (sede || part || week || wday || staffMaybe || category) {
    return { intent, sede, part, week, wday, staff: staffMaybe, category }
  }

  // IA (DeepSeek) para extraer todo lo demás
  const sys = `Eres un parser. Devuelve SOLO JSON con este esquema:
{
  "intent": "book|cancel|list|help",
  "sede": "torremolinos|la_luz|null",
  "category": "uñas|pestañas|cejas|depilación|pedicura|tratamiento facial|tratamiento corporal|dental|null",
  "service_hint": "texto o null",
  "staff_token": "nombre o null",
  "part": "morning|afternoon|null",
  "week": "this|next|null",
  "weekday": 1..5 o null,
  "datetime_hint": "texto libre tipo 'lunes a las 17:00' o null"
}
No expliques nada.`
  const parsed = await deepseekJSON(sys, userText) || {}
  return {
    intent: parsed.intent || intent,
    sede: parsed.sede || null,
    category: parsed.category || null,
    service_hint: parsed.service_hint || null,
    staff: parsed.staff_token ? findStaffByAliasToken(norm(parsed.staff_token)) : null,
    part: parsed.part || null,
    week: parsed.week || null,
    wday: Number.isInteger(parsed.weekday) ? parsed.weekday : null,
    datetime_hint: parsed.datetime_hint || null
  }
}

// ============== WHATSAPP (Baileys) ==============
const app=express()
const PORT=process.env.PORT||8080
let lastQR=null, conectado=false

app.get("/", (_req,res)=>{
  res.send(`<!doctype html><meta charset="utf-8"><style>
  body{font-family:system-ui;display:grid;place-items:center;min-height:100vh;background:#f8f9fa}
  .card{max-width:680px;padding:32px;border-radius:20px;box-shadow:0 8px 32px rgba(0,0,0,.1);background:white}
  .status{padding:12px;border-radius:8px;margin:8px 0}
  .success{background:#d4edda;color:#155724}
  .error{background:#f8d7da;color:#721c24}
  .hint{background:#e9ecef;color:#333;border-radius:8px;padding:8px 12px;display:inline-block}
  </style><div class="card">
  <h1>🩷 Gapink Bot v33.1.0</h1>
  <div class="status ${conectado ? 'success' : 'error'}">WhatsApp: ${conectado ? "✅ Conectado" : "❌ Desconectado"}</div>
  ${!conectado&&lastQR?`<div style="text-align:center;margin:20px 0"><img src="/qr.png" width="280" style="border-radius:8px"></div>`:""}
  <div class="hint">Sede Torremolinos: ${LOC_TORRE || "—"} · La Luz: ${LOC_LUZ || "—"}</div>
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

// ============== FLUJO PRINCIPAL (ultra simple) ==============
function sayMenuServices(sedeKey, start=0){
  const { total, items } = listServicesChunked(sedeKey, start, 25)
  const lines = items.map(it=>`${it.index}) ${it.label}`).join("\n")
  const more = (start+25<total) ? `\n\nEscribe "*más*" para ver más opciones.` : ""
  return `Opciones en *${locationNice(sedeKey)}*:\n\n${lines}${more}\n\nResponde con el número.`
}
async function proposeTimes({ sede, envKey, staffId, part, week, wday }, maxOptions=3){
  const nowEU = dayjs().tz(EURO_TZ)
  let fromEU = (week==="next")
    ? nowEU.startOf("week").add(1,"day").add(7,"day").hour(OPEN.start).minute(0)
    : nextOpeningFrom(nowEU.add(NOW_MIN_OFFSET_MIN,"minute"))

  const all = await searchAvailability({
    locationKey: sede,
    envServiceKey: envKey,
    fromEU,
    days: SEARCH_WINDOW_DAYS,
    staffId,
    limit: 60
  })

  const filtered = all.filter(s => {
    if (part==="morning") return s.date.hour()>=MORNING.start && s.date.hour()<MORNING.end
    if (part==="afternoon") return s.date.hour()>=AFTERNOON.start && s.date.hour()<AFTERNOON.end
    return true
  }).filter(s => (Number.isInteger(wday)? s.date.day()===wday : true))

  return (filtered.length?filtered:all).slice(0, maxOptions)
}
function parseNumberChoice(text){
  const m = String(text||"").trim().match(/^(\d{1,3})\b/)
  return m ? parseInt(m[1],10) : null
}
function findServiceByIndex(sedeKey, idx){
  const all = servicesForSedeKeyRaw(sedeKey)
  const pick = all[idx-1]
  return pick ? { key: pick.key, label: pick.label } : null
}
async function ensureCustomerFor(phone, sessionData, sock, jid){
  // intenta encontrar o pedir datos
  const matches = await searchCustomersByPhone(phone)
  if (matches.length === 1){
    return matches[0]
  }
  await sock.sendMessage(jid, { text:"Para terminar, dime tu *nombre* y (opcional) tu *email* 😊" })
  sessionData.stage = "awaiting_identity"
  saveSession(phone, sessionData)
  return null
}

// ============== START BOT ==============
async function startBot(){
  const { makeWASocket, useMultiFileAuthState, fetchLatestBaileysVersion, Browsers } = await loadBaileys()
  if(!fs.existsSync("auth_info")) fs.mkdirSync("auth_info",{recursive:true})
  const { state, saveCreds } = await useMultiFileAuthState("auth_info")
  const { version } = await fetchLatestBaileysVersion().catch(()=>({version:[2,3000,0]}))
  const sock = makeWASocket({ logger:pino({level:"silent"}), printQRInTerminal:false, auth:state, version, browser:Browsers.macOS("Desktop"), syncFullHistory:false })
  globalThis.sock=sock

  sock.ev.on("connection.update", ({connection,qr})=>{
    if (qr){ lastQR=qr; conectado=false; try{ qrcodeTerminal.generate(qr,{small:true}) }catch{} }
    if (connection==="open"){ lastQR=null; conectado=true; }
    if (connection==="close"){ conectado=false; setTimeout(()=>{ startBot().catch(console.error) }, 1500) }
  })
  sock.ev.on("creds.update", saveCreds)

  sock.ev.on("messages.upsert", async ({messages})=>{
    const m=messages?.[0]; if (!m?.message) return
    const jid = m.key.remoteJid
    if (m.key.fromMe) return
    const phone = normalizePhoneES((jid||"").split("@")[0]||"") || (jid||"").split("@")[0]
    const textRaw = (m.message.conversation || m.message.extendedTextMessage?.text || m.message?.imageMessage?.caption || "").trim()
    if (!textRaw) return

    let s = loadSession(phone) || {
      stage: "initial",
      sede: null,
      serviceEnvKey: null,
      serviceLabel: null,
      category: null,
      preferredStaffId: null,
      preferredStaffName: null,
      part: null, week: null, wday: null,
      lastHours: null,
      listStart: 0,
      name: null, email: null
    }

    // Si estaba esperando identidad
    if (s.stage==="awaiting_identity"){
      const emailMatch = textRaw.match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i)
      const name = textRaw.replace(emailMatch?.[0]||"","").trim()
      s.name = name || s.name
      s.email = (emailMatch?.[0]||s.email) || null
      saveSession(phone,s)
      // seguimos al confirmar más tarde
    }

    // 1) Detecta profesional preferido “con X”
    const staffMaybe = parsePreferredStaffFromText(textRaw)
    if (staffMaybe){
      s.preferredStaffId = staffMaybe.id
      s.preferredStaffName = staffLabelFromId(staffMaybe.id)
    }

    // 2) IA para entender el mensaje (simple, 1 tiro)
    const cls = await classifyIntent(textRaw, s)
    if (!s.sede && cls.sede) s.sede = cls.sede
    if (!s.category && cls.category) s.category = cls.category
    if (!s.part && cls.part) s.part = cls.part
    if (!s.week && cls.week) s.week = cls.week
    if (!Number.isInteger(s.wday) && Number.isInteger(cls.wday)) s.wday = cls.wday
    if (!s.preferredStaffId && cls.staff) {
      s.preferredStaffId = cls.staff.id
      s.preferredStaffName = staffLabelFromId(cls.staff.id)
    }

    // 3) Ruta de cancel/list si procede
    if (cls.intent === "list"){
      // listar próximas citas (por teléfono)
      try{
        const e164=normalizePhoneES(phone)
        const sres=await square.customersApi.searchCustomers({ query:{ filter:{ phoneNumber:{ exact:e164 } } } })
        const cid=(sres?.result?.customers||[])[0]?.id||null
        if (!cid){ await sock.sendMessage(jid,{ text:"No encuentro citas asociadas a este número 🙈" }); return }
        const resp=await square.bookingsApi.listBookings(undefined, undefined, cid)
        const list=(resp?.result?.bookings||[]).filter(b=> b?.startAt > new Date().toISOString())
        if (!list.length){ await sock.sendMessage(jid,{ text:"No tienes citas próximas 😊 ¿Agendamos una?" }); return }
        const msg = list.map((b,i)=>{
          const d=dayjs(b.startAt).tz(EURO_TZ); const seg=(b.appointmentSegments||[{}])[0]
          return `${i+1}) ${fmtES(d)} · ${locationNice(idToLocKey(b.locationId)||"")} · ${staffLabelFromId(seg?.teamMemberId)||"Equipo"}`
        }).join("\n")
        await sock.sendMessage(jid,{ text:`Tus próximas citas:\n\n${msg}` })
        return
      }catch{ await sock.sendMessage(jid,{ text:"No pude listar ahora 😅" }); return }
    }
    if (cls.intent === "cancel"){
      try{
        const e164=normalizePhoneES(phone)
        const sres=await square.customersApi.searchCustomers({ query:{ filter:{ phoneNumber:{ exact:e164 } } } })
        const cid=(sres?.result?.customers||[])[0]?.id||null
        if (!cid){ await sock.sendMessage(jid,{ text:"No encuentro fichas para cancelar 🙈" }); return }
        const resp=await square.bookingsApi.listBookings(undefined, undefined, cid)
        const list=(resp?.result?.bookings||[]).filter(b=> b?.startAt > new Date().toISOString())
        if (!list.length){ await sock.sendMessage(jid,{ text:"No tienes citas futuras 😇" }); return }
        const msg = list.map((b,i)=>{
          const d=dayjs(b.startAt).tz(EURO_TZ)
          return `${i+1}) ${fmtES(d)} — ${b.id}`
        }).join("\n")
        await sock.sendMessage(jid,{ text:`¿Cuál cancelo? Responde con el *número*:\n\n${msg}` })
        s.stage="awaiting_cancel"
        s.cancelList = list.map((b,i)=>({ index:i+1, id:b.id, start:b.startAt }))
        saveSession(phone,s)
        return
      }catch{ await sock.sendMessage(jid,{ text:"No pude cancelar ahora 😅" }); return }
    }
    if (s.stage==="awaiting_cancel"){
      const n = parseNumberChoice(textRaw)
      const pick = (s.cancelList||[]).find(x=>x.index===n)
      if (!pick){ await sock.sendMessage(jid,{ text:"Número inválido. Prueba otra vez 😅" }); return }
      const ok = await cancelBooking(pick.id)
      await sock.sendMessage(jid,{ text: ok ? "✅ Cita cancelada" : "No pude cancelarla 😕" })
      s.stage="initial"; delete s.cancelList
      saveSession(phone,s)
      return
    }

    // 4) Sede
    if (!s.sede){
      await sock.sendMessage(jid, { text:"¿Prefieres *Torremolinos* o *La Luz*? 😊" })
      s.stage="choose_sede"; saveSession(phone,s); return
    }
    if (s.stage==="choose_sede"){
      const trySede = parseSede(textRaw)
      if (!trySede){ await sock.sendMessage(jid,{ text:"Dime *Torremolinos* o *La Luz* 🙏" }); return }
      s.sede=trySede; s.stage="initial"; saveSession(phone,s)
    }

    // 5) Servicio
    // Si hay pista de servicio por IA o texto, intenta resolverlo
    if (!s.serviceEnvKey){
      let found=null
      if (cls.service_hint){ found = findServiceFuzzy(cls.service_hint, s.sede) }
      if (!found && s.category){
        // Mostrar menú completo (por sede); simple: primer chunk
        const msg = sayMenuServices(s.sede, s.listStart)
        await sock.sendMessage(jid, { text: `Para *${locationNice(s.sede)}* te paso opciones:\n\n${msg}` })
        s.stage="choose_service"; saveSession(phone,s); return
      }
      if (!found){
        // sin categoría: lista todo (chunk)
        const msg = sayMenuServices(s.sede, s.listStart)
        await sock.sendMessage(jid, { text: msg })
        s.stage="choose_service"; saveSession(phone,s); return
      }
      s.serviceEnvKey = found.key
      s.serviceLabel  = found.label
    }

    if (s.stage==="choose_service"){
      if (/^\s*mas\b/i.test(textRaw)){ s.listStart += 25; const msg = sayMenuServices(s.sede, s.listStart); await sock.sendMessage(jid,{ text: msg }); saveSession(phone,s); return }
      const n = parseNumberChoice(textRaw)
      if (!n){ await sock.sendMessage(jid,{ text:"Responde con el *número* del servicio 😄" }); return }
      const pick = findServiceByIndex(s.sede, n)
      if (!pick){ await sock.sendMessage(jid,{ text:"Ese número no está en la lista 🙈" }); return }
      s.serviceEnvKey = pick.key
      s.serviceLabel  = pick.label
      s.stage="initial"; saveSession(phone,s)
    }

    // 6) Preferencias de hora
    if (!s.part && cls.part) s.part = cls.part
    if (!s.week && cls.week) s.week = cls.week
    if (!Number.isInteger(s.wday) && Number.isInteger(cls.wday)) s.wday = cls.wday

    if (!s.part || !s.week){
      await sock.sendMessage(jid, { text:"¿Te viene mejor *mañana* o *tarde*? ¿Y *esta semana* o *la próxima*? (También puedes decir un día: *miércoles*, *jueves*…)" })
      s.stage="set_time_prefs"; saveSession(phone,s); return
    }

    // 7) Proponer horas (2-3)
    const staffId = s.preferredStaffId && isStaffAllowedInLocation(s.preferredStaffId, s.sede) ? s.preferredStaffId : null
    const times = await proposeTimes({ sede:s.sede, envKey:s.serviceEnvKey, staffId, part:s.part, week:s.week, wday:s.wday }, 3)
    if (!times.length){
      await sock.sendMessage(jid, { text:`No veo huecos ${s.week==="next"?"la próxima semana":"esta semana"} en esa franja 😕 ¿Probamos otra franja o cambiamos de semana?` })
      s.stage="set_time_prefs"; saveSession(phone,s); return
    }
    s.lastHours = times.map(x=>x.date)
    const map = {}; for (const t of times) map[t.date.format("YYYY-MM-DDTHH:mm")] = t.staffId || null
    s.lastStaffByIso = map
    s.stage="choose_time"; saveSession(phone,s)
    const lines = enumerateHours(s.lastHours).map(h=>{
      const sid = map[h.iso]; const tag = sid ? ` — ${staffLabelFromId(sid)}` : ""
      return `${h.index}) ${h.pretty}${tag}`
    }).join("\n")
    await sock.sendMessage(jid, { text:`Tengo esto:\n${lines}\n\nResponde con el número.` })
    return

    // (continúa abajo en choose_time)
  })
  // Segunda escucha para choose_time (mismo upsert ya cubre, pero dejamos por claridad)
  sock.ev.on("messages.upsert", async ({messages})=>{
    const m=messages?.[0]; if (!m?.message) return
    const jid = m.key.remoteJid
    if (m.key.fromMe) return
    const phone = normalizePhoneES((jid||"").split("@")[0]||"") || (jid||"").split("@")[0]
    const textRaw = (m.message.conversation || m.message.extendedTextMessage?.text || m.message?.imageMessage?.caption || "").trim()
    if (!textRaw) return

    let s = loadSession(phone) || null
    if (!s) return
    if (s.stage!=="choose_time") return

    const n = parseNumberChoice(textRaw)
    if (!n){ await sock.sendMessage(jid, { text:"Elige una *opción* (1, 2 o 3) 😇" }); return }
    const pick = (s.lastHours||[])[n-1]
    if (!dayjs.isDayjs(pick)){ await sock.sendMessage(jid,{ text:"Esa opción ya no está disponible, te paso nuevas." }); s.stage="set_time_prefs"; saveSession(phone,s); return }

    const iso = pick.format("YYYY-MM-DDTHH:mm")
    let staffId = s.lastStaffByIso?.[iso] || (s.preferredStaffId && isStaffAllowedInLocation(s.preferredStaffId,s.sede) ? s.preferredStaffId : null)
    if (!staffId) staffId = pickStaffForLocation(s.sede, null)
    if (!staffId){ await sock.sendMessage(jid, { text:"No hay profesionales disponibles en esa sede 😕" }); return }

    // Cliente
    let customer = await ensureCustomerFor(phone, s, sock, jid)
    if (!customer) return // ya pedimos nombre/email

    // Si acabamos de recoger identidad
    if (s.stage==="awaiting_identity"){
      customer = await findOrCreateCustomer({ name: s.name, email: s.email, phone })
      if (!customer){ await sock.sendMessage(jid,{ text:"No pude crear tu ficha. Dime *nombre* y (opcional) *email* otra vez 🙏" }); return }
      s.stage="choose_time"; saveSession(phone,s)
    }

    const startEU = pick.clone()
    const durationMin = getDurationForEnvKey(s.serviceEnvKey, 60)
    const result = await createBooking({
      startEU, locationKey: s.sede, envServiceKey: s.serviceEnvKey,
      durationMin, customerId: customer.id, teamMemberId: staffId
    })
    if (!result.success){
      await sock.sendMessage(jid, { text:"No pude crear la reserva ahora 😅 ¿Quieres que te proponga otro horario?" })
      return
    }

    // Guardar en BD (confirmed)
    try {
      db.prepare(`INSERT INTO appointments 
        (id,customer_name,customer_phone,customer_square_id,location_key,service_env_key,service_label,duration_min,start_iso,end_iso,staff_id,status,created_at,square_booking_id,square_error,retry_count)
        VALUES (@id,@customer_name,@customer_phone,@customer_square_id,@location_key,@service_env_key,@service_label,@duration_min,@start_iso,@end_iso,@staff_id,@status,@created_at,@square_booking_id,@square_error,@retry_count)`)
        .run({
          id: result.booking.id,
          customer_name: s?.name || customer?.givenName || null,
          customer_phone: phone,
          customer_square_id: customer.id,
          location_key: s.sede,
          service_env_key: s.serviceEnvKey,
          service_label: serviceLabelFromEnvKey(s.serviceEnvKey) || s.serviceLabel || "Servicio",
          duration_min: durationMin,
          start_iso: startEU.tz("UTC").toISOString(),
          end_iso: startEU.clone().add(durationMin,"minute").tz("UTC").toISOString(),
          staff_id: staffId,
          status: "confirmed",
          created_at: new Date().toISOString(),
          square_booking_id: result.booking.id,
          square_error: null,
          retry_count: 0
        })
    } catch {}

    const staffName = staffLabelFromId(staffId) || "nuestro equipo"
    const address = s.sede === "la_luz" ? ADDRESS_LUZ : ADDRESS_TORRE
    await sock.sendMessage(jid, { text:
`🎉 ¡Cita confirmada!

📍 ${locationNice(s.sede)}
${address}

💼 ${serviceLabelFromEnvKey(s.serviceEnvKey) || s.serviceLabel}
👤 ${staffName}
🗓️ ${fmtES(startEU)}

Ref.: ${result.booking.id}

Te recordaremos por aquí. ¡Nos vemos!`
    })
    clearSession(phone)
  })
}

// ============== ARRANQUE ==============
let serverStarted = false
function safeListen(){
  if (serverStarted) return
  try{
    app.listen(PORT, ()=>{ serverStarted = true; startBot().catch(console.error) })
  }catch(e){
    console.error("Error al iniciar:", e?.message||e)
    startBot().catch(console.error)
  }
}

console.log("🩷 Gapink Bot v33.1.0 (simple)")
safeListen()

process.on("uncaughtException", (e)=>{ console.error("💥 uncaughtException:", e?.stack||e?.message||e) })
process.on("unhandledRejection", (e)=>{ console.error("💥 unhandledRejection:", e) })
process.on("SIGTERM", ()=>{ process.exit(0) })
process.on("SIGINT", ()=>{ process.exit(0) })
