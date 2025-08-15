// index.js — Gapink Nails · v13 “IA robusta • 2 locales • elección de técnica • Square-safe”

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
dayjs.extend(utc); dayjs.extend(tz)
dayjs.locale("es")

// ====== Zona/horario negocio (España)
const EURO_TZ = "Europe/Madrid"

// Horario partido L–V: 10–14 y 16–20 (S/D cerrado)
const WORK_DAYS = [1,2,3,4,5]
const SLOT_MIN = 30
const MORNING = { start: 10, end: 14 }
const AFTERNOON = { start: 16, end: 20 }
const NOW_MIN_OFFSET_MIN = Number(process.env.BOT_NOW_OFFSET_MIN || 30)

// Festivos dd/mm (local + nacionales)
const HOLIDAYS_EXTRA = (process.env.HOLIDAYS_EXTRA || "06/01,28/02,15/08,12/10,01/11,06/12,08/12,25/12")
  .split(",").map(s => s.trim()).filter(Boolean)

// ====== OpenAI (IA para extracción ultra-cauta de nombre/email)
const OPENAI_API_KEY = process.env.OPENAI_API_KEY || ""
const OPENAI_MODEL   = process.env.OPENAI_MODEL || "gpt-4o-mini"
const OPENAI_API_URL = process.env.OPENAI_API_URL || "https://api.openai.com/v1/chat/completions"

async function aiChat(messages, { temperature=0.2 } = {}) {
  if (!OPENAI_API_KEY) return ""
  try {
    const r = await fetch(OPENAI_API_URL, {
      method: "POST",
      headers: { "Content-Type":"application/json", "Authorization":`Bearer ${OPENAI_API_KEY}` },
      body: JSON.stringify({ model: OPENAI_MODEL, messages, temperature })
    })
    if (!r.ok) throw new Error(`OpenAI ${r.status}`)
    const j = await r.json()
    return (j?.choices?.[0]?.message?.content || "").trim()
  } catch { return "" }
}

// ====== Square
const square = new Client({
  accessToken: process.env.SQUARE_ACCESS_TOKEN,
  environment: (process.env.SQUARE_ENV==="production") ? Environment.Production : Environment.Sandbox
})

// IDs de local
const LOC_TORRE = (process.env.SQUARE_LOCATION_ID || "").trim()
const LOC_LUZ   = (process.env.SQUARE_LOCATION_ID_LA_LUZ || "").trim()

// Textos de dirección
const ADDRESS_TORRE = process.env.ADDRESS_TORREMOLINOS || "Av. de Benyamina 18, Torremolinos"
const ADDRESS_LUZ   = process.env.ADDRESS_LA_LUZ || "Málaga – Barrio de La Luz"

// ====== Helpers
const onlyDigits = s => String(s||"").replace(/\D+/g,"")
const rm = s => String(s||"").normalize("NFD").replace(/\p{Diacritic}/gu, "")
const norm = s => rm(s).toLowerCase()
  .replace(/[+.,;:()/_-]/g, " ")
  .replace(/[^\p{Letter}\p{Number}\s]/gu, " ")
  .replace(/\s+/g, " ").trim()

function normalizePhoneES(raw){
  const d=onlyDigits(raw); if(!d) return null
  if (raw.startsWith("+") && d.length>=8 && d.length<=15) return `+${d}`
  if (d.startsWith("34") && d.length===11) return `+${d}`
  if (d.length===9) return `+34${d}`
  if (d.startsWith("00")) return `+${d.slice(2)}`
  return `+${d}`
}

const YES_RE = /\b(s[ií]|ok|vale|okey|okay|va|genial|de acuerdo|confirmo|perfecto|si claro|sí claro|yes)\b/i
const NO_RE  = /\b(no|otra|cambia|cambiar|cancel|cancela|anula|mejor mas tarde|mejor más tarde)\b/i

// Detección local
function detectLocation(text){
  const t=norm(text)
  if (/\b(luz|malaga|málaga)\b/.test(t)) return "luz"
  if (/\b(torre|torremolinos)\b/.test(t)) return "torre"
  return null
}
function locationToId(key){ return key==="luz" ? LOC_LUZ : LOC_TORRE }
function locationToAddress(key){ return key==="luz" ? ADDRESS_LUZ : ADDRESS_TORRE }
function locationNice(key){ return key==="luz" ? "Málaga – La Luz" : "Torremolinos" }

// Festivos y horario
function isHolidayEU(d){
  const dd = String(d.date()).padStart(2,"0")
  const mm = String(d.month()+1).padStart(2,"0")
  return HOLIDAYS_EXTRA.includes(`${dd}/${mm}`)
}
function insideBusinessBlock(d, block){ return d.hour()>=block.start && d.hour()<block.end }
function insideBusinessHours(d, durMin){
  const t=d.clone()
  if (!WORK_DAYS.includes(t.day())) return false
  if (isHolidayEU(t)) return false
  const end=t.clone().add(durMin,"minute")
  const inMorning = insideBusinessBlock(t, MORNING) && insideBusinessBlock(end, MORNING) && t.isSame(end,"day")
  const inAfternoon = insideBusinessBlock(t, AFTERNOON) && insideBusinessBlock(end, AFTERNOON) && t.isSame(end,"day")
  return inMorning || inAfternoon
}
function ceilToSlotEU(t){
  const m=t.minute(), rem=m%SLOT_MIN
  return rem===0 ? t.second(0).millisecond(0) : t.add(SLOT_MIN-rem,"minute").second(0).millisecond(0)
}
function nextOpeningFrom(d){
  let t=d.clone()
  if (t.hour()>=AFTERNOON.end) t = t.add(1,"day").hour(MORNING.start).minute(0).second(0).millisecond(0)
  else if (t.hour()>=MORNING.end && t.hour()<AFTERNOON.start) t = t.hour(AFTERNOON.start).minute(0).second(0).millisecond(0)
  else if (t.hour()<MORNING.start) t = t.hour(MORNING.start).minute(0).second(0).millisecond(0)
  while (!WORK_DAYS.includes(t.day()) || isHolidayEU(t)) {
    t=t.add(1,"day").hour(MORNING.start).minute(0).second(0).millisecond(0)
  }
  return t
}

// ====== DB mínima
const db=new Database("gapink.db");db.pragma("journal_mode = WAL")
db.exec(`
CREATE TABLE IF NOT EXISTS appointments (
  id TEXT PRIMARY KEY,
  customer_name TEXT,
  customer_phone TEXT,
  customer_square_id TEXT,
  location_key TEXT,
  service_key TEXT,
  service_name TEXT,
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
CREATE INDEX IF NOT EXISTS idx_apt_loc_time ON appointments(location_key, start_iso);
`)
const insertAppt = db.prepare(`INSERT INTO appointments
(id, customer_name, customer_phone, customer_square_id, location_key, service_key, service_name, duration_min, start_iso, end_iso, staff_id, status, created_at, square_booking_id)
VALUES (@id,@customer_name,@customer_phone,@customer_square_id,@location_key,@service_key,@service_name,@duration_min,@start_iso,@end_iso,@staff_id,@status,@created_at,@square_booking_id)`)
const updateApptStatus = db.prepare(`UPDATE appointments SET status=@status, square_booking_id=@square_booking_id WHERE id=@id`)

function loadSession(phone){
  const row = db.prepare(`SELECT data_json FROM sessions WHERE phone=@phone`).get({phone})
  if (!row?.data_json) return null
  const json = JSON.parse(row.data_json)
  if (json.startEU_ms) json.startEU = dayjs.tz(json.startEU_ms, EURO_TZ)
  if (json.selectedStartEU_ms) json.selectedStartEU = dayjs.tz(json.selectedStartEU_ms, EURO_TZ)
  return json
}
function saveSession(phone, data){
  const d={...data}
  d.startEU_ms = data.startEU?.valueOf?.() ?? data.startEU_ms ?? null
  d.selectedStartEU_ms = data.selectedStartEU?.valueOf?.() ?? data.selectedStartEU_ms ?? null
  delete d.startEU; delete d.selectedStartEU
  const j = JSON.stringify(d)
  const upd = db.prepare(`UPDATE sessions SET data_json=@data_json, updated_at=@updated_at WHERE phone=@phone`)
  const res = upd.run({ phone, data_json:j, updated_at:new Date().toISOString() })
  if (res.changes===0) {
    db.prepare(`INSERT INTO sessions (phone, data_json, updated_at) VALUES (@phone,@data_json,@updated_at)`)
      .run({ phone, data_json:j, updated_at:new Date().toISOString() })
  }
}
function clearSession(phone){ db.prepare(`DELETE FROM sessions WHERE phone=@phone`).run({phone}) }

// ====== Empleadas (dinámico por .env “ID|BOOKABLE|LOCS”), con etiquetas de nombre
function deriveLabelsFromEnvKey(envKey){
  // SQ_EMP_DESI_DESI -> ["desi"]
  // SQ_EMP_ROCIO_CHICA_ROCIO -> ["rocio","chica"]
  const raw = envKey.replace(/^SQ_EMP_/, "")
  const toks = raw.split("_").map(t=>norm(t)).filter(t=>t && t!=="sq" && t!=="emp")
  const uniq = Array.from(new Set(toks))
  // elige etiquetas útiles (1 y 2 tokens)
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

function detectPreferredStaff(text, locKey){
  const t = norm(text)
  const locId = locationToId(locKey)
  // match por cualquier etiqueta
  let cand = null
  for (const e of EMPLOYEES){
    if (e.labels.some(lbl => t.includes(lbl))){
      cand = e; break
    }
  }
  if (!cand) return null
  // si no es bookable o no trabaja en ese local, lo marcamos “preferido” pero no forzamos
  if (!cand.bookable || !(cand.allow.includes("ALL") || cand.allow.includes(locId))) {
    return { id: null, preferId: cand.id, preferLabel: (cand.labels[0]||"") }
  }
  return { id: cand.id, preferId: cand.id, preferLabel: (cand.labels[0]||"") }
}

function pickStaffForLocation(locKey, preferId=null){
  const locId = locationToId(locKey)
  // primero, si preferId es válido-bookable
  if (preferId){
    const e = EMPLOYEES.find(x=>x.id===preferId)
    if (e && e.bookable && (e.allow.includes("ALL") || e.allow.includes(locId))) return e.id
  }
  // si no, cualquiera bookable del local
  const found = EMPLOYEES.find(e=>e.bookable && (e.allow.includes("ALL") || e.allow.includes(locId)))
  return found?.id || null
}

// ====== Servicios
const SVC = {
  // pestañas
  LIFITNG_DE_PESTANAS_Y_TINTE: { name:"Lifting de pestañas y tinte", dur:60 },
  EXTENSIONES_DE_PESTANAS_NUEVAS_PELO_A_PELO: { name:"Extensiones nuevas pelo a pelo", dur:120 },
  EXTENSIONES_PESTANAS_NUEVAS_2D: { name:"Extensiones nuevas 2D", dur:120 },
  EXTENSIONES_PESTANAS_NUEVAS_3D: { name:"Extensiones nuevas 3D", dur:120 },
  RELLENO_EXTENSIONES_PESTANAS_PELO_A_PELO: { name:"Relleno pestañas pelo a pelo", dur:90 },
  RELLENO_PESTANAS_2D: { name:"Relleno pestañas 2D", dur:90 },
  RELLENO_PESTANAS_3D: { name:"Relleno pestañas 3D", dur:90 },
  QUITAR_EXTENSIONES_PESTANAS: { name:"Quitar extensiones de pestañas", dur:30 },

  // uñas rápidas
  MANICURA_SEMIPERMANENTE: { name:"Manicura semipermanente", dur:30 },
  MANICURA_SEMIPERMANENTE_QUITAR: { name:"Manicura semipermanente + quitar", dur:40 },
  MANICURA_CON_ESMALTE_NORMAL: { name:"Manicura con esmalte normal", dur:30 },
  PEDICURA_SPA_CON_ESMALTE_SEMIPERMANENTE: { name:"Pedicura spa (semipermanente)", dur:60 },
}
const SERVICE_KEYS = Object.keys(SVC)

const SVC_SYNON = [
  // pestañas
  ["lifting tinte", "LIFITNG_DE_PESTANAS_Y_TINTE", ["lifting + tinte","lifting y tinte","lash lift","lifting pestañas"]],
  ["pelo a pelo", "EXTENSIONES_DE_PESTANAS_NUEVAS_PELO_A_PELO", ["clasicas","clásicas","classic"]],
  ["extensiones 2d", "EXTENSIONES_PESTANAS_NUEVAS_2D", ["2 d"]],
  ["extensiones 3d", "EXTENSIONES_PESTANAS_NUEVAS_3D", ["3 d"]],
  ["relleno pelo a pelo", "RELLENO_EXTENSIONES_PESTANAS_PELO_A_PELO", []],
  ["relleno 2d", "RELLENO_PESTANAS_2D", []],
  ["relleno 3d", "RELLENO_PESTANAS_3D", []],
  ["quitar extensiones", "QUITAR_EXTENSIONES_PESTANAS", ["retirar extensiones","retirar pestañas","quitar pestañas"]],
  // uñas
  ["manicura semipermanente", "MANICURA_SEMIPERMANENTE", ["semi"]],
  ["manicura semipermanente quitar", "MANICURA_SEMIPERMANENTE_QUITAR", ["retirar semi","remove gel"]],
  ["manicura normal", "MANICURA_CON_ESMALTE_NORMAL", []],
  ["pedicura semipermanente", "PEDICURA_SPA_CON_ESMALTE_SEMIPERMANENTE", []],
]

function detectService(text){
  const t=norm(text)
  if (/\blifting\b/.test(t) && (/\btinte\b/.test(t) || /lash\s*lift/.test(t))) {
    return "LIFITNG_DE_PESTANAS_Y_TINTE"
  }
  for (const [label,key,extra] of SVC_SYNON) {
    if (t.includes(norm(label)) || (extra||[]).some(x => t.includes(norm(x)))) return key
  }
  // fuzzy simple
  const tokens=t.split(/\s+/).filter(Boolean)
  let best=null, scoreBest=0
  for (const k of SERVICE_KEYS) {
    const words = norm(SVC[k].name).split(/\s+/)
    let score=0; for(const tok of tokens){ if (words.includes(tok)) score++ }
    if (score>scoreBest){ scoreBest=score; best=k }
  }
  return scoreBest>=2 ? best : null
}

// Env de servicio por local
function svcEnvName(key, locKey){
  return locKey==="luz" ? `SQ_SVC_luz_${key}` : `SQ_SVC_${key}`
}
function pickServiceEnvPair(key, locKey){
  const envName = svcEnvName(key, locKey)
  const raw = process.env[envName]
  if (!raw) return null
  const [id, versionStr] = String(raw).split("|")
  return { id, version: versionStr ? Number(versionStr) : undefined, duration: SVC[key]?.dur ?? 60, name: SVC[key]?.name || key }
}
async function getServiceVariationVersion(id){
  try{
    const resp = await square.catalogApi.retrieveCatalogObject(id, true)
    return resp?.result?.object?.version
  }catch{ return undefined }
}
function stableKey(parts){
  const raw = Object.values(parts).join("|")
  return createHash("sha256").update(raw).digest("hex").slice(0,48)
}

// ====== Bookings Square
async function createSquareBooking({ startEU, serviceKey, locationKey, customerId, teamMemberId }){
  try{
    const pair = pickServiceEnvPair(serviceKey, locationKey)
    if (!pair?.id || !locationToId(locationKey) || !teamMemberId) return null
    const version = pair.version || await getServiceVariationVersion(pair.id)
    if (!version) return null
    const startISO = startEU.tz("UTC").toISOString()
    const idempotencyKey = stableKey({loc:locationToId(locationKey), sv:pair.id, startISO, customerId, teamMemberId})
    const body = {
      idempotencyKey,
      booking: {
        locationId: locationToId(locationKey),
        startAt: startISO,
        customerId,
        appointmentSegments: [{
          teamMemberId,
          serviceVariationId: pair.id,
          serviceVariationVersion: Number(version),
          durationMinutes: pair.duration
        }]
      }
    }
    const resp = await square.bookingsApi.createBooking(body)
    return resp?.result?.booking || null
  }catch(e){ console.error("createSquareBooking:", e?.message||e); return null }
}

// ====== Clientes Square
function isValidEmail(e){ return /^[^\s@]+@[^\s@]+\.[^\s@]{2,}$/.test(String(e||"").trim()) }
async function squareFindCustomerByPhone(phoneRaw){
  try{
    const e164=normalizePhoneES(phoneRaw); if(!e164) return null
    const resp=await square.customersApi.searchCustomers({ query:{ filter:{ phoneNumber:{ exact:e164 } } } })
    return (resp?.result?.customers||[])[0]||null
  }catch{ return null }
}
async function squareCreateCustomer({ givenName, emailAddress, phoneNumber }){
  try{
    if (!isValidEmail(emailAddress)) return null
    const resp=await square.customersApi.createCustomer({
      idempotencyKey:`cust_${Date.now()}_${Math.random().toString(36).slice(2,8)}`,
      givenName, emailAddress, phoneNumber
    })
    return resp?.result?.customer||null
  }catch(e){ console.error("squareCreateCustomer:", e?.message||e); return null }
}

// ====== Propuestas de hora (no availability)
function fmtES(d){
  const dias=["domingo","lunes","martes","miércoles","jueves","viernes","sábado"]
  const t=(dayjs.isDayjs(d)?d:dayjs(d)).tz(EURO_TZ)
  const DD=String(t.date()).padStart(2,"0")
  const MM=String(t.month()+1).padStart(2,"0")
  const HH=String(t.hour()).padStart(2,"0")
  const mm=String(t.minute()).padStart(2,"0")
  return `${dias[t.day()]} ${DD}/${MM} ${HH}:${mm}`
}
function proposeSlots({ fromEU, durationMin, n=3 }){
  const out=[]
  let t = ceilToSlotEU(fromEU.clone())
  t = nextOpeningFrom(t)
  while (out.length<n){
    const end=t.clone().add(durationMin,"minute")
    if (insideBusinessHours(t,durationMin)){
      out.push(t.clone())
      t = t.add(SLOT_MIN,"minute")
    } else {
      if (t.hour()>=AFTERNOON.end) t = t.add(1,"day").hour(MORNING.start).minute(0)
      else if (t.hour()>=MORNING.end && t.hour()<AFTERNOON.start) t = t.hour(AFTERNOON.start).minute(0)
      else t=t.add(SLOT_MIN,"minute")
      while (!WORK_DAYS.includes(t.day()) || isHolidayEU(t)) {
        t=t.add(1,"day").hour(MORNING.start).minute(0)
      }
    }
  }
  return out
}

// ====== Parser de fecha/hora simple
const DOW = { domingo:0,lunes:1,martes:2,miércoles:3,miercoles:3,jueves:4,viernes:5,sábado:6,sabado:6 }
const WHEN = { hoy:0, today:0, mañana:1, manana:1, tomorrow:1 }
function parseDateTimeEU(text){
  const t = norm(text)
  const m = t.match(/\b(\d{1,2})[\/\-](\d{1,2})(?:[\/\-](\d{2,4}))?\s+(\d{1,2})(?::(\d{2}))?\b/)
  if (m){
    let dd=+m[1], mm=+m[2], yy=m[3]?+m[3]:dayjs().tz(EURO_TZ).year()
    if (yy<100) yy+=2000
    const hh=+m[4], mi=m[5]?+m[5]:0
    let dt=dayjs.tz(`${yy}-${String(mm).padStart(2,"0")}-${String(dd).padStart(2,"0")} ${String(hh).padStart(2,"0")}:${String(mi).padStart(2,"0")}`, EURO_TZ)
    const guard = dayjs().tz(EURO_TZ).add(NOW_MIN_OFFSET_MIN,"minute")
    if (dt.isBefore(guard)) dt = guard
    return dt
  }
  const hm = t.match(/\b(\d{1,2})(?::(\d{2}))?\b/)
  const w = Object.keys(WHEN).find(k=>t.includes(k))
  if (w && hm){
    const base = dayjs().tz(EURO_TZ).add(WHEN[w],"day").startOf("day")
    let dt = base.hour(+hm[1]).minute(hm[2]?+hm[2]:0)
    const guard = dayjs().tz(EURO_TZ).add(NOW_MIN_OFFSET_MIN,"minute")
    if (dt.isBefore(guard)) dt = guard
    return dt
  }
  for (const [name, dow] of Object.entries(DOW)){
    if (t.includes(name) && hm){
      const now = dayjs().tz(EURO_TZ)
      let dd = now.startOf("day")
      let delta = (dow - dd.day() + 7) % 7
      if (delta===0 && now.hour()>=AFTERNOON.end) delta=7
      dd = dd.add(delta,"day")
      let dt = dd.hour(+hm[1]).minute(hm[2]?+hm[2]:0)
      const guard = dayjs().tz(EURO_TZ).add(NOW_MIN_OFFSET_MIN,"minute")
      if (dt.isBefore(guard)) dt = guard
      return dt
    }
  }
  return null
}

// ====== Selección por “el primero / el segundo / 10:00 / 1”
function parseChoiceIndex(text){
  const t = norm(text)
  // ordinales
  if (/\b(primero|primera|1ro|1ª|1a|1º)\b/.test(t)) return 0
  if (/\b(segundo|segunda|2do|2ª|2a|2º)\b/.test(t)) return 1
  if (/\b(tercero|tercera|3ro|3ª|3a|3º)\b/.test(t)) return 2
  // número suelto 1/2/3
  const n = t.match(/\b([123])\b/); if (n) return Number(n[1])-1
  return null
}
function parseTimeAgainstOffers(text, offers){
  if (!offers?.length) return null
  const t = norm(text)
  const hm = t.match(/\b(\d{1,2})(?::(\d{2}))?\b/)
  if (!hm) return null
  const hh = +hm[1], mi = hm[2]?+hm[2]:0
  const target = `${String(hh).padStart(2,"0")}:${String(mi).padStart(2,"0")}`
  const hit = offers.find(o => o.format("HH:mm")===target)
  return hit || null
}

// ====== Mensajes tipo
const WELCOME_MSG =
`Gracias por comunicarte con Gapink Nails. Por favor, haznos saber cómo podemos ayudarte.

Solo atenderemos por WhatsApp y llamadas en horario de lunes a viernes de 10 a 14:00 y de 16:00 a 20:00

Si quieres reservar una cita puedes hacerlo a través de este link:

https://gapinknails.square.site/

Y si quieres modificarla puedes hacerlo a través del link del sms que llega con su cita.

Para cualquier otra consulta, déjenos saber y en el horario establecido le responderemos.
Gracias 😘`

const OOH_MSG = (link) =>
`Ahora estamos fuera de horario. Si necesitas una cita, dímela y te la gestiono igual 😉 (o usa ${link}).`

const ASK_LOC_AND_SVC =
"¿En qué salón te viene mejor, Málaga – La Luz o Torremolinos?\n\n¿Qué servicio necesitas? (ej.: “manicura semipermanente”, “Extensiones 2D”, “Lifting + tinte”)."

const ASK_LASH = (haveLoc)=>
`${haveLoc ? "" : "¿En qué salón te viene mejor, *Málaga – La Luz* o *Torremolinos*?\n\n"}¿Qué servicio de *pestañas* necesitas?
• *Lifting + tinte*
• *Extensiones nuevas*: pelo a pelo (clásicas) / 2D / 3D
• *Relleno*: pelo a pelo / 2D / 3D
• *Quitar* extensiones

Escribe por ejemplo: "Extensiones 2D", "Relleno pelo a pelo" o "Lifting + tinte".`

// ====== Mini-web
const app=express()
const PORT=process.env.PORT||8080
let lastQR=null, conectado=false, sock=null

app.get("/", (_req,res)=>{
  res.send(`<!doctype html><meta charset="utf-8"><style>
  body{font-family:system-ui;display:grid;place-items:center;min-height:100vh}
  .card{max-width:560px;padding:24px;border-radius:16px;box-shadow:0 6px 24px rgba(0,0,0,.08)}
  </style><div class="card"><h1>Gapink Nails</h1>
  <p>Estado: ${conectado?"✅ Conectado":"❌ Desconectado"}</p>
  ${!conectado&&lastQR?`<img src="/qr.png" width="300">`:""}
  </div>`)
})
app.get("/qr.png", async (_req,res)=>{
  if(!lastQR) return res.status(404).send("No QR")
  const png = await qrcode.toBuffer(lastQR, { type:"png", width:512, margin:1 })
  res.set("Content-Type","image/png").send(png)
})

app.listen(PORT, ()=>{ console.log("🌐 Web", PORT); startBot().catch(console.error) })

// ====== Carga ROBUSTA de Baileys (ESM/CJS)
async function loadBaileys(){
  const require = createRequire(import.meta.url)
  let mod = null
  try { mod = require("@whiskeysockets/baileys") } catch {}
  if (!mod) {
    try { mod = await import("@whiskeysockets/baileys") } catch {}
  }
  if (!mod) throw new Error("No se pudo cargar @whiskeysockets/baileys")

  const makeWASocket =
    mod.makeWASocket ||
    mod.default?.makeWASocket ||
    (typeof mod.default === "function" ? mod.default : undefined)

  const useMultiFileAuthState =
    mod.useMultiFileAuthState ||
    mod.default?.useMultiFileAuthState

  const fetchLatestBaileysVersion =
    mod.fetchLatestBaileysVersion ||
    mod.default?.fetchLatestBaileysVersion ||
    (async()=>({ version:[2,3000,0] }))

  const Browsers =
    mod.Browsers ||
    mod.default?.Browsers || { macOS:(n="Desktop")=>["MacOS",n,"121.0.0"] }

  if (typeof makeWASocket !== "function" || typeof useMultiFileAuthState !== "function") {
    throw new Error("Baileys incompatible. Instala @whiskeysockets/baileys ^6.x")
  }
  return { makeWASocket, useMultiFileAuthState, fetchLatestBaileysVersion, Browsers }
}

async function startBot(){
  try{
    const { makeWASocket, useMultiFileAuthState, fetchLatestBaileysVersion, Browsers } = await loadBaileys()
    if(!fs.existsSync("auth_info")) fs.mkdirSync("auth_info",{recursive:true})
    const { state, saveCreds } = await useMultiFileAuthState("auth_info")
    const { version } = await fetchLatestBaileysVersion().catch(()=>({version:[2,3000,0]}))

    sock = makeWASocket({
      logger: pino({ level:"silent" }),
      printQRInTerminal: false,
      auth: state,
      version,
      browser: Browsers.macOS("Desktop"),
      syncFullHistory: false
    })

    sock.ev.on("connection.update", ({connection, qr})=>{
      if (qr){ lastQR=qr; conectado=false; try{ qrcodeTerminal.generate(qr,{small:true}) }catch{} }
      if (connection==="open"){ lastQR=null; conectado=true; console.log("✅ WhatsApp listo") }
      if (connection==="close"){ conectado=false; console.log("❌ Conexión cerrada. Reintentando…"); setTimeout(()=>startBot().catch(console.error), 2500) }
    })
    sock.ev.on("creds.update", saveCreds)

    sock.ev.on("messages.upsert", async ({messages})=>{
      const m=messages?.[0]; if (!m?.message || m.key.fromMe) return
      const from = m.key.remoteJid
      const phone = normalizePhoneES((from||"").split("@")[0]||"") || (from||"").split("@")[0]
      const body  = m.message.conversation || m.message.extendedTextMessage?.text || m.message?.imageMessage?.caption || ""
      const text  = (body||"").trim()
      const low   = norm(text)

      let s = loadSession(phone) || {
        greeted:false, lastOOHDay:null,
        locationKey:null, serviceKey:null, durationMin:null,
        lastOptions:[], pendingConfirm:false,
        selectedStartEU:null, preferredStaffId:null, preferredStaffLabel:null,
        name:null, email:null, awaitingName:false, awaitingEmail:false
      }

      // ===== Bienvenida (una vez)
      if (!s.greeted){
        await sock.sendMessage(from, { text: WELCOME_MSG })
        s.greeted=true; saveSession(phone,s)
      }

      // ===== Fuera de horario ⇒ aviso 1 vez/día
      const nowEU = dayjs().tz(EURO_TZ)
      const isOpenNow = insideBusinessHours(nowEU.clone(), 15)
      const todayKey = nowEU.format("YYYY-MM-DD")
      if (!isOpenNow && s.lastOOHDay !== todayKey){
        await sock.sendMessage(from, { text: OOH_MSG("https://gapinknails.square.site/") })
        s.lastOOHDay = todayKey; saveSession(phone,s)
      }

      // ===== Si estamos esperando NOMBRE/EMAIL, captura directa y sigue al cierre
      if (s.awaitingName){
        const candidate = text.replace(/\s+/g," ").trim()
        if (candidate.length>=3){
          s.name = candidate; s.awaitingName=false; saveSession(phone,s)
        } else {
          await sock.sendMessage(from,{ text:"Dime tu *nombre y apellidos* (por ejemplo: Ana López)." })
          return
        }
      } else if (s.awaitingEmail){
        if (isValidEmail(text)){
          s.email = text.trim(); s.awaitingEmail=false; saveSession(phone,s)
        } else {
          await sock.sendMessage(from,{ text:"Necesito un email válido (ej.: nombre@correo.com)." })
          return
        }
      }

      // ===== Captura LOCAL + SERVICIO + PREFERENCIA DE STAFF (en cualquier momento)
      const locTxt = detectLocation(text); if (locTxt) s.locationKey = locTxt
      const svcTxt = detectService(text);  if (svcTxt) { s.serviceKey = svcTxt; s.durationMin = SVC[svcTxt]?.dur || 60 }
      const staffPref = detectPreferredStaff(text, s.locationKey || "torre")
      if (staffPref){
        s.preferredStaffId = staffPref.preferId || s.preferredStaffId
        s.preferredStaffLabel = staffPref.preferLabel || s.preferredStaffLabel
      }

      // ===== Menú pestañas si hace falta
      const mentionsLash = /\bpesta(?:n|ñ)as\b/.test(low) || /lash/.test(low) || /lifting/.test(low)
      if (mentionsLash && !s.serviceKey){
        await sock.sendMessage(from, { text: ASK_LASH(!!s.locationKey) })
        saveSession(phone,s); return
      }

      // ===== Falta info base
      if (!s.locationKey && !s.serviceKey){
        await sock.sendMessage(from, { text: ASK_LOC_AND_SVC })
        saveSession(phone,s); return
      }
      if (!s.locationKey && s.serviceKey){
        await sock.sendMessage(from, { text:"¿En qué salón te viene mejor, Málaga – La Luz o Torremolinos?" })
        saveSession(phone,s); return
      }
      if (s.locationKey && !s.serviceKey){
        const msg = mentionsLash ? ASK_LASH(true) : "¿Qué servicio necesitas? (ej.: “manicura semipermanente”, “Extensiones 2D”, “Lifting + tinte”)."
        await sock.sendMessage(from, { text: msg })
        saveSession(phone,s); return
      }

      // ===== Si ya dimos opciones: entiende “el primero / a las 10 / 1 / con desi”
      let picked = null
      if (s.lastOptions?.length){
        const idx = parseChoiceIndex(text)
        if (idx!=null && s.lastOptions[idx]) picked = s.lastOptions[idx]
        if (!picked) picked = parseTimeAgainstOffers(text, s.lastOptions)
        const saysYes = YES_RE.test(text)
        if (!picked && saysYes) picked = s.lastOptions[0]
        if (picked){
          s.selectedStartEU = picked
          // si vuelve a decir “con X” ahora, ya lo capturamos arriba
          saveSession(phone,s)
        }
      }

      // ===== Si no hemos propuesto aún o no hay selección, proponemos
      if (!s.selectedStartEU){
        // ¿parsing directo de “a las 10:00” sin día? -> intenta casar con ofertas
        const parsed = parseDateTimeEU(text)
        if (parsed){
          s.selectedStartEU = ceilToSlotEU(parsed)
          saveSession(phone,s)
        }
      }

      if (!s.selectedStartEU){
        // Proponer huecos a partir de ahora
        const base = dayjs().tz(EURO_TZ).add(NOW_MIN_OFFSET_MIN,"minute")
        const fromEU = nextOpeningFrom(base)
        const opts = proposeSlots({ fromEU, durationMin: s.durationMin, n: 3 })
        s.lastOptions = opts
        s.pendingConfirm = true
        const list = opts.map(x=>`• ${fmtES(x)}`).join("\n")
        await sock.sendMessage(from, { text:
`Tengo estos huecos para *${SVC[s.serviceKey].name}* en *${locationNice(s.locationKey)}*:
${list}

¿Te viene bien el primero? Si prefieres otro día/hora, dímelo.${s.preferredStaffLabel?`\nSi puedo, te lo asigno con *${s.preferredStaffLabel}*.`:""}` })
        saveSession(phone,s); return
      }

      // ===== En este punto hay selección (p. ej. “el primero” o “a las 10, con desi”)
      // Comprobamos nombre/email o cliente existente
      let customer = await squareFindCustomerByPhone(phone)
      if (!customer){
        if (!s.name){
          s.awaitingName = true; saveSession(phone,s)
          await sock.sendMessage(from,{ text:"Para cerrar, dime tu *nombre y apellidos*." })
          return
        }
        if (!s.email){
          // Intento IA ultra-cauta (no responde si duda)
          const guess = await aiChat([
            { role:"system", content:"Extrae de forma ultra-cauta nombre y email si aparecen. Responde SOLO JSON con {\"name\":\"\",\"email\":\"\"}. Si dudas, deja vacío." },
            { role:"user", content:text }
          ])
          try {
            const j = JSON.parse(guess)
            if (!s.email && j?.email && isValidEmail(j.email)) s.email = j.email
          } catch {}
        }
        if (!s.email){
          s.awaitingEmail = true; saveSession(phone,s)
          await sock.sendMessage(from,{ text:"Genial. Ahora tu email (tipo: nombre@correo.com)." })
          return
        }
        // crear cliente
        customer = await squareCreateCustomer({ givenName: s.name, emailAddress: s.email, phoneNumber: phone })
        if (!customer){
          await sock.sendMessage(from,{ text:"Ese email no me funciona 🤕. Mándame uno válido y sigo." })
          return
        }
      }

      // Staff final (respeta preferencia si es válida en ese local)
      const teamId = pickStaffForLocation(s.locationKey, s.preferredStaffId)
      if (!teamId){
        await sock.sendMessage(from,{ text:"Ahora mismo no puedo asignar profesional para ese salón. ¿Te da igual con quién?" })
        return
      }

      const startEU = ceilToSlotEU(s.selectedStartEU.clone())
      if (!insideBusinessHours(startEU, s.durationMin)){
        s.selectedStartEU = null
        await sock.sendMessage(from,{ text:"Esa hora cae fuera de horario. Dime otra dentro de L–V 10–14 o 16–20." })
        saveSession(phone,s); return
      }

      // Guardamos y creamos en Square
      const startUTC = startEU.tz("UTC"), endUTC = startUTC.clone().add(s.durationMin,"minute")
      const aptId = `apt_${Math.random().toString(36).slice(2,8)}${Date.now().toString(36).slice(-4)}`
      insertAppt.run({
        id:aptId, customer_name:s.name || customer?.givenName || null, customer_phone:phone, customer_square_id:customer.id,
        location_key:s.locationKey, service_key:s.serviceKey, service_name:SVC[s.serviceKey].name, duration_min:s.durationMin,
        start_iso:startUTC.toISOString(), end_iso:endUTC.toISOString(), staff_id:teamId, status:"pending",
        created_at:new Date().toISOString(), square_booking_id:null
      })

      const sq = await createSquareBooking({
        startEU, serviceKey:s.serviceKey, locationKey:s.locationKey, customerId:customer.id, teamMemberId:teamId
      })
      if (!sq){
        db.prepare(`DELETE FROM appointments WHERE id=@id`).run({id:aptId})
        s.pendingConfirm=false; s.selectedStartEU=null
        await sock.sendMessage(from,{ text:"No pude reservar ese hueco. Dime otra hora/día y lo intento de nuevo, o usa el link: https://gapinknails.square.site/" })
        saveSession(phone,s); return
      }

      updateApptStatus.run({ id:aptId, status:"confirmed", square_booking_id: sq.id || null })
      await sock.sendMessage(from,{ text:
`Reserva confirmada 🎉
Salón: ${locationNice(s.locationKey)} (${locationToAddress(s.locationKey)})
Servicio: ${SVC[s.serviceKey].name}${s.preferredStaffLabel?`\nProfesional: ${s.preferredStaffLabel} (si está disponible)`:``}
Fecha: ${fmtES(startEU)}
Duración: ${s.durationMin} min
Pago en persona.` })

      clearSession(phone)
    })
  }catch(e){
    console.error("startBot:", e?.message||e)
  }
}
