// index.js — Gapink Nails · PROD (v4 “hora exacta, cancelación Square fix, reoferta amable”)

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

if (!globalThis.crypto) globalThis.crypto = webcrypto
dayjs.extend(utc); dayjs.extend(tz); dayjs.locale("es")
const EURO_TZ = "Europe/Madrid"

const { makeWASocket, useMultiFileAuthState, fetchLatestBaileysVersion, Browsers } = baileys

// ===== Config negocio
const WORK_DAYS = [1,2,3,4,5,6]  // L-S (domingo cerrado)
const OPEN_HOUR  = 10
const CLOSE_HOUR = 20
const SLOT_MIN   = 30

// “Rellenar semana” solo cuando NO hay hora exacta pedida
const STEER_ON = (process.env.BOT_STEER_BALANCE || "on").toLowerCase() === "on"
const STEER_WINDOW_DAYS = Number(process.env.BOT_STEER_WINDOW_DAYS || 7)
const SEARCH_WINDOW_DAYS = Number(process.env.BOT_SEARCH_WINDOW_DAYS || 14)
// Desviación máxima el mismo día si el cliente pide hora concreta:
const MAX_SAME_DAY_DEVIATION_MIN = Number(process.env.BOT_MAX_SAME_DAY_DEVIATION_MIN || 60)
// Umbral de “sí” seguro si la propuesta se aleja de lo pedido:
const STRICT_YES_DEVIATION_MIN = Number(process.env.BOT_STRICT_YES_DEVIATION_MIN || 45)

const onlyDigits = (s="") => (s||"").replace(/\D+/g,"")
const rmDiacritics = (s="") => s.normalize("NFD").replace(/\p{Diacritic}/gu,"")
const norm = (s="") => rmDiacritics(String(s).toLowerCase()).replace(/[^a-z0-9]+/g," ").trim()
const STOP = new Set("de del la el los las un una unos unas y o u a al con por para en me mi su sus quiero quisiera querria hazme hacerme ponme dame porfa por favor hola buenas tardes buenos dias noches necesito reservar cita hora con que tal am pm por la manana por la mañana temprano antes primero siguiente otro otra cualquier cuando sea".split(" "))
function tokenize(s){ return norm(s).split(/\s+/).filter(w=>w && w.length>1 && !STOP.has(w)) }

const YES_RE = /\b(s[ií]|ok|okay|okey+|vale+|va|venga|dale|confirmo|confirmar|de acuerdo|perfecto|genial)\b/i
const NO_RE  = /\b(no+|otra|cambia|no confirmo|mejor mas tarde|mejor más tarde|anula|cancela|cancelemos|quitar cita)\b/i
const RESCH_RE = /\b(cambia|cambiar|modifica|mover|reprograma|reprogramar|edita)\b/i
const CANCEL_RE = /\b(cancela(?:r|me|la)?|anula(?:r|me|la)?|elimina(?:r|me|la)?|borra(?:r|me|la)?|quitar(?: la)? cita|anulaci[oó]n)\b/i

function normalizePhoneES(raw){
  const d=onlyDigits(raw); if(!d) return null
  if (raw.startsWith("+") && d.length>=8 && d.length<=15) return `+${d}`
  if (d.startsWith("34") && d.length===11) return `+${d}`
  if (d.length===9) return `+34${d}`
  if (d.startsWith("00")) return `+${d.slice(2)}`
  return `+${d}`
}
const isValidEmail=(e)=>/^[^\s@]+@[^\s@]+\.[^\s@]{2,}$/.test(String(e||"").trim())
const minutesApart=(a,b)=>Math.abs(a.diff(b,"minute"))

// ===== Empleadas — SOLO Playamar (desde .env)
const EMPLOYEES = {
  rocio:     process.env.SQ_EMP_ROCIO     || "",
  cristina:  process.env.SQ_EMP_CRISTINA  || "",
  sami:      process.env.SQ_EMP_SAMI      || "",
  elisabeth: process.env.SQ_EMP_ELISABETH || "",
  tania:     process.env.SQ_EMP_TANIA     || "",
  jamaica:   process.env.SQ_EMP_JAMAICA   || "",
  johana:    process.env.SQ_EMP_JOHANA    || "",
  chabeli:   process.env.SQ_EMP_CHABELI   || "",
  desi:      process.env.SQ_EMP_DESI      || "",
  martina:   process.env.SQ_EMP_MARTINA   || "",
  ginna:     process.env.SQ_EMP_GINNA     || "",
  edurne:    process.env.SQ_EMP_EDURNE    || "",
}
const EMP_ALIASES = {
  "rocio":"rocio","rocío":"rocio","rosi":"rocio","rocio chica":"rocio","rocío chica":"rocio",
  "cristina":"cristina","cristi":"cristina","cristina jaime":"cristina","cristina castro":"cristina",
  "sami":"sami",
  "elisabeth":"elisabeth","elisabet":"elisabeth","eli":"elisabeth",
  "tania":"tania",
  "jamaica":"jamaica",
  "johana":"johana","yohana":"johana",
  "chabeli":"chabeli","chabela":"chabeli",
  "desi":"desi","desiree":"desi","desirée":"desi",
  "martina":"martina",
  "ginna":"ginna","gina":"ginna",
  "edurne":"edurne"
}
const TEAM_MEMBER_IDS = Object.values(EMPLOYEES).filter(Boolean)
if (!TEAM_MEMBER_IDS.length) { console.error("⛔ Falta configurar empleadas SQ_EMP_* en .env"); process.exit(1) }

const STAFF_NAME_BY_ID = (()=> {
  const m={}
  for (const [k,id] of Object.entries(EMPLOYEES)) if (id) m[id]=k
  return m
})()
const displayStaff = (id) => {
  const k = STAFF_NAME_BY_ID[id] || ""
  if (!k) return ""
  return k.charAt(0).toUpperCase()+k.slice(1)
}

// ===== Servicios desde .env (variationId|version)
function titleCase(s){return s.replace(/\b[a-záéíóúñ0-9]+\b/gi, w => w.charAt(0).toUpperCase()+w.slice(1).toLowerCase())}
function humanizeEnvKey(k){return titleCase(k.replace(/^SQ_SVC_/,"").replace(/_/g," ").trim())}
function loadServiceCatalogFromEnv(){
  const catalog=[]
  for (const [k,v] of Object.entries(process.env)){
    if(!k.startsWith("SQ_SVC_")) continue
    const [variationId, versionRaw] = String(v||"").split("|")
    if(!variationId) continue
    const display = humanizeEnvKey(k)
    const toks = tokenize(display)
    catalog.push({
      envKey:k, displayName:display, variationId,
      variationVersion:versionRaw?Number(versionRaw):undefined,
      normName: norm(display),
      tokens: new Set(toks)
    })
  }
  catalog.sort((a,b)=>b.normName.length - a.normName.length)
  return catalog
}
const SERVICE_CATALOG = loadServiceCatalogFromEnv()

// Alias frecuentes
const SERVICE_ALIASES = {
  "depilacion de cejas con hilo":"SQ_SVC_DEPILACION_CEJAS_CON_HILO",
  "depilacion cejas con hilo":"SQ_SVC_DEPILACION_CEJAS_CON_HILO",
  "depilar cejas con hilo":"SQ_SVC_DEPILACION_CEJAS_CON_HILO",
  "depilacion cejas":"SQ_SVC_DEPILACION_CEJAS_CON_HILO",
  "cejas con hilo":"SQ_SVC_DEPILACION_CEJAS_CON_HILO",
  "depilacion labio con hilo":"SQ_SVC_DEPILACION_LABIO_CON_HILO",
  "manicura semipermanente":"SQ_SVC_MANICURA_SEMIPERMANENTE",
  "manicura rusa":"SQ_SVC_MANICURA_RUSA_CON_NIVELACION",
  "semipermanente pies":"SQ_SVC_ESMALTADO_SEMIPERMANETE_PIES",
  "lifting pestañas":"SQ_SVC_LIFITNG_DE_PESTANAS_Y_TINTE",
  "punta de diamante":"SQ_SVC_LIMPIEZA_FACIAL_CON_PUNTA_DE_DIAMANTE",
  "hydra facial":"SQ_SVC_LIMPIEZA_HYDRA_FACIAL",
  "laser cejas":"SQ_SVC_LASER_CEJAS",
  "microblading":"SQ_SVC_MICROBLADING",
  "fotodepilacion ingles":"SQ_SVC_FOTODEPILACION_INGLES",
  "fotodepilacion axilas":"SQ_SVC_FOTODEPILACION_AXILAS",
  "fotodepilacion facial":"SQ_SVC_FOTODEPILACION_FACIAL_COMPLETO",
}
function resolveServiceFromText(userText){
  const n = norm(userText)
  for (const [k, envKey] of Object.entries(SERVICE_ALIASES)){
    if (n.includes(norm(k))){
      const svc = SERVICE_CATALOG.find(s => s.envKey === envKey)
      if (svc) return svc
    }
  }
  for (const svc of SERVICE_CATALOG){
    if (n.includes(svc.normName)) return svc
  }
  const t = tokenize(userText)
  if (!t.length) return null
  const U = new Set(t)
  let best = null
  for (const svc of SERVICE_CATALOG){
    let match = 0
    for (const tok of svc.tokens){
      if (U.has(tok)) match += (tok.length>=6 ? 2 : 1)
    }
    const denom = Math.max(1, svc.tokens.size)
    const score = match/denom
    if (!best || score > best.score) best = { svc, score, hits: match }
  }
  if (best && (best.score >= 0.5 || best.hits >= 2)) return best.svc
  return null
}

// Duraciones (fallback 60)
const DURATION_MIN = {
  "SQ_SVC_DEPILACION_CEJAS_CON_HILO":15,
  "SQ_SVC_DEPILACION_LABIO_CON_HILO":10,
  "SQ_SVC_ESMALTADO_SEMIPERMANETE_PIES":30,
  "SQ_SVC_LIFITNG_DE_PESTANAS_Y_TINTE":60,
  "SQ_SVC_LIMPIEZA_FACIAL_CON_PUNTA_DE_DIAMANTE":90,
  "SQ_SVC_LIMPIEZA_HYDRA_FACIAL":90,
  "SQ_SVC_FOTODEPILACION_INGLES":30,
}
const getDuration=(envKey)=>DURATION_MIN[envKey] ?? 60

// ===== Fecha/hora
const M_MAP={enero:1,febrero:2,marzo:3,abril:4,mayo:5,junio:6,julio:7,agosto:8,septiembre:9,setiembre:9,octubre:10,noviembre:11,diciembre:12,ene:1,feb:2,mar:3,abr:4,may:5,jun:6,jul:7,ago:8,sep:9,oct:10,nov:11,dic:12}
const WD_MAP={domingo:0,lunes:1,martes:2,miercoles:3,miércoles:3,jueves:4,viernes:5,sabado:6,sábado:6}
const TIME_RE=/\b(\d{1,2})(?::|h)?(\d{2})?\s*(am|pm)?\b/i

function detectExplicitDateEU(s){
  const t=rmDiacritics((s||"").toLowerCase())
  const m1=t.match(/\b(\d{1,2})\s+de\s+(enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|setiembre|octubre|noviembre|diciembre|ene|feb|mar|abr|may|jun|jul|ago|sep|oct|nov|dic)\b(?:\s+de\s+(\d{4}))?/)
  if(m1){const dd=+m1[1],mm=M_MAP[m1[2]],yy=m1[3]?+m1[3]:dayjs().tz(EURO_TZ).year();return dayjs.tz(`${yy}-${String(mm).padStart(2,"0")}-${String(dd).padStart(2,"0")} 00:00`,EURO_TZ)}
  const m2=t.match(/\b(\d{1,2})[\/\-](\d{1,2})(?:[\/\-](\d{2,4}))?\b/)
  if(m2){let yy=m2[3]?+m2[3]:dayjs().tz(EURO_TZ).year();if(yy<100)yy+=2000;return dayjs.tz(`${yy}-${String(+m2[2]).padStart(2,"0")}-${String(+m2[1]).padStart(2,"0")} 00:00`,EURO_TZ)}
  for(const k of Object.keys(WD_MAP)){
    if(t.includes(k)){
      const target=WD_MAP[k]; const now=dayjs().tz(EURO_TZ); let add=(target-now.day()+7)%7
      if(add===0) add=7
      return now.add(add,"day").startOf("day")
    }
  }
  if(/\bhoy\b/.test(t)) return dayjs().tz(EURO_TZ).startOf("day")
  if(/\bmanana\b/.test(t)) return dayjs().tz(EURO_TZ).add(1,"day").startOf("day")
  return null
}
function detectTime(s){
  const m = (s||"").toLowerCase().match(TIME_RE)
  if(!m) return null
  let h=+m[1], min=m[2]?+m[2]:0
  const ap=m[3]
  if(ap==="pm"&&h<12)h+=12
  if(ap==="am"&&h===12)h=0
  return {h, min}
}
function mergeDateTimeEU(dateEU,{h,min}){ return dateEU.clone().hour(h).minute(min).second(0).millisecond(0) }

const fmtES=(d)=>{const t=(dayjs.isDayjs(d)?d:dayjs(d)).tz(EURO_TZ);const dias=["domingo","lunes","martes","miércoles","jueves","viernes","sábado"];const DD=String(t.date()).padStart(2,"0"),MM=String(t.month()+1).padStart(2,"0"),HH=String(t.hour()).padStart(2,"0"),mm=String(t.minute()).padStart(2,"0");return `${dias[t.day()]} ${DD}/${MM} ${HH}:${mm}`}
function ceilToSlotEU(t){const m=t.minute();const rem=m%SLOT_MIN;if(rem===0)return t.second(0).millisecond(0);return t.add(SLOT_MIN-rem,"minute").second(0).millisecond(0)}

// ===== Square
const square = new Client({
  accessToken: process.env.SQUARE_ACCESS_TOKEN,
  environment: (process.env.SQUARE_ENV || "sandbox")==="production"?Environment.Production:Environment.Sandbox
})
const locationId = process.env.SQUARE_LOCATION_ID_PLAYAMAR || process.env.SQUARE_LOCATION_ID
let LOCATION_TZ = EURO_TZ
async function squareCheckCredentials(){try{const locs=await square.locationsApi.listLocations();const loc=(locs.result.locations||[]).find(l=>l.id===locationId)||(locs.result.locations||[])[0];if(loc?.timezone)LOCATION_TZ=loc.timezone;console.log(`✅ Square listo. Location ${locationId}, TZ=${LOCATION_TZ}`)}catch(e){console.error("⛔ Square:",e?.message||e)}}
async function squareFindCustomerByPhone(phoneRaw){try{const e164=normalizePhoneES(phoneRaw);if(!e164||!e164.startsWith("+")||e164.length<8||e164.length>16)return null;const resp=await square.customersApi.searchCustomers({query:{filter:{phoneNumber:{exact:e164}}}});return (resp?.result?.customers||[])[0]||null}catch(e){console.error("Square search:",e?.message||e);return null}}
async function squareCreateCustomer({givenName,emailAddress,phoneNumber}){try{
  if(!isValidEmail(emailAddress)) return null
  const phone=normalizePhoneES(phoneNumber)
  const resp=await square.customersApi.createCustomer({
    idempotencyKey:`cust_${Date.now()}_${Math.random().toString(36).slice(2,8)}`,
    givenName,emailAddress,phoneNumber:phone||undefined,
    note:"Creado desde bot WhatsApp Gapink Nails (Playamar)"
  })
  return resp?.result?.customer||null
}catch(e){console.error("Square create:",e?.message||e);return null}}

async function getServiceVariationVersion(id){try{const resp=await square.catalogApi.retrieveCatalogObject(id,true);return resp?.result?.object?.version}catch(e){console.error("getServiceVariationVersion:",e?.message||e);return undefined}}
function stableKey({locationId,serviceVariationId,startISO,customerId,teamMemberId}){const raw=`${locationId}|${serviceVariationId}|${startISO}|${customerId}|${teamMemberId}`;return createHash("sha256").update(raw).digest("hex").slice(0,48)}
async function createSquareBooking({startEU,svc,customerId,teamMemberId}){try{
  const serviceVariationId=svc.variationId;let version=svc.variationVersion||await getServiceVariationVersion(serviceVariationId)
  if(!serviceVariationId||!teamMemberId||!locationId||!version)return null
  const startISO=startEU.tz("UTC").toISOString()
  const body={idempotencyKey:stableKey({locationId,serviceVariationId,startISO,customerId,teamMemberId}),booking:{locationId,startAt:startISO,customerId,appointmentSegments:[{teamMemberId,serviceVariationId,serviceVariationVersion:Number(version),durationMinutes:getDuration(svc.envKey)}]}}
  const resp=await square.bookingsApi.createBooking(body)
  return resp?.result?.booking||null
}catch(e){console.error("createSquareBooking:",e?.message||e);return null}}
async function cancelSquareBooking(bookingId){ // ← FIX: requiere body {idempotencyKey, bookingVersion}
  try{
    const rb = await square.bookingsApi.retrieveBooking(bookingId)
    const version = rb?.result?.booking?.version
    if (version===undefined || version===null) return false
    const body = { idempotencyKey: `cancel_${bookingId}_${Date.now()}`, bookingVersion: version }
    const r = await square.bookingsApi.cancelBooking(bookingId, body)
    return !!r?.result?.booking?.id
  }catch(e){console.error("cancelSquareBooking:",e?.message||e);return false}
}
async function updateSquareBooking(bookingId,{startEU,svc,customerId,teamMemberId}){try{
  const get=await square.bookingsApi.retrieveBooking(bookingId);const booking=get?.result?.booking;if(!booking)return null
  let version=svc.variationVersion||await getServiceVariationVersion(svc.variationId)
  const startISO=startEU.tz("UTC").toISOString()
  const body={idempotencyKey:stableKey({locationId,serviceVariationId:svc.variationId,startISO,customerId,teamMemberId}),booking:{id:bookingId,version:booking.version,locationId,customerId,startAt:startISO,appointmentSegments:[{teamMemberId,serviceVariationId:svc.variationId,serviceVariationVersion:Number(version),durationMinutes:getDuration(svc.envKey)}]}}
  const resp=await square.bookingsApi.updateBooking(bookingId, body);return resp?.result?.booking||null
}catch(e){console.error("updateSquareBooking:",e?.message||e);return null}}

// ===== DB & sesiones
const db=new Database("gapink.db");db.pragma("journal_mode = WAL")
db.exec(`
CREATE TABLE IF NOT EXISTS appointments (
  id TEXT PRIMARY KEY,
  customer_name TEXT,
  customer_phone TEXT,
  customer_square_id TEXT,
  service_env_key TEXT,
  service_display TEXT,
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
CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_slot
ON appointments(staff_id, start_iso)
WHERE status IN ('pending','confirmed');
`)
const insertAppt=db.prepare(`INSERT INTO appointments
(id, customer_name, customer_phone, customer_square_id, service_env_key, service_display, duration_min, start_iso, end_iso, staff_id, status, created_at, square_booking_id)
VALUES (@id, @customer_name, @customer_phone, @customer_square_id, @service_env_key, @service_display, @duration_min, @start_iso, @end_iso, @staff_id, @status, @created_at, @square_booking_id)`)
const updateAppt=db.prepare(`UPDATE appointments SET status=@status, square_booking_id=@square_booking_id WHERE id=@id`)
const updateApptTimes=db.prepare(`UPDATE appointments SET start_iso=@start_iso, end_iso=@end_iso, staff_id=@staff_id WHERE id=@id`)
const markCancelled=db.prepare(`UPDATE appointments SET status='cancelled' WHERE id=@id`)
const deleteAppt=db.prepare(`DELETE FROM appointments WHERE id=@id`)
const getSessionRow=db.prepare(`SELECT * FROM sessions WHERE phone=@phone`)
const upsertSession=db.prepare(`INSERT INTO sessions (phone, data_json, updated_at)
VALUES (@phone, @data_json, @updated_at)
ON CONFLICT(phone) DO UPDATE SET data_json=excluded.data_json, updated_at=excluded.updated_at`)
const clearSession=db.prepare(`DELETE FROM sessions WHERE phone=@phone`)
const getUpcomingByPhone=db.prepare(`SELECT * FROM appointments WHERE customer_phone=@phone AND status='confirmed' AND start_iso > @now ORDER BY start_iso ASC LIMIT 1`)

function loadSession(phone){
  const row=getSessionRow.get({phone}); if(!row?.data_json) return null
  const raw=JSON.parse(row.data_json); const data={...raw}
  if (raw.startEU_ms) data.startEU = dayjs.tz(raw.startEU_ms, EURO_TZ)
  if (raw.pendingOfferEU_ms) data.pendingOfferEU = dayjs.tz(raw.pendingOfferEU_ms, EURO_TZ)
  if (raw.lastRequestedEU_ms) data.lastRequestedEU = dayjs.tz(raw.lastRequestedEU_ms, EURO_TZ)
  if (raw.anchorDateEU_ms) data.anchorDateEU = dayjs.tz(raw.anchorDateEU_ms, EURO_TZ)
  return data
}
function saveSession(phone,data){
  const s={...data}
  s.startEU_ms = data.startEU?.valueOf?.() ?? data.startEU_ms ?? null; delete s.startEU
  s.pendingOfferEU_ms = data.pendingOfferEU?.valueOf?.() ?? data.pendingOfferEU_ms ?? null; delete s.pendingOfferEU
  s.lastRequestedEU_ms = data.lastRequestedEU?.valueOf?.() ?? data.lastRequestedEU_ms ?? null; delete s.lastRequestedEU
  s.anchorDateEU_ms = data.anchorDateEU?.valueOf?.() ?? data.anchorDateEU_ms ?? null; delete s.anchorDateEU
  upsertSession.run({phone, data_json:JSON.stringify(s), updated_at:new Date().toISOString()})
}

// ===== Disponibilidad (DB local)
function getBookedIntervals(fromIso,toIso){
  const rows=db.prepare(`SELECT start_iso,end_iso,staff_id FROM appointments WHERE status IN ('pending','confirmed') AND start_iso < @to AND end_iso > @from`).all({from:fromIso,to:toIso})
  return rows.map(r=>({start:dayjs(r.start_iso),end:dayjs(r.end_iso),staff_id:r.staff_id}))
}
function isFree(intervals, staffId, startUTC, endUTC){
  return !intervals.filter(i=>i.staff_id===staffId).some(i => (startUTC<i.end) && (i.start<endUTC))
}
function findExactSlot(startEU, durationMin, staffId=null){
  const dayStart=startEU.clone().hour(OPEN_HOUR).minute(0).second(0)
  const dayEnd=startEU.clone().hour(CLOSE_HOUR).minute(0).second(0)
  if (startEU.isBefore(dayStart) || startEU.add(durationMin,"minute").isAfter(dayEnd)) return null
  const start = ceilToSlotEU(startEU.clone())
  const end = start.clone().add(durationMin,"minute")
  const from = dayStart.tz("UTC").toISOString()
  const to   = dayEnd.tz("UTC").toISOString()
  const intervals=getBookedIntervals(from,to)
  if (staffId) {
    if (isFree(intervals,staffId,start.tz("UTC"),end.tz("UTC"))) return { time:start, staffId }
    return null
  }
  for (const id of TEAM_MEMBER_IDS){
    if (isFree(intervals,id,start.tz("UTC"),end.tz("UTC"))) return { time:start, staffId:id }
  }
  return null
}
function* sameDayRing(startEU, preferEarlier=false){
  const base = ceilToSlotEU(startEU.clone())
  yield base
  for (let k=1;k<=16;k++){
    if (preferEarlier){ yield base.clone().subtract(k*SLOT_MIN,"minute"); yield base.clone().add(k*SLOT_MIN,"minute") }
    else { yield base.clone().add(k*SLOT_MIN,"minute"); yield base.clone().subtract(k*SLOT_MIN,"minute") }
  }
}
function findNearestSameDay(startEU, durationMin, staffId=null, declined=[], preferEarlier=false, maxDeviationMin=MAX_SAME_DAY_DEVIATION_MIN) {
  const dayStart=startEU.clone().hour(OPEN_HOUR).minute(0).second(0)
  const dayEnd=startEU.clone().hour(CLOSE_HOUR).minute(0).second(0)
  const from = dayStart.tz("UTC").toISOString()
  const to   = dayEnd.tz("UTC").toISOString()
  const intervals=getBookedIntervals(from,to)
  const base = ceilToSlotEU(startEU.clone())
  for (const t of sameDayRing(startEU, preferEarlier)){
    const diff = Math.abs(t.diff(base, "minute"))
    if (diff > maxDeviationMin) break
    const e=t.clone().add(durationMin,"minute")
    if (t.isBefore(dayStart) || e.isAfter(dayEnd)) continue
    if (declined.includes(t.valueOf())) continue
    if (staffId) {
      if (isFree(intervals,staffId,t.tz("UTC"),e.tz("UTC"))) return { time:t, staffId }
    } else {
      for (const id of TEAM_MEMBER_IDS){
        if (isFree(intervals,id,t.tz("UTC"),e.tz("UTC"))) return { time:t, staffId:id }
      }
    }
  }
  return null
}
function preferredDayList(startBase, daysWindow){
  const days=[]
  for(let d=0; d<=daysWindow; d++){
    days.push(startBase.clone().add(d,"day").startOf("day"))
  }
  const early = days.filter(d => [1,2,3].includes(d.day()))
  const others= days.filter(d => ![1,2,3].includes(d.day()))
  return [...early, ...others]
}
function findEarliestAny(startEU, durationMin, daysWindow){
  const now=dayjs().tz(EURO_TZ).add(30,"minute").second(0).millisecond(0)
  const startBase = startEU && startEU.isAfter(now) ? ceilToSlotEU(startEU.clone()) : ceilToSlotEU(now.clone())
  const toEU = startBase.clone().add(daysWindow,"day").hour(CLOSE_HOUR).minute(0).second(0)
  const intervals=getBookedIntervals(startBase.tz("UTC").toISOString(), toEU.tz("UTC").toISOString())
  const dayOrder = preferredDayList(startBase, daysWindow)
  for(const day of dayOrder){
    const dow=day.day()===0?7:day.day()
    if(!WORK_DAYS.includes(dow)) continue
    const dayStart = day.clone().hour(OPEN_HOUR).minute(0).second(0)
    const dayEnd   = day.clone().hour(CLOSE_HOUR).minute(0).second(0)
    const firstSlot = day.isSame(startBase, "day") ? (startBase.isAfter(dayStart) ? startBase.clone() : dayStart) : dayStart
    for(let t=ceilToSlotEU(firstSlot.clone()); !t.isAfter(dayEnd); t=t.add(SLOT_MIN,"minute")){
      const e=t.clone().add(durationMin,"minute"); if(e.isAfter(dayEnd)) break
      const tUTC=t.tz("UTC"), eUTC=e.tz("UTC")
      for(const staffId of TEAM_MEMBER_IDS){
        if(isFree(intervals,staffId,tUTC,eUTC)) return { time:t, staffId }
      }
    }
  }
  return null
}

// ===== Mini web
const app=express()
const PORT=process.env.PORT||8080
let lastQR=null,conectado=false
app.get("/",(_req,res)=>{res.send(`<!doctype html><meta charset="utf-8"><style>
  body{font-family:system-ui;display:grid;place-items:center;min-height:100vh;background:linear-gradient(135deg,#fce4ec,#f8bbd0);color:#4a148c}
  .card{background:#fff;padding:24px;border-radius:16px;box-shadow:0 6px 24px rgba(0,0,0,.08);text-align:center;max-width:520px}
  a{color:#4a148c;text-decoration:underline;font-weight:600}
</style>
<div class="card">
  <h1>Gapink Nails</h1>
  <p>Estado: ${conectado?"✅ Conectado":"❌ Desconectado"}</p>
  ${!conectado&&lastQR?`<img src="/qr.png" width="320" />`:``}
  <p><small><a href="https://gonzalog.co" target="_blank" rel="noopener noreferrer">Gonzalo García Aranda</a></small></p>
</div>`)})

app.get("/qr.png",async(_req,res)=>{if(!lastQR)return res.status(404).send("No hay QR");const png=await qrcode.toBuffer(lastQR,{type:"png",width:512,margin:1});res.set("Content-Type","image/png").send(png)})

const wait=(ms)=>new Promise(r=>setTimeout(r,ms))
app.listen(PORT,async()=>{
  console.log(`🌐 Web en puerto ${PORT}`)
  await squareCheckCredentials()
  startBot().catch(console.error)
})

// ===== Bot
async function startBot(){
  console.log("🚀 Bot arrancando…")
  try{
    if(!fs.existsSync("auth_info"))fs.mkdirSync("auth_info",{recursive:true})
    const { state, saveCreds } = await useMultiFileAuthState("auth_info")
    const { version } = await fetchLatestBaileysVersion()
    let isOpen=false,reconnecting=false
    const sock=makeWASocket({logger:pino({level:"silent"}),printQRInTerminal:false,auth:state,version,browser:Browsers.macOS("Desktop"),syncFullHistory:false,connectTimeoutMs:30000})

    const outbox=[]; let sending=false
    const __SAFE_SEND__=(jid,content)=>new Promise((resolve,reject)=>{outbox.push({jid,content,resolve,reject});processOutbox().catch(console.error)})
    async function processOutbox(){if(sending)return;sending=true;while(outbox.length){const {jid,content,resolve,reject}=outbox.shift();let guard=0;while(!isOpen&&guard<60){await wait(1000);guard++}if(!isOpen){reject(new Error("WA not connected"));continue}let ok=false,err=null;for(let a=1;a<=4;a++){try{await sock.sendMessage(jid,content);ok=true;break}catch(e){err=e;const msg=e?.data?.stack||e?.message||String(e);if(/Timed Out/i.test(msg)||/Boom/i.test(msg)){await wait(500*a);continue}await wait(400)}}if(ok)resolve(true);else{console.error("sendMessage failed:",err?.message||err);reject(err);try{await sock.ws.close()}catch{}}}sending=false}

    sock.ev.on("connection.update",async({connection,lastDisconnect,qr})=>{
      if(qr){lastQR=qr;conectado=false;try{qrcodeTerminal.generate(qr,{small:true})}catch{}}
      if(connection==="open"){lastQR=null;conectado=true;isOpen=true;console.log("✅ Conectado a WhatsApp");processOutbox().catch(console.error)}
      if(connection==="close"){conectado=false;isOpen=false;const reason=lastDisconnect?.error?.message||String(lastDisconnect?.error||"");console.log("❌ Conexión cerrada:",reason);if(!reconnecting){reconnecting=true;await wait(2000);try{await startBot()}finally{reconnecting=false}}}
    })
    sock.ev.on("creds.update",saveCreds)

    sock.ev.on("messages.upsert",async({messages})=>{
      try{
        const m=messages?.[0]; if (!m?.message || m.key.fromMe) return
        const from=m.key.remoteJid
        const phone=normalizePhoneES((from||"").split("@")[0]||"")||(from||"").split("@")[0]||""
        const body=m.message.conversation||m.message.extendedTextMessage?.text||m.message?.imageMessage?.caption||""
        const textRaw=(body||"").trim()
        const textNorm=norm(textRaw)

        // ===== Session
        let data=loadSession(phone)||{
          serviceEnvKey:null, service:null, durationMin:null,
          startEU:null, lastRequestedEU:null, anchorDateEU:null,
          timeInsistCount:0,
          requestedStaffId:null, staffInsistCount:0,
          declinedSlots:[], pendingOfferEU:null, selectedStaffId:null,
          mode:"idle", // idle|await_name|await_email
          name:null, email:null,
          confirmAsked:false, bookingInFlight:false,
          editBookingId:null
        }

        const userSaysYes = YES_RE.test(textRaw)
        const userSaysNo  = NO_RE.test(textRaw)

        // ===== CANCELAR
        if (CANCEL_RE.test(textRaw)) {
          const upc = getUpcomingByPhone.get({ phone, now: dayjs().utc().toISOString() })
          if (upc) {
            let ok=true
            if (upc.square_booking_id) ok = await cancelSquareBooking(upc.square_booking_id)
            if (ok) { markCancelled.run({ id: upc.id }); clearSession.run({ phone }); await __SAFE_SEND__(from,{ text:`He cancelado tu cita del ${fmtES(dayjs(upc.start_iso))}.` }) }
            else { await __SAFE_SEND__(from,{ text:"No pude cancelarla ahora mismo. Prueba de nuevo en un minuto." }) }
          } else {
            await __SAFE_SEND__(from,{ text:"No veo ninguna cita futura tuya. Si quieres, dime día y hora y te doy hueco." })
          }
          return
        }

        // ===== Reprogramar
        if (RESCH_RE.test(textRaw)) {
          const upc = getUpcomingByPhone.get({ phone, now: dayjs().utc().toISOString() })
          if (upc) {
            data.editBookingId = upc.id; data.serviceEnvKey = upc.service_env_key
            data.service = upc.service_display; data.durationMin = upc.duration_min
            data.requestedStaffId = upc.staff_id
          }
        }

        // ===== Pedir datos (alta)
        if (data.mode==="await_name") {
          if (textRaw.length<3 || /\d/.test(textRaw)) { await __SAFE_SEND__(from,{text:"Dime tu nombre y apellidos tal cual quieres que aparezca."}); return }
          data.name = textRaw.trim(); data.mode="await_email"; saveSession(phone,data)
          await __SAFE_SEND__(from,{text:"Perfecto. Ahora tu email (ejemplo@correo.com)."})
          return
        }
        if (data.mode==="await_email") {
          if (!isValidEmail(textRaw)) { await __SAFE_SEND__(from,{text:"Ese correo no me vale. Ponme uno válido (tipo nombre@dominio.com)."}); return }
          data.email = textRaw.trim(); data.mode="idle"; saveSession(phone,data)
          if (data.pendingOfferEU && data.serviceEnvKey) { await finalizeBooking({ from, phone, data, safeSend: __SAFE_SEND__ }); return }
        }

        // ===== Empleada por alias
        for (const w of textNorm.split(/\s+/)) {
          const key=EMP_ALIASES[w]; if (key && EMPLOYEES[key]) {
            if (data.requestedStaffId===EMPLOYEES[key]) data.staffInsistCount++
            else { data.requestedStaffId=EMPLOYEES[key]; data.staffInsistCount=1 }
          }
        }
        const wantsStaff = !!data.requestedStaffId
        const forceStaff = wantsStaff && data.staffInsistCount>=2

        // ===== Servicio
        const svcDetected = resolveServiceFromText(textRaw)
        if (svcDetected) {
          if (svcDetected.envKey!==data.serviceEnvKey) { data.confirmAsked=false; data.pendingOfferEU=null }
          data.serviceEnvKey = svcDetected.envKey
          data.service = svcDetected.displayName
          data.durationMin = getDuration(svcDetected.envKey)
        }

        // ===== Fecha/hora (ancla)
        const dateEU = detectExplicitDateEU(textRaw)
        const time = detectTime(textRaw)
        if (dateEU) data.anchorDateEU = dateEU
        if (time) {
          const baseDate = data.anchorDateEU || dateEU || dayjs().tz(EURO_TZ).startOf("day")
          const dt = mergeDateTimeEU(baseDate, time)
          if (data.lastRequestedEU && data.lastRequestedEU.valueOf()===dt.valueOf()) data.timeInsistCount = (data.timeInsistCount||0)+1
          else { data.timeInsistCount = 1; data.lastRequestedEU = dt.clone() }
          data.startEU = dt
          data.confirmAsked=false; data.pendingOfferEU=null
        }

        // ===== NO ⇒ marca oferta rechazada
        if (userSaysNo && data.pendingOfferEU) {
          data.declinedSlots.push(data.pendingOfferEU.valueOf())
          data.pendingOfferEU=null; data.confirmAsked=false
          saveSession(phone,data)
          await __SAFE_SEND__(from,{text:"Sin problema. Dime otra hora o día y lo miro."})
          return
        }

        // ===== YES ⇒ cerrar si hay oferta pendiente (guardia de desvío)
        if (userSaysYes && data.pendingOfferEU && data.serviceEnvKey) {
          const lastWanted = data.lastRequestedEU
          if (lastWanted && minutesApart(lastWanted, data.pendingOfferEU) > STRICT_YES_DEVIATION_MIN && data.timeInsistCount>=2) {
            await __SAFE_SEND__(from,{text:`Me dijiste ${fmtES(lastWanted)}. La propuesta es ${fmtES(data.pendingOfferEU)}. Si te encaja, escribe “confirmo ${data.pendingOfferEU.format("HH:mm")}”.`})
            return
          }
          const existing = await squareFindCustomerByPhone(phone)
          if (!existing && (!data.name || !data.email)) {
            data.mode = data.name ? "await_email" : "await_name"
            saveSession(phone,data)
            await __SAFE_SEND__(from,{text: data.name ? "Me falta tu email para cerrar la cita." : "Antes de cerrar, dime tu nombre y apellidos."})
            return
          }
          await finalizeBooking({ from, phone, data, safeSend: __SAFE_SEND__ })
          return
        }

        // ===== GUIADO
        if (!data.serviceEnvKey) { saveSession(phone,data); await __SAFE_SEND__(from,{ text:"¿Qué te hago? (Ej: “Manicura semipermanente”, “Depilación cejas con hilo”…)" }); return }
        if (!data.startEU) { saveSession(phone,data); await __SAFE_SEND__(from,{ text:`Genial, ${data.service}. Dime día y hora (ej: “lunes 10:00” o “15/09 18:00”).` }); return }

        const duration = data.durationMin || 60
        let offer = null
        const declined = data.declinedSlots || []
        const hardTime   = !!time || (data.timeInsistCount>=1)
        const dayLock    = !!data.anchorDateEU // con fecha explícita, no mover de día

        // 1) Exacto con staff si lo pidió
        if (!offer && wantsStaff) offer = findExactSlot(data.startEU, duration, data.requestedStaffId)
        // 2) Exacto con cualquiera (si no está “forzado” el staff)
        if (!offer && (!wantsStaff || !forceStaff)) {
          const sameAny = findExactSlot(data.startEU, duration, null)
          if (sameAny) offer = sameAny
        }
        // 3) Misma fecha ±desviación (antes→después)
        if (!offer) {
          const near = findNearestSameDay(
            data.startEU, duration,
            forceStaff ? data.requestedStaffId : (wantsStaff ? data.requestedStaffId : null),
            declined, true /* preferEarlier */,
            MAX_SAME_DAY_DEVIATION_MIN
          )
          if (near) offer = near
        }
        // 4) Otros días SOLO si no hay fecha anclada ni hora dura
        if (!offer && !dayLock && !hardTime) {
          offer = (STEER_ON ? findEarliestAny(data.startEU, duration, STEER_WINDOW_DAYS) : null)
               || findEarliestAny(data.startEU, duration, SEARCH_WINDOW_DAYS)
        }

        // Guardia: no proponer fuera del margen
        if (offer && hardTime && data.startEU && minutesApart(data.startEU, offer.time) > MAX_SAME_DAY_DEVIATION_MIN) {
          offer = null
        }
        // Guardia: si hay fecha anclada, NO proponer otro día
        if (offer && data.anchorDateEU && !offer.time.isSame(data.anchorDateEU, "day")){
          offer = null
        }

        if (!offer) {
          data.confirmAsked=false; saveSession(phone,data)
          if (dayLock || hardTime) {
            if (forceStaff) {
              await __SAFE_SEND__(from,{ text:`Ese día a esa hora con ${displayStaff(data.requestedStaffId)} no tengo hueco. ¿Miro esa MISMA hora otro día o te vale con otra compañera? (Pago en persona)` })
            } else {
              await __SAFE_SEND__(from,{ text:`Ese día a esa hora no tengo hueco. ¿Miro esa MISMA hora otro día u otra franja ese día? (Pago en persona)` })
            }
          } else {
            await __SAFE_SEND__(from,{ text:"Ahora mismo no veo huecos en esa franja. Dime otra hora o día y te digo." })
          }
          return
        }

        data.pendingOfferEU = offer.time
        data.selectedStaffId = offer.staffId
        data.confirmAsked = true
        saveSession(phone,data)

        const sameAsAsked = data.startEU && offer.time.valueOf()===ceilToSlotEU(data.startEU).valueOf()
        const staffTxt = displayStaff(offer.staffId) ? ` con ${displayStaff(offer.staffId)}` : ""

        if (sameAsAsked) {
          await __SAFE_SEND__(from,{ text:`Tengo ${fmtES(offer.time)}${staffTxt}. ¿Confirmo la ${data.editBookingId?"modificación":"cita"}? (Pago en persona)` })
        } else {
          // Reoferta amable (no “impone”)
          let msg = `No tengo ${data.startEU.format("HH:mm")} `
          if (wantsStaff) msg += `con ${displayStaff(data.requestedStaffId)} `
          msg += `ese día; me sale ${offer.time.format("HH:mm")}${staffTxt}. ¿Te viene bien?`
          // Empujoncito suave para probar otra compañera
          if (wantsStaff && !forceStaff) {
            msg += ` Si quieres probar otra compañera, puedo mirar también ${data.startEU.format("HH:mm")} ese mismo día.`
          }
          await __SAFE_SEND__(from,{ text: msg })
        }
        return

      }catch(e){ console.error("messages.upsert error:",e) }
    })

  }catch(e){ console.error("startBot error:",e) }
}

// ===== Finalizar alta
async function finalizeBooking({ from, phone, data, safeSend }) {
  try {
    if (data.bookingInFlight) return
    data.bookingInFlight = true; saveSession(phone, data)

    let customer = await squareFindCustomerByPhone(phone)
    if (!customer) {
      if (!data.name || !data.email || !isValidEmail(data.email)) {
        data.mode = data.name ? "await_email" : "await_name"
        data.bookingInFlight=false; saveSession(phone,data);
        await safeSend(from,{text: data.name ? "Me falta tu email para cerrar la cita." : "Antes de cerrar, dime tu nombre y apellidos."})
        return
      }
      customer = await squareCreateCustomer({ givenName: data.name, emailAddress: data.email, phoneNumber: phone })
    }
    if (!customer) { data.bookingInFlight=false; saveSession(phone,data); await safeSend(from,{text:"No pude crear tu ficha con ese email. Ponme uno válido y seguimos."}); return }

    const svc = SERVICE_CATALOG.find(s=>s.envKey===data.serviceEnvKey)
    if (!svc) { data.bookingInFlight=false; saveSession(phone,data); await safeSend(from,{text:"No encuentro el servicio ahora mismo. Dime de nuevo el servicio, por favor."}); return }

    const startEU = data.pendingOfferEU || data.startEU
    if (!startEU) { data.bookingInFlight=false; saveSession(phone,data); await safeSend(from,{text:"Necesito una hora concreta. Dímela y te la reservo."}); return }

    const teamMemberId = data.selectedStaffId || TEAM_MEMBER_IDS[0]
    const durationMin = getDuration(svc.envKey)
    const startUTC = startEU.tz("UTC"), endUTC = startUTC.clone().add(durationMin,"minute")

    const aptId = `apt_${Math.random().toString(36).slice(2,8)}${Date.now().toString(36).slice(-4)}`
    try {
      insertAppt.run({
        id: aptId, customer_name: data.name || customer?.givenName || null, customer_phone: phone,
        customer_square_id: customer.id, service_env_key: svc.envKey, service_display: svc.displayName,
        duration_min: durationMin, start_iso: startUTC.toISOString(), end_iso: endUTC.toISOString(),
        staff_id: teamMemberId, status: "pending", created_at: new Date().toISOString(), square_booking_id: null
      })
    } catch (e) {
      if (String(e?.message||"").includes("UNIQUE")) { data.bookingInFlight=false; saveSession(phone,data); await safeSend(from,{text:"Ese hueco acaba de ocuparse. Dime otra hora y te doy opción."}); return }
      throw e
    }

    const sq = await createSquareBooking({ startEU, svc, customerId: customer.id, teamMemberId })
    if (!sq) { deleteAppt.run({ id: aptId }); data.bookingInFlight=false; saveSession(phone,data); await safeSend(from,{text:"No pude confirmar ahora mismo. Probamos con otra hora?"}); return }

    updateAppt.run({ id: aptId, status: "confirmed", square_booking_id: sq.id || null })
    clearSession.run({ phone })
    const staffTxt = displayStaff(teamMemberId) ? ` con ${displayStaff(teamMemberId)}` : ""
    await safeSend(from,{ text:
`Reserva confirmada 🎉
Servicio: ${svc.displayName}
Fecha: ${fmtES(startEU)}${staffTxt}
Duración: ${durationMin} min
Pago en persona. ¡Te esperamos!` })
  } catch (e) { console.error("finalizeBooking:", e) }
  finally { data.bookingInFlight=false; try{ saveSession(phone, data) }catch{} }
}

// ===== Reprogramar
async function finalizeReschedule({ from, phone, data, safeSend }) {
  try{
    if (data.bookingInFlight) return
    data.bookingInFlight = true; saveSession(phone, data)

    const upc = getUpcomingByPhone.get({ phone, now: dayjs().utc().toISOString() })
    if (!upc || upc.id !== data.editBookingId) { data.bookingInFlight=false; saveSession(phone,data); return }

    const startEU = data.pendingOfferEU || data.startEU
    if (!startEU) { data.bookingInFlight=false; saveSession(phone,data); return }

    const svc = SERVICE_CATALOG.find(s=>s.envKey === (data.serviceEnvKey || upc.service_env_key))
    if (!svc) { data.bookingInFlight=false; saveSession(phone,data); return }

    const startUTC = startEU.tz("UTC"), endUTC = startUTC.clone().add(upc.duration_min,"minute")
    const teamId   = data.selectedStaffId || upc.staff_id || TEAM_MEMBER_IDS[0]
    if(!teamId){ data.bookingInFlight=false; saveSession(phone,data); await safeSend(from,{text:"No puedo asignar equipo ahora mismo. Probamos otro día?"}); return }

    let ok=false
    if (upc.square_booking_id) {
      const sq = await updateSquareBooking(upc.square_booking_id, { startEU, svc, customerId: upc.customer_square_id, teamMemberId: teamId })
      if (sq) ok=true
    }
    if (!ok) {
      if (upc.square_booking_id) {
        const cancelled = await cancelSquareBooking(upc.square_booking_id)
        if (!cancelled) { data.bookingInFlight=false; saveSession(phone,data); await safeSend(from,{text:"No pude reprogramar ahora mismo. Probamos otra franja?"}); return }
      }
      const sqNew = await createSquareBooking({ startEU, svc, customerId: upc.customer_square_id, teamMemberId: teamId })
      if (!sqNew) { data.bookingInFlight=false; saveSession(phone,data); await safeSend(from,{text:"No pude reprogramar ahora mismo. Probamos otra franja?"}); return }
      markCancelled.run({ id: upc.id })
      const newId=`apt_${Math.random().toString(36).slice(2,8)}${Date.now().toString(36).slice(-4)}`
      insertAppt.run({
        id:newId, customer_name: upc.customer_name, customer_phone: phone, customer_square_id: upc.customer_square_id,
        service_env_key: svc.envKey, service_display: upc.service_display, duration_min: upc.duration_min,
        start_iso: startUTC.toISOString(), end_iso: endUTC.toISOString(),
        staff_id: teamId, status:"confirmed", created_at:new Date().toISOString(), square_booking_id: sqNew.id || null
      })
    } else {
      updateApptTimes.run({ id: upc.id, start_iso: startUTC.toISOString(), end_iso: endUTC.toISOString(), staff_id: teamId })
    }

    clearSession.run({ phone })
    await safeSend(from,{ text:
`Cita actualizada ✔️
Servicio: ${upc.service_display}
Nueva fecha: ${fmtES(startEU)}
Duración: ${upc.duration_min} min
Pago en persona.` })
  }catch(e){ console.error("finalizeReschedule:", e) }
  finally{ data.bookingInFlight=false; try{ saveSession(phone, data) }catch{} }
}
