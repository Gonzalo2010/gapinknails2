// index.js — Gapink Nails · v26.4
// Anti-fake-confirm + captura de identidad local + intercept 1/2/3 + horario continuo + Square guardrails.

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

// ====== Config horario (continuo)
const WORK_DAYS = [1,2,3,4,5]
const SLOT_MIN = 30
const OPEN = { start: 9, end: 20 }
const NOW_MIN_OFFSET_MIN = Number(process.env.BOT_NOW_OFFSET_MIN || 30)
const HOLIDAYS_EXTRA = (process.env.HOLIDAYS_EXTRA || "06/01,28/02,15/08,12/10,01/11,06/12,08/12,25/12")
  .split(",").map(s=>s.trim()).filter(Boolean)

// ====== Flags
const BOT_DEBUG = /^true$/i.test(process.env.BOT_DEBUG || "")

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
const AI_MAX_RETRIES = Number(process.env.AI_MAX_RETRIES || 2)
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

function proposeSlots({ fromEU, durationMin=60, n=3 }){
  const out=[]
  let t=ceilToSlotEU(fromEU.clone())
  t=nextOpeningFrom(t)
  while (out.length<n){
    if (insideBusinessHours(t,durationMin)){
      out.push(t.clone())
      t=t.add(SLOT_MIN,"minute")
    } else {
      const nowMin = t.hour()*60 + t.minute()
      const closeMin = OPEN.end*60
      if (nowMin >= closeMin){
        t = t.add(1,"day").hour(OPEN.start).minute(0).second(0).millisecond(0)
      } else {
        t = t.add(SLOT_MIN,"minute")
      }
      while (!WORK_DAYS.includes(t.day()) || isHolidayEU(t)) {
        t = t.add(1,"day").hour(OPEN.start).minute(0).second(0).millisecond(0)
      }
    }
  }
  return out
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
  square_booking_id TEXT
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
  PRIMARY KEY (phone, message_id)
);
`)
const insertAppt = db.prepare(`INSERT INTO appointments
(id,customer_name,customer_phone,customer_square_id,location_key,service_env_key,service_label,duration_min,start_iso,end_iso,staff_id,status,created_at,square_booking_id)
VALUES (@id,@customer_name,@customer_phone,@customer_square_id,@location_key,@service_env_key,@service_label,@duration_min,@start_iso,@end_iso,@staff_id,@status,@created_at,@square_booking_id)`)
const insertAIConversation = db.prepare(`INSERT OR REPLACE INTO ai_conversations
(phone, message_id, user_message, ai_response, timestamp, session_data)
VALUES (@phone, @message_id, @user_message, @ai_response, @timestamp, @session_data)`)

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
  c.pendingDateTime_ms = s.pendingDateTime? s.pendingDateTime.valueOf(): null
  delete c.lastHours; delete c.pendingDateTime
  const j=JSON.stringify(c)
  const up=db.prepare(`UPDATE sessions SET data_json=@j, updated_at=@u WHERE phone=@p`).run({j,u:new Date().toISOString(),p:phone})
  if (up.changes===0) db.prepare(`INSERT INTO sessions (phone,data_json,updated_at) VALUES (@p,@j,@u)`).run({p:phone,j,u:new Date().toISOString()})
}
function clearSession(phone){ db.prepare(`DELETE FROM sessions WHERE phone=@phone`).run({phone}) }

// ====== Empleadas (.env)
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

// ====== Servicios
function servicesForSedeKeyRaw(sedeKey){
  const prefix = (sedeKey==="la_luz") ? "SQ_SVC_luz_" : "SQ_SVC_"
  const out=[]
  for (const [k,v] of Object.entries(process.env)){
    if (!k.startsWith(prefix)) continue
    const [id] = String(v||"").split("|"); if (!id) continue
    const label = k.replace(prefix,"").replaceAll("_"," ").replace(/\b([a-z])/g,m=>m.toUpperCase()).replace("Pestan","Pestañ")
    out.push({ key:k, id, rawKey:k, label })
  }
  return out
}
function findServiceByText(sedeKey, txt){
  const pool = servicesForSedeKeyRaw(sedeKey)
  const q = norm(txt)
  // heurísticas mínimas (2D/3D/pelo a pelo)
  const score = (lab)=>{
    const n = norm(lab)
    let s = 0
    if (/\b2d\b/.test(q) && /\b2d\b/.test(n)) s+=3
    if (/\b3d\b/.test(q) && /\b3d\b/.test(n)) s+=3
    if (/pelo a pelo|peloapelo|clásic/.test(q) && /pelo a pelo|clas/i.test(n)) s+=3
    if (/pesta/.test(q) && /pesta/.test(n)) s+=1
    if (/extens/.test(q) && /extens/.test(n)) s+=1
    return s
  }
  let best=null, bestS=-1
  for (const s of pool){ const sc=score(s.label); if (sc>bestS){ bestS=sc; best=s } }
  return (bestS>0)? best : null
}

// ====== Square helpers
async function getServiceIdAndVersion(envKey){
  const raw = process.env[envKey]; if (!raw) return null
  let [id, ver] = String(raw).split("|"); ver=ver?Number(ver):null
  if (!id) return null
  if (!ver){
    try{ const resp=await square.catalogApi.retrieveCatalogObject(id,true); ver=resp?.result?.object?.version?Number(resp.result.object.version):1 }catch{ ver=1 }
  }
  return {id,version:ver||1}
}
async function findOrCreateCustomer({ name, email, phone }){
  try{
    const e164=normalizePhoneES(phone); if(!e164) return null
    const got = await square.customersApi.searchCustomers({ query:{ filter:{ phoneNumber:{ exact:e164 } } } })
    const c=(got?.result?.customers||[])[0]; if (c) return c
  }catch{}
  try{
    const created = await square.customersApi.createCustomer({
      idempotencyKey:`cust_${Date.now()}_${Math.random().toString(36).slice(2,6)}`,
      givenName:name||undefined,
      emailAddress:email||undefined,
      phoneNumber:normalizePhoneES(phone)||undefined
    })
    return created?.result?.customer||null
  }catch{ return null }
}
async function createBooking({ startEU, locationKey, envServiceKey, durationMin, customerId, teamMemberId }){
  if (!envServiceKey) return null
  if (!teamMemberId || typeof teamMemberId!=="string" || !teamMemberId.trim()){ console.error("createBooking: teamMemberId requerido"); return null }
  if (DRY_RUN) return { id:`TEST_SIM_${Date.now()}`, __sim:true }
  const sv = await getServiceIdAndVersion(envServiceKey); if (!sv?.id || !sv?.version) return null
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
    const b = resp?.result?.booking || null
    if (b) console.log("✅ Booking created:", b.id)
    return b
  }catch(e){
    console.error("createBooking:", e?.message||e, e?.body||"")
    return null
  }
}
async function cancelBooking(bookingId){
  if (DRY_RUN) return true
  try{
    const body = { idempotencyKey:`cancel_${bookingId}_${Date.now()}` }
    const resp = await square.bookingsApi.cancelBooking(bookingId, body)
    return !!resp?.result?.booking
  }catch(e){ console.error("cancelBooking:", e?.message||e); return false }
}
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
      const list=resp?.result?.bookings||[]; const nowISO=new Date().toISOString()
      for (const b of list){
        if (!b?.startAt || b.startAt<nowISO) continue
        const start=dayjs.tz(b.startAt,EURO_TZ)
        const seg=(b.appointmentSegments||[{}])[0]
        items.push({
          index:items.length+1,
          id:b.id,
          fecha_iso:start.format("YYYY-MM-DD"),
          pretty:fmtES(start),
          sede: locationNice(idToLocKey(b.locationId)||""),
          profesional: staffLabelFromId(seg?.teamMemberId) || "Profesional",
          servicio: "Servicio"
        })
      }
      items.sort((a,b)=>a.fecha_iso.localeCompare(b.fecha_iso) || a.pretty.localeCompare(b.pretty))
    }catch(e){ console.error("listBookings:", e?.message||e) }
  }
  return items
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
      query:{
        filter:{
          startAtRange:{ startAt, endAt },
          locationId,
          segmentFilters:[{
            serviceVariationId: sv.id,
            teamMemberIdFilter:{ any:[ staffId ] }
          }]
        }
      }
    }
    const resp = await square.bookingsApi.searchAvailability(body)
    const avail = resp?.result?.availabilities || []
    const slots=[]
    const seenDays=new Set()
    for (const a of avail){
      if (!a?.startAt) continue
      const d=dayjs.tz(a.startAt, EURO_TZ)
      if (!insideBusinessHours(d,60)) continue
      if (distinctDays){
        const key=d.format("YYYY-MM-DD")
        if (seenDays.has(key)) continue
        seenDays.add(key)
      }
      slots.push({ date:d, staffId })
      if (slots.length>=n) break
    }
    return slots
  }catch(e){
    if (BOT_DEBUG) console.error("searchAvailabilityForStaff error:", e?.message||e, e?.body||"")
    return []
  }
}
async function searchAvailabilityGeneric({ locationKey, envServiceKey, fromEU, days=14, n=3, distinctDays=false }){
  try{
    const sv = await getServiceIdAndVersion(envServiceKey)
    if (!sv?.id) return []
    const startAt = fromEU.tz("UTC").toISOString()
    const endAt = fromEU.clone().add(days,"day").tz("UTC").toISOString()
    const locationId = locationToId(locationKey)
    const body = {
      query:{
        filter:{
          startAtRange:{ startAt, endAt },
          locationId,
          segmentFilters:[{ serviceVariationId: sv.id }]
        }
      }
    }
    const resp = await square.bookingsApi.searchAvailability(body)
    const avail = resp?.result?.availabilities || []
    const slots=[]
    const seenDays=new Set()
    for (const a of avail){
      if (!a?.startAt) continue
      const d = dayjs.tz(a.startAt, EURO_TZ)
      if (!insideBusinessHours(d,60)) continue
      let tm = null
      const segs = Array.isArray(a.appointmentSegments) ? a.appointmentSegments
                 : Array.isArray(a.segments) ? a.segments
                 : []
      if (segs[0]?.teamMemberId) tm = segs[0].teamMemberId
      if (distinctDays){
        const key=d.format("YYYY-MM-DD")
        if (seenDays.has(key)) continue
        seenDays.add(key)
      }
      slots.push({ date:d, staffId: tm })
      if (slots.length>=n) break
    }
    return slots
  }catch(e){
    if (BOT_DEBUG) console.error("searchAvailabilityGeneric error:", e?.message||e, e?.body||"")
    return []
  }
}

// ====== IA (retries)
async function callAIOnce(messages, systemPrompt = "") {
  try {
    const allMessages = systemPrompt ? 
      [{ role: "system", content: systemPrompt }, ...messages] : 
      messages;
    const response = await fetch("https://api.deepseek.com/chat/completions", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${AI_API_KEY}`
      },
      body: JSON.stringify({
        model: AI_MODEL,
        messages: allMessages,
        max_tokens: 1000,
        temperature: 0.7,
        stream: false
      })
    });
    if (!response.ok) {
      const errorText = await response.text();
      console.error("DeepSeek API Error:", response.status, errorText);
      return null
    }
    const data = await response.json();
    return data?.choices?.[0]?.message?.content ?? null
  } catch (error) {
    console.error("Error calling DeepSeek AI:", error?.message||error)
    return null
  }
}
async function callAIWithRetries(messages, systemPrompt=""){
  for (let i=0;i<=AI_MAX_RETRIES;i++){
    const res = await callAIOnce(messages, systemPrompt)
    if (res && typeof res==="string" && res.trim()) return res
    await sleep(300 * (i+1))
  }
  return null
}

// ====== Fallback local + parsers
function extractIdentity(msg){
  const emailMatch = msg.match(/[\w.%+-]+@[\w.-]+\.[A-Za-z]{2,}/)
  const email = emailMatch ? emailMatch[0] : null
  // nombre = línea limpia si parece nombre (2+ palabras, letras/espacios)
  const raw = msg.replace(/\s+/g," ").trim()
  const onlyLetters = raw.replace(/[^A-Za-zÁÉÍÓÚÜÑáéíóúüñ\s'-]/g,"")
  const words = onlyLetters.split(" ").filter(Boolean)
  const name = (!email && words.length>=2 && onlyLetters.length<=60) ? onlyLetters : null
  return { name, email }
}
function hasCore(s){ return s?.sede && s?.selectedServiceEnvKey && s?.pendingDateTime }
function haveIdentity(s){ return !!(s?.name || s?.email) }

function looksLikeConfirm(text){
  const t=(text||"").toLowerCase()
  return /reserva (confirmada|creada|hecha)|confirmad[oa]|he reservado|queda agendada|cita confirmada|cita creada/.test(t)
}

function buildLocalFallback(userMessage, sessionData){
  const msg = String(userMessage||"").trim()
  const lower = norm(msg)
  const numMatch = lower.match(/^(?:opcion|opción)?\s*([1-5])\b/)
  const yesMatch = /\b(si|sí|ok|vale|confirmo|de\ acuerdo)\b/i.test(msg)
  const cancelMatch = /\b(cancelar|anular|borra|elimina)\b/i.test(lower)
  const listMatch = /\b(mis citas|lista|ver citas)\b/i.test(lower)
  const bookMatch = /\b(reservar|cita|quiero.*(cita|reservar))\b/i.test(lower)

  // Captura identidad si estábamos esperando
  if ((sessionData?.stage||"")==="awaiting_identity"){
    const id = extractIdentity(msg)
    if (id.name || id.email){
      return {
        message: "¡Gracias! Voy a confirmarlo.",
        action: haveIdentity({...sessionData, ...id}) && hasCore(sessionData) ? "create_booking" : "need_info",
        session_updates: { ...id, stage: null },
        action_params: {}
      }
    }
  }

  // 1) Número con lastHours
  if (numMatch && Array.isArray(sessionData?.lastHours) && sessionData.lastHours.length){
    const idx = Number(numMatch[1]) - 1
    const pick = sessionData.lastHours[idx]
    if (dayjs.isDayjs(pick)){
      const iso = pick.format("YYYY-MM-DDTHH:mm")
      const staffFromIso = sessionData?.lastStaffByIso?.[iso] || null
      const updates = {
        pendingDateTime: pick.tz(EURO_TZ).toISOString(),
        stage: haveIdentity(sessionData) ? null : "awaiting_identity"
      }
      if (staffFromIso) {
        updates.preferredStaffId = staffFromIso
        updates.preferredStaffLabel = null
      }
      const okToCreate = hasCore({...sessionData, ...updates}) && haveIdentity(sessionData)
      return {
        message: okToCreate ? "Perfecto, confirmo esa hora ✨" : "Genial. Dime tu nombre (o email) para terminar la reserva.",
        action: okToCreate ? "create_booking" : "need_info",
        session_updates: updates,
        action_params: {}
      }
    }
  }

  // 2) Sí / ok
  if (yesMatch){
    if (hasCore(sessionData) && haveIdentity(sessionData)){
      return { message:"¡Voy a crear la reserva! ✨", action:"create_booking", session_updates:{}, action_params:{} }
    } else {
      const faltan=[]
      if (!sessionData?.sede) faltan.push("sede")
      if (!sessionData?.selectedServiceEnvKey) faltan.push("servicio")
      if (!sessionData?.pendingDateTime) faltan.push("fecha y hora")
      if (!haveIdentity(sessionData)) faltan.push("nombre o email")
      return { 
        message:`Me faltan: ${faltan.join(", ")}.`,
        action:"need_info",
        session_updates:{},
        action_params:{} 
      }
    }
  }

  // 3) Cancelar (no si estamos esperando identidad/hora)
  if (cancelMatch && !/^awaiting_/.test(sessionData?.stage||"")){
    return { message:"Vale, te muestro tus citas y eliges cuál cancelar.", action:"cancel_appointment", session_updates:{}, action_params:{} }
  }

  // 4) Listado
  if (listMatch){
    return { message:"Estas son tus próximas citas:", action:"list_appointments", session_updates:{}, action_params:{} }
  }

  // 5) Reservar
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

  // 6) Intento de identificar servicio 2D/3D/pelo a pelo si ya dijo sede
  if (sessionData?.sede && !sessionData?.selectedServiceEnvKey){
    const s = findServiceByText(sessionData.sede, msg)
    if (s) {
      return { message:`Perfecto: ${s.label}. Te propongo horas.`, action:"propose_times", session_updates:{ selectedServiceEnvKey:s.key, selectedServiceLabel:s.label }, action_params:{} }
    }
  }

  return { message:"¿Quieres reservar, cancelar o ver tus citas? Si es para reservar, dime sede y servicio.", action:"none", session_updates:{}, action_params:{} }
}

// ====== System prompt (abreviado para foco)
function buildSystemPrompt() {
  const nowEU = dayjs().tz(EURO_TZ);
  const employees = EMPLOYEES.map(e => ({ id: e.id, labels: e.labels, bookable: e.bookable, locations: e.allow }));
  const torremolinos_services = servicesForSedeKeyRaw("torremolinos");
  const laluz_services = servicesForSedeKeyRaw("la_luz");
  return `Eres el asistente de WhatsApp para Gapink Nails (L-V 09:00-20:00).
Devuelve SOLO JSON válido: {"message":"","action":"","session_updates":{},"action_params":{}}
Si no hay huecos con la profesional preferida, no digas "con <nombre>"; indica "nuestro equipo" y muestra el staff por opción. Confirma solo una vez.`
}

async function getAIResponse(userMessage, sessionData, phone) {
  const systemPrompt = buildSystemPrompt();
  const recent = db.prepare(`
    SELECT user_message, ai_response 
    FROM ai_conversations 
    WHERE phone = ? 
    ORDER BY timestamp DESC 
    LIMIT 5
  `).all(phone);
  const conversationHistory = recent.reverse().map(msg => [
    { role: "user", content: msg.user_message },
    { role: "assistant", content: msg.ai_response }
  ]).flat();

  const messages = [
    ...conversationHistory,
    { role: "user", content: `Mensaje: "${userMessage}"\n\nEstado:\n${JSON.stringify(sessionData, null, 2)}` }
  ];

  const aiText = await callAIWithRetries(messages, systemPrompt)
  if (!aiText || /^error de conexión/i.test(aiText.trim())) return buildLocalFallback(userMessage, sessionData)

  const cleaned = aiText.replace(/```json\s*/gi, "").replace(/```\s*/g, "").trim()
  try {
    return JSON.parse(cleaned)
  } catch {
    return buildLocalFallback(userMessage, sessionData)
  }
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
  await new Promise(r=>setTimeout(r, 600+Math.random()*1000))
  return sock.sendMessage(jid, { text })
}

// ====== Acciones
async function executeProposeTime(params, sessionData, phone, sock, jid) {
  const nowEU = dayjs().tz(EURO_TZ);
  const baseFrom = nextOpeningFrom(nowEU.add(NOW_MIN_OFFSET_MIN, "minute"));
  if (!sessionData.sede || !sessionData.selectedServiceEnvKey) {
    await sendWithPresence(sock, jid, "Necesito que me digas la sede y el servicio primero.");
    return;
  }

  let slots = []
  let usedPreferred = false

  if (sessionData.preferredStaffId) {
    const staffSlots = await searchAvailabilityForStaff({
      locationKey: sessionData.sede,
      envServiceKey: sessionData.selectedServiceEnvKey,
      staffId: sessionData.preferredStaffId,
      fromEU: baseFrom,
      n: 3
    })
    if (staffSlots.length){ slots = staffSlots; usedPreferred = true }
  }
  if (!slots.length) {
    const generic = await searchAvailabilityGeneric({
      locationKey: sessionData.sede,
      envServiceKey: sessionData.selectedServiceEnvKey,
      fromEU: baseFrom,
      n: 3
    })
    slots = generic
  }
  if (!slots.length) {
    const generalSlots = proposeSlots({ fromEU: baseFrom, durationMin: 60, n: 3 });
    slots = generalSlots.map(d => ({ date: d, staffId: null }))
  }
  if (!slots.length) {
    await sendWithPresence(sock, jid, "No encuentro horarios disponibles en los próximos días. ¿Te interesa otra fecha?");
    return;
  }

  const hoursEnum = enumerateHours(slots.map(s => s.date))
  const map = {}
  for (const s of slots) map[s.date.format("YYYY-MM-DDTHH:mm")] = s.staffId || null
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
    : `Horarios disponibles (nuestro equipo):` + (sessionData.preferredStaffLabel ? `\nNota: no hay huecos con ${sessionData.preferredStaffLabel} en los próximos días.`:"")

  await sendWithPresence(sock, jid, `${header}\n${lines}\n\nResponde con el número (1, 2 o 3)`)
}

async function executeCreateBooking(params, sessionData, phone, sock, jid) {
  if (!sessionData.sede) { await sendWithPresence(sock, jid, "Falta seleccionar la sede (Torremolinos o La Luz)"); return; }
  if (!sessionData.selectedServiceEnvKey) { await sendWithPresence(sock, jid, "Falta seleccionar el servicio"); return; }
  if (!sessionData.pendingDateTime) { await sendWithPresence(sock, jid, "Falta seleccionar la fecha y hora"); return; }

  const startEU = dayjs.tz(sessionData.pendingDateTime, EURO_TZ);
  if (!insideBusinessHours(startEU, 60)) {
    await sendWithPresence(sock, jid, "Esa hora está fuera del horario de atención (L-V 09:00–20:00)");
    return;
  }

  const iso = startEU.format("YYYY-MM-DDTHH:mm")
  let staffId = sessionData.lastStaffByIso?.[iso] || sessionData.preferredStaffId || null
  if (!staffId) {
    const probe = await searchAvailabilityGeneric({
      locationKey: sessionData.sede,
      envServiceKey: sessionData.selectedServiceEnvKey,
      fromEU: startEU.clone().subtract(1, "minute"),
      days: 1,
      n: 10
    })
    const match = probe.find(x => x.date.isSame(startEU, "minute"))
    if (match?.staffId) staffId = match.staffId
  }
  if (!staffId) staffId = pickStaffForLocation(sessionData.sede, null)
  if (!staffId) { await sendWithPresence(sock, jid, "No hay profesionales disponibles en esa sede"); return; }

  const customer = await findOrCreateCustomer({ name: sessionData.name, email: sessionData.email, phone })
  if (!customer) { await sendWithPresence(sock, jid, "Para completar la reserva necesito tu nombre o email"); return; }

  const booking = await createBooking({
    startEU,
    locationKey: sessionData.sede,
    envServiceKey: sessionData.selectedServiceEnvKey,
    durationMin: 60,
    customerId: customer.id,
    teamMemberId: staffId
  })

  if (!booking) { await sendWithPresence(sock, jid, "No pude crear la reserva. ¿Prefieres otro horario?"); return; }
  if (booking.__sim) {
    await sendWithPresence(sock, jid, "SIMULACIÓN: Reserva creada (DRY_RUN activo). No se ha tocado Square.")
    return
  }

  const aptId = `apt_${Math.random().toString(36).slice(2,8)}${Date.now().toString(36).slice(-4)}`
  insertAppt.run({
    id: aptId,
    customer_name: customer?.givenName || null,
    customer_phone: phone,
    customer_square_id: customer.id,
    location_key: sessionData.sede,
    service_env_key: sessionData.selectedServiceEnvKey,
    service_label: sessionData.selectedServiceLabel || "Servicio",
    duration_min: 60,
    start_iso: startEU.tz("UTC").toISOString(),
    end_iso: startEU.clone().add(60, "minute").tz("UTC").toISOString(),
    staff_id: staffId,
    status: "confirmed",
    created_at: new Date().toISOString(),
    square_booking_id: booking.id
  })

  const staffName = staffLabelFromId(staffId) || sessionData.preferredStaffLabel || "nuestro equipo";
  const address = sessionData.sede === "la_luz" ? ADDRESS_LUZ : ADDRESS_TORRE;
  
  const confirmMessage = `🎉 ¡Reserva confirmada!

📍 ${locationNice(sessionData.sede)}
${address}

💅 ${sessionData.selectedServiceLabel || "Servicio"}
👩‍💼 ${staffName}
📅 ${fmtES(startEU)}
⏱️ 60 minutos

¡Te esperamos!`;

  await sendWithPresence(sock, jid, confirmMessage);
  clearSession(phone);
}

async function executeListAppointments(params, sessionData, phone, sock, jid) {
  const appointments = await enumerateCitasByPhone(phone);
  if (!appointments.length) { await sendWithPresence(sock, jid, "No tienes citas programadas. ¿Quieres agendar una?"); return; }
  const message = `Tus próximas citas:\n\n${appointments.map(apt => 
    `${apt.index}) ${apt.pretty}\n📍 ${apt.sede}\n👩‍💼 ${apt.profesional}\n`
  ).join("\n")}`;
  await sendWithPresence(sock, jid, message);
}

async function executeCancelAppointment(params, sessionData, phone, sock, jid) {
  const appointments = await enumerateCitasByPhone(phone);
  if (!appointments.length) { await sendWithPresence(sock, jid, "No tienes citas para cancelar"); return; }
  const appointmentIndex = params.appointmentIndex;
  if (!appointmentIndex) {
    const message = `¿Cuál cita quieres cancelar?\n\n${appointments.map(apt => 
      `${apt.index}) ${apt.pretty} - ${apt.sede}`
    ).join("\n")}\n\nResponde con el número`;
    await sendWithPresence(sock, jid, message);
    return;
  }
  const appointment = appointments.find(apt => apt.index === appointmentIndex);
  if (!appointment) { await sendWithPresence(sock, jid, "No encontré esa cita. ¿Puedes verificar el número?"); return; }
  const success = await cancelBooking(appointment.id);
  if (success) await sendWithPresence(sock, jid, `✅ Cita cancelada: ${appointment.pretty} en ${appointment.sede}`);
  else await sendWithPresence(sock, jid, "No pude cancelar la cita. Por favor contacta directamente al salón.");
}

// ====== Mini-web + QR
const app=express()
const PORT=process.env.PORT||8080
let lastQR=null, conectado=false

app.get("/", (_req,res)=>{
  res.send(`<!doctype html><meta charset="utf-8"><style>
  body{font-family:system-ui;display:grid;place-items:center;min-height:100vh}
  .card{max-width:560px;padding:24px;border-radius:16px;box-shadow:0 6px 24px rgba(0,0,0,.08)}
  </style><div class="card"><h1>Gapink Nails v26.4</h1>
  <p>Estado: ${conectado?"✅ Conectado":"❌ Desconectado"}</p>
  ${!conectado&&lastQR?`<img src="/qr.png" width="300">`:""}
  <p style="opacity:.7">Modo: ${DRY_RUN?"Simulación (no toca Square)":"Producción"}</p>
  <p style="opacity:.7">Horario: L-V 09:00–20:00</p>
  <p style="opacity:.7">IA: DeepSeek (${AI_MODEL}) · Retries: ${AI_MAX_RETRIES}</p>
  <p style="color:#e74c3c">🤖 Anti-fake confirm + identidad local</p>
  </div>`)
})

app.get("/qr.png", async (_req,res)=>{
  if(!lastQR) return res.status(404).send("No QR")
  const png = await qrcode.toBuffer(lastQR, { type:"png", width:512, margin:1 })
  res.set("Content-Type","image/png").send(png)
})

app.listen(PORT, ()=>{ console.log("🌐 Web", PORT); startBot().catch(console.error) })

// ====== Baileys
async function loadBaileys(){
  const require = createRequire(import.meta.url); let mod=null
  try{ mod=require("@whiskeysockets/baileys") }catch{}; if(!mod){ try{ mod=await import("@whiskeysockets/baileys") }catch{} }
  if(!mod) throw new Error("No se pudo cargar @whiskeysockets/baileys")
  const makeWASocket = mod.makeWASocket || mod.default?.makeWASocket || (typeof mod.default==="function"?mod.default:undefined)
  const useMultiFileAuthState = mod.useMultiFileAuthState || mod.default?.useMultiFileAuthState
  const fetchLatestBaileysVersion = mod.fetchLatestBaileysVersion || mod.default?.fetchLatestBaileysVersion || (async()=>({version:[2,3000,0]}))
  const Browsers = mod.Browsers || mod.default?.Browsers || { macOS:(n="Desktop")=>["MacOS",n,"121.0.0"] }
  if (typeof makeWASocket!=="function" || typeof useMultiFileAuthState!=="function") throw new Error("Baileys incompatible")
  return { makeWASocket, useMultiFileAuthState, fetchLatestBaileysVersion, Browsers }
}

async function startBot(){
  try{
    const { makeWASocket, useMultiFileAuthState, fetchLatestBaileysVersion, Browsers } = await loadBaileys()
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
      if (qr){ lastQR=qr; conectado=false; try{ qrcodeTerminal.generate(qr,{small:true}) }catch{} }
      if (connection==="open"){ lastQR=null; conectado=true; RECONNECT_ATTEMPTS=0; RECONNECT_SCHEDULED=false; console.log("✅ WhatsApp listo") }
      if (connection==="close"){ 
        conectado=false; console.log("❌ Conexión cerrada. Reintentando…"); 
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
            greeted: false,
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
            stage: null
          }
          if (sessionData.last_msg_id === m.key.id) return
          sessionData.last_msg_id = m.key.id
          if (BOT_DEBUG) {
            console.log("[DEBUG] User message:", textRaw)
            console.log("[DEBUG] Session before:", sessionData)
          }

          // === PRE: si esperamos identidad, intenta capturarla localmente
          if ((sessionData.stage||"")==="awaiting_identity"){
            const id = extractIdentity(textRaw)
            if (id.name || id.email){
              sessionData = { ...sessionData, ...id, stage:null }
              saveSession(phone, sessionData)
            }
          }

          // === PRE-INTERCEPT: selección 1/2/3 ANTES de la IA
          const lower = norm(textRaw)
          const numMatch = lower.match(/^(?:opcion|opción)?\s*([1-5])\b/)
          if (numMatch && Array.isArray(sessionData.lastHours) && sessionData.lastHours.length){
            const idx = Number(numMatch[1]) - 1
            const pick = sessionData.lastHours[idx]
            if (dayjs.isDayjs(pick)){
              const iso = pick.format("YYYY-MM-DDTHH:mm")
              const staffFromIso = sessionData?.lastStaffByIso?.[iso] || null
              sessionData.pendingDateTime = pick.tz(EURO_TZ).toISOString()
              if (staffFromIso){ sessionData.preferredStaffId = staffFromIso; sessionData.preferredStaffLabel = null }
              sessionData.stage = (sessionData.name || sessionData.email) ? null : "awaiting_identity"
              saveSession(phone, sessionData)
              const okToCreate = hasCore(sessionData) && haveIdentity(sessionData)
              const aiObj = okToCreate 
                ? { message:"Confirmo esa hora ✨", action:"create_booking", session_updates:{}, action_params:{} }
                : { message:"Genial. Dime tu nombre (o email) para terminar la reserva.", action:"need_info", session_updates:{}, action_params:{} }
              await routeAIResult(aiObj, sessionData, textRaw, m, phone, sock, jid)
              return
            }
          }

          // === IA o fallback local
          let aiObj = await getAIResponse(textRaw, sessionData, phone)

          // === Anti-fake confirm: si IA dice "confirmada" sin action, forzamos create_booking con guardas
          if (aiObj && looksLikeConfirm(aiObj.message) && aiObj.action !== "create_booking"){
            if (hasCore(sessionData) && haveIdentity(sessionData)){
              aiObj = { ...aiObj, action:"create_booking" }
            } else {
              const faltan=[]
              if (!sessionData?.sede) faltan.push("sede")
              if (!sessionData?.selectedServiceEnvKey) faltan.push("servicio")
              if (!sessionData?.pendingDateTime) faltan.push("fecha y hora")
              if (!haveIdentity(sessionData)) faltan.push("nombre o email")
              aiObj = { message:`Para confirmar de verdad me faltan: ${faltan.join(", ")}.`, action:"need_info", session_updates: aiObj.session_updates||{}, action_params:{} }
            }
          }

          await routeAIResult(aiObj, sessionData, textRaw, m, phone, sock, jid)

        } catch (error) {
          console.error("Error processing message:", error)
          await sendWithPresence(sock, jid, "Disculpa, hubo un error técnico. ¿Puedes repetir tu mensaje?")
        }
      })
    })
  }catch(e){ 
    console.error("startBot:", e?.message||e) 
  }
}

async function routeAIResult(aiObj, sessionData, textRaw, m, phone, sock, jid){
  if (BOT_DEBUG) console.log("[DEBUG] AI (obj):", aiObj)

  if (aiObj.session_updates) {
    Object.keys(aiObj.session_updates).forEach(key => {
      if (aiObj.session_updates[key] !== null && aiObj.session_updates[key] !== undefined) {
        sessionData[key] = aiObj.session_updates[key]
      }
    })
  }

  insertAIConversation.run({
    phone,
    message_id: m.key.id,
    user_message: textRaw,
    ai_response: JSON.stringify(aiObj),
    timestamp: new Date().toISOString(),
    session_data: JSON.stringify(sessionData)
  })
  saveSession(phone, sessionData)

  switch (aiObj.action) {
    case "propose_times":
      await executeProposeTime(aiObj.action_params, sessionData, phone, sock, jid)
      break
    case "create_booking":
      await executeCreateBooking(aiObj.action_params, sessionData, phone, sock, jid)
      break
    case "list_appointments":
      await executeListAppointments(aiObj.action_params, sessionData, phone, sock, jid)
      break
    case "cancel_appointment":
      await executeCancelAppointment(aiObj.action_params, sessionData, phone, sock, jid)
      break
    case "need_info":
    case "none":
    default:
      await sendWithPresence(sock, jid, aiObj.message || "¿Puedes repetirlo, por fa?")
      break
  }
}

// ====== Arranque
startBot().catch(console.error)

// Señales
process.on("uncaughtException", (e)=>console.error("uncaughtException:", e?.stack||e?.message||e))
process.on("unhandledRejection", (e)=>console.error("unhandledRejection:", e))
process.on("SIGTERM", ()=>{ console.log("🛑 SIGTERM recibido"); process.exit(0) })
process.on("SIGINT", ()=>{ console.log("🛑 SIGINT recibido"); process.exit(0) })
