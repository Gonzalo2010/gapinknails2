// index.js — Gapink Nails · v24.0
// Staff LOCK (Desi), categorías primero, “otro día/otra hora” con Desi, cancelación real por usuario (próxima o lista breve)

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

// ====== Horario negocio
const WORK_DAYS = [1,2,3,4,5]
const SLOT_MIN = 30
const MORNING = { start:10, end:14 }
const AFTERNOON = { start:16, end:20 }
const NOW_MIN_OFFSET_MIN = Number(process.env.BOT_NOW_OFFSET_MIN || 30)
const HOLIDAYS_EXTRA = (process.env.HOLIDAYS_EXTRA || "06/01,28/02,15/08,12/10,01/11,06/12,08/12,25/12")
  .split(",").map(s=>s.trim()).filter(Boolean)

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

// ====== LLM (DeepSeek/OpenAI compatible)
const LLM_API_KEY = process.env.DEEPSEEK_API_KEY || process.env.OPENAI_API_KEY || ""
const LLM_MODEL   = process.env.DEEPSEEK_MODEL   || process.env.OPENAI_MODEL || "deepseek-chat"
const LLM_URL     = process.env.DEEPSEEK_API_URL || "https://api.deepseek.com/v1/chat/completions"

// ====== Prompt del orquestador
const SYSTEM_PROMPT = `[SYSTEM ROLE — ORQUESTADOR DE CITAS GAPINK NAILS] 
Eres una IA que clasifica y guía el flujo de reservas de un salón con dos sedes (Torremolinos y Málaga–La Luz). No llamas a APIs ni "haces" reservas: SOLO devuelves JSON con decisiones y un mensaje listo para enviar al cliente. No inventes datos. Usa índices 1-based para listas. Devuelve SIEMPRE un único JSON con "client_message".
Opciones: 1 Concertar, 2 Cancelar, 3 Editar, 4 Hola, 5 Información.
Constantes: sedes={torremolinos, la_luz}, TZ=Europe/Madrid, no inventes huecos.`

async function aiChat(messages, { temperature=0.2, retries=3 } = {}){
  if (!LLM_API_KEY) return ""
  let err=null
  for (let i=0;i<=retries;i++){
    try{
      const r = await fetch(LLM_URL, {
        method:"POST",
        headers:{ "Content-Type":"application/json", "Authorization":`Bearer ${LLM_API_KEY}` },
        body: JSON.stringify({ model: LLM_MODEL, messages, temperature })
      })
      if (!r.ok) throw new Error(`HTTP ${r.status}`)
      const j = await r.json()
      return (j?.choices?.[0]?.message?.content || "").trim()
    }catch(e){
      err=e; await new Promise(r=>setTimeout(r, 300*Math.pow(2,i)))
    }
  }
  console.error("aiChat failed:", err?.message||err); return ""
}

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

const LOC_SYNON = {
  la_luz:[/\bluz\b/i,/\bmalaga\b/i,/\bmálaga\b/i],
  torremolinos:[/\btorre\b/i,/\btorremolinos\b/i]
}
function detectSedeFromText(t){
  const low=norm(t)
  for (const [k,rs] of Object.entries(LOC_SYNON)) if (rs.some(r=>r.test(low))) return k
  return null
}
function wantsChangeSede(t){
  const x=norm(t)
  return /\b(cambiar|mejor|prefiero|pasar a|voy a)\b/.test(x) && (/\bluz\b|\bmalaga\b|\bmálaga\b|\btorre\b|\btorremolinos\b/.test(x))
}

// Horario helpers
function isHolidayEU(d){ const dd=String(d.date()).padStart(2,"0"), mm=String(d.month()+1).padStart(2,"0"); return HOLIDAYS_EXTRA.includes(`${dd}/${mm}`) }
function insideBlock(d,b){ return d.hour()>=b.start && d.hour()<b.end }
function insideBusinessHours(d,dur){
  const t=d.clone(); if (!WORK_DAYS.includes(t.day())) return false; if (isHolidayEU(t)) return false
  const end=t.clone().add(dur,"minute")
  return (insideBlock(t,MORNING)&&insideBlock(end,MORNING)&&t.isSame(end,"day")) || (insideBlock(t,AFTERNOON)&&insideBlock(end,AFTERNOON)&&t.isSame(end,"day"))
}
function nextOpeningFrom(d){
  let t=d.clone()
  if (t.hour()>=AFTERNOON.end) t=t.add(1,"day").hour(MORNING.start).minute(0).second(0).millisecond(0)
  else if (t.hour()>=MORNING.end && t.hour()<AFTERNOON.start) t=t.hour(AFTERNOON.start).minute(0).second(0).millisecond(0)
  else if (t.hour()<MORNING.start) t=t.hour(MORNING.start).minute(0).second(0).millisecond(0)
  while (!WORK_DAYS.includes(t.day()) || isHolidayEU(t)) t=t.add(1,"day").hour(MORNING.start).minute(0).second(0).millisecond(0)
  return t
}
function ceilToSlotEU(t){ const m=t.minute(), rem=m%SLOT_MIN; return rem===0 ? t.second(0).millisecond(0) : t.add(SLOT_MIN-rem,"minute").second(0).millisecond(0) }
function fmtES(d){ const dias=["domingo","lunes","martes","miércoles","jueves","viernes","sábado"]; const t=(dayjs.isDayjs(d)?d:dayjs(d)).tz(EURO_TZ); return `${dias[t.day()]} ${String(t.date()).padStart(2,"0")}/${String(t.month()+1).padStart(2,"0")} ${String(t.hour()).padStart(2,"0")}:${String(t.minute()).padStart(2,"0")}` }
function enumerateHours(list){ return list.map((d,i)=>({ index:i+1, iso:d.format("YYYY-MM-DDTHH:mm"), pretty:fmtES(d) })) }
function stableKey(parts){ const raw=Object.values(parts).join("|"); return createHash("sha256").update(raw).digest("hex").slice(0,48) }

// Fallback de slots por horario comercial
function proposeSlots({ fromEU, durationMin=60, n=3 }){
  const out=[]
  let t=ceilToSlotEU(fromEU.clone())
  t=nextOpeningFrom(t)
  while (out.length<n){
    if (insideBusinessHours(t,durationMin)){
      out.push(t.clone())
      t=t.add(SLOT_MIN,"minute")
    } else {
      if (t.hour()>=AFTERNOON.end) t=t.add(1,"day").hour(MORNING.start).minute(0)
      else if (t.hour()>=MORNING.end && t.hour()<AFTERNOON.start) t=t.hour(AFTERNOON.start).minute(0)
      else t=t.add(SLOT_MIN,"minute")
      while (!WORK_DAYS.includes(t.day()) || isHolidayEU(t)) t=t.add(1,"day").hour(MORNING.start).minute(0)
    }
  }
  return out
}

// ====== Emojis sutiles
function addEmoji(text, emoji="💅"){ return /\p{Emoji}/u.test(text) ? text : `${text} ${emoji}` }
function ensurePunct(text){
  const s=String(text||"").trim()
  if (!s) return s
  const withPunct = /[.!?…]$/.test(s) ? s : s+"."
  return withPunct
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
`)
const insertAppt = db.prepare(`INSERT INTO appointments
(id,customer_name,customer_phone,customer_square_id,location_key,service_env_key,service_label,duration_min,start_iso,end_iso,staff_id,status,created_at,square_booking_id)
VALUES (@id,@customer_name,@customer_phone,@customer_square_id,@location_key,@service_env_key,@service_label,@duration_min,@start_iso,@end_iso,@staff_id,@status,@created_at,@square_booking_id)`)

// ====== Sesiones
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

const DESI_MATCHERS = [/(\b|_)desi(\b|_)/i, /\bdesy\b/i, /\bdesir[eé]e?\b/i]
function getStaffByAliasLoose(aliasOrText){
  const t = norm(aliasOrText)
  // intenta por alias conocidos primero
  if (/desi|desy|desiree|desir[eé]e?/.test(t)){
    const cand = EMPLOYEES.find(e => e.labels.some(l => /desi|desy|desiree|desir[eé]e?/i.test(l)) || /desi/i.test(e.envKey))
    if (cand) return cand
  }
  // genérico: busca por labels dentro del texto
  let cand2 = null
  for (const e of EMPLOYEES){
    if (e.labels.some(lbl => t.includes(norm(lbl)))) { cand2 = e; break }
  }
  return cand2
}

function detectPreferredStaff(text, locKey){
  const cand = getStaffByAliasLoose(text)
  if (!cand) return { id:null, preferId:null, preferLabel:null, locked:false }
  const locId = locationToId(locKey||"torremolinos")
  const isAllowed = e => e.bookable && (e.allow.includes("ALL") || e.allow.includes(locId))
  const locked = DESI_MATCHERS.some(rx=>rx.test(text)) // bloqueo duro si pide Desi
  if (isAllowed(cand)) return { id:cand.id, preferId:cand.id, preferLabel:(cand.labels[0]||"Desi"), locked }
  return { id:null, preferId:cand.id, preferLabel:(cand.labels[0]||"Desi"), locked }
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

// ====== Servicios (.env) + CATEGORÍAS
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
function categorizeServices(sedeKey){
  const items = servicesForSedeKeyRaw(sedeKey)
  const cat = { lash:[], nails:[], brows:[], wax:[], other:[] }
  for (const it of items){
    const nk = norm(it.rawKey)
    if (/(pestan|pestañ|lifting.*pestan|pestañas|lash)/.test(nk)) { cat.lash.push(it); continue }
    if (/(manic|pedic|uñas|unas|gel|acrilic|esculp)/.test(nk)) { cat.nails.push(it); continue }
    if (/(ceja|brow|laminado_cejas|henna_cejas)/.test(nk)) { cat.brows.push(it); continue }
    if (/(depil|cera|wax)/.test(nk)) { cat.wax.push(it); continue }
    cat.other.push(it)
  }
  const toMenu = arr => arr.map((x,i)=>({ index:i+1, label:x.label, key:x.key }))
  return {
    lash: toMenu(cat.lash),
    nails: toMenu(cat.nails),
    brows: toMenu(cat.brows),
    wax: toMenu(cat.wax),
    other: toMenu(cat.other),
    countAll: items.length
  }
}
function buildLashMenu(sedeKey){
  // Curado para pestañas
  const p=(sedeKey==="la_luz")?"SQ_SVC_luz_":"SQ_SVC_"
  const want = [
    [p+"EXTENSIONES_DE_PESTANAS_NUEVAS_PELO_A_PELO","Extensiones de pestañas nuevas pelo a pelo"],
    [p+"EXTENSIONES_PESTANAS_NUEVAS_2D","Extensiones pestañas nuevas 2D"],
    [p+"EXTENSIONES_PESTANAS_NUEVAS_3D","Extensiones pestañas nuevas 3D"],
    [p+"LIFTING_DE_PESTANAS_Y_TINTE","Lifting de pestañas y tinte"]
  ]
  const out=[]
  for (const [key,label] of want){
    const [id] = String(process.env[key]||"").split("|")
    if (id) out.push({ index: out.length+1, label, key })
  }
  return out
}

// ====== Detectar tipo de pestañas desde texto libre
function detectLashTypeFromText(t){
  const x=norm(t)
  if (/\b(2d|volumen 2d|doble|dos d)\b/.test(x)) return "2D"
  if (/\b(3d|volumen 3d|triple|tres d|rusa)\b/.test(x)) return "3D"
  if (/\b(pelo a pelo|clasicas|clásicas|classicas|classics)\b/.test(x)) return "pelo a pelo"
  if (/\b(lifting|laminado).*tinte|\blifting\b/.test(x)) return "lifting"
  return null
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
  if (DRY_RUN) return { id:`TEST_${Date.now()}` }
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
    return resp?.result?.booking || null
  }catch(e){
    console.error("createBooking:", e?.message||e)
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
function staffLabelFromId(id){
  const e = EMPLOYEES.find(x=>x.id===id)
  return e?.labels?.[0] || (id ? `Prof. ${String(id).slice(-4)}` : null)
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
          servicio: "Servicio" // opcional: podríamos mapear desde catalog si hiciera falta
        })
      }
      // ordena por fecha asc
      items.sort((a,b)=>a.fecha_iso.localeCompare(b.fecha_iso) || a.pretty.localeCompare(b.pretty))
    }catch(e){ console.error("listBookings:", e?.message||e) }
  }
  return items
}

// === Disponibilidad real por staff + servicio vía searchAvailability
async function searchAvailabilityForStaff({ locationKey, envServiceKey, staffId, fromEU, days=14, n=3, distinctDays=false }){
  try{
    const sv = await getServiceIdAndVersion(envServiceKey)
    if (!sv?.id || !staffId) return []
    const startAt = fromEU.tz("UTC").toISOString()
    const endAt = fromEU.clone().add(days,"day").tz("UTC").toISOString()
    const body = {
      query:{
        filter:{
          startAtRange:{ startAt, endAt },
          locationId: locationToId(locationKey),
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
      slots.push(d)
      if (slots.length>=n) break
    }
    return slots
  }catch(e){
    console.error("searchAvailabilityForStaff:", e?.message||e)
    return []
  }
}

// ====== Menús pegajosos (20 min) + parsing
const MENU_TTL_MS = 20*60*1000
function parseIndexFromText(t){
  const x=norm(t)
  if (/\b(primero|primera|1ro|1ª|1a|1º)\b/.test(x)) return 1
  if (/\b(segundo|segunda|2do|2ª|2a|2º)\b/.test(x)) return 2
  if (/\b(tercero|tercera|3ro|3ª|3a|3º)\b/.test(x)) return 3
  const m=x.match(/\b([1-9])\b/); if (m) return Number(m[1])
  return null
}
function parseConfirmFromText(t){
  const x=norm(t)
  if (/\b(s[ií]|ok|vale|confirmo|confirmar|hecho|perfecto|adelante|sí)\b/.test(x)) return 1
  if (/\b(no|otra|otro|cambia|cambiar|distinto|diferente|mas tarde|más tarde|no me viene bien)\b/.test(x)) return 2
  if (/otro momento|otra hora|otro dia|otro día/.test(x)) return 2
  return null
}
function wantsAltTime(t){ const x=norm(t); return /(otra hora|otro momento|mas tarde|más tarde|antes|después|temprano)/.test(x) }
function wantsOtherDay(t){ const x=norm(t); return /(otro dia|otro día)/.test(x) }
function looksLikeCancel(t){ const x=norm(t); return /\b(cancelar|anular|borrar)\b.*\bcita\b/.test(x) }
function wantsNextOnly(t){ const x=norm(t); return /\b(proxima|pr[oó]xima|siguiente|la de mañana|la de hoy)\b/.test(x) }
function setPendingMenu(s, type, items){ s.pendingMenu = { type, items, createdAt: Date.now() } }
function getPendingMenu(s){
  if (!s?.pendingMenu) return null
  if (Date.now() - (s.pendingMenu.createdAt||0) > MENU_TTL_MS){ s.pendingMenu=null; return null }
  return s.pendingMenu
}

// ====== Mini-web + QR
const app=express()
const PORT=process.env.PORT||8080
let lastQR=null, conectado=false
app.get("/", (_req,res)=>{
  res.send(`<!doctype html><meta charset="utf-8"><style>
  body{font-family:system-ui;display:grid;place-items:center;min-height:100vh}
  .card{max-width:560px;padding:24px;border-radius:16px;box-shadow:0 6px 24px rgba(0,0,0,.08)}
  </style><div class="card"><h1>Gapink Nails</h1>
  <p>Estado: ${conectado?"✅ Conectado":"❌ Desconectado"}</p>
  ${!conectado&&lastQR?`<img src="/qr.png" width="300">`:""}
  <p style="opacity:.7">Modo: ${DRY_RUN?"Simulación (no toca Square)":"Producción"}</p>
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
let RECONNECT_SCHEDULED = false
let RECONNECT_ATTEMPTS = 0

// ====== Cola por usuario
const QUEUE=new Map()
function enqueue(key,job){
  const prev=QUEUE.get(key)||Promise.resolve()
  const next=prev.then(job,job).finally(()=>{ if (QUEUE.get(key)===next) QUEUE.delete(key) })
  QUEUE.set(key,next); return next
}

// ====== Presence typing
async function sendWithPresence(sock, jid, text){
  try{ await sock.sendPresenceUpdate("composing", jid) }catch{}
  await new Promise(r=>setTimeout(r, 600+Math.random()*1000))
  return sock.sendMessage(jid, { text })
}

// ====== Helpers orquestador
function safeParseJSON(txt){ 
  try{ 
    const a=txt.indexOf("{"), b=txt.lastIndexOf("}"); 
    if (a>=0&&b>a) txt=txt.slice(a,b+1); 
    return JSON.parse(txt) 
  }catch{return null} 
}
function sanitizeAIDecision(dec, serviciosForAI, hoursList, citas, sede){
  const base = { 
    intent:5, 
    needs_clarification:true, 
    requires_confirmation:false,
    slots:{ 
      sede: sede||null, 
      service_index:null, 
      appointment_index:null, 
      date_iso:null,
      time_iso:null,
      datetime_iso:null,
      profesional:null,
      notes:null 
    },
    selection:{ 
      time_index:null, 
      date_index:null, 
      confirm_index:null 
    }, 
    client_message:"" 
  }
  if (!dec||typeof dec!=="object") return base
  const out = structuredClone(base)
  const clamp=(n,max)=>Number.isInteger(n)&&n>=1&&(max? n<=max:true)?n:null
  out.intent = [1,2,3,4,5].includes(Number(dec.intent)) ? Number(dec.intent) : base.intent
  out.needs_clarification=!!dec.needs_clarification
  out.requires_confirmation=!!dec.requires_confirmation
  out.client_message = String(dec.client_message||"")
  const sev=dec.slots||{}
  out.slots.sede = (sev.sede==="torremolinos"||sev.sede==="la_luz")?sev.sede:base.slots.sede
  out.slots.service_index = clamp(sev.service_index,(serviciosForAI||[]).length)
  out.slots.appointment_index = clamp(sev.appointment_index,citas.length)
  out.slots.datetime_iso = sev.datetime_iso||null
  out.slots.profesional = sev.profesional||null
  out.slots.notes = sev.notes||null
  out.slots.date_iso = sev.date_iso||null
  out.slots.time_iso = sev.time_iso||null
  const sel=dec.selection||{}
  out.selection.time_index = clamp(sel.time_index,hoursList.length)
  out.selection.confirm_index = [1,2].includes(Number(sel.confirm_index))?Number(sel.confirm_index):null
  out.selection.date_index = clamp(sel.date_index,0)
  return out
}

// ====== Bot
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
        let s = loadSession(phone) || {
          greeted:false, lastWelcomeAt:null, lastOOHAt:null,
          sede:null, serviceCategory:null,
          name:null, email:null,
          preferredStaffId:null, preferredStaffLabel:null,
          lockedStaffId:null, lockedStaffName:null,
          selectedServiceEnvKey:null, selectedServiceLabel:null,
          pendingDateTime:null, lastHours:[], last_msg_id:null, pendingMenu:null,
          lastServiceMenu:null, lastServiceMenuAt:null,
          appointmentIndexLocal:null,
          lockedFlow:null // "book"|"edit"
        }
        if (s.last_msg_id===m.key.id) return
        s.last_msg_id=m.key.id

        const nowEU=dayjs().tz(EURO_TZ)
        if (!s.greeted || Date.now()- (s.lastWelcomeAt||0) > 6*60*60*1000){ s.greeted=true; s.lastWelcomeAt=Date.now(); saveSession(phone,s) }
        const inHours=insideBusinessHours(nowEU.clone(),15)
        if (!inHours && Date.now()- (s.lastOOHAt||0) > 4*60*60*1000){ s.lastOOHAt=Date.now(); saveSession(phone,s) }

        // Detección contexto acumulado
        const maybeSede=detectSedeFromText(textRaw)
        if (!s.sede && maybeSede) s.sede=maybeSede
        else if (maybeSede && wantsChangeSede(textRaw)) s.sede=maybeSede

        const pref = detectPreferredStaff(textRaw, s.sede || "torremolinos")
        if (pref.preferLabel) s.preferredStaffLabel = pref.preferLabel
        if (pref.id) s.preferredStaffId = pref.id
        if (pref.locked && pref.id){ s.lockedStaffId = pref.id; s.lockedStaffName = s.preferredStaffLabel || "Desi"; s.lockedFlow="book" }

        // ====== Flujo CANCELAR sin IA (real, por usuario)
        if (looksLikeCancel(textRaw)){
          const citas = await enumerateCitasByPhone(phone)
          if (!citas.length){
            await sendWithPresence(sock,jid, ensurePunct("No veo citas futuras asociadas a tu número. Si crees que falta algo, revisa el enlace del SMS de tu cita o dime la fecha/hora para buscarla."))
            return
          }
          if (wantsNextOnly(textRaw)){
            const first = citas[0]
            setPendingMenu(s,"confirm", [{index:1,label:"sí"},{index:2,label:"no"}]); s.appointmentIndexLocal = first.index; saveSession(phone,s)
            await sendWithPresence(sock,jid, ensurePunct(`¿Confirmo la cancelación de tu próxima cita (${first.pretty} — ${first.sede}, ${first.profesional})? (sí/no)`))
            return
          }
          setPendingMenu(s,"appointments",citas); saveSession(phone,s)
          await sendWithPresence(sock,jid, ensurePunct(`Elige la cita a cancelar: ${citas.map(c=>`${c.index}) ${c.pretty} — ${c.sede} · ${c.profesional}`).join(" · ")}`))
          return
        }

        // Categoría pestañas + detectar tipo
        const saidLashes = /\bpesta(?:n|ñ)as\b/i.test(norm(textRaw)) || /lifting/.test(norm(textRaw))
        if (saidLashes){
          s.serviceCategory="lash"
          if (!s.lastServiceMenu || Date.now()-(s.lastServiceMenuAt||0)>MENU_TTL_MS){
            s.lastServiceMenu = s.sede ? buildLashMenu(s.sede) : null
            s.lastServiceMenuAt = Date.now()
          }
          const detected = detectLashTypeFromText(textRaw)
          if (detected && s.sede && s.lastServiceMenu?.length){
            const match = s.lastServiceMenu.find(x=>{
              const l = x.label.toLowerCase()
              if (detected==="2D") return /2d/.test(l)
              if (detected==="3D") return /3d/.test(l)
              if (detected==="pelo a pelo") return /pelo a pelo|cl[aá]sicas/.test(l)
              if (detected==="lifting") return /lifting/.test(l)
              return false
            })
            if (match){
              s.selectedServiceEnvKey = match.key
              s.selectedServiceLabel = match.label
            }
          }
        }

        // Menús pendientes
        const pending = getPendingMenu(s)
        const idxPick = parseIndexFromText(textRaw)
        let localConfirmIdx = null
        let justPickedHour = false

        if (pending && pending.type==="confirm"){
          const c = parseConfirmFromText(textRaw)
          if (c){ localConfirmIdx = c; s.pendingMenu=null; saveSession(phone,s) }
        }
        if (pending && idxPick){
          const item = pending.items.find(x=>x.index===idxPick)
          if (item){
            if (pending.type==="services"){
              s.selectedServiceEnvKey=item.key
              s.selectedServiceLabel=item.label
              s.pendingMenu=null
              saveSession(phone,s)
              s.lockedFlow = "book"
            }
            if (pending.type==="hours"){
              s.pendingDateTime = dayjs.tz(item.iso, EURO_TZ)
              s.pendingMenu=null
              saveSession(phone,s)
              s.lockedFlow = "book"
              justPickedHour = true
            }
            if (pending.type==="appointments"){ 
              s.appointmentIndexLocal=idxPick
              s.pendingMenu=null
              saveSession(phone,s) 
              // al elegir cita a cancelar pedimos confirmación:
              setPendingMenu(s,"confirm", [{index:1,label:"sí"},{index:2,label:"no"}]); saveSession(phone,s)
              await sendWithPresence(sock,jid, ensurePunct("¿Confirmo la cancelación? (sí/no)"))
              return
            }
          }
        }

        // === FAST-CONFIRM: respuesta sí/no tras proponer hora
        if (localConfirmIdx!=null && (s.lockedFlow==="book" || s.selectedServiceEnvKey) ){
          if (localConfirmIdx===1){
            await executeCreateBookingInline()
          } else {
            const nextBase = nextOpeningFrom(nowEU.add(NOW_MIN_OFFSET_MIN+30,"minute"))
            const more = await proposeHoursForContext({ baseFromEU: nextBase, n:3, wantDistinctDays:wantsOtherDay(textRaw) })
            if (!more.length) return
            setPendingMenu(s,"hours", more); saveSession(phone,s)
            const staffText = s.lockedStaffName || s.preferredStaffLabel || "nuestro equipo"
            await sendWithPresence(sock,jid, ensurePunct(`Ok, te paso otras opciones con ${staffText}: ${more.map(h=>`${h.index}) ${h.pretty}`).join(" · ")}. Responde con 1/2/3`))
          }
          return
        }

        // Si justo eligió una hora, confirmación directa
        if (justPickedHour && s.selectedServiceEnvKey && s.pendingDateTime){
          const staffText = s.lockedStaffName || s.preferredStaffLabel || "nuestro equipo"
          const msg =
            `Perfecto. ¿Confirmo la cita con ${staffText} para ${s.selectedServiceLabel||"pestañas"} el ${fmtES(s.pendingDateTime)} en ${locationNice(s.sede||"torremolinos")}? Responde con 'sí' o 'no'.`
          setPendingMenu(s,"confirm", [{index:1,label:"sí"},{index:2,label:"no"}])
          saveSession(phone,s)
          await sendWithPresence(sock, jid, ensurePunct(msg))
          return
        }

        // ====== Categorías primero si hay demasiados servicios
        const MAX_DIRECT_SERVICES = 15
        let categoryMenus = null
        if (s.sede && !s.selectedServiceEnvKey){
          const cat = categorizeServices(s.sede)
          categoryMenus = cat
          if (!s.serviceCategory){
            if (cat.countAll > MAX_DIRECT_SERVICES && !saidLashes){
              const cats = [
                cat.lash.length ? "1) Pestañas" : null,
                cat.nails.length ? "2) Uñas" : null,
                cat.brows.length ? "3) Cejas" : null,
                cat.wax.length ? "4) Depilación" : null,
                cat.other.length ? "5) Otros" : null
              ].filter(Boolean).join(" · ")
              setPendingMenu(s,"category",[
                {index:1, key:"lash", label:"Pestañas"},
                {index:2, key:"nails", label:"Uñas"},
                {index:3, key:"brows", label:"Cejas"},
                {index:4, key:"wax", label:"Depilación"},
                {index:5, key:"other", label:"Otros"},
              ])
              await sendWithPresence(sock,jid, ensurePunct(`¿Qué categoría quieres reservar? ${cats}`))
              return
            }
          }
        }
        // Selección de categoría por texto o índice
        if (getPendingMenu(s)?.type==="category"){
          const x = norm(textRaw)
          const mapTxt = { pesta:"lash", uña:"nails", ceja:"brows", depil:"wax", otro:"other" }
          let pickedKey = null
          for (const [k,v] of Object.entries(mapTxt)) if (x.includes(k)) pickedKey=v
          if (!pickedKey && idxPick){
            const item = getPendingMenu(s).items.find(i=>i.index===idxPick)
            pickedKey = item?.key||null
          }
          if (pickedKey){
            s.serviceCategory = pickedKey==="lash" ? "lash" : s.serviceCategory
            const menu = (pickedKey==="lash") ? (buildLashMenu(s.sede)||[]) : (categoryMenus?.[pickedKey]||[]).map((x,i)=>({index:i+1, label:x.label, key:x.key}))
            if (menu.length){
              setPendingMenu(s,"services", menu); saveSession(phone,s)
              await sendWithPresence(sock,jid, ensurePunct(`Elige el servicio: ${menu.map(x=>`${x.index}) ${x.label}`).join(" · ")}`))
              return
            }
          }
        }

        // ====== Proponer horas (si tenemos sede + servicio, con Desi si bloqueada)
        const baseFrom = nextOpeningFrom(nowEU.add(NOW_MIN_OFFSET_MIN,"minute"))
        async function proposeHoursForContext({ baseFromEU, n=3, wantDistinctDays=false }){
          const envKey = s.selectedServiceEnvKey || (s.serviceCategory==="lash" && buildLashMenu(s.sede||"torremolinos")[0]?.key) || null

          // Bloqueo Desi: no caemos a otra profesional sin permiso
          if (s.lockedStaffId && envKey){
            const arr = await searchAvailabilityForStaff({
              locationKey:s.sede,
              envServiceKey:envKey,
              staffId:s.lockedStaffId,
              fromEU: baseFromEU,
              n,
              distinctDays: wantDistinctDays
            })
            if (arr.length) return enumerateHours(arr)
            setPendingMenu(s,"confirm", [{index:1,label:"sí"},{index:2,label:"no"}]); saveSession(phone,s)
            await sendWithPresence(sock,jid, ensurePunct(`No veo huecos con ${s.lockedStaffName} en los próximos días. ¿Te vale con otra profesional? (sí/no)`))
            return []
          }

          const staffId = s.preferredStaffId || null
          if (s.sede && envKey && staffId){
            const arr = await searchAvailabilityForStaff({
              locationKey:s.sede, envServiceKey:envKey, staffId,
              fromEU: baseFromEU, n, distinctDays: wantDistinctDays
            })
            if (arr.length) return enumerateHours(arr)
          }

          // Fallback sin staff
          const arr2 = proposeSlots({ fromEU: baseFromEU, durationMin:60, n })
          return enumerateHours(arr2)
        }

        // Si el usuario pide otro día/otra hora, proponemos ya:
        const userWantsAlt = wantsAltTime(textRaw) || wantsOtherDay(textRaw)
        if (userWantsAlt && s.sede){
          const more = await proposeHoursForContext({ baseFromEU: baseFrom.add(wantsOtherDay(textRaw)?1:0,"day"), n:3, wantDistinctDays:wantsOtherDay(textRaw) })
          if (!more.length) return
          setPendingMenu(s,"hours", more); saveSession(phone,s)
          const staffText = s.lockedStaffName || s.preferredStaffLabel || "nuestro equipo"
          const word = wantsOtherDay(textRaw) ? "fechas" : "horas"
          await sendWithPresence(sock, jid, ensurePunct(`Sin problema. Te propongo estas ${word} con ${staffText}: ${more.map(h=>`${h.index}) ${h.pretty}`).join(" · ")}. Responde con 1/2/3`))
          return
        }

        // ====== Si no hay servicio elegido todavía, guiamos con menú corto
        if (!s.selectedServiceEnvKey){
          if (s.serviceCategory==="lash"){
            const lashMenu = s.lastServiceMenu || (s.sede ? buildLashMenu(s.sede) : [])
            if (lashMenu?.length){
              setPendingMenu(s,"services", lashMenu); saveSession(phone,s)
              await sendWithPresence(sock,jid, ensurePunct(`Para pestañas en ${locationNice(s.sede||"torremolinos")}, elige: ${lashMenu.map(x=>`${x.index}) ${x.label}`).join(" · ")}`))
              return
            }
          }
          // fallback genérico con categorías ya manejadas arriba
          await sendWithPresence(sock,jid, ensurePunct("¿Qué servicio necesitas y en qué sede (Torremolinos o Málaga – La Luz)?"))
          return
        }

        // Ya hay servicio -> proponemos horas iniciales con Desi si procede
        const initial = await proposeHoursForContext({ baseFromEU: baseFrom, n:3 })
        if (!initial.length) return
        setPendingMenu(s,"hours", initial); saveSession(phone,s)
        const staffText = s.lockedStaffName || s.preferredStaffLabel || "nuestro equipo"
        await sendWithPresence(sock,jid, ensurePunct(`Genial. Te dejo 3 opciones con ${staffText}: ${initial.map(h=>`${h.index}) ${h.pretty}`).join(" · ")}. Responde con 1/2/3`))

        // ====== Acciones finales y helpers dentro del scope
        async function executeCreateBookingInline(){
          if (!s.sede){ await sendWithPresence(sock,jid, ensurePunct("¿Te viene mejor Torremolinos o Málaga – La Luz?")); return }
          if (!s.selectedServiceEnvKey){ await sendWithPresence(sock,jid, ensurePunct("Elige el tipo de pestañas (responde con el número).")); return }
          if (!s.pendingDateTime){ await sendWithPresence(sock,jid, ensurePunct("Elige una hora (1/2/3) o dime otra.")); return }

          const startEU = ceilToSlotEU(s.pendingDateTime.clone())
          if (!insideBusinessHours(startEU,60)){ 
            s.pendingDateTime=null; saveSession(phone,s)
            await sendWithPresence(sock,jid, ensurePunct("Esa hora cae fuera de L–V 10–14 / 16–20. Dime otra."))
            return 
          }
          const staffId = s.lockedStaffId || s.preferredStaffId || pickStaffForLocation(s.sede, null)
          const staffName = s.lockedStaffName || s.preferredStaffLabel || "nuestro equipo"
          if (!staffId){ await sendWithPresence(sock,jid, ensurePunct("Ahora mismo no puedo asignar profesional en ese salón. ¿Te da igual con quién?")); return }
          const customer = await findOrCreateCustomer({ name:s.name, email:s.email, phone })
          if (!customer){ await sendWithPresence(sock,jid, ensurePunct("Para cerrar, pásame nombre o email.")); return }

          const booking = await createBooking({
            startEU, locationKey:s.sede, envServiceKey:s.selectedServiceEnvKey,
            durationMin:60, customerId:customer.id, teamMemberId:staffId
          })
          if (!booking){
            await sendWithPresence(sock,jid, ensurePunct("No pude reservar ese hueco. ¿Te paso otras horas o prefieres el link? https://gapinknails.square.site/"))
            return
          }

          const aptId = `apt_${Math.random().toString(36).slice(2,8)}${Date.now().toString(36).slice(-4)}`
          insertAppt.run({
            id:aptId, 
            customer_name:customer?.givenName||null, 
            customer_phone:phone, 
            customer_square_id:customer.id,
            location_key:s.sede, 
            service_env_key:s.selectedServiceEnvKey, 
            service_label:s.selectedServiceLabel||"Servicio",
            duration_min:60, 
            start_iso:startEU.tz("UTC").toISOString(), 
            end_iso:startEU.clone().add(60,"minute").tz("UTC").toISOString(),
            staff_id:staffId, 
            status:"confirmed", 
            created_at:new Date().toISOString(), 
            square_booking_id:booking.id
          })

          await sendWithPresence(sock, jid, ensurePunct(
`Reserva confirmada 🎉
${locationNice(s.sede)} — ${s.sede==="la_luz"?ADDRESS_LUZ:ADDRESS_TORRE}
Servicio: ${s.selectedServiceLabel||"—"}
Profesional: ${staffName}
Fecha: ${fmtES(startEU)}
Duración: 60 min

¡Te esperamos!`
          ))
          clearSession(phone)
        }

        // Hook de confirm tras propuesta inicial si el usuario responde sí/no sin menú
        const adhocConfirm = parseConfirmFromText(textRaw)
        if (adhocConfirm===1 && s.selectedServiceEnvKey && s.pendingDateTime){
          await executeCreateBookingInline()
          return
        } else if (adhocConfirm===2 && s.selectedServiceEnvKey){
          const more = await proposeHoursForContext({ baseFromEU: baseFrom.add(1,"hour"), n:3 })
          if (!more.length) return
          setPendingMenu(s,"hours", more); saveSession(phone,s)
          await sendWithPresence(sock,jid, ensurePunct(`Ok, te paso otras horas: ${more.map(h=>`${h.index}) ${h.pretty}`).join(" · ")}. Responde con 1/2/3`))
          return
        }

      })
    })
  }catch(e){ 
    console.error("startBot:", e?.message||e) 
  }
}

// Señales
process.on("uncaughtException", (e)=>console.error("uncaughtException:", e?.stack||e?.message||e))
process.on("unhandledRejection", (e)=>console.error("unhandledRejection:", e))
process.on("SIGTERM", ()=>{ console.log("🛑 SIGTERM recibido"); process.exit(0) })
process.on("SIGINT", ()=>{ console.log("🛑 SIGINT recibido"); process.exit(0) })
