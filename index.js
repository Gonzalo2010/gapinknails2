// index.js — Gapink Nails · v38.0.0 (DeepSeek-only, holds 6h, IA sin números)
// Autor: Gonzalo-friendly edition 🫶
// Características clave:
// - IA DeepSeek en TODO (interpretación natural y elección de hora/servicio sin números).
// - Mini web con QR (http://localhost:8080) y estado.
// - Bloqueo de huecos en SQLite 6h (no ofrece a otra persona por WhatsApp). NO toca Square para reservar.
// - Lista completa de servicios de UÑAS (manos) con duración.
// - Si el cliente pide “con <nombre>”, se filtra SOLO esa profesional. Si no hay huecos, no cae a “equipo”.
// - Oculta nombres de profesionales salvo que el cliente lo pida explícitamente.
// - Evita el “spam”: hace resumen solo cuando ya tiene todo, y usa prompts de IA cortitos (ahorro tokens).

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
import { createHash, webcrypto } from "crypto"
import { Client, Environment } from "square"

if (!globalThis.crypto) globalThis.crypto = webcrypto

dayjs.extend(utc); dayjs.extend(tz); dayjs.extend(isoWeek); dayjs.locale("es")
const EURO_TZ = "Europe/Madrid"

// ====== Config horario
const WORK_DAYS = [1,2,3,4,5]        // L–V
const SLOT_MIN = 30
const OPEN = { start: 9, end: 20 }
const NOW_MIN_OFFSET_MIN = Number(process.env.BOT_NOW_OFFSET_MIN || 30)
const SEARCH_WINDOW_DAYS = Number(process.env.BOT_SEARCH_WINDOW_DAYS || 14)
const EXTENDED_WINDOW_DAYS = Number(process.env.BOT_STEER_WINDOW_DAYS || 7)
const HOLIDAYS_EXTRA = (process.env.HOLIDAYS_EXTRA || "06/01,28/02,15/08,12/10,01/11,06/12,08/12,25/12")
  .split(",").map(s=>s.trim()).filter(Boolean)
const SHOW_TOP_N = Number(process.env.SHOW_TOP_N || 5)

// ====== Flags
const BOT_DEBUG = /^true$/i.test(process.env.BOT_DEBUG || "")
const DRY_RUN = /^true$/i.test(process.env.DRY_RUN || "")
const SQUARE_MAX_RETRIES = Number(process.env.SQUARE_MAX_RETRIES || 3)

// ====== Square (solo CONSULTA de disponibilidad)
const square = new Client({
  accessToken: process.env.SQUARE_ACCESS_TOKEN,
  environment: (process.env.SQUARE_ENV==="production") ? Environment.Production : Environment.Sandbox
})
const LOC_TORRE = (process.env.SQUARE_LOCATION_ID_TORREMOLINOS || "").trim()
const LOC_LUZ   = (process.env.SQUARE_LOCATION_ID_LA_LUZ || "").trim()
const ADDRESS_TORRE = process.env.ADDRESS_TORREMOLINOS || "Av. de Benyamina 18, Torremolinos"
const ADDRESS_LUZ   = process.env.ADDRESS_LA_LUZ || "Málaga – Barrio de La Luz"

// ====== IA (DeepSeek only)
const DEEPSEEK_API_KEY = process.env.DEEPSEEK_API_KEY || ""
const DEEPSEEK_MODEL = process.env.DEEPSEEK_MODEL || process.env.AI_MODEL || "deepseek-chat"
const DEEPSEEK_URL = process.env.DEEPSEEK_API_URL || "https://api.deepseek.com/v1/chat/completions"
const AI_TIMEOUT_MS = Number(process.env.AI_TIMEOUT_MS || 12000)
const sleep = ms => new Promise(r=>setTimeout(r, ms))

async function aiChat(system, user, extraMsgs=[]){
  if (!DEEPSEEK_API_KEY) return null
  const controller = new AbortController()
  const timeout = setTimeout(()=>controller.abort(), AI_TIMEOUT_MS)
  try{
    const messages = [
      system ? { role:"system", content: system } : null,
      ...extraMsgs,
      { role:"user", content: user }
    ].filter(Boolean)
    const resp = await fetch(DEEPSEEK_URL,{
      method:"POST",
      headers:{ "Content-Type":"application/json", "Authorization":`Bearer ${DEEPSEEK_API_KEY}` },
      body: JSON.stringify({ model: DEEPSEEK_MODEL, messages, temperature:0.2, max_tokens:500 }),
      signal: controller.signal
    })
    clearTimeout(timeout)
    if (!resp.ok) return null
    const data = await resp.json()
    return data?.choices?.[0]?.message?.content || null
  }catch{ clearTimeout(timeout); return null }
}
function stripToJSON(text){
  if (!text) return null
  let s = text.trim()
  s = s.replace(/```json/gi,"```")
  if (s.startsWith("```")) s = s.slice(3)
  if (s.endsWith("```")) s = s.slice(0,-3)
  s = s.trim()
  const i = s.indexOf("{"), j = s.lastIndexOf("}")
  if (i>=0 && j>i) s = s.slice(i, j+1)
  try{ return JSON.parse(s) }catch{ return null }
}

// ====== Utils
const nowEU = ()=>dayjs().tz(EURO_TZ)
const stableKey = parts => createHash("sha256").update(Object.values(parts).join("|")).digest("hex").slice(0,48)
const onlyDigits = s => String(s||"").replace(/\D+/g,"")
const rm = s => String(s||"").normalize("NFD").replace(/\p{Diacritic}/gu,"")
const norm = s => rm(s).toLowerCase().replace(/[+.,;:()/_-]/g," ").replace(/[^\p{Letter}\p{Number}\s]/gu," ").replace(/\s+/g," ").trim()
function titleCase(str){ return String(str||"").toLowerCase().replace(/\b([a-z])/g, (m)=>m.toUpperCase()) }
function locationNice(key){ return key==="la_luz" ? "Málaga – La Luz" : "Torremolinos" }
function locationToId(key){ return key==="la_luz" ? LOC_LUZ : LOC_TORRE }
function idToLocKey(id){ return id===LOC_LUZ ? "la_luz" : id===LOC_TORRE ? "torremolinos" : null }
function normalizePhoneES(raw){
  const d=onlyDigits(raw); if(!d) return null
  if (raw.startsWith("+") && d.length>=8 && d.length<=15) return `+${d}`
  if (d.startsWith("34") && d.length===11) return `+${d}`
  if (d.length===9) return `+34${d}`
  if (d.startsWith("00")) return `+${d.slice(2)}`
  return `+${d}`
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
CREATE TABLE IF NOT EXISTS sessions (
  phone TEXT PRIMARY KEY,
  data_json TEXT,
  updated_at TEXT
);
CREATE TABLE IF NOT EXISTS holds (
  hold_id TEXT PRIMARY KEY,
  phone TEXT,
  sede TEXT,
  service_env_key TEXT,
  start_utc TEXT,
  end_utc TEXT,
  staff_id TEXT,
  created_at TEXT,
  expire_at TEXT,
  active INTEGER
);
`)

function loadSession(phone){
  const row = db.prepare(`SELECT data_json FROM sessions WHERE phone=@phone`).get({phone})
  if (!row?.data_json) return null
  const s = JSON.parse(row.data_json)
  if (Array.isArray(s.lastSlots)) s.lastSlots = s.lastSlots.map(o=>({ ...o, date: dayjs(o.date).tz(EURO_TZ) }))
  return s
}
function saveSession(phone,s){
  const c={...s}
  if (Array.isArray(c.lastSlots)) c.lastSlots = c.lastSlots.map(o=>({ ...o, date: dayjs(o.date).toISOString() }))
  const j=JSON.stringify(c)
  const up=db.prepare(`UPDATE sessions SET data_json=@j, updated_at=@u WHERE phone=@p`).run({j,u:new Date().toISOString(),p:phone})
  if (up.changes===0) db.prepare(`INSERT INTO sessions (phone,data_json,updated_at) VALUES (@p,@j,@u)`).run({p:phone,j,u:new Date().toISOString()})
}
function clearSession(phone){ db.prepare(`DELETE FROM sessions WHERE phone=@phone`).run({phone}) }

// ====== Holds (bloqueo de huecos 6h)
function cleanupHolds(){
  const nowISO = new Date().toISOString()
  db.prepare(`UPDATE holds SET active=0 WHERE active=1 AND expire_at < @now`).run({now:nowISO})
}
function hasActiveOverlap({ sede, startISO, endISO }){
  cleanupHolds()
  const rows = db.prepare(`
    SELECT 1 FROM holds
    WHERE active=1 AND sede=@sede
      AND start_utc < @endISO AND end_utc > @startISO
    LIMIT 1
  `).all({ sede, startISO, endISO })
  return rows && rows.length>0
}
function placeHold({ phone, sede, envKey, startEU, durMin, staffId=null }){
  const startISO = startEU.tz("UTC").toISOString()
  const endISO = startEU.clone().add(durMin,"minute").tz("UTC").toISOString()
  const expireISO = dayjs().add(6,"hour").toISOString()
  const hold_id = `hold_${stableKey({phone,sede,envKey,startISO,endISO,staffId})}`
  db.prepare(`
    INSERT OR REPLACE INTO holds
      (hold_id, phone, sede, service_env_key, start_utc, end_utc, staff_id, created_at, expire_at, active)
    VALUES (@hold_id,@phone,@sede,@envKey,@startISO,@endISO,@staffId,@created,@expire,1)
  `).run({
    hold_id, phone, sede, envKey,
    startISO, endISO, staffId,
    created: new Date().toISOString(),
    expire: expireISO
  })
  return { hold_id, expireISO, startISO, endISO }
}
function releaseHoldsForPhone(phone){
  db.prepare(`UPDATE holds SET active=0 WHERE active=1 AND phone=@phone`).run({phone})
}

// ====== Staff & sedes por profesional
function parseEmployees(){
  // SQ_EMP_* envs — soporta alias en el nombre de la variable (p.ej. SQ_EMP_TANIA_TANIA)
  const out=[]
  for (const [k,v] of Object.entries(process.env)) {
    if (!k.startsWith("SQ_EMP_")) continue
    const parts = String(v||"").split("|").map(s=>s.trim())
    const id = parts[0]; if (!id) continue
    const bookTag = (parts[1]||"BOOKABLE").toUpperCase()
    const bookable = ["BOOKABLE","TRUE","YES","1"].includes(bookTag)
    const labels = k.replace(/^SQ_EMP_/,"").toLowerCase().split("_").filter(Boolean)
    out.push({ envKey:k, id, bookable, labels })
  }
  return out
}
let EMPLOYEES = parseEmployees()
function staffLabelFromId(id){
  const e = EMPLOYEES.find(x=>x.id===id)
  return e?.labels?.[0]?.toUpperCase() || (id ? `PROF.${String(id).slice(-4)}` : "Equipo")
}
// Map de centros por profesional (EMP_CENTER_*)
function parseStaffCenters(){
  const map = new Map()
  for(const [k,v] of Object.entries(process.env)){
    if(!k.startsWith("EMP_CENTER_")) continue
    const name = k.replace(/^EMP_CENTER_/,"").toLowerCase().replace(/_/g," ")
    const centers = String(v||"").split(",").map(x=>x.trim()).filter(Boolean).map(x=> x==="la_luz"?"la_luz":"torremolinos")
    map.set(name.split(" ")[0], new Set(centers)) // token corto: "tania", "ganna"…
  }
  return map
}
const STAFF_CENTERS = parseStaffCenters()
function staffAllowedInSede(shortName, sede){
  const set = STAFF_CENTERS.get((shortName||"").toLowerCase())
  if(!set) return true
  return set.has(sede)
}
function staffCentersHuman(shortName){
  const set = STAFF_CENTERS.get((shortName||"").toLowerCase())
  if(!set||!set.size) return null
  return [...set].map(locationNice).join(" / ")
}
function resolveStaffFromText(text){
  const t = norm(text||"")
  const m = t.match(/\bcon\s+([a-zñáéíóúüï ]{2,})/)
  const token = (m?m[1]:t).split(/\s+/).filter(Boolean)[0]
  if(!token) return null
  // match por palabra exacta en labels
  for(const e of EMPLOYEES){
    if (e.labels.some(lb=> norm(lb)===token )) return { id:e.id, label: e.labels[0].toUpperCase() }
  }
  // contiene token
  for(const e of EMPLOYEES){
    if (e.labels.some(lb=> norm(lb).includes(token) )) return { id:e.id, label: e.labels[0].toUpperCase() }
  }
  return null
}

// ====== Servicios y Duraciones
function cleanDisplayLabel(label){
  const s = String(label||"").replace(/^\s*(luz|la\s*luz)\s+/i,"").trim()
  return s
}
function servicesForSede(sedeKey){
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
function allServices(){ return [...servicesForSede("torremolinos"), ...servicesForSede("la_luz")] }

function buildDurationMap(){
  const map = new Map()
  for(const [k,v] of Object.entries(process.env)){
    if(!k.startsWith("SQ_DUR_")) continue
    let sede = "any"
    let raw = k.replace(/^SQ_DUR_/,"")
    if (raw.startsWith("luz_")) { sede="la_luz"; raw=raw.replace(/^luz_/,"") }
    const label = titleCase(raw.replaceAll("_"," "))
    const mins = Number(v||"0")||0
    map.set(`${sede}:${label.toLowerCase()}`, mins)
  }
  return map
}
const DUR_MAP = buildDurationMap()

function durationMinForLabel(label, sede){
  const k1 = `${sede||"any"}:${String(label||"").toLowerCase()}`
  const k2 = `any:${String(label||"").toLowerCase()}`
  return DUR_MAP.get(k1) || DUR_MAP.get(k2) || 60
}
function durationMinForEnvKey(envKey){
  const svc = allServices().find(s=>s.key===envKey)
  if(!svc) return 60
  return durationMinForLabel(svc.label, svc.sedeKey)
}

// Lista completa de UÑAS (manos)
function nailServicesForSede(sedeKey){
  const arr = servicesForSede(sedeKey)
  return arr
    .filter(s=>{
      const L = s.label.toUpperCase()
      const isNail = /(UÑAS|UNAS|MANICURA|RELLENO|QUITAR U|UÑA ROTA|UNA ROTA|ESCULPIDAS)/.test(L)
      const isFoot = /(PEDICURA|PIES)/.test(L)
      return isNail && !isFoot
    })
    .map(s=>({ label:s.label, key:s.key, mins: durationMinForLabel(s.label, sedeKey) }))
    .sort((a,b)=> a.label.localeCompare(b.label,'es',{sensitivity:'base'}))
}

// ====== IA Prompts (cortos para ahorrar tokens)
function systemNLP(now, sede, svc, staff, haveSlots){
  return `Eres un orquestador de WhatsApp para un salón de belleza. Devuelve SOLO JSON.
Fecha/hora EU: ${now.format("YYYY-MM-DD HH:mm")}
Sede actual: ${sede||"—"}
Servicio actual: ${svc||"—"}
Profesional actual: ${staff||"equipo"}
Tenemos huecos listados ahora: ${haveSlots?"sí":"no"}

Campos posibles:
{"intent":"greet|set_salon|set_service|set_staff|ask_slots|pick_slot|refine_time|list_nails|view|edit|cancel|none",
 "salon":"torremolinos|la_luz|null",
 "service_text":"string|null",
 "staff_name":"string|null",
 "time_text":"string|null"}

Reglas:
- Si dice “uñas” sin especificar: intent=list_nails.
- Si menciona “con <nombre>”: intent=set_staff (staff_name).
- Si pide “huecos”, “horario”, o dice una franja (“viernes tarde”, “me vale 13:00”): intent=ask_slots o refine_time (time_text).
- Si responde “la primera”, “la de las 13”, “martes 10:30”: intent=pick_slot (time_text).
- Nunca inventes número; usa lenguaje natural del usuario.
- Si dice ver/editar/cancelar cita: intent=view|edit|cancel.
- Si solo saluda: greet.`
}

async function aiUnderstand(userText, ctx){
  const sys = systemNLP(nowEU(), ctx.sede, ctx.svcLabel, ctx.prefStaffName, Array.isArray(ctx.lastSlots)&&ctx.lastSlots.length>0)
  const out = await aiChat(sys, `Cliente: "${userText}"\nResponde JSON.`)
  return stripToJSON(out) || { intent:"none" }
}

// Elegir servicio por IA (sin números). Se le pasa la lista de la sede.
async function aiChooseServiceLabel(userText, sedeKey){
  const list = servicesForSede(sedeKey)
  const pack = list.map(s=>s.label).slice(0,150) // límite para ahorrar tokens
  const sys = `Eres un clasificador. Devuelve SOLO JSON {"label":"<exacto o null>"}.
Elige el *mejor* servicio de esta lista EXACTA (sensible a tildes) según el texto del cliente. Si duda, escoge el más genérico/rápido.
Lista:\n- ${pack.join("\n- ")}`
  const out = await aiChat(sys, `Texto del cliente: "${userText}"`)
  const obj = stripToJSON(out)
  const label = obj?.label && list.find(s=> s.label.toLowerCase() === String(obj.label||"").toLowerCase())?.label
  return label || null
}

// Interpretar elección de hora (sin números): “la de las 13”, “martes”, “viernes tarde”…
async function aiPickHour(userText, candidateSlots){
  const options = candidateSlots.map(s=>`${s.date.format("YYYY-MM-DD HH:mm")}${s.staffId?` ${staffLabelFromId(s.staffId)}`:""}`)
  const sys = `Devuelve SOLO JSON {"iso":"YYYY-MM-DDTHH:mm","fallback":"none|ask_other"}.
Elige la mejor opción EXACTA de esta lista (prioriza coincidencia de hora/día, luego primera de la lista).
Lista:\n- ${options.join("\n- ")}`
  const out = await aiChat(sys, `Cliente: "${userText}"`)
  const obj = stripToJSON(out)
  const iso = obj?.iso
  if (!iso) return null
  // Validar que esté en candidates (por minuto)
  const match = candidateSlots.find(s=> s.date.format("YYYY-MM-DDTHH:mm")===iso.slice(0,16))
  return match || null
}

// ====== Disponibilidad (solo consulta a Square) + filtros + holds
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

async function searchAvailWindow({ locationKey, envServiceKey, startEU, endEU, limit=500, part=null, forceStaffId=null, durMin=60 }){
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
  }catch(e){ if(BOT_DEBUG) console.error("searchAvailability:", e?.message) }

  cleanupHolds()
  const out=[]
  for (const a of avail){
    if (!a?.startAt) continue
    const d = dayjs(a.startAt).tz(EURO_TZ)
    if (!insideBusinessHours(d,durMin)) continue
    let tm = null
    const segs = Array.isArray(a.appointmentSegments) ? a.appointmentSegments
                 : Array.isArray(a.segments) ? a.segments
                 : []
    if (segs[0]?.teamMemberId) tm = segs[0].teamMemberId
    if (forceStaffId && tm !== forceStaffId) continue
    if (part){
      // recorte franja (sencillo)
      const from = d.clone().hour(part==="mañana"?9:part==="tarde"?15:18).minute(0)
      const to   = d.clone().hour(part==="mañana"?13:part==="tarde"?20:20).minute(0)
      if (!(d.isAfter(from.subtract(1,"minute")) && d.isBefore(to.add(1,"minute")))) continue
    }
    const startISO = d.tz("UTC").toISOString()
    const endISO   = d.clone().add(durMin,"minute").tz("UTC").toISOString()
    if (hasActiveOverlap({ sede:locationKey, startISO, endISO })) continue
    out.push({ date:d, staffId: tm || null, durMin })
    if (out.length>=limit) break
  }
  out.sort((a,b)=>a.date.valueOf()-b.date.valueOf())
  return out
}

async function proposeHoursForService(session, jid, phone, sock, envKey, label, dateHint, { enforceStaff=false }={}){
  const base = nextOpeningFrom(nowEU().add(NOW_MIN_OFFSET_MIN,"minute"))
  let start = base.clone()
  let end   = base.clone().add(SEARCH_WINDOW_DAYS,"day")
  let part  = null
  if (dateHint && typeof dateHint==="string"){
    // delega en IA para “viernes tarde”, “martes”, “mañana”, “noche” (pero ahorrando tokens ya lo manejamos arriba si viene text free)
    if (/tarde/.test(norm(dateHint))) part="tarde"
    else if (/manana/.test(norm(dateHint))) part="mañana"
    else if (/noche/.test(norm(dateHint))) part="noche"
  }

  const durMin = durationMinForEnvKey(envKey)
  let slots = await searchAvailWindow({
    locationKey: session.sede, envServiceKey: envKey,
    startEU: start, endEU: end, limit: 500, part,
    forceStaffId: enforceStaff ? (session.prefStaffId||null) : null,
    durMin
  })
  if(!slots.length && part){
    slots = await searchAvailWindow({
      locationKey: session.sede, envServiceKey: envKey,
      startEU: start, endEU: end, limit: 500, part:null,
      forceStaffId: enforceStaff ? (session.prefStaffId||null) : null,
      durMin
    })
  }
  if(!slots.length){
    const start2 = base.clone()
    const end2   = base.clone().add(EXTENDED_WINDOW_DAYS,"day")
    slots = await searchAvailWindow({
      locationKey: session.sede, envServiceKey: envKey,
      startEU: start2, endEU: end2, limit: 500, part:null,
      forceStaffId: enforceStaff ? (session.prefStaffId||null) : null,
      durMin
    })
  }
  if(!slots.length && enforceStaff && session.prefStaffName){
    await sock.sendMessage(jid,{text:`Con *${session.prefStaffName}* no veo huecos en ese rango. Dime otra fecha/franja o si quieres te enseño el *equipo*.`})
    session.lastSlots=[]; saveSession(phone, session)
    return false
  }
  if(!slots.length){
    await sock.sendMessage(jid,{text:`No veo huecos ahora mismo. Dime otra fecha/franja (ej. “viernes tarde”).`})
    session.lastSlots=[]; saveSession(phone, session)
    return false
  }
  const usedPreferred = !!(enforceStaff && session.prefStaffId)
  const shown = slots.slice(0, SHOW_TOP_N)
  session.lastSlots = shown
  session.svcKey = envKey
  session.svcLabel = label
  session.lastListAt = Date.now()
  saveSession(phone, session)

  const bullets = shown.map(s=>{
    const base = `• ${fmtDay(s.date)} ${fmtHour(s.date)}`
    if (usedPreferred) return `${base} — ${session.prefStaffName || staffLabelFromId(session.prefStaffId)}`
    return base
  }).join("\n")

  const title = usedPreferred
    ? `Huecos con ${session.prefStaffName} para *${label}*:\n`
    : `Huecos del equipo para *${label}*:\n`
  const hint = `\nDime en texto cuál te viene (ej. “me vale la de las 13”, “otra tarde”, “martes”).`
  await sock.sendMessage(jid,{text: title + bullets + hint})
  return true
}

// ====== Mini resumen (solo si ya está todo)
function maybeSendSummary(session, jid, sock){
  if (session.sede && session.svcLabel && session.chosenSlot){
    const lines = [
      `Resumen:`,
      `• Salón: ${locationNice(session.sede)}`,
      `• Servicio: ${session.svcLabel}`,
      session.prefStaffName ? `• Profesional: ${session.prefStaffName}` : `• Profesional: Equipo`,
      `• Hora: ${fmtES(session.chosenSlot.date)}`,
      `• Duración: ${session.chosenSlot.durMin} min`
    ].filter(Boolean)
    const tail = `\nAhora una de las compañeras dará el OK ✅`
    sock.sendMessage(jid,{text: lines.join("\n")+tail})
  }
}

// ====== Web mini (QR + estado)
const app=express()
const PORT=process.env.PORT||8080
let lastQR=null, conectado=false
app.get("/", (_req,res)=>{
  res.send(`<!doctype html><meta charset="utf-8"><style>
  body{font-family:system-ui;display:grid;place-items:center;min-height:100vh;background:#f8f9fa;margin:0}
  .card{max-width:760px;padding:32px;border-radius:20px;box-shadow:0 8px 32px rgba(0,0,0,.08);background:white}
  .status{padding:12px;border-radius:8px;margin:8px 0}
  .success{background:#d4edda;color:#155724}
  .error{background:#f8d7da;color:#721c24}
  .warning{background:#fff3cd;color:#856404}
  </style><div class="card">
  <h1>Gapink Nails Bot</h1>
  <div class="status ${conectado ? 'success' : 'error'}">WhatsApp: ${conectado ? "✅ Conectado" : "❌ Desconectado"}</div>
  ${!conectado&&lastQR?`<div style="text-align:center;margin:20px 0"><img src="/qr.png" width="300" style="border-radius:8px"></div>`:""}
  <div class="status warning">Modo: ${DRY_RUN ? "Simulación" : "Producción"} | IA: DeepSeek (${DEEPSEEK_MODEL})</div>
  </div>`)
})
app.get("/qr.png", async (_req,res)=>{
  if(!lastQR) return res.status(404).send("No QR")
  const png = await qrcode.toBuffer(lastQR, { type:"png", width:512, margin:1 })
  res.set("Content-Type","image/png").send(png)
})

// ====== Baileys loader (ESM dinámico)
async function loadBaileys(){
  const mod = await import("@whiskeysockets/baileys")
  const makeWASocket = mod.makeWASocket || mod.default?.makeWASocket || (typeof mod.default==="function"?mod.default:undefined)
  const useMultiFileAuthState = mod.useMultiFileAuthState || mod.default?.useMultiFileAuthState
  const fetchLatestBaileysVersion = mod.fetchLatestBaileysVersion || mod.default?.fetchLatestBaileysVersion || (async()=>({version:[2,3000,0]}))
  const Browsers = mod.Browsers || mod.default?.Browsers || { macOS:(n="Desktop")=>["MacOS",n,"121.0.0"] }
  return { makeWASocket, useMultiFileAuthState, fetchLatestBaileysVersion, Browsers }
}

// ====== WhatsApp loop
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
      if (isFromMe) return

      // Sesión base
      let s = loadSession(phone) || {
        greeted:false, sede:null,
        svcKey:null, svcLabel:null,
        prefStaffId:null, prefStaffName:null,
        requestedStaffExplicitly:false,
        lastSlots:[],
        chosenSlot:null,
        lastListAt:0,
        lastAsked:null
      }

      // Primer saludo (una sola vez por día)
      if (!s.greeted){
        s.greeted = true
        saveSession(phone, s)
        await sock.sendMessage(jid,{text:`¡Hola! Soy el asistente de Gapink Nails 💅
Cuéntame en tus palabras qué necesitas (salón, servicio, día/franja y si quieres con alguien). Yo te lo resumo y te paso opciones.

Horario atención humana: L–V 10–14 y 16–20.`})
      }

      // ===== IA: entender intención
      const ai = await aiUnderstand(textRaw, s)

      // SALÓN
      if ((ai.intent==="set_salon" || /torremolinos|la luz|luz\b/i.test(textRaw)) && !s.sede){
        s.sede = /luz\b/i.test(textRaw) ? "la_luz" : /torremol/i.test(textRaw) ? "torremolinos" : (ai.salon||null)
        if(!s.sede && ai.salon) s.sede = ai.salon
        if(s.sede) saveSession(phone, s)
      }

      // STAFF
      if (ai.intent==="set_staff" || /\bcon\s+[a-zñáéíóúüï ]{2,}/i.test(norm(textRaw))){
        const r = resolveStaffFromText(ai.staff_name ? `con ${ai.staff_name}` : textRaw)
        if(r?.id){
          s.prefStaffId = r.id
          s.prefStaffName = r.label
          s.requestedStaffExplicitly = true
          saveSession(phone, s)
          if (s.sede && !staffAllowedInSede(s.prefStaffName, s.sede)){
            const where = staffCentersHuman(s.prefStaffName)
            await sock.sendMessage(jid,{text:`${s.prefStaffName} atiende en ${where}. Si quieres, dime “cámbialo a ${where.includes("La Luz")?"La Luz":"Torremolinos"}” o “me da igual, equipo”.`})
          } else {
            await sock.sendMessage(jid,{text:`Perfecto, lo miro con *${s.prefStaffName}* 👌`})
          }
        } else if (ai.staff_name){
          await sock.sendMessage(jid,{text:`No tengo a “${ai.staff_name}” en el equipo. Te enseño huecos del equipo salvo que me digas otro nombre.`})
        }
      }

      // Si no hay salón, pídelo
      if (!s.sede){
        await sock.sendMessage(jid,{text:`¿En qué salón te viene mejor? *Torremolinos* o *La Luz*.`})
        return
      }

      // LISTA COMPLETA DE UÑAS si el tema es uñas y no hay servicio
      if (ai.intent==="list_nails" || /\buñas|unas|manicura\b/i.test(norm(textRaw))){
        if (!s.svcKey){
          const list = nailServicesForSede(s.sede)
          if(!list.length){
            await sock.sendMessage(jid,{text:`No tengo servicios de *uñas* configurados en ${locationNice(s.sede)}.`})
          } else {
            const chunk=20
            for(let i=0;i<list.length;i+=chunk){
              const pg = list.slice(i,i+chunk).map(x=>`• ${x.label} — ${x.mins} min`).join("\n")
              await sock.sendMessage(jid,{text: (i===0?`Servicios de *uñas* en ${locationNice(s.sede)}:\n`:"Más opciones:\n")+pg })
            }
            await sock.sendMessage(jid,{text:`Dímelo tal cual en texto (p. ej. “Manicura Semipermanente” o “Quitar Uñas Esculpidas”).`})
            s.lastAsked = "fullNails"; saveSession(phone, s)
            return
          }
        }
      }

      // SERVICIO (por IA, sin números)
      if (!s.svcKey){
        const picked = await aiChooseServiceLabel(textRaw, s.sede)
        if (picked){
          const svc = servicesForSede(s.sede).find(x=> x.label.toLowerCase()===picked.toLowerCase())
          if (svc){
            s.svcKey = svc.key; s.svcLabel = svc.label; saveSession(phone, s)
            await sock.sendMessage(jid,{text:`Ok, *${s.svcLabel}* en ${locationNice(s.sede)}.`})
          }
        } else if (!/horario|huecos|tarde|mañana|noche|hoy|mañana|pasado|con\s+/i.test(textRaw)){
            await sock.sendMessage(jid,{text:`Dímelo con un poco más de detalle (ej. “cejas con hilo”, “manicura semipermanente”, “carbon peel”).`})
            return
        }
      }

      // Si ya tenemos servicio, ¿proponemos horas?
      if (s.svcKey && (ai.intent==="ask_slots" || ai.intent==="refine_time" || /horario|huecos|tarde|mañana|noche|hoy|mañana|pasado|viernes|lunes|martes|miércoles|jueves|sábado|domingo/i.test(textRaw))){
        const ok = await proposeHoursForService(
          s, jid, phone, sock, s.svcKey, s.svcLabel, ai.time_text || textRaw,
          { enforceStaff: !!(s.prefStaffId && s.requestedStaffExplicitly) }
        )
        if (ok) return
      }

      // Si tenemos lista mostrada y el cliente elige “la de las 13 / martes / primera…”
      if (Array.isArray(s.lastSlots) && s.lastSlots.length && (ai.intent==="pick_slot" || /primera|segunda|la de las|me vale|me viene|esa|esa misma|ok|perfect/i.test(norm(textRaw)))){
        const choice = await aiPickHour(textRaw, s.lastSlots)
        if (!choice){
          await sock.sendMessage(jid,{text:`No me quedó clara la hora. Dímela tal cual (ej. “martes 13:00” o “la de las 17:00”).`})
          return
        }
        // HOLD 6h
        releaseHoldsForPhone(phone) // soltamos anteriores del mismo cliente
        const hold = placeHold({ phone, sede:s.sede, envKey:s.svcKey, startEU:choice.date, durMin:choice.durMin, staffId: s.requestedStaffExplicitly ? s.prefStaffId : null })
        s.chosenSlot = choice; saveSession(phone, s)
        await sock.sendMessage(jid,{text:`Te reservo *provisionalmente* ${fmtES(choice.date)} (${choice.durMin} min) en ${locationNice(s.sede)}${s.prefStaffName?` con ${s.prefStaffName}`:""} durante 6 h ⏳.
Si te viene mal, dime otra hora/franja. Si te viene bien, te lo dejamos listo cuando lo revise una compañera.`})
        maybeSendSummary(s, jid, sock)
        return
      }

      // Si tenemos servicio pero aún no hemos mostrado horas
      if (s.svcKey && (!Array.isArray(s.lastSlots) || !s.lastSlots.length)){
        const ok = await proposeHoursForService(
          s, jid, phone, sock, s.svcKey, s.svcLabel, ai.time_text || "",
          { enforceStaff: !!(s.prefStaffId && s.requestedStaffExplicitly) }
        )
        if (ok) return
      }

      // Ver/editar/cancelar -> mensaje informativo (no gestionamos por aquí)
      if (ai.intent==="view" || /ver mi cita|cuando es|a que hora/i.test(norm(textRaw))){
        await sock.sendMessage(jid,{text:`Para *consultar, editar o cancelar* usa el enlace del *SMS/email* de confirmación ✅`})
        return
      }

      // Fallback amable
      if (!s.svcKey){
        await sock.sendMessage(jid,{text:`¿Qué quieres exactamente? (ej. “cejas con hilo”, “manicura semipermanente”).`})
      } else {
        await sock.sendMessage(jid,{text:`Dime una franja u hora (ej. “viernes tarde” o “13:00”) y te propongo huecos.`})
      }

    })
  }catch(e){
    console.error(e)
    setTimeout(() => startBot().catch(console.error), 4000)
  }
}

// ====== Arranque
console.log(`🩷 Gapink Nails Bot v38.0.0 — DeepSeek-only — Mini Web QR http://localhost:${PORT}`)
const appListen = app.listen(PORT, ()=>{ startBot().catch(console.error) })
process.on("uncaughtException", (e)=>{ console.error("💥 uncaughtException:", e?.stack||e?.message||e) })
process.on("unhandledRejection", (e)=>{ console.error("💥 unhandledRejection:", e) })
process.on("SIGTERM", ()=>{ try{ appListen.close(()=>process.exit(0)) }catch{ process.exit(0) } })
process.on("SIGINT", ()=>{ try{ appListen.close(()=>process.exit(0)) }catch{ process.exit(0) } })
