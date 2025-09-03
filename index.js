// index.js — Gapink Nails · v36.0.0 (DeepSeek-only, IA full, holds 6h, sin números)
// by Gonzalo + ChatGPT 👾

// ===== Core deps
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
import { Client, Environment } from "square"

// ===== Setup
if (!globalThis.crypto) globalThis.crypto = webcrypto
dayjs.extend(utc); dayjs.extend(tz); dayjs.extend(isoWeek); dayjs.locale("es")
const EURO_TZ = "Europe/Madrid"
const nowEU = ()=>dayjs().tz(EURO_TZ)

// ===== Config horario
const WORK_DAYS = [1,2,3,4,5]  // L–V
const SLOT_MIN = 15
const OPEN = { start: 9, end: 20 }
const NOW_MIN_OFFSET_MIN = Number(process.env.BOT_NOW_OFFSET_MIN || 30)
const SEARCH_WINDOW_DAYS = Number(process.env.BOT_SEARCH_WINDOW_DAYS || 14)
const HOLIDAYS_EXTRA = (process.env.HOLIDAYS_EXTRA || "06/01,28/02,15/08,12/10,01/11,06/12,08/12,25/12")
  .split(",").map(s=>s.trim()).filter(Boolean)
const SHOW_TOP_N = Number(process.env.SHOW_TOP_N || 5)
const HOLD_HOURS = 6

// ===== Flags
const BOT_DEBUG = /^true$/i.test(process.env.BOT_DEBUG || "")
const DRY_RUN = /^true$/i.test(process.env.DRY_RUN || "")
const STEER_BALANCE = (process.env.BOT_STEER_BALANCE||"on").toLowerCase()==="on"
const SQUARE_MAX_RETRIES = Number(process.env.SQUARE_MAX_RETRIES || 3)

// ===== Square (SOLO CONSULTA — no crear reservas)
const square = new Client({
  accessToken: process.env.SQUARE_ACCESS_TOKEN,
  environment: (process.env.SQUARE_ENV==="production") ? Environment.Production : Environment.Sandbox
})
const LOC_TORRE = (process.env.SQUARE_LOCATION_ID_TORREMOLINOS || "").trim()
const LOC_LUZ   = (process.env.SQUARE_LOCATION_ID_LA_LUZ || "").trim()
const ADDRESS_TORRE = process.env.ADDRESS_TORREMOLINOS || "Av. de Benyamina 18, Torremolinos"
const ADDRESS_LUZ   = process.env.ADDRESS_LA_LUZ || "Málaga – Barrio de La Luz"

// ===== IA DeepSeek (único proveedor)
const DEEPSEEK_API_KEY = process.env.DEEPSEEK_API_KEY || ""
const DEEPSEEK_MODEL = process.env.DEEPSEEK_MODEL || process.env.AI_MODEL || "deepseek-chat"
const DEEPSEEK_URL = process.env.DEEPSEEK_API_URL || "https://api.deepseek.com/v1/chat/completions"
const AI_TIMEOUT_MS = Number(process.env.AI_TIMEOUT_MS || 12000)
const sleep = ms => new Promise(r=>setTimeout(r, ms))

async function aiChat(system, user, extraMsgs=[]){
  if (!DEEPSEEK_API_KEY) return null
  const controller = new AbortController()
  const to = setTimeout(()=>controller.abort(), AI_TIMEOUT_MS)
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
    clearTimeout(to)
    if (!resp.ok) return null
    const data = await resp.json()
    return data?.choices?.[0]?.message?.content || null
  }catch{ clearTimeout(to); return null }
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

// ===== Utils
const onlyDigits = s => String(s||"").replace(/\D+/g,"")
const rm = s => String(s||"").normalize("NFD").replace(/\p{Diacritic}/gu,"")
const norm = s => rm(s).toLowerCase().replace(/[+.,;:()/_-]/g," ").replace(/[^\p{Letter}\p{Number}\s]/gu," ").replace(/\s+/g," ").trim()
function stableKey(parts){ const raw=Object.values(parts).join("|"); return createHash("sha256").update(raw).digest("hex").slice(0,48) }
function applySpanishDiacritics(label){
  let x = String(label||"")
  x = x.replace(/\bunas\b/gi, m => m[0] === 'U' ? 'Uñas' : 'uñas')
  x = x.replace(/\bpestan(as?)?\b/gi, (m) => (m[0]==='P'?'Pestañ':'pestañ') + 'as')
  x = x.replace(/\bnivelacion\b/gi, m => m[0]==='N' ? 'Nivelación' : 'nivelación')
  x = x.replace(/\bfrances\b/gi, m => m[0]==='F' ? 'Francés' : 'francés')
  x = x.replace(/\bmas\b/gi, (m) => (m[0]==='M' ? 'Más' : 'más'))
  x = x.replace(/\bsemi ?permanente\b/gi, m => /[A-Z]/.test(m[0]) ? 'Semipermanente' : 'semipermanente')
  x = x.replace(/\bninas\b/gi, 'niñas')
  return x
}
function titleCase(str){ return String(str||"").toLowerCase().replace(/\b([a-záéíóúñ])/g, (m)=>m.toUpperCase()) }
function cleanDisplayLabel(label){
  const s = String(label||"").replace(/^\s*(luz|la\s*luz)\s+/i,"").trim()
  return applySpanishDiacritics(s)
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
function ceilToSlotEU(t){
  const m=t.minute(), rem=m%SLOT_MIN
  return rem===0 ? t.second(0).millisecond(0) : t.add(SLOT_MIN-rem,"minute").second(0).millisecond(0)
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
function isHolidayEU(d){
  const dd=String(d.date()).padStart(2,"0"), mm=String(d.month()+1).padStart(2,"0")
  return HOLIDAYS_EXTRA.includes(`${dd}/${mm}`)
}
function locationNice(key){ return key==="la_luz" ? "Málaga – La Luz" : "Torremolinos" }
function locationToId(key){ return key==="la_luz" ? LOC_LUZ : LOC_TORRE }

// ===== DB
const db=new Database("gapink.db"); db.pragma("journal_mode = WAL")
db.exec(`
CREATE TABLE IF NOT EXISTS sessions (
  phone TEXT PRIMARY KEY,
  data_json TEXT,
  updated_at TEXT
);
CREATE TABLE IF NOT EXISTS holds (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  phone TEXT,
  location_key TEXT,
  service_env_key TEXT,
  start_iso TEXT,
  expires_at TEXT,
  created_at TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_holds_unique ON holds (location_key, service_env_key, start_iso);
CREATE INDEX IF NOT EXISTS idx_holds_exp ON holds (expires_at);
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
function saveSession(phone,s){
  const j = JSON.stringify(s||{})
  const up=db.prepare(`UPDATE sessions SET data_json=@j, updated_at=@u WHERE phone=@p`).run({j,u:new Date().toISOString(),p:phone})
  if (up.changes===0) db.prepare(`INSERT INTO sessions (phone,data_json,updated_at) VALUES (@p,@j,@u)`).run({p:phone,j,u:new Date().toISOString()})
}
function loadSession(phone){
  const row = db.prepare(`SELECT data_json FROM sessions WHERE phone=@phone`).get({phone})
  if (!row?.data_json) return {}
  return JSON.parse(row.data_json)
}
function clearExpiredHolds(){
  db.prepare(`DELETE FROM holds WHERE datetime(expires_at) <= datetime('now')`).run()
}
function isHeldByOther({ location_key, service_env_key, start_iso, phone }){
  clearExpiredHolds()
  const row = db.prepare(`SELECT phone FROM holds WHERE location_key=@l AND service_env_key=@k AND start_iso=@s AND datetime(expires_at) > datetime('now')`).get({l:location_key,k:service_env_key,s:start_iso})
  return !!(row && row.phone !== phone)
}
function putHolds({ phone, location_key, service_env_key, start_isos=[] }){
  clearExpiredHolds()
  const stmt = db.prepare(`INSERT OR REPLACE INTO holds (phone,location_key,service_env_key,start_iso,expires_at,created_at)
  VALUES (@phone,@l,@k,@s, datetime('now','+${HOLD_HOURS} hours'), datetime('now'))`)
  db.transaction(()=>{
    for (const s of start_isos){
      stmt.run({ phone, l:location_key, k:service_env_key, s })
    }
  })()
}
function releaseHoldsForPhone({ phone, location_key=null, service_env_key=null }){
  const q = location_key && service_env_key
    ? `DELETE FROM holds WHERE phone=@p AND location_key=@l AND service_env_key=@k`
    : `DELETE FROM holds WHERE phone=@p`
  db.prepare(q).run({ p:phone, l:location_key, k:service_env_key })
}

// ===== Logging helper
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
function logEvent({direction, action, phone, raw_text=null, reply_text=null, extra=null, success=1, error=null}){
  try{
    const req = safeJSONStringify({direction, raw_text, extra})
    const res = safeJSONStringify({reply_text})
    db.prepare(`INSERT INTO square_logs (phone, action, request_data, response_data, error_data, timestamp, success)
      VALUES (@phone, @action, @req, @res, @err, @ts, @ok)`)
      .run({ phone: phone||"unknown", action: `${direction}_${action||"event"}`, req, res, err: error? safeJSONStringify(error): null, ts:new Date().toISOString(), ok: success?1:0 })
  }catch{}
}

// ===== Empleadas
function deriveLabelsFromEnvKey(envKey){
  const raw = envKey.replace(/^SQ_EMP_/, "")
  const toks = raw.split("_").map(t=>norm(t)).filter(Boolean)
  const uniq = Array.from(new Set(toks))
  const labels = [...uniq]
  if (uniq.length>1) labels.push(uniq.join(" "))
  return labels.map(l=>l.replace(/\b([a-z])/g,m=>m.toUpperCase()))
}
function parseStaffCenters(){
  // EMP_CENTER_<NAME>= "la_luz" | "torremolinos" | "la_luz,torremolinos"
  const centers = {}
  for (const [k,v] of Object.entries(process.env)) {
    if (!k.startsWith("EMP_CENTER_")) continue
    const name = k.replace(/^EMP_CENTER_/, "")
    const vals = String(v||"").split(",").map(s=>norm(s)).filter(Boolean)
    const set = new Set(vals.map(s=> s.includes("luz") ? "la_luz" : "torremolinos"))
    centers[name.toLowerCase()] = [...set]
  }
  return centers
}
const STAFF_CENTERS = parseStaffCenters()
function parseEmployees(){
  const out=[]
  for (const [k,v] of Object.entries(process.env)) {
    if (!k.startsWith("SQ_EMP_")) continue
    const parts = String(v||"").split("|").map(s=>s.trim())
    const id = parts[0]; if (!id) continue
    const bookTag = (parts[1]||"BOOKABLE").toUpperCase()
    const bookable = !/NO_BOOKABLE/.test(bookTag)
    const labels = deriveLabelsFromEnvKey(k)
    const keyname = k.replace(/^SQ_EMP_/, "").toLowerCase()
    const centers = STAFF_CENTERS[keyname] || ["la_luz","torremolinos"] // default ambas
    out.push({ envKey:k, id, bookable, labels, centers })
  }
  return out
}
let EMPLOYEES = parseEmployees()
function staffLabelFromId(id){
  const e = EMPLOYEES.find(x=>x.id===id)
  return e?.labels?.[0] || (id ? `Prof. ${String(id).slice(-4)}` : null)
}
function fuzzyStaffFromText(text){
  const t = " " + norm(text) + " "
  // nombres aislados (palabra completa)
  for (const e of EMPLOYEES){
    for (const lbl of e.labels){
      const tok = norm(lbl)
      const re = new RegExp(`(^|\\s)${tok}(\\s|$)`)
      if (re.test(t)) return e
    }
  }
  if (/\bcon\s+(quien\s*sea|el\s*equipo|cualquiera|me\s*da\s*igual)\b/i.test(t)) return { anyTeam:true }
  return null
}
function uniqueStaffFromLastProposed(session){
  const vals = Object.values(session.lastStaffByIso||{}).filter(Boolean)
  const uniq = [...new Set(vals)]
  return (uniq.length===1) ? uniq[0] : null
}

// ===== Servicios + Duraciones
function makeLabelFromKey(base, sedeKey){
  let label = base.replaceAll("_"," ")
  label = titleCase(label)
  label = applySpanishDiacritics(label)
  if (sedeKey==="la_luz") label = label.replace(/^Luz\s+/i,"").trim()
  return cleanDisplayLabel(label)
}
function servicesForSedeKeyRaw(sedeKey){
  const prefix = (sedeKey==="la_luz") ? "SQ_SVC_luz_" : "SQ_SVC_"
  const out=[]
  for (const [k,v] of Object.entries(process.env)){
    if (!k.startsWith(prefix)) continue
    const [id, verRaw] = String(v||"").split("|")
    if (!id) continue
    const base = k.replace(prefix,"")
    out.push({ sedeKey, key:k, id, version: verRaw? Number(verRaw): null, label: makeLabelFromKey(base, sedeKey) })
  }
  return out
}
function allServices(){ return [...servicesForSedeKeyRaw("torremolinos"), ...servicesForSedeKeyRaw("la_luz")] }

function durationMapForSede(sedeKey){
  const prefix = (sedeKey==="la_luz") ? "SQ_DUR_luz_" : "SQ_DUR_"
  const out = new Map()
  for (const [k,v] of Object.entries(process.env)){
    if (!k.startsWith(prefix)) continue
    const mins = Number(String(v||"0").trim() || "0")
    const base = k.replace(prefix,"")
    const label = makeLabelFromKey(base, sedeKey)
    out.set(label.toLowerCase(), mins>0? mins : 60)
  }
  return out
}
function attachDurations(list){
  const dm_luz = durationMapForSede("la_luz")
  const dm_tor = durationMapForSede("torremolinos")
  return list.map(s=>{
    const dm = s.sedeKey==="la_luz" ? dm_luz : dm_tor
    const mins = dm.get((s.label||"").toLowerCase()) || 60
    return { ...s, mins }
  })
}
function resolveEnvKeyFromLabelAndSede(label, sedeKey){
  const list = servicesForSedeKeyRaw(sedeKey)
  const found = list.find(s=> s.label.toLowerCase() === String(label||"").toLowerCase())
  return found ? { envKey:found.key, id:found.id, version:found.version, label:found.label, mins: attachDurations([found])[0].mins } : null
}

// ===== IA: comprensión
function buildSystemPrompt(session){
  const now = nowEU().format("YYYY-MM-DD HH:mm")
  // Para ahorrar tokens: solo nombres de staff + centros y SOLO etiquetas de servicios (sin IDs)
  const staffLines = EMPLOYEES.filter(e=>e.bookable).map(e=>`- ${e.labels[0]} (centros: ${e.centers.map(c=>c==="la_luz"?"La Luz":"Torremolinos").join("/")})`).join("\n")
  const svcTor = servicesForSedeKeyRaw("torremolinos").map(s=>`- ${s.label}`).join("\n")
  const svcLuz = servicesForSedeKeyRaw("la_luz").map(s=>`- ${s.label}`).join("\n")
  return `Eres el asistente de WhatsApp de Gapink Nails. Responde SOLO JSON válido. Objetivo: entender mensaje libre y avanzar con la mínima pregunta posible.

Hora local: ${now} Europe/Madrid.
Centros: Torremolinos y La Luz.

Profesionales (bookable):
${staffLines}

Servicios TORREMOLINOS (solo etiquetas):
${svcTor}

Servicios LA LUZ (solo etiquetas):
${svcLuz}

Devuelve JSON con la forma:
{
 "salon": "torremolinos|la_luz|null",
 "service_label": "texto exacto de servicio o null si no seguro",
 "staff_name": "nombre o null",
 "datetime_hint": "frase de tiempo del usuario o null",
 "need_services_list": boolean,  // true si el usuario quiere/conviene ver lista (p.ej. pidió 'uñas' sin concretar)
 "category_guess": "unas|cejas|depilacion|facial|pestanas|null"
}

Reglas:
- Si dice "con ella" y previamente viste huecos con una sola profesional, considera staff_name="__CON_ELLA__".
- Si dice algo como “quitarme las uñas” o la intención no está clara, marca need_services_list=true y category_guess="unas".
- No inventes servicios; usa etiquetas de la lista del centro correspondiente si puedes.
`
}

async function aiInterpret(textRaw, session){
  const sys = buildSystemPrompt(session)
  const ctx = `Mensaje: "${textRaw}"`
  const out = await aiChat(sys, ctx)
  const obj = stripToJSON(out) || {}
  return {
    salon: (obj.salon==="la_luz"||obj.salon==="torremolinos") ? obj.salon : null,
    service_label: obj.service_label || null,
    staff_name: obj.staff_name || null,
    datetime_hint: obj.datetime_hint || null,
    need_services_list: !!obj.need_services_list,
    category_guess: obj.category_guess || null
  }
}

// Ranking por IA (ordenar lista completa por probabilidad)
async function rankServicesByAI(userText, services){
  try{
    const sys = `Ordena servicios por lo que mejor encaja con la petición del usuario. 
Devuelve SOLO: {"ordered":["label1","label2",...]}`
    const msg = `Usuario: "${userText}"\nServicios:\n${services.map(s=>`- ${s.label}`).join("\n")}\nSolo JSON.`
    const out = await aiChat(sys, msg)
    const js = stripToJSON(out)
    if (Array.isArray(js?.ordered) && js.ordered.length){
      const pos = new Map(js.ordered.map((l,i)=>[l.toLowerCase(), i]))
      const known = services.filter(s=>pos.has(s.label.toLowerCase()))
      const unknown = services.filter(s=>!pos.has(s.label.toLowerCase()))
      known.sort((a,b)=> pos.get(a.label.toLowerCase()) - pos.get(b.label.toLowerCase()))
      return [...known, ...unknown]
    }
  }catch{}
  return services
}

// Coincidencia natural de hora elegida (sin números)
async function aiPickFromOffered(userText, offeredPrettyList){
  try{
    const sys = `Tienes una lista de opciones de fecha/hora en español. El usuario responde en texto libre ("me vale la de las 13", "viernes tarde", "la primera"). 
Devuelve SOLO JSON: {"pick_index": <0-based or null>}. Si no se puede decidir, null.`
    const msg = `Opciones:\n${offeredPrettyList.map((t,i)=>`${i+1}. ${t}`).join("\n")}\nUsuario: "${userText}"\nJSON:`
    const out = await aiChat(sys, msg)
    const js = stripToJSON(out)
    if (js && Number.isInteger(js.pick_index) && js.pick_index>=0 && js.pick_index<offeredPrettyList.length) return js.pick_index
  }catch{}
  return null
}

// ===== Square helpers (CONSULTA)
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
async function searchAvailWindow({ locationKey, envServiceKey, startEU, endEU, limit=500 }){
  const sv = await getServiceIdAndVersion(envServiceKey)
  if (!sv?.id) return []
  const body = {
    query:{ filter:{
      startAtRange:{ startAt: startEU.tz("UTC").toISOString(), endAt: endEU.tz("UTC").toISOString() },
      locationId: locationToId(locationKey),
      segmentFilters: [{ serviceVariationId: sv.id }]
    } }
  }
  try{
    const resp = await square.bookingsApi.searchAvailability(body)
    const avail = resp?.result?.availabilities || []
    const out=[]
    for (const a of avail){
      if (!a?.startAt) continue
      const d = dayjs(a.startAt).tz(EURO_TZ)
      if (d.hour()<OPEN.start || d.hour()>=OPEN.end) continue
      const segs = Array.isArray(a.appointmentSegments) ? a.appointmentSegments
                 : Array.isArray(a.segments) ? a.segments : []
      const tm = segs[0]?.teamMemberId || null
      out.push({ date:d, staffId: tm })
      if (out.length>=limit) break
    }
    out.sort((a,b)=>a.date.valueOf()-b.date.valueOf())
    return out
  }catch(e){
    return []
  }
}

// ===== IA listas (uñas) — sin keywords visibles
async function nailsServicesForSedeRankedByAI(sedeKey, userText){
  const raw = servicesForSedeKeyRaw(sedeKey)
  const withDur = attachDurations(raw)
  // Pide al modelo que filtre SOLO uñas y ordene por probabilidad
  try{
    const sys = `De la lista dada, filtra SOLO servicios de UÑAS (manicura/pedicura/esculturado/esmaltados/quitado/rellenos) y ordénalos por relevancia al texto del usuario.
Devuelve SOLO JSON: {"ordered":["label1","label2",...]}`
    const msg = `Usuario: "${userText}"\nServicios:\n${withDur.map(s=>`- ${s.label}`).join("\n")}\nSolo JSON.`
    const out = await aiChat(sys, msg)
    const js = stripToJSON(out)
    if (Array.isArray(js?.ordered) && js.ordered.length){
      const map = new Map(js.ordered.map((l,i)=>[l.toLowerCase(), i]))
      const list = withDur.filter(s=>map.has(s.label.toLowerCase()))
      list.sort((a,b)=>map.get(a.label.toLowerCase()) - map.get(b.label.toLowerCase()))
      return list
    }
  }catch{}
  // fallback: mostrar todos con duración
  return withDur
}

// ===== Proponer horas (con holds 6h y filtro staff si procede)
async function proposeTimes({ phone, sede, svcKey, svcLabel, durationMin, staffIdOrNull=null, temporalHint=null }){
  const now = nowEU()
  let startEU = nextOpeningFrom(now.add(NOW_MIN_OFFSET_MIN, "minute"))
  let endEU = startEU.clone().add(SEARCH_WINDOW_DAYS, "day").hour(OPEN.end).minute(0)
  // Amortiguar con “próxima semana / viernes tarde…” vía IA ligera (opcional)
  if (STEER_BALANCE && temporalHint){
    const t = norm(temporalHint)
    if (/\bmanana\b/.test(t)) startEU = startEU.add(1,"day")
    if (/\bpasado\b/.test(t)) startEU = startEU.add(2,"day")
    if (/\bproxima\s+semana|semana\s+que\s+viene\b/.test(t)) { startEU = startEU.add(7,"day"); endEU = startEU.clone().add(7,"day") }
    if (/\b(tarde)\b/.test(t)) startEU = startEU.hour(15).minute(0)
    if (/\b(manana|mañana)\b/.test(t)) startEU = startEU.hour(9).minute(0)
  }

  const rawSlots = await searchAvailWindow({
    locationKey: sede,
    envServiceKey: svcKey,
    startEU, endEU, limit: 500
  })

  // Filtro staff si se pide (o “con ella” resuelto antes)
  let slots = staffIdOrNull ? rawSlots.filter(s => s.staffId === staffIdOrNull) : rawSlots

  // Filtrar los que estén en hold por OTROS
  slots = slots.filter(s => !isHeldByOther({ location_key:sede, service_env_key:svcKey, start_iso:s.date.tz("UTC").toISOString(), phone }))

  // Top N
  const top = slots.slice(0, SHOW_TOP_N)
  const mapIsoToStaff = {}
  const offeredISO = []
  for (const s of top){
    const iso = s.date.format("YYYY-MM-DDTHH:mm")
    offeredISO.push(s.date.tz("UTC").toISOString())
    mapIsoToStaff[iso] = s.staffId || null
  }
  // Poner holds 6h para este teléfono (bloquean al resto en WhatsApp)
  putHolds({ phone, location_key:sede, service_env_key:svcKey, start_isos:offeredISO })

  return {
    list: top.map(s=>s.date),
    staffByIso: Object.entries(mapIsoToStaff).map(([k,v])=>[k,v]),
    usedStaffFilter: !!staffIdOrNull
  }
}

// ===== Mini web + Baileys ESM
const app=express()
const PORT=process.env.PORT||8080
let lastQR=null, conectado=false
app.get("/", (_req,res)=>{
  res.send(`<!doctype html><meta charset="utf-8"><style>
  body{font-family:system-ui;display:grid;place-items:center;min-height:100vh;background:#f6f7f8}
  .card{max-width:780px;padding:28px;border-radius:18px;box-shadow:0 10px 40px rgba(0,0,0,.08);background:white}
  .status{padding:10px 12px;border-radius:8px;margin:8px 0;font-weight:600}
  .success{background:#e6ffed;color:#057a55}
  .error{background:#ffe8e8;color:#b00020}
  .muted{color:#666}
  </style><div class="card">
  <h1>Gapink Nails Bot — v36</h1>
  <div class="status ${conectado ? 'success' : 'error'}">WhatsApp: ${conectado ? "✅ Conectado" : "❌ Desconectado"}</div>
  ${!conectado&&lastQR?`<div style="text-align:center;margin:16px 0"><img src="/qr.png" width="300" style="border-radius:8px;border:1px solid #eee"></div>`:""}
  <p class="muted">Modo: ${DRY_RUN ? "Simulación (sin Square)" : "Consulta en Square"} · IA: DeepSeek</p>
  </div>`)
})
app.get("/qr.png", async (_req,res)=>{
  if(!lastQR) return res.status(404).send("No QR")
  const png = await qrcode.toBuffer(lastQR, { type:"png", width:512, margin:1 })
  res.set("Content-Type","image/png").send(png)
})
async function loadBaileys(){
  // ESM safe
  let mod = null
  try{ mod = await import("@whiskeysockets/baileys") }catch{}
  if(!mod) throw new Error("Baileys no disponible")
  const makeWASocket = mod.makeWASocket || mod.default?.makeWASocket || (typeof mod.default==="function"?mod.default:undefined)
  const useMultiFileAuthState = mod.useMultiFileAuthState || mod.default?.useMultiFileAuthState
  const fetchLatestBaileysVersion = mod.fetchLatestBaileysVersion || mod.default?.fetchLatestBaileysVersion || (async()=>({version:[2,3000,0]}))
  const Browsers = mod.Browsers || mod.default?.Browsers || { macOS:(n="Desktop")=>["MacOS",n,"121.0.0"] }
  return { makeWASocket, useMultiFileAuthState, fetchLatestBaileysVersion, Browsers }
}

// ===== Square: customers (para pedir datos solo al final si no hay ficha)
function normalizePhoneES(raw){
  const d=onlyDigits(raw); if(!d) return null
  if (raw.startsWith("+") && d.length>=8 && d.length<=15) return `+${d}`
  if (d.startsWith("34") && d.length===11) return `+${d}`
  if (d.length===9) return `+34${d}`
  if (d.startsWith("00")) return `+${d.slice(2)}`
  return `+${d}`
}
async function searchCustomersByPhone(phone){
  try{
    const e164=normalizePhoneES(phone); if(!e164) return []
    const got = await square.customersApi.searchCustomers({ query:{ filter:{ phoneNumber:{ exact:e164 } } } })
    return got?.result?.customers || []
  }catch{ return [] }
}

// ===== Saludo minimal
function buildGreeting(){
  return `¡Hola! Soy el asistente de Gapink Nails 💅
Cuéntame *salón*, lo que quieres y (si quieres) con quién y cuándo. Te propongo horas.
Horario atención humana: L–V 10–14 y 16–20.`
}

// ===== Conversación
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

    // Cola por teléfono para evitar carreras
    if (!globalThis.__q) globalThis.__q = new Map()

    sock.ev.on("messages.upsert", async ({messages})=>{
      const m=messages?.[0]; if (!m?.message) return
      const jid = m.key.remoteJid
      const isFromMe = !!m.key.fromMe
      const phoneRaw = (jid||"").split("@")[0]||""
      const phone = normalizePhoneES(phoneRaw) || phoneRaw
      const textRaw = (m.message.conversation || m.message.extendedTextMessage?.text || m.message?.imageMessage?.caption || "").trim()
      if (!textRaw) return

      const QUEUE = globalThis.__q
      const prev=QUEUE.get(phone)||Promise.resolve()
      const job=prev.then(async ()=>{
        try{
          let s = loadSession(phone) || {}
          s.lastStaffByIso = s.lastStaffByIso || {}
          const now = nowEU()

          // Silencio si solo puntitos -> 6h
          if (/^[\s.·•⋅]+$/.test(textRaw)){
            s.snooze_until_ms = now.add(6,"hour").valueOf(); saveSession(phone, s)
            return
          }
          if (s.snooze_until_ms && now.valueOf() < s.snooze_until_ms){ saveSession(phone,s); return }

          // Saludo una vez cada 24h
          if (!s.greetedAt || (Date.now()- (s.greetedAt||0)) > 24*60*60*1000){
            s.greetedAt = Date.now(); saveSession(phone,s)
            await sock.sendMessage(jid,{ text: buildGreeting() })
          }

          // === IA interpretación principal
          const ai = await aiInterpret(textRaw, s)

          // Actualizar salón si lo trae
          if (ai.salon){ s.sede = ai.salon; saveSession(phone,s) }

          // Staff pedido (“con Tania” o “con ella”)
          let wantStaffId = null
          if (ai.staff_name === "__CON_ELLA__"){
            const uid = uniqueStaffFromLastProposed(s)
            if (uid) { wantStaffId = uid; s.preferStaffId = uid; s.preferStaffLabel = staffLabelFromId(uid); s.preferExplicit = true; saveSession(phone,s) }
          } else if (ai.staff_name){
            const fz = fuzzyStaffFromText("con " + ai.staff_name)
            if (fz && !fz.anyTeam && fz.bookable){
              wantStaffId = fz.id
              s.preferStaffId = fz.id
              s.preferStaffLabel = fz.labels[0]
              s.preferExplicit = true
              saveSession(phone,s)
              if (s.sede && !fz.centers.includes(s.sede)){
                await sock.sendMessage(jid,{ text: `${s.preferStaffLabel} atiende en ${fz.centers.map(c=>c==="la_luz"?"La Luz":"Torremolinos").join(" / ")}. Si te viene, dime “cámbialo a ${fz.centers[0]==="la_luz"?"La Luz":"Torremolinos"}” o “me vale el equipo”.` })
              }
            }
          }

          // Servicio: si viene claro, resolvemos a envKey
          if (ai.service_label && s.sede){
            const r = resolveEnvKeyFromLabelAndSede(ai.service_label, s.sede)
            if (r){
              s.svcKey = r.envKey
              s.svcLabel = r.label
              s.durationMin = r.mins
              saveSession(phone,s)
            }
          }

          // Si pide uñas ambiguo o “quitarme las uñas” o IA pide lista -> lista completa UÑAS ordenada por IA
          const ambiguousNails = ai.need_services_list && (ai.category_guess==="unas")
          const textN = norm(textRaw)
          const isQuitarUnas = /\bquitar[a-z\s]*\b.*\b(uñ|unas)\b/.test(textN)
          if ((ambiguousNails || isQuitarUnas) && s.sede){
            const ranked = await nailsServicesForSedeRankedByAI(s.sede, textRaw)
            const bullets = ranked.map(x=>`• ${x.label} — ${x.mins} min`).join("\n")
            await sock.sendMessage(jid,{ text:
              `Opciones de uñas en ${locationNice(s.sede)} (ordenadas por lo que más te encaja):\n${bullets}\n\nDímelo tal cual en texto (ej. “Quitar Uñas Esculpidas”).`
            })
            saveSession(phone,s)
            return
          }

          // Si no hay salón aún: pregunta mínima
          if (!s.sede){
            await sock.sendMessage(jid,{ text:"¿En qué salón te viene mejor? *Torremolinos* o *La Luz*." })
            saveSession(phone,s); return
          }

          // Si no hay servicio claro: ofrecer lista *ordenada por IA* de TODO el salón (o intentar categoría)
          if (!s.svcKey){
            const full = attachDurations(servicesForSedeKeyRaw(s.sede))
            const ranked = await rankServicesByAI(textRaw, full)
            const bullets = ranked.map(x=>`• ${x.label} — ${x.mins} min`).join("\n")
            await sock.sendMessage(jid,{ text:
              `Dímelo en tus palabras o elige uno (te los ordeno por lo más probable):\n${bullets}\n\nEscríbelo tal cual en texto (sin números).`
            })
            saveSession(phone,s); return
          }

          // Tenemos salón + servicio -> proponer horas (equipo o con staff si lo pidió)
          // Limpia holds previos del mismo usuario para este servicio y salón (evita “secuestro” acumulado)
          releaseHoldsForPhone({ phone, location_key:s.sede, service_env_key:s.svcKey })

          const staffIdForQuery = wantStaffId || s.preferStaffId || null
          const prop = await proposeTimes({
            phone, sede:s.sede, svcKey:s.svcKey, svcLabel:s.svcLabel||"Servicio",
            durationMin:s.durationMin||60, staffIdOrNull:staffIdForQuery, temporalHint: ai.datetime_hint
          })

          if (!prop.list.length){
            // Si pidió alguien concreto y no hay, probar con equipo
            if (staffIdForQuery){
              const propTeam = await proposeTimes({
                phone, sede:s.sede, svcKey:s.svcKey, svcLabel:s.svcLabel||"Servicio",
                durationMin:s.durationMin||60, staffIdOrNull:null, temporalHint: ai.datetime_hint
              })
              if (propTeam.list.length){
                const pretty = propTeam.list.map(d=>`${fmtDay(d)} ${fmtHour(d)}`)
                s.lastProposedISO = propTeam.list.map(d=>d.format("YYYY-MM-DDTHH:mm"))
                s.lastStaffByIso = Object.fromEntries(propTeam.staffByIso||[])
                saveSession(phone,s)
                await sock.sendMessage(jid,{ text:
                  `No veo huecos con ${s.preferStaffLabel||"esa profesional"} en ese rango.\nTe paso *huecos del equipo* en ${locationNice(s.sede)} para ${s.svcLabel}:\n`+
                  pretty.map(p=>`• ${p}`).join("\n")+
                  `\n\nDime en texto cuál te viene (ej. “la de las 13”, “viernes tarde”, “otra”).`
                })
                return
              }
            }
            await sock.sendMessage(jid,{ text:`No veo huecos en ese rango. Dime otra franja/fecha (ej. “viernes tarde”, “la próxima semana”).` })
            return
          }

          const enumd = prop.list.map(d=>({ iso:d.format("YYYY-MM-DDTHH:mm"), pretty:`${fmtDay(d)} ${fmtHour(d)}` }))
          s.lastProposedISO = enumd.map(e=>e.iso)
          s.lastStaffByIso = Object.fromEntries(prop.staffByIso||[])
          s.lastSvc = { key:s.svcKey, label:s.svcLabel, sede:s.sede, dur:s.durationMin||60 }
          saveSession(phone,s)

          const header = s.preferStaffId ? `Huecos con ${s.preferStaffLabel}:` : `Huecos del equipo:`
          await sock.sendMessage(jid,{ text:
            `${s.svcLabel} en ${locationNice(s.sede)}\n${header}\n`+
            enumd.map(e=>`• ${e.pretty}${s.preferStaffId?"":""}`).join("\n")+
            `\n\nDime en texto cuál te viene (ej. “la de las 13”, “viernes tarde”, “otra”).`
          })

          // === Espera siguiente mensaje -> intentar casar con una de las opciones sin números
          // (Se resuelve en el próximo turno con aiPickFromOffered)

        }catch(err){
          if (BOT_DEBUG) console.error(err)
          logEvent({direction:"sys", action:"handler_error", phone, error:{message:err?.message, stack:err?.stack}, success:0})
          await sock.sendMessage(jid,{ text:"No te he entendido bien. ¿Puedes decirlo de otra forma? 😊" })
        }
      })
      QUEUE.set(phone, job.finally(()=>{ if (QUEUE.get(phone)===job) QUEUE.delete(phone) }))

      // Segundo paso: si ya había una lista propuesta, intentar casar elección libre
      job.then(async ()=>{
        try{
          let s = loadSession(phone) || {}
          if (!s.lastProposedISO || !Array.isArray(s.lastProposedISO) || !s.lastProposedISO.length) return
          // Intentar pick con IA
          const offeredPretty = s.lastProposedISO.map(iso=>{
            const d = dayjs(iso).tz(EURO_TZ)
            return `${fmtDay(d)} ${fmtHour(d)}`
          })
          const pickIdx = await aiPickFromOffered(textRaw, offeredPretty)
          if (pickIdx==null) return
          const pickIso = s.lastProposedISO[pickIdx]
          if (!pickIso) return

          const staffIdFromIso = s.lastStaffByIso?.[dayjs(pickIso).format("YYYY-MM-DDTHH:mm")] || s.preferStaffId || null
          s.chosen = { iso:pickIso, staffId: staffIdFromIso }
          saveSession(phone,s)

          // Datos cliente (pedir al final solo si no hay ficha)
          const custs = await searchCustomersByPhone(phone)
          const haveFicha = custs.length>0

          const d = dayjs(pickIso).tz(EURO_TZ)
          const resumen = `✅ Te reservo provisionalmente:
📍 ${locationNice(s.sede)}
🧾 ${s.svcLabel} — ${s.durationMin||60} min
🕐 ${fmtES(d)}${staffIdFromIso?`\n👩‍💼 ${staffLabelFromId(staffIdFromIso)}`:""}

He hecho un *bloqueo interno* durante ${HOLD_HOURS}h para ese hueco. `
          const tail = haveFicha
            ? `Ahora una compañera lo confirma en el sistema y te avisamos ✅`
            : `Para dejarlo listo, dime *tu nombre* y (opcional) tu *email*. En cuanto lo tenga, una compañera lo confirma ✅`

          await sock.sendMessage(jid,{ text: resumen + tail })
          // Tras confirmar intent, mantenemos el hold y no tocamos Square (solo consulta).
        }catch(e){}
      })
    })
  }catch(e){
    setTimeout(() => startBot().catch(console.error), 4000)
  }
}

// ===== Arranque
console.log(`🩷 Gapink Nails Bot v36.0.0 — DeepSeek-only — Mini Web QR http://localhost:${PORT}`)
const appListen = app.listen(PORT, ()=>{ startBot().catch(console.error) })
process.on("uncaughtException", (e)=>{ console.error("💥 uncaughtException:", e?.stack||e?.message||e) })
process.on("unhandledRejection", (e)=>{ console.error("💥 unhandledRejection:", e) })
process.on("SIGTERM", ()=>{ try{ appListen.close(()=>process.exit(0)) }catch{ process.exit(0) } })
process.on("SIGINT", ()=>{ try{ appListen.close(()=>process.exit(0)) }catch{ process.exit(0) } })
