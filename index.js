// index.js — Gapink Nails · Conversación limpia + IA total + holds 6h
// Requisitos clave:
// - DeepSeek-only (sin OpenAI).
// - Mini web con QR.
// - Baileys con import dinámico (evita ERR_REQUIRE_ESM).
// - IA para TODO: detectar salón/servicio/staff/fecha-hora de mensajes en lenguaje natural.
// - Sin números: el usuario responde en texto (ej. “la de las 13”, “viernes tarde”) y la IA lo entiende.
// - Duraciones por ENV (SQ_DUR_* y SQ_DUR_luz_*), mapeadas por etiqueta.
// - Proponer huecos filtrando HOLDS de 6h (SQLite) — no crea reservas en Square, solo consulta.
// - Mostrar nombre de profesional SOLO si el cliente lo pidió explícitamente.
// - Lista completa de servicios de UÑAS cuando se pida; por defecto, lista corta y sin duplicados.
// - Resumen final breve y “Ahora una de las compañeras dará el OK ✅”.

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

// ====== Setup básico
if (!globalThis.crypto) globalThis.crypto = webcrypto
dayjs.extend(utc); dayjs.extend(tz); dayjs.extend(isoWeek); dayjs.locale("es")
const EURO_TZ = "Europe/Madrid"
const log = pino({ level: process.env.LOG_LEVEL || "info" })

// ====== Config horario
const WORK_DAYS = [1,2,3,4,5]      // L–V
const OPEN = { start: 9, end: 20 } // 09–20
const SLOT_MIN = 15
const NOW_MIN_OFFSET_MIN = Number(process.env.BOT_NOW_OFFSET_MIN || 30)
const SEARCH_WINDOW_DAYS = Number(process.env.BOT_SEARCH_WINDOW_DAYS || 14)
const HOLIDAYS_EXTRA = (process.env.HOLIDAYS_EXTRA || "06/01,28/02,15/08,12/10,01/11,06/12,08/12,25/12")
  .split(",").map(s=>s.trim()).filter(Boolean)
const SHOW_TOP_N = Number(process.env.SHOW_TOP_N || 5)

// ====== Flags
const DRY_RUN = /^true$/i.test(process.env.DRY_RUN || "")
const BOT_DEBUG = /^true$/i.test(process.env.BOT_DEBUG || "")

// ====== Square (SOLO CONSULTA)
const square = new Client({
  accessToken: process.env.SQUARE_ACCESS_TOKEN,
  environment: (process.env.SQUARE_ENV==="production") ? Environment.Production : Environment.Sandbox
})
const LOC_TORRE = (process.env.SQUARE_LOCATION_ID_TORREMOLINOS || process.env.SQUARE_LOCATION_ID || "").trim()
const LOC_LUZ   = (process.env.SQUARE_LOCATION_ID_LA_LUZ || "").trim()

// ====== IA DeepSeek ONLY
const DEEPSEEK_API_KEY = process.env.DEEPSEEK_API_KEY || ""
const DEEPSEEK_MODEL   = process.env.DEEPSEEK_MODEL || process.env.AI_MODEL || "deepseek-chat"
const DEEPSEEK_URL     = process.env.DEEPSEEK_API_URL || "https://api.deepseek.com/v1/chat/completions"
const AI_TIMEOUT_MS    = Number(process.env.AI_TIMEOUT_MS || 12000)
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
    const body = {
      model: DEEPSEEK_MODEL,
      messages,
      temperature: 0.2,
      max_tokens: 300,
      stream: false
    }
    const resp = await fetch(DEEPSEEK_URL, {
      method:"POST",
      headers:{ "Content-Type":"application/json", "Authorization":`Bearer ${DEEPSEEK_API_KEY}` },
      body: JSON.stringify(body),
      signal: controller.signal
    })
    clearTimeout(timeout)
    if (!resp.ok) return null
    const data = await resp.json()
    return data?.choices?.[0]?.message?.content || null
  }catch(e){
    clearTimeout(timeout)
    if (BOT_DEBUG) log.error(e)
    return null
  }
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
const onlyDigits = s => String(s||"").replace(/\D+/g,"")
const rm = s => String(s||"").normalize("NFD").replace(/\p{Diacritic}/gu,"")
const norm = s => rm(s).toLowerCase().replace(/[+.,;:()/_-]/g," ").replace(/[^\p{Letter}\p{Number}\s]/gu," ").replace(/\s+/g," ").trim()
function titleCase(str){ return String(str||"").toLowerCase().replace(/\b([a-z])/g, (m)=>m.toUpperCase()) }
function applySpanishDiacritics(label){
  let x = String(label||"")
  x = x.replace(/\bunas\b/gi, m => m[0] === 'U' ? 'Uñas' : 'uñas')
  x = x.replace(/\bfrances\b/gi, m => m[0]==='F' ? 'Francés' : 'francés')
  x = x.replace(/\bnivelacion\b/gi, m => m[0]==='N' ? 'Nivelación' : 'nivelación')
  x = x.replace(/\bsemi ?permanente\b/gi, m => /[A-Z]/.test(m[0]) ? 'Semipermanente' : 'semipermanente')
  x = x.replace(/\bpestan(as?)?\b/gi, (m) => (m[0]==='P'?'Pestañ':'pestañ') + 'as')
  return x
}
function cleanDisplayLabel(label){
  const s = String(label||"").replace(/^\s*(luz|la\s*luz)\s+/i,"").trim()
  return applySpanishDiacritics(s)
}
function stableKey(parts){ const raw=Object.values(parts).join("|"); return createHash("sha256").update(raw).digest("hex").slice(0,48) }
const nowEU = ()=>dayjs().tz(EURO_TZ)
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
function parseToEU(input){
  if (dayjs.isDayjs(input)) return input.clone().tz(EURO_TZ)
  const s = String(input||"")
  if (/[Zz]|[+\-]\d{2}:?\d{2}$/.test(s)) return dayjs(s).tz(EURO_TZ)
  return dayjs.tz(s, EURO_TZ)
}

// ====== Horario helpers
function isHolidayEU(d){
  const dd=String(d.date()).padStart(2,"0"), mm=String(d.month()+1).padStart(2,"0")
  return HOLIDAYS_EXTRA.includes(`${dd}/${mm}`)
}
function insideBusinessHours(d, dur){
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

// ====== DB (sesiones + holds)
const db = new Database("gapink.db"); db.pragma("journal_mode=WAL")
db.exec(`
CREATE TABLE IF NOT EXISTS sessions (
  phone TEXT PRIMARY KEY,
  data_json TEXT,
  updated_at TEXT
);
CREATE TABLE IF NOT EXISTS slot_holds (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  phone TEXT,
  sede TEXT,
  service_env_key TEXT,
  service_label TEXT,
  staff_id TEXT,
  start_iso TEXT,
  duration_min INTEGER,
  reason TEXT,
  created_at TEXT,
  expires_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_slot_holds ON slot_holds (sede, start_iso, staff_id);
`)
function loadSession(phone){
  const row = db.prepare(`SELECT data_json FROM sessions WHERE phone=@phone`).get({phone})
  if (!row?.data_json) return null
  return JSON.parse(row.data_json)
}
function saveSession(phone, s){
  const j=JSON.stringify(s||{})
  const up=db.prepare(`UPDATE sessions SET data_json=@j, updated_at=@u WHERE phone=@p`).run({j,u:new Date().toISOString(),p:phone})
  if (up.changes===0) db.prepare(`INSERT INTO sessions (phone,data_json,updated_at) VALUES (@p,@j,@u)`).run({p:phone,j,u:new Date().toISOString()})
}
function pruneExpiredHolds(){
  db.prepare(`DELETE FROM slot_holds WHERE expires_at <= @now`).run({ now:new Date().toISOString() })
}
function isSlotHeldByOther({sede, start_iso, staff_id, phone}){
  pruneExpiredHolds()
  const row = db.prepare(`
    SELECT 1 FROM slot_holds
    WHERE sede=@sede AND start_iso=@start_iso AND IFNULL(staff_id,'')=IFNULL(@staff_id,'')
      AND phone != @phone AND expires_at > @now LIMIT 1
  `).get({ sede, start_iso, staff_id: staff_id||"", phone, now:new Date().toISOString() })
  return !!row
}
function placeHold({ phone, sede, service_env_key, service_label, staff_id, start_iso, duration_min, reason }){
  pruneExpiredHolds()
  const expires = dayjs().add(6,"hour").toISOString()
  db.prepare(`
    INSERT INTO slot_holds (phone, sede, service_env_key, service_label, staff_id, start_iso, duration_min, reason, created_at, expires_at)
    VALUES (@phone,@sede,@service_env_key,@service_label,@staff_id,@start_iso,@duration_min,@reason,@now,@exp)
  `).run({
    phone, sede, service_env_key, service_label, staff_id: staff_id||null,
    start_iso, duration_min, reason: reason||"user_pick",
    now: new Date().toISOString(), exp: expires
  })
}

// ====== Empleadas (bookable + sedes admitidas vía EMP_CENTER_* o sufijo en ENV)
function parseEmployees(){
  const out=[]
  for (const [k,v] of Object.entries(process.env)){
    if (!k.startsWith("SQ_EMP_")) continue
    const parts = String(v||"").split("|").map(s=>s.trim())
    const id = parts[0]; if (!id) continue
    const bookTag = (parts[1]||"BOOKABLE").toUpperCase()
    const bookable = ["BOOKABLE","TRUE","YES","1"].includes(bookTag)
    const labels = k.replace(/^SQ_EMP_/,"").toLowerCase().split("_").filter(Boolean)
    const display = titleCase(labels[0] || `Prof ${id.slice(-4)}`)
    out.push({ id, bookable, labels: [display, ...new Set(labels.map(titleCase))] })
  }
  // centros permitidos (si hay EMP_CENTER_NAME="la_luz,torremolinos")
  for (const e of out){
    const key = `EMP_CENTER_${rm(e.labels[0]).toUpperCase().replace(/\s+/g,"_")}`
    const raw = process.env[key] || ""
    const centers = raw.split(",").map(s=>s.trim()).filter(Boolean)
    e.centers = centers.length ? centers : ["la_luz","torremolinos"] // por defecto ambas
  }
  return out
}
let EMPLOYEES = parseEmployees()
function staffLabelFromId(id){ return EMPLOYEES.find(x=>x.id===id)?.labels?.[0] || null }
function fuzzyStaffFromText(text){
  const t = " " + norm(text) + " "
  // coin cid simple por nombres conocidos (primera etiqueta)
  for (const e of EMPLOYEES){
    for (const lbl of e.labels){
      const token = norm(lbl)
      const re = new RegExp(`(^|\\s)${token}(\\s|$)`)
      if (re.test(t)) return e
    }
  }
  const m = t.match(/\scon\s+([a-zñáéíóúüï\s]{2,})\b/i)
  if (m){
    const guess = norm(m[1])
    for (const e of EMPLOYEES){
      for (const lbl of e.labels){
        if (norm(lbl)===guess) return e
      }
    }
  }
  return null
}

// ====== Servicios + Duraciones por sede
function servicesForSede(sedeKey){
  const out = []
  for (const [k,v] of Object.entries(process.env)){
    if (sedeKey === "la_luz") {
      if (!/^SQ_SVC_luz_/.test(k)) continue
    } else {
      if (!/^SQ_SVC_/.test(k) || /^SQ_SVC_luz_/.test(k)) continue
    }
    const [id] = String(v||"").split("|"); if (!id) continue
    const raw = k.replace(/^SQ_SVC_(luz_)?/,"").replaceAll("_"," ")
    const label = cleanDisplayLabel(titleCase(raw))
    out.push({ sedeKey, key:k, id, label, norm: norm(label) })
  }
  return out
}
function allServices(){ return [...servicesForSede("torremolinos"), ...servicesForSede("la_luz")] }

function dedupeByLabel(list){
  const seen=new Set()
  return list.filter(x=>{
    const key = x.label.toLowerCase()
    if (seen.has(key)) return false
    seen.add(key); return true
  })
}

// Duraciones por ENV -> mapa por (sede:any|la_luz|torremolinos) + labelLower
const DUR_MAP = new Map()
;(function buildDurMap(){
  for (const [k,v] of Object.entries(process.env)){
    if (!/^SQ_DUR_/.test(k)) continue
    const mins = Number(String(v||"").trim() || "0")
    const isLuz = /^SQ_DUR_luz_/.test(k)
    const sede = isLuz ? "la_luz" : "any"
    const raw = k.replace(/^SQ_DUR_(luz_)?/,"").replaceAll("_"," ")
    const label = cleanDisplayLabel(titleCase(raw)).toLowerCase()
    DUR_MAP.set(`${sede}:${label}`, mins>0?mins:0)
  }
})()
function durationMinForLabel(label, sede){
  const k1 = `${sede||"any"}:${String(label||"").toLowerCase()}`
  const k2 = `any:${String(label||"").toLowerCase()}`
  if (DUR_MAP.has(k1)) return DUR_MAP.get(k1)
  if (DUR_MAP.has(k2)) return DUR_MAP.get(k2)
  return 60
}

// ====== Categoría “uñas” para lista completa/limpia
function nailServicesForSede(sedeKey){
  const bad = /(pestañ|pestanas|extensiones|relleno pesta|ceja|facial|depil|axilas|labio|piernas|endosphere|masaje|laser)/i
  const good = /(uñas|unas|manicura|esculpidas|uña rota|una rota|reconstruccion de una|relleno.*uñas|quitar.*uñas|baby boomer|francesa|nivelacion|rusa)/i
  const arr = servicesForSede(sedeKey)
  const filtered = arr.filter(s=>{
    const L = s.label.toLowerCase()
    return good.test(L) && !bad.test(L)
  }).map(s=>({ label:s.label, key:s.key, mins: durationMinForLabel(s.label, sedeKey) }))
  const unique = dedupeByLabel(filtered)
  return unique.sort((a,b)=> a.label.localeCompare(b.label,'es',{sensitivity:'base'}))
}

// ====== Square: disponibilidad (SOLO consulta)
function locationToId(key){ return key==="la_luz" ? LOC_LUZ : LOC_TORRE }
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
    let avail = resp?.result?.availabilities || []
    const out=[]
    for (const a of avail){
      const d = dayjs(a.startAt).tz(EURO_TZ)
      const segs = Array.isArray(a.appointmentSegments) ? a.appointmentSegments
                 : Array.isArray(a.segments) ? a.segments : []
      const staffId = segs[0]?.teamMemberId || null
      out.push({ date:d, staffId })
      if (out.length>=limit) break
    }
    out.sort((a,b)=>a.date.valueOf()-b.date.valueOf())
    return out
  }catch{
    return []
  }
}

// ====== Mini web QR
const app = express()
const PORT = process.env.PORT || 8080
let lastQR=null, conectado=false
app.get("/", (_req,res)=>{
  res.send(`<!doctype html><meta charset="utf-8"><style>
  :root{color-scheme:light dark}
  body{font-family:system-ui;display:grid;place-items:center;min-height:100vh;background:#0f1115;color:#eaeef5;margin:0}
  .card{max-width:720px;padding:28px;border-radius:20px;background:#151922;box-shadow:0 10px 40px rgba(0,0,0,.35)}
  .muted{opacity:.8}
  .ok{color:#7dffaf}.bad{color:#ff8a8a}
  a{color:#8ab4ff}
  </style>
  <div class="card">
    <h1>Gapink Nails — WhatsApp Bot</h1>
    <p class="muted">Estado WhatsApp: ${conectado ? '<b class="ok">Conectado</b>' : '<b class="bad">Desconectado</b>'}</p>
    ${!conectado&&lastQR?`<p>Escanea este QR:</p><img src="/qr.png" width="300" style="border-radius:12px">`:""}
    <p class="muted">Modo: ${DRY_RUN ? "Simulación" : "Producción"} · IA: DeepSeek</p>
  </div>`)
})
app.get("/qr.png", async (_req,res)=>{
  if(!lastQR) return res.status(404).send("No QR")
  const png = await qrcode.toBuffer(lastQR, { type:"png", width:512, margin:1 })
  res.set("Content-Type","image/png").send(png)
})

// ====== IA de conversación (JSON minimalista)
function staffRosterForPrompt(){
  return EMPLOYEES.map(e=>{
    return `• ID:${e.id} | Nombres:[${e.labels.join(", ")}] | Sedes:[${(e.centers||[]).join(", ")}] | Reservable:${e.bookable}`
  }).join("\n")
}
function servicesBlockForPrompt(){
  const tor = servicesForSede("torremolinos").map(s=>`- ${s.label} | ${s.key}`).join("\n")
  const luz = servicesForSede("la_luz").map(s=>`- ${s.label} | ${s.key}`).join("\n")
  return `SERVICIOS TORREMOLINOS:\n${tor}\n\nSERVICIOS LA LUZ:\n${luz}`
}
function durationsBlockForPrompt(){
  const lines=[]
  for (const [k,v] of DUR_MAP.entries()){ lines.push(`${k} => ${v}`) }
  return lines.join("\n")
}
function buildSystemPrompt(){
  const now = nowEU().format("dddd DD/MM/YYYY HH:mm")
  return `Eres el asistente de WhatsApp de un salón de belleza. Respondes SOLO JSON válido y corto.

Ahora: ${now} (Europe/Madrid)
Sedes: torremolinos (LOC:${LOC_TORRE||"?"}), la_luz (LOC:${LOC_LUZ||"?"})

Profesionales:
${staffRosterForPrompt()}

${servicesBlockForPrompt()}

DURACIONES (clave: "sede:labelLower"):
${durationsBlockForPrompt()}

REGLAS:
- Devuelve SIEMPRE JSON con claves: { "intent": "...", "salon":?, "servicio_label":?, "staff_name":?, "texto_hora":?, "pide_lista":?, "categoria":? }
- "intent" ∈ {"saludo","escoger_salon","escoger_servicio","escoger_staff","pedir_horas","elegir_hora","lista_servicios","ver_editar_cancelar","otro"}.
- Extrae TODO del lenguaje natural ("la de las 13", "viernes tarde", "con ganna", "uñas", "cejas con hilo", "cámbialo a la luz"...).
- Si dice "uñas" o "lista de uñas", pon {"intent":"lista_servicios","categoria":"uñas"}.
- Si pide horas y no hay servicio claro, pon {"intent":"escoger_servicio"}.
- Si especifica profesional, rellena "staff_name" literal.
- "texto_hora" debe contener la preferencia temporal libre ("viernes tarde", "martes 13 a las 13", "la primera", "la de las 13").
- Nunca pidas números; el usuario SIEMPRE escribe en texto.
- Mantén tokens al mínimo, sin prosa ni explicaciones.
`
}
async function aiInterpret(textRaw){
  const sys = buildSystemPrompt()
  const out = await aiChat(sys, `Mensaje: "${textRaw}"\nDevuelve SOLO el JSON.`)
  return stripToJSON(out) || { intent:"otro" }
}

// ====== Propuesta de horas (filtra holds y opcional staff)
function enumerateSlots(slots){ return slots.map(d=>({ iso:d.format("YYYY-MM-DDTHH:mm"), pretty:`${fmtDay(d)} ${fmtHour(d)}` })) }

async function proposeTimes({ phone, sede, svcKey, svcLabel, durationMin, staffIdOrNull, temporalHint }){
  const baseNow = nextOpeningFrom(nowEU().add(NOW_MIN_OFFSET_MIN,"minute"))
  const from = baseNow.clone()
  const to   = baseNow.clone().add(SEARCH_WINDOW_DAYS,"day").hour(OPEN.end).minute(0)
  const raw = await searchAvailWindow({ locationKey:sede, envServiceKey:svcKey, startEU:from, endEU:to, limit:500 })

  // filtro staff si se pidió explícitamente
  let filt = raw
  if (staffIdOrNull){
    filt = raw.filter(s => s.staffId === staffIdOrNull)
  }

  // filtra por HOLDS de otros
  const free = []
  for (const r of filt){
    const iso = r.date.tz("UTC").toISOString()
    if (isSlotHeldByOther({ sede, start_iso:iso, staff_id: staffIdOrNull?staffIdOrNull:(r.staffId||""), phone })) continue
    if (insideBusinessHours(r.date, durationMin||60)) free.push(r)
    if (free.length>=250) break
  }
  free.sort((a,b)=>a.date.valueOf()-b.date.valueOf())

  if (!free.length) return { list:[], text:"No veo huecos en ese rango. Dime otra franja o día (ej. “viernes tarde”)." }

  // Si hay preferencia temporal (“viernes tarde”, “la de las 13”...) intentamos priorizar
  function scoreByHint(d, hint){
    if (!hint) return 0
    const t = norm(hint)
    let s = 0
    if (/\bmanana\b/.test(t)) s += (d.hour()<13)?3:0
    if (/\btarde\b/.test(t))  s += (d.hour()>=15 && d.hour()<19)?3:0
    if (/\bnoche\b/.test(t))  s += (d.hour()>=18)?2:0
    const m = t.match(/\b(\d{1,2})[:.](\d{2})\b/)
    if (m){ const hh=Number(m[1]), mm=Number(m[2]); if (d.hour()===hh && d.minute()===mm) s += 5 }
    return s
  }
  free.sort((a,b)=> (scoreByHint(b.date,temporalHint)-scoreByHint(a.date,temporalHint)) || (a.date.valueOf()-b.date.valueOf()))

  const shown = free.slice(0, SHOW_TOP_N)
  const slots = shown.map(s=>s.date)
  const withStaff = new Map(shown.map(s=>[s.date.format("YYYY-MM-DDTHH:mm"), s.staffId||null]))
  return { list: slots, staffByIso: withStaff }
}

// ====== Texto UX ultra simple
const SELF_SERVICE = "Para ver/editar/cancelar usa el enlace del SMS/email de confirmación ✅"
function greet(){
  return "¡Hola! Soy el asistente de Gapink Nails 💅\nCuéntame salón, lo que quieres y (si quieres) con quién. Te propongo horas."
}

// ====== WhatsApp (Baileys con import dinámico)
async function loadBaileys(){
  const require = createRequire(import.meta.url); let mod=null
  try{ mod=require("@whiskeysockets/baileys") }catch{}; 
  if(!mod){ try{ mod=await import("@whiskeysockets/baileys") }catch{} }
  if(!mod) throw new Error("Baileys no disponible")
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
    globalThis.sock = sock

    sock.ev.on("connection.update", ({connection,qr})=>{
      if (qr){ lastQR=qr; conectado=false; try{ qrcodeTerminal.generate(qr,{small:true}) }catch{} }
      if (connection==="open"){ lastQR=null; conectado=true; log.info("WhatsApp conectado") }
      if (connection==="close"){ conectado=false; setTimeout(()=>{ startBot().catch(console.error) }, 2500) }
    })
    sock.ev.on("creds.update", saveCreds)

    // ====== Mensajería
    sock.ev.on("messages.upsert", async ({messages})=>{
      const m=messages?.[0]; if (!m?.message) return
      const jid = m.key.remoteJid
      const isFromMe = !!m.key.fromMe
      const phone = (jid||"").split("@")[0]
      const textRaw = (m.message.conversation || m.message.extendedTextMessage?.text || m.message?.imageMessage?.caption || "").trim()
      if (!textRaw) return

      // Cola por teléfono para evitar condiciones de carrera
      if (!globalThis.__q) globalThis.__q = new Map()
      const QUEUE = globalThis.__q
      const prev=QUEUE.get(phone)||Promise.resolve()
      const job=prev.then(async ()=>{
        try{
          let s = loadSession(phone) || {
            greeted:false, sede:null, svcKey:null, svcLabel:null, durationMin:60,
            preferStaffId:null, preferStaffLabel:null, preferExplicit:false,
            lastProposedISO:[], lastProposedStaffByIso:{}, lastTemporalHint:null,
            name:null, email:null
          }

          // silenciar puntitos (cliente)
          if (/^[\s.·•⋅]+$/.test(textRaw)){ return }

          if (isFromMe) { saveSession(phone, s); return }

          // Saludo 1a vez
          if (!s.greeted){
            s.greeted=true; saveSession(phone,s)
            await sock.sendMessage(jid,{ text: greet() })
          }

          // Interpretación IA
          const ai = await aiInterpret(textRaw)

          // Actualizaciones de estado desde IA
          if (ai?.salon){
            const t = norm(ai.salon)
            if (/\bluz\b/.test(t)) s.sede = "la_luz"
            if (/\btorre|torremolinos\b/.test(t)) s.sede = "torremolinos"
          }

          // Staff si lo pide
          if (ai?.staff_name){
            const staff = fuzzyStaffFromText("con " + ai.staff_name)
            if (staff && staff.bookable){
              s.preferStaffId = staff.id
              s.preferStaffLabel = staff.labels[0]
              s.preferExplicit = true
              // si la sede no coincide con el staff, sugerimos cambio:
              if (s.sede && !staff.centers.includes(s.sede)){
                await sock.sendMessage(jid,{ text:`${s.preferStaffLabel} atiende en ${staff.centers.map(c=> c==="la_luz"?"La Luz":"Torremolinos").join(" / ")}. Si quieres, dime “cámbialo a ${staff.centers[0]==="la_luz"?"La Luz":"Torremolinos"}” o “me vale el equipo”.` })
              }
            }
          }

          // Servicio
          if (ai?.servicio_label){
            const pickFrom = s.sede ? servicesForSede(s.sede) : allServices()
            const exact = pickFrom.find(x=> x.label.toLowerCase()===ai.servicio_label.toLowerCase())
            if (exact){
              s.svcKey = exact.key
              s.svcLabel = exact.label
              s.durationMin = durationMinForLabel(exact.label, s.sede||"any")
            }
          }

          // Lista de uñas si la pide
          if (ai?.intent==="lista_servicios" && (ai?.categoria?.toLowerCase?.()==="uñas" || /uñ|unas|manicura/.test(norm(textRaw)))){
            if (!s.sede){
              await sock.sendMessage(jid,{ text:"¿En qué salón? Torremolinos o La Luz." })
              saveSession(phone,s); return
            }
            const list = nailServicesForSede(s.sede)
            if (!list.length){
              await sock.sendMessage(jid,{ text:`No tengo servicios de *uñas* en ${s.sede==="la_luz"?"La Luz":"Torremolinos"}.` })
              saveSession(phone,s); return
            }
            const showAll = /completa|toda|entera|todas/i.test(textRaw)
            const subset = showAll ? list : list.slice(0,12)
            const bullets = subset.map(x=>`• ${x.label} — ${x.mins||60} min`).join("\n")
            const more = showAll ? "" : `\n\n¿Quieres la *lista completa*? dímelo.`
            await sock.sendMessage(jid,{ text:`Servicios de uñas en ${s.sede==="la_luz"?"La Luz":"Torremolinos"}:\n${bullets}${more}` })
            saveSession(phone,s); return
          }

          // Si dice “uñas” sin detalle -> sugerencias cortas
          if (!s.svcKey && /uñ|unas|manicura/.test(norm(textRaw))){
            if (!s.sede){ await sock.sendMessage(jid,{ text:"¿En qué salón te viene mejor? Torremolinos o La Luz." }); saveSession(phone,s); return }
            const list = nailServicesForSede(s.sede)
            if (!list.length){ await sock.sendMessage(jid,{ text:`No tengo servicios de uñas en ${s.sede==="la_luz"?"La Luz":"Torremolinos"}.` }); saveSession(phone,s); return }
            const popular = [
              "Manicura Semipermanente",
              "Manicura Semipermanete Con Nivelación",
              "Manicura Rusa Con Nivelación",
              "Uñas Nuevas Esculpidas",
              "Relleno Uñas Esculpidas",
              "Quitar Uñas Esculpidas",
              "Uñas Nuevas Esculpidas Francesa Baby Boomer Encapsulados"
            ].map(x=>x.toLowerCase())
            const map = new Map(list.map(x=>[x.label.toLowerCase(),x]))
            const top = popular.map(n=>map.get(n)).filter(Boolean)
            const rest = list.filter(x=>!top.find(t=>t.label.toLowerCase()===x.label.toLowerCase()))
            const short = dedupeByLabel([...top,...rest]).slice(0,8)
            const bullets = short.map(x=>`• ${x.label} — ${x.mins||60} min`).join("\n")
            await sock.sendMessage(jid,{ text:`En ${s.sede==="la_luz"?"La Luz":"Torremolinos"}, ¿cuál te encaja?\n${bullets}\n\nDímelo tal cual en texto (ej. “Quitar Uñas Esculpidas”).` })
            saveSession(phone,s); return
          }

          // Pedir horas (si tenemos salón+servicio)
          const wantsHours = ai?.intent==="pedir_horas" || ai?.intent==="elegir_hora" || /hora|hueco|cuando|cuándo|agenda|esta semana|pr[oó]xima semana|viernes|tarde|mañana|noche|la de las/i.test(norm(textRaw))
          if (wantsHours && (!s.sede || !s.svcKey)){
            if (!s.sede){ await sock.sendMessage(jid,{ text:"¿En qué salón te viene mejor? Torremolinos o La Luz." }); saveSession(phone,s); return }
            if (!s.svcKey){ await sock.sendMessage(jid,{ text:"Dime el servicio tal cual (ej. “Cejas con hilo”, “Manicura semipermanente”)." }); saveSession(phone,s); return }
          }

          // Si ya tenemos salón + servicio → proponemos
          if (s.sede && s.svcKey && (wantsHours || s.lastProposedISO.length===0)){
            const staffId = s.preferExplicit ? s.preferStaffId : null
            const hint = ai?.texto_hora || null
            const prop = await proposeTimes({
              phone, sede:s.sede, svcKey:s.svcKey, svcLabel:s.svcLabel||"Servicio",
              durationMin:s.durationMin||60, staffIdOrNull:staffId, temporalHint:hint
            })
            if (!prop.list.length){
              await sock.sendMessage(jid,{ text:`No veo huecos en ese rango. Dime otra franja/día (ej. “viernes tarde”).` })
              saveSession(phone,s); return
            }
            const enumd = prop.list.map(d=>({ iso:d.format("YYYY-MM-DDTHH:mm"), pretty:`${fmtDay(d)} ${fmtHour(d)}` }))
            s.lastProposedISO = enumd.map(e=>e.iso)
            s.lastProposedStaffByIso = Object.fromEntries(prop.staffByIso||[])
            s.lastTemporalHint = hint
            saveSession(phone,s)

            const header = s.preferExplicit && s.preferStaffLabel
              ? `Huecos con ${s.preferStaffLabel}:`
              : `Huecos del equipo:`
            const lines = enumd.map(e=>{
              const name = (s.preferExplicit ? "" : "") // no mostramos nombres si no lo pidió
              return `• ${e.pretty}${s.preferExplicit && s.preferStaffLabel ? "" : ""}`
            }).join("\n")
            await sock.sendMessage(jid,{ text:
              `${s.svcLabel || "Servicio"} en ${s.sede==="la_luz"?"La Luz":"Torremolinos"}:\n${header}\n${lines}\n\nDime en texto cuál te viene (ej. “la de las 12:30”, “viernes tarde”, “otra”).`
            })
            return
          }

          // Elegir hora concreta en texto → adquirir HOLD 6h
          if (s.sede && s.svcKey && ai?.intent==="elegir_hora"){
            // heurística: si dice "la primera" cogemos el primer propuesto; si dice hora exacta, buscamos match
            let pickIso = null
            const t = norm(ai?.texto_hora || textRaw)
            if (/primera|la de arriba|la que sea/i.test(t) && s.lastProposedISO.length){
              pickIso = s.lastProposedISO[0]
            } else {
              const h = t.match(/\b(\d{1,2})[:.](\d{2})\b/)
              if (h){
                const hh = String(h[1]).padStart(2,"0"), mm=String(h[2]).padStart(2,"0")
                const found = s.lastProposedISO.find(iso => iso.endsWith(`${hh}:${mm}`))
                if (found) pickIso = found
              }
            }

            if (!pickIso && s.lastProposedISO.length){
              // fallback: si no reconoce, coge el primero pero le pedimos confirmación suave
              pickIso = s.lastProposedISO[0]
            }
            if (!pickIso){
              await sock.sendMessage(jid,{ text:`No me quedó clara la hora. Dímela tal cual (ej. “martes 10/09 a las 13:00” o “la de las 13”).` })
              saveSession(phone,s); return
            }

            // HOLD 6h (no toca Square booking)
            const staffForIso = s.lastProposedStaffByIso?.[pickIso] || (s.preferExplicit ? s.preferStaffId : null) || null
            placeHold({
              phone, sede:s.sede, service_env_key:s.svcKey, service_label:s.svcLabel||"Servicio",
              staff_id: staffForIso, start_iso: dayjs.tz(pickIso, EURO_TZ).tz("UTC").toISOString(),
              duration_min: s.durationMin||60, reason:"user_pick"
            })

            await sock.sendMessage(jid,{ text:
              `Perfecto. Dejo bloqueado ${fmtES(dayjs.tz(pickIso, EURO_TZ))} para ${s.svcLabel || "tu servicio"} en ${s.sede==="la_luz"?"La Luz":"Torremolinos"}${s.preferExplicit&&s.preferStaffLabel?` (con ${s.preferStaffLabel})`:""}.\n\nAhora una de las compañeras revisa y te da el OK ✅\n${SELF_SERVICE}`
            })
            return
          }

          // Si pide ver/editar/cancelar
          if (ai?.intent==="ver_editar_cancelar"){
            await sock.sendMessage(jid,{ text: SELF_SERVICE })
            saveSession(phone,s); return
          }

          // Si aún falta algo, guía mínima:
          if (!s.sede){
            await sock.sendMessage(jid,{ text:"¿En qué salón te viene mejor? Torremolinos o La Luz." })
            saveSession(phone,s); return
          }
          if (!s.svcKey){
            await sock.sendMessage(jid,{ text:"Dime el servicio tal cual (ej. “Cejas con hilo”, “Manicura semipermanente”)." })
            saveSession(phone,s); return
          }

          // Fallback amable
          await sock.sendMessage(jid,{ text:"Te leo — dime una hora o franja (ej. “viernes tarde”, “la de las 13”)." })
          saveSession(phone,s)

        }catch(err){
          if (BOT_DEBUG) console.error(err)
          try{ await globalThis.sock.sendMessage(messages?.[0]?.key?.remoteJid, { text:"No te he entendido bien. ¿Puedes decirlo de otra forma? 😊" }) }catch{}
        }
      })
      QUEUE.set(phone, job.finally(()=>{ if (QUEUE.get(phone)===job) QUEUE.delete(phone) }))
    })
  }catch(e){
    log.error(e)
    setTimeout(()=>{ startBot().catch(console.error) }, 4000)
  }
}

// ====== Arranque
console.log(`🩷 Gapink Nails Bot — DeepSeek-only — Mini Web QR http://localhost:${PORT}`)
const server = app.listen(PORT, ()=>{ startBot().catch(console.error) })

process.on("SIGTERM", ()=>{ try{ server.close(()=>process.exit(0)) }catch{ process.exit(0) } })
process.on("SIGINT",  ()=>{ try{ server.close(()=>process.exit(0)) }catch{ process.exit(0) } })
