// index.js — Gapink Nails · v38.2.0 (Básico + Centros por empleada)
// Conversación para “analfabetos”: todo por números y frases cortas.
// Novedad: usa EMP_CENTER_* para restringir cada profesional a sus salones.
// Si piden “con X” en un salón donde X no atiende → ofrece: 1) cambiar salón, 2) ver equipo aquí.

import express from "express"
import pino from "pino"
import qrcodeTerminal from "qrcode-terminal"
import qrcode from "qrcode"
import "dotenv/config"
import fs from "fs"
import Database from "better-sqlite3"
import dayjs from "dayjs"
import utc from "dayjs/plugin/utc.js"
import tz from "dayjs/plugin/timezone.js"
import isoWeek from "dayjs/plugin/isoWeek.js"
import "dayjs/locale/es.js"
import { Client, Environment } from "square"
import { createHash, webcrypto } from "crypto"
if (!globalThis.crypto) globalThis.crypto = webcrypto

// ====== Fecha/Hora
dayjs.extend(utc); dayjs.extend(tz); dayjs.extend(isoWeek); dayjs.locale("es")
const EURO_TZ = "Europe/Madrid"
const nowEU = ()=>dayjs().tz(EURO_TZ)

// ====== Config simple
const WORK_DAYS = [1,2,3,4,5]              // L–V
const OPEN = { start: 9, end: 20 }         // 09:00–20:00
const SLOT_MIN = 30
const SHOW_TOP_N = Number(process.env.SHOW_TOP_N || 5)
const HOLD_HOURS = Number(process.env.HOLD_HOURS || 6)
const SEARCH_WINDOW_DAYS = Number(process.env.SEARCH_WINDOW_DAYS || 14)
const SEARCH_WINDOW_EXT_DAYS = Number(process.env.SEARCH_WINDOW_EXT_DAYS || 30)
const NOW_MIN_OFFSET_MIN = Number(process.env.NOW_MIN_OFFSET_MIN || 30)
const DRY_RUN = /^true$/i.test(process.env.DRY_RUN || "")

// ====== Square
const square = new Client({
  accessToken: process.env.SQUARE_ACCESS_TOKEN,
  environment: (process.env.SQUARE_ENV==="production") ? Environment.Production : Environment.Sandbox
})
const LOC_TORRE = (process.env.SQUARE_LOCATION_ID_TORREMOLINOS || "").trim()
const LOC_LUZ   = (process.env.SQUARE_LOCATION_ID_LA_LUZ || "").trim()
const ADDRESS_TORRE = process.env.ADDRESS_TORREMOLINOS || "Av. de Benyamina 18, Torremolinos"
const ADDRESS_LUZ   = process.env.ADDRESS_LA_LUZ || "Málaga – Barrio de La Luz"

// ====== Utils cortos
const onlyDigits = s => String(s||"").replace(/\D+/g,"")
const norm = s => String(s||"").normalize("NFD").replace(/\p{Diacritic}/gu,"").toLowerCase().replace(/[^\p{Letter}\p{Number}\s]/gu," ").replace(/\s+/g," ").trim()
function fmtES(d){ const t=dayjs(d).tz(EURO_TZ); const dias=["domingo","lunes","martes","miércoles","jueves","viernes","sábado"]; return `${dias[t.day()]} ${String(t.date()).padStart(2,"0")}/${String(t.month()+1).padStart(2,"0")} ${String(t.hour()).padStart(2,"0")}:${String(t.minute()).padStart(2,"0")}` }
function fmtDay(d){ const t=dayjs(d).tz(EURO_TZ); const dias=["domingo","lunes","martes","miércoles","jueves","viernes","sábado"]; return `${dias[t.day()]} ${String(t.date()).padStart(2,"0")}/${String(t.month()+1).padStart(2,"0")}` }
function fmtHour(d){ const t=dayjs(d).tz(EURO_TZ); return `${String(t.hour()).padStart(2,"0")}:${String(t.minute()).padStart(2,"0")}` }
function ceilToSlotEU(t){ const m=t.minute(), r=m%SLOT_MIN; return r===0? t.second(0).millisecond(0): t.add(SLOT_MIN-r,"minute").second(0).millisecond(0) }
function nextOpeningFrom(d){
  let t=d.clone()
  const nowMin=t.hour()*60+t.minute(), openMin=OPEN.start*60, closeMin=OPEN.end*60
  if (nowMin<openMin) t=t.hour(OPEN.start).minute(0)
  if (nowMin>=closeMin) t=t.add(1,"day").hour(OPEN.start).minute(0)
  while(!WORK_DAYS.includes(t.day())) t=t.add(1,"day").hour(OPEN.start).minute(0)
  return t
}
function locationNice(key){ return key==="la_luz" ? "Málaga – La Luz" : "Torremolinos" }
function locationToId(key){ return key==="la_luz" ? LOC_LUZ : LOC_TORRE }
function parseSalonText(text){
  const t = " " + norm(text) + " "
  if (/\b(luz|la luz)\b/.test(t)) return "la_luz"
  if (/\b(torre|torremolinos|playamar)\b/.test(t)) return "torremolinos"
  return null
}
function normalizePhoneES(raw){
  const d=onlyDigits(raw); if(!d) return null
  if (raw.startsWith("+") && d.length>=8 && d.length<=15) return `+${d}`
  if (d.startsWith("34") && d.length===11) return `+${d}`
  if (d.length===9) return `+34${d}`
  if (d.startsWith("00")) return `+${d.slice(2)}`
  return `+${d}`
}

// ====== DB mínima
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
CREATE UNIQUE INDEX IF NOT EXISTS idx_hold_unique ON holds(location_key,service_env_key,start_iso);
`)
function saveSession(phone, s){
  const j=JSON.stringify(s||{})
  const up=db.prepare(`UPDATE sessions SET data_json=@j,updated_at=@u WHERE phone=@p`).run({j,u:new Date().toISOString(),p:phone})
  if (up.changes===0) db.prepare(`INSERT INTO sessions (phone,data_json,updated_at) VALUES (@p,@j,@u)`).run({p:phone,j,u:new Date().toISOString()})
}
function loadSession(phone){
  const row=db.prepare(`SELECT data_json FROM sessions WHERE phone=@p`).get({p:phone})
  return row?.data_json? JSON.parse(row.data_json) : {}
}
function clearExpiredHolds(){ db.prepare(`DELETE FROM holds WHERE datetime(expires_at)<=datetime('now')`).run() }
function putHolds({phone,location_key,service_env_key,start_isos}){
  clearExpiredHolds()
  const stmt=db.prepare(`INSERT OR REPLACE INTO holds(phone,location_key,service_env_key,start_iso,expires_at,created_at)
    VALUES(@p,@l,@k,@s,datetime('now','+${HOLD_HOURS} hours'),datetime('now'))`)
  const tx=db.transaction(()=>{ for (const s of start_isos){ stmt.run({p:phone,l:location_key,k:service_env_key,s}) } })
  tx()
}
function isHeldByOther({phone,location_key,service_env_key,start_iso}){
  clearExpiredHolds()
  const row=db.prepare(`SELECT phone FROM holds WHERE location_key=@l AND service_env_key=@k AND start_iso=@s AND datetime(expires_at)>datetime('now')`).get({l:location_key,k:service_env_key,s:start_iso})
  return !!(row && row.phone!==phone)
}

// ====== Empleadas + Centros
function titleCase(str){ return String(str||"").toLowerCase().replace(/\b([a-záéíóúñ])/g, m=>m.toUpperCase()) }
function empKeyToNiceLabel(k){
  return titleCase(k.replace(/^SQ_EMP_/, "").replaceAll("_"," ").trim())
}
function parseCenterMap(){
  // EMP_CENTER_* = "la_luz", "torremolinos" o "la_luz,torremolinos"
  const map = new Map() // key: nombre canon (lower, con espacios), val: Set(["la_luz","torremolinos"])
  for (const [k,v] of Object.entries(process.env)){
    if (!k.startsWith("EMP_CENTER_")) continue
    const tail = k.replace("EMP_CENTER_","").replaceAll("_"," ").toLowerCase().trim()
    const centers = String(v||"").split(",").map(s=>s.trim().toLowerCase()).filter(Boolean)
    const set = new Set()
    for (const c of centers){
      if (c==="la_luz" || c==="la luz") set.add("la_luz")
      if (c==="torremolinos") set.add("torremolinos")
    }
    if (set.size===0){ set.add("la_luz"); set.add("torremolinos") }
    map.set(tail, set)
  }
  return map
}
const CENTER_MAP = parseCenterMap()

function parseEmployees(){
  // Leemos SQ_EMP_* para ID y “bookable”, y unimos centros desde EMP_CENTER_*
  const byId = new Map()
  for (const [k,v] of Object.entries(process.env)){
    if (!k.startsWith("SQ_EMP_")) continue
    const [id, tag] = String(v||"").split("|")
    if (!id) continue
    const bookable = !(String(tag||"").toUpperCase().includes("NO_BOOK"))
    const nice = empKeyToNiceLabel(k) // "Cristina", "Rocio Chica Rocio", etc.
    const labels = Array.from(new Set([nice, ...nice.split(" ")])).filter(Boolean)

    // Inferir centros por nombre (probamos combinaciones: todo, primeras 2 palabras, 1 palabra)
    const tail = nice.toLowerCase()
    const toks = tail.split(" ").filter(Boolean)
    let centers = new Set()
    const candidates = new Set([tail])
    if (toks.length>=2) candidates.add(`${toks[0]} ${toks[1]}`)
    if (toks.length>=1) candidates.add(toks[0])
    for (const cand of candidates){
      if (CENTER_MAP.has(cand)){
        for (const c of CENTER_MAP.get(cand)) centers.add(c)
      }
    }
    if (centers.size===0){ centers.add("la_luz"); centers.add("torremolinos") }

    if (!byId.has(id)){
      byId.set(id, { id, bookable, labels, centers })
    } else {
      const e = byId.get(id)
      e.bookable = e.bookable || bookable
      for (const l of labels) if (!e.labels.includes(l)) e.labels.push(l)
      for (const c of centers) e.centers.add(c)
    }
  }
  return Array.from(byId.values())
}
let EMPLOYEES = parseEmployees()
function staffLabelFromId(id){ return EMPLOYEES.find(e=>e.id===id)?.labels?.[0] || "Equipo" }
function isStaffAllowedInSede(id, sedeKey){
  const e = EMPLOYEES.find(x=>x.id===id)
  if (!e) return false
  return e.centers.has(sedeKey)
}

// Alias manuales extra (palabras cortas / apodos)
const ALIAS_EXTRA = {
  "patri":"patricia", "belen":"carmen belen", "carmen":"carmen belen",
  "cristi":"cristina", "cris":"cristina",
  "chabely":"chabeli", "chabeli":"chabeli",
  "rocio chica":"rocio chica", "rocio":"rocio",
  "gana":"ganna", "ana":"ganna", "anna":"ganna", // ojo: no casa “johana” al usar límites de palabra
  "gina":"ginna", "desi":"desi", "dani":"daniela",
  "maria":"maria", "sude":"sudemis", "tani":"tania"
}
function fuzzyStaff(text){
  const t=" "+norm(text)+" "
  if (/\bequipo|cualquiera|me da igual\b/.test(t)) return { any:true }
  let query=null
  const m = t.match(/\bcon\s+([a-zñáéíóú ]{2,})\b/i)
  if (m) query = norm(m[1])
  // Si no vino “con …”, buscamos nombres sueltos
  if (!query){
    const names = Object.keys(ALIAS_EXTRA).concat(
      EMPLOYEES.flatMap(e=>e.labels.map(l=>norm(l))).filter((v,i,a)=>a.indexOf(v)===i)
    )
    for (const n of names){
      const re = new RegExp(`\\b${n}\\b`,"i")
      if (re.test(t)){ query = n; break }
    }
  }
  if (!query) return null
  // Normalizar por alias
  const canon = ALIAS_EXTRA[query] || query
  // Buscar empleada por etiqueta
  for (const e of EMPLOYEES){
    const has = e.labels.some(l => norm(l)===canon)
    if (has) return { id:e.id, label:e.labels[0], centers:e.centers }
  }
  // Búsqueda por palabra exacta contenida en labels
  for (const e of EMPLOYEES){
    const has = e.labels.some(l => new RegExp(`(^|\\s)${canon}(\\s|$)`,"i").test(norm(l)))
    if (has) return { id:e.id, label:e.labels[0], centers:e.centers }
  }
  return null
}

// ====== Servicios (desde ENV)
function applySpanishDiacritics(label){
  let x = String(label||"")
  x = x.replace(/\bunas\b/gi, m => m[0]==='U'?'Uñas':'uñas')
  x = x.replace(/\bpestan(as?)?\b/gi, m => (m[0]==='P'?'Pestañ':'pestañ') + 'as')
  x = x.replace(/\bnivelacion\b/gi, m => m[0]==='N' ? 'Nivelación' : 'nivelación')
  x = x.replace(/\bfrances\b/gi, m => m[0]==='F' ? 'Francés' : 'francés')
  x = x.replace(/\bsemi ?permanente\b/gi, m => /[A-Z]/.test(m[0]) ? 'Semipermanente' : 'semipermanente')
  return x
}
function labelFromKey(k, sedeKey){
  let label = titleCase(k.replace(/^SQ_SVC(_luz)?_/, "").replaceAll("_"," "))
  label = applySpanishDiacritics(label)
  if (sedeKey==="la_luz") label = label.replace(/^Luz\s+/i,"").trim()
  return label
}
function servicesForSede(sedeKey){
  const prefix = (sedeKey==="la_luz") ? "SQ_SVC_luz_" : "SQ_SVC_"
  const out=[]
  for (const [k,v] of Object.entries(process.env)){
    if (!k.startsWith(prefix)) continue
    const [id, verRaw] = String(v||"").split("|")
    if (!id) continue
    const label = labelFromKey(k, sedeKey)
    out.push({ key:k, id, version: verRaw? Number(verRaw): null, label })
  }
  // dedupe por label
  const seen=new Set(), clean=[]
  for (const s of out){ const L=s.label.toLowerCase(); if (seen.has(L)) continue; seen.add(L); clean.push(s) }
  return clean
}
function resolveService(sedeKey, label){
  const list = servicesForSede(sedeKey)
  return list.find(s => s.label.toLowerCase() === String(label||"").toLowerCase()) || null
}
function guessCategory(text){
  const t=norm(text)
  if (/\b(uñ|manicur|pedicur|esmalt|acril|gel|tips|frances|pies|quitar)\b/.test(t)) return "unas"
  if (/\b(microblading|microshading|powder|aquarela|eyeliner|labios|micropigment)\b/.test(t)) return "micro"
  if (/\b(pestañ|pestanas|lifting|extensiones)\b/.test(t)) return "pestanas"
  if (/\b(depil|fotodepil|axilas|ingles|labio|nasales|pubis|piernas)\b/.test(t)) return "depilacion"
  if (/\b(facial|limpieza|dermapen|hydra|peel|carbon|vitamina|manchas|acne|colageno|jade)\b/.test(t)) return "faciales"
  return null
}
// Orden simple
const PRIORITY = [
  "Manicura Semipermanente",
  "Manicura Con Esmalte Normal",
  "Manicura Rusa Con Nivelación",
  "Quitar Uñas Esculpidas",
  "Pedicura Spa Con Esmalte Semipermanente",
  "Pedicura Spa Con Esmalte Normal",
  "Pedicura Glam Jelly Con Esmalte Semipermanente",
  "Pedicura Glam Jelly Con Esmalte Normal",
  "Lifitng De Pestañas Y Tinte",
  "Microblading",
  "Cejas Efecto Polvo Microshading",
]
function orderServicesSimple(list){
  const pos = new Map(PRIORITY.map((l,i)=>[l.toLowerCase(), i]))
  return [...list].sort((a,b)=>{
    const pa = pos.has(a.label.toLowerCase()) ? pos.get(a.label.toLowerCase()) : 999
    const pb = pos.has(b.label.toLowerCase()) ? pos.get(b.label.toLowerCase()) : 999
    return pa - pb || a.label.localeCompare(b.label)
  })
}
function filterByCategory(list, cat){
  if (!cat) return list
  const t=cat
  if (t==="unas") return list.filter(s=>/\b(uñas|manicura|pedicura|esculpidas|relleno|esmaltado|tips|franc[eé]s)\b/i.test(s.label))
  if (t==="micro") return list.filter(s=>/\b(microblading|microshading|aquarela|eyeliner|labios|hairstroke|powder)\b/i.test(s.label))
  if (t==="pestanas") return list.filter(s=>/\b(pestañ|pestanas|lifting|extensiones|relleno)\b/i.test(norm(s.label)))
  if (t==="depilacion") return list.filter(s=>/\b(depil|fotodepil|axilas|ingles|labio|nasales|piernas|pubis|perianal)\b/i.test(norm(s.label)))
  if (t==="faciales") return list.filter(s=>/\b(facial|limpieza|dermapen|hydra|peel|carbon|vitamina|manchas|acne|colageno|jade)\b/i.test(norm(s.label)))
  return list
}
// Alias directos → servicio
function aliasToServiceLabel(sedeKey, text){
  const t=norm(text)
  const alias = [
    [/quitar( me)? las unas|quitar unas|retirar unas/, "Quitar Uñas Esculpidas"],
    [/manicura semi|semi manos|manicura semipermanente/, "Manicura Semipermanente"],
    [/pedicura semi/, "Pedicura Spa Con Esmalte Semipermanente"],
    [/lifting.*pestan/, "Lifitng De Pestañas Y Tinte"],
    [/micro ?bland|micro ?blading/, "Microblading"],
    [/powder|microshading/, "Cejas Efecto Polvo Microshading"],
    [/aquarela|acuarela.*labios/, "Labios Efecto Aquarela"],
  ]
  for (const [re,label] of alias){
    if (re.test(t)){
      const r = resolveService(sedeKey, label)
      if (r) return r.label
    }
  }
  return null
}

// ====== Disponibilidad (Square)
async function getServiceIdAndVersion(envKey){
  const raw = process.env[envKey]; if (!raw) return null
  let [id, ver] = String(raw).split("|"); ver=ver?Number(ver):null
  if (!id) return null
  if (!ver){
    try{
      const resp=await square.catalogApi.retrieveCatalogObject(id,true)
      const vRaw = resp?.result?.object?.version
      ver = vRaw != null ? Number(vRaw) : 1
    }catch{ ver=1 }
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
  }catch{ return [] }
}
async function searchAvailWindowExtended({ locationKey, envServiceKey, startEU, staffId=null, maxDays=SEARCH_WINDOW_EXT_DAYS }){
  const results=[]
  const end = startEU.clone().add(maxDays,"day")
  let cur = startEU.clone()
  while (cur.isBefore(end) && results.length<1000){
    const to = dayjs.min(end, cur.clone().add(7,"day"))
    const chunk = await searchAvailWindow({ locationKey, envServiceKey, startEU:cur, endEU:to, limit:500 })
    results.push(...(staffId ? chunk.filter(s=>s.staffId===staffId): chunk))
    cur = to.clone()
    await new Promise(r=>setTimeout(r,60))
  }
  results.sort((a,b)=>a.date.valueOf()-b.date.valueOf())
  return results
}

// ====== Horas sencillas
function parseTemporalHint(text){
  const t=norm(text)
  let part=null; if (/\btarde\b/.test(t)) part="tarde"; if (/\bnoche\b/.test(t)) part="noche"; if (/\bmanana\b/.test(t)) part="mañana"
  let dayShift=0
  if (/\bhoy\b/.test(t)) dayShift=0
  else if (/\bmanana\b/.test(t)) dayShift=1
  else if (/\bpasado\b/.test(t)) dayShift=2
  const map = { "lunes":1,"martes":2,"miercoles":3,"miércoles":3,"jueves":4,"viernes":5 }
  for (const [k,v] of Object.entries(map)){ if (t.includes(k)){ // próximo k
      let d=nowEU(); while(d.day()!==v) d=d.add(1,"day"); return { target:d, part } } }
  return { shift:dayShift, part }
}
function applyHintToRange(baseStart, part, shift){
  let s = baseStart.clone().add(shift||0, "day")
  if (part==="mañana") s=s.hour(9).minute(0)
  if (part==="tarde")  s=s.hour(15).minute(0)
  if (part==="noche")  s=s.hour(18).minute(0)
  const e = s.clone().add(SEARCH_WINDOW_DAYS, "day").hour(OPEN.end).minute(0)
  return { startEU:s, endEU:e }
}
async function proposeSimple({ phone, sede, envServiceKey, staffId=null, hintText=null }){
  const openFrom = nextOpeningFrom(nowEU().add(NOW_MIN_OFFSET_MIN,"minute"))
  const hint = parseTemporalHint(hintText||"")
  const { startEU, endEU } = applyHintToRange(openFrom, hint.part, hint.shift||0)

  let slots = await searchAvailWindow({ locationKey:sede, envServiceKey, startEU, endEU, limit:500 })
  if (staffId) slots = slots.filter(s=>s.staffId===staffId)

  if (staffId && !slots.length){
    const ext = await searchAvailWindowExtended({ locationKey:sede, envServiceKey, startEU, staffId, maxDays:SEARCH_WINDOW_EXT_DAYS })
    slots = ext
  }
  // filtra holds
  slots = slots.filter(s=> !isHeldByOther({ phone, location_key:sede, service_env_key:envServiceKey, start_iso:s.date.tz("UTC").toISOString() }))
  const top = slots.slice(0, SHOW_TOP_N)
  putHolds({ phone, location_key:sede, service_env_key:envServiceKey, start_isos: top.map(s=>s.date.tz("UTC").toISOString()) })
  return top
}

// ====== Mini web
const app=express()
const PORT=process.env.PORT||8080
let lastQR=null, conectado=false
app.get("/", (_req,res)=>{
  res.send(`<!doctype html><meta charset="utf-8"><style>
  body{font-family:system-ui;display:grid;place-items:center;min-height:100vh;background:#f7f7f7}
  .card{max-width:740px;padding:28px;border-radius:18px;background:#fff;box-shadow:0 10px 40px rgba(0,0,0,.08)}
  .status{padding:10px 12px;border-radius:8px;margin:8px 0;font-weight:600}
  .ok{background:#e6ffed;color:#057a55}.bad{background:#ffe8e8;color:#b00020}
  </style><div class="card">
  <h1>Gapink Nails — Bot Básico</h1>
  <div class="status ${conectado?'ok':'bad'}">WhatsApp: ${conectado?'✅ Conectado':'❌ Desconectado'}</div>
  ${!conectado&&lastQR?`<img src="/qr.png" width="260" style="border-radius:10px;border:1px solid #eee">`:""}
  <p>Modo: ${DRY_RUN?'Simulación (no crea reservas)':'Consulta Square'}</p>
  </div>`)
})
app.get("/qr.png", async (_req,res)=>{
  if(!lastQR) return res.status(404).send("No QR")
  const png = await qrcode.toBuffer(lastQR, { type:"png", width:512, margin:1 })
  res.set("Content-Type","image/png").send(png)
})

// ====== WhatsApp (Baileys ESM)
async function loadBaileys(){
  let mod=null
  try{ mod=await import("@whiskeysockets/baileys") }catch{}
  if(!mod) throw new Error("Baileys no disponible")
  const makeWASocket = mod.makeWASocket || mod.default?.makeWASocket
  const useMultiFileAuthState = mod.useMultiFileAuthState || mod.default?.useMultiFileAuthState
  const fetchLatestBaileysVersion = mod.fetchLatestBaileysVersion || mod.default?.fetchLatestBaileysVersion || (async()=>({version:[2,3000,0]}))
  const Browsers = mod.Browsers || mod.default?.Browsers || { macOS:(n="Desktop")=>["MacOS",n,"121.0.0"] }
  return { makeWASocket, useMultiFileAuthState, fetchLatestBaileysVersion, Browsers }
}

// ====== Mensajes
function greeting(){
  return `Hola 👋 Soy Gapink Nails.
1) Salón: 1 Torremolinos · 2 La Luz
Dime el número. Luego te paso lista de servicios.
Puedes decir: "con Cristina" si quieres alguien.`
}
function askSalon(){ return `Salón:
1) Torremolinos
2) La Luz
Responde 1 o 2.` }
function askService(list, sede){
  const lines = list.slice(0,12).map((s,i)=>`${i+1}) ${s.label}`).join("\n")
  return `Servicios en ${locationNice(sede)}:
${lines}

Responde con el *número*.`
}
function askStaff(){ return `¿Con alguien?
• Escribe el nombre (ej. "Cristina") o pon "equipo".
Si te da igual, escribe "equipo".` }
function listTimes(txtHeader, slots){
  const lines = slots.map((s,i)=>`${i+1}) ${fmtDay(s.date)} ${fmtHour(s.date)}${s.staffId?` — ${staffLabelFromId(s.staffId)}`:""}`).join("\n")
  return `${txtHeader}
${lines}

Elige con el *número*.`
}
function finalHoldMsg({sede, label, pick, staff}){
  return `Hecho ✅
📍 ${locationNice(sede)}
🧾 ${label}
🕐 ${fmtES(pick)}${staff?`\n👩‍💼 ${staff}`:""}

Lo dejo *bloqueado 6h*. Si necesitas cambiar, dímelo aquí.`
}
function crossSalonChoice(name, centers, current){
  const arr = Array.from(centers||[])
  const nice = arr.map(c=>locationNice(c)).join(" y ")
  const first = arr[0] || "la_luz"
  return `${name} atiende en ${nice}.
¿Qué prefieres?
1) Cambiar a ${locationNice(first)}
2) Ver horas del *equipo* en ${locationNice(current)}`
}

// ====== Bucle principal
async function startBot(){
  try{
    const { makeWASocket, useMultiFileAuthState, fetchLatestBaileysVersion, Browsers } = await loadBaileys()
    if(!fs.existsSync("auth_info")) fs.mkdirSync("auth_info",{recursive:true})
    const { state, saveCreds } = await useMultiFileAuthState("auth_info")
    const { version } = await fetchLatestBaileysVersion().catch(()=>({version:[2,3000,0]}))
    const sock = makeWASocket({ logger:pino({level:"silent"}), auth:state, printQRInTerminal:false, version, browser:Browsers.macOS("Desktop"), syncFullHistory:false })
    globalThis.sock = sock

    sock.ev.on("connection.update", ({connection, qr})=>{
      if (qr){ lastQR=qr; conectado=false; try{ qrcodeTerminal.generate(qr,{small:true}) }catch{} }
      if (connection==="open"){ lastQR=null; conectado=true }
      if (connection==="close"){ conectado=false; setTimeout(()=>startBot().catch(console.error), 3000) }
    })
    sock.ev.on("creds.update", saveCreds)

    if (!globalThis.__q) globalThis.__q = new Map()

    sock.ev.on("messages.upsert", async ({messages})=>{
      const m=messages?.[0]; if (!m?.message) return
      const jid = m.key.remoteJid || ""
      const fromMe = !!m.key.fromMe
      if (fromMe) return
      if (jid.endsWith("@g.us")) return

      const phone = normalizePhoneES((jid||"").split("@")[0]||"") || (jid||"").split("@")[0]
      const textRaw = (m.message.conversation || m.message.extendedTextMessage?.text || m.message?.imageMessage?.caption || "").trim()
      if (!textRaw) return

      const QUEUE = globalThis.__q
      const prev = QUEUE.get(phone)||Promise.resolve()
      const job = prev.then(async ()=>{
        try{
          let s = loadSession(phone) || {}
          s.stage = s.stage || null
          s.sede = s.sede || null
          s.svcKey = s.svcKey || null
          s.svcLabel = s.svcLabel || null
          s.serviceChoices = s.serviceChoices || []
          s.lastSlots = s.lastSlots || []
          s.lastStaffByIso = s.lastStaffByIso || {}
          s.preferStaffId = s.preferStaffId || null
          s.preferStaffLabel = s.preferStaffLabel || null
          s.pendingCross = s.pendingCross || null // {id,label,centers}

          // Silencio para puntitos
          if (/^[\s.·•⋅]+$/.test(textRaw)){ s.snooze_until = nowEU().add(6,"hour").valueOf(); saveSession(phone,s); return }
          if (s.snooze_until && Date.now() < s.snooze_until){ saveSession(phone,s); return }

          // Saludo 24h
          if (!s.greetedAt || (Date.now()-s.greetedAt) > 24*60*60*1000){
            s.greetedAt = Date.now(); saveSession(phone,s)
            await sock.sendMessage(jid,{ text: greeting() })
          }

          const tNorm = norm(textRaw)
          const numPick = tNorm.match(/^\s*([1-9]\d*)\b/)

          // ===== Paso especial: elección por cruce de salón tras pedir “con X” fuera de su sede =====
          if (s.stage==="await_staff_loc_choice" && numPick && s.pendingCross){
            const n = Number(numPick[1])
            if (n===1){
              // cambiar a el primer salón permitido por esa profesional
              const arr = Array.from(s.pendingCross.centers||[])
              const newSede = arr[0] || "la_luz"
              s.sede = newSede
              s.preferStaffId = s.pendingCross.id
              s.preferStaffLabel = s.pendingCross.label
              s.stage = null; s.pendingCross=null; saveSession(phone,s)
              const hint = /\bhoy|manana|pasado|lunes|martes|miercoles|jueves|viernes|tarde|noche/.test(tNorm) ? textRaw : ""
              const slots = await proposeSimple({ phone, sede:s.sede, envServiceKey:s.svcKey, staffId:s.preferStaffId, hintText:hint })
              if (!slots.length){
                const team = await proposeSimple({ phone, sede:s.sede, envServiceKey:s.svcKey, staffId:null, hintText:hint })
                if (!team.length){ await sock.sendMessage(jid,{ text:"No veo huecos en ese rango. Di otra fecha o franja (ej. “viernes tarde”)." }); return }
                s.lastSlots = team.map(x=>x.date.toISOString())
                s.lastStaffByIso = Object.fromEntries(team.map(x=>[x.date.format("YYYY-MM-DDTHH:mm"), x.staffId||null]))
                s.stage="await_time"; saveSession(phone,s)
                await sock.sendMessage(jid,{ text: listTimes(`${s.svcLabel} en ${locationNice(s.sede)}\n(No había con ${s.preferStaffLabel}. Horas del equipo:)`, team) })
                return
              }
              s.lastSlots = slots.map(x=>x.date.toISOString())
              s.lastStaffByIso = Object.fromEntries(slots.map(x=>[x.date.format("YYYY-MM-DDTHH:mm"), x.staffId||null]))
              s.stage="await_time"; saveSession(phone,s)
              await sock.sendMessage(jid,{ text: listTimes(`${s.svcLabel} en ${locationNice(s.sede)}\nHoras con ${s.preferStaffLabel}:`, slots) })
              return
            } else if (n===2){
              // ver equipo en el salón actual
              s.preferStaffId = null; s.preferStaffLabel=null
              s.stage=null; s.pendingCross=null; saveSession(phone,s)
              const hint = /\bhoy|manana|pasado|lunes|martes|miercoles|jueves|viernes|tarde|noche/.test(tNorm) ? textRaw : ""
              const team = await proposeSimple({ phone, sede:s.sede, envServiceKey:s.svcKey, staffId:null, hintText:hint })
              if (!team.length){ await sock.sendMessage(jid,{ text:"No veo huecos en ese rango. Di otra fecha o franja (ej. “viernes tarde”)." }); return }
              s.lastSlots = team.map(x=>x.date.toISOString())
              s.lastStaffByIso = Object.fromEntries(team.map(x=>[x.date.format("YYYY-MM-DDTHH:mm"), x.staffId||null]))
              s.stage="await_time"; saveSession(phone,s)
              await sock.sendMessage(jid,{ text: listTimes(`${s.svcLabel} en ${locationNice(s.sede)}\nHoras del equipo:`, team) })
              return
            } else {
              await sock.sendMessage(jid,{ text:"Elige 1 o 2." })
              return
            }
          }

          // ===== Paso 1: salón (número o texto)
          const salonInline = parseSalonText(textRaw)
          if (!s.sede){
            if (numPick){
              const n=Number(numPick[1])
              if (n===1) s.sede="torremolinos"
              else if (n===2) s.sede="la_luz"
              if (!s.sede){
                await sock.sendMessage(jid,{ text: askSalon() }); saveSession(phone,s); return
              }
            } else if (salonInline){ s.sede=salonInline }
            else { await sock.sendMessage(jid,{ text: askSalon() }); saveSession(phone,s); return }
            saveSession(phone,s)
          }

          // ===== Paso 2: servicio (alias directos → saltar lista)
          if (!s.svcKey){
            const alias = aliasToServiceLabel(s.sede, textRaw)
            if (alias){
              const r = resolveService(s.sede, alias)
              if (r){ s.svcKey=r.key; s.svcLabel=r.label; saveSession(phone,s) }
            }
          }
          if (!s.svcKey){
            if (s.stage==="await_service" && numPick){
              const n=Number(numPick[1])
              const ch = s.serviceChoices.find(x=>x.index===n)
              if (ch){ s.svcKey=ch.key; s.svcLabel=ch.label; s.stage=null; saveSession(phone,s) }
            }
          }
          if (!s.svcKey){
            const all = orderServicesSimple(servicesForSede(s.sede))
            const cat = guessCategory(textRaw)
            const filtered = orderServicesSimple(filterByCategory(all, cat)).slice(0, 12)
            const list = filtered.map((s,i)=>({ index:i+1, key:s.key, label:s.label }))
            s.serviceChoices = list; s.stage="await_service"; saveSession(phone,s)
            await sock.sendMessage(jid,{ text: askService(list, s.sede) })
            return
          }

          // ===== Paso 3: profesional (opcional)
          let preferStaff = s.preferStaffId || null
          const staffReq = fuzzyStaff(textRaw)
          if (staffReq){
            if (staffReq.any){ preferStaff = null; s.preferStaffId=null; s.preferStaffLabel=null }
            else {
              // Comprobar centros
              const allowed = staffReq.centers || new Set(["la_luz","torremolinos"])
              if (!allowed.has(s.sede)){
                // Ofrecer cambio de salón o equipo aquí
                s.pendingCross = { id: staffReq.id, label: staffReq.label, centers: allowed }
                s.stage = "await_staff_loc_choice"; saveSession(phone,s)
                await sock.sendMessage(jid,{ text: crossSalonChoice(staffReq.label, allowed, s.sede) })
                return
              }
              preferStaff = staffReq.id; s.preferStaffId=staffReq.id; s.preferStaffLabel=staffReq.label
            }
            saveSession(phone,s)
          }

          // ===== Paso 4: proponer horas
          // Si ya ofrecimos horas y ahora nos mandan un número → “reserva” (hold)
          if (s.stage==="await_time" && numPick && Array.isArray(s.lastSlots) && s.lastSlots.length){
            const n=Number(numPick[1])
            const pickISO = s.lastSlots[n-1]
            if (!pickISO){ await sock.sendMessage(jid,{ text:"Elige un número de la lista." }); return }
            const staffId = s.lastStaffByIso[dayjs(pickISO).format("YYYY-MM-DDTHH:mm")] || s.preferStaffId || null
            await sock.sendMessage(jid,{ text: finalHoldMsg({ sede:s.sede, label:s.svcLabel, pick:dayjs(pickISO), staff: staffId? staffLabelFromId(staffId): null }) })
            s.stage=null; saveSession(phone,s); return
          }

          // Si piden “viernes tarde / mañana / hoy / mañana” se usa como hint
          const hint = /\bhoy\b|\bmanana\b|\bpasado\b|\blunes\b|\bmartes\b|\bmiercoles\b|\bjueves\b|\bviernes\b|\btarde\b|\bmanana\b|\bnoche\b/i.test(tNorm) ? textRaw : ""

          // Primero con profesional (si la pidieron)
          let slots = await proposeSimple({ phone, sede:s.sede, envServiceKey:s.svcKey, staffId:preferStaff, hintText:hint })
          let header = s.preferStaffLabel ? `Horas con ${s.preferStaffLabel}:` : `Horas del equipo:`

          // Si pidieron con X y no hay, enseñar equipo
          if (s.preferStaffId && slots.length===0){
            slots = await proposeSimple({ phone, sede:s.sede, envServiceKey:s.svcKey, staffId:null, hintText:hint })
            header = `No hay con ${s.preferStaffLabel} en ${SEARCH_WINDOW_EXT_DAYS} días. Horas del equipo:`
          }

          if (!slots.length){
            await sock.sendMessage(jid,{ text:"No veo huecos en ese rango. Di otra fecha o franja (ej. “viernes tarde”)." })
            return
          }

          s.lastSlots = slots.map(s=>s.date.toISOString())
          s.lastStaffByIso = Object.fromEntries(slots.map(s=>[s.date.format("YYYY-MM-DDTHH:mm"), s.staffId||null]))
          s.stage="await_time"; saveSession(phone,s)
          await sock.sendMessage(jid,{ text: listTimes(`${s.svcLabel} en ${locationNice(s.sede)}\n${header}`, slots) })

        }catch(err){
          await sock.sendMessage(jid,{ text:"No te he entendido. Dímelo con números. Ejemplo: 1 (salón), luego 3 (servicio)." })
        }
      })
      QUEUE.set(phone, job.finally(()=>{ if (QUEUE.get(phone)===job) QUEUE.delete(phone) }))
    })
  }catch(e){
    setTimeout(()=>startBot().catch(console.error), 3000)
  }
}

// ====== Arranque
const appListen = app.listen(PORT, ()=>{
  console.log(`🩷 Gapink Nails · Bot Básico v38.2.0 — http://localhost:${PORT}`)
  startBot().catch(console.error)
})
process.on("uncaughtException", e=>{ console.error("💥 uncaughtException:", e?.stack||e) })
process.on("unhandledRejection", e=>{ console.error("💥 unhandledRejection:", e) })
process.on("SIGTERM", ()=>{ try{ appListen.close(()=>process.exit(0)) }catch{ process.exit(0) } })
process.on("SIGINT",  ()=>{ try{ appListen.close(()=>process.exit(0)) }catch{ process.exit(0) } })
