// index.js — Gapink Nails · v34.3.0
// DeepSeek-only · IA en TODO con mejor comprensión + shortlist inteligente
// Prefiltro semántico (fuzzy+sinónimos) -> IA (top 6) -> elección natural de hora (sin números)
// Holds SQLite 6h por duración (ENV SQ_DUR_*). Square solo para consultar availability.
// Mini web QR. Baileys ESM por dynamic import.

// ===== Imports
import express from "express"
import pino from "pino"
import qrcode from "qrcode"
import qrcodeTerminal from "qrcode-terminal"
import "dotenv/config"
import dayjs from "dayjs"
import utc from "dayjs/plugin/utc.js"
import tz from "dayjs/plugin/timezone.js"
import isoWeek from "dayjs/plugin/isoWeek.js"
import "dayjs/locale/es.js"
import { webcrypto } from "crypto"
import Database from "better-sqlite3"
import { Client, Environment } from "square"

if (!globalThis.crypto) globalThis.crypto = webcrypto
dayjs.extend(utc); dayjs.extend(tz); dayjs.extend(isoWeek); dayjs.locale("es")
const EURO_TZ = "Europe/Madrid"
const nowEU = () => dayjs().tz(EURO_TZ)

// ===== Config
const OPEN = { start: 9, end: 20 }               // L–V 09:00–20:00
const WORK_DAYS = [1,2,3,4,5]
const SEARCH_WINDOW_DAYS = Number(process.env.BOT_SEARCH_WINDOW_DAYS || 30)
const NOW_MIN_OFFSET_MIN = Number(process.env.BOT_NOW_OFFSET_MIN || 30)
const SHOW_TOP_N = Number(process.env.SHOW_TOP_N || 5)
const HOLD_HOURS = Number(process.env.HOLD_HOURS || 6)
const PORT = Number(process.env.PORT || 8080)
const BOT_DEBUG = /^true$/i.test(process.env.BOT_DEBUG || "")

// ===== Square (solo consultar)
const square = new Client({
  accessToken: process.env.SQUARE_ACCESS_TOKEN,
  environment: (process.env.SQUARE_ENV==="production") ? Environment.Production : Environment.Sandbox
})
const LOC_TORRE = (process.env.SQUARE_LOCATION_ID_TORREMOLINOS || "").trim()
const LOC_LUZ   = (process.env.SQUARE_LOCATION_ID_LA_LUZ || "").trim()

// ===== IA (DeepSeek only; prompts compactos para ahorrar tokens)
const DEEPSEEK_API_KEY = process.env.DEEPSEEK_API_KEY || ""
const DEEPSEEK_MODEL   = process.env.DEEPSEEK_MODEL || "deepseek-chat"
const AI_MAX_TOKENS    = Number(process.env.AI_MAX_TOKENS || 160)
const AI_TIMEOUT_MS    = Number(process.env.AI_TIMEOUT_MS || 10000)

async function aiChat(system, user){
  if(!DEEPSEEK_API_KEY) return null
  const controller = new AbortController()
  const t = setTimeout(()=>controller.abort(), AI_TIMEOUT_MS)
  try{
    const resp = await fetch("https://api.deepseek.com/chat/completions", {
      method:"POST",
      headers:{ "Content-Type":"application/json", "Authorization":`Bearer ${DEEPSEEK_API_KEY}` },
      body: JSON.stringify({
        model: DEEPSEEK_MODEL,
        temperature: 0.15,
        max_tokens: AI_MAX_TOKENS,
        messages: [
          { role:"system", content: system },
          { role:"user", content: user }
        ]
      }),
      signal: controller.signal
    })
    clearTimeout(t)
    if(!resp.ok) return null
    const data = await resp.json()
    return data?.choices?.[0]?.message?.content || null
  }catch{ clearTimeout(t); return null }
}
function stripJSON(s){
  if(!s) return null
  s = s.trim().replace(/```json/gi,"```")
  if(s.startsWith("```")) s=s.slice(3)
  if(s.endsWith("```")) s=s.slice(0,-3)
  const i=s.indexOf("{"), j=s.lastIndexOf("}")
  if(i>=0 && j>i) s=s.slice(i,j+1)
  try{ return JSON.parse(s) }catch{ return null }
}

// ===== Utils
const rm = s => String(s||"").normalize("NFD").replace(/\p{Diacritic}/gu,"")
const norm = s => rm(s).toLowerCase().replace(/[+.,;:()/_-]/g," ").replace(/[^\p{Letter}\p{Number}\s]/gu," ").replace(/\s+/g," ").trim()
function fmtES(d){ const dias=["domingo","lunes","martes","miércoles","jueves","viernes","sábado"]; const t=dayjs(d).tz(EURO_TZ); return `${dias[t.day()]} ${String(t.date()).padStart(2,"0")}/${String(t.month()+1).padStart(2,"0")} ${String(t.hour()).padStart(2,"0")}:${String(t.minute()).padStart(2,"0")}` }
function insideBusinessHours(d,mins=60){
  const t=dayjs(d); if(!WORK_DAYS.includes(t.day())) return false
  const end=t.add(mins,"minute"); if(!t.isSame(end,"day")) return false
  const s=t.hour()*60+t.minute(), e=end.hour()*60+end.minute()
  return s >= OPEN.start*60 && e <= OPEN.end*60
}
function nextOpeningFrom(d){
  let t=d.clone()
  const nowMin=t.hour()*60+t.minute(), openMin=OPEN.start*60, closeMin=OPEN.end*60
  if(nowMin<openMin) t=t.hour(OPEN.start).minute(0)
  if(nowMin>=closeMin) t=t.add(1,"day").hour(OPEN.start).minute(0)
  while(!WORK_DAYS.includes(t.day())) t=t.add(1,"day").hour(OPEN.start).minute(0)
  return t
}
function locationToId(key){ return key==="la_luz" ? LOC_LUZ : LOC_TORRE }
function locationNice(key){ return key==="la_luz" ? "Málaga – La Luz" : "Torremolinos" }

// ===== Staff (opcional)
function parseEmployees(){
  const out=[]
  for(const [k,v] of Object.entries(process.env)){
    if(!k.startsWith("SQ_EMP_")) continue
    const [id, tag] = String(v||"").split("|").map(x=>x?.trim())
    if(!id) continue
    const label = k.replace(/^SQ_EMP_/,"").replace(/_/g," ").toLowerCase()
    out.push({ id, label, bookable: (tag||"BOOKABLE").toUpperCase()!=="OFF" })
  }
  return out
}
const EMPLOYEES = parseEmployees()
function staffLabelFromId(id){ return EMPLOYEES.find(e=>e.id===id)?.label?.split(" ")[0] || "equipo" }

// ===== Servicios por sede
function servicesForSede(sedeKey){
  const prefix = (sedeKey==="la_luz") ? "SQ_SVC_luz_" : "SQ_SVC_"
  const out=[]
  const seen = new Set()
  for(const [k,v] of Object.entries(process.env)){
    if(!k.startsWith(prefix)) continue
    const [id,ver] = String(v||"").split("|")
    if(!id) continue
    const label = k.replace(prefix,"").replace(/_/g," ").replace(/\b\w/g, m=>m.toUpperCase()).replace(/\bLuz\b/i,"").trim()
    const key = `${label}::${id}`
    if(seen.has(key)) continue
    seen.add(key)
    out.push({ key:k, id, version: ver?Number(ver):null, label, norm: norm(label) })
  }
  return out
}
function listLabels(sede){ return servicesForSede(sede).map(s=>s.label) }
function labelToEnvKey(label, sede){
  const hit = servicesForSede(sede).find(s=> s.label.toLowerCase()===String(label||"").toLowerCase())
  return hit?.key || null
}
function labelFromEnvKey(key){
  for(const sede of ["torremolinos","la_luz"]){
    const hit = servicesForSede(sede).find(s=>s.key===key)
    if(hit) return hit.label
  }
  return null
}
async function getServiceIdAndVersion(envKey){
  const raw = process.env[envKey]; if(!raw) return null
  let [id,ver] = String(raw).split("|"); ver=ver?Number(ver):null
  if(!ver){
    try{
      const resp = await square.catalogApi.retrieveCatalogObject(id,true)
      ver = Number(resp?.result?.object?.version || 1)
    }catch{ ver=1 }
  }
  return { id, version: ver }
}
// Duración desde ENV: SQ_SVC_* -> SQ_DUR_* (mismo sufijo)
function durationMinForEnvKey(envKey){
  if(!envKey) return 60
  const durKey = String(envKey).replace(/^SQ_SVC/, "SQ_DUR")
  const v = Number(process.env[durKey] || "")
  return Number.isFinite(v) && v>0 ? v : 60
}

// ===== SQLite: holds (bloqueo 6h)
const db = new Database("gapink_simple.db")
db.pragma("journal_mode = WAL")
db.exec(`
CREATE TABLE IF NOT EXISTS holds (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  phone TEXT,
  sede TEXT,
  env_service_key TEXT,
  start_iso TEXT,
  end_iso TEXT,
  staff_id TEXT,
  created_at TEXT,
  expires_at TEXT,
  status TEXT DEFAULT 'active'
);
CREATE INDEX IF NOT EXISTS idx_holds_lookup ON holds (sede, start_iso, end_iso, status, expires_at);
`)
const insertHold = db.prepare(`INSERT INTO holds (phone,sede,env_service_key,start_iso,end_iso,staff_id,created_at,expires_at,status)
VALUES (@phone,@sede,@env_service_key,@start_iso,@end_iso,@staff_id,@created_at,@expires_at,'active')`)
const findOverlappingHolds = db.prepare(`
SELECT id FROM holds
WHERE status='active'
  AND sede=@sede
  AND expires_at > @now
  AND NOT( end_iso <= @start_iso OR start_iso >= @end_iso )
`)
const expireOldHolds = db.prepare(`UPDATE holds SET status='expired' WHERE status='active' AND expires_at<=@now`)
function cleanupHolds(){ try{ expireOldHolds.run({ now: new Date().toISOString() }) }catch(e){ if(BOT_DEBUG) console.error(e) } }
function hasActiveOverlap({ sede, startISO, endISO }){
  try{
    const row = findOverlappingHolds.get({ sede, start_iso:startISO, end_iso:endISO, now: new Date().toISOString() })
    return !!row
  }catch{ return false }
}
function createHold({ phone, sede, envServiceKey, startISO, endISO, staffId }){
  try{
    const exp = dayjs().add(HOLD_HOURS,"hour").toISOString()
    insertHold.run({
      phone, sede, env_service_key: envServiceKey,
      start_iso:startISO, end_iso:endISO,
      staff_id: staffId || null,
      created_at: new Date().toISOString(),
      expires_at: exp
    })
    return true
  }catch(e){ if(BOT_DEBUG) console.error(e); return false }
}

// ===== Semántica low-cost (mejor comprensión)
// Stopwords y sinónimos base
const STOP = new Set("de del la el con para y o a en una un al por las los y".split(" "))
const SYN = {
  "cejas":["ceja","caza","henna","henna cejas","diseno","diseño","laminacion","laminación","hilo","microblading","microshading","efecto polvo","hairstroke","retoque"],
  "pestanas":["pestañas","pestanas","lifting","extensiones","2d","3d","pelo a pelo","relleno"],
  "unas":["uña","uñas","unias","manicura","semipermanente","gel","acrilico","acrílico","esculpidas","relleno","tips","frances","francés","nivelacion","nivelación","rusa"],
  "depilacion":["depilación","depilacion","laser","láser","ingles","inglés","axilas","labio","fosas nasales","pubis","perianal","brazos","piernas"],
  "faciales":["facial","limpieza","hydra","hidra","dermapen","carbon peel","carbón","manchas","acne","acné","vitamina c","oro","jade","hialuronico","hialurónico"],
  "labios":["labio","lips","aquarela","hydra lips"]
}
// Tokenizar servicio -> set de tokens + sinónimos inferidos
function tokenize(str){
  return norm(str).split(" ").filter(w=>w && !STOP.has(w))
}
function buildServiceIndex(){
  const index = { torremolinos:[], la_luz:[] }
  for (const sede of ["torremolinos","la_luz"]){
    const svcs = servicesForSede(sede)
    for (const s of svcs){
      const toks = new Set(tokenize(s.label))
      // añadir sinónimos por categoría implícita
      if([...toks].some(t=>["cejas","ceja","hairstroke","microblading","microshading","henna","laminacion","laminación","hilo"].includes(t))){
        SYN["cejas"].forEach(x=>tokenize(x).forEach(t=>toks.add(t)))
      }
      if([...toks].some(t=>["pestañas","pestanas","lifting","extensiones"].includes(t))){
        SYN["pestanas"].forEach(x=>tokenize(x).forEach(t=>toks.add(t)))
      }
      if([...toks].some(t=>["uña","uñas","manicura","esculpidas","semipermanente","rusa"].includes(t))){
        SYN["unas"].forEach(x=>tokenize(x).forEach(t=>toks.add(t)))
      }
      if([...toks].some(t=>["depilacion","depilación","laser","axilas","labio","ingles","inglés"].includes(t))){
        SYN["depilacion"].forEach(x=>tokenize(x).forEach(t=>toks.add(t)))
      }
      if([...toks].some(t=>["facial","limpieza","hydra","dermapen","carbon","manchas","acne"].includes(t))){
        SYN["faciales"].forEach(x=>tokenize(x).forEach(t=>toks.add(t)))
      }
      index[sede].push({ key:s.key, label:s.label, tokens:toks, norm:s.norm })
    }
  }
  return index
}
const SVC_INDEX = buildServiceIndex()

function scoreServiceByText(svc, textTokens, catHint){
  // Jaccard + boosts por sinónimos de la categoría
  const svcTokens = svc.tokens
  const inter = textTokens.filter(t=>svcTokens.has(t))
  const unionSize = new Set([...textTokens, ...svcTokens]).size || 1
  let s = inter.length / unionSize
  // boost si el nombre contiene bigramas exactos del texto
  const txt = " " + textTokens.join(" ") + " "
  const name = " " + svc.norm + " "
  if (textTokens.length >= 2){
    for (let i=0;i<textTokens.length-1;i++){
      const bg = ` ${textTokens[i]} ${textTokens[i+1]} `
      if (name.includes(bg)) s += 0.15
    }
  }
  // boost categoría
  if (catHint){
    const map = { cejas:"cejas", pestañas:"pestanas", "pestañas":"pestanas", uñas:"unas", "depilación":"depilacion", faciales:"faciales" }
    const k = map[catHint] || catHint
    const syn = (SYN[k]||[]).flatMap(w=>tokenize(w))
    if (syn.some(t=>svcTokens.has(t))) s += 0.2
  }
  // pequeña penalización por servicios muy largos si el texto es corto
  if (svc.norm.split(" ").length>=5 && textTokens.length<=2) s -= 0.05
  return s
}
function prefilterCandidates(text, sede, catHint=null, max=15){
  const tTokens = tokenize(text)
  const pool = (SVC_INDEX[sede]||[])
  const scored = pool.map(svc => ({ svc, score: scoreServiceByText(svc, tTokens, catHint) }))
  scored.sort((a,b)=>b.score-a.score)
  return scored.filter(x=>x.score>0).slice(0,max).map(x=>x.svc.label)
}

// ===== IA — router (mejorado con ejemplos y compactado)
function aiRouterSystem(){
  return `JSON solo.
{"intent":"book|view|edit|cancel|info|other",
 "sede":"torremolinos|la_luz|null",
 "category_hint":"uñas|pestañas|cejas|depilación|faciales|null",
 "staff":"string|null",
 "date_hint":"string|null",
 "part_of_day":"mañana|tarde|noche|null",
 "time_hint":"HH:MM|null"}
Ejemplos:
"para cejas con hilo en la luz" -> {"intent":"book","sede":"la_luz","category_hint":"cejas"}
"quiero cambiar mi cita" -> {"intent":"edit"}
"viernes tarde" -> {"intent":"book","sede":null,"part_of_day":"tarde"}
`
}
async function aiResolve(text){
  const sys = aiRouterSystem()
  const out = await aiChat(sys, `Cliente: "${text}"`)
  return stripJSON(out) || {}
}

// ===== IA — shortlist de servicios (top 3–6) con prefiltro semántico
function aiShortlistSystem(sedeLabels){
  return `Toma de esta lista (máx 6) las etiquetas exactas que mejor encajan. Devuelve JSON: {"labels":["..."]}. No expliques.
Lista: ${sedeLabels.join(" | ")}`
}
async function aiShortlist(text, sede, catHint){
  // Prefiltro local (ahorra tokens)
  const pre = prefilterCandidates(text, sede, catHint, 15)
  const labels = pre.length ? pre : listLabels(sede).slice(0,20) // guardarraíl
  const sys = aiShortlistSystem(labels)
  const out = await aiChat(sys, `Texto: "${text}"`)
  try{
    const obj = stripJSON(out) || {}
    const arr = Array.isArray(obj.labels) ? obj.labels.filter(l=>labels.some(x=>x.toLowerCase()===String(l).toLowerCase())) : []
    if(arr.length) return arr.slice(0,6)
    // fallback local si la IA no devuelve nada
    return pre.slice(0,5)
  }catch{ return pre.slice(0,5) }
}

// ===== IA — elegir 1 servicio de shortlist segun frase (con alias)
async function aiChooseFromShortlist(text, shortlist){
  const sys = `Shortlist: ${shortlist.join(" | ")}
JSON solo: {"pick":"uno de la shortlist o null"}
Alias comunes: hilo->"Depilación cejas con hilo"; micro->"Microblading" o "Cejas efecto polvo microshading"; laminación->"Laminación y diseño de cejas".`
  const out = await aiChat(sys, `Frase: "${text}"`)
  try{
    const obj = stripJSON(out) || {}
    const pick = shortlist.find(l=>l.toLowerCase()===String(obj.pick||"").toLowerCase())
    // Fallback por alias directos
    if(!pick){
      const t=norm(text)
      const alias = [
        { kw:/\bhilo\b/, name: shortlist.find(x=>/hilo/i.test(x)) },
        { kw:/\bmicro(shading|blading)?\b/, name: shortlist.find(x=>/micro|efecto polvo/i.test(x)) },
        { kw:/\blaminaci[oó]n\b/, name: shortlist.find(x=>/laminaci/i.test(x)) },
        { kw:/\bhenna\b/, name: shortlist.find(x=>/henna/i.test(x)) }
      ]
      const hit = alias.find(a=>a.name && a.kw.test(t))
      if(hit?.name) return hit.name
    }
    return pick || null
  }catch{ return null }
}

// ===== IA — elegir un hueco de la lista sin números (más ejemplos)
function aiPickSystem(slots){
  const comp = slots.map(s=>{
    const d=s.date.tz(EURO_TZ)
    return { iso:d.format("YYYY-MM-DDTHH:mm"), dow:["do","lu","ma","mi","ju","vi","sa"][d.day()], ddmm:`${String(d.date()).padStart(2,"0")}/${String(d.month()+1).padStart(2,"0")}`, time:`${String(d.hour()).padStart(2,"0")}:${String(d.minute()).padStart(2,"0")}` }
  })
  return `JSON solo. Elige "iso" de slots que case con la frase.
Frases soporte: "la del martes", "la de las 13", "la primera", "la última", "otra tarde", "la de las 1".
slots=${JSON.stringify(comp)}
Schema: {"iso":"YYYY-MM-DDTHH:mm|null"}`
}
async function aiPick(text, slots){
  if(!slots?.length) return { iso:null }
  const sys = aiPickSystem(slots)
  const out = await aiChat(sys, `Frase: "${text}"`)
  return stripJSON(out) || { iso:null }
}

// ===== Disponibilidad (respeta duración + bloqueos)
async function searchAvail({ sede, envKey, startEU, endEU, part=null }){
  const sv = await getServiceIdAndVersion(envKey); if(!sv) return []
  const durMin = durationMinForEnvKey(envKey)
  const body = {
    query:{ filter:{
      startAtRange:{ startAt: startEU.tz("UTC").toISOString(), endAt: endEU.tz("UTC").toISOString() },
      locationId: locationToId(sede),
      segmentFilters:[{ serviceVariationId: sv.id }]
    } }
  }
  let avail=[]
  try{ const r = await square.bookingsApi.searchAvailability(body); avail=r?.result?.availabilities||[] }catch(e){ if(BOT_DEBUG) console.error(e) }
  cleanupHolds()
  const out=[]
  for(const a of avail){
    const d=dayjs(a.startAt).tz(EURO_TZ)
    const end = d.clone().add(durMin,"minute")
    if(!insideBusinessHours(d,durMin)) continue
    if(part){
      const from = d.clone().hour(part==="mañana"?9:part==="tarde"?15:18).minute(0)
      const to   = d.clone().hour(part==="mañana"?13:part==="tarde"?20:20).minute(0)
      if(!(d.isAfter(from.subtract(1,"minute")) && d.isBefore(to.add(1,"minute")))) continue
    }
    const startISO = d.tz("UTC").toISOString()
    const endISO   = end.tz("UTC").toISOString()
    if (hasActiveOverlap({ sede, startISO, endISO })) continue

    const segs = a.appointmentSegments||a.segments||[]
    out.push({ date:d, end, staffId: segs?.[0]?.teamMemberId || null, durMin })
    if(out.length>=500) break
  }
  return out.sort((x,y)=>x.date.valueOf()-y.date.valueOf())
}

// ===== Mensajes cortos
const GREET = `¡Hola! Soy el asistente de Gapink Nails 💅
Dime *salón* (Torremolinos/La Luz) y qué quieres (ej. “cejas con hilo”).`
const BOOKING_SELF = "Para *ver/editar/cancelar* usa el enlace del SMS/email de confirmación ✅"

// ===== Estado en memoria (simple)
const SESS = new Map() // phone -> session
function getS(phone){
  return SESS.get(phone) || SESS.set(phone,{
    greetedAt:0,
    lastPromptType:null,
    sede:null,
    pendingCat:null,       // <-- NUEVO: guardamos pista de categoría hasta tener sede
    svcKey:null, svcLabel:null,
    shortlist:[],
    prefStaffId:null, prefStaffLabel:null,
    lastSlots:[], lastMap:{}, prompted:false, lastListAt:0,
    pickedISO:null, pickedDurMin:60,
    snoozeUntil:0
  }).get(phone)
}
function setPrompt(s,type){ s.lastPromptType=type; return s }

// ===== Formateo de horas
const formatSlots = (slots, showNames=false) =>
  slots.map(s=>`• ${fmtES(s.date)}${showNames && s.staffId?` — ${staffLabelFromId(s.staffId)}`:""}`).join("\n")

// ===== Proponer horas (solo si HAY servicio)
async function proposeOnce(session, jid, phone, sock, {text,date_hint,part}){
  if(!session.svcKey){
    const labels = listLabels(session.sede)
    await sock.sendMessage(jid,{text:`¿Qué *servicio* quieres en ${locationNice(session.sede)}?\n(${labels.length} disponibles) Dímelo en tus palabras (ej. “cejas con hilo”).`})
    return
  }
  const now = nowEU()
  const base = nextOpeningFrom(now.add(NOW_MIN_OFFSET_MIN,"minute"))
  let start = base.clone(), end = base.clone().add(SEARCH_WINDOW_DAYS,"day")
  const T = norm([text||"",date_hint||""].join(" "))

  const findDay = k=>{
    const map={lunes:1,martes:2,miercoles:3,miércoles:3,jueves:4,viernes:5}
    if(!map[k]) return null; let d=base.clone(); while(d.day()!==map[k]) d=d.add(1,"day"); return d
  }
  if(/\bhoy\b/.test(T)) start=now.clone().hour(OPEN.start).minute(0), end=now.clone().hour(OPEN.end).minute(0)
  else if(/\bmanana\b/.test(T)) { const d=now.clone().add(1,"day"); start=d.hour(OPEN.start).minute(0); end=d.hour(OPEN.end).minute(0) }
  else for(const k of ["lunes","martes","miercoles","miércoles","jueves","viernes"]){ if(T.includes(k)){ const d=findDay(k); start=d.hour(OPEN.start).minute(0); end=d.hour(OPEN.end).minute(0); break } }

  const raw = await searchAvail({ sede:session.sede, envKey:session.svcKey, startEU:start, endEU:end, part })
  let slots = raw
  if(session.prefStaffId){
    const f=raw.filter(x=>x.staffId===session.prefStaffId)
    if(f.length) slots=f
  }
  if(!slots.length){ await sock.sendMessage(jid,{text:"No veo huecos en ese rango. Dime otra fecha/franja (p. ej. “viernes tarde”)."}); return }

  const shown = slots.slice(0, SHOW_TOP_N)
  session.lastSlots = shown
  session.lastMap = Object.fromEntries(shown.map(s=>[s.date.format("YYYY-MM-DDTHH:mm"), s.staffId||null]))
  session.prompted = true
  session.lastListAt = Date.now()

  const header = session.prefStaffLabel ? `Huecos con ${session.prefStaffLabel}:` : `Huecos del equipo:`
  await sock.sendMessage(jid,{text: `${header}\n${formatSlots(shown, !!session.prefStaffLabel)}\n\nElige con texto: “la del martes”, “la de las 13”, “otra tarde”…`})
}

// ===== Mini web QR
const app = express()
let lastQR = null
let conectado = false

app.get("/", (_req,res)=>{
  res.send(`<!doctype html><meta charset="utf-8"><style>
  :root{color-scheme:light dark}
  body{font-family:system-ui,-apple-system,Segoe UI,Roboto,Inter,Ubuntu;display:grid;place-items:center;min-height:100vh;margin:0;background:Canvas}
  .card{max-width:760px;width:92vw;padding:28px 24px;border-radius:16px;box-shadow:0 10px 30px rgba(0,0,0,.12);background:Canvas;}
  h1{margin:0 0 4px;font-size:24px}
  .row{display:flex;gap:8px;align-items:center;margin:8px 0}
  .pill{padding:6px 10px;border-radius:999px;font-weight:600;font-size:12px}
  .ok{background:#d1fae5;color:#065f46}
  .bad{background:#fee2e2;color:#991b1b}
  .warn{background:#fef3c7;color:#92400e}
  .qr{margin:16px 0;text-align:center}
  footer{opacity:.6;font-size:12px;margin-top:12px}
  </style>
  <div class="card">
    <h1>Gapink Nails · Bot</h1>
    <div class="row">
      <span class="pill ${conectado?'ok':'bad'}">${conectado?'✅ WhatsApp conectado':'❌ WhatsApp desconectado'}</span>
      <span class="pill warn">Modo: consulta • IA: DeepSeek</span>
    </div>
    ${!conectado && lastQR ? `<div class="qr"><img src="/qr.png" width="280" alt="QR WhatsApp" style="border-radius:12px"/></div>` : `<p style="opacity:.8;margin:12px 0">Escanea el QR en terminal o espera reconexión automática.</p>`}
    <footer>Puerto ${PORT} · ${new Date().toLocaleString("es-ES",{ timeZone: "Europe/Madrid" })}</footer>
  </div>`)
})
app.get("/qr.png", async (_req,res)=>{
  if(!lastQR) return res.status(404).send("No QR")
  try{
    const png = await qrcode.toBuffer(lastQR, { type:"png", width:512, margin:1 })
    res.set("Content-Type","image/png").send(png)
  }catch{ res.status(500).send("QR error") }
})

// ===== Baileys (ESM dynamic import)
async function loadBaileys(){
  const mod = await import("@whiskeysockets/baileys")
  const makeWASocket = mod.makeWASocket ?? mod.default?.makeWASocket ?? mod.default
  const useMultiFileAuthState = mod.useMultiFileAuthState ?? mod.default?.useMultiFileAuthState
  const fetchLatestBaileysVersion = mod.fetchLatestBaileysVersion ?? mod.default?.fetchLatestBaileysVersion
  const Browsers = mod.Browsers ?? mod.default?.Browsers ?? { macOS:(n="Desktop")=>["MacOS",n,"121.0.0"] }
  if (!makeWASocket || !useMultiFileAuthState) throw new Error("Baileys ESM no expone funciones esperadas")
  return { makeWASocket, useMultiFileAuthState, fetchLatestBaileysVersion, Browsers }
}

// ===== Bot loop
async function startBot(){
  const { makeWASocket, useMultiFileAuthState, fetchLatestBaileysVersion, Browsers } = await loadBaileys()
  const { state, saveCreds } = await useMultiFileAuthState("auth_info")
  const { version } = await fetchLatestBaileysVersion().catch(()=>({version:[2,3000,0]}))
  const sock = makeWASocket({ logger:pino({level:"silent"}), printQRInTerminal:false, auth:state, version, browser:Browsers.macOS("Desktop"), syncFullHistory:false })

  sock.ev.on("creds.update", saveCreds)
  sock.ev.on("connection.update", ({connection, qr})=>{
    if (qr){
      lastQR = qr
      conectado = false
      try{ qrcodeTerminal.generate(qr,{small:true}) }catch{}
    }
    if (connection==="open"){ lastQR=null; conectado=true }
    if (connection==="close"){ conectado=false; setTimeout(()=>startBot().catch(console.error), 2500) }
  })

  sock.ev.on("messages.upsert", async ({messages})=>{
    const m = messages?.[0]; if(!m?.message) return
    const jid = m.key.remoteJid
    const isFromMe = !!m.key.fromMe
    const phone = (jid||"").split("@")[0]
    const text = (m.message.conversation || m.message.extendedTextMessage?.text || m.message?.imageMessage?.caption || "").trim()
    if(!text) return

    // Puntitos => silencio 6h (también si lo mandas tú, sin respuesta)
    if(/^[\s.·•⋅]+$/.test(text)){
      const s=getS(phone); s.snoozeUntil=nowEU().add(6,"hour").valueOf(); SESS.set(phone,s)
      if(!isFromMe) return
      return
    }
    if(isFromMe) return

    const s = getS(phone)
    if(s.snoozeUntil && Date.now()<s.snoozeUntil) return

    // Saludo 24h
    if(Date.now()-s.greetedAt > 24*60*60*1000){
      s.greetedAt = Date.now(); SESS.set(phone,s)
      await sock.sendMessage(jid,{text:GREET})
      setPrompt(s,"greet"); SESS.set(phone,s)
    }

    // 0) Si ya mostramos horas y no hay hora elegida -> IA elige y HOLD
    if(s.lastSlots?.length && !s.pickedISO){
      const pick = await aiPick(text, s.lastSlots)
      const iso = pick?.iso || null
      if(iso){
        const hit = s.lastSlots.find(x=>x.date.format("YYYY-MM-DDTHH:mm")===iso)
        if(hit){
          const durMin = hit.durMin || durationMinForEnvKey(s.svcKey)
          const startISO = hit.date.tz("UTC").toISOString()
          const endISO   = hit.date.clone().add(durMin,"minute").tz("UTC").toISOString()
          cleanupHolds()
          if (hasActiveOverlap({ sede:s.sede, startISO, endISO })){
            await sock.sendMessage(jid,{text:"Ese hueco se acaba de bloquear por otra conversación. Te paso alternativas:"})
            await proposeOnce(s, jid, phone, sock, { text, date_hint:null, part:null })
            return
          }
          const ok = createHold({ phone, sede:s.sede, envServiceKey:s.svcKey, startISO, endISO, staffId: hit.staffId||null })
          if(!ok){
            await sock.sendMessage(jid,{text:"No he podido bloquear ese hueco. Te paso alternativas:"})
            await proposeOnce(s, jid, phone, sock, { text, date_hint:null, part:null })
            return
          }
          s.pickedISO = iso
          s.pickedDurMin = durMin
          SESS.set(phone,s)
        }
      }
    }

    // 1) IA Router (intención + pistas)
    const ai = await aiResolve(text)

    // “ver/editar/cancelar/info”
    if(ai.intent==="view"){
      await sock.sendMessage(jid,{text:`Si ya tienes cita, revísala desde el SMS/email de confirmación.\n\n${BOOKING_SELF}`})
      s.snoozeUntil = nowEU().add(6,"hour").valueOf(); SESS.set(phone,s); return
    }
    if(ai.intent==="edit" || ai.intent==="cancel" || ai.intent==="info"){
      await sock.sendMessage(jid,{text:BOOKING_SELF}); return
    }

    // Guardar category_hint si aún no hay sede
    if(!s.sede && ai.category_hint) s.pendingCat = ai.category_hint

    // Sede (obligatoria antes de shortlist/horas)
    const prevSede = s.sede
    if(ai.sede==="la_luz" || ai.sede==="torremolinos") s.sede = ai.sede
    if(!s.sede){
      if(s.lastPromptType!=="ask_sede"){
        await sock.sendMessage(jid,{text:"¿Salón? *Torremolinos* o *La Luz*."})
        setPrompt(s,"ask_sede"); SESS.set(phone,s)
      }
      return
    }
    if(prevSede && prevSede!==s.sede){
      // si cambia de sede, limpiamos servicio incompatible
      if(!servicesForSede(s.sede).some(x=>x.key===s.svcKey)){ s.svcKey=null; s.svcLabel=null; s.shortlist=[] }
    }

    // Profesional opcional (IA)
    if(ai.staff){
      const label = norm(ai.staff).split(" ")[0]
      const found = EMPLOYEES.find(e=> e.bookable && e.label.includes(label))
      if(found){ s.prefStaffId=found.id; s.prefStaffLabel=found.label.split(" ")[0] }
    }

    // 2) Servicio: shortlist IA + prefiltro semántico
    if(!s.svcKey){
      const shortlist = await aiShortlist(text, s.sede, s.pendingCat || ai.category_hint || null)
      if(shortlist.length){
        s.shortlist = shortlist
        const pickFromText = await aiChooseFromShortlist(text, shortlist)
        if(pickFromText){
          const key = labelToEnvKey(pickFromText, s.sede)
          if(key){ s.svcKey=key; s.svcLabel=labelFromEnvKey(key) }
        }
      }
      if(!s.svcKey){
        const enriched = (s.shortlist.length ? s.shortlist : prefilterCandidates(text, s.sede, s.pendingCat || ai.category_hint || null, 5))
          .slice(0,6)
          .map(l=>{
            const env = labelToEnvKey(l, s.sede)
            const dur = durationMinForEnvKey(env)
            return `• ${l} — ${dur} min`
          }).join("\n")
        if(s.lastPromptType!=="ask_service"){
          await sock.sendMessage(jid,{text:`En ${locationNice(s.sede)} te encaja algo de esto (di uno en texto):\n${enriched}`})
          setPrompt(s,"ask_service"); SESS.set(phone,s)
        }
        return
      }
    } else {
      // Re-map si el usuario suelta un nombre exacto de servicio
      const keyTry = labelToEnvKey(text, s.sede)
      if(keyTry){ s.svcKey=keyTry; s.svcLabel=labelFromEnvKey(keyTry) }
    }

    SESS.set(phone,s)

    // 3) Proponer horas (solo si HAY servicio)
    const wantMore = /\b(otra|mas|m[aá]s|ver horarios?)\b/i.test(norm(text))
    if(!s.pickedISO && s.svcKey && (!s.prompted || wantMore || (Date.now()-s.lastListAt>2*60*1000))){
      await proposeOnce(s, jid, phone, sock, { text, date_hint: ai.date_hint||null, part: ai.part_of_day||null })
      setPrompt(s,"hours_list"); SESS.set(phone,s)
      return
    }

    // 4) Resumen final (si ya hay hora bloqueada)
    if(s.pickedISO){
      const picked = dayjs.tz(s.pickedISO, EURO_TZ)
      const staff = s.prefStaffLabel ? s.prefStaffLabel : "Equipo"
      const summary = `Resumen:\n• Salón: ${locationNice(s.sede)}\n• Servicio: ${s.svcLabel}\n• Profesional: ${staff}\n• Hora: ${fmtES(picked)}\n• Duración: ${s.pickedDurMin} min\n\nAhora una de las compañeras da el OK ✅`
      await sock.sendMessage(jid,{text:summary})
      // reset suave (mantenemos sede/servicio por si pide “otra tarde”)
      s.lastSlots = []; s.lastMap={}; s.prompted=false; s.pickedISO=null
      setPrompt(s,"summary"); SESS.set(phone,s)
      return
    }

    // 5) Si aún no eligió y ya mostramos horas, recordatorio corto
    if(s.prompted && s.svcKey){
      if(s.lastPromptType!=="ask_time"){
        await sock.sendMessage(jid,{text:"Dime la hora/frase (p. ej. “la del martes” o “la de las 13”)."})
        setPrompt(s,"ask_time"); SESS.set(phone,s)
      }
      return
    }

    // Fallback mínimo
    if(s.lastPromptType!=="fallback"){
      await sock.sendMessage(jid,{text:GREET})
      setPrompt(s,"fallback"); SESS.set(phone,s)
    }
  })
}

// ===== Arranque
const server = app.listen(PORT, ()=>{ 
  console.log(`🩷 Gapink Nails Bot v34.3.0 — DeepSeek-only · mejor comprensión · QR http://localhost:${PORT}`)
  startBot().catch(console.error)
})
process.on("SIGTERM", ()=>{ try{ server.close(()=>process.exit(0)) }catch{ process.exit(0) } })
process.on("SIGINT", ()=>{ try{ server.close(()=>process.exit(0)) }catch{ process.exit(0) } })
