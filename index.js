// index.js — Gapink Nails · v37.0.0 (IA compacta, números opcionales)
// Objetivo de esta versión:
// - La conversación entiende salón, servicio, con quién y cuándo “a lo humano”.
// - Puedes elegir por *número* o con lenguaje natural (“la de las 13”, “viernes tarde”…).
// - IA con *coste bajo*: prompts súper cortos, salida JSON mínima, caché y menos tokens.
// - Si pides “con X”, filtra *sólo* huecos de esa profesional; si no hay, cae a equipo.
// - Si no está 100% claro el servicio (p.ej. “uñas”), lista *todas* las opciones (orden inteligente).
//
// Requisitos: Node 18+, vars de entorno (Square + servicios/empleadas) como ya usas.

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

if (!globalThis.crypto) globalThis.crypto = webcrypto
dayjs.extend(utc); dayjs.extend(tz); dayjs.extend(isoWeek); dayjs.locale("es")
const EURO_TZ = "Europe/Madrid"
const nowEU = () => dayjs().tz(EURO_TZ)

// =================== Config rápida ===================
const WORK_DAYS = [1,2,3,4,5]      // L–V
const SLOT_MIN = 15
const OPEN = { start: 9, end: 20 }
const NOW_MIN_OFFSET_MIN = Number(process.env.BOT_NOW_OFFSET_MIN || 30)
const SEARCH_WINDOW_DAYS = Number(process.env.BOT_SEARCH_WINDOW_DAYS || 14)
const SHOW_TOP_N = Number(process.env.SHOW_TOP_N || 5)
const SERVICES_LIST_MAX_N = Number(process.env.SERVICES_LIST_MAX_N || 16)
const HOLIDAYS_EXTRA = (process.env.HOLIDAYS_EXTRA || "06/01,28/02,15/08,12/10,01/11,06/12,08/12,25/12").split(",").map(s=>s.trim())

const DRY_RUN   = /^true$/i.test(process.env.DRY_RUN || "")
const BOT_DEBUG = /^true$/i.test(process.env.BOT_DEBUG || "")

// =================== Square (consulta/booking) ===================
const square = new Client({
  accessToken: process.env.SQUARE_ACCESS_TOKEN,
  environment: (process.env.SQUARE_ENV==="production") ? Environment.Production : Environment.Sandbox
})
const LOC_TORRE = (process.env.SQUARE_LOCATION_ID_TORREMOLINOS || "").trim()
const LOC_LUZ   = (process.env.SQUARE_LOCATION_ID_LA_LUZ || "").trim()
const ADDRESS_TORRE = process.env.ADDRESS_TORREMOLINOS || "Av. de Benyamina 18, Torremolinos"
const ADDRESS_LUZ   = process.env.ADDRESS_LA_LUZ || "Málaga – Barrio de La Luz"
const locationNice = k => k==="la_luz" ? "Málaga – La Luz" : "Torremolinos"
const locationToId = k => k==="la_luz" ? LOC_LUZ : LOC_TORRE

// =================== IA COMPACTA (DeepSeek/OpenAI) ===================
const AI_PROVIDER = (process.env.AI_PROVIDER || (process.env.DEEPSEEK_API_KEY? "deepseek" : process.env.OPENAI_API_KEY? "openai" : "none")).toLowerCase()
const DS_KEY   = process.env.DEEPSEEK_API_KEY || ""
const DS_MODEL = process.env.DEEPSEEK_MODEL || "deepseek-chat"
const OA_KEY   = process.env.OPENAI_API_KEY || ""
const OA_MODEL = process.env.OPENAI_MODEL || "gpt-4o-mini"
const AI_TIMEOUT_MS = Number(process.env.AI_TIMEOUT_MS || 9000)
const aiFetchUrl = AI_PROVIDER==="deepseek" ? "https://api.deepseek.com/v1/chat/completions" : "https://api.openai.com/v1/chat/completions"

// Minimizamos coste: system *muy* corto + salida con claves de 1–3 letras + máx 160 tokens
async function aiChatTiny(system, user){
  if (AI_PROVIDER==="none") return null
  const controller = new AbortController()
  const to = setTimeout(()=>controller.abort(), AI_TIMEOUT_MS)
  try{
    const headers = { "Content-Type":"application/json", "Authorization":`Bearer ${AI_PROVIDER==="deepseek"? DS_KEY : OA_KEY}` }
    const body = JSON.stringify({
      model: AI_PROVIDER==="deepseek" ? DS_MODEL : OA_MODEL,
      temperature: 0.2,
      max_tokens: 160,
      messages: [{ role:"system", content: system }, { role:"user", content: user }]
    })
    const resp = await fetch(aiFetchUrl, { method:"POST", headers, body, signal: controller.signal })
    clearTimeout(to)
    if (!resp.ok) return null
    const data = await resp.json()
    return data?.choices?.[0]?.message?.content || null
  }catch{ clearTimeout(to); return null }
}
function stripJSON(s){
  if (!s) return null
  let t = s.trim().replace(/```json/gi,"```")
  if (t.startsWith("```")) t=t.slice(3)
  if (t.endsWith("```")) t=t.slice(0,-3)
  const i=t.indexOf("{"), j=t.lastIndexOf("}")
  if (i>=0 && j>i) t=t.slice(i,j+1)
  try{ return JSON.parse(t) }catch{ return null }
}
// caché simple por teléfono para no repetir IA si el mensaje es igual (ahorra tokens)
const aiCache = new Map()
function cacheKey(phone, sys, user){ return createHash("sha256").update(phone+"|"+sys+"|"+user).digest("hex").slice(0,32) }
async function aiTiny(phone, sys, user){
  const key = cacheKey(phone, sys, user)
  const item = aiCache.get(key)
  const freshMs = 2*60*1000
  if (item && (Date.now()-item.t)<freshMs) return item.v
  const out = await aiChatTiny(sys, user)
  aiCache.set(key, { v: out, t: Date.now() }); return out
}

// =================== Utils ===================
const onlyDigits = s => String(s||"").replace(/\D+/g,"")
const rm = s => String(s||"").normalize("NFD").replace(/\p{Diacritic}/gu,"")
const norm = s => rm(s).toLowerCase().replace(/[+.,;:()/_-]/g," ").replace(/[^\p{Letter}\p{Number}\s]/gu," ").replace(/\s+/g," ").trim()
const fmtES = d => {
  const dias=["domingo","lunes","martes","miércoles","jueves","viernes","sábado"]
  const t=(dayjs.isDayjs(d)?d:dayjs(d)).tz(EURO_TZ)
  return `${dias[t.day()]} ${String(t.date()).padStart(2,"0")}/${String(t.month()+1).padStart(2,"0")} ${String(t.hour()).padStart(2,"0")}:${String(t.minute()).padStart(2,"0")}`
}
const fmtDay = d => {
  const dias=["domingo","lunes","martes","miércoles","jueves","viernes","sábado"]
  const t=(dayjs.isDayjs(d)?d:dayjs(d)).tz(EURO_TZ)
  return `${dias[t.day()]} ${String(t.date()).padStart(2,"0")}/${String(t.month()+1).padStart(2,"0")}`
}
const fmtHour = d => { const t=(dayjs.isDayjs(d)?d:dayjs(d)).tz(EURO_TZ); return `${String(t.hour()).padStart(2,"0")}:${String(t.minute()).padStart(2,"0")}` }
const titleCase = s => String(s||"").toLowerCase().replace(/\b([a-záéíóúñ])/g, m=>m.toUpperCase())
function applySpanishDiacritics(label){
  let x = String(label||"")
  x = x.replace(/\bunas\b/gi, m => m[0] === 'U' ? 'Uñas' : 'uñas')
  x = x.replace(/\bpestan(as?)?\b/gi, (m) => (m[0]==='P'?'Pestañ':'pestañ') + 'as')
  x = x.replace(/\bnivelacion\b/gi, m => m[0]==='N' ? 'Nivelación' : 'nivelación')
  x = x.replace(/\bfrances\b/gi, m => m[0]==='F' ? 'Francés' : 'francés')
  x = x.replace(/\bsemi ?permanente\b/gi, m => /[A-Z]/.test(m[0]) ? 'Semipermanente' : 'semipermanente')
  return x
}
const cleanDisplayLabel = s => applySpanishDiacritics(String(s||"").replace(/^\s*(luz|la\s*luz)\s+/i,"").trim())

// =================== Horario ===================
function isHolidayEU(d){ const dd=String(d.date()).padStart(2,"0"), mm=String(d.month()+1).padStart(2,"0"); return HOLIDAYS_EXTRA.includes(`${dd}/${mm}`) }
function nextOpeningFrom(d){
  let t=d.clone()
  const nowMin = t.hour()*60 + t.minute()
  const openMin= OPEN.start*60, closeMin=OPEN.end*60
  if (nowMin < openMin) t = t.hour(OPEN.start).minute(0).second(0).millisecond(0)
  if (nowMin >= closeMin) t = t.add(1,"day").hour(OPEN.start).minute(0).second(0).millisecond(0)
  while (!WORK_DAYS.includes(t.day()) || isHolidayEU(t)) t = t.add(1,"day").hour(OPEN.start).minute(0).second(0).millisecond(0)
  return t
}
function ceilToSlotEU(t){ const m=t.minute(), rem=m%SLOT_MIN; return rem===0 ? t.second(0).millisecond(0) : t.add(SLOT_MIN-rem,"minute").second(0).millisecond(0) }

// =================== DB ligera ===================
const db = new Database("gapink.db"); db.pragma("journal_mode = WAL")
db.exec(`
CREATE TABLE IF NOT EXISTS sessions (phone TEXT PRIMARY KEY, data_json TEXT, updated_at TEXT);
CREATE TABLE IF NOT EXISTS logs (id INTEGER PRIMARY KEY AUTOINCREMENT, phone TEXT, direction TEXT, text TEXT, at TEXT);
`)
function saveSession(phone,s){ const j=JSON.stringify(s||{}); const up=db.prepare(`UPDATE sessions SET data_json=@j,updated_at=@u WHERE phone=@p`).run({j,u:new Date().toISOString(),p:phone}); if(!up.changes) db.prepare(`INSERT INTO sessions (phone,data_json,updated_at) VALUES (@p,@j,@u)`).run({p:phone,j,u:new Date().toISOString()}) }
function loadSession(phone){ const r=db.prepare(`SELECT data_json FROM sessions WHERE phone=@p`).get({p:phone}); return r?.data_json? JSON.parse(r.data_json) : {} }
function logIO(phone, direction, text){ try{ db.prepare(`INSERT INTO logs (phone,direction,text,at) VALUES (@p,@d,@t,@a)`).run({p:phone,direction:direction||"sys",t:text||"",a:new Date().toISOString()}) }catch{} }

// =================== Empleadas (fuzzy + sedes) ===================
function parseStaffCenters(){
  const centers = {}
  for (const [k,v] of Object.entries(process.env)) {
    if (!k.startsWith("EMP_CENTER_")) continue
    const name = k.replace(/^EMP_CENTER_/,"")
    const set = new Set(String(v||"").split(",").map(s=>norm(s)).map(s=> s.includes("luz") ? "la_luz" : "torremolinos"))
    centers[name.toLowerCase()] = [...set]
  }
  return centers
}
const STAFF_CENTERS = parseStaffCenters()
function deriveLabelsFromEnvKey(envKey){
  const raw = envKey.replace(/^SQ_EMP_/,"")
  const toks = raw.split("_").map(t=>norm(t)).filter(Boolean)
  const uniq = Array.from(new Set(toks))
  const labels = [...uniq]
  if (uniq.length>1) labels.push(uniq.join(" "))
  return labels.map(l=>l.replace(/\b([a-z])/g,m=>m.toUpperCase()))
}
function parseEmployees(){
  const tmp=[]
  for (const [k,v] of Object.entries(process.env)) {
    if (!k.startsWith("SQ_EMP_")) continue
    const [id, tag, locs] = String(v||"").split("|")
    if (!id) continue
    const bookable = !(String(tag||"").toUpperCase().includes("NO_BOOKABLE"))
    const labels = deriveLabelsFromEnvKey(k)
    const keyname = k.replace(/^SQ_EMP_/,"").toLowerCase()
    const centers = STAFF_CENTERS[keyname] || (String(locs||"").includes("LF5")||String(locs||"").includes("la_luz") ? ["la_luz"] : String(locs||"").includes("LSMN")||String(locs||"").includes("torremolinos") ? ["torremolinos"] : ["la_luz","torremolinos"])
    tmp.push({ id, bookable, labels, centers })
  }
  // dedupe por id
  const map = new Map()
  for (const e of tmp){
    const prev = map.get(e.id)
    if (!prev) map.set(e.id, e)
    else map.set(e.id, { id:e.id, bookable:(prev.bookable||e.bookable), labels:Array.from(new Set([...prev.labels,...e.labels])), centers:Array.from(new Set([...prev.centers,...e.centers])) })
  }
  return [...map.values()]
}
let EMPLOYEES = parseEmployees()
const staffLabelFromId = id => EMPLOYEES.find(e=>e.id===id)?.labels?.[0] || null
function bestStaffFromText(text, sedeKey){
  const t = " "+norm(text)+" "
  if (/\bcon\s+ella\b/.test(t)) return { withHer:true }
  const aliases = Array.from(new Set(EMPLOYEES.flatMap(e=>e.labels.map(l=>({ id:e.id, name:norm(l), centers:e.centers, bookable:e.bookable })))))
  let pick = null
  for (const a of aliases){
    const pref = a.name.slice(0, Math.max(4, Math.ceil(a.name.length*0.6)))
    const re = new RegExp(`(^|\\s)${pref}[a-zñ]*?(\\s|$)`)
    if (re.test(t)) { pick = a; break }
  }
  if (!pick) return null
  // preferir que atienda en esa sede y sea bookable
  const matches = EMPLOYEES.filter(e=> e.id===pick.id)
  matches.sort((A,B)=>{
    const b1=(B.bookable?1:0)-(A.bookable?1:0); if (b1) return b1
    const b2=((B.centers||[]).includes(sedeKey)?1:0)-((A.centers||[]).includes(sedeKey)?1:0); if (b2) return b2
    return (B.labels?.[0]||"").length - (A.labels?.[0]||"").length
  })
  return matches[0] || null
}
function uniqueStaffFromLastProposed(session){
  const vals = Object.values(session.lastStaffByIso||{}).filter(Boolean)
  const uniq = [...new Set(vals)]
  return uniq.length===1 ? uniq[0] : null
}

// =================== Servicios ===================
function makeLabelFromKey(base, sedeKey){
  let label = titleCase(base.replaceAll("_"," "))
  label = applySpanishDiacritics(label)
  if (sedeKey==="la_luz") label = label.replace(/^Luz\s+/i,"").trim()
  return cleanDisplayLabel(label)
}
function servicesForSedeKeyRaw(sedeKey){
  const prefix = (sedeKey==="la_luz") ? "SQ_SVC_luz_" : "SQ_SVC_"
  const out=[]
  for (const [k,v] of Object.entries(process.env)){
    if (!k.startsWith(prefix)) continue
    const [id] = String(v||"").split("|"); if (!id) continue
    const base = k.replace(prefix,"")
    out.push({ sedeKey, key:k, id, label: makeLabelFromKey(base, sedeKey) })
  }
  // dedupe por label
  const seen = new Set(), res=[]
  for (const s of out){ const L=s.label.toLowerCase(); if (seen.has(L)) continue; seen.add(L); res.push(s) }
  return res
}
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
  if (!found) return null
  return { envKey: found.key, id: found.id, label: found.label }
}

// Filtrado por “uñas” si está ambiguo (minimiza IA)
function looksNails(text){ return /\b(uñ|unias|unas|manicura|pedicur|gel|acril|semiperman|nivelaci[oó]n|tips|frances|franc[eé]s)\b/i.test(norm(text)) }
function listNails(sedeKey){
  const all = servicesForSedeKeyRaw(sedeKey)
  return all.filter(s => /\b(uñ|manicura|pedicur|gel|acril|semi|nivel|tips|franc[eé]s)\b/i.test(norm(s.label)))
}

// =================== IA: prompts MINIS ===================
function sysInterpret(){ 
  // Formato ultra corto. Claves:
  // a: acción (set_salon,set_cat,set_staff,choose_service,propose,weekly,none)
  // s: salón (torremolinos|la_luz)
  // v: label exacto del servicio (o null)
  // st: nombre profesional (o "__ELLA__")
  // t: pista temporal (“viernes tarde”, “la próxima semana”)
  // cat: categoría texto corta (“uñas”,“depilación”,“faciales”,“pestañas”,“micro”)
  // list: true si hace falta lista de servicios
  return "Eres NLU WhatsApp de un centro de belleza. Devuelve SOLO JSON con claves {a,s,v,st,t,cat,list}. Nada de explicaciones."
}
function userInterpret(msg, ctx, staffPipe, labelsPipe){
  // En vez de volcar listas largas, comprimimos en pipes una sola línea
  const u = (""+msg).slice(0,320) // recortamos entrada
  const c = (ctx||"").slice(0,120)
  const st = (staffPipe||"").slice(0,240)
  const sv = (labelsPipe||"").slice(0,600) // top 600 chars
  return `msg="${u}" ctx="${c}" staff="${st}" servicios="${sv}"
Responde como {"a":"...","s":"...","v":"...","st":"...","t":"...","cat":"...","list":true|false}`
}

// Ranking mini (si hace falta ordenar lista completa/“uñas”)
function sysRank(){ return "Ordena por relevancia. Devuelve SOLO {o:[labels_en_orden]}." }
function userRank(userText, labels){ 
  const u = (""+userText).slice(0,240)
  const l = labels.slice(0,50).join("|").slice(0,1200) // máximo ~1200 chars
  return `u="${u}" l="${l}"`
}

// Mapping de natural → índice
function sysPick(){ return "De opciones numeradas 1..N y frase libre del usuario, devuelve SOLO {i:0-based or null}." }
function userPick(optsPretty, userText){
  const list = optsPretty.map((t,i)=>`${i+1}. ${t}`).join(" | ").slice(0,1000)
  const u = (""+userText).slice(0,180)
  return `opciones="${list}" usuario="${u}"`
}

// =================== Interpretación local ===================
function parseSede(text){ const t=norm(text); if (/\b(luz|la luz)\b/.test(t)) return "la_luz"; if (/\b(torre|torre?molinos|playamar)\b/.test(t)) return "torremolinos"; return null }
function parseTemporal(text){
  const t=norm(text)
  const now=nowEU()
  let when=null, part=null, nextWeek=false
  if (/\bhoy\b/.test(t)) when=now
  else if (/\bmanana\b/.test(t)) when=now.add(1,"day")
  else if (/\bpasado\b/.test(t)) when=now.add(2,"day")
  if (/\b(lunes|martes|miercoles|miércoles|jueves|viernes)\b/.test(t)){
    const map={lunes:1,martes:2,"miercoles":3,"miércoles":3,jueves:4,viernes:5}
    const m = t.match(/\b(lunes|martes|miercoles|miércoles|jueves|viernes)\b/); const d=map[m[1]]
    let p=now.clone(); for(let k=0;k<7;k++){ if (p.day()===d){ when=p; break } p=p.add(1,"day") }
  }
  if (/\b(tarde)\b/.test(t)) part="tarde"
  if (/\b(por la manana|por la mañana|manana|mañana)\b/.test(t)) part=part||"mañana"
  if (/\bnoche\b/.test(t)) part="noche"
  if (/\bproxima semana|semana que viene\b/.test(t)) nextWeek=true
  return { when, part, nextWeek }
}

// =================== Square helpers ===================
async function getServiceIdAndVersion(envKey){
  const raw = process.env[envKey]; if (!raw) return null
  let [id, ver] = String(raw).split("|"); ver=ver?Number(ver):null
  if (!ver){
    try{ const r = await square.catalogApi.retrieveCatalogObject(id,true); ver = Number(r?.result?.object?.version)||1 }catch{ ver=1 }
  }
  return { id, version: ver||1 }
}
async function searchAvailWindow({ locationKey, envServiceKey, startEU, endEU, limit=300 }){
  const sv = await getServiceIdAndVersion(envServiceKey)
  if (!sv?.id) return []
  const body = { query:{ filter:{
    startAtRange:{ startAt:startEU.tz("UTC").toISOString(), endAt:endEU.tz("UTC").toISOString() },
    locationId: locationToId(locationKey),
    segmentFilters:[{ serviceVariationId: sv.id }]
  }}}
  try{
    const resp = await square.bookingsApi.searchAvailability(body)
    const avail = resp?.result?.availabilities || []
    const out=[]
    for (const a of avail){
      const d = dayjs(a?.startAt).tz(EURO_TZ); if (!d || d.hour()<OPEN.start || d.hour()>=OPEN.end) continue
      const segs = Array.isArray(a.appointmentSegments) ? a.appointmentSegments : (Array.isArray(a.segments)? a.segments : [])
      const tm = segs[0]?.teamMemberId || null
      out.push({ date:d, staffId: tm })
      if (out.length>=limit) break
    }
    out.sort((A,B)=>A.date.valueOf()-B.date.valueOf())
    return out
  }catch{ return [] }
}

// =================== Propuestas ===================
async function proposeTimes({ phone, s, svcKey, svcLabel, preferStaffId=null, hintText=null }){
  const now = nowEU()
  const base = nextOpeningFrom(now.add(NOW_MIN_OFFSET_MIN,"minute"))
  let startEU = base.clone(), endEU = base.clone().add(SEARCH_WINDOW_DAYS,"day").hour(OPEN.end).minute(0)
  if (hintText){
    const { when, part, nextWeek } = parseTemporal(hintText)
    if (when){ startEU = when.clone().hour(OPEN.start); endEU = when.clone().hour(OPEN.end) }
    if (nextWeek){ startEU = startEU.add(7,"day"); endEU = endEU.add(7,"day") }
    if (part==="tarde") startEU = startEU.hour(15)
    if (part==="mañana") startEU = startEU.hour(9)
    if (part==="noche") startEU = startEU.hour(18)
  }
  const all = await searchAvailWindow({ locationKey:s, envServiceKey:svcKey, startEU, endEU, limit:400 })
  let slots = all
  let usedPreferred=false
  if (preferStaffId){
    slots = all.filter(x=>x.staffId===preferStaffId)
    usedPreferred = true
    if (!slots.length){ slots = all; usedPreferred=false }
  }
  const top = slots.slice(0,SHOW_TOP_N)
  const map = {}; top.forEach(x=>{ map[x.date.format("YYYY-MM-DDTHH:mm")] = x.staffId || null })
  const pretty = top.map(d=>`${fmtDay(d.date)} ${fmtHour(d.date)}`)
  // persistimos en sesión
  let sess = loadSession(phone) || {}
  sess.lastTimes = top.map(x=>x.date.format("YYYY-MM-DDTHH:mm"))
  sess.lastStaffByIso = map
  sess.lastSvc = { key: svcKey, label: svcLabel, sede: s }
  saveSession(phone, sess)
  return { prettyList: pretty, topDates: top.map(x=>x.date), map, usedPreferred }
}

// =================== WhatsApp (Baileys mini web) ===================
const app = express()
const PORT = process.env.PORT || 8080
let lastQR=null, conectado=false
app.get("/", (_req,res)=> res.send(`<!doctype html><meta charset="utf-8"><style>body{font-family:system-ui;display:grid;place-items:center;min-height:100vh;background:#f6f7f8}.card{max-width:740px;padding:28px;border-radius:18px;box-shadow:0 8px 30px rgba(0,0,0,.08);background:#fff}</style><div class="card"><h1>Gapink Nails Bot — v37 (IA compacta)</h1><p>WhatsApp: ${conectado?"✅ Conectado":"❌ Desconectado"}</p>${!conectado&&lastQR?`<img src="/qr.png" width="300">`:""}</div>`))
app.get("/qr.png", async (_req,res)=>{ if(!lastQR) return res.status(404).send("no qr"); const png=await qrcode.toBuffer(lastQR,{type:"png",width:512,margin:1}); res.set("Content-Type","image/png").send(png) })
async function loadBaileys(){ let mod=null; try{ mod=await import("@whiskeysockets/baileys") }catch{}; if(!mod) throw new Error("Baileys no disponible"); const makeWASocket=mod.makeWASocket||mod.default?.makeWASocket||mod.default; const useMultiFileAuthState=mod.useMultiFileAuthState||mod.default?.useMultiFileAuthState; const fetchLatestBaileysVersion=mod.fetchLatestBaileysVersion||mod.default?.fetchLatestBaileysVersion|| (async()=>({version:[2,3000,0]})); const Browsers=mod.Browsers||mod.default?.Browsers||{ macOS:(n="Desktop")=>["MacOS",n,"121.0.0"] }; return { makeWASocket, useMultiFileAuthState, fetchLatestBaileysVersion, Browsers } }

// =================== Conversación ===================
function buildGreeting(){
  return `¡Hola! Soy el asistente de Gapink Nails 💅
Dime salón (*Torremolinos* o *La Luz*), lo que quieres y si lo prefieres *con alguien* y *cuándo*. Yo te paso horas.
Puedes responder con números o en tus palabras (ej. “viernes tarde”, “la de las 13”).`
}
function parseSedeFromSessionOrText(s, txt){ return s?.sede || parseSede(txt) || null }
function staffPipe(){ return EMPLOYEES.filter(e=>e.bookable).map(e=>e.labels[0]).join("|") }

// IA: interpretar mensaje (compacto, y sólo las *etiquetas* de servicio de la sede ya elegida)
async function interpretMessage(phone, text, session){
  const sede = parseSedeFromSessionOrText(session, text)
  const labels = sede ? servicesForSedeKeyRaw(sede).map(s=>s.label) : []
  const sys = sysInterpret()
  const ctx = `s:${session?.sede||""}|v:${session?.svcLabel||""}|st:${session?.preferStaffLabel||""}|cat:${session?.cat||""}`
  const user = userInterpret(text, ctx, staffPipe(), labels.join("|"))
  const out = await aiTiny(phone, sys, user)
  const js = stripJSON(out) || {}
  // normalizamos mínimos
  return {
    a: js.a || "none",
    s: js.s==="la_luz"||js.s==="torremolinos" ? js.s : null,
    v: js.v || null,
    st: js.st || null,
    t: js.t || null,
    cat: js.cat || null,
    list: !!js.list
  }
}

// IA: ordenar lista (sólo cuando haga falta — ahorra tokens)
async function rankLabels(phone, userText, labels){
  if (!labels?.length) return labels
  const sys=sysRank(), user=userRank(userText, labels)
  const out=await aiTiny(phone, sys, user)
  const js=stripJSON(out)
  if (Array.isArray(js?.o) && js.o.length){
    const set = new Set(js.o.map(x=>String(x).toLowerCase()))
    const known = labels.filter(l=>set.has(l.toLowerCase()))
    const unknown = labels.filter(l=>!set.has(l.toLowerCase()))
    return [...known, ...unknown]
  }
  return labels
}

// IA: elegir opción por lenguaje (“la segunda”, “viernes 13 a las 13”) → índice
async function pickIndexFromNatural(phone, prettyList, userText){
  const sys=sysPick(), user=userPick(prettyList, userText)
  const out=await aiTiny(phone, sys, user)
  const js=stripJSON(out)
  return (js && Number.isInteger(js.i) && js.i>=0 && js.i<prettyList.length) ? js.i : null
}

// =================== Loop WhatsApp ===================
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
      if (connection==="open"){ lastQR=null; conectado=true }
      if (connection==="close"){ conectado=false; setTimeout(()=>startBot().catch(console.error), 3000) }
    })
    sock.ev.on("creds.update", saveCreds)

    sock.ev.on("messages.upsert", async ({messages})=>{
      const m=messages?.[0]; if (!m?.message) return
      if (m.key.fromMe) return
      const jid = m.key.remoteJid || ""
      if (jid.endsWith("@g.us")) return
      const phone = ((jid||"").split("@")[0]||"")
      const text = (m.message.conversation || m.message.extendedTextMessage?.text || m.message?.imageMessage?.caption || "").trim()
      if (!text) return
      logIO(phone,"in",text)

      let s = loadSession(phone) || {}
      if (!s.greetedAt || (Date.now() - (s.greetedAt||0)) > 24*60*60*1000){
        s.greetedAt = Date.now(); saveSession(phone,s)
        await sock.sendMessage(jid,{ text: buildGreeting() })
      }

      // 1) Sede
      const sedeMsg = parseSede(text); if (sedeMsg){ s.sede = sedeMsg; saveSession(phone,s) }

      // 2) “con X / con ella”
      const staffReq = bestStaffFromText(text, s.sede||null)
      if (staffReq?.withHer){ const uid=uniqueStaffFromLastProposed(s); if (uid){ s.preferStaffId=uid; s.preferStaffLabel=staffLabelFromId(uid); saveSession(phone,s) } }
      else if (staffReq){ s.preferStaffId=staffReq.id; s.preferStaffLabel=staffReq.labels[0]; saveSession(phone,s) }

      // 3) IA compacta para pegar saltos (sin listas largas)
      const sede = s.sede || parseSede(text) || null
      const labelsForSede = sede ? servicesForSedeKeyRaw(sede).map(x=>x.label) : []
      const tiny = await interpretMessage(phone, text, s)

      // actualizamos con lo que venga
      if (tiny.s) { s.sede = tiny.s; saveSession(phone,s) }
      if (tiny.st && tiny.st==="__ELLA__"){ const uid=uniqueStaffFromLastProposed(s); if (uid){ s.preferStaffId=uid; s.preferStaffLabel=staffLabelFromId(uid); saveSession(phone,s) } }
      else if (tiny.st){ const f=bestStaffFromText("con "+tiny.st, s.sede||null); if (f){ s.preferStaffId=f.id; s.preferStaffLabel=f.labels[0]; saveSession(phone,s) } }
      if (tiny.cat) { s.cat = tiny.cat; saveSession(phone,s) }

      // 4) Servicio directo si label exacto
      if (tiny.v && s.sede){
        const r = resolveEnvKeyFromLabelAndSede(tiny.v, s.sede)
        if (r){ s.svcKey=r.envKey; s.svcLabel=r.label; saveSession(phone,s) }
      }

      // 5) Si dijo “uñas” o no hay servicio claro → listar (orden inteligente)
      let needList = tiny.list || (!s.svcKey)
      if (s.sede && needList){
        const base = looksNails(text) || s.cat==="uñas" ? listNails(s.sede) : servicesForSedeKeyRaw(s.sede)
        let labels = base.map(x=>x.label)
        // IA de ranking sólo si de verdad está ambiguo
        if (!tiny.v){ try{ labels = await rankLabels(phone, text, labels) }catch{} }
        const withDur = attachDurations(base).sort((a,b)=> labels.indexOf(a.label)-labels.indexOf(b.label))
        const top = withDur.slice(0, SERVICES_LIST_MAX_N)
        const bullets = top.map(i=>`• ${i.label} — ${i.mins} min`).join("\n")
        s.serviceMenu = top.map((x,i)=>({ idx:i+1, key:x.key, label:x.label }))
        saveSession(phone,s)
        await sock.sendMessage(jid,{ text:
          `Dímelo en tus palabras o elige (lo ordeno por lo más probable):\n${bullets}\n\nPuedes escribir el nombre (“Manicura Semipermanente”) o el *número*.`
        })
        return
      }

      // 6) Si el usuario ha escrito uno de la lista por número
      const t = norm(text)
      const pickNum = t.match(/^\s*([1-9]\d*)\b/)
      if (pickNum && Array.isArray(s.serviceMenu) && s.serviceMenu.length){
        const n = Number(pickNum[1]); const sel = s.serviceMenu.find(x=>x.idx===n)
        if (sel){ s.svcKey=sel.key; s.svcLabel=sel.label; saveSession(phone,s) }
      }

      // 7) Si ya tenemos salón + servicio → proponer horas (filtrando staff si hay)
      if (s.sede && s.svcKey){
        const prop = await proposeTimes({
          phone, s: s.sede, svcKey: s.svcKey, svcLabel: s.svcLabel || "Servicio",
          preferStaffId: s.preferStaffId || null, hintText: tiny.t || text
        })
        if (!prop.topDates.length){
          await sock.sendMessage(jid,{ text:`No veo huecos en ese rango. ¿Otra fecha/franja (ej. “viernes tarde”, “próxima semana”)?` })
          return
        }
        const header = (prop.usedPreferred && s.preferStaffLabel) ? `Huecos con ${s.preferStaffLabel}:` : `Huecos del equipo:`
        const lines = prop.prettyList.map((p,i)=>`${i+1}) ${p}${prop.map[s.lastTimes?.[i]]?` — ${staffLabelFromId(prop.map[s.lastTimes?.[i]])||""}`:""}`).join("\n")
        await sock.sendMessage(jid,{ text:
          `${s.svcLabel} en ${locationNice(s.sede)}\n${header}\n${lines}\n\nElige con *número* o dime “la de las 13”, “viernes tarde”…`
        })

        // Capturamos reservas con lenguaje (sin número)
        const idx = await pickIndexFromNatural(phone, prop.prettyList, text)
        if (idx!=null){
          const iso = s.lastTimes[idx]
          const staffId = s.lastStaffByIso?.[iso] || s.preferStaffId || null
          const d = dayjs(iso).tz(EURO_TZ)
          await sock.sendMessage(jid,{ text:
            `✅ Te reservo provisionalmente:\n📍 ${locationNice(s.sede)}\n🧾 ${s.svcLabel}\n🕐 ${fmtES(d)}${staffId?`\n👩‍💼 ${staffLabelFromId(staffId)}`:""}\n\nUna compañera lo confirma y te avisa ✅`
          })
        }
        return
      }

      // 8) Si aún no hay salón
      if (!s.sede){
        await sock.sendMessage(jid,{ text: "¿En qué *salón* te viene mejor? *Torremolinos* o *La Luz*." })
        return
      }

      // 9) Fallback: pide servicio claro
      if (!s.svcKey){
        const base = looksNails(text) || s.cat==="uñas" ? listNails(s.sede) : servicesForSedeKeyRaw(s.sede)
        const labels = base.map(x=>x.label)
        const withDur = attachDurations(base)
        const top = withDur.slice(0,SERVICES_LIST_MAX_N)
        const bullets = top.map(i=>`• ${i.label} — ${i.mins} min`).join("\n")
        s.serviceMenu = top.map((x,i)=>({ idx:i+1, key:x.key, label:x.label })); saveSession(phone,s)
        await sock.sendMessage(jid,{ text:
          `Dime el *servicio* o elige de la lista:\n${bullets}\n\nPuedes escribir el nombre o el *número*.`
        })
        return
      }
    })
  }catch(e){
    if (BOT_DEBUG) console.error(e)
    setTimeout(()=>startBot().catch(console.error), 4000)
  }
}

// =================== Arranque ===================
const server=app.listen(PORT, ()=>{ console.log(`🩷 Gapink Nails Bot v37 — IA compacta (${AI_PROVIDER}) — http://localhost:${PORT}`); startBot().catch(console.error) })
process.on("uncaughtException", e=>{ console.error("💥 uncaughtException:", e?.stack||e) })
process.on("unhandledRejection", e=>{ console.error("💥 unhandledRejection:", e) })
process.on("SIGTERM", ()=>{ try{ server.close(()=>process.exit(0)) }catch{ process.exit(0) } })
process.on("SIGINT",  ()=>{ try{ server.close(()=>process.exit(0)) }catch{ process.exit(0) } })
