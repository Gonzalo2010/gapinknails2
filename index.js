// index.js — Gapink Nails · v37.4.0 (IA mejorada full-NLU + holds 6h + DeepSeek-only + mini web QR)
// - IA en TODO: entiende salón/tema/servicio/fecha (“la primera”, “otra tarde”, “viernes por la mañana”),
//   y también “con <nombre>” (solo entonces muestra/filtra por profesional).
// - Cero keywords: la IA decide; hay heurística solo como “airbag” si el LLM falla.
// - Duración real por servicio desde ENV (SQ_DUR_*), encaja slots por duración + horario laboral.
// - Bloqueo local 6h en SQLite (holds), sin tocar Square (solo consulta disponibilidad).
// - UX: pocas preguntas, no repite, no lista infinito, resumen al final cuando hay hold.
// - DeepSeek-only con presupuestado adaptativo: LITE → BOOST si hay baja confianza.
// - Mini web QR y Baileys con import ESM dinámico.

import express from "express"
import pino from "pino"
import qrcode from "qrcode"
import qrcodeTerminal from "qrcode-terminal"
import "dotenv/config"
import Database from "better-sqlite3"
import dayjs from "dayjs"
import utc from "dayjs/plugin/utc.js"
import tz from "dayjs/plugin/timezone.js"
import isoWeek from "dayjs/plugin/isoWeek.js"
import "dayjs/locale/es.js"
import { webcrypto } from "crypto"
import { Client, Environment } from "square"

if (!globalThis.crypto) globalThis.crypto = webcrypto
dayjs.extend(utc); dayjs.extend(tz); dayjs.extend(isoWeek); dayjs.locale("es")
const EURO_TZ = "Europe/Madrid"
const nowEU = () => dayjs().tz(EURO_TZ)

// ===== Config negocio
const OPEN = { start: 9, end: 20 }                 // L–V 09–20
const WORK_DAYS = [1,2,3,4,5]                      // L–V
const SEARCH_WINDOW_DAYS   = Number(process.env.BOT_SEARCH_WINDOW_DAYS   || 30)
const EXTENDED_WINDOW_DAYS = Number(process.env.BOT_EXTENDED_WINDOW_DAYS || 90)
const NOW_MIN_OFFSET_MIN   = Number(process.env.BOT_NOW_OFFSET_MIN       || 30)
const SHOW_TOP_N           = Number(process.env.SHOW_TOP_N               || 5)
const HOLD_HOURS           = Number(process.env.HOLD_HOURS               || 6)
const PORT                 = Number(process.env.PORT                     || 8080)
const BOT_DEBUG            = /^true$/i.test(process.env.BOT_DEBUG        || "")

// ===== Square (solo consultas de disponibilidad)
const square = new Client({
  accessToken: process.env.SQUARE_ACCESS_TOKEN,
  environment: (process.env.SQUARE_ENV==="production") ? Environment.Production : Environment.Sandbox
})
const LOC_TORRE = (process.env.SQUARE_LOCATION_ID_TORREMOLINOS || "").trim()
const LOC_LUZ   = (process.env.SQUARE_LOCATION_ID_LA_LUZ || "").trim()
function locationToId(key){ return key==="la_luz" ? LOC_LUZ : LOC_TORRE }
function locationNice(key){ return key==="la_luz" ? "Málaga – La Luz" : "Torremolinos" }

// ===== IA DeepSeek (presupuesto adaptativo)
const DEEPSEEK_API_KEY = process.env.DEEPSEEK_API_KEY || ""
const DEEPSEEK_MODEL   = process.env.DEEPSEEK_MODEL || "deepseek-chat"
const AI_TIMEOUT_MS    = Number(process.env.AI_TIMEOUT_MS || 10000)
// Presupuestos escalonados
const TOKENS = {
  router_lite: Number(process.env.AI_TOKENS_ROUTER_LITE || 80),
  router_boost: Number(process.env.AI_TOKENS_ROUTER_BOOST || 160),
  shortlist: Number(process.env.AI_TOKENS_SHORTLIST || 160),
  choose: Number(process.env.AI_TOKENS_CHOOSE || 110),
  pick: Number(process.env.AI_TOKENS_PICK || 110)
}
async function aiCall(messages, {maxTokens=120, temperature=0.15}={}){
  if(!DEEPSEEK_API_KEY) return null
  const controller = new AbortController()
  const t = setTimeout(()=>controller.abort(), AI_TIMEOUT_MS)
  try{
    const resp = await fetch("https://api.deepseek.com/chat/completions",{
      method:"POST",
      headers:{ "Content-Type":"application/json", "Authorization":`Bearer ${DEEPSEEK_API_KEY}` },
      body: JSON.stringify({ model: DEEPSEEK_MODEL, temperature, max_tokens:maxTokens, messages }),
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
  let x = s.trim().replace(/```json/gi,"```")
  if (x.startsWith("```")) x=x.slice(3)
  if (x.endsWith("```")) x=x.slice(0,-3)
  const i=x.indexOf("{"), j=x.lastIndexOf("}")
  if(i>=0 && j>i) x=x.slice(i,j+1)
  try{ return JSON.parse(x) }catch{ return null }
}

// ===== Utils
function rm(s){ return String(s||"").normalize("NFD").replace(/\p{Diacritic}/gu,"") }
function norm(s){ return rm(s).toLowerCase().replace(/[+.,;:()/_-]/g," ").replace(/[^\p{Letter}\p{Number}\s]/gu," ").replace(/\s+/g," ").trim() }
function fmtES(d){
  const dias=["domingo","lunes","martes","miércoles","jueves","viernes","sábado"]
  const t=dayjs(d).tz(EURO_TZ)
  return `${dias[t.day()]} ${String(t.date()).padStart(2,"0")}/${String(t.month()+1).padStart(2,"0")} ${String(t.hour()).padStart(2,"0")}:${String(t.minute()).padStart(2,"0")}`
}
function insideBusinessHours(d,mins){
  const t=dayjs(d)
  if(!WORK_DAYS.includes(t.day())) return false
  const end=t.add(mins,"minute"); if(!t.isSame(end,"day")) return false
  const s=t.hour()*60+t.minute(), e=end.hour()*60+end.minute()
  return s >= OPEN.start*60 && e <= OPEN.end*60
}
function nextOpeningFrom(d){
  let t=d.clone()
  const nowMin=t.hour()*60+t.minute(), openMin=OPEN.start*60, closeMin=OPEN.end*60
  if(nowMin<openMin) t=t.hour(OPEN.start).minute(0).second(0)
  if(nowMin>=closeMin) t=t.add(1,"day").hour(OPEN.start).minute(0).second(0)
  while(!WORK_DAYS.includes(t.day())) t=t.add(1,"day").hour(OPEN.start).minute(0).second(0)
  return t
}

// ===== Staff (solo si cliente pide “con <nombre>”)
function parseEmployees(){
  const out=[]
  for(const [k,v] of Object.entries(process.env)){
    if(!k.startsWith("SQ_EMP_")) continue
    const [id, tag] = String(v||"").split("|").map(x=>x?.trim())
    if(!id) continue
    const label = k.replace(/^SQ_EMP_/,"").replace(/_/g," ")
    const tokens = new Set(norm(label).split(" ").filter(Boolean))
    out.push({ id, label, short: label.split(" ")[0], tokens, bookable: (tag||"BOOKABLE").toUpperCase()!=="OFF" })
  }
  return out
}
const EMPLOYEES = parseEmployees()
function staffLabelFromId(id){ return EMPLOYEES.find(e=>e.id===id)?.short || "Equipo" }
const STAFF_ALIASES = [
  ["ganna","gana","ana","anna"],
  ["patri","patricia","paty"],
  ["cristi","cristina"],
  ["johana","joana","yohana"],
  ["tania","tani"],
  ["maria","maría","ma ria"],
  ["elisabeth","elisabet","elis"],
  ["chabely","chabeli","chabelí"],
  ["edurne","edur"]
]
const aliasCanonical = new Map(); for(const arr of STAFF_ALIASES){ for(const a of arr) aliasCanonical.set(a, arr[0]) }
function resolveStaffFromText(text){
  const t = " " + norm(text) + " "
  const m = t.match(/\scon\s+([a-zñáéíóúüï ]{2,})\b/i)
  const rawToken = m ? norm(m[1]) : null
  const candidates = []
  if(rawToken){ candidates.push(rawToken, ...rawToken.split(" ").filter(Boolean)) }
  for(const [alias,_] of aliasCanonical.entries()){ if(t.includes(" "+alias+" ")) candidates.push(alias) }
  const canon = candidates.map(x=>aliasCanonical.get(x)||x)
  for(const c of canon){
    for(const e of EMPLOYEES){
      if(e.tokens.has(c)) return { id:e.id, label:e.short }
    }
  }
  return null
}

// ===== Servicios + duración real
function servicesForSede(sedeKey){
  const prefix = (sedeKey==="la_luz") ? "SQ_SVC_luz_" : "SQ_SVC_"
  const out=[]
  const seen = new Set()
  for(const [k,v] of Object.entries(process.env)){
    if(!k.startsWith(prefix)) continue
    const [id,ver] = String(v||"").split("|")
    if(!id) continue
    const label = k.replace(prefix,"").replace(/_/g," ").replace(/\b\w/g, m=>m.toUpperCase()).replace(/\bLuz\b/i,"").trim()
    const key = `${label}::${id}`; if(seen.has(key)) continue
    seen.add(key)
    out.push({ key:k, id, version: ver?Number(ver):null, label })
  }
  return out
}
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
function durationMinForEnvKey(envKey){
  if(!envKey) return 60
  const durKey = String(envKey).replace(/^SQ_SVC/, "SQ_DUR")
  const v = Number(process.env[durKey] || "")
  return Number.isFinite(v) && v>0 ? v : 60
}

// ===== SQLite holds (bloqueo 6h)
const db = new Database("gapink_holds.db")
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
CREATE INDEX IF NOT EXISTS idx_holds_phone   ON holds (phone, status, expires_at);
`)
const insertHold = db.prepare(`INSERT INTO holds (phone,sede,env_service_key,start_iso,end_iso,staff_id,created_at,expires_at,status)
VALUES (@phone,@sede,@env_service_key,@start_iso,@end_iso,@staff_id,@created_at,@expires_at,'active')`)
const findOverlap = db.prepare(`
SELECT id FROM holds
WHERE status='active'
  AND sede=@sede
  AND expires_at > @now
  AND NOT( end_iso <= @start_iso OR start_iso >= @end_iso )
`)
const expireOld = db.prepare(`UPDATE holds SET status='expired' WHERE status='active' AND expires_at<=@now`)
const releaseByPhone = db.prepare(`UPDATE holds SET status='released' WHERE phone=@phone AND status='active'`)
function cleanupHolds(){ try{ expireOld.run({ now: new Date().toISOString() }) }catch(e){ if(BOT_DEBUG) console.error(e) } }
function hasActiveOverlap({ sede, startISO, endISO }){
  try{ return !!findOverlap.get({ sede, start_iso:startISO, end_iso:endISO, now:new Date().toISOString() }) }catch{ return false }
}
function createHold({ phone, sede, envServiceKey, startISO, endISO, staffId }){
  try{
    const exp = dayjs().add(HOLD_HOURS,"hour").toISOString()
    insertHold.run({ phone, sede, env_service_key: envServiceKey, start_iso:startISO, end_iso:endISO, staff_id:staffId||null, created_at:new Date().toISOString(), expires_at:exp })
    return true
  }catch(e){ if(BOT_DEBUG) console.error(e); return false }
}
function releaseActiveHoldsByPhone(phone){ try{ releaseByPhone.run({ phone }) }catch(e){ if(BOT_DEBUG) console.error(e) } }

// ===== Mensajes breves
const MSG = {
  greet: `¡Hola! Soy el asistente de Gapink Nails 💅\nDime *salón* (Torremolinos/La Luz) y *qué te haces* (ej. “cejas con hilo”).`,
  askSalon: `¿Salón? *Torremolinos* o *La Luz*.`,
  askDetail: `Cuéntame un poco más (ej. “cejas con hilo”, “laminación de cejas”, “microblading”).`,
  self: `Para ver/editar/cancelar usa el enlace del SMS/email ✅`,
  propose(label, sede, showNames){ return `${label} en ${locationNice(sede)}:\n${showNames?`Huecos con ${showNames}:\n`:`Huecos disponibles:\n`}` },
  pickHint: `Dime en texto: “la de las 12”, “viernes tarde”, “otra”…`,
  heldSummary(label, sede, when, dur){ return `He bloqueado **${label}** en **${locationNice(sede)}** para **${fmtES(when)}** (${dur} min).\nUna compañera te da el OK ✅` },
  noSlots: `No veo huecos ahora. Dime otra fecha/franja y lo busco 🔁`
}

// ===== Sesión (memoria corta con historial para dar más contexto a la IA)
const SESS = new Map()
function getS(phone){
  return SESS.get(phone) || SESS.set(phone,{
    greetedAt:0,
    sede:null,
    topic:null,
    shortlist:[],
    svcKey:null, svcLabel:null,
    prefStaffName:null, prefStaffId:null,
    lastSlots:[], prompted:false, lastListAt:0,
    pickedISO:null, pickedDurMin:60,
    snoozeUntil:0,
    lastAsked:null,
    history:[], // {role:"user"|"assistant", text}
    phone
  }).get(phone)
}
function pushHistory(s, role, text){
  s.history.push({ role, text: String(text).slice(0,400) })
  if(s.history.length>10) s.history = s.history.slice(-10)
}

// ===== IA: Router/Shortlist/Choose/Pick (con confianza y escalado)
const TOPIC_VALUES = ["cejas","pestañas","uñas","faciales","depilación facial","depilación corporal","otros"]

function routerSystem(){
  const now = nowEU().format("YYYY-MM-DD HH:mm")
  return `Responde SOLO JSON.
Hoy: ${now} Europe/Madrid.
Objetivo: entender petición de cita en lenguaje natural (nada de números).
Campos:
- intent: "book"|"view"|"edit"|"cancel"|"info"|"other"
- sede: "torremolinos"|"la_luz"|null
- topic: ${TOPIC_VALUES.join("|")}
- staff_name: string|null  // si dice "con <nombre>"
- date: {type:"none"|"day", day: "YYYY-MM-DD"|null, part:"mañana"|"tarde"|"noche"|null}
- service_hint: string|null // texto libre tipo “cejas con hilo”
- confidence: number  // 0..1 tu seguridad total

Reglas:
- Entiende “la primera”, “otra tarde”, “cuando puedas por la mañana”.
- Si no hay sala ni tema, intenta deducirlos. Si dudas, deja null y baja confidence.
- Si detectas “con <nombre>”, rellena staff_name con el nombre tal cual.
- No inventes.`
}

async function aiRouter(s, text){
  const msgs = [
    { role:"system", content: routerSystem() },
    ...s.history.map(h=>({ role:h.role, content:h.text })),
    { role:"user", content: text }
  ]
  // LITE
  let out = await aiCall(msgs,{ maxTokens: TOKENS.router_lite, temperature:0.1 })
  let obj = stripJSON(out) || {}
  if((obj.confidence||0) < 0.6){
    // BOOST (más tokens si duda)
    out = await aiCall(msgs,{ maxTokens: TOKENS.router_boost, temperature:0.2 })
    obj = stripJSON(out) || obj
  }
  // saneo mínimo
  if(obj && typeof obj==="object"){
    if(!TOPIC_VALUES.includes(obj.topic)) obj.topic="otros"
    if(obj.sede!=="torremolinos" && obj.sede!=="la_luz") obj.sede=null
    if(!obj.date) obj.date={type:"none",day:null,part:null}
  }
  return obj
}

function shortlistSystem(labels, topic){
  return `Solo JSON. Devuelve {"labels":[hasta 5]} con nombres EXACTOS sacados de esta lista:
${labels.join(" | ")}
Tema sugerido: ${topic||"auto"}.
Elige opciones populares y claras del tema (no repitas, nada ambiguo).`
}
async function aiShortlist(s, userText){
  const labels = servicesForSede(s.sede).map(x=>x.label)
  const msgs = [
    { role:"system", content: shortlistSystem(labels, s.topic) },
    ...s.history.map(h=>({ role:h.role, content:h.text })),
    { role:"user", content: userText }
  ]
  const out = await aiCall(msgs,{ maxTokens: TOKENS.shortlist, temperature:0.2 })
  const obj = stripJSON(out) || {}
  const arr = Array.isArray(obj.labels) ? obj.labels.filter(l=>labels.some(x=>x.toLowerCase()===String(l).toLowerCase())) : []
  return (arr.length?arr:[]).slice(0,5)
}

async function aiChooseFromShortlist(s, userText, shortlist){
  const sys = `Solo JSON. Lista: ${shortlist.join(" | ")}.
Si el texto es genérico (“la primera”, “cejas”), escoge la opción base MÁS habitual.
Devuelve {"pick":"<etiqueta|null>", "confidence":0..1}`
  const msgs = [
    { role:"system", content: sys },
    ...s.history.map(h=>({ role:h.role, content:h.text })),
    { role:"user", content: userText }
  ]
  const out = await aiCall(msgs,{ maxTokens: TOKENS.choose, temperature:0.2 })
  const obj = stripJSON(out) || {}
  const pick = shortlist.find(l=>l.toLowerCase()===String(obj.pick||"").toLowerCase()) || null
  const conf = typeof obj.confidence==="number" ? obj.confidence : 0.5
  return { pick, confidence: conf }
}

function pickSystem(slotsCompact){
  return `Solo JSON. Elige "iso" según frases tipo “la del martes”, “a las 13”, “otra tarde”, “la primera”.
slots=${JSON.stringify(slotsCompact)}
Devuelve {"iso":"YYYY-MM-DDTHH:mm|null", "confidence":0..1}`
}
async function aiPick(s, userText, slots){
  if(!slots?.length) return { iso:null, confidence:0 }
  const compact = slots.map(x=>{
    const d=x.date.tz(EURO_TZ)
    return { iso:d.format("YYYY-MM-DDTHH:mm"), dow:["do","lu","ma","mi","ju","vi","sa"][d.day()], ddmm:`${String(d.date()).padStart(2,"0")}/${String(d.month()+1).padStart(2,"0")}`, hhmm:`${String(d.hour()).padStart(2,"0")}:${String(d.minute()).padStart(2,"0")}` }
  })
  const msgs = [
    { role:"system", content: pickSystem(compact) },
    ...s.history.map(h=>({ role:h.role, content:h.text })),
    { role:"user", content: userText }
  ]
  const out = await aiCall(msgs,{ maxTokens: TOKENS.pick, temperature:0.2 })
  const obj = stripJSON(out) || {}
  return { iso: obj.iso || null, confidence: typeof obj.confidence==="number"?obj.confidence:0.5 }
}

// ===== Disponibilidad por servicio (dur + holds + staff opcional)
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
  try{
    const r = await square.bookingsApi.searchAvailability(body)
    avail = r?.result?.availabilities || []
  }catch(e){ if(BOT_DEBUG) console.error(e) }
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
    if(out.length>=600) break
  }
  return out.sort((x,y)=>x.date.valueOf()-y.date.valueOf())
}

// ===== Presentación (no nombres salvo que lo pidan)
function bullets(slots, {showNames=false, name=null}={}){
  return slots.map(s=>{
    const base = `• ${fmtES(s.date)}`
    return (showNames && name) ? `${base} — ${name}` : base
  }).join("\n")
}
async function proposeHoursForService(s, jid, phone, sock, envKey, label, aiDate){
  const base = nextOpeningFrom(nowEU().add(NOW_MIN_OFFSET_MIN,"minute"))
  let start = base.clone(), end = base.clone().add(SEARCH_WINDOW_DAYS, "day"), part=null
  if (aiDate && aiDate.type==="day" && aiDate.day){
    const d = dayjs.tz(aiDate.day + " 09:00", EURO_TZ)
    start = d.clone().hour(OPEN.start).minute(0)
    end   = d.clone().hour(OPEN.end).minute(0)
    part  = aiDate.part || null
  } else if (aiDate && aiDate.part){ part = aiDate.part }

  let slots = await searchAvail({ sede:s.sede, envKey, startEU:start, endEU:end, part })
  if(!slots.length && part){
    slots = await searchAvail({ sede:s.sede, envKey, startEU:start, endEU:end, part:null })
  }
  if(!slots.length){
    const start2 = base.clone(), end2 = base.clone().add(EXTENDED_WINDOW_DAYS, "day")
    slots = await searchAvail({ sede:s.sede, envKey, startEU:start2, endEU:end2, part:null })
  }
  // Filtro por profesional si lo pidieron
  let usedPreferred = false
  if(s.prefStaffId){
    const keep = slots.filter(x => x.staffId === s.prefStaffId)
    if(keep.length){ slots = keep; usedPreferred = true }
  }
  if(!slots.length) return false

  const shown = slots.slice(0, SHOW_TOP_N)
  s.lastSlots = shown
  s.prompted  = true
  s.lastListAt = Date.now()
  s.svcKey = envKey
  s.svcLabel = label
  // Mensaje
  const title = MSG.propose(label, s.sede, usedPreferred ? (s.prefStaffName || staffLabelFromId(s.prefStaffId)) : null)
  const list = bullets(shown, { showNames: usedPreferred, name: usedPreferred ? (s.prefStaffName || staffLabelFromId(s.prefStaffId)) : null })
  await sock.sendMessage(jid,{text:`${title}${list}\n\n${MSG.pickHint}`})
  s.lastAsked="hours"
  return true
}
async function proposeHoursWithFallback(s, jid, phone, sock, aiDate, userTextForShortlist){
  if(s.svcKey){
    const ok = await proposeHoursForService(s, jid, phone, sock, s.svcKey, s.svcLabel, aiDate)
    if(ok) return true
  }
  // probar shortlist (AI), sin listar infinito
  if(!s.shortlist.length){
    const list = await aiShortlist(s, userTextForShortlist || (s.topic||""))
    s.shortlist = list
  }
  // intenta pick automático
  if(s.shortlist.length){
    const { pick } = await aiChooseFromShortlist(s, userTextForShortlist || (s.topic||""), s.shortlist)
    const chosen = pick || s.shortlist[0]
    const key = labelToEnvKey(chosen, s.sede)
    if(key){
      const ok = await proposeHoursForService(s, jid, phone, sock, key, chosen, aiDate)
      if(ok) return true
    }
    // prueba alternativas restantes
    for(const alt of s.shortlist){
      if(alt.toLowerCase()===chosen.toLowerCase()) continue
      const k = labelToEnvKey(alt, s.sede); if(!k) continue
      const ok = await proposeHoursForService(s, jid, phone, sock, k, alt, aiDate)
      if(ok) return true
    }
  }
  await sock.sendMessage(jid,{text:MSG.noSlots})
  s.lastAsked="noSlots"
  return false
}

// ===== Mini web QR
const app = express()
let lastQR = null, conectado = false
app.get("/", (_req,res)=>{
  res.send(`<!doctype html><meta charset="utf-8"><style>
  :root{color-scheme:light dark}
  body{font-family:system-ui,Inter,Segoe UI,Roboto;display:grid;place-items:center;min-height:100vh;margin:0;background:Canvas}
  .card{max-width:760px;width:92vw;padding:28px 24px;border-radius:16px;box-shadow:0 10px 30px rgba(0,0,0,.12);background:CanvasTextBlend}
  .row{display:flex;gap:8px;align-items:center;margin:8px 0}
  .pill{padding:6px 10px;border-radius:999px;font-weight:600;font-size:12px}
  .ok{background:#d1fae5;color:#065f46}.bad{background:#fee2e2;color:#991b1b}.warn{background:#fef3c7;color:#92400e}
  .qr{margin:16px 0;text-align:center}
  footer{opacity:.6;font-size:12px;margin-top:12px}
  </style>
  <div class="card">
    <h1>Gapink Nails · Bot</h1>
    <div class="row">
      <span class="pill ${conectado?'ok':'bad'}">${conectado?'✅ WhatsApp conectado':'❌ WhatsApp desconectado'}</span>
      <span class="pill warn">IA DeepSeek · Solo consulta</span>
    </div>
    ${!conectado && lastQR ? `<div class="qr"><img src="/qr.png" width="280" alt="QR WhatsApp" style="border-radius:12px"/></div>` : `<p style="opacity:.8;margin:12px 0">Escanea el QR (también sale en consola) o espera reconexión.</p>`}
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

// ===== Baileys (import dinámico ESM)
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
    if (qr){ lastQR = qr; conectado = false; try{ qrcodeTerminal.generate(qr,{small:true}) }catch{} }
    if (connection==="open"){ lastQR=null; conectado=true }
    if (connection==="close"){ conectado=false; setTimeout(()=>startBot().catch(console.error), 2500) }
  })

  sock.ev.on("messages.upsert", async ({messages})=>{
    const m = messages?.[0]; if(!m?.message) return
    const jid = m.key.remoteJid
    const isFromMe = !!m.key.fromMe
    const phone = (jid||"").split("@")[0]
    const textRaw = (m.message.conversation || m.message.extendedTextMessage?.text || m.message?.imageMessage?.caption || "").trim()
    if(!textRaw) return

    // Puntitos → silencio 6h (si lo envío yo, sin respuesta)
    if(/^[\s.·•⋅]+$/.test(textRaw)){
      const s=getS(phone); s.snoozeUntil=nowEU().add(6,"hour").valueOf(); SESS.set(phone,s)
      if(!isFromMe) return; else return
    }
    if(isFromMe) return

    const s = getS(phone)
    if(s.snoozeUntil && Date.now()<s.snoozeUntil) return

    pushHistory(s,"user",textRaw)

    // Saludo cada 24h máximo
    if(Date.now()-s.greetedAt > 24*60*60*1000){
      s.greetedAt = Date.now(); SESS.set(phone,s)
      await sock.sendMessage(jid,{text:MSG.greet})
      pushHistory(s,"assistant",MSG.greet)
    }

    // Si ya listamos horas y responde con texto → IA elige y bloquea
    if(s.lastSlots?.length && !s.pickedISO){
      const picked = await aiPick(s, textRaw, s.lastSlots)
      const iso = picked?.iso || null
      if(iso){
        const hit = s.lastSlots.find(x=>x.date.format("YYYY-MM-DDTHH:mm")===iso)
        if(hit){
          const durMin = hit.durMin || durationMinForEnvKey(s.svcKey)
          const startISO = hit.date.tz("UTC").toISOString()
          const endISO   = hit.date.clone().add(durMin,"minute").tz("UTC").toISOString()
          cleanupHolds()
          if (hasActiveOverlap({ sede:s.sede, startISO, endISO })){
            const warn = "Ese hueco se acaba de bloquear. Te paso otras opciones:"
            await sock.sendMessage(jid,{text:warn})
            pushHistory(s,"assistant",warn)
            await proposeHoursWithFallback(s, jid, phone, sock, { type:"none", day:null, part:null }, textRaw)
            return
          }
          const ok = createHold({ phone, sede:s.sede, envServiceKey:s.svcKey, startISO, endISO, staffId: hit.staffId||null })
          if(!ok){
            const msg="No he podido bloquear ese hueco. Te paso alternativas:"
            await sock.sendMessage(jid,{text:msg})
            pushHistory(s,"assistant",msg)
            await proposeHoursWithFallback(s, jid, phone, sock, { type:"none", day:null, part:null }, textRaw)
            return
          }
          s.pickedISO = iso
          s.pickedDurMin = durMin
          SESS.set(phone,s)
        }
      }
    }

    // IA Router
    const ai = await aiRouter(s, textRaw)

    // Sede / tema
    if(ai.sede==="la_luz" || ai.sede==="torremolinos"){ s.sede = ai.sede }
    if(ai?.topic && TOPIC_VALUES.includes(ai.topic)){ s.topic = ai.topic }

    // Profesional (solo si lo pide)
    let staffChanged = false
    if(ai?.staff_name || /\bcon\s+[a-zñáéíóúüï ]{2,}/i.test(norm(textRaw))){
      const r = resolveStaffFromText(ai?.staff_name ? `con ${ai.staff_name}` : textRaw)
      if(r?.id && s.prefStaffId!==r.id){ s.prefStaffId=r.id; s.prefStaffName=r.label; staffChanged=true }
      if(!r && ai?.staff_name){
        const msg=`No tengo a “${ai.staff_name}” en el equipo. Te paso huecos del equipo 👇`
        await sock.sendMessage(jid,{text:msg}); pushHistory(s,"assistant",msg)
        s.prefStaffId=null; s.prefStaffName=null
      }
    }
    SESS.set(phone,s)

    // Autogestión → enlace; si “ver” pone silencio 6h
    if(ai.intent==="view" || ai.intent==="edit" || ai.intent==="cancel" || ai.intent==="info"){
      await sock.sendMessage(jid,{text:MSG.self})
      pushHistory(s,"assistant",MSG.self)
      if(ai.intent==="view"){ s.snoozeUntil = nowEU().add(6,"hour").valueOf(); SESS.set(phone,s) }
      return
    }

    // Falta sede → pregúntalo una vez (no repetir)
    if(!s.sede){
      if(s.lastAsked!=="sede"){
        await sock.sendMessage(jid,{text:MSG.askSalon}); pushHistory(s,"assistant",MSG.askSalon); s.lastAsked="sede"
      }
      return
    }

    // Si cambian profesional y había hold, liberar y recalcular
    if(staffChanged && s.pickedISO){
      releaseActiveHoldsByPhone(phone)
      s.pickedISO = null
      s.lastSlots = []
      const msg = `Ok, lo miro con *${s.prefStaffName}* 👌`
      await sock.sendMessage(jid,{text:msg}); pushHistory(s,"assistant",msg)
      await proposeHoursWithFallback(s, jid, phone, sock, ai?.date||{type:"none",day:null,part:null}, textRaw)
      return
    }

    // Si no hay servicio elegido aún → shortlist IA (máx 5, sin listar infinito)
    if(!s.svcKey){
      if(!s.shortlist.length){
        s.shortlist = await aiShortlist(s, ai?.service_hint || textRaw)
      }
      if(s.shortlist.length){
        const { pick, confidence } = await aiChooseFromShortlist(s, ai?.service_hint || textRaw, s.shortlist)
        if(pick && confidence>=0.55){
          const key = labelToEnvKey(pick, s.sede)
          if(key){ s.svcKey=key; s.svcLabel=labelFromEnvKey(key) }
        }else{
          const lines = s.shortlist.map(l=>{
            const env = labelToEnvKey(l, s.sede)
            const dur = durationMinForEnvKey(env)
            return `• ${l} — ${dur} min`
          }).join("\n")
          const msg = `En ${locationNice(s.sede)}, ¿cuál prefieres?\n${lines}\n(Dímelo tal cual en texto, p. ej. “${s.shortlist[0]}” o “la primera”)`
          await sock.sendMessage(jid,{text:msg}); pushHistory(s,"assistant",msg); s.lastAsked="shortlist"
          return
        }
      }else{
        await sock.sendMessage(jid,{text:MSG.askDetail}); pushHistory(s,"assistant",MSG.askDetail); s.lastAsked="detail"
        return
      }
    }

    // Proponer horas si todavía no se han mostrado o han pasado 2 min
    if(s.svcKey && (!s.prompted || (Date.now()-s.lastListAt>2*60*1000))){
      await proposeHoursWithFallback(s, jid, phone, sock, ai?.date||{type:"none",day:null,part:null}, textRaw)
      return
    }

    // Si ya hay hold → resumen final (solo una vez)
    if(s.pickedISO){
      const when = dayjs.tz(s.pickedISO, EURO_TZ)
      const msg = MSG.heldSummary(s.svcLabel, s.sede, when, s.pickedDurMin)
      await sock.sendMessage(jid,{text:msg}); pushHistory(s,"assistant",msg)
      // reset suave de la parte de horas (mantenemos sede/topic)
      s.lastSlots=[]; s.prompted=false; s.pickedISO=null; s.lastAsked="summary"
      SESS.set(phone,s)
      return
    }

    // Fallback amable sin repetir
    if(s.lastAsked!=="greet"){
      await sock.sendMessage(jid,{text:MSG.greet}); pushHistory(s,"assistant",MSG.greet); s.lastAsked="greet"
    }
  })
}

// ===== Arranque + mini web QR
const appListen = app.listen(PORT, ()=>{ 
  console.log(`🩷 Gapink Nails Bot v37.4.0 — DeepSeek-only · IA inteligente · QR http://localhost:${PORT}`)
  startBot().catch(console.error)
})
process.on("SIGTERM", ()=>{ try{ appListen.close(()=>process.exit(0)) }catch{ process.exit(0) } })
process.on("SIGINT", ()=>{ try{ appListen.close(()=>process.exit(0)) }catch{ process.exit(0) } })
