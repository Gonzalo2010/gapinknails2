// index.js — Gapink Nails · v32.0.0 (backend-first, sin crear en Square)
// Cambios clave (modo “para analfabetos”):
// - NO se crea reserva en Square. Solo se consulta disponibilidad.
// - Reserva en DB con status "pending_manual" y bloquea el hueco para el resto.
// - Editar/Cancelar/Otra gestión → mensaje “una empleada se pondrá…” + silencio 6h.
// - IA ultra-corta y barata para adivinar servicio desde la lista (si no ponen número).
// - Prompts recortados y max_tokens bajos.
// - Búsqueda de citas adaptada desde v31.6.4: ventana extendida 30 días, limit=500,
//   filtro por profesional preferida y fallback a próxima semana. + Filtro local de
//   bloqueos en DB para que no ofrezca huecos ya retenidos.

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

if (!globalThis.crypto) globalThis.crypto = webcrypto
dayjs.extend(utc); dayjs.extend(tz); dayjs.extend(isoWeek); dayjs.locale("es")
const EURO_TZ = "Europe/Madrid"

// ====== Config horario (simple y claro)
const WORK_DAYS = [1,2,3,4,5] // L–V
const SLOT_MIN = 30
const OPEN = { start: 9, end: 20 }
const NOW_MIN_OFFSET_MIN = Number(process.env.BOT_NOW_OFFSET_MIN || 30)
const SEARCH_WINDOW_DAYS = Number(process.env.BOT_SEARCH_WINDOW_DAYS || 30)
const HOLIDAYS_EXTRA = (process.env.HOLIDAYS_EXTRA || "06/01,28/02,15/08,12/10,01/11,06/12,08/12,25/12")
  .split(",").map(s=>s.trim()).filter(Boolean)
// — número máx. de huecos que mostramos
const SHOW_TOP_N = Number(process.env.SHOW_TOP_N || 5)

// ====== Flags
const BOT_DEBUG = /^true$/i.test(process.env.BOT_DEBUG || "")
const DRY_RUN = /^true$/i.test(process.env.DRY_RUN || "")
const SQUARE_MAX_RETRIES = Number(process.env.SQUARE_MAX_RETRIES || 3)

// ====== Square (solo para disponibilidad / clientes / búsquedas)
const square = new Client({
  accessToken: process.env.SQUARE_ACCESS_TOKEN,
  environment: (process.env.SQUARE_ENV==="production") ? Environment.Production : Environment.Sandbox
})
const LOC_TORRE = (process.env.SQUARE_LOCATION_ID_TORREMOLINOS || "").trim()
const LOC_LUZ   = (process.env.SQUARE_LOCATION_ID_LA_LUZ || "").trim()
const ADDRESS_TORRE = process.env.ADDRESS_TORREMOLINOS || "Av. de Benyamina 18, Torremolinos"
const ADDRESS_LUZ   = process.env.ADDRESS_LA_LUZ || "Málaga – Barrio de La Luz"

// ====== IA (Deepseek / OpenAI) — costes bajos
const AI_PROVIDER = (process.env.AI_PROVIDER || (process.env.DEEPSEEK_API_KEY? "deepseek" : process.env.OPENAI_API_KEY? "openai" : "none")).toLowerCase()
const DEEPSEEK_API_KEY = process.env.DEEPSEEK_API_KEY || ""
const DEEPSEEK_MODEL = process.env.DEEPSEEK_MODEL || "deepseek-chat"
const OPENAI_API_KEY = process.env.OPENAI_API_KEY || ""
const OPENAI_MODEL = process.env.OPENAI_MODEL || "gpt-4o-mini"
const AI_TIMEOUT_MS = Number(process.env.AI_TIMEOUT_MS || 12000)
const AI_MAX_TOKENS = Number(process.env.AI_MAX_TOKENS || 256) // bajito
const sleep = ms => new Promise(r=>setTimeout(r, ms))

async function aiChat(system, user, extraMsgs=[]){
  if (AI_PROVIDER==="none") return null
  const controller = new AbortController()
  const timeout = setTimeout(()=>controller.abort(), AI_TIMEOUT_MS)
  try{
    const messages = [
      system ? { role:"system", content: system } : null,
      ...extraMsgs,
      { role:"user", content: user }
    ].filter(Boolean)
    if (AI_PROVIDER==="deepseek"){
      const resp = await fetch("https://api.deepseek.com/chat/completions",{
        method:"POST",
        headers:{ "Content-Type":"application/json", "Authorization":`Bearer ${DEEPSEEK_API_KEY}` },
        body: JSON.stringify({ model: DEEPSEEK_MODEL, messages, temperature:0.1, max_tokens:AI_MAX_TOKENS }),
        signal: controller.signal
      })
      clearTimeout(timeout)
      if (!resp.ok) return null
      const data = await resp.json()
      return data?.choices?.[0]?.message?.content || null
    } else {
      const resp = await fetch("https://api.openai.com/v1/chat/completions",{
        method:"POST",
        headers:{ "Content-Type":"application/json", "Authorization":`Bearer ${OPENAI_API_KEY}` },
        body: JSON.stringify({ model: OPENAI_MODEL, messages, temperature:0.1, max_tokens:AI_MAX_TOKENS }),
        signal: controller.signal
      })
      clearTimeout(timeout)
      if (!resp.ok) return null
      const data = await resp.json()
      return data?.choices?.[0]?.message?.content || null
    }
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
const onlyDigits = s => String(s||"").replace(/\D+/g,"")
const rm = s => String(s||"").normalize("NFD").replace(/\p{Diacritic}/gu,"")
const norm = s => rm(s).toLowerCase().replace(/[+.,;:()/_-]/g," ").replace(/[^\p{Letter}\p{Number}\s]/gu," ").replace(/\s+/g," ").trim()
function stableKey(parts){ const raw=Object.values(parts).join("|"); return createHash("sha256").update(raw).digest("hex").slice(0,48) }
const nowEU = ()=>dayjs().tz(EURO_TZ)

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
function titleCase(str){ return String(str||"").toLowerCase().replace(/\b([a-z])/g, (m)=>m.toUpperCase()) }
function cleanDisplayLabel(label){
  const s = String(label||"").replace(/^\s*(luz|la\s*luz)\s+/i,"").trim()
  return applySpanishDiacritics(s)
}
function normalizePhoneES(raw){
  const d=onlyDigits(raw); if(!d) return null
  if (raw.startsWith("+") && d.length>=8 && d.length<=15) return `+${d}`
  if (d.startsWith("34") && d.length===11) return `+${d}`
  if (d.length===9) return `+34${d}`
  if (d.startsWith("00")) return `+${d.slice(2)}`
  return `+${d}`
}
function parseToEU(input){
  if (dayjs.isDayjs(input)) return input.clone().tz(EURO_TZ)
  const s = String(input||"")
  if (/[Zz]|[+\-]\d{2}:?\d{2}$/.test(s)) return dayjs(s).tz(EURO_TZ)
  return dayjs.tz(s, EURO_TZ)
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
function enumerateHours(list){ return list.map((d,i)=>({ index:i+1, iso:d.format("YYYY-MM-DDTHH:mm"), pretty:fmtES(d) })) }
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
  status TEXT,                 -- "pending_manual" | "confirmed" | "failed"
  created_at TEXT,
  square_booking_id TEXT,
  square_error TEXT,
  retry_count INTEGER DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_appt_block ON appointments(location_key, start_iso, status);
CREATE TABLE IF NOT EXISTS sessions (
  phone TEXT PRIMARY KEY,
  data_json TEXT,
  updated_at TEXT
);
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
const insertAppt = db.prepare(`INSERT INTO appointments
(id,customer_name,customer_phone,customer_square_id,location_key,service_env_key,service_label,duration_min,start_iso,end_iso,staff_id,status,created_at,square_booking_id,square_error,retry_count)
VALUES (@id,@customer_name,@customer_phone,@customer_square_id,@location_key,@service_env_key,@service_label,@duration_min,@start_iso,@end_iso,@staff_id,@status,@created_at,@square_booking_id,@square_error,@retry_count)`)

const insertSquareLog = db.prepare(`INSERT INTO square_logs
(phone, action, request_data, response_data, error_data, timestamp, success)
VALUES (@phone, @action, @request_data, @response_data, @error_data, @timestamp, @success)`)

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

// ====== Logging helper
function logEvent({direction, action, phone, intent=null, stage=null, raw_text=null, reply_text=null, extra=null, success=1, error=null}){
  try{
    const req = safeJSONStringify({direction, intent, stage, raw_text, extra})
    const res = safeJSONStringify({reply_text})
    insertSquareLog.run({
      phone: phone || "unknown",
      action: `${direction}_${action || "event"}`,
      request_data: req,
      response_data: res,
      error_data: error ? safeJSONStringify(error) : null,
      timestamp: new Date().toISOString(),
      success: success?1:0
    })
  }catch(e){}
  const tag = direction==="in" ? "[IN]" : direction==="out" ? "[OUT]" : "[SYS]"
  const payload = {
    action: action||null, direction,
    phone, intent, stage,
    raw_text, reply_text,
    time: Date.now(),
    extra
  }
  try{ console.log(JSON.stringify({ message:`${tag}`, attributes:{...payload, level:"info", hostname:process.env.HOSTNAME||"local", pid:process.pid, timestamp:new Date().toISOString()}})) }catch{}
}

// ====== send wrapper
async function sendWithLog(sock, jid, text, {phone, intent, action, stage, extra}={}){
  logEvent({direction:"out", action: action||"send", phone, intent:intent||null, stage:stage||null, raw_text:null, reply_text:text, extra: {payload:{text}, ...(extra||{})}})
  try{
    await sock.sendMessage(jid, { text })
  }catch(e){
    logEvent({direction:"sys", action:"send_error", phone, intent, stage, raw_text:text, error:{message:e?.message}})
  }
}

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
  c.pendingDateTime_ms = s.pendingDateTime? (dayjs.isDayjs(s.pendingDateTime)? s.pendingDateTime.valueOf() : dayjs(s.pendingDateTime).valueOf()) : null
  delete c.lastHours; delete c.pendingDateTime
  const j=JSON.stringify(c)
  const up=db.prepare(`UPDATE sessions SET data_json=@j, updated_at=@u WHERE phone=@p`).run({j,u:new Date().toISOString(),p:phone})
  if (up.changes===0) db.prepare(`INSERT INTO sessions (phone,data_json,updated_at) VALUES (@p,@j,@u)`).run({p:phone,j,u:new Date().toISOString()})
}
function clearSession(phone){ db.prepare(`DELETE FROM sessions WHERE phone=@phone`).run({phone}) }

// ====== Empleadas (global bookable)
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
    const parts = String(v||"").split("|").map(s=>s.trim())
    const id = parts[0]
    if (!id) continue
    const bookTag = (parts[1]||"BOOKABLE").toUpperCase()
    const bookable = ["BOOKABLE","TRUE","YES","1"].includes(bookTag)
    const labels = deriveLabelsFromEnvKey(k)
    out.push({ envKey:k, id, bookable, allow:["ALL"], labels })
  }
  return out
}
let EMPLOYEES = parseEmployees()

function staffLabelFromId(id){
  const e = EMPLOYEES.find(x=>x.id===id)
  return e?.labels?.[0] || (id ? `Prof. ${String(id).slice(-4)}` : null)
}
function isStaffAllowedInLocation(staffId, _locKey){
  const e = EMPLOYEES.find(x=>x.id===staffId)
  return !!(e && e.bookable)
}
function pickStaffForLocation(_locKey, preferId=null){
  if (preferId){
    const e = EMPLOYEES.find(x=>x.id===preferId && x.bookable)
    if (e) return e.id
  }
  const found = EMPLOYEES.find(e=>e.bookable)
  return found?.id || null
}

// ====== Aliases de nombres (clúster)
const NAME_ALIASES = [
  ["patri","patricia"],["patricia","patri"],
  ["cristi","cristina","cristy"],
  ["rocio chica","rociochica","rocio  chica","rocio c","rocio chica"],["rocio","rosio"],
  ["carmen belen","carmen","belen"],["tania","tani"],["johana","joana","yohana"],
  ["ganna","gana","ana","anna"],
  ["ginna","gina"],["chabely","chabeli","chabelí"],["elisabeth","elisabet","elis"],
  ["desi","desiree","desirée"],["daniela","dani"],["jamaica","jahmaica"],["edurne","edur"],
  ["sudemis","sude"],["maria","maría"],["anaira","an aira"],["thalia","thalía","talia","talía"]
]
function findAliasCluster(token){
  for (const arr of NAME_ALIASES){ if (arr.some(a=>token===a)) return arr }
  return null
}

// ====== Fuzzy staff
function fuzzyStaffFromText(text){
  const tnorm = norm(text)
  if (/\b(con el equipo|me da igual|cualquiera|con quien sea|lo que haya)\b/i.test(tnorm)) return { anyTeam:true }
  const t = " " + tnorm + " "
  const m = t.match(/\scon\s+([a-zñáéíóú ]{2,})\b/i)
  let token = m ? norm(m[1]).trim() : null
  if (!token){
    const nm = t.match(/\b(patri|patricia|cristi|cristina|rocio chica|rocio|carmen belen|carmen|belen|ganna|gana|ana|anna|maria|anaira|ginna|daniela|desi|jamaica|johana|edurne|sudemis|tania|chabely|elisabeth|thalia|thalía|talia|talía)\b/i)
    if (nm) token = norm(nm[0])
  }
  if (!token) return null
  const cluster = findAliasCluster(token)
  if (cluster){
    const canonical = cluster[0]
    for (const e of EMPLOYEES){
      for (const lbl of e.labels){
        const nlbl = norm(lbl)
        const re = new RegExp(`(^|\\s)${canonical}(\\s|$)`)
        if (re.test(nlbl)) return e
      }
    }
    for (const e of EMPLOYEES){
      const nlbls = e.labels.map(norm)
      if (cluster.some(alias => nlbls.includes(alias))) return e
    }
    return null
  }
  for (const e of EMPLOYEES){
    for (const lbl of e.labels){
      const nlbl = norm(lbl)
      if (nlbl===token) return e
      const re = new RegExp(`(^|\\s)${token}(\\s|$)`)
      if (re.test(nlbl)) return e
    }
  }
  return null
}

function staffRosterForPrompt(){
  return EMPLOYEES.map(e=> `• ID:${e.id} | Nombres:[${e.labels.join(", ")}] | Reservable:${e.bookable}`).join("\n")
}

// ====== Servicios
function servicesForSedeKeyRaw(sedeKey){
  const prefix = (sedeKey==="la_luz") ? "SQ_SVC_luz_" : "SQ_SVC_"
  const out=[]
  for (const [k,v] of Object.entries(process.env)){
    if (!k.startsWith(prefix)) continue
    const [id] = String(v||"").split("|"); if (!id) continue
    const raw = k.replace(prefix,"").replaceAll("_"," ")
    let label = titleCase(raw)
    label = applySpanishDiacritics(label)
    out.push({ sedeKey, key:k, id, rawKey:k, label: cleanDisplayLabel(label), norm: norm(label) })
  }
  return out
}
function allServices(){ return [...servicesForSedeKeyRaw("torremolinos"), ...servicesForSedeKeyRaw("la_luz")] }
function resolveEnvKeyFromLabelAndSede(label, sedeKey){
  const list = servicesForSedeKeyRaw(sedeKey)
  return list.find(s=>s.label.toLowerCase()===String(label||"").toLowerCase())?.key || null
}
function serviceLabelFromEnvKey(envKey){
  if (!envKey) return null
  const all = allServices()
  return all.find(s=>s.key===envKey)?.label || null
}

// ====== Categorías y filtros (igual que antes)
const CATS = {
  "uñas": (s,u)=> {
    const NEG = /\b(pesta|ceja|facial|labios|eyeliner|micro|blading|laser|endosphere|madero|masaje|vitamina|limpieza|tratamiento)\b/i
    if (NEG.test(s.label)) return false
    const POS = /\b(uñ|manicura|gel|acril|nivel|semiperman|press|tips|francés|frances|pedicura|pies)\b/i
    const pediUser = /\b(pedicur|pie|pies)\b/i.test(norm(u||""))
    const isPediLabel = /\b(pedicur|pie|pies)\b/i.test(norm(s.label))
    if (!pediUser && isPediLabel) return false
    return POS.test(s.label)
  },
  "depilación": (s,_u)=> /\b(depil|fotodepil|axilas|ingles|inglés|labio|fosas|nasales)\b/i.test(s.label),
  "micropigmentación": (s,_u)=> /\b(microblading|microshading|efecto polvo|aquarela|eyeliner|retoque|labios|cejas)\b/i.test(s.label),
  "faciales": (s,_u)=> /\b(limpieza|facial|dermapen|carbon|peel|vitamina|hidra|acne|manchas|colageno|colágeno)\b/i.test(s.label),
  "pestañas": (s,_u)=> /\b(pestañ|pestanas|extensiones|lifting|relleno pesta)\b/i.test(s.label)
}
const CAT_ALIASES = {
  "unas":"uñas","unias":"uñas","unyas":"uñas","depilacion":"depilación","depilacion laser":"depilación",
  "micro":"micropigmentación","micropigmentacion":"micropigmentación","facial":"faciales","pestanas":"pestañas"
}
function parseCategory(text){
  const t = norm(text)
  if (/\buñ|manicura|pedicur|acril|gel|semi|tips|frances/i.test(t)) return "uñas"
  if (/\bdepil|fotodepil|axilas|ingles|labio|fosas/i.test(t)) return "depilación"
  if (/\bmicroblading|microshading|aquarela|eyeliner|retoque|efecto polvo|cejas\b/i.test(t)) return "micropigmentación"
  if (/\blimpieza|facial|dermapen|carbon|peel|vitamina|hidra\b/i.test(t)) return "faciales"
  if (/\bpestañ|pestanas|lifting|extensiones\b/i.test(t)) return "pestañas"
  for (const [k,v] of Object.entries(CAT_ALIASES)){ if (t.includes(k)) return v }
  return null
}
function listServicesByCategory(sedeKey, category, userMsg){
  const all = servicesForSedeKeyRaw(sedeKey)
  const fn = CATS[category]; if (!fn) return []
  const filtered = all.filter(s=>fn(s,userMsg))
  const seen=new Set(); const out=[]
  for (const s of filtered){
    const key = s.label.toLowerCase()
    if (seen.has(key)) continue
    seen.add(key); out.push({ label:s.label, key:s.key, id:s.id })
  }
  return out
}

// ====== Square helpers (buscar cliente / disponibilidad)
async function searchCustomersByPhone(phone){
  try{
    const e164=normalizePhoneES(phone); if(!e164) return []
    const got = await square.customersApi.searchCustomers({ query:{ filter:{ phoneNumber:{ exact:e164 } } } })
    return got?.result?.customers || []
  }catch{ return [] }
}
async function getUniqueCustomerByPhoneOrPrompt(phone, sessionData, sock, jid){
  // Igual que antes: se usa para mostrar nombre en el resumen (no para reservar en Square)
  const matches = await searchCustomersByPhone(phone)
  if (matches.length === 1){
    const c = matches[0]
    sessionData.name = sessionData.name || c?.givenName || null
    sessionData.email = sessionData.email || c?.emailAddress || null
    return { status:"single", customer:c }
  }
  if (matches.length === 0){
    sessionData.stage = "awaiting_identity"
    saveSession(phone, sessionData)
    await sendWithLog(sock, jid, "Para terminar, dime tu *nombre* (y, si quieres, tu *email*).", {phone, intent:"ask_identity", action:"guide", stage:sessionData.stage})
    return { status:"need_new" }
  }
  const choices = matches.map((c,i)=>({
    index:i+1, id:c.id,
    name:c?.givenName || "Sin nombre",
    email:c?.emailAddress || "—"
  }))
  sessionData.identityChoices = choices
  sessionData.stage = "awaiting_identity_pick"
  saveSession(phone, sessionData)
  const lines = choices.map(ch => `${ch.index}) ${ch.name} ${ch.email!=="—" ? `(${ch.email})`:""}`).join("\n")
  await sendWithLog(sock, jid, `He encontrado varias fichas con tu número. ¿Cuál eres?\n\n${lines}\n\nResponde con el número.`, {phone, intent:"ask_identity_pick", action:"guide", stage:sessionData.stage})
  return { status:"need_pick" }
}
async function findOrCreateCustomerWithRetry({ name, email, phone }){
  let lastError = null
  for (let attempt = 1; attempt <= SQUARE_MAX_RETRIES; attempt++) {
    try{
      const e164=normalizePhoneES(phone); if(!e164) return null
      const got = await square.customersApi.searchCustomers({ query:{ filter:{ phoneNumber:{ exact:e164 } } } })
      const c=(got?.result?.customers||[])[0];
      if (c) return c
      const created = await square.customersApi.createCustomer({
        idempotencyKey:`cust_${Date.now()}_${Math.random().toString(36).slice(2,6)}`,
        givenName:name||undefined,
        emailAddress:email||undefined,
        phoneNumber:e164||undefined
      })
      const newCustomer = created?.result?.customer||null
      if (newCustomer) return newCustomer
    } catch(e) { lastError = e; if (attempt < SQUARE_MAX_RETRIES) await sleep(1000 * attempt) }
  }
  return null
}
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

// ====== DISPONIBILIDAD (Square) + FILTRO de bloqueos locales
async function searchAvailWindow({ locationKey, envServiceKey, startEU, endEU, limit=500, part=null }){
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
  }catch{}
  const out=[]
  for (const a of avail){
    if (!a?.startAt) continue
    const d = dayjs(a.startAt).tz(EURO_TZ)
    if (!insideBusinessHours(d,60)) continue
    let tm = null
    const segs = Array.isArray(a.appointmentSegments) ? a.appointmentSegments
                 : Array.isArray(a.segments) ? a.segments
                 : []
    if (segs[0]?.teamMemberId) tm = segs[0].teamMemberId
    if (part){
      const { start, end } = partOfDayWindow(d, part)
      if (!(d.isSame(start,"day") && d.isAfter(start.subtract(1,"minute")) && d.isBefore(end.add(1,"minute")))) continue
    }
    out.push({ date:d, staffId: tm || null })
    if (out.length>=limit) break
  }
  out.sort((a,b)=>a.date.valueOf()-b.date.valueOf())
  return out
}
function partOfDayWindow(dateEU, part){
  let start=dateEU.clone().hour(OPEN.start).minute(0).second(0).millisecond(0)
  let end  =dateEU.clone().hour(OPEN.end).minute(0).second(0).millisecond(0)
  if (part==="mañana") end = dateEU.clone().hour(13).minute(0)
  if (part==="tarde") { start = dateEU.clone().hour(15).minute(0) }
  if (part==="noche") { start = dateEU.clone().hour(18).minute(0) }
  return { start, end }
}

// ---- Bloqueo local: filtrar slots ocupados en DB
function isSlotBlockedInDB({ locationKey, dateEU }){
  const startISO = dateEU.tz("UTC").toISOString()
  const row = db.prepare(`
    SELECT 1
    FROM appointments
    WHERE location_key = @loc
      AND start_iso = @start
      AND status IN ('pending_manual','confirmed')
    LIMIT 1
  `).get({ loc: locationKey, start: startISO })
  return !!row
}
function filterOutBlockedSlots(slots, locationKey){
  return slots.filter(s => !isSlotBlockedInDB({ locationKey, dateEU: s.date }))
}

// ====== Conversación determinista / IA mínima
function parseSede(text){
  const t=norm(text)
  if (/\b(luz|la luz)\b/.test(t)) return "la_luz"
  if (/\b(torre|torremolinos)\b/.test(t)) return "torremolinos"
  return null
}
function parseNameEmailFromText(txt){
  const emailMatch = String(txt||"").match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i)
  const email = emailMatch ? emailMatch[0] : null
  const name = String(txt||"").replace(email||"", "").replace(/(email|correo)[:\s]*/ig,"").trim()
  return { name: name || null, email }
}
function proposeLines(slots, mapIsoToStaff){
  const hoursEnum = enumerateHours(slots.map(s=>s.date))
  const lines = hoursEnum.map(h => {
    const sid = mapIsoToStaff[h.iso] || null
    const tag = sid ? ` — ${staffLabelFromId(sid)}` : ""
    return `${h.index}) ${h.pretty}${tag}`
  }).join("\n")
  return { lines, hoursEnum }
}
function buildGreeting(){
  return `Hola 👋 Soy el asistente de Gapink Nails.
Dime *salón* (Torremolinos o La Luz), *servicio* y te paso *horas*.

Atendemos por WhatsApp y llamadas: L–V 10–14 y 16–20.

Si ya tienes cita y quieres cambiar/cancelar, te ayudamos por aquí.`
}

// Prompt reducido para interpretar intenciones básicas (barato)
function buildSystemPromptMini(){
  return `Eres un parser de WhatsApp. Devuelve SOLO JSON.
Acciones: "set_sede","set_category","set_staff","choose_service_label","propose_times","weekly_schedule","create_booking","list_appointments","cancel_appointment","none".
Formato: {"action":"...","params":{...}}
No expliques nada.`
}
async function aiInterpretMini(textRaw, session){
  if (AI_PROVIDER==="none") return null
  const sys = buildSystemPromptMini()
  const ctx = `Estado: sede=${session.sede||"-"}, cat=${session.category||"-"}, servicio=${session.selectedServiceLabel||"-"}, staff=${session.preferredStaffLabel||"-"}`
  const out = await aiChat(sys, `Mensaje: "${textRaw}"\n${ctx}\nJSON:`)
  return stripToJSON(out)
}

// IA pequeña para elegir servicio desde una lista numerada
async function aiPickServiceFromList(userMsg, choices){
  if (AI_PROVIDER==="none") return null
  const sys = `Elige opción por significado. Devuelve SOLO JSON {"index":N}. Si no estás seguro, {"index":null}.`
  const list = choices.map(c => `${c.index}) ${c.label}`).join("\n")
  const out = await aiChat(sys, `Usuario: "${userMsg}"\nLista:\n${list}\nJSON:`)
  const obj = stripToJSON(out)
  if (!obj) return null
  const n = Number(obj.index)
  if (!Number.isInteger(n)) return null
  const ok = choices.find(c=>c.index===n)
  return ok ? ok : null
}

// ====== Anti-repeat lista servicios
function serviceListSignature({sede, category, list}){
  return stableKey({ sede, category, labels:list.map(i=>i.label).join("|") })
}
function shouldSuppressServiceList(session, sig){
  const lastSig = session.lastServiceListSig || null
  const lastAt = session.lastServiceListAt_ms || 0
  const freshMs = 90 * 1000
  return (lastSig && lastSig===sig && (Date.now()-lastAt) < freshMs)
}
function noteServiceListSignature(session, sig, phone){
  session.lastServiceListSig = sig
  session.lastServiceListAt_ms = Date.now()
  saveSession(phone, session)
}

// ====== Parser de preferencias temporales (del v31.6.4)
function parseTemporalPreference(text){
  const t = norm(text)
  const now = dayjs().tz(EURO_TZ)
  const mapDia = { "lunes":1,"martes":2,"miercoles":3,"miércoles":3,"jueves":4,"viernes":5,"sabado":6,"sábado":6,"domingo":0 }
  let targetDay=null
  for (const k of Object.keys(mapDia)){ if (t.includes(k)) { targetDay = mapDia[k]; break } }
  let when = null
  if (/\bhoy\b/.test(t)) when = now
  else if (/\bmanana\b/.test(t)) when = now.add(1,"day")
  else if (/\bpasado\b/.test(t)) when = now.add(2,"day")
  if (targetDay!=null){ let d = now.clone(); while (d.day() !== targetDay) d = d.add(1,"day"); when = d }
  let part = null
  if (/\bpor la manana\b/.test(t) || (/\bmanana\b/.test(t) && !when)) part="mañana"
  if (/\btarde\b/.test(t)) part="tarde"
  if (/\bnoche\b/.test(t)) part="noche"
  const nextWeek = /\b(pr[oó]xima\s+semana|semana\s+que\s+viene)\b/i.test(t)
  return { when, part, nextWeek }
}

// ====== Búsqueda extendida (del v31.6.4)
async function searchAvailWindowExtended({ locationKey, envServiceKey, startEU, staffId, maxDays=30 }){
  const results = []
  const endDate = startEU.clone().add(maxDays, 'day')
  let currentStart = startEU.clone()
  while (currentStart.isBefore(endDate) && results.length < 1000) {
    let currentEnd = currentStart.clone().add(7, 'day')
    if (currentEnd.isAfter(endDate)) currentEnd = endDate.clone()
    const weekSlots = await searchAvailWindow({ locationKey, envServiceKey, startEU: currentStart, endEU: currentEnd, limit: 500 })
    const filteredSlots = staffId ? weekSlots.filter(s => s.staffId === staffId) : weekSlots
    results.push(...filteredSlots)
    currentStart = currentEnd.clone()
    await sleep(100)
  }
  results.sort((a,b)=>a.date.valueOf()-b.date.valueOf())
  return results
}

// ====== Proponer horas (***PORTADO y MEJORADO***)
// - Trae huecos 30 días con limit=500.
// - Si hay profesional preferida y no hay huecos, busca extendido hasta 30 días solo con esa profesional.
// - Fallback a “próxima semana”.
// - Filtra huecos ya bloqueados en DB (pending_manual / confirmed) para no ofrecerlos.
async function proposeTimes(sessionData, phone, sock, jid, opts={}){
  const now = nowEU();
  const baseFrom = nextOpeningFrom(now.add(NOW_MIN_OFFSET_MIN, "minute"))
  const days = SEARCH_WINDOW_DAYS

  let when=null, part=null
  if (opts.date_hint || opts.part_of_day){
    if (opts.date_hint){
      const p = parseTemporalPreference(String(opts.date_hint))
      when = p.when; part = p.part || opts.part_of_day || null
    } else { part = opts.part_of_day || null }
  } else if (opts.text){
    const p = parseTemporalPreference(opts.text)
    when = p.when; part = p.part
  }

  let startEU = when ? when.clone().hour(OPEN.start).minute(0) : baseFrom.clone()
  let endEU   = when ? when.clone().hour(OPEN.end).minute(0)   : baseFrom.clone().add(days,"day")

  if (!sessionData.sede || !sessionData.selectedServiceEnvKey){
    await sendWithLog(sock, jid, "Necesito primero *salón* y *servicio* para proponerte horas.", {phone, intent:"need_sede_service", action:"guide", stage:sessionData.stage})
    return
  }

  // 1) Traemos huecos crudos
  const rawSlots = await searchAvailWindow({
    locationKey: sessionData.sede,
    envServiceKey: sessionData.selectedServiceEnvKey,
    startEU, endEU, limit: 500, part
  })

  // 2) Filtramos bloqueados por DB
  let slots = filterOutBlockedSlots(rawSlots, sessionData.sede)

  // 3) Profesional preferida
  let usedPreferred = false
  if (sessionData.preferredStaffId){
    let pref = slots.filter(s => s.staffId === sessionData.preferredStaffId)
    usedPreferred = true
    // Si no hay huecos con la preferida, miramos extendido hasta 30 días SOLO para ella
    if (!pref.length){
      const extended = await searchAvailWindowExtended({
        locationKey: sessionData.sede,
        envServiceKey: sessionData.selectedServiceEnvKey,
        startEU: startEU,
        staffId: sessionData.preferredStaffId,
        maxDays: 30
      })
      pref = filterOutBlockedSlots(extended, sessionData.sede)
      if (pref.length){
        slots = pref
      } else {
        // Sin huecos con la preferida → mostramos del equipo
        usedPreferred = false
      }
    } else {
      slots = pref
    }
  }

  slots.sort((a,b)=>a.date.valueOf()-b.date.valueOf())

  // 4) Si no hay huecos, probamos la próxima semana (equipo o preferida si existiera)
  if (!slots.length){
    const startNext = startEU.clone().add(7, "day")
    const endNext   = endEU.clone().add(7, "day")
    const rawNext = await searchAvailWindow({
      locationKey: sessionData.sede,
      envServiceKey: sessionData.selectedServiceEnvKey,
      startEU: startNext, endEU: endNext, limit: 200, part
    })
    let nextSlots = filterOutBlockedSlots(rawNext, sessionData.sede)
    if (sessionData.preferredStaffId){
      const onlyPreferred = nextSlots.filter(s => s.staffId === sessionData.preferredStaffId)
      if (onlyPreferred.length){
        nextSlots = onlyPreferred
        usedPreferred = true
      } else {
        usedPreferred = false
      }
    }
    nextSlots.sort((a,b)=>a.date.valueOf()-b.date.valueOf())
    if (nextSlots.length){
      const shown = nextSlots.slice(0, SHOW_TOP_N)
      const mapN={}; for (const s of shown) mapN[s.date.format("YYYY-MM-DDTHH:mm")] = s.staffId || null
      const { lines } = proposeLines(shown, mapN)
      sessionData.lastHours = shown.map(s => s.date)
      sessionData.lastStaffByIso = mapN
      sessionData.lastProposeUsedPreferred = usedPreferred
      sessionData.stage = "awaiting_time"
      saveSession(phone, sessionData)
      const warn = usedPreferred
        ? `No había huecos inmediatos con ${sessionData.preferredStaffLabel || "tu profesional"}. *La próxima semana* sí hay:`
        : `No había huecos inmediatos. *La próxima semana* sí hay:`
      await sendWithLog(sock, jid, `${warn}\n${lines}\n\nResponde con el número.`, {phone, intent:"times_next_week", action:"guide", stage:sessionData.stage})
      return
    }
  }

  // 5) Nada de nada
  if (!slots.length){
    const msg = when
      ? `No veo huecos para ese día${part?` por la ${part}`:""}. ¿Otra fecha o franja?`
      : `No encuentro huecos en los próximos ${days} días. ¿Otra fecha/franja (ej. “viernes tarde”)?`
    await sendWithLog(sock, jid, msg, {phone, intent:"no_slots", action:"guide"})
    return
  }

  // 6) Presentamos TOP N
  const shown = slots.slice(0, SHOW_TOP_N)
  const map = {}; for (const s of shown) map[s.date.format("YYYY-MM-DDTHH:mm")] = s.staffId || null
  const { lines } = proposeLines(shown, map)

  sessionData.lastHours = shown.map(s => s.date)
  sessionData.lastStaffByIso = map
  sessionData.lastProposeUsedPreferred = usedPreferred
  sessionData.stage = "awaiting_time"
  saveSession(phone, sessionData)

  const header = usedPreferred
    ? `Horarios con ${sessionData.preferredStaffLabel || "tu profesional"} — primeras ${SHOW_TOP_N}:`
    : `Horarios del equipo — primeras ${SHOW_TOP_N}:`
  await sendWithLog(sock, jid, `${header}\n${lines}\n\nResponde con el número.`, {phone, intent:"times_list", action:"guide", stage:sessionData.stage})
}

// ====== HORARIO SEMANAL (igual, filtrando bloqueos locales)
function nextMondayEU(base){ return base.clone().add(1,"week").isoWeekday(1).hour(OPEN.start).minute(0).second(0).millisecond(0) }
async function weeklySchedule(sessionData, phone, sock, jid, opts={}){
  if (!sessionData.sede){
    await sendWithLog(sock, jid, "¿Salón? *Torremolinos* o *La Luz*.", {phone, intent:"ask_sede", action:"guide"})
    return
  }
  if (!sessionData.selectedServiceEnvKey){
    await sendWithLog(sock, jid, "Dime el *servicio* (o la *categoría* para listarte opciones).", {phone, intent:"ask_service", action:"guide"})
    return
  }
  const now = nowEU()
  let startEU = nextOpeningFrom(now.add(NOW_MIN_OFFSET_MIN,"minute"))
  if (opts.nextWeek){ startEU = nextMondayEU(now) }
  const endEU = startEU.clone().add(7,"day").hour(OPEN.end).minute(0)

  const rawSlots = await searchAvailWindow({
    locationKey: sessionData.sede,
    envServiceKey: sessionData.selectedServiceEnvKey,
    startEU, endEU, limit: 500
  })
  let slots = filterOutBlockedSlots(rawSlots, sessionData.sede)

  if (!slots.length){
    await sendWithLog(sock, jid, `No encuentro huecos en ese rango. ¿Otra semana o franja?`, {phone, intent:"no_weekly_slots", action:"guide"})
    return
  }

  let staffIdFilter = null
  if (opts.staffName){
    const fz = fuzzyStaffFromText("con " + opts.staffName)
    if (fz && !fz.anyTeam) staffIdFilter = fz.id
  } else if (opts.usePreferred && sessionData.preferredStaffId){
    staffIdFilter = sessionData.preferredStaffId
  }

  if (staffIdFilter){
    const only = slots.filter(s => s.staffId === staffIdFilter)
    if (only.length) slots = only
  }

  slots.sort((a,b)=>a.date.valueOf()-b.date.valueOf())
  const limited = slots.slice(0, SHOW_TOP_N)

  const byDay = new Map()
  for (const s of limited){
    const key = s.date.format("YYYY-MM-DD")
    if (!byDay.has(key)) byDay.set(key, [])
    byDay.get(key).push(s)
  }
  const dayKeys = Array.from(byDay.keys()).sort()
  const lines = []
  const enumerated = []
  let idx=1
  for (const dk of dayKeys){
    const list = byDay.get(dk).sort((a,b)=>a.date.valueOf()-b.date.valueOf())
    lines.push(`\n📅 ${fmtDay(list[0].date)}`)
    for (const s of list){
      const iso = s.date.format("YYYY-MM-DDTHH:mm")
      const tag = s.staffId ? ` — ${staffLabelFromId(s.staffId)}` : ""
      lines.push(`${idx}) ${fmtHour(s.date)}${tag}`)
      enumerated.push({ index:idx, date:s.date, iso, staffId:s.staffId||null })
      idx++
    }
  }

  const map={}; const arr=[]
  for (const e of enumerated){ map[e.iso]=e.staffId; arr.push(e.date) }
  sessionData.lastHours = arr
  sessionData.lastStaffByIso = map
  sessionData.lastProposeUsedPreferred = !!staffIdFilter
  sessionData.stage = "awaiting_time"
  saveSession(phone, sessionData)

  const header = `🗓️ ${opts.nextWeek? "Próxima semana":"Próximos 7 días"} — ${locationNice(sessionData.sede)}\n${serviceLabelFromEnvKey(sessionData.selectedServiceEnvKey) || sessionData.selectedServiceLabel || "Servicio"}${staffIdFilter? ` · con ${staffLabelFromId(staffIdFilter)}`:""}\n`
  await sendWithLog(sock, jid, `${header}${lines.join("\n")}\n\nResponde con el *número*.`, {phone, intent:"weekly_list", action:"guide", stage:sessionData.stage})
}

// ====== Crear “reserva” SOLO en DB (bloquea hueco)
async function executeCreateLocalHold(sessionData, phone, sock, jid){
  if (!sessionData.sede) { await sendWithLog(sock, jid, "Falta el *salón*.", {phone, intent:"missing_sede", action:"guide"}); return }
  if (!sessionData.selectedServiceEnvKey) { await sendWithLog(sock, jid, "Falta el *servicio*.", {phone, intent:"missing_service", action:"guide"}); return }
  if (!sessionData.pendingDateTime) { await sendWithLog(sock, jid, "Falta la *fecha/hora*.", {phone, intent:"missing_datetime", action:"guide"}); return }

  const startEU = parseToEU(sessionData.pendingDateTime)
  if (!insideBusinessHours(startEU, 60)) { await sendWithLog(sock, jid, "Esa hora está fuera del horario (L–V 09:00–20:00).", {phone, intent:"outside_hours", action:"guide"}); return }

  const iso = startEU.format("YYYY-MM-DDTHH:mm")
  let staffId = sessionData.lastProposeUsedPreferred ? (sessionData.preferredStaffId || sessionData.lastStaffByIso?.[iso] || null)
                                                    : (sessionData.lastStaffByIso?.[iso] || sessionData.preferredStaffId || null)
  if (staffId && !isStaffAllowedInLocation(staffId, sessionData.sede)) staffId = null
  if (!staffId) staffId = pickStaffForLocation(sessionData.sede, null)

  // Identidad (para el nombre en el resumen)
  let customerId = sessionData.identityResolvedCustomerId || null
  if (!customerId){
    const got = await getUniqueCustomerByPhoneOrPrompt(phone, sessionData, sock, jid) || {}
    if (got.status === "need_new" || got.status === "need_pick") return
    customerId = got.customer?.id || null
  }
  if (!customerId && (sessionData.name || sessionData.email)){
    const created = await findOrCreateCustomerWithRetry({ name: sessionData.name, email: sessionData.email, phone })
    if (created) customerId = created.id
  }
  if (!customerId){
    sessionData.stage = "awaiting_identity"
    saveSession(phone, sessionData)
    await sendWithLog(sock, jid,"Para terminar, dime tu *nombre* y (opcional) tu *email* 😊",{phone, intent:"ask_identity", action:"guide", stage:sessionData.stage})
    return
  }

  // Si ya está bloqueado, avisamos
  const already = isSlotBlockedInDB({ locationKey: sessionData.sede, dateEU: startEU })
  if (already){
    await sendWithLog(sock, jid, "Ese hueco acaba de ocuparse. Te paso otros ahora mismo:", {phone, intent:"slot_taken", action:"info"})
    await proposeTimes(sessionData, phone, sock, jid, { text:"" })
    return
  }

  const aptId = `apt_${Math.random().toString(36).slice(2,8)}${Date.now().toString(36).slice(-4)}`
  insertAppt.run({
    id: aptId,
    customer_name: sessionData?.name || null,
    customer_phone: phone,
    customer_square_id: customerId,
    location_key: sessionData.sede,
    service_env_key: sessionData.selectedServiceEnvKey,
    service_label: sessionData.selectedServiceLabel || serviceLabelFromEnvKey(sessionData.selectedServiceEnvKey) || "Servicio",
    duration_min: 60,
    start_iso: startEU.tz("UTC").toISOString(),
    end_iso: startEU.clone().add(60, "minute").tz("UTC").toISOString(),
    staff_id: staffId,
    status: "pending_manual",
    created_at: new Date().toISOString(),
    square_booking_id: null,
    square_error: null,
    retry_count: 0
  })

  // Resumen super simple para que una empleada lo pase a Square manualmente
  const address = sessionData.sede === "la_luz" ? ADDRESS_LUZ : ADDRESS_TORRE
  const svcLabel = serviceLabelFromEnvKey(sessionData.selectedServiceEnvKey) || sessionData.selectedServiceLabel || "Servicio"
  const proLabel = staffId ? (staffLabelFromId(staffId) || "Equipo") : "Equipo"

  const msg = `✅ Te lo reservo *a falta de confirmación* (lo pasamos nosotros):
📍 ${locationNice(sessionData.sede)} — ${address}
🧾 ${svcLabel}
👩‍💼 ${proLabel}
🕐 ${fmtES(startEU)}

Una empleada te confirma en breve por aquí.`
  await sendWithLog(sock, jid, msg, {phone, intent:"booking_local_ok", action:"confirm"})
  clearSession(phone);
}

// ====== Listar/cancelar/otra gestión → mano humana + silencio 6h
function looksLikeAppointmentInfoQuery(text){
  const t = norm(text)
  return /\b(mi|la|de)\s*cita\b/.test(t) && /\b(cuando|hora|donde|detall|info|confirm|ver|consultar)\b/.test(t)
      || /\b(confirmaci[oó]n|recordatorio|comprobante)\b/.test(t)
}
function looksLikeIHaveAppointment(text){
  const t = norm(text)
  return /\btengo\s+cita\b/.test(t) || /\bye?\s*tengo\s*cita\b/.test(t)
}
function looksLikeEditOrCancel(text){
  const t = norm(text)
  return /\b(cancel|anular|mover|cambiar|modificar|editar|reprogramar)\b/.test(t)
}
function looksLikeOtherHumanTask(text){
  const t = norm(text)
  return /\b(reclam|factur|pago|devoluc|precio|tarifa|ubicaci[oó]n|direcci[oó]n|promoci[oó]n)\b/.test(t)
}
async function handoffToHuman(sessionData, phone, sock, jid){
  sessionData.snooze_until_ms = nowEU().add(6,"hour").valueOf()
  saveSession(phone, sessionData)
  await sendWithLog(sock, jid, "Una empleada se pone con ello en un momento. Te escribimos por aquí ✅", {phone, intent:"human_handoff", action:"info"})
}

// ====== Mini-web + Baileys
const app=express()
const PORT=process.env.PORT||8080
let lastQR=null, conectado=false
app.get("/", (_req,res)=>{
  res.send(`<!doctype html><meta charset="utf-8"><style>
  body{font-family:system-ui;display:grid;place-items:center;min-height:100vh;background:#f8f9fa}
  .card{max-width:720px;padding:32px;border-radius:20px;box-shadow:0 8px 32px rgba(0,0,0,.1);background:white}
  .status{padding:12px;border-radius:8px;margin:8px 0}
  .success{background:#d4edda;color:#155724}
  .error{background:#f8d7da;color:#721c24}
  .warning{background:#fff3cd;color:#856404}
  </style><div class="card">
  <h1>Gapink Nails Bot</h1>
  <div class="status ${conectado ? 'success' : 'error'}">WhatsApp: ${conectado ? "✅ Conectado" : "❌ Desconectado"}</div>
  ${!conectado&&lastQR?`<div style="text-align:center;margin:20px 0"><img src="/qr.png" width="300" style="border-radius:8px"></div>`:""}
  <div class="status warning">Modo: ${DRY_RUN ? "Simulación" : "Producción"} | IA: ${AI_PROVIDER.toUpperCase()}</div>
  </div>`)
})
app.get("/qr.png", async (_req,res)=>{
  if(!lastQR) return res.status(404).send("No QR")
  const png = await qrcode.toBuffer(lastQR, { type:"png", width:512, margin:1 })
  res.set("Content-Type","image/png").send(png)
})

async function loadBaileys(){
  const require = createRequire(import.meta.url); let mod=null
  try{ mod=require("@whiskeysockets/baileys") }catch{}; if(!mod){ try{ mod=await import("@whiskeysockets/baileys") }catch{} }
  if(!mod) throw new Error("Baileys incompatible")
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

      // Cola simple por teléfono
      if (!globalThis.__q) globalThis.__q = new Map()
      const QUEUE = globalThis.__q
      const prev=QUEUE.get(phone)||Promise.resolve()
      const job=prev.then(async ()=>{
        try{
          let session = loadSession(phone) || {
            greetedAt_ms:null,
            greeted:false, sede:null, category:null,
            selectedServiceEnvKey:null, selectedServiceLabel:null,
            preferredStaffId:null, preferredStaffLabel:null,
            pendingDateTime:null, lastHours:null, lastStaffByIso:{},
            lastProposeUsedPreferred:false, stage:null,
            identityChoices:null, identityResolvedCustomerId:null,
            cancelList:null,
            snooze_until_ms:null, name:null, email:null,
            lastServiceListSig:null, lastServiceListAt_ms:null
          }

          const now = nowEU()

          // Silencio si lo envías TÚ con puntitos
          if (isFromMe && /^[\s.·•⋅]+$/.test(textRaw)){
            session.snooze_until_ms = now.add(6,"hour").valueOf()
            saveSession(phone, session)
            logEvent({direction:"sys", action:"admin_snooze_6h", phone, raw_text:textRaw})
            return
          }

          if (isFromMe) { saveSession(phone, session); return }

          // IN log
          logEvent({direction:"in", action:"message", phone, raw_text:textRaw, stage:session.stage, extra:{isFromMe:false}})

          // Silencio 6h si el CLIENTE manda puntitos
          if (/^[\s.·•⋅]+$/.test(textRaw)){
            session.snooze_until_ms = now.add(6,"hour").valueOf()
            saveSession(phone, session)
            logEvent({direction:"sys", action:"snooze_6h", phone, raw_text:textRaw})
            return
          }

          // Si está en silencio, no respondemos
          if (session.snooze_until_ms && now.valueOf() < session.snooze_until_ms) { saveSession(phone, session); logEvent({direction:"sys", action:"snoozing_skip", phone}); return }

          // Primer mensaje en 24h: saludo
          const lastGreetAt = session.greetedAt_ms || 0
          if (!session.greeted || (Date.now()-lastGreetAt) > 24*60*60*1000){
            session.greeted=true; session.greetedAt_ms = Date.now(); saveSession(phone, session)
            await sendWithLog(sock, jid, buildGreeting(), {phone, intent:"greeting_24h", action:"send_greeting"})
          }

          // Si pide editar/cancelar u otra gestión → mano humana + silencio
          if (looksLikeEditOrCancel(textRaw) || looksLikeOtherHumanTask(textRaw)){
            await handoffToHuman(session, phone, sock, jid)
            return
          }

          // Consultas de info de cita: versión simple → lo lleva humana
          if (looksLikeAppointmentInfoQuery(textRaw) || looksLikeIHaveAppointment(textRaw)){
            await handoffToHuman(session, phone, sock, jid)
            return
          }

          // Parseos básicos
          const t = norm(textRaw)
          const numMatch = t.match(/^\s*([1-9]\d*)\b/)
          const sedeMention = parseSede(textRaw)
          const catMention = parseCategory(textRaw)

          if (/\b(con el equipo|me da igual|cualquiera|con quien sea|lo que haya)\b/i.test(t)){
            session.preferredStaffId = null
            session.preferredStaffLabel = null
            saveSession(phone, session)
            if (session.sede && session.selectedServiceEnvKey){
              await proposeTimes(session, phone, sock, jid, { text:textRaw })
            } else {
              const faltan=[]
              if (!session.sede) faltan.push("salón")
              if (!session.category) faltan.push("categoría")
              if (!session.selectedServiceEnvKey) faltan.push("servicio")
              await sendWithLog(sock, jid, `Genial. Te paso huecos del equipo en cuanto me digas ${faltan.join(", ")}.`, {phone, intent:"need_data", action:"guide"})
            }
            return
          }

          // ====== BLOQUE DETERMINISTA POR NÚMERO ======
          if (session.stage==="awaiting_identity_pick" && numMatch){
            const n = Number(numMatch[1])
            const choice = (session.identityChoices||[]).find(c=>c.index===n)
            if (!choice){ await sendWithLog(sock, jid, "No encontré esa opción. Responde con el número de tu ficha.", {phone, intent:"bad_pick", action:"guide", stage:session.stage}); return }
            session.identityResolvedCustomerId = choice.id
            session.stage = null
            saveSession(phone, session)
            await sendWithLog(sock, jid, "¡Gracias! Reservo en nuestro sistema…", {phone, intent:"identity_ok", action:"info"})
            await executeCreateLocalHold(session, phone, sock, jid)
            return
          }
          if (session.stage==="awaiting_identity"){
            const {name,email} = parseNameEmailFromText(textRaw)
            if (!name && !email){ await sendWithLog(sock, jid, "Dime tu *nombre* (y opcional tu *email*) 😊", {phone, intent:"ask_identity_again", action:"guide", stage:session.stage}); return }
            if (name) session.name = name
            if (email) session.email = email
            const created = await findOrCreateCustomerWithRetry({ name: session.name, email: session.email, phone })
            if (!created){ await sendWithLog(sock, jid, "No pude crear tu ficha. ¿Puedes repetir nombre y (opcional) email?", {phone, intent:"identity_fail", action:"guide", stage:session.stage}); return }
            session.identityResolvedCustomerId = created.id
            session.stage = null
            saveSession(phone, session)
            await sendWithLog(sock, jid, "¡Gracias! Reservo en nuestro sistema…", {phone, intent:"identity_created", action:"info"})
            await executeCreateLocalHold(session, phone, sock, jid)
            return
          }
          if (session.stage==="awaiting_service_choice" && numMatch && Array.isArray(session.serviceChoices) && session.serviceChoices.length){
            const n = Number(numMatch[1])
            const choice = session.serviceChoices.find(it=>it.index===n)
            if (!choice){ await sendWithLog(sock, jid, "No encontré esa opción. Responde con el número de la lista.", {phone, intent:"bad_service_pick", action:"guide", stage:session.stage}); return }
            session.selectedServiceEnvKey = choice.key
            session.selectedServiceLabel = choice.label
            session.stage = null
            saveSession(phone, session)
            if (session.preferredStaffId){
              await proposeTimes(session, phone, sock, jid, { text:"" })
            } else {
              await sendWithLog(sock, jid, `Perfecto: ${choice.label} en ${locationNice(session.sede)}. ¿Con alguien en concreto? Si no, te paso huecos.`, {phone, intent:"got_service", action:"guide"})
            }
            return
          }
          if ((!session.stage || session.stage==="awaiting_time") && numMatch && Array.isArray(session.lastHours) && session.lastHours.length){
            const idx = Number(numMatch[1]) - 1
            const pick = session.lastHours[idx]
            if (!dayjs.isDayjs(pick)){ await sendWithLog(sock, jid, "No encontré esa opción. Responde con un número válido.", {phone, intent:"bad_time_pick", action:"guide", stage:session.stage}); return }
            const iso = pick.format("YYYY-MM-DDTHH:mm")
            const staffFromIso = session?.lastStaffByIso?.[iso] || null
            session.pendingDateTime = pick.tz(EURO_TZ).toISOString()
            if (staffFromIso){ session.preferredStaffId = staffFromIso; session.preferredStaffLabel = staffLabelFromId(staffFromIso) }
            saveSession(phone, session)
            await sendWithLog(sock, jid, "¡Perfecto! Te la reservo en nuestro sistema…", {phone, intent:"time_selected", action:"info"})
            await executeCreateLocalHold(session, phone, sock, jid)
            return
          }
          // ====== FIN BLOQUE POR NÚMERO ======

          if (sedeMention) { session.sede = sedeMention; saveSession(phone, session) }
          if (catMention)  { session.category = catMention; saveSession(phone, session) }

          const fuzzy = fuzzyStaffFromText(textRaw)
          if (fuzzy){
            if (fuzzy.anyTeam){
              session.preferredStaffId = null
              session.preferredStaffLabel = null
              saveSession(phone, session)
              if (session.sede && session.selectedServiceEnvKey){
                await proposeTimes(session, phone, sock, jid, { text:textRaw })
              }else{
                const faltan=[]; if (!session.sede) faltan.push("salón"); if (!session.category) faltan.push("categoría"); if (!session.selectedServiceEnvKey) faltan.push("servicio")
                await sendWithLog(sock, jid, `Hecho. Te paso huecos del equipo cuando me digas ${faltan.join(", ")}.`, {phone, intent:"prefer_team_again", action:"guide"})
              }
              return
            }
            session.preferredStaffId = fuzzy.id
            session.preferredStaffLabel = staffLabelFromId(fuzzy.id)
            saveSession(phone, session)
            if (session.sede && session.selectedServiceEnvKey){
              await proposeTimes(session, phone, sock, jid, { text:textRaw })
              return
            }
            if (session.stage==="awaiting_service_choice"){
              await sendWithLog(sock, jid, `Genial, *con ${session.preferredStaffLabel}*. Ahora responde con el *número* del servicio 👆`, {phone, intent:"staff_set_during_service_choice", action:"guide", stage:session.stage})
              return
            }
          }

          // Horario semanal
          if (/\b(horario|agenda|esta\s*semana|pr[oó]xima\s*semana|semana\s*que\s*viene|7\s*d[ií]as|siete\s*d[ií]as)\b/i.test(t)){
            if (!session.selectedServiceEnvKey){
              if (!session.category){
                await sendWithLog(sock, jid, "Antes del horario, dime *categoría* (Uñas/Depilación/Micropigmentación/Faciales/Pestañas).", {phone, intent:"ask_category_for_schedule", action:"guide"})
                return
              }
              if (!session.sede){
                await sendWithLog(sock, jid, "¿En qué *salón* te viene mejor? *Torremolinos* o *La Luz*.", {phone, intent:"ask_sede_for_schedule", action:"guide"})
                return
              }
              const itemsRaw = listServicesByCategory(session.sede, session.category, textRaw)
              if (!itemsRaw.length){ await sendWithLog(sock, jid, `No tengo servicios de *${session.category}* en ${locationNice(session.sede)}.`, {phone, intent:"no_services_in_cat", action:"guide"}); return }
              const list = itemsRaw.slice(0,22).map((s,i)=>({ index:i+1, key:s.key, label:s.label }))
              session.serviceChoices = list
              session.stage = "awaiting_service_choice"
              const sig = serviceListSignature({sede:session.sede, category:session.category, list})
              if (shouldSuppressServiceList(session, sig)){
                await sendWithLog(sock, jid, "Te pasé la lista arriba 👆. Responde con el *número* del servicio para ver el horario.", {phone, intent:"anti_repeat_list", action:"guide", stage:session.stage})
              } else {
                saveSession(phone, session); noteServiceListSignature(session, sig, phone)
                const lines = list.map(it=> `${it.index}) ${it.label}`).join("\n")
                await sendWithLog(sock, jid, `Elige el *servicio* para mostrarte el horario semanal en ${locationNice(session.sede)}:\n\n${lines}\n\nResponde con el número.`, {phone, intent:"ask_service_number", action:"guide", stage:session.stage})
              }
              return
            }
            const temporal = parseTemporalPreference(textRaw)
            await weeklySchedule(session, phone, sock, jid, { nextWeek: temporal.nextWeek, staffName: null, usePreferred: true })
            return
          }

          // Propuesta de horas por lenguaje natural
          if (session.sede && session.selectedServiceEnvKey && /\botro dia\b|\botro día\b|\bhoy\b|\bmanana\b|\bpasado\b|\blunes\b|\bmartes\b|\bmiercoles\b|\bjueves\b|\bviernes\b|\btarde\b|\bpor la manana\b|\bnoche\b/i.test(t)){
            await proposeTimes(session, phone, sock, jid, { text:textRaw })
            return
          }

          // ====== IA MINI (parser de intención) ======
          const aiObj = await aiInterpretMini(textRaw, session)
          logEvent({direction:"sys", action:"ai_interpretation", phone, extra:{aiObj}})

          if (aiObj && typeof aiObj==="object"){
            const action = aiObj.action
            const p = aiObj.params || {}

            if ((action==="set_sede") && p.sede){
              const lk = parseSede(String(p.sede))
              if (lk){ session.sede = lk; saveSession(phone, session) }
              if (!session.category){
                await sendWithLog(sock, jid, "¿Qué *categoría* necesitas? *Uñas*, *Depilación*, *Micropigmentación*, *Faciales* o *Pestañas*.", {phone, intent:"ask_category", action:"guide"})
                return
              }
            }

            if (action==="set_category" && p.category){
              const cm = parseCategory(String(p.category))
              if (cm){ session.category = cm; saveSession(phone, session) }
              if (!session.sede){
                await sendWithLog(sock, jid, "¿En qué *salón* te viene mejor? *Torremolinos* o *La Luz*.", {phone, intent:"ask_sede", action:"guide"})
                return
              }
            }

            if (action==="set_staff" && p.name){
              const byAI = fuzzyStaffFromText("con " + p.name)
              if (byAI && !byAI.anyTeam){
                session.preferredStaffId = byAI.id
                session.preferredStaffLabel = staffLabelFromId(byAI.id)
                saveSession(phone, session)
                if (!session.sede){
                  session.stage="awaiting_sede"; saveSession(phone, session)
                  await sendWithLog(sock, jid, `¿En qué *salón* prefieres con ${session.preferredStaffLabel}? Torremolinos o La Luz.`, {phone, intent:"ask_sede_after_staff", action:"guide", stage:session.stage})
                  return
                }
              } else {
                session.preferredStaffId = null
                session.preferredStaffLabel = null
                saveSession(phone, session)
              }
            }

            if (action==="choose_service_label" && p.label && session.sede){
              const ek = resolveEnvKeyFromLabelAndSede(p.label, session.sede)
              if (ek){
                session.selectedServiceEnvKey = ek
                session.selectedServiceLabel = p.label
                session.stage=null; saveSession(phone, session)
                await sendWithLog(sock, jid, `Perfecto, ${p.label} en ${locationNice(session.sede)}.`, {phone, intent:"service_set", action:"info"})
                await proposeTimes(session, phone, sock, jid, { text:textRaw })
                return
              }
            }

            if (action==="weekly_schedule"){
              if (!session.selectedServiceEnvKey){
                await sendWithLog(sock, jid, "Dime el *servicio* y te muestro el horario semanal.", {phone, intent:"need_service_for_weekly", action:"guide"})
                return
              }
              await weeklySchedule(session, phone, sock, jid, { nextWeek: !!p.next_week, staffName: p.staff_name || null, usePreferred: !p.staff_name })
              return
            }

            if (action==="propose_times"){
              if (!session.sede){ await sendWithLog(sock, jid, "¿En qué *salón* te viene mejor? *Torremolinos* o *La Luz*.", {phone, intent:"need_sede_for_times", action:"guide"}); return }
              if (!session.category){ await sendWithLog(sock, jid, "¿Qué *categoría* necesitas? *Uñas*, *Depilación*, *Micropigmentación*, *Faciales* o *Pestañas*.", {phone, intent:"need_category_for_times", action:"guide"}); return }
              if (!session.selectedServiceEnvKey){
                const itemsRaw = listServicesByCategory(session.sede, session.category, textRaw)
                if (!itemsRaw.length){ await sendWithLog(sock, jid, `No tengo servicios de *${session.category}* en ${locationNice(session.sede)}.`, {phone, intent:"no_services_in_cat", action:"guide"}); return }
                const list = itemsRaw.slice(0,22).map((s,i)=>({ index:i+1, key:s.key, label:s.label }))
                session.serviceChoices = list
                session.stage = "awaiting_service_choice"
                const sig = serviceListSignature({sede:session.sede, category:session.category, list})
                if (shouldSuppressServiceList(session, sig)){
                  await sendWithLog(sock, jid, "Te pasé la lista arriba 👆. Responde con el *número*.", {phone, intent:"anti_repeat_list", action:"guide", stage:session.stage})
                } else {
                  saveSession(phone, session); noteServiceListSignature(session, sig, phone)
                  const lines = list.map(it=> `${it.index}) ${it.label}`).join("\n")
                  await sendWithLog(sock, jid, `Opciones de *${session.category}* en ${locationNice(session.sede)}:\n\n${lines}\n\nResponde con el número.`, {phone, intent:"ask_service_number", action:"guide", stage:session.stage})
                }
                return
              }
              await proposeTimes(session, phone, sock, jid, { date_hint:p.date_hint, part_of_day:p.part_of_day, text:textRaw })
              return
            }

            if (action==="list_appointments" || action==="cancel_appointment"){
              await handoffToHuman(session, phone, sock, jid)
              return
            }
          }

          // Si falta info, pedimos lo mínimo
          if (!session.sede){
            session.stage="awaiting_sede"; saveSession(phone, session)
            await sendWithLog(sock, jid, "¿En qué *salón* te viene mejor? *Torremolinos* o *La Luz*.", {phone, intent:"ask_sede", action:"guide", stage:session.stage})
            return
          }
          if (!session.category){
            session.stage="awaiting_category"; saveSession(phone, session)
            await sendWithLog(sock, jid, "¿Qué *categoría* necesitas? *Uñas*, *Depilación*, *Micropigmentación*, *Faciales* o *Pestañas*.", {phone, intent:"ask_category", action:"guide", stage:session.stage})
            return
          }
          if (!session.selectedServiceEnvKey){
            const itemsRaw = listServicesByCategory(session.sede, session.category, textRaw)
            if (!itemsRaw.length){ await sendWithLog(sock, jid, `No tengo servicios de *${session.category}* en ${locationNice(session.sede)}.`, {phone, intent:"no_services_in_cat", action:"guide"}); return }
            const list = itemsRaw.slice(0,22).map((s,i)=>({ index:i+1, key:s.key, label:s.label }))
            session.serviceChoices = list
            session.stage = "awaiting_service_choice"
            const sig = serviceListSignature({sede:session.sede, category:session.category, list})
            if (shouldSuppressServiceList(session, sig)){
              await sendWithLog(sock, jid, "Te pasé la lista arriba 👆. Responde con el *número*.", {phone, intent:"anti_repeat_list", action:"guide", stage:session.stage})
            } else {
              saveSession(phone, session); noteServiceListSignature(session, sig, phone)
              const lines = list.map(it=> `${it.index}) ${it.label}`).join("\n")
              await sendWithLog(sock, jid, `Opciones de *${session.category}* en ${locationNice(session.sede)}:\n\n${lines}\n\nResponde con el número.`, {phone, intent:"ask_service_number", action:"guide", stage:session.stage})
            }
            return
          }

          // Si menciona franjas, proponemos
          if (/\botro dia\b|\botro día\b|\bhoy\b|\bmanana\b|\bpasado\b|\blunes\b|\bmartes\b|\bmiercoles\b|\bjueves\b|\bviernes\b|\btarde\b|\bpor la manana\b|\bnoche\b/i.test(t)){
            await proposeTimes(session, phone, sock, jid, { text:textRaw })
            return
          }

          // Fallback
          await sendWithLog(sock, jid, buildGreeting(), {phone, intent:"fallback_greeting", action:"guide"})
        }catch(err){
          if (BOT_DEBUG) console.error(err)
          logEvent({direction:"sys", action:"handler_error", phone, error:{message:err?.message, stack:err?.stack}, success:0})
          await sendWithLog(globalThis.sock, messages?.[0]?.key?.remoteJid, "No te he entendido bien. ¿Puedes decirlo de otra forma? 😊", {phone, intent:"error_recover", action:"guide"})
        }
      })
      QUEUE.set(phone, job.finally(()=>{ if (QUEUE.get(phone)===job) QUEUE.delete(phone) }))
    })
  }catch(e){
    setTimeout(() => startBot().catch(console.error), 5000)
  }
}

// ====== Arranque
console.log(`🩷 Gapink Nails Bot v32.0.0 — Top ${SHOW_TOP_N} (L–V)`)
const appListen = app.listen(PORT, ()=>{ startBot().catch(console.error) })
process.on("uncaughtException", (e)=>{ console.error("💥 uncaughtException:", e?.stack||e?.message||e) })
process.on("unhandledRejection", (e)=>{ console.error("💥 unhandledRejection:", e) })
process.on("SIGTERM", ()=>{ try{ appListen.close(()=>process.exit(0)) }catch{ process.exit(0) } })
process.on("SIGINT",  ()=>{ try{ appListen.close(()=>process.exit(0)) }catch{ process.exit(0) } })
