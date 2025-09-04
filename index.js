// index.js — Gapink Nails · v32.4.0 (resumen manual, memoria fuerte, lock estricto, sedes por staff)
// Claves nuevas:
// - RESPECT_LOCATIONS: cada profesional puede tener sedes permitidas por ENV (SQ_EMP_*_ALLOW).
// - "ana/anna" ⇒ Ganna (sin colarse en Johana ni Anaira). "anaira" sigue siendo Anaira.
// - No revalida tras elegir número (adiós “no hay” tras una lista).
// - Al listar horas se limpia serviceChoices para que el número sea SIEMPRE una hora.
// - strictStaffLock: si el cliente dice "con X" → buscar SOLO con X hasta 52 semanas.
// - Anti-spam de preguntas (cooldown).

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

// ====== Config horario
const WORK_DAYS = [1,2,3,4,5]        // L–V
const SLOT_MIN = 30
const OPEN = { start: 9, end: 20 }
const NOW_MIN_OFFSET_MIN = Number(process.env.BOT_NOW_OFFSET_MIN || 30)
const SEARCH_WINDOW_DAYS = Number(process.env.BOT_SEARCH_WINDOW_DAYS || 30)
const HOLIDAYS_EXTRA = (process.env.HOLIDAYS_EXTRA || "06/01,28/02,15/08,12/10,01/11,06/12,08/12,25/12")
  .split(",").map(s=>s.trim()).filter(Boolean)
const SHOW_TOP_N = Number(process.env.SHOW_TOP_N || 5)
const MAX_WEEKS_LOOKAHEAD = Number(process.env.MAX_WEEKS_LOOKAHEAD || 52)

// ====== Flags
const BOT_DEBUG = /^true$/i.test(process.env.BOT_DEBUG || "")
const DRY_RUN = /^true$/i.test(process.env.DRY_RUN || "")
const SQUARE_MAX_RETRIES = Number(process.env.SQUARE_MAX_RETRIES || 3)

// ====== Square (consultas; NO reservar)
const square = new Client({
  accessToken: process.env.SQUARE_ACCESS_TOKEN,
  environment: (process.env.SQUARE_ENV==="production") ? Environment.Production : Environment.Sandbox
})
const LOC_TORRE = (process.env.SQUARE_LOCATION_ID_TORREMOLINOS || "").trim()
const LOC_LUZ   = (process.env.SQUARE_LOCATION_ID_LA_LUZ || "").trim()
const ADDRESS_TORRE = process.env.ADDRESS_TORREMOLINOS || "Av. de Benyamina 18, Torremolinos"
const ADDRESS_LUZ   = process.env.ADDRESS_LA_LUZ || "Málaga – Barrio de La Luz"

// ====== IA (opcional)
const AI_PROVIDER = (process.env.AI_PROVIDER || (process.env.DEEPSEEK_API_KEY? "deepseek" : process.env.OPENAI_API_KEY? "openai" : "none")).toLowerCase()
const DEEPSEEK_API_KEY = process.env.DEEPSEEK_API_KEY || ""
const DEEPSEEK_MODEL = process.env.DEEPSEEK_MODEL || "deepseek-chat"
const OPENAI_API_KEY = process.env.OPENAI_API_KEY || ""
const OPENAI_MODEL = process.env.OPENAI_MODEL || "gpt-4o-mini"
const AI_TIMEOUT_MS = Number(process.env.AI_TIMEOUT_MS || 15000)
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
    const url = AI_PROVIDER==="deepseek" ? "https://api.deepseek.com/chat/completions" : "https://api.openai.com/v1/chat/completions"
    const key = AI_PROVIDER==="deepseek" ? DEEPSEEK_API_KEY : OPENAI_API_KEY
    const model = AI_PROVIDER==="deepseek" ? DEEPSEEK_MODEL : OPENAI_MODEL
    const resp = await fetch(url,{
      method:"POST",
      headers:{ "Content-Type":"application/json", "Authorization":`Bearer ${key}` },
      body: JSON.stringify({ model, messages, temperature:0.2, max_tokens:700 }),
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
  let s = text.trim().replace(/```json/gi,"```")
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
function titleCase(str){ return String(str||"").toLowerCase().replace(/\b([a-z])/g, (m)=>m.toUpperCase()) }
function cleanDisplayLabel(label){ const s = String(label||"").replace(/^\s*(luz|la\s*luz)\s+/i,"").trim(); return applySpanishDiacritics(s) }
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
  square_booking_id TEXT,
  square_error TEXT,
  retry_count INTEGER DEFAULT 0
);
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

// ====== Logging + envío
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
  try{
    console.log(JSON.stringify({ message:`[${direction.toUpperCase()}] ${action||"event"}`, attributes:{ phone, intent, stage, raw_text, reply_text, extra, timestamp:new Date().toISOString()}}))
  }catch{}
}
async function sendWithLog(sock, jid, text, {phone, intent, action, stage, extra}={}){
  logEvent({direction:"out", action: action||"send", phone, intent:intent||null, stage:stage||null, raw_text:null, reply_text:text, extra: {payload:{text}, ...(extra||{})}})
  try{ await sock.sendMessage(jid, { text }) }catch(e){
    logEvent({direction:"sys", action:"send_error", phone, intent, stage, raw_text:text, error:{message:e?.message}})
  }
}

// ====== Sesión + anti-repeat
const ASK_COOLDOWN_MS = Number(process.env.ASK_COOLDOWN_MS || (90*1000))
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
function canAsk(session, key){ const t = Date.now(); const last = session.lastAskAt?.[key] || 0; return (t - last) > ASK_COOLDOWN_MS }
function noteAsk(session, key){ session.lastAskAt = session.lastAskAt || {}; session.lastAskAt[key] = Date.now() }

// ====== Empleadas (con sedes permitidas) — Anna/Anna⇒Ganna
function deriveLabelsFromEnvKey(envKey){
  const raw = envKey.replace(/^SQ_EMP_/, "")
  const toks = raw.split("_").map(t=>norm(t)).filter(Boolean)
  const uniq = Array.from(new Set(toks))
  const labels = [...uniq]
  if (uniq.length>1) labels.push(uniq.join(" "))
  return labels
}
function normalizeAllow(val){
  if (!val) return ["ALL"]
  const items = String(val).split(",").map(s=>norm(s)).map(s=>s.replace(/\s+/g,"_"))
  const out = new Set()
  for (const i of items){
    if (!i || i==="all") { out.add("ALL"); continue }
    if (i==="la_luz" || i==="la" || i==="luz") out.add("la_luz")
    if (i==="torremolinos" || i==="torre") out.add("torremolinos")
  }
  return out.size? Array.from(out) : ["ALL"]
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
    const allowEnv = process.env[`${k}_ALLOW`] || process.env[`${k}_LOC`] || "ALL"
    const allow = normalizeAllow(allowEnv)
    out.push({ envKey:k, id, bookable, allow, labels })
  }
  return out
}
let EMPLOYEES = parseEmployees()

function staffLabelFromId(id){
  const e = EMPLOYEES.find(x=>x.id===id)
  const base = e?.labels?.[0] || (id ? `Profesional ${String(id).slice(-4)}` : null)
  return base ? titleCase(base) : null
}
function isStaffAllowedInLocation(staffId, locKey){
  const e = EMPLOYEES.find(x=>x.id===staffId)
  if (!e || !e.bookable) return false
  if (!locKey) return true
  if (e.allow?.includes("ALL")) return true
  return e.allow?.includes(locKey)
}
function pickStaffForLocation(locKey, preferId=null){
  if (preferId){
    const e = EMPLOYEES.find(x=>x.id===preferId && x.bookable && isStaffAllowedInLocation(x.id, locKey))
    if (e) return e.id
  }
  const found = EMPLOYEES.find(e=>e.bookable && isStaffAllowedInLocation(e.id, locKey))
  return found?.id || null
}

// Aliases de nombres y matching robusto
// Regla especial: si el cliente dice "ana" o "anna" *exacto* ⇒ Ganna.
// "anaira" o "an aira" ⇒ Anaira (no Ganna). Nada de colarse con Johana.
const NAME_ALIASES = [
  ["ganna","ghanna","hanna","hana","anna","ana"], // ana/anna => ganna (prioritario)
  ["anaira","an aira"],
  ["patri","patricia"],["patricia","patri"],
  ["cristi","cristina","christina","cristy"],
  ["rocio chica","rociochica","rocio  chica","rocio c","rocio chica"],["rocio","rosio"],
  ["carmen belen","carmen","belen"],
  ["tania","tani"],
  ["johana","joana","yohana"], // cuidado: no capturamos "ana" por substring
  ["ginna","gina"],
  ["chabely","chabeli","chabelí"],
  ["elisabeth","elisabet","elis"],
  ["desi","desiree","desirée"],
  ["daniela","dani"],
  ["jamaica","jahmaica"],
  ["edurne","edur"],
  ["sudemis","sude"],
  ["maria","maría"],
  ["thalia","thalía","talia","talía"]
]
function tokenWords(txt){ return norm(txt).split(/\s+/).filter(Boolean) }
function exactTokenPresent(txt, token){ return tokenWords(txt).includes(norm(token)) }
function fuzzyStaffFromText(text){
  const tnorm = norm(text)
  if (/\b(con el equipo|me da igual|cualquiera|con quien sea|lo que haya)\b/i.test(tnorm)) return { anyTeam:true }

  // Anaira prioridad si está el token completo
  if (/\banaira\b|\ban\s+aira\b/i.test(text)) {
    for (const e of EMPLOYEES){ if (e.labels.some(l=>/\banaira\b/i.test(l))) return e }
  }
  // Si aparece "ana" o "anna" como token independiente → Ganna
  if (/\bana\b/i.test(tnorm) || /\banna\b/i.test(tnorm)) {
    for (const e of EMPLOYEES){ if (e.labels.some(l=>/\bganna\b/i.test(l))) return e }
  }

  // Captura "con <nombre>"
  const m = /(?:^|\s)con\s+([a-zñáéíóúüï\s]{2,})(?:[?.!,]|$)/i.exec(text)
  let token = m ? norm(m[1]).trim() : null

  // También aceptamos un nombre suelto conocido
  if (!token){
    const nm = tnorm.match(/\b(patri|patricia|cristi|cristina|christina|rocio chica|rocio|carmen belen|carmen|belen|ganna|ghanna|hanna|hana|anna|anaira|maria|ginna|daniela|desi|jamaica|johana|edurne|sudemis|tania|chabely|elisabeth|thalia|thalía|talia|talía)\b/i)
    if (nm) token = norm(nm[0])
  }
  if (!token) return null

  // Map alias → canónico (sin que "ana" toque johana/anaira)
  for (const arr of NAME_ALIASES){
    if (arr.some(a=>(" "+token+" ").includes(" "+a+" "))) { token = arr[0]; break }
  }

  // Buscar por labels
  for (const e of EMPLOYEES){
    for (const lbl of e.labels){
      if ((" "+norm(lbl)+" ").includes(" "+token+" ")) return e
    }
  }
  return null
}
function staffRosterForPrompt(){
  return EMPLOYEES.map(e=>{
    return `• ID:${e.id} | Nombres:[${e.labels.join(", ")}] | Reservable:${e.bookable} | Allow:${(e.allow||[]).join("/")}`
  }).join("\n")
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
    label = cleanDisplayLabel(label)
    out.push({ sedeKey, key:k, id, rawKey:k, label, norm: norm(label) })
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

// ====== Categorías
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
  "faciales": (s,_u)=> /\b(limpieza|facial|dermapen|carbon|peel|vitamina|hidra|piedras|oro|acne|manchas|colageno|colágeno)\b/i.test(s.label),
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

// ====== Square helpers (consultas)
async function searchCustomersByPhone(phone){
  try{
    const e164=normalizePhoneES(phone); if(!e164) return []
    const got = await square.customersApi.searchCustomers({ query:{ filter:{ phoneNumber:{ exact:e164 } } } })
    return got?.result?.customers || []
  }catch{ return [] }
}
async function getUniqueCustomerByPhoneOrPrompt(phone, sessionData, sock, jid){
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
    await sendWithLog(sock, jid, "Para terminar, no encuentro tu ficha por este número. Dime tu *nombre completo* y, si quieres, tu *email* para crearte 😊", {phone, intent:"ask_identity", action:"guide", stage:sessionData.stage})
    return { status:"need_new" }
  }
  const choices = matches.map((c,i)=>({
    index:i+1, id:c.id, name:c?.givenName || "Sin nombre", email:c?.emailAddress || "—"
  }))
  sessionData.identityChoices = choices
  sessionData.stage = "awaiting_identity_pick"
  saveSession(phone, sessionData)
  const lines = choices.map(ch => `${ch.index}) ${ch.name} ${ch.email!=="—" ? `(${ch.email})`:""}`).join("\n")
  await sendWithLog(sock, jid, `Para terminar, he encontrado varias fichas con tu número. ¿Cuál eres?\n\n${lines}\n\nResponde con el número.`, {phone, intent:"ask_identity_pick", action:"guide", stage:sessionData.stage})
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
async function createBookingWithRetry(){ return { success:false, error:"disabled" } } // deshabilitado

// ====== DISPONIBILIDAD
function partOfDayWindow(dateEU, part){
  let start=dateEU.clone().hour(OPEN.start).minute(0).second(0).millisecond(0)
  let end  =dateEU.clone().hour(OPEN.end).minute(0).second(0).millisecond(0)
  if (part==="mañana") end = dateEU.clone().hour(13).minute(0)
  if (part==="tarde") { start = dateEU.clone().hour(15).minute(0) }
  if (part==="noche") { start = dateEU.clone().hour(18).minute(0) }
  return { start, end }
}
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
function parseExplicitDateTime(text){
  const t = text.trim()
  const re = /(?<!\d)(\d{1,2})[\/\-\.](\d{1,2})(?:[\/\-\.](\d{2,4}))?\s+(?:a\s+las\s+)?(\d{1,2}):(\d{2})(?!\d)/i
  const m = re.exec(t)
  if (!m) return null
  let [_, dd, mm, yyyy, hh, min] = m
  dd = Number(dd); mm = Number(mm); hh = Number(hh); min = Number(min)
  if (!yyyy){ yyyy = dayjs().year() } else { yyyy = Number(yyyy.length===2 ? (2000 + Number(yyyy)) : yyyy) }
  const d = dayjs.tz(`${yyyy}-${String(mm).padStart(2,"0")}-${String(dd).padStart(2,"0")}T${String(hh).padStart(2,"0")}:${String(min).padStart(2,"0")}:00`, EURO_TZ)
  if (!d.isValid()) return null
  return d
}

async function searchAvailWindow({ locationKey, envServiceKey, startEU, endEU, limit=200, part=null }){
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
    // NO empujamos staff no permitidos en la sede
    if (tm && !isStaffAllowedInLocation(tm, idToLocKey(locationToId(locationKey)))) continue
    if (part){
      const { start, end } = partOfDayWindow(d, part)
      if (!(d.isSame(start,"day") && d.isAfter(start.subtract(1,"minute")) && d.isBefore(end.add(1,"minute")))) continue
    }
    out.push({ date:d, staffId: tm || null })
    if (out.length>=limit) break
  }
  return out
}
function idToLocKey(id){ return id===LOC_LUZ ? "la_luz" : id===LOC_TORRE ? "torremolinos" : null }

// Búsqueda estricta multi-semana para una profesional concreta
async function findStrictPreferredSlots({ locationKey, envServiceKey, baseStart, preferredId, part=null }){
  let results=[]
  for (let w=0; w<MAX_WEEKS_LOOKAHEAD && results.length<SHOW_TOP_N; w++){
    const startEU = baseStart.clone().add(w,"week").hour(OPEN.start).minute(0).second(0).millisecond(0)
    const endEU   = startEU.clone().add(7,"day").hour(OPEN.end).minute(0).second(0).millisecond(0)
    const raw = await searchAvailWindow({ locationKey, envServiceKey, startEU, endEU, limit: 500, part })
    const onlyPreferred = raw.filter(s => s.staffId === preferredId)
    results = results.concat(onlyPreferred)
  }
  return results
}

// ====== IA system prompt
function buildGreeting(){
  return `¡Hola! Soy el asistente de Gapink Nails.\n\nPara reservar dime *salón* (Torremolinos o La Luz) y *categoría*: Uñas / Depilación / Micropigmentación / Faciales / Pestañas.\nEj.: “depilación en Torremolinos con Patri el viernes por la tarde”.\nTambién puedo mostrarte el *horario de los próximos 7 días* (“horario esta semana” o “próxima semana con Cristina”).`
}
function buildSystemPrompt(session){
  const nowEU = dayjs().tz(EURO_TZ)
  const torremolinos_services = servicesForSedeKeyRaw("torremolinos");
  const laluz_services = servicesForSedeKeyRaw("la_luz");
  const staffLines = staffRosterForPrompt()
  return `Eres el asistente de WhatsApp de Gapink Nails. Devuelves SOLO JSON válido.
Fecha/hora: ${nowEU.format("dddd DD/MM/YYYY HH:mm")} (Madrid)
Salones: Torremolinos (${ADDRESS_TORRE}) y Málaga – La Luz (${ADDRESS_LUZ})
Profesionales (IDs, aliases y sedes permitidas):
${staffLines}

Servicios TORREMOLINOS:
${torremolinos_services.map(s => `- ${s.label} (Clave: ${s.key})`).join("\n")}

Servicios LA LUZ:
${laluz_services.map(s => `- ${s.label} (Clave: ${s.key})`).join("\n")}

REGLAS:
- Si el cliente escribe números para elegir, NO intervengas. Tú solo interpretas lenguaje natural.
- Mapea nombres de profesionales (alias) a IDs listados (respeta sedes si la profesional no trabaja allí).
- Antes de listar servicios, identifica *categoría* y *salón*.
- Si pide “horario”, “esta/está semana” o “próxima semana”, devuelve {action:"weekly_schedule", next_week:boolean, staff_name?:string}.
- Si dice “otro día / viernes tarde…”, devuelve {action:"propose_times", date_hint, part_of_day}.
- Para reservar hace falta: salón + servicio + fecha/hora. La identidad se resuelve por teléfono.
- Acciones: set_salon (alias set_sede), set_category, set_staff, choose_service_label, propose_times, weekly_schedule, create_booking, list_appointments, cancel_appointment, none.

FORMATO:
{"message":"...","action":"set_salon|set_sede|set_category|set_staff|choose_service_label|propose_times|weekly_schedule|create_booking|list_appointments|cancel_appointment|none","params":{ ... } }`
}
async function aiInterpret(textRaw, session){
  if (AI_PROVIDER==="none") return null
  const sys = buildSystemPrompt(session)
  const ctx = `Estado actual:
- Salón: ${session.sede||"—"}
- Categoría: ${session.category||"—"}
- Servicio: ${session.selectedServiceLabel||"—"}
- Profesional: ${session.preferredStaffLabel||"—"} (lock:${session.strictStaffLock? "on":"off"})`
  const out = await aiChat(sys, `Mensaje cliente: "${textRaw}"\n${ctx}\nDevuelve SOLO JSON (sin explicaciones).`)
  const obj = stripToJSON(out)
  return obj
}

// ====== Anti-repeat heurística
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

// ====== Proponer horas (respeta lock + limpia serviceChoices)
function proposeLines(slots, mapIsoToStaff){
  const hoursEnum = enumerateHours(slots.map(s=>s.date))
  const lines = hoursEnum.map(h => {
    const sid = mapIsoToStaff[h.iso] || null
    const tag = sid ? ` — ${staffLabelFromId(sid)}` : ""
    return `${h.index}) ${h.pretty}${tag}`
  }).join("\n")
  return { lines, hoursEnum }
}

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
    if (canAsk(sessionData,"sede_service")){
      noteAsk(sessionData,"sede_service"); saveSession(phone, sessionData)
      await sendWithLog(sock, jid, "Necesito primero *salón* y *servicio* para proponerte horas.", {phone, intent:"need_sede_service", action:"guide", stage:sessionData.stage})
    }
    return
  }

  const targetPreferred = opts.forceStaffId ?? sessionData.preferredStaffId ?? null
  const strictPreferred = opts.strictPreferred ?? !!sessionData.strictStaffLock

  // — Limpiar serviceChoices para que el número sea SIEMPRE una hora
  sessionData.serviceChoices = null; saveSession(phone, sessionData)

  // ===== MODO ESTRICTO
  if (strictPreferred && targetPreferred){
    // Si la profesional no está permitida en esta sede, lo decimos limpio
    if (!isStaffAllowedInLocation(targetPreferred, sessionData.sede)){
      await sendWithLog(sock, jid, `${staffLabelFromId(targetPreferred)} no trabaja en ${locationNice(sessionData.sede)}. ¿Prefieres *otra profesional* o *el equipo*?`, {phone, intent:"staff_not_allowed", action:"guide"})
      return
    }
    const strictBase = startEU.clone()
    const strictSlots = await findStrictPreferredSlots({
      locationKey: sessionData.sede,
      envServiceKey: sessionData.selectedServiceEnvKey,
      baseStart: strictBase,
      preferredId: targetPreferred,
      part
    })
    if (strictSlots.length){
      const shown = strictSlots.slice(0, SHOW_TOP_N)
      const map={}; for (const s of shown) map[s.date.format("YYYY-MM-DDTHH:mm")] = s.staffId || null
      const { lines } = proposeLines(shown, map)

      sessionData.lastHours = shown.map(s => s.date)
      sessionData.lastStaffByIso = map
      sessionData.lastProposeUsedPreferred = true
      sessionData.stage = "awaiting_time"
      saveSession(phone, sessionData)

      await sendWithLog(sock, jid, `Horarios disponibles *con ${staffLabelFromId(targetPreferred)}* — primeras ${SHOW_TOP_N}:\n${lines}\n\nResponde con el número.`, {phone, intent:"times_list_strict", action:"guide", stage:sessionData.stage})
      return
    } else {
      await sendWithLog(sock, jid, `He mirado *hasta ${MAX_WEEKS_LOOKAHEAD} semanas* y no veo huecos *con ${staffLabelFromId(targetPreferred)}*.\n¿Quieres que lo deje *con el equipo* u otra profesional?`, {phone, intent:"no_slots_strict", action:"guide"})
      return
    }
  }

  // ===== MODO NORMAL
  const rawSlots = await searchAvailWindow({
    locationKey: sessionData.sede,
    envServiceKey: sessionData.selectedServiceEnvKey,
    startEU, endEU, limit: 200, part
  })

  // Filtramos por sedes permitidas (por si Square devolviera algo raro)
  let slots = rawSlots.filter(s => !s.staffId || isStaffAllowedInLocation(s.staffId, sessionData.sede))
  let usedPreferred = false
  if (targetPreferred){
    const filtered = slots.filter(s => s.staffId === targetPreferred)
    if (filtered.length){ slots = filtered; usedPreferred = true }
  }

  // Fallback próxima semana si vacío
  if (!slots.length){
    const startNext = startEU.clone().add(7, "day")
    const endNext   = endEU.clone().add(7, "day")
    const rawNext = await searchAvailWindow({
      locationKey: sessionData.sede,
      envServiceKey: sessionData.selectedServiceEnvKey,
      startEU: startNext, endEU: endNext, limit: 200, part
    })
    let nextSlots = rawNext.filter(s => !s.staffId || isStaffAllowedInLocation(s.staffId, sessionData.sede))
    if (targetPreferred){
      const pf = nextSlots.filter(s => s.staffId === targetPreferred)
      if (pf.length){ nextSlots = pf; usedPreferred = true }
    }
    if (nextSlots.length){
      const shown = nextSlots.slice(0, SHOW_TOP_N)
      const mapN={}; for (const s of shown) mapN[s.date.format("YYYY-MM-DDTHH:mm")] = s.staffId || null
      const { lines } = proposeLines(shown, mapN)
      sessionData.lastHours = shown.map(s => s.date)
      sessionData.lastStaffByIso = mapN
      sessionData.lastProposeUsedPreferred = usedPreferred
      sessionData.stage = "awaiting_time"
      saveSession(phone, sessionData)
      const staffTag = (usedPreferred && targetPreferred) ? ` con ${staffLabelFromId(targetPreferred)}` : ""
      await sendWithLog(sock, jid, `*Próxima semana* hay huecos${staffTag} (primeras ${SHOW_TOP_N}):\n${lines}\n\nResponde con el número.`, {phone, intent:"times_next_week", action:"guide", stage:sessionData.stage})
      return
    }
  }

  if (!slots.length){
    const msg = when
      ? `No veo huecos para ese día${part?` por la ${part}`:""}${targetPreferred?` con ${staffLabelFromId(targetPreferred)}`:""}. ¿Otra fecha o franja?`
      : `No encuentro huecos en este rango${targetPreferred?` con ${staffLabelFromId(targetPreferred)}`:""}. ¿Otra fecha/franja (ej. “viernes por la tarde”)?`
    await sendWithLog(sock, jid, msg, {phone, intent:"no_slots", action:"guide"})
    return
  }

  const shown = slots.slice(0, SHOW_TOP_N)
  const map = {}; for (const s of shown) map[s.date.format("YYYY-MM-DDTHH:mm")] = s.staffId || null
  const { lines } = proposeLines(shown, map)

  sessionData.lastHours = shown.map(s => s.date)
  sessionData.lastStaffByIso = map
  sessionData.lastProposeUsedPreferred = usedPreferred
  sessionData.stage = "awaiting_time"
  saveSession(phone, sessionData)

  const header = usedPreferred
    ? `Horarios disponibles *con ${staffLabelFromId(targetPreferred)}* — primeras ${SHOW_TOP_N}:`
    : `Horarios disponibles (equipo) — primeras ${SHOW_TOP_N}:`
  await sendWithLog(sock, jid, `${header}\n${lines}\n\nResponde con el número.`, {phone, intent:"times_list", action:"guide", stage:sessionData.stage})
}

// ====== HORARIO SEMANAL
function nextMondayEU(base){ return base.clone().add(1,"week").isoWeekday(1).hour(OPEN.start).minute(0).second(0).millisecond(0) }
async function weeklySchedule(sessionData, phone, sock, jid, opts={}){
  if (!sessionData.sede){
    if (canAsk(sessionData,"sede")){ noteAsk(sessionData,"sede"); saveSession(phone, sessionData)
      await sendWithLog(sock, jid, "¿En qué *salón* te viene mejor? *Torremolinos* o *La Luz*.", {phone, intent:"ask_sede", action:"guide"}) }
    return
  }
  if (!sessionData.selectedServiceEnvKey){
    if (canAsk(sessionData,"servicio")){ noteAsk(sessionData,"servicio"); saveSession(phone, sessionData)
      await sendWithLog(sock, jid, "Dime el *servicio* (o la *categoría* para listarte opciones) y te muestro el horario semanal.", {phone, intent:"ask_service", action:"guide"}) }
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

  if (!rawSlots.length){
    await sendWithLog(sock, jid, `No encuentro huecos en ese rango. ¿Quieres que mire otra semana o cambiar de franja?`, {phone, intent:"no_weekly_slots", action:"guide"})
    return
  }

  // Limpiamos serviceChoices para que el número sea hora
  sessionData.serviceChoices = null; saveSession(phone, sessionData)

  let staffIdFilter = opts.forceStaffId || (sessionData.strictStaffLock ? sessionData.preferredStaffId : null)
  let slots = rawSlots.filter(s => !s.staffId || isStaffAllowedInLocation(s.staffId, sessionData.sede))
  if (staffIdFilter){
    if (!isStaffAllowedInLocation(staffIdFilter, sessionData.sede)){
      await sendWithLog(sock, jid, `${staffLabelFromId(staffIdFilter)} no trabaja en ${locationNice(sessionData.sede)}. ¿Miro otra profesional?`, {phone, intent:"weekly_not_allowed", action:"guide"})
      return
    }
    const pf = slots.filter(s => s.staffId === staffIdFilter)
    if (pf.length){ slots = pf } else {
      await sendWithLog(sock, jid, `Sin huecos *esta semana* con ${staffLabelFromId(staffIdFilter)}. ¿Miro semanas siguientes o prefieres otra profesional?`, {phone, intent:"weekly_no_lock", action:"guide"})
      return
    }
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

  const header = `🗓️ Horario ${opts.nextWeek? "de la *próxima semana*":"de los *próximos 7 días*"} — primeras ${SHOW_TOP_N} — ${locationNice(sessionData.sede)}\n` +
                 `${serviceLabelFromEnvKey(sessionData.selectedServiceEnvKey) || sessionData.selectedServiceLabel || "Servicio"}${staffIdFilter? ` · con ${staffLabelFromId(staffIdFilter)}`:""}\n`
  await sendWithLog(sock, jid, `${header}${lines.join("\n")}\n\nResponde con el *número* para reservar ese hueco.`, {phone, intent:"weekly_list", action:"guide", stage:sessionData.stage})
}

// ====== Crear “reserva” → RESUMEN manual
async function executeCreateBooking(sessionData, phone, sock, jid){
  if (!sessionData.sede) { if (canAsk(sessionData,"sede")){ noteAsk(sessionData,"sede"); saveSession(phone, sessionData)
      await sendWithLog(sock, jid, "Falta el *salón* (Torremolinos o La Luz)", {phone, intent:"missing_sede", action:"guide"}) } return }
  if (!sessionData.selectedServiceEnvKey) { if (canAsk(sessionData,"servicio")){ noteAsk(sessionData,"servicio"); saveSession(phone, sessionData)
      await sendWithLog(sock, jid, "Falta el *servicio*", {phone, intent:"missing_service", action:"guide"}) } return }
  if (!sessionData.pendingDateTime) { if (canAsk(sessionData,"fecha")){ noteAsk(sessionData,"fecha"); saveSession(phone, sessionData)
      await sendWithLog(sock, jid, "Falta la *fecha y hora*", {phone, intent:"missing_datetime", action:"guide"}) } return }

  const startEU = parseToEU(sessionData.pendingDateTime)
  if (!insideBusinessHours(startEU, 60)) {
    await sendWithLog(sock, jid, "Esa hora está fuera del horario (L–V 09:00–20:00)", {phone, intent:"outside_hours", action:"guide"})
    return
  }

  // NO revalidamos disponibilidad: usamos el hueco elegido o escrito por el cliente
  const iso = startEU.format("YYYY-MM-DDTHH:mm")
  let staffId = sessionData.lastStaffByIso?.[iso] || sessionData.preferredStaffId || null
  if (staffId && !isStaffAllowedInLocation(staffId, sessionData.sede)) staffId = null

  const staffName = staffId ? (staffLabelFromId(staffId) || "Profesional") : (sessionData.preferredStaffLabel || "Equipo")
  const address = sessionData.sede === "la_luz" ? ADDRESS_LUZ : ADDRESS_TORRE
  const svcLabel = serviceLabelFromEnvKey(sessionData.selectedServiceEnvKey) || sessionData.selectedServiceLabel || "Servicio"

  const name = sessionData?.name || "—"
  const email = sessionData?.email || "—"
  const phonePretty = phone || "—"

  const summaryText =
`📝 *Resumen para que una empleada lo coja manualmente* (no reservado aún)

📍 ${locationNice(sessionData.sede)}
${address}

🧾 ${svcLabel}
👤 ${staffId ? `Con ${staffName}` : `Con el equipo (${staffName})`}
📅 ${fmtES(startEU)}
⏱️ 60 min

*Datos de la persona:*
• Nombre: ${name}
• Teléfono: ${phonePretty}
• Email: ${email}

Cuando lo coja una compañera, te llegará la confirmación ✅`

  try{
    insertSquareLog.run({
      phone: phone || 'unknown',
      action: 'summary_ready',
      request_data: safeJSONStringify({ sede:sessionData.sede, service:svcLabel, start: iso, staffId, staffName, strict:sessionData.strictStaffLock }),
      response_data: safeJSONStringify({ message: summaryText }),
      error_data: null,
      timestamp: new Date().toISOString(),
      success: 1
    })
  }catch{}

  await sendWithLog(sock, jid, summaryText, {phone, intent:"summary_ready", action:"summary"})
  clearSession(phone)
}

// ====== Listar/cancelar/info de cita -> redirigir
const BOOKING_SELF_SERVICE_MSG = "Para *consultar, editar o cancelar* tu cita usa el enlace del *email/SMS de confirmación*. Desde ahí puedes ver la hora exacta y gestionar cambios al instante ✅"
async function executeListAppointments(_session, phone, sock, jid){
  await sendWithLog(sock, jid, BOOKING_SELF_SERVICE_MSG, {phone, intent:"ask_info_list", action:"redirect"})
}
async function executeCancelAppointment(sessionData, phone, sock, jid){
  sessionData.cancelList=null; sessionData.stage=null; saveSession(phone, sessionData)
  await sendWithLog(sock, jid, BOOKING_SELF_SERVICE_MSG, {phone, intent:"cancel_redirect", action:"redirect"})
}
function looksLikeAppointmentInfoQuery(text){
  const t = norm(text)
  return /\b(mi|la|de)\s*cita\b/.test(t) && /\b(cuando|cuando es|hora|a que hora|donde|detall|info|confirm|ver|consultar)\b/.test(t)
      || /\b(confirmaci[oó]n|recordatorio|comprobante)\b/.test(t)
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
  <h1>🩷 Gapink Nails Bot v32.4.0 — Top ${SHOW_TOP_N}</h1>
  <div class="status ${conectado ? 'success' : 'error'}">WhatsApp: ${conectado ? "✅ Conectado" : "❌ Desconectado"}</div>
  ${!conectado&&lastQR?`<div style="text-align:center;margin:20px 0"><img src="/qr.png" width="300" style="border-radius:8px"></div>`:""}
  <div class="status warning">Modo: ${DRY_RUN ? "🧪 Simulación" : "🚀 Producción"} | IA: ${AI_PROVIDER.toUpperCase()}</div>
  <p>Staff por sede: respeta SQ_EMP_*_ALLOW (ej.: SQ_EMP_CARMEN_ALLOW=torremolinos).</p>
  </div>`)
})
app.get("/qr.png", async (_req,res)=>{
  if(!lastQR) return res.status(404).send("No QR")
  const png = await qrcode.toBuffer(lastQR, { type:"png", width:512, margin:1 })
  res.set("Content-Type","image/png").send(png)
})

async function loadBaileys(){
  const require = createRequire(import.meta.url); let mod=null
  try{ mod=require("@whiskeysockets/baileys") }catch{}; if(!mod){ mod=await import("@whiskeysockets/baileys") }
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
            strictStaffLock:false,
            pendingDateTime:null, lastHours:null, lastStaffByIso:{},
            lastProposeUsedPreferred:false, stage:null,
            identityChoices:null, identityResolvedCustomerId:null,
            cancelList:null,
            snooze_until_ms:null, name:null, email:null,
            lastServiceListSig:null, lastServiceListAt_ms:null,
            serviceChoices:null,
            lastAskAt:{}
          }
          if (isFromMe) { saveSession(phone, session); return }

          logEvent({direction:"in", action:"message", phone, raw_text:textRaw, stage:session.stage, extra:{isFromMe:false}})

          const now = nowEU()
          if (textRaw.trim()==="."){ session.snooze_until_ms = now.add(6,"hour").valueOf(); saveSession(phone, session); return }
          if (session.snooze_until_ms && now.valueOf() < session.snooze_until_ms) { saveSession(phone, session); return }

          if (!session.greeted){
            session.greeted=true; session.greetedAt_ms = Date.now(); saveSession(phone, session)
            await sendWithLog(sock, jid, buildGreeting(), {phone, intent:"greeting_once", action:"send_greeting"})
          }

          if (looksLikeAppointmentInfoQuery(textRaw)){
            await sendWithLog(sock, jid, BOOKING_SELF_SERVICE_MSG, {phone, intent:"booking_info_redirect", action:"redirect"})
            return
          }

          const t = norm(textRaw)
          const numMatch = t.match(/^\s*([1-9]\d*)\b/)
          const sedeMention = parseSede(textRaw)
          const catMention = parseCategory(textRaw)
          const temporal = parseTemporalPreference(textRaw)
          const explicitDT = parseExplicitDateTime(textRaw)

          // Equipo explícito -> limpia lock
          if (/\b(con el equipo|me da igual|cualquiera|con quien sea|lo que haya)\b/i.test(t)){
            session.preferredStaffId = null
            session.preferredStaffLabel = null
            session.strictStaffLock = false
            saveSession(phone, session)
            if (session.sede && session.selectedServiceEnvKey){
              await proposeTimes(session, phone, sock, jid, { text:textRaw })
            } else {
              const faltan=[]
              if (!session.sede) faltan.push("salón")
              if (!session.category) faltan.push("categoría")
              if (!session.selectedServiceEnvKey) faltan.push("servicio")
              if (canAsk(session,"faltan")){ noteAsk(session,"faltan"); saveSession(phone, session)
                await sendWithLog(sock, jid, `Perfecto, te paso huecos del equipo en cuanto me digas ${faltan.join(", ")}.`, {phone, intent:"need_data", action:"guide"}) }
            }
            return
          }

          // ====== BLOQUE NÚMERO (robusto)
          if (session.stage==="awaiting_identity_pick" && numMatch){
            const n = Number(numMatch[1])
            const choice = (session.identityChoices||[]).find(c=>c.index===n)
            if (!choice){ await sendWithLog(sock, jid, "No encontré esa opción. Responde con el número de tu ficha.", {phone, intent:"bad_pick", action:"guide", stage:session.stage}); return }
            session.identityResolvedCustomerId = choice.id
            session.stage = null
            saveSession(phone, session)
            await sendWithLog(sock, jid, "¡Gracias! Te paso el *resumen* para que lo coja una empleada…", {phone, intent:"identity_ok", action:"info"})
            await executeCreateBooking(session, phone, sock, jid)
            return
          }
          if (numMatch && Array.isArray(session.serviceChoices) && session.serviceChoices.length){
            const n = Number(numMatch[1])
            const choice = session.serviceChoices.find(it=>it.index===n)
            if (choice){
              session.selectedServiceEnvKey = choice.key
              session.selectedServiceLabel = choice.label
              session.serviceChoices = null
              session.stage = null
              saveSession(phone, session)
              await proposeTimes(session, phone, sock, jid, { text:"", forceStaffId: session.preferredStaffId || null, strictPreferred: session.strictStaffLock })
              return
            }
          }
          if ((!session.stage || session.stage==="awaiting_time") && numMatch && Array.isArray(session.lastHours) && session.lastHours.length){
            const idx = Number(numMatch[1]) - 1
            const pick = session.lastHours[idx]
            if (!dayjs.isDayjs(pick)){ await sendWithLog(sock, jid, "No encontré esa opción. Responde con el número válido.", {phone, intent:"bad_time_pick", action:"guide", stage:session.stage}); return }
            const iso = pick.format("YYYY-MM-DDTHH:mm")
            const staffFromIso = session?.lastStaffByIso?.[iso] || null
            session.pendingDateTime = pick.tz(EURO_TZ).toISOString()
            if (staffFromIso){ session.preferredStaffId = staffFromIso; session.preferredStaffLabel = staffLabelFromId(staffFromIso) }
            saveSession(phone, session)
            await sendWithLog(sock, jid, "¡Perfecto! Te paso el *resumen* para que lo coja una empleada…", {phone, intent:"time_selected", action:"info"})
            await executeCreateBooking(session, phone, sock, jid)
            return
          }
          // ====== FIN BLOQUE NÚMERO

          if (sedeMention) { session.sede = sedeMention; saveSession(phone, session) }
          if (catMention)  { session.category = catMention; saveSession(phone, session) }

          // Fuzzy staff (explícito): lock estricto
          const fuzzy = fuzzyStaffFromText(textRaw)
          if (fuzzy){
            if (fuzzy.anyTeam){
              session.preferredStaffId = null
              session.preferredStaffLabel = null
              session.strictStaffLock = false
              saveSession(phone, session)
              if (session.sede && session.selectedServiceEnvKey){
                await proposeTimes(session, phone, sock, jid, { text:textRaw })
              } else {
                const faltan=[]; if (!session.sede) faltan.push("salón"); if (!session.category) faltan.push("categoría"); if (!session.selectedServiceEnvKey) faltan.push("servicio")
                if (canAsk(session,"faltan2")){ noteAsk(session,"faltan2"); saveSession(phone, session)
                  await sendWithLog(sock, jid, `Perfecto, te paso huecos del equipo en cuanto me digas ${faltan.join(", ")}.`, {phone, intent:"prefer_team_again", action:"guide"}) }
              }
              return
            }
            session.preferredStaffId = fuzzy.id
            session.preferredStaffLabel = staffLabelFromId(fuzzy.id)
            session.strictStaffLock = true
            saveSession(phone, session)
            if (session.sede && session.selectedServiceEnvKey){
              await proposeTimes(session, phone, sock, jid, { text:textRaw, forceStaffId: session.preferredStaffId, strictPreferred: true })
              return
            }
          } else {
            const unknownNameMatch = /(?:^|\s)con\s+([a-zñáéíóúüï\s]{2,})\??$/i.exec(textRaw)
            if (unknownNameMatch){
              const alternativas = Array.from(new Set(EMPLOYEES.filter(e=> e.bookable).map(e=>titleCase(e.labels[0])))).slice(0,6)
              await sendWithLog(sock, jid, `No tengo a *${titleCase(unknownNameMatch[1].trim())}* en el equipo. Disponibles: ${alternativas.join(", ")}.`, {phone, intent:"unknown_staff", action:"guide"})
              return
            }
          }

          // Fecha/hora explícita + datos → resumen directo
          if (explicitDT && session.sede && session.selectedServiceEnvKey){
            session.pendingDateTime = explicitDT.toISOString()
            saveSession(phone, session)
            await sendWithLog(sock, jid, "¡Perfecto! Te paso el *resumen* para que lo coja una empleada…", {phone, intent:"explicit_dt", action:"info"})
            await executeCreateBooking(session, phone, sock, jid)
            return
          }

          // “horario”
          if (/\b(horario|agenda|est[áa]\s+semana|esta\s+semana|pr[oó]xima\s+semana|semana\s+que\s+viene|7\s+d[ií]as|siete\s+d[ií]as)\b/i.test(t)){
            if (!session.selectedServiceEnvKey){
              if (!session.category){
                if (canAsk(session,"categoria")){ noteAsk(session,"categoria"); saveSession(phone, session)
                  await sendWithLog(sock, jid, "Antes del horario, dime *categoría* (Uñas / Depilación / Micropigmentación / Faciales / Pestañas).", {phone, intent:"ask_category_for_schedule", action:"guide"}) }
                return
              }
              if (!session.sede){
                if (canAsk(session,"sede")){ noteAsk(session,"sede"); saveSession(phone, session)
                  await sendWithLog(sock, jid, "¿En qué *salón* te viene mejor? *Torremolinos* o *La Luz*.", {phone, intent:"ask_sede_for_schedule", action:"guide"}) }
                return
              }
              const itemsRaw = listServicesByCategory(session.sede, session.category, textRaw)
              if (!itemsRaw.length){ await sendWithLog(sock, jid, `No tengo servicios de *${session.category}* en ${locationNice(session.sede)}.`, {phone, intent:"no_services_in_cat", action:"guide"}); return }
              const list = itemsRaw.slice(0,22).map((s,i)=>({ index:i+1, key:s.key, label:s.label }))
              session.serviceChoices = list
              session.stage = "awaiting_service_choice"
              const sig = serviceListSignature({sede:session.sede, category:session.category, list})
              if (shouldSuppressServiceList(session, sig)){
                await sendWithLog(sock, jid, `Te pasé la lista arriba 👆. Responde con el *número* del servicio para ver el horario.`, {phone, intent:"anti_repeat_list", action:"guide", stage:session.stage})
              } else {
                saveSession(phone, session); noteServiceListSignature(session, sig, phone)
                const lines = list.map(it=> `${it.index}) ${it.label}`).join("\n")
                await sendWithLog(sock, jid, `Elige el *servicio* para mostrarte el horario semanal en ${locationNice(session.sede)}:\n\n${lines}\n\nResponde con el número.`, {phone, intent:"ask_service_number", action:"guide", stage:session.stage})
              }
              return
            }
            await weeklySchedule(session, phone, sock, jid, {
              nextWeek: temporal.nextWeek,
              usePreferred: true,
              forceStaffId: session.strictStaffLock ? session.preferredStaffId : null
            })
            return
          }

          // Si ya hay salón+servicio y menciona día/franja → proponer
          if (session.sede && session.selectedServiceEnvKey && /\botro dia\b|\botro día\b|\bhoy\b|\bmanana\b|\bpasado\b|\blunes\b|\bmartes\b|\bmiercoles\b|\bjueves\b|\bviernes\b|\btarde\b|\bpor la manana\b|\bnoche\b/i.test(t)){
            await proposeTimes(session, phone, sock, jid, { text:textRaw, forceStaffId: session.preferredStaffId || null, strictPreferred: session.strictStaffLock })
            return
          }

          // IA para el resto
          const aiObj = await aiInterpret(textRaw, session)
          if (aiObj && typeof aiObj==="object"){
            const action = aiObj.action
            const p = aiObj.params || {}

            if ((action==="set_salon" || action==="set_sede") && p.sede){
              const lk = parseSede(String(p.sede))
              if (lk){ session.sede = lk; saveSession(phone, session) }
              if (!session.category){
                if (canAsk(session,"categoria")){ noteAsk(session,"categoria"); saveSession(phone, session)
                  await sendWithLog(sock, jid, "¿Qué *categoría* necesitas? *Uñas*, *Depilación*, *Micropigmentación*, *Faciales* o *Pestañas*.", {phone, intent:"ask_category", action:"guide"}) }
                return
              }
            }

            if (action==="set_category" && p.category){
              const cm = parseCategory(String(p.category))
              if (cm){ session.category = cm; saveSession(phone, session) }
              if (!session.sede){
                if (canAsk(session,"sede")){ noteAsk(session,"sede"); saveSession(phone, session)
                  await sendWithLog(sock, jid, "¿En qué *salón* te viene mejor? *Torremolinos* o *La Luz*.", {phone, intent:"ask_sede", action:"guide"}) }
                return
              }
            }

            if (action==="set_staff" && p.name){
              const byAI = fuzzyStaffFromText("con " + p.name)
              if (byAI && !byAI.anyTeam){
                session.preferredStaffId = byAI.id
                session.preferredStaffLabel = staffLabelFromId(byAI.id)
                session.strictStaffLock = true
                saveSession(phone, session)
                if (session.sede && session.selectedServiceEnvKey){
                  await proposeTimes(session, phone, sock, jid, { text:textRaw, forceStaffId: session.preferredStaffId, strictPreferred: true })
                  return
                }
              } else {
                session.preferredStaffId = null
                session.preferredStaffLabel = null
                session.strictStaffLock = false
                saveSession(phone, session)
              }
            }

            if (action==="choose_service_label" && p.label && session.sede){
              const ek = resolveEnvKeyFromLabelAndSede(p.label, session.sede)
              if (ek){
                session.selectedServiceEnvKey = ek
                session.selectedServiceLabel = p.label
                session.stage=null; saveSession(phone, session)
                await proposeTimes(session, phone, sock, jid, { text:textRaw, forceStaffId: session.preferredStaffId || null, strictPreferred: session.strictStaffLock })
                return
              }
            }

            if (action==="weekly_schedule"){
              if (!session.selectedServiceEnvKey){
                if (canAsk(session,"servicio")){ noteAsk(session,"servicio"); saveSession(phone, session)
                  await sendWithLog(sock, jid, "Dime el *servicio* y te muestro el horario semanal.", {phone, intent:"need_service_for_weekly", action:"guide"}) }
                return
              }
              await weeklySchedule(session, phone, sock, jid, {
                nextWeek: !!p.next_week,
                usePreferred: true,
                forceStaffId: session.strictStaffLock ? session.preferredStaffId : null
              })
              return
            }

            if (action==="propose_times"){
              if (!session.sede){
                if (canAsk(session,"sede")){ noteAsk(session,"sede"); saveSession(phone, session)
                  await sendWithLog(sock, jid, "¿En qué *salón* te viene mejor? *Torremolinos* o *La Luz*.", {phone, intent:"need_sede_for_times", action:"guide"}) }
                return
              }
              if (!session.category){
                if (canAsk(session,"categoria")){ noteAsk(session,"categoria"); saveSession(phone, session)
                  await sendWithLog(sock, jid, "¿Qué *categoría* necesitas? *Uñas*, *Depilación*, *Micropigmentación*, *Faciales* o *Pestañas*.", {phone, intent:"need_category_for_times", action:"guide"}) }
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
                  await sendWithLog(sock, jid, `Te pasé la lista arriba 👆. Responde con el *número*.`, {phone, intent:"anti_repeat_list", action:"guide", stage:session.stage})
                } else {
                  saveSession(phone, session); noteServiceListSignature(session, sig, phone)
                  const lines = list.map(it=> `${it.index}) ${it.label}`).join("\n")
                  await sendWithLog(sock, jid, `Opciones de *${session.category}* en ${locationNice(session.sede)}:\n\n${lines}\n\nResponde con el *número*.`, {phone, intent:"ask_service_number", action:"guide", stage:session.stage})
                }
                return
              }
              await proposeTimes(session, phone, sock, jid, { date_hint:p.date_hint, part_of_day:p.part_of_day, text:textRaw, forceStaffId: session.preferredStaffId || null, strictPreferred: session.strictStaffLock })
              return
            }

            if (action==="list_appointments"){ await executeListAppointments(session, phone, sock, jid); return }
            if (action==="cancel_appointment"){ await executeCancelAppointment(session, phone, sock, jid); return }
          } // fin IA válida

          // Faltan datos (con cooldown); NUNCA preguntamos “¿con quién?”
          if (!session.sede){
            session.stage="awaiting_sede"; saveSession(phone, session)
            if (canAsk(session,"sede")){ noteAsk(session,"sede"); saveSession(phone, session)
              await sendWithLog(sock, jid, "¿En qué *salón* te viene mejor? *Torremolinos* o *La Luz*.", {phone, intent:"ask_sede", action:"guide", stage:session.stage}) }
            return
          }
          if (!session.category){
            session.stage="awaiting_category"; saveSession(phone, session)
            if (canAsk(session,"categoria")){ noteAsk(session,"categoria"); saveSession(phone, session)
              await sendWithLog(sock, jid, "¿Qué *categoría* necesitas? *Uñas*, *Depilación*, *Micropigmentación*, *Faciales* o *Pestañas*.", {phone, intent:"ask_category", action:"guide", stage:session.stage}) }
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
              await sendWithLog(sock, jid, `Te pasé la lista arriba 👆. Responde con el *número*.`, {phone, intent:"anti_repeat_list", action:"guide", stage:session.stage})
            } else {
              saveSession(phone, session); noteServiceListSignature(session, sig, phone)
              const lines = list.map(it=> `${it.index}) ${it.label}`).join("\n")
              await sendWithLog(sock, jid, `Opciones de *${session.category}* en ${locationNice(session.sede)}:\n\n${lines}\n\nResponde con el *número*.`, {phone, intent:"ask_service_number", action:"guide", stage:session.stage})
            }
            return
          }
          if (explicitDT){
            session.pendingDateTime = explicitDT.toISOString()
            saveSession(phone, session)
            await sendWithLog(sock, jid, "¡Perfecto! Te paso el *resumen* para que lo coja una empleada…", {phone, intent:"explicit_dt_late", action:"info"})
            await executeCreateBooking(session, phone, sock, jid)
            return
          }
          if (/\botro dia\b|\botro día\b|\bhoy\b|\bmanana\b|\bpasado\b|\blunes\b|\bmartes\b|\bmiercoles\b|\bjueves\b|\bviernes\b|\btarde\b|\bpor la manana\b|\bnoche\b/i.test(t)){
            await proposeTimes(session, phone, sock, jid, { text:textRaw, forceStaffId: session.preferredStaffId || null, strictPreferred: session.strictStaffLock })
            return
          }
          await sendWithLog(sock, jid, buildGreeting(), {phone, intent:"fallback_greeting", action:"guide"})
        }catch(err){
          if (BOT_DEBUG) console.error(err)
          logEvent({direction:"sys", action:"handler_error", phone, error:{message:err?.message, stack:err?.stack}, success:0})
          await sendWithLog(globalThis.sock, messages?.[0]?.key?.remoteJid, "No te he entendido bien. ¿Puedes decirlo de otra forma? 😊", {phone, intent:"error_recover", action:"guide"})
        }
      })
      QUEUE.set(phone, job.finally(()=>{ if (QUEUE.get(phone)===job) QUEUE.delete(phone) }))
    })
  }catch(e){ setTimeout(() => startBot().catch(console.error), 5000) }
}

// ====== Arranque
console.log(`🩷 Gapink Nails Bot v32.4.0 — Top ${SHOW_TOP_N} (L–V · resumen manual · strictStaffLock · 52w lookup · sedes por staff)`)
const appListen = app.listen(PORT, ()=>{ startBot().catch(console.error) })
process.on("uncaughtException", (e)=>{ console.error("💥 uncaughtException:", e?.stack||e?.message||e) })
process.on("unhandledRejection", (e)=>{ console.error("💥 unhandledRejection:", e) })
process.on("SIGTERM", ()=>{ try{ appListen.close(()=>process.exit(0)) }catch{ process.exit(0) } })
process.on("SIGINT", ()=>{ try{ appListen.close(()=>process.exit(0)) }catch{ process.exit(0) } })
