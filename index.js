// index.js — Gapink Nails · v32.0.0 (Resumen IA sin números, horario sin nombres, pausa 6h en “ver”)
// Cambios clave:
// - IA conversa en natural: nada de “responde con números”.
// - Siempre generamos RESUMEN al final (+ guardado como intake para el equipo). No auto-book salvo que AUTO_BOOK="true".
// - Horario sin nombres por defecto; si piden a alguien, filtramos y mostramos como antes.
// - “.” (o variantes) silencia 6h si lo envía cliente o tú.
// - En “ver/consultar” cita: mostramos info y activamos silencio 6h para que conteste el equipo.
// - Logs mejorados: línea simple con phone=... para filtrar fácil en Railway.

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

// ====== Flags
const BOT_DEBUG = /^true$/i.test(process.env.BOT_DEBUG || "")
const DRY_RUN = /^true$/i.test(process.env.DRY_RUN || "")
const SQUARE_MAX_RETRIES = Number(process.env.SQUARE_MAX_RETRIES || 3)
const AUTO_BOOK = /^true$/i.test(process.env.AUTO_BOOK || "") // << por defecto false (no creamos)

// ====== Square
const square = new Client({
  accessToken: process.env.SQUARE_ACCESS_TOKEN,
  environment: (process.env.SQUARE_ENV==="production") ? Environment.Production : Environment.Sandbox
})
const LOC_TORRE = (process.env.SQUARE_LOCATION_ID_TORREMOLINOS || "").trim()
const LOC_LUZ   = (process.env.SQUARE_LOCATION_ID_LA_LUZ || "").trim()
const ADDRESS_TORRE = process.env.ADDRESS_TORREMOLINOS || "Av. de Benyamina 18, Torremolinos"
const ADDRESS_LUZ   = process.env.ADDRESS_LA_LUZ || "Málaga – Barrio de La Luz"

// ====== IA (Deepseek / OpenAI)
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
    if (AI_PROVIDER==="deepseek"){
      const resp = await fetch("https://api.deepseek.com/chat/completions",{
        method:"POST",
        headers:{ "Content-Type":"application/json", "Authorization":`Bearer ${DEEPSEEK_API_KEY}` },
        body: JSON.stringify({ model: DEEPSEEK_MODEL, messages, temperature:0.2, max_tokens:900 }),
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
        body: JSON.stringify({ model: OPENAI_MODEL, messages, temperature:0.2, max_tokens:900 }),
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
function enumerateHours(list){ return list.map((d)=>({ iso:d.format("YYYY-MM-DDTHH:mm"), pretty:fmtES(d) })) }
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
CREATE TABLE IF NOT EXISTS intakes (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  phone TEXT,
  intent TEXT,
  summary TEXT,
  data_json TEXT,
  created_at TEXT
);
`)
const insertAppt = db.prepare(`INSERT INTO appointments
(id,customer_name,customer_phone,customer_square_id,location_key,service_env_key,service_label,duration_min,start_iso,end_iso,staff_id,status,created_at,square_booking_id,square_error,retry_count)
VALUES (@id,@customer_name,@customer_phone,@customer_square_id,@location_key,@service_env_key,@service_label,@duration_min,@start_iso,@end_iso,@staff_id,@status,@created_at,@square_booking_id,@square_error,@retry_count)`)

const insertSquareLog = db.prepare(`INSERT INTO square_logs
(phone, action, request_data, response_data, error_data, timestamp, success)
VALUES (@phone, @action, @request_data, @response_data, @error_data, @timestamp, @success)`)

const insertIntake = db.prepare(`INSERT INTO intakes
(phone,intent,summary,data_json,created_at)
VALUES (@phone,@intent,@summary,@data_json,@created_at)`)

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
  // Línea fácil de buscar por número en Railway:
  try{
    const rawShort = (raw_text||"").replace(/\s+/g," ").slice(0,120)
    const repShort = (reply_text||"").replace(/\s+/g," ").slice(0,120)
    console.log(`phone=${phone||"unknown"} dir=${direction} action=${action||"event"} intent=${intent||""} stage=${stage||""} raw="${rawShort}" reply="${repShort}"`)
  }catch{}
  // JSON bonito por si quieres parsear
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

async function sendWithLog(sock, jid, text, {phone, intent, action, stage, extra}={}){
  logEvent({direction:"out", action: action||"send", phone, intent:intent||null, stage:stage||null, raw_text:null, reply_text:text, extra: {payload:{text}, ...(extra||{})}})
  try{ await sock.sendMessage(jid, { text }) }
  catch(e){ logEvent({direction:"sys", action:"send_error", phone, intent, stage, raw_text:text, error:{message:e?.message}}) }
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

// ====== Empleadas (global)
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
function staffLabelFromId(id){ const e = EMPLOYEES.find(x=>x.id===id); return e?.labels?.[0] || (id ? `Prof. ${String(id).slice(-4)}` : null) }
function isStaffAllowedInLocation(staffId, _locKey){ const e = EMPLOYEES.find(x=>x.id===staffId); return !!(e && e.bookable) }
function pickStaffForLocation(_locKey, preferId=null){
  if (preferId){
    const e = EMPLOYEES.find(x=>x.id===preferId && x.bookable)
    if (e) return e.id
  }
  const found = EMPLOYEES.find(e=>e.bookable)
  return found?.id || null
}

// ====== Aliases de nombres
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
  return EMPLOYEES.map(e=>`• ID:${e.id} | Nombres:[${e.labels.join(", ")}] | Reservable:${e.bookable}`).join("\n")
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

// ====== Square helpers
async function searchCustomersByPhone(phone){
  try{
    const e164=normalizePhoneES(phone); if(!e164) return []
    const got = await square.customersApi.searchCustomers({ query:{ filter:{ phoneNumber:{ exact:e164 } } } })
    return got?.result?.customers || []
  }catch{ return [] }
}

// ====== Disponibilidad
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
      const start = d.clone().hour(part==="mañana"?9:part==="tarde"?15:18).minute(0)
      const end   = d.clone().hour(part==="mañana"?13:part==="tarde"?20:20).minute(0)
      if (!(d.isAfter(start.subtract(1,"minute")) && d.isBefore(end.add(1,"minute")))) continue
    }
    out.push({ date:d, staffId: tm || null })
    if (out.length>=limit) break
  }
  out.sort((a,b)=>a.date.valueOf()-b.date.valueOf())
  return out
}
async function searchAvailWindowExtended({ locationKey, envServiceKey, startEU, staffId, maxDays=30 }){
  const results = []
  const endDate = startEU.clone().add(maxDays, 'day')
  let currentStart = startEU.clone()
  while (currentStart.isBefore(endDate) && results.length < 1000) {
    let currentEnd = currentStart.clone().add(7, 'day')
    if (currentEnd.isAfter(endDate)) currentEnd = endDate.clone()
    const weekSlots = await searchAvailWindow({
      locationKey, envServiceKey, startEU: currentStart, endEU: currentEnd, limit: 500
    })
    const filteredSlots = staffId ? weekSlots.filter(s => s.staffId === staffId) : weekSlots
    results.push(...filteredSlots)
    currentStart = currentEnd.clone()
    await sleep(100)
  }
  results.sort((a,b)=>a.date.valueOf()-b.date.valueOf())
  return results
}

// ====== IA: interpretación + resumen
function buildAISystemPrompt(session){
  const nowTxt = nowEU().format("dddd DD/MM/YYYY HH:mm")
  const staffLines = staffRosterForPrompt()
  const torremolinos_services = servicesForSedeKeyRaw("torremolinos").map(s=>s.label).join(", ")
  const laluz_services = servicesForSedeKeyRaw("la_luz").map(s=>s.label).join(", ")

  return `Eres el asistente de WhatsApp de Gapink Nails. Devuelves SOLO JSON válido.
- Tu misión: interpretar lenguaje natural (sin listas numeradas) y extraer un RESUMEN estructurado.
- Intenciones: "book" (reservar), "view" (ver/consultar), "edit" (cambiar), "cancel" (cancelar), "info" (preguntas generales), "other".
- Extrae: sede (torremolinos|la_luz), category (uñas|depilación|micropigmentación|faciales|pestañas), service_label (si lo hay),
  staff_name (si pide con alguien), date_hint (texto: "viernes", "hoy 16:00", "tarde", "próxima semana"),
  part_of_day (mañana|tarde|noche|null), next_week (bool), explicit_time ("HH:MM" o null).
- Si detectas “ver mi cita” o similares, intención "view".
- Si pide con alguien, conserva staff_name exacto.
- Nunca pidas que responda con números.

Contexto:
- Fecha/hora actual (Madrid): ${nowTxt}
- Salones: Torremolinos (${ADDRESS_TORRE}) y Málaga – La Luz (${ADDRESS_LUZ})
- Profesionales: 
${staffLines}

Servicios Torremolinos (muestras): ${torremolinos_services}
Servicios La Luz (muestras): ${laluz_services}

FORMATO JSON:
{
 "intent":"book|view|edit|cancel|info|other",
 "sede": "torremolinos|la_luz|null",
 "category": "uñas|depilación|micropigmentación|faciales|pestañas|null",
 "service_label": "Texto o null",
 "staff_name": "Texto o null",
 "date_hint": "Texto o null",
 "part_of_day": "mañana|tarde|noche|null",
 "next_week": true|false,
 "explicit_time": "HH:MM|null",
 "notes": "breve resumen en 1 línea"
}`
}
async function aiInterpret(textRaw, session){
  if (AI_PROVIDER==="none") return null
  const sys = buildAISystemPrompt(session)
  const history = Array.isArray(session.history)? session.history.slice(-6).join(" • ") : ""
  const prompt = `Conversación (últimos mensajes): ${history}\nMensaje: "${textRaw}"\nDevuelve SOLO JSON.`
  const out = await aiChat(sys, prompt)
  return stripToJSON(out)
}

// ====== Helpers “sin números”
function proposeLines(slots, {showStaffNames=false}={}){
  const arr = enumerateHours(slots.map(s=>s.date))
  const lines = arr.map(h => {
    const sid = slots.find(x=>x.date.format("YYYY-MM-DDTHH:mm")===h.iso)?.staffId || null
    const tag = (showStaffNames && sid) ? ` — ${staffLabelFromId(sid)}` : ""
    return `• ${h.pretty}${tag}`
  }).join("\n")
  return { lines, arr }
}
function pickServiceByText(session, text){
  const t = norm(text)
  const list = Array.isArray(session.serviceChoices)? session.serviceChoices : []
  // Coincidencia exacta/fuzzy por etiqueta
  let hit = list.find(it => norm(it.label)===t)
  if (hit) return hit
  hit = list.find(it => norm(it.label).includes(t)) || list.find(it => t.includes(norm(it.label)))
  if (hit) return hit
  // plan B: buscar en todos los servicios de esa sede
  if (session.sede){
    const all = servicesForSedeKeyRaw(session.sede)
    const h2 = all.find(s => norm(s.label)===t) || all.find(s => norm(s.label).includes(t))
    if (h2) return { key:h2.key, label:h2.label }
  }
  return null
}
function parseExplicitTime(text){
  const s = text.toLowerCase()
  // 16:30 / 16.30 / 16h / a las 16
  let m = s.match(/\b([01]?\d|2[0-3])[:h\.]?([0-5]\d)?\b/)
  if (m){
    const hh = Number(m[1]); const mm = m[2]? Number(m[2]) : (s.includes("y media")?30: s.includes("y cuarto")?15: 0)
    return { hh, mm }
  }
  if (/y\s+media/.test(s)){
    m = s.match(/\b([01]?\d|2[0-3])\b/); if (m) return { hh:Number(m[1]), mm:30 }
  }
  if (/y\s+cuarto/.test(s)){
    m = s.match(/\b([01]?\d|2[0-3])\b/); if (m) return { hh:Number(m[1]), mm:15 }
  }
  if (/menos\s+cuarto/.test(s)){
    m = s.match(/\b([01]?\d|2[0-3])\b/); if (m){ let hh=Number(m[1])-1; if (hh<0) hh=0; return { hh, mm:45 } }
  }
  return null
}
function matchTimeAgainstLast(session, text){
  const t = parseExplicitTime(text)
  if (!t || !Array.isArray(session.lastHours) || !session.lastHours.length) return null
  const wanted = `${String(t.hh).padStart(2,"0")}:${String(t.mm).padStart(2,"0")}`
  const hit = session.lastHours.find(d => fmtHour(d)===wanted)
  return hit || null
}

// ====== Propuestas de horas (sin números)
async function proposeTimes(sessionData, phone, sock, jid, { text=null, date_hint=null, part_of_day=null }={}){
  const now = nowEU();
  const baseFrom = nextOpeningFrom(now.add(NOW_MIN_OFFSET_MIN, "minute"))
  const days = SEARCH_WINDOW_DAYS

  let when=null, part=null
  if (date_hint || part_of_day){
    if (date_hint){
      const t = norm(String(date_hint))
      const mapDia = { "lunes":1,"martes":2,"miercoles":3,"miércoles":3,"jueves":4,"viernes":5,"sabado":6,"sábado":6,"domingo":0 }
      if (/\bhoy\b/.test(t)) when = now
      else if (/\bmanana\b/.test(t)) when = now.add(1,"day")
      else if (/\bpasado\b/.test(t)) when = now.add(2,"day")
      else {
        for (const k of Object.keys(mapDia)){ if (t.includes(k)) { let d=now.clone(); while (d.day()!==mapDia[k]) d=d.add(1,"day"); when=d; break } }
      }
      part = part_of_day || (/\btarde\b/.test(t)? "tarde" : /\bmañana\b/.test(t)? "mañana" : /\bnoche\b/.test(t)? "noche" : null)
    } else { part = part_of_day || null }
  } else if (text){
    const t = norm(text)
    if (/\bhoy\b/.test(t)) when = now
    else if (/\bmanana\b/.test(t)) when = now.add(1,"day")
    else if (/\bpasado\b/.test(t)) when = now.add(2,"day")
    else {
      const mapDia = { "lunes":1,"martes":2,"miercoles":3,"miércoles":3,"jueves":4,"viernes":5,"sabado":6,"sábado":6,"domingo":0 }
      for (const k of Object.keys(mapDia)){ if (t.includes(k)) { let d=now.clone(); while (d.day()!==mapDia[k]) d=d.add(1,"day"); when=d; break } }
    }
    part = /\btarde\b/.test(t)? "tarde" : /\bmañana\b/.test(t)? "mañana" : /\bnoche\b/.test(t)? "noche" : null
  }

  let startEU = when ? when.clone().hour(OPEN.start).minute(0) : baseFrom.clone()
  let endEU   = when ? when.clone().hour(OPEN.end).minute(0)   : baseFrom.clone().add(days,"day")

  if (!sessionData.sede || !sessionData.selectedServiceEnvKey){
    await sendWithLog(sock, jid, "Antes de horarios, dime *salón* y *servicio*.", {phone, intent:"need_sede_service", action:"guide", stage:sessionData.stage})
    return
  }

  const rawSlots = await searchAvailWindow({
    locationKey: sessionData.sede,
    envServiceKey: sessionData.selectedServiceEnvKey,
    startEU, endEU, limit: 500, part
  })

  let slots = rawSlots
  let usedPreferred = false
  if (sessionData.preferredStaffId){
    slots = rawSlots.filter(s => s.staffId === sessionData.preferredStaffId)
    usedPreferred = true
    if (!slots.length) {
      const ext = await searchAvailWindowExtended({
        locationKey: sessionData.sede,
        envServiceKey: sessionData.selectedServiceEnvKey,
        startEU: startEU, staffId: sessionData.preferredStaffId, maxDays: 30
      })
      if (ext.length) { slots = ext; usedPreferred = true } else { slots = rawSlots; usedPreferred = false }
    }
  }
  slots.sort((a,b)=>a.date.valueOf()-b.date.valueOf())

  if (!slots.length){
    await sendWithLog(sock, jid, `No veo huecos en ese rango. Dime otra fecha o franja (“viernes tarde”, “próxima semana”).`, {phone, intent:"no_slots", action:"guide"})
    return
  }

  const shown = slots.slice(0, SHOW_TOP_N)
  sessionData.lastHours = shown.map(s => s.date)
  sessionData.lastStaffByIso = Object.fromEntries(shown.map(s=>[s.date.format("YYYY-MM-DDTHH:mm"), s.staffId||null]))
  sessionData.lastProposeUsedPreferred = usedPreferred
  sessionData.stage = "awaiting_time_text"
  saveSession(phone, sessionData)

  const header = usedPreferred
    ? `Huecos con ${sessionData.preferredStaffLabel || "tu profesional"} (primeras ${SHOW_TOP_N}):`
    : `Huecos del equipo (primeras ${SHOW_TOP_N}):`
  const { lines } = proposeLines(shown, { showStaffNames: !!sessionData.preferredStaffId })
  await sendWithLog(sock, jid, `${header}\n${lines}\n\nEscríbeme la *hora en texto* (“me vale 11:30” o “otra tarde”).`, {phone, intent:"times_list_text", action:"guide", stage:sessionData.stage})
}

// ====== Horario semanal (sin números)
async function weeklySchedule(sessionData, phone, sock, jid, { nextWeek=false }={}){
  if (!sessionData.sede){
    await sendWithLog(sock, jid, "¿En qué *salón* te viene mejor? *Torremolinos* o *La Luz*.", {phone, intent:"ask_sede", action:"guide"})
    return
  }
  if (!sessionData.selectedServiceEnvKey){
    await sendWithLog(sock, jid, "Dime el *servicio* (o la *categoría* para listarte opciones) y te muestro el horario semanal.", {phone, intent:"ask_service", action:"guide"})
    return
  }
  const now = nowEU()
  const startEU = nextWeek ? now.clone().add(1,"week").isoWeekday(1).hour(OPEN.start).minute(0) : nextOpeningFrom(now.add(NOW_MIN_OFFSET_MIN,"minute"))
  const endEU = startEU.clone().add(7,"day").hour(OPEN.end).minute(0)

  const rawSlots = await searchAvailWindow({
    locationKey: sessionData.sede,
    envServiceKey: sessionData.selectedServiceEnvKey,
    startEU, endEU, limit: 500
  })
  if (!rawSlots.length){
    await sendWithLog(sock, jid, `No encuentro huecos en ese rango. ¿Miro otra semana o cambiamos franja?`, {phone, intent:"no_weekly_slots", action:"guide"})
    return
  }

  let slots = rawSlots
  if (sessionData.preferredStaffId){
    const f = rawSlots.filter(s => s.staffId === sessionData.preferredStaffId)
    if (f.length) slots = f
  }
  slots.sort((a,b)=>a.date.valueOf()-b.date.valueOf())
  const limited = slots.slice(0, SHOW_TOP_N)

  sessionData.lastHours = limited.map(s=>s.date)
  sessionData.lastStaffByIso = Object.fromEntries(limited.map(s=>[s.date.format("YYYY-MM-DDTHH:mm"), s.staffId||null]))
  sessionData.lastProposeUsedPreferred = !!sessionData.preferredStaffId
  sessionData.stage = "awaiting_time_text"
  saveSession(phone, sessionData)

  const header = `🗓️ Horario ${nextWeek? "de la *próxima semana*":"de los *próximos 7 días*"} — ${locationNice(sessionData.sede)}`
  const { lines } = proposeLines(limited, { showStaffNames: !!sessionData.preferredStaffId })
  await sendWithLog(sock, jid, `${header}\n${lines}\n\nDime la *hora* que te encaja (“me viene 10:00”).`, {phone, intent:"weekly_list_text", action:"guide", stage:sessionData.stage})
}

// ====== Crear reserva (solo si AUTO_BOOK=true)
async function executeCreateBooking(sessionData, phone, sock, jid){
  if (!AUTO_BOOK){
    await sendWithLog(sock, jid, "He registrado tu solicitud. *Una compañera te dará el OK* enseguida 💅", {phone, intent:"intake_recorded", action:"info"})
    return
  }
  if (!sessionData.sede || !sessionData.selectedServiceEnvKey || !sessionData.pendingDateTime){
    await sendWithLog(sock, jid, "Falta algún dato para crear la reserva (salón/servicio/fecha).", {phone, intent:"missing_data_create", action:"guide"}); 
    return
  }
  const startEU = parseToEU(sessionData.pendingDateTime)
  if (!insideBusinessHours(startEU, 60)){
    await sendWithLog(sock, jid, "Esa hora está fuera del horario (L–V 09:00–20:00).", {phone, intent:"outside_hours", action:"guide"}); 
    return
  }
  let staffId = sessionData.lastProposeUsedPreferred ? (sessionData.preferredStaffId || sessionData.lastStaffByIso?.[startEU.format("YYYY-MM-DDTHH:mm")] || null)
                                                    : (sessionData.lastStaffByIso?.[startEU.format("YYYY-MM-DDTHH:mm")] || sessionData.preferredStaffId || null)
  if (staffId && !isStaffAllowedInLocation(staffId, sessionData.sede)) staffId = null
  if (!staffId) {
    const probe = await searchAvailWindow({
      locationKey: sessionData.sede,
      envServiceKey: sessionData.selectedServiceEnvKey,
      startEU: startEU.clone().subtract(5,"minute"),
      endEU: startEU.clone().add(5,"minute"),
      limit: 3
    })
    const match = probe.find(x => x.date.isSame(startEU, "minute"))
    if (match?.staffId && isStaffAllowedInLocation(match.staffId, sessionData.sede)) staffId = match.staffId
  }
  if (!staffId) staffId = pickStaffForLocation(sessionData.sede, null)
  if (!staffId){ await sendWithLog(sock, jid, "No hay profesionales disponibles ahora mismo.", {phone, intent:"no_staff", action:"guide"}); return }

  // Cliente (optativo si ya existe)
  let customerId = null
  try{
    const found = await searchCustomersByPhone(phone)
    customerId = found[0]?.id || null // da igual si hay 2: cogemos la primera
  }catch{}
  const result = await (async ()=>{
    if (DRY_RUN) return { success:true, booking:{ id:`TEST_SIM_${Date.now()}`, __sim:true } }
    const sv = await getServiceIdAndVersion(sessionData.selectedServiceEnvKey)
    if (!sv?.id) return { success:false, error:"Servicio inválido" }
    const startISO = startEU.tz("UTC").toISOString()
    const idempotencyKey = stableKey({ loc:locationToId(sessionData.sede), sv:sv.id, startISO, customerId, staffId })
    try{
      const requestData = {
        idempotencyKey,
        booking:{
          locationId: locationToId(sessionData.sede),
          startAt: startISO,
          customerId: customerId || undefined,
          appointmentSegments:[{
            teamMemberId: staffId,
            serviceVariationId: sv.id,
            serviceVariationVersion: Number(sv.version),
            durationMinutes: 60
          }]
        }
      }
      const resp = await square.bookingsApi.createBooking(requestData)
      return { success:true, booking: resp?.result?.booking||null }
    }catch(e){ return { success:false, error:e?.message } }
  })()
  if (!result.success){
    await sendWithLog(sock, jid, "No pude crear la reserva ahora. La dejamos como *pendiente* para que te confirme una compañera.", {phone, intent:"create_failed", action:"guide"})
    return
  }
  const confirmMessage = `🎉 ¡Reserva creada!
📍 ${locationNice(sessionData.sede)}
🧾 ${serviceLabelFromEnvKey(sessionData.selectedServiceEnvKey) || sessionData.selectedServiceLabel || "Servicio"}
🕐 ${fmtES(startEU)}`
  await sendWithLog(sock, jid, confirmMessage, {phone, intent:"booking_confirmed", action:"confirm"})
}

// ====== Info/Cancel/Edit helpers
const BOOKING_SELF_SERVICE_MSG = "Para *consultar, editar o cancelar* tu cita usa el enlace del *email/SMS de confirmación*. Ahí puedes verlo y gestionarlo al instante ✅"

async function searchExistingBookings(phone, fromDate = null) {
  try {
    const e164 = normalizePhoneES(phone)
    if (!e164) return []
    const customers = await searchCustomersByPhone(phone)
    if (!customers.length) return []
    const customerId = customers[0].id
    const startAt = fromDate ? fromDate.tz("UTC").toISOString() : nowEU().tz("UTC").toISOString()
    const body = { query: { filter: { customerId, startAtRange: { startAt } } } }
    let retries = 0
    while (retries < SQUARE_MAX_RETRIES) {
      try {
        const resp = await square.bookingsApi.searchBookings(body)
        const bookings = resp?.result?.bookings || []
        insertSquareLog.run({
          phone: phone || 'unknown',
          action: 'search_existing_bookings',
          request_data: safeJSONStringify(body),
          response_data: safeJSONStringify(resp?.result || {}),
          error_data: null,
          timestamp: new Date().toISOString(),
          success: 1
        })
        return bookings.filter(b => b.status !== 'CANCELLED' && b.status !== 'DECLINED')
      } catch (e) {
        retries++; if (retries >= SQUARE_MAX_RETRIES) { 
          insertSquareLog.run({ phone: phone || 'unknown', action:'search_existing_bookings', request_data:safeJSONStringify(body), response_data:null, error_data:safeJSONStringify({message:e?.message}), timestamp:new Date().toISOString(), success:0 })
          return [] 
        }
        await sleep(1000 * retries)
      }
    }
    return []
  } catch { return [] }
}

// ====== Mensajes base
function buildGreeting(){
  return `¡Hola! Soy el asistente de Gapink Nails 💅
Cuéntame en tus palabras qué necesitas (salón, servicio, día/franja y si quieres con alguien). Yo te lo resumo y te paso opciones.

Horario atención humana: L–V 10–14 y 16–20.`
}
function composeSummary({ sede, category, service, staff, timeHint, pickedTime, hasFicha, needsIdentity }){
  const parts=[]
  parts.push(`Resumen:`)
  parts.push(`• Salón: ${sede? locationNice(sede): "—"}`)
  parts.push(`• ${service? "Servicio":"Categoría"}: ${service || (category||"—")}`)
  parts.push(`• Profesional: ${staff? staff : "Equipo"}`)
  if (pickedTime) parts.push(`• Hora elegida: ${fmtES(pickedTime)}`)
  else if (timeHint) parts.push(`• Preferencia: ${timeHint}`)
  parts.push(`• Ficha en sistema: ${hasFicha? "sí" : "no"}`)
  if (needsIdentity) parts.push(`• Datos: dime *nombre completo* y (opcional) *email* para crearte ficha`)
  parts.push(`\nAhora una de las compañeras revisa y te da el OK ✅`)
  return parts.join("\n")
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
  <div class="status warning">Modo: ${DRY_RUN ? "Simulación" : "Producción"} | IA: ${AI_PROVIDER.toUpperCase()} | Auto-book: ${AUTO_BOOK}</div>
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
            snooze_until_ms:null, name:null, email:null,
            serviceChoices:null, history:[]
          }

          const now = nowEU()

          // Admin/cliente: silencio con puntitos
          if (/^[\s.·•⋅]+$/.test(textRaw)){
            session.snooze_until_ms = now.add(6,"hour").valueOf()
            saveSession(phone, session)
            logEvent({direction:"sys", action:isFromMe?"admin_snooze_6h":"snooze_6h", phone, raw_text:textRaw})
            // Si lo envías tú, no respondemos nada; si lo envía cliente, tampoco.
            return
          }

          if (isFromMe) { saveSession(phone, session); return }

          // Si está en silencio, no respondemos
          if (session.snooze_until_ms && now.valueOf() < session.snooze_until_ms) { saveSession(phone, session); logEvent({direction:"sys", action:"snoozing_skip", phone}); return }

          // Log IN
          logEvent({direction:"in", action:"message", phone, raw_text:textRaw, stage:session.stage, extra:{isFromMe:false}})
          // Historial corto para IA
          session.history = (session.history||[]).concat([textRaw]).slice(-10)
          saveSession(phone, session)

          // Saludo cada 24h
          const lastGreetAt = session.greetedAt_ms || 0
          if (!session.greeted || (Date.now()-lastGreetAt) > 24*60*60*1000){
            session.greeted=true; session.greetedAt_ms = Date.now(); saveSession(phone, session)
            await sendWithLog(sock, jid, buildGreeting(), {phone, intent:"greeting_24h", action:"send_greeting"})
          }

          // IA: interpretar
          const ai = await aiInterpret(textRaw, session)
          logEvent({direction:"sys", action:"ai_interpret", phone, extra:{ai}})
          let intent = ai?.intent || "other"

          // Actualiza sede/categoría/servicio/staff
          if (ai?.sede){ const s = ai.sede==="la_luz"?"la_luz": ai.sede==="torremolinos"?"torremolinos":null; if (s){ session.sede=s } }
          if (ai?.category){ const c = parseCategory(ai.category)||ai.category; if (c) session.category = c }
          if (ai?.staff_name){
            const fz = fuzzyStaffFromText("con " + ai.staff_name)
            if (fz && !fz.anyTeam){ session.preferredStaffId=fz.id; session.preferredStaffLabel=staffLabelFromId(fz.id) }
          }
          if (ai?.service_label && session.sede){
            const ek = resolveEnvKeyFromLabelAndSede(ai.service_label, session.sede)
            if (ek){ session.selectedServiceEnvKey=ek; session.selectedServiceLabel=ai.service_label }
          }
          saveSession(phone, session)

          // Si pide VER/CONSULTAR: mostramos info y pausamos 6h
          if (intent==="view"){
            const existing = await searchExistingBookings(phone, nowEU())
            if (existing.length){
              const b=existing[0]
              const startTime = dayjs(b.startAt).tz(EURO_TZ)
              const locationName = b.locationId === LOC_LUZ ? "Málaga – La Luz" : "Torremolinos"
              const serviceName = b.appointmentSegments?.[0]?.serviceVariation?.name || "Servicio"
              const staffName = staffLabelFromId(b.appointmentSegments?.[0]?.teamMemberId) || "Equipo"
              const msg = `📅 Tu próxima cita:\n📍 ${locationName}\n🧾 ${serviceName}\n👩‍💼 ${staffName}\n🕐 ${fmtES(startTime)}\n\n${BOOKING_SELF_SERVICE_MSG}\n\nTe dejo 6h para que hable contigo una compañera 👩‍🦰`
              await sendWithLog(sock, jid, msg, {phone, intent:"booking_info_found", action:"info"})
            } else {
              await sendWithLog(sock, jid, `No encuentro citas próximas a tu nombre. ${BOOKING_SELF_SERVICE_MSG}\n\nTe atiende una compi en cuanto pueda.`, {phone, intent:"booking_info_not_found", action:"info"})
            }
            // Pausa 6h
            session.snooze_until_ms = now.add(6,"hour").valueOf()
            saveSession(phone, session)
            // Guardamos intake resumen
            const hasFicha = (await searchCustomersByPhone(phone)).length>0
            const summaryText = composeSummary({
              sede: session.sede, category: session.category,
              service: session.selectedServiceLabel, staff: session.preferredStaffLabel,
              timeHint: ai?.date_hint || ai?.part_of_day || null, pickedTime: null,
              hasFicha, needsIdentity: !hasFicha
            })
            insertIntake.run({
              phone, intent:"view", summary: summaryText,
              data_json: safeJSONStringify({ ai, session }),
              created_at: new Date().toISOString()
            })
            return
          }

          // CANCEL/EDIT -> resumen + autoservicio (sin pausa)
          if (intent==="cancel" || intent==="edit"){
            await sendWithLog(sock, jid, `${BOOKING_SELF_SERVICE_MSG}\nSi te parece, te hago un *resumen* para que lo revise una compañera.`, {phone, intent, action:"redirect"})
            const hasFicha = (await searchCustomersByPhone(phone)).length>0
            const summaryText = composeSummary({
              sede: session.sede, category: session.category,
              service: session.selectedServiceLabel, staff: session.preferredStaffLabel,
              timeHint: ai?.date_hint || ai?.part_of_day || null, pickedTime: null,
              hasFicha, needsIdentity: !hasFicha
            })
            await sendWithLog(sock, jid, summaryText, {phone, intent, action:"summary"})
            insertIntake.run({
              phone, intent, summary: summaryText,
              data_json: safeJSONStringify({ ai, session }),
              created_at: new Date().toISOString()
            })
            return
          }

          // Si ya tenemos sede + categoría pero NO servicio, listamos opciones (sin números)
          if (session.sede && session.category && !session.selectedServiceEnvKey){
            const itemsRaw = listServicesByCategory(session.sede, session.category, textRaw)
            if (!itemsRaw.length){
              await sendWithLog(sock, jid, `No tengo servicios de *${session.category}* en ${locationNice(session.sede)}. Prueba otra categoría.`, {phone, intent:"no_services_in_cat", action:"guide"})
            } else {
              const list = itemsRaw.slice(0,22).map((s)=>({ key:s.key, label:s.label }))
              session.serviceChoices = list
              session.stage = "awaiting_service_text"
              saveSession(phone, session)
              const bullets = list.map(it=>`• ${it.label}`).join("\n")
              await sendWithLog(sock, jid, `Opciones de *${session.category}* en ${locationNice(session.sede)}:\n\n${bullets}\n\nEscríbeme el *nombre del servicio* tal cual (o similar).`, {phone, intent:"ask_service_text", action:"guide", stage:session.stage})
            }
            // seguimos para resumen al final
          }

          // Si estamos esperando servicio por texto, intenta casar
          if (session.stage==="awaiting_service_text" && !session.selectedServiceEnvKey){
            const hit = pickServiceByText(session, textRaw)
            if (hit){
              session.selectedServiceEnvKey = hit.key
              session.selectedServiceLabel = hit.label
              session.stage = null
              saveSession(phone, session)
              await sendWithLog(sock, jid, `Perfecto: ${hit.label} en ${locationNice(session.sede)}.`, {phone, intent:"service_set", action:"info"})
            }
          }

          // Si tenemos servicio, proponemos horarios (sin nombres salvo staff preferida)
          if (session.sede && session.selectedServiceEnvKey){
            // Preferencia temporal
            const dateHint = ai?.date_hint || null
            const part = ai?.part_of_day || null
            // Si el texto contiene una hora concreta y tenemos slots recientes -> intenta match directo
            const direct = matchTimeAgainstLast(session, textRaw)
            if (direct){
              session.pendingDateTime = direct.tz(EURO_TZ).toISOString()
              saveSession(phone, session)
            } else if (!session.pendingDateTime){
              await proposeTimes(session, phone, sock, jid, { text:textRaw, date_hint:dateHint, part_of_day:part })
            }
          }

          // Si el user ha escrito una hora que no coincide con últimas, pero es válida, lo aceptamos como preferencia
          if (!session.pendingDateTime){
            const maybe = parseExplicitTime(textRaw)
            if (maybe && Array.isArray(session.lastHours) && session.lastHours.length){
              const wanted = `${String(maybe.hh).padStart(2,"0")}:${String(maybe.mm).padStart(2,"0")}`
              const hit = session.lastHours.find(d => fmtHour(d)===wanted)
              if (hit){ session.pendingDateTime = hit.tz(EURO_TZ).toISOString(); saveSession(phone, session) }
            }
          }

          // Si ya hay hora elegida, no creamos la cita por defecto: registramos intake
          if (session.pendingDateTime){
            const hasFicha = (await searchCustomersByPhone(phone)).length>0
            const picked = parseToEU(session.pendingDateTime)
            const summaryText = composeSummary({
              sede: session.sede, category: session.category,
              service: session.selectedServiceLabel, staff: session.preferredStaffLabel,
              timeHint: ai?.date_hint || ai?.part_of_day || null, pickedTime: picked,
              hasFicha, needsIdentity: !hasFicha
            })
            insertIntake.run({
              phone, intent:"book_request", summary: summaryText,
              data_json: safeJSONStringify({ ai, session }),
              created_at: new Date().toISOString()
            })
            await sendWithLog(sock, jid, summaryText, {phone, intent:"book_request_summary", action:"summary"})
            if (!hasFicha){
              await sendWithLog(sock, jid, "Para crearte ficha, dime tu *nombre completo* y (opcional) tu *email* 😊", {phone, intent:"ask_identity", action:"guide"})
            } else {
              await sendWithLog(sock, jid, "Si quieres que la deje cerrada ya mismo, dímelo y la intentamos crear al instante (según disponibilidad).", {phone, intent:"offer_autobook", action:"guide"})
            }
            // Si está permitido, intenta crear ya
            if (AUTO_BOOK){ await executeCreateBooking(session, phone, sock, jid) }
            return
          }

          // Si aún faltan datos, preguntamos de forma natural
          if (!session.sede){
            session.stage="awaiting_sede"; saveSession(phone, session)
            await sendWithLog(sock, jid, "¿En qué *salón* te viene mejor? *Torremolinos* o *La Luz*.", {phone, intent:"ask_sede", action:"guide", stage:session.stage})
          } else if (!session.category){
            session.stage="awaiting_category"; saveSession(phone, session)
            await sendWithLog(sock, jid, "¿Qué *categoría* necesitas? *Uñas*, *Depilación*, *Micropigmentación*, *Faciales* o *Pestañas*.", {phone, intent:"ask_category", action:"guide", stage:session.stage})
          } else if (!session.selectedServiceEnvKey){
            // ya listamos arriba; recordatorio suave
            await sendWithLog(sock, jid, "Escríbeme el *nombre del servicio* de la lista de arriba 👆 (o similar).", {phone, intent:"ask_service_text_again", action:"guide"})
          } else {
            // Último recurso: saludo corto
            await sendWithLog(sock, jid, "Te leo — dime qué horario te viene (ej. “viernes tarde” o “10:30”).", {phone, intent:"fallback", action:"guide"})
          }

          // Resumen “poco invasivo” al final
          const hasFicha = (await searchCustomersByPhone(phone)).length>0
          const summaryText = composeSummary({
            sede: session.sede, category: session.category,
            service: session.selectedServiceLabel, staff: session.preferredStaffLabel,
            timeHint: ai?.date_hint || ai?.part_of_day || ai?.notes || null, pickedTime: null,
            hasFicha, needsIdentity: !hasFicha
          })
          await sendWithLog(sock, jid, summaryText, {phone, intent:"soft_summary", action:"summary"})
          insertIntake.run({
            phone, intent:"soft_summary", summary: summaryText,
            data_json: safeJSONStringify({ ai, session }),
            created_at: new Date().toISOString()
          })
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
console.log(`🩷 Gapink Nails Bot v32.0.0 — IA sin números, resumen final, pausa “ver” 6h`)
const appListen = app.listen(PORT, ()=>{ startBot().catch(console.error) })
process.on("uncaughtException", (e)=>{ console.error("💥 uncaughtException:", e?.stack||e?.message||e) })
process.on("unhandledRejection", (e)=>{ console.error("💥 unhandledRejection:", e) })
process.on("SIGTERM", ()=>{ try{ appListen.close(()=>process.exit(0)) }catch{ process.exit(0) } })
process.on("SIGINT", ()=>{ try{ appListen.close(()=>process.exit(0)) }catch{ process.exit(0) } })
