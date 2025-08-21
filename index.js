// index.js — Gapink Nails · v27.5.1 (stable)
// Cambios vs v27.5.0:
// • Fix de comillas/template strings, SQL y HTML.
// • Evita funciones/vars duplicadas y EADDRINUSE.
// • Cambio de categoría incluso dentro de selección de servicio.
// • Lógica “con {nombre}” + alternativas por sede más claras.
// • Depilación como categoría separada; cejas sin corporales.
// • Manejo más robusto del JSON devuelto por la IA.

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
import "dayjs/locale/es.js"
import { webcrypto, createHash } from "crypto"
import { createRequire } from "module"
import { Client, Environment } from "square"

if (!globalThis.crypto) globalThis.crypto = webcrypto
dayjs.extend(utc); dayjs.extend(tz); dayjs.locale("es")
const EURO_TZ = "Europe/Madrid"

// ====== Config horario
const WORK_DAYS = [1,2,3,4,5]
const SLOT_MIN = 30
const OPEN = { start: 9, end: 20 }
const NOW_MIN_OFFSET_MIN = Number(process.env.BOT_NOW_OFFSET_MIN || 30)
const HOLIDAYS_EXTRA = (process.env.HOLIDAYS_EXTRA || "06/01,28/02,15/08,12/10,01/11,06/12,08/12,25/12")
  .split(",").map(s=>s.trim()).filter(Boolean)

// ====== Flags
const BOT_DEBUG = /^true$/i.test(process.env.BOT_DEBUG || "")
const SQUARE_MAX_RETRIES = Number(process.env.SQUARE_MAX_RETRIES || 3)
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

// ====== IA
const AI_API_KEY = process.env.DEEPSEEK_API_KEY || ""
const AI_MODEL = process.env.AI_MODEL || "deepseek-chat"
const AI_MAX_RETRIES = Number(process.env.AI_MAX_RETRIES || 3)
const AI_TIMEOUT_MS = Number(process.env.AI_TIMEOUT_MS || 15000)
const sleep = ms => new Promise(r=>setTimeout(r, ms))

// ====== Utils básicos
const onlyDigits = s => String(s||"").replace(/\D+/g,"")
const rm = s => String(s||"").normalize("NFD").replace(/\p{Diacritic}/gu,"")
const norm = s => rm(s).toLowerCase().replace(/[+.,;:()/_-]/g," ").replace(/[^\p{Letter}\p{Number}\s]/gu," ").replace(/\s+/g," ").trim()
function applySpanishDiacritics(label){
  let x = String(label||"")
  x = x.replace(/\bunas\b/gi, m => m[0] === 'U' ? 'Uñas' : 'uñas')
  x = x.replace(/\bpestan(as?|)\b/gi, (m, suf) => (m[0]==='P'?'Pestañ':'pestañ') + (suf||''))
  x = x.replace(/\bnivelacion\b/gi, m => m[0]==='N' ? 'Nivelación' : 'nivelación')
  x = x.replace(/\bacrilic[oa]s?\b/gi, m => {
    const cap = m[0] === m[0].toUpperCase()
    const plural = /s$/.test(m.toLowerCase())
    const fem = /a/i.test(m.slice(-1))
    const base = fem ? 'acrílica' : 'acrílico'
    const out = base + (plural ? 's' : '')
    return cap ? out[0].toUpperCase()+out.slice(1) : out
  })
  x = x.replace(/\bfrances\b/gi, m => m[0]==='F' ? 'Francés' : 'francés')
  x = x.replace(/\bmas\b/gi, (m) => (m[0]==='M' ? 'Más' : 'más'))
  x = x.replace(/\bsemi ?permanente\b/gi, m => /[A-Z]/.test(m[0]) ? 'Semipermanente' : 'semipermanente')
  x = x.replace(/\bninas\b/gi, 'niñas')
  x = x.replace(/Esculpid(a|as)\b/gi, (m)=> {
    const cap = /[A-Z]/.test(m[0]); const suf = m.endsWith('as') ? 'as' : 'a'
    return (cap?'E':'e') + 'sculpid' + suf
  })
  return x
}
function normalizePhoneES(raw){
  const d = onlyDigits(raw); 
  if (!d) return null;
  if (raw.startsWith("+") && d.length >= 8 && d.length <= 15) return `+${d}`;
  if (d.startsWith("34") && d.length === 11) return `+${d}`;
  if (d.length === 9) return `+34${d}`;
  if (d.startsWith("00")) return `+${d.slice(2)}`;
  return `+${d}`;
}

function locationToId(key){ return key==="la_luz" ? LOC_LUZ : LOC_TORRE }
function idToLocKey(id){ return id===LOC_LUZ ? "la_luz" : id===LOC_TORRE ? "torremolinos" : null }
function locationNice(key){ return key==="la_luz" ? "Málaga – La Luz" : "Torremolinos" }

// ====== Horario helpers
function isHolidayEU(d){
  const dd = String(d.date()).padStart(2,"0");
  const mm = String(d.month()+1).padStart(2,"0");
  return HOLIDAYS_EXTRA.includes(`${dd}/${mm}`);
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
function fmtES(d){
  const dias=["domingo","lunes","martes","miércoles","jueves","viernes","sábado"]
  const t=(dayjs.isDayjs(d)?d:dayjs(d)).tz(EURO_TZ)
  return `${dias[t.day()]} ${String(t.date()).padStart(2,"0")}/${String(t.month()+1).padStart(2,"0")} ${String(t.hour()).padStart(2,"0")}:${String(t.minute()).padStart(2,"0")}`
}
function enumerateHours(list){ return list.map((d,i)=>({ index:i+1, iso:d.format("YYYY-MM-DDTHH:mm"), pretty:fmtES(d) })) }
function stableKey(parts){ const raw=Object.values(parts).join("|"); return createHash("sha256").update(raw).digest("hex").slice(0,48) }

// ====== JSON seguro
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

// ====== TZ robusto
function parseToEU(input){
  if (dayjs.isDayjs(input)) return input.clone().tz(EURO_TZ)
  const s = String(input||"")
  if (/[Zz]|[+\-]\d{2}:?\d{2}$/.test(s)) return dayjs(s).tz(EURO_TZ)
  return dayjs.tz(s, EURO_TZ)
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
CREATE TABLE IF NOT EXISTS ai_conversations (
  phone TEXT,
  message_id TEXT,
  user_message TEXT,
  ai_response TEXT,
  timestamp TEXT,
  session_data TEXT,
  ai_error TEXT,
  fallback_used BOOLEAN DEFAULT 0,
  PRIMARY KEY (phone, message_id)
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

const insertAIConversation = db.prepare(`INSERT OR REPLACE INTO ai_conversations
(phone, message_id, user_message, ai_response, timestamp, session_data, ai_error, fallback_used)
VALUES (@phone, @message_id, @user_message, @ai_response, @timestamp, @session_data, @ai_error, @fallback_used)`)

const insertSquareLog = db.prepare(`INSERT INTO square_logs
(phone, action, request_data, response_data, error_data, timestamp, success)
VALUES (@phone, @action, @request_data, @response_data, @error_data, @timestamp, @success)`)

// ====== Empleadas y servicios
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
    const [id, book, locs] = String(v||"").split("|")
    if (!id) continue
    const bookable = (book||"").toUpperCase()==="BOOKABLE"
    let allow = (locs||"").split(",").map(s=>s.trim()).filter(Boolean)

    const empKey = "EMP_CENTER_" + k.replace(/^SQ_EMP_/, "")
    const empVal = process.env[empKey]
    if (empVal) {
      const centers = String(empVal).split(",").map(s=>s.trim().toLowerCase()).filter(Boolean)
      if (centers.some(c => c === "all")) {
        allow = ["ALL"]
      } else {
        const normCenter = c => (c==="la luz" ? "la_luz" : c)
        const ids = centers.map(c => locationToId(normCenter(c))).filter(Boolean)
        if (ids.length) allow = ids
      }
    }

    const labels = deriveLabelsFromEnvKey(k)
    out.push({ envKey:k, id, bookable, allow, labels })
  }
  return out
}
const EMPLOYEES = parseEmployees()
function staffLabelFromId(id){
  const e = EMPLOYEES.find(x=>x.id===id)
  return e?.labels?.[0] || (id ? `Prof. ${String(id).slice(-4)}` : null)
}
function isStaffAllowedInLocation(staffId, locKey){
  const e = EMPLOYEES.find(x=>x.id===staffId)
  if (!e || !e.bookable) return false
  const locId = locationToId(locKey)
  return e.allow.includes("ALL") || e.allow.includes(locId)
}
function pickStaffForLocation(locKey, preferId=null){
  const locId = locationToId(locKey)
  const isAllowed = e => e.bookable && (e.allow.includes("ALL") || e.allow.includes(locId))
  if (preferId){
    const e = EMPLOYEES.find(x=>x.id===preferId)
    if (e && isAllowed(e)) return e.id
  }
  const found = EMPLOYEES.find(isAllowed)
  return found?.id || null
}
function allowedSedesText(e){
  const ids = e.allow.includes("ALL") ? [LOC_TORRE, LOC_LUZ] : e.allow
  const keys = ids.map(id => idToLocKey(id)).filter(Boolean)
  return keys.map(locationNice).join(" / ")
}

// ====== Aliases de staff (ENV)
function parseStaffAliases(){
  const raw = (process.env.STAFF_ALIASES || "").trim()
  const map = new Map()
  if (!raw) return map
  if (raw.startsWith("{")){
    try{
      const obj = JSON.parse(raw)
      for (const [alias,id] of Object.entries(obj||{})){
        if (alias && id) map.set(norm(alias), String(id).trim())
      }
      return map
    }catch{ return map }
  }
  // CSV: "desi:TM123, lara:TM456"
  raw.split(",").map(s=>s.trim()).filter(Boolean).forEach(pair=>{
    const [a,b] = pair.split(":").map(x=>x?.trim())
    if (a && b) map.set(norm(a), b)
  })
  return map
}
const STAFF_ALIAS_MAP = parseStaffAliases()
function findStaffByAliasToken(tokenNorm){
  const directId = STAFF_ALIAS_MAP.get(tokenNorm)
  if (directId){
    const e = EMPLOYEES.find(x=>x.id===directId)
    if (e) return e
  }
  for (const e of EMPLOYEES){
    for (const lbl of e.labels){
      if (norm(lbl).includes(tokenNorm)) return e
    }
  }
  return null
}

// ====== Servicios
function titleCase(str){ return String(str||"").toLowerCase().replace(/\b([a-z])/g, (m)=>m.toUpperCase()) }
function cleanDisplayLabel(label){
  const s = String(label||"").replace(/^\s*(luz|la\s*luz)\s+/i,"").trim()
  return applySpanishDiacritics(s)
}
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
function serviceLabelFromEnvKey(envKey){
  if (!envKey) return null
  const all = [...servicesForSedeKeyRaw("torremolinos"), ...servicesForSedeKeyRaw("la_luz")]
  return all.find(s=>s.key===envKey)?.label || null
}
function allServices(){ return [...servicesForSedeKeyRaw("torremolinos"), ...servicesForSedeKeyRaw("la_luz")] }

// ====== Clasificación por categorías
const POS_NAIL_ANCHORS = [
  "uña","unas","uñas","manicura","gel","acrilic","acrilico","acrílico","semi","semipermanente",
  "esculpida","esculpidas","press on","press-on","tips","francesa","frances","baby boomer","encapsulado","encapsulados","nivelacion","nivelación","esmaltado","esmalte"
]
const NEG_NOT_NAILS = ["pesta","pestañ","ceja","cejas","ojos","pelo a pelo","eyelash"]
const PEDI_RE = /\b(pedicur\w*|pies?)\b/i

const ALLOW_LIP_IN_BROWS = !/^false$/i.test(process.env.ALLOW_LIP_IN_BROWS || "true")

// Detectar categoría
function detectCategory(text){
  const t = norm(text||"")
  if (/\b(ceja|cejas|brow|henna|laminad|perfilad|microblad|microshad|hairstroke|polvo|powder|ombr|hilo)\b/.test(t)) return "cejas"
  if (/\b(pesta|pestañ|eyelash|lifting|lash|volumen|2d|3d|mega|megavolumen|tinte|rizado)\b/.test(t)) return "pestañas"
  if (POS_NAIL_ANCHORS.some(a=>t.includes(norm(a))) || /\buñas?\b/.test(t)) return "uñas"
  if (/\b(depil|cera|cer[ao]|fotodepil|foto-depil|láser|laser|ipl)\b/.test(t)) return "depilación"
  return null
}

function shouldIncludePedicure(userMsg){ return PEDI_RE.test(String(userMsg||"")) }
function isNailsLabel(labelNorm, allowPedicure){
  if (NEG_NOT_NAILS.some(n=>labelNorm.includes(norm(n)))) return false
  const hasPos = POS_NAIL_ANCHORS.some(p=>labelNorm.includes(norm(p))) || /uñ|manicura|gel|acril|semi/.test(labelNorm)
  if (!hasPos) return false
  const isPedi = PEDI_RE.test(labelNorm)
  if (isPedi && !allowPedicure) return false
  return true
}
function uniqueByLabel(arr){
  const seen=new Set(); const out=[]
  for (const s of arr){
    const key = s.label.toLowerCase()
    if (seen.has(key)) continue
    seen.add(key); out.push(s)
  }
  return out
}

// — Uñas
function nailsServicesForSede(sedeKey, userMsg){
  const allowPedi = shouldIncludePedicure(userMsg)
  const list = servicesForSedeKeyRaw(sedeKey)
  const filtered = list.filter(s=>isNailsLabel(s.norm, allowPedi))
  return uniqueByLabel(filtered)
}

// — Pestañas
const LASH_EXCLUDE = ["foto","depil","láser","laser","pierna","axila","pubis","brazo","ingle","ingles","facial completo"]
function lashesServicesForSede(sedeKey){
  const list = servicesForSedeKeyRaw(sedeKey)
  const anchors = ["pesta","pestañ","eyelash","lash","lifting","rizado","volumen","2d","3d","megavolumen","tinte","mega"]
  return uniqueByLabel(
    list.filter(s => anchors.some(a=>s.norm.includes(norm(a))) && !LASH_EXCLUDE.some(x=>s.norm.includes(norm(x))))
  )
}

// — Cejas (limpio de zonas corporales)
const BROW_POS = ["ceja","cejas","brow","henna","laminad","perfilad","microblad","microshad","hairstroke","polvo","powder","ombr","retoque","hilo","threading","diseñ"]
const BROW_EXCLUDE_ZONES = ["pierna","piernas","axila","axilas","pubis","ingle","ingles","bikini","brazos","espalda","facial completo","piernas completas","medias piernas"]
function isBrowLabel(lbl){
  const hasPos = BROW_POS.some(a => lbl.includes(norm(a))) || /\b(ceja|cejas|brow)\b/.test(lbl)
  if (!hasPos) return false
  if (BROW_EXCLUDE_ZONES.some(z => lbl.includes(norm(z)))) return false
  if (/\bdepilaci/i.test(lbl) && !/\b(ceja|cejas|brow)\b/.test(lbl)) return false
  if (!ALLOW_LIP_IN_BROWS && /\blabio\b/.test(lbl) && !/\b(ceja|cejas|brow)\b/.test(lbl)) return false
  return true
}
function browsServicesForSede(sedeKey){
  const list = servicesForSedeKeyRaw(sedeKey)
  return uniqueByLabel(list.filter(s => isBrowLabel(s.norm)))
}

// — Depilación (láser/foto/cera; facial y corporal)
const DEPIL_POS = ["depil","cera","cerado","fotodepil","láser","laser","ipl","fotodep","hilo","wax"]
const DEPIL_ALIAS_ZONES = ["pierna","piernas","axila","axilas","pubis","perianal","ingle","ingles","bikini","brazos","espalda","labio","facial","ceja","cejas","mentón","patillas","abdomen","pecho","hombros","nuca","glúteos"]
function isDepilLabel(lbl){
  const hasDepil = DEPIL_POS.some(a => lbl.includes(norm(a)))
  if (!hasDepil) return false
  if (/\buñ|manicura|gel|acril|pestañ|eyelash|lash\b/.test(lbl)) return false
  const mentionsZone = DEPIL_ALIAS_ZONES.some(z => lbl.includes(norm(z)))
  return hasDepil || mentionsZone
}
function depilacionServicesForSede(sedeKey){
  const list = servicesForSedeKeyRaw(sedeKey)
  return uniqueByLabel(list.filter(s => isDepilLabel(s.norm)))
}

// — Selector por categoría
function servicesByCategory(sedeKey, category, userMsg){
  switch ((category||"").toLowerCase()){
    case "uñas": return nailsServicesForSede(sedeKey, userMsg)
    case "pestañas": return lashesServicesForSede(sedeKey)
    case "cejas": return browsServicesForSede(sedeKey)
    case "depilación": return depilacionServicesForSede(sedeKey)
    default: return []
  }
}

function scoreServiceRelevance(userMsg, label){
  const u = norm(userMsg), l = norm(label); let score = 0
  if (/\b(uñas|unas)\b/.test(u) && /\b(uñas|unas|manicura)\b/.test(l)) score += 3
  if (/\bmanicura\b/.test(u) && /\bmanicura\b/.test(l)) score += 3
  if (/\b(acrilic|acrilico|acrílico)\b/.test(u) && l.includes("acril")) score += 2.5
  if (/\bgel\b/.test(u) && l.includes("gel")) score += 2.5
  if (/\bsemi|semipermanente\b/.test(u) && l.includes("semi")) score += 2
  if (/\brelleno\b/.test(u) && (l.includes("uña") || l.includes("manicura") || l.includes("gel") || l.includes("acril"))) score += 2
  if (/\bretir(ar|o)\b/.test(u) && (l.includes("retir")||l.includes("retiro"))) score += 1.5
  if (/\bpress\b/.test(u) && l.includes("press")) score += 1.2
  const tokens = ["natural","francesa","frances","decoracion","diseño","extra","exprés","express","completa","nivelacion","nivelación","henna","lamin","láser","laser","cera","depil"]
  for (const t of tokens){ if (u.includes(norm(t)) && l.includes(norm(t))) score += 0.4 }
  const utoks = new Set(u.split(" ").filter(Boolean))
  const ltoks = new Set(l.split(" ").filter(Boolean))
  let overlap=0; for (const t of utoks){ if (ltoks.has(t)) overlap++ }
  score += Math.min(overlap,3)*0.25
  return score
}
function resolveEnvKeyFromLabelAndSede(label, sedeKey){
  const list = servicesForSedeKeyRaw(sedeKey)
  return list.find(s=>s.label.toLowerCase()===String(label||"").toLowerCase())?.key || null
}

// ====== Square helpers (identidad)
async function searchCustomersByPhone(phone){
  try{
    const e164=normalizePhoneES(phone); if(!e164) return []
    const got = await square.customersApi.searchCustomers({ query:{ filter:{ phoneNumber:{ exact:e164 } } } })
    return got?.result?.customers || []
  }catch{ return [] }
}
async function getUniqueCustomerByPhoneOrPrompt(phone, sessionData, sock, jid){
  const matches = await searchCustomersByPhone(phone)
  if (matches.length ==