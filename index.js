// index.js — Gapink Nails Bot · v29.0.0 (QR fix + panel QR autorefresco + reconexión sólida)

// ============== IMPORTS ==============
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

// Node 20: fetch global. Asegura crypto global
if (!globalThis.crypto) globalThis.crypto = webcrypto

dayjs.extend(utc); dayjs.extend(tz); dayjs.locale("es")
const EURO_TZ = "Europe/Madrid"

// ============== CONFIG HORARIO ==============
const WORK_DAYS = [1,2,3,4,5] // lun-vie
const SLOT_MIN = 30
const OPEN = { start: 9, end: 20 } // 09:00–20:00
const NOW_MIN_OFFSET_MIN = Number(process.env.BOT_NOW_OFFSET_MIN || 30)
const HOLIDAYS_EXTRA = (process.env.HOLIDAYS_EXTRA || "06/01,28/02,15/08,12/10,01/11,06/12,08/12,25/12")
  .split(",").map(s=>s.trim()).filter(Boolean)

// ============== FLAGS ==============
const BOT_DEBUG = /^true$/i.test(process.env.BOT_DEBUG || "")
const SQUARE_MAX_RETRIES = Number(process.env.SQUARE_MAX_RETRIES || 3)
const DRY_RUN = /^true$/i.test(process.env.DRY_RUN || "")

// ============== SQUARE ==============
const square = new Client({
  accessToken: process.env.SQUARE_ACCESS_TOKEN,
  environment: (process.env.SQUARE_ENV==="production") ? Environment.Production : Environment.Sandbox
})
const LOC_TORRE = (process.env.SQUARE_LOCATION_ID_TORREMOLINOS || "").trim()
const LOC_LUZ   = (process.env.SQUARE_LOCATION_ID_LA_LUZ || "").trim()
const ADDRESS_TORRE = process.env.ADDRESS_TORREMOLINOS || "Av. de Benyamina 18, Torremolinos"
const ADDRESS_LUZ   = process.env.ADDRESS_LA_LUZ || "Málaga – Barrio de La Luz"

// ============== IA (DeepSeek) ==============
const AI_API_KEY = process.env.DEEPSEEK_API_KEY || ""
const AI_MODEL = process.env.AI_MODEL || "deepseek-chat"
const AI_MAX_RETRIES = Number(process.env.AI_MAX_RETRIES || 2)
const AI_TIMEOUT_MS = Number(process.env.AI_TIMEOUT_MS || 8000)
const sleep = (ms)=>new Promise(r=>setTimeout(r,ms))

async function aiWithRetries(messages, system = ""){
  if (!AI_API_KEY) return null
  for (let i=0;i<=AI_MAX_RETRIES;i++){
    try{
      const controller = new AbortController()
      const timeoutId = setTimeout(()=>controller.abort(), AI_TIMEOUT_MS)
      const body = {
        model: AI_MODEL,
        messages: system ? [{ role:"system", content: system }, ...messages] : messages,
        max_tokens: 400, temperature: 0.6, stream: false
      }
      const res = await fetch("https://api.deepseek.com/chat/completions", {
        method: "POST",
        headers: { "Content-Type":"application/json", "Authorization": `Bearer ${AI_API_KEY}` },
        body: JSON.stringify(body),
        signal: controller.signal
      })
      clearTimeout(timeoutId)
      if (res.ok){
        const data = await res.json()
        const txt = data?.choices?.[0]?.message?.content || null
        if (txt && txt.trim()) return txt.trim()
      }
    }catch(_e){}
    if (i<AI_MAX_RETRIES) await sleep(300*(i+1))
  }
  return null
}

// Reescritura amable (fallback local si falla)
async function aiRewrite(text){
  const sys = "Reescribe el texto para WhatsApp en español, cercano, claro y breve. Mantén la intención."
  const out = await aiWithRetries([{ role:"user", content: text }], sys)
  return out || text
}

// ============== UTILS ==============
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

// ============== HORARIO ==============
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
function parseToEU(input){
  if (dayjs.isDayjs(input)) return input.clone().tz(EURO_TZ)
  const s = String(input||"")
  if (/[Zz]|[+\-]\d{2}:?\d{2}$/.test(s)) return dayjs(s).tz(EURO_TZ)
  return dayjs.tz(s, EURO_TZ)
}

// ============== DB ==============
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

const insertAppt = db.prepare(`
INSERT INTO appointments
(id,customer_name,customer_phone,customer_square_id,location_key,service_env_key,service_label,duration_min,start_iso,end_iso,staff_id,status,created_at,square_booking_id,square_error,retry_count)
VALUES (@id,@customer_name,@customer_phone,@customer_square_id,@location_key,@service_env_key,@service_label,@duration_min,@start_iso,@end_iso,@staff_id,@status,@created_at,@square_booking_id,@square_error,@retry_count)
`)
const insertAIConversation = db.prepare(`
INSERT OR REPLACE INTO ai_conversations
(phone, message_id, user_message, ai_response, timestamp, session_data, ai_error, fallback_used)
VALUES (@phone, @message_id, @user_message, @ai_response, @timestamp, @session_data, @ai_error, @fallback_used)
`)
const insertSquareLog = db.prepare(`
INSERT INTO square_logs
(phone, action, request_data, response_data, error_data, timestamp, success)
VALUES (@phone, @action, @request_data, @response_data, @error_data, @timestamp, @success)
`)

// ============== EMPLEADAS ==============
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
function allowedStaffNamesForSede(locKey){
  const locId = locationToId(locKey)
  return EMPLOYEES
    .filter(e => e.bookable && (e.allow.includes("ALL") || e.allow.includes(locId)))
    .map(e => staffLabelFromId(e.id))
    .filter(Boolean)
}

// Aliases “desi:TM123”
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

// ============== SERVICIOS ==============
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

// === Clasificación por categorías (ampliado)
const POS_NAIL_ANCHORS = ["uña","unas","uñas","manicura","gel","acrilic","acrilico","acrílico","semi","semipermanente","esculpida","esculpidas","press on","press-on","tips","francesa","frances","baby boomer","encapsulado","encapsulados","nivelacion","nivelación","esmaltado","esmalte"]
const NEG_NOT_NAILS   = ["pesta","pestañ","ceja","cejas","ojos","pelo a pelo","eyelash"]
const PEDI_RE = /\b(pedicur\w*|pies?)\b/i
const ALLOW_LIP_IN_BROWS = !/^false$/i.test(process.env.ALLOW_LIP_IN_BROWS || "true")

function detectCategory(text){
  const t = norm(text||"")
  if (/\b(ceja|cejas|brow|henna|laminad|perfilad|microblad|microshad|hairstroke|polvo|powder|ombr|hilo|threading)\b/.test(t)) return "cejas"
  if (/\b(pesta|pestañ|eyelash|lifting|lash|volumen|2d|3d|mega|megavolumen|tinte|rizado)\b/.test(t)) return "pestañas"
  if (/(^|\W)(depil|depilar|depilarme|fotodepil|foto depil|foto-depil|laser|láser|ipl|cera|encerar)(\W|$)/.test(t)) return "depilación"
  if (/\b(fotodepil|foto depil|foto-depil|laser|láser|ipl)\b/.test(t)) return "fotodepilación"
  if (/\b(micropigment|microblading|shading|powder brow|ombre)\b/.test(t)) return "micropigmentación"
  if (/\b(pedicur|pies)\b/.test(t)) return "pedicura"
  if (/\b(tratamiento facial|higiene facial|radiofrecuencia|peeling|facial|limpieza facial)\b/.test(t)) return "tratamiento facial"
  if (/\b(tratamiento corporal|maderoterapia|drenaje|corporal|anticelulit|reafirmante)\b/.test(t)) return "tratamiento corporal"
  if (POS_NAIL_ANCHORS.some(a=>t.includes(norm(a))) || /\buñas?\b/.test(t)) return "uñas"
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

// Listas por categoría (con filtros)
const LASH_EXCLUDE = ["foto","depil","láser","laser","pierna","axila","pubis","brazo","ingle","ingles","facial completo"]
function nailsServicesForSede(sedeKey, userMsg){
  const allowPedi = shouldIncludePedicure(userMsg)
  const list = servicesForSedeKeyRaw(sedeKey)
  const filtered = list.filter(s=>isNailsLabel(s.norm, allowPedi))
  return uniqueByLabel(filtered)
}
function lashesServicesForSede(sedeKey){
  const list = servicesForSedeKeyRaw(sedeKey)
  const anchors = ["pesta","pestañ","eyelash","lash","lifting","rizado","volumen","2d","3d","megavolumen","tinte","mega"]
  return uniqueByLabel(list.filter(s => anchors.some(a=>s.norm.includes(norm(a))) && !LASH_EXCLUDE.some(x=>s.norm.includes(norm(x)))))
}
const BROW_POS = ["ceja","cejas","brow","henna","laminad","perfilad","microblad","microshad","hairstroke","polvo","powder","ombr","retoque","hilo","threading","diseñ"]
const BROW_EXCLUDE_ZONES = ["pierna","piernas","axila","axilas","pubis","ingle","ingles","bikini","brazos","espalda","facial completo","piernas completas","medias piernas"]
function isBrowLabel(lbl){
  const hasPos = BROW_POS.some(a => lbl.includes(norm(a))) || /\bceja|cejas|brow\b/.test(lbl)
  if (!hasPos) return false
  if (BROW_EXCLUDE_ZONES.some(z => lbl.includes(norm(z)))) return false
  if (/\bdepilaci/i.test(lbl) && !/\bceja|cejas|brow\b/.test(lbl)) return false
  if (!ALLOW_LIP_IN_BROWS && /\blabio\b/.test(lbl) && !/\bceja|cejas|brow\b/.test(lbl)) return false
  return true
}
function browsServicesForSede(sedeKey){
  const list = servicesForSedeKeyRaw(sedeKey)
  return uniqueByLabel(list.filter(s => isBrowLabel(s.norm)))
}
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
// Extra categorías (ligeras)
function simpleFilterBy(words, sedeKey){
  const list = servicesForSedeKeyRaw(sedeKey)
  return uniqueByLabel(list.filter(s => words.some(w => s.norm.includes(norm(w)))))
}
function micropigmentacionServicesForSede(sedeKey){
  const words = ["micro","powder","ombre","hairstroke","labios","aquarela","eyeliner","micropigment"]
  return simpleFilterBy(words, sedeKey)
}
function fotodepilacionServicesForSede(sedeKey){
  const words = ["fotodepil","laser","láser","ipl"]
  return uniqueByLabel(servicesForSedeKeyRaw(sedeKey).filter(s => words.some(w => s.norm.includes(norm(w))) && !/uñ|pestañ/.test(s.norm)))
}
function pedicuraServicesForSede(sedeKey){
  const words = ["pedicur","pies","callos","durezas"]
  return simpleFilterBy(words, sedeKey)
}
function facialServicesForSede(sedeKey){
  const words = ["facial","higiene","limpieza","peeling","radiofrecuencia"]
  return simpleFilterBy(words, sedeKey)
}
function corporalServicesForSede(sedeKey){
  const words = ["corporal","maderoterapia","drenaje","anticelulit","reafirmante","espalda"]
  return simpleFilterBy(words, sedeKey)
}

function servicesByCategory(sedeKey, category, userMsg){
  const c = (category||"").toLowerCase()
  switch (c){
    case "uñas": return nailsServicesForSede(sedeKey, userMsg)
    case "pestañas": return lashesServicesForSede(sedeKey)
    case "cejas": return browsServicesForSede(sedeKey)
    case "depilación": return depilacionServicesForSede(sedeKey)
    case "fotodepilación": return fotodepilacionServicesForSede(sedeKey)
    case "micropigmentación": return micropigmentacionServicesForSede(sedeKey)
    case "pedicura": return pedicuraServicesForSede(sedeKey)
    case "tratamiento facial": return facialServicesForSede(sedeKey)
    case "tratamiento corporal": return corporalServicesForSede(sedeKey)
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

// ============== SQUARE HELPERS ==============
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
    await sock.sendMessage(jid, { text: await aiRewrite("Para terminar, no encuentro tu ficha. Dime tu *nombre completo* y (opcional) tu *email* 😊") })
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
  await sock.sendMessage(jid, { text: await aiRewrite(`Para terminar, he encontrado varias fichas con tu número. ¿Cuál eres?\n\n${lines}\n\nResponde con el número.`) })
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
  if (BOT_DEBUG) console.error("findOrCreateCustomerWithRetry failed", lastError?.message)
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
async function createBookingWithRetry({ startEU, locationKey, envServiceKey, durationMin, customerId, teamMemberId, phone }){
  if (!envServiceKey) return { success: false, error: "No se especificó servicio" }
  if (!teamMemberId || typeof teamMemberId!=="string" || !teamMemberId.trim()){ 
    return { success: false, error: "teamMemberId requerido" }
  }
  if (DRY_RUN) return { success: true, booking: { id:`TEST_SIM_${Date.now()}`, __sim:true } }
  const sv = await getServiceIdAndVersion(envServiceKey)
  if (!sv?.id || !sv?.version) return { success: false, error: `No se pudo obtener servicio ${envServiceKey}` }
  const startISO = startEU.tz("UTC").toISOString()
  const idempotencyKey = stableKey({ loc:locationToId(locationKey), sv:sv.id, startISO, customerId, teamMemberId })
  let lastError = null
  for (let attempt = 1; attempt <= SQUARE_MAX_RETRIES; attempt++) {
    try{
      const requestData = {
        idempotencyKey,
        booking:{
          locationId: locationToId(locationKey),
          startAt: startISO,
          customerId,
          appointmentSegments:[{
            teamMemberId,
            serviceVariationId: sv.id,
            serviceVariationVersion: Number(sv.version),
            durationMinutes: durationMin||60
          }]
        }
      }
      const resp = await square.bookingsApi.createBooking(requestData)
      const booking = resp?.result?.booking || null
      try{
        insertSquareLog.run({
          phone: phone || 'unknown',
          action: 'create_booking',
          request_data: safeJSONStringify(requestData),
          response_data: safeJSONStringify(resp?.result || {}),
          error_data: null,
          timestamp: new Date().toISOString(),
          success: 1
        })
      }catch{}
      if (booking) return { success: true, booking }
    } catch(e) {
      lastError = e
      try{
        insertSquareLog.run({
          phone: phone || 'unknown',
          action: 'create_booking',
          request_data: safeJSONStringify({ attempt, envServiceKey, locationKey, startISO }),
          response_data: null,
          error_data: safeJSONStringify({ message: e?.message, body: e?.body }),
          timestamp: new Date().toISOString(),
          success: 0
        })
      }catch{}
      if (attempt < SQUARE_MAX_RETRIES) await sleep(2000 * attempt)
    }
  }
  return { success: false, error: `No se pudo crear reserva: ${lastError?.message || 'Error desconocido'}`, lastError }
}
async function cancelBooking(bookingId){
  if (DRY_RUN) return true
  try{
    const body = { idempotencyKey:`cancel_${bookingId}_${Date.now()}` }
    const resp = await square.bookingsApi.cancelBooking(bookingId, body)
    return !!resp?.result?.booking
  }catch(e){ return false }
}

// ============== DISPONIBILIDAD ==============
async function searchAvailabilityForStaff({ locationKey, envServiceKey, staffId, fromEU, days=14, n=6, distinctDays=false }){
  try{
    const sv = await getServiceIdAndVersion(envServiceKey)
    if (!sv?.id || !staffId) return []
    const startAt = fromEU.tz("UTC").toISOString()
    const endAt = fromEU.clone().add(days,"day").tz("UTC").toISOString()
    const locationId = locationToId(locationKey)
    const body = {
      query:{ filter:{
        startAtRange:{ startAt, endAt },
        locationId,
        segmentFilters:[{ serviceVariationId: sv.id, teamMemberIdFilter:{ any:[ staffId ] } }]
      } }
    }
    const resp = await square.bookingsApi.searchAvailability(body)
    const avail = resp?.result?.availabilities || []
    const slots=[], seenDays=new Set()
    for (const a of avail){
      if (!a?.startAt) continue
      const d = dayjs(a.startAt).tz(EURO_TZ)
      if (!insideBusinessHours(d,60)) continue
      if (distinctDays){
        const key=d.format("YYYY-MM-DD"); if (seenDays.has(key)) continue; seenDays.add(key)
      }
      if (!isStaffAllowedInLocation(staffId, locationKey)) continue
      slots.push({ date:d, staffId })
      if (slots.length>=n) break
    }
    return slots
  }catch{ return [] }
}
async function searchAvailabilityGeneric({ locationKey, envServiceKey, fromEU, days=14, n=6, distinctDays=false }){
  try{
    const sv = await getServiceIdAndVersion(envServiceKey)
    if (!sv?.id) return []
    const startAt = fromEU.tz("UTC").toISOString()
    const endAt = fromEU.clone().add(days,"day").tz("UTC").toISOString()
    const locationId = locationToId(locationKey)
    const body = { query:{ filter:{ startAtRange:{ startAt, endAt }, locationId, segmentFilters:[{ serviceVariationId: sv.id }] } } }
    const resp = await square.bookingsApi.searchAvailability(body)
    const avail = resp?.result?.availabilities || []
    const slots=[], seenDays=new Set()
    for (const a of avail){
      if (!a?.startAt) continue
      const d = dayjs(a.startAt).tz(EURO_TZ)
      if (!insideBusinessHours(d,60)) continue
      let tm = null
      const segs = Array.isArray(a.appointmentSegments) ? a.appointmentSegments
                 : Array.isArray(a.segments) ? a.segments
                 : []
      if (segs[0]?.teamMemberId) tm = segs[0].teamMemberId
      if (distinctDays){
        const key=d.format("YYYY-MM-DD"); if (seenDays.has(key)) continue; seenDays.add(key)
      }
      if (tm && !isStaffAllowedInLocation(tm, locationKey)) continue
      slots.push({ date:d, staffId: tm || null })
      if (slots.length>=n) break
    }
    return slots
  }catch{ return [] }
}

// ============== MENÚS Y ELECCIÓN ==============
function buildServiceChoiceListBySede(sedeKey, userMsg, aiCandidates, category){
  const list = servicesByCategory(sedeKey, category, userMsg)
  if (!list.length) return []
  const localScores = new Map()
  for (const s of list){ localScores.set(s.label, scoreServiceRelevance(userMsg, s.label)) }
  const aiMap = new Map()
  if (Array.isArray(aiCandidates)){
    for (const c of aiCandidates){
      const label = String(cleanDisplayLabel(c.label||"")).trim(); if (!label) continue
      const conf = Number(c.confidence ?? 0)
      const prev = localScores.get(label) ?? 0
      localScores.set(label, prev + Math.max(0, conf*3))
      aiMap.set(label, conf)
    }
  }
  const inAI = list.filter(s=>aiMap.has(s.label)).sort((a,b)=> (aiMap.get(b.label)-aiMap.get(a.label)) || ((localScores.get(b.label)||0)-(localScores.get(a.label)||0)))
  const rest = list.filter(s=>!aiMap.has(s.label)).sort((a,b)=> (localScores.get(b.label)||0)-(localScores.get(a.label)||0))
  const final = uniqueByLabel([...inAI, ...rest]).slice(0, 30)
  return final.map((s,i)=>({ index:i+1, label:s.label }))
}

async function executeChooseService(params, sessionData, phone, sock, jid, userMsg){
  const incomingCat = params?.category || sessionData.category || sessionData.pendingCategory || detectCategory(userMsg)
  const VALID = ["uñas","pestañas","cejas","depilación","fotodepilación","micropigmentación","pedicura","tratamiento facial","tratamiento corporal"]
  if (!incomingCat || !VALID.includes(incomingCat)){
    sessionData.stage = "awaiting_category"
    saveSession(phone, sessionData)
    await sendWithPresence(sock, jid, await aiRewrite("¿Qué te quieres hacer: *uñas*, *pestañas*, *cejas* o *depilación*?"))
    return
  }
  if (!sessionData.sede){
    sessionData.pendingCategory = incomingCat
    sessionData.stage = "awaiting_sede_for_services"
    saveSession(phone, sessionData)
    await sendWithPresence(sock, jid, await aiRewrite(`Para ${incomingCat}, ¿prefieres *Torremolinos* o *La Luz*?`))
    return
  }

  const aiCands = Array.isArray(params?.candidates) ? params.candidates : []
  let items = buildServiceChoiceListBySede(sessionData.sede, userMsg||"", aiCands, incomingCat)
  if (!items.length){
    // Fallback inteligente
    const all = servicesForSedeKeyRaw(sessionData.sede)
    const loose = all.map((s,i)=>({ index:i+1, label:s.label }))
    items = uniqueByLabel(loose).slice(0, 20)
  }
  if (!items.length){
    await sendWithPresence(sock, jid, await aiRewrite("Ahora mismo no puedo listar ese menú. *Cristina* te contesta en cuanto pueda 😊. Si quieres, dime el *nombre exacto* del servicio."))
    return
  }

  sessionData.category = incomingCat
  sessionData.serviceChoices = items
  sessionData.stage = "awaiting_service_choice"
  saveSession(phone, sessionData)

  const lines = items.map(it=>{
    const star = aiCands.find(c=>cleanDisplayLabel(String(c.label||"")).toLowerCase()===it.label.toLowerCase()) ? " ⭐" : ""
    return `${it.index}) ${applySpanishDiacritics(it.label)}${star}`
  }).join("\n")
  await sendWithPresence(sock, jid, await aiRewrite(`Estas son nuestras opciones de **${incomingCat}** en ${locationNice(sessionData.sede)}:\n\n${lines}\n\nResponde con el número.`))
}

// ============== PROPUESTA DÍA → HORAS ==============
function proposeSlots({ fromEU, durationMin=60, n=6 }){
  const out=[]
  let t = ceilToSlotEU(fromEU.clone())
  while (out.length<n){
    if (insideBusinessHours(t, durationMin)) out.push(t.clone())
    t = t.add(SLOT_MIN, "minute")
    if (t.hour()>=OPEN.end) { t = nextOpeningFrom(t) }
  }
  return out
}
function listNext3BusinessDays(fromEU){
  const out=[]; let t = fromEU.clone().startOf('day')
  while (out.length<3){
    t = nextOpeningFrom(t)
    if (!isHolidayEU(t)) out.push(t.clone())
    t = t.add(1,'day')
  }
  return out
}
async function startPickDayFlow(sessionData, phone, sock, jid){
  const nowEU = dayjs().tz(EURO_TZ)
  const baseFrom = nextOpeningFrom(nowEU.add(NOW_MIN_OFFSET_MIN,"minute"))
  const days = listNext3BusinessDays(baseFrom)
  sessionData.lastDays = days
  sessionData.stage = "awaiting_day_pick"
  saveSession(phone, sessionData)
  const opts = days.map((d,i)=>`${i+1}) ${fmtES(d).split(' ').slice(0,2).join(' ')}`).join("\n")
  await sendWithPresence(sock, jid, await aiRewrite(`¿Qué *día* te viene mejor?\n${opts}\n\nResponde con el número.`))
}
async function proposeHoursForDay(sessionData, phone, sock, jid, options = { forcePreferred:false }){
  const day = sessionData.chosenDayISO ? dayjs(sessionData.chosenDayISO).tz(EURO_TZ) : nextOpeningFrom(dayjs().tz(EURO_TZ))
  const from = day.hour(OPEN.start).minute(0).second(0).millisecond(0)
  let slots=[]
  let usedPreferred = false

  if (sessionData.preferredStaffId && isStaffAllowedInLocation(sessionData.preferredStaffId, sessionData.sede)){
    const staffSlots = await searchAvailabilityForStaff({
      locationKey: sessionData.sede,
      envServiceKey: sessionData.selectedServiceEnvKey,
      staffId: sessionData.preferredStaffId,
      fromEU: from, days: 1, n: 6, distinctDays:false
    })
    if (staffSlots.length){
      slots = staffSlots
      usedPreferred = true
    }
  }
  if (!slots.length && !options.forcePreferred){
    const generic = await searchAvailabilityGeneric({
      locationKey: sessionData.sede,
      envServiceKey: sessionData.selectedServiceEnvKey,
      fromEU: from, days: 1, n: 6, distinctDays:false
    })
    slots = generic
  }
  if (!slots.length){
    const fallback = proposeSlots({ fromEU: from, durationMin:60, n:3 }).map(d=>({date:d, staffId:null}))
    slots = fallback
  }

  const hoursEnum = enumerateHours(slots.map(s=>s.date))
  const map = {}; for (const s of slots) map[s.date.format("YYYY-MM-DDTHH:mm")] = s.staffId || null
  sessionData.lastHours = slots.map(s=>s.date)
  sessionData.lastStaffByIso = map
  sessionData.lastProposeUsedPreferred = usedPreferred
  sessionData.stage = "awaiting_time"
  saveSession(phone, sessionData)

  const lines = hoursEnum.map(h=>{
    const sid = map[h.iso]; const tag = sid ? ` — ${staffLabelFromId(sid)}` : ""
    return `${h.index}) ${h.pretty}${tag}`
  }).join("\n")
  const header = usedPreferred
    ? `Horarios con ${sessionData.preferredStaffLabel}:`
    : `Horarios disponibles (ese día):`
  await sendWithPresence(sock, jid, await aiRewrite(`${header}\n${lines}\n\nResponde con el número.`))
}

// ============== CREAR RESERVA ==============
async function executeCreateBooking(_params, sessionData, phone, sock, jid) {
  if (!sessionData.sede) { await sendWithPresence(sock, jid, await aiRewrite("Falta seleccionar la sede (Torremolinos o La Luz)")); return; }
  if (!sessionData.selectedServiceEnvKey) { await sendWithPresence(sock, jid, await aiRewrite("Falta seleccionar el servicio")); return; }
  if (!sessionData.pendingDateTime) { await sendWithPresence(sock, jid, await aiRewrite("Falta seleccionar la fecha y hora")); return; }

  const startEU = parseToEU(sessionData.pendingDateTime)
  if (!insideBusinessHours(startEU, 60)) { await sendWithPresence(sock, jid, await aiRewrite("Esa hora está fuera del horario (L-V 09:00–20:00)")); return; }

  const iso = startEU.format("YYYY-MM-DDTHH:mm")
  let staffId = null

  // 💡 Preferencia explícita manda
  if (sessionData.preferredStaffId && isStaffAllowedInLocation(sessionData.preferredStaffId, sessionData.sede)) {
    // ¿Está ese mismo minuto con esa profesional?
    const probeStaff = await searchAvailabilityForStaff({
      locationKey: sessionData.sede,
      envServiceKey: sessionData.selectedServiceEnvKey,
      staffId: sessionData.preferredStaffId,
      fromEU: startEU.clone().subtract(1,"minute"),
      days: 1, n: 20
    })
    const matchStaff = probeStaff.find(x => x.date.isSame(startEU,"minute"))
    if (matchStaff){
      staffId = sessionData.preferredStaffId
    } else {
      // No está esa hora con la pro pedida → re-proponer solo con ella
      await sendWithPresence(sock, jid, await aiRewrite(`Justo a esa hora no veo hueco con ${sessionData.preferredStaffLabel}. Te paso opciones de ese día con ${sessionData.preferredStaffLabel} 👇`))
      await proposeHoursForDay(sessionData, phone, sock, jid, { forcePreferred:true })
      return
    }
  }

  // Si no hay preferida o no coincide exacto, usar mapa del slot o cualquier permitido
  if (!staffId){
    staffId = sessionData.lastStaffByIso?.[iso] || null
    if (staffId && !isStaffAllowedInLocation(staffId, sessionData.sede)) staffId = null
  }
  if (!staffId) {
    const probe = await searchAvailabilityGeneric({ locationKey: sessionData.sede, envServiceKey: sessionData.selectedServiceEnvKey, fromEU: startEU.clone().subtract(1, "minute"), days: 1, n: 10 })
    const match = probe.find(x => x.date.isSame(startEU, "minute"))
    if (match?.staffId && isStaffAllowedInLocation(match.staffId, sessionData.sede)) staffId = match.staffId
  }
  if (!staffId) staffId = pickStaffForLocation(sessionData.sede, null)
  if (!staffId) { await sendWithPresence(sock, jid, await aiRewrite("No hay profesionales disponibles en esa sede")); return; }

  // Identidad
  let customerId = sessionData.identityResolvedCustomerId || null
  if (!customerId){
    const { status, customer } = await getUniqueCustomerByPhoneOrPrompt(phone, sessionData, sock, jid) || {}
    if (status === "need_new" || status === "need_pick") { return }
    customerId = customer?.id || null
  }
  if (!customerId && (sessionData.name || sessionData.email)){
    const created = await findOrCreateCustomerWithRetry({ name: sessionData.name, email: sessionData.email, phone })
    if (created) customerId = created.id
  }
  if (!customerId){
    sessionData.stage = "awaiting_identity"
    saveSession(phone, sessionData)
    await sendWithPresence(sock, jid, await aiRewrite("Para terminar, dime tu *nombre* y (opcional) tu *email* para crear tu ficha 😊"))
    return
  }

  const result = await createBookingWithRetry({ startEU, locationKey: sessionData.sede, envServiceKey: sessionData.selectedServiceEnvKey, durationMin: 60, customerId, teamMemberId: staffId, phone })
  if (!result.success) {
    const aptId = `apt_failed_${Math.random().toString(36).slice(2,8)}${Date.now().toString(36).slice(-4)}`
    insertAppt.run({
      id: aptId, customer_name: sessionData?.name || null, customer_phone: phone,
      customer_square_id: customerId, location_key: sessionData.sede, service_env_key: sessionData.selectedServiceEnvKey,
      service_label: sessionData.selectedServiceLabel || serviceLabelFromEnvKey(sessionData.selectedServiceEnvKey) || "Servicio", duration_min: 60,
      start_iso: startEU.tz("UTC").toISOString(), end_iso: startEU.clone().add(60, "minute").tz("UTC").toISOString(),
      staff_id: staffId, status: "failed", created_at: new Date().toISOString(),
      square_booking_id: null, square_error: result.error, retry_count: SQUARE_MAX_RETRIES
    })
    await sendWithPresence(sock, jid, await aiRewrite("No pude crear la reserva ahora. Nuestro equipo te contactará. ¿Quieres que te proponga otro horario?"))
    return
  }

  if (result.booking.__sim) { await sendWithPresence(sock, jid, await aiRewrite("🧪 SIMULACIÓN: Reserva creada exitosamente (modo prueba)")); clearSession(phone); return }

  const aptId = `apt_${Math.random().toString(36).slice(2,8)}${Date.now().toString(36).slice(-4)}`
  insertAppt.run({
    id: aptId, customer_name: sessionData?.name || null, customer_phone: phone,
    customer_square_id: customerId, location_key: sessionData.sede, service_env_key: sessionData.selectedServiceEnvKey,
    service_label: sessionData.selectedServiceLabel || serviceLabelFromEnvKey(sessionData.selectedServiceEnvKey) || "Servicio",
    duration_min: 60, start_iso: startEU.tz("UTC").toISOString(), end_iso: startEU.clone().add(60, "minute").tz("UTC").toISOString(),
    staff_id: staffId, status: "confirmed", created_at: new Date().toISOString(),
    square_booking_id: result.booking.id, square_error: null, retry_count: 0
  })

  const staffName = staffLabelFromId(staffId) || sessionData.preferredStaffLabel || "nuestro equipo";
  const address = sessionData.sede === "la_luz" ? ADDRESS_LUZ : ADDRESS_TORRE;
  const svcLabel = serviceLabelFromEnvKey(sessionData.selectedServiceEnvKey) || sessionData.selectedServiceLabel || "Servicio"
  const confirmMessage = `🎉 ¡Reserva confirmada!

📍 ${locationNice(sessionData.sede)}
${address}

💅 ${svcLabel}
👩‍💼 ${staffName}
📅 ${fmtES(startEU)}

Referencia: ${result.booking.id}

¡Te esperamos!`
  await sendWithPresence(sock, jid, confirmMessage);
  clearSession(phone);
}

// ============== LISTAR / CANCELAR ==============
async function enumerateCitasByPhone(phone){
  const items=[]
  let cid=null
  try{
    const e164=normalizePhoneES(phone)
    const s=await square.customersApi.searchCustomers({ query:{ filter:{ phoneNumber:{ exact:e164 } } } })
    cid=(s?.result?.customers||[])[0]?.id||null
  }catch{}
  if (cid){
    try{
      const resp=await square.bookingsApi.listBookings(undefined, undefined, cid)
      const list=resp?.result?.bookings||[]
      const nowISO=new Date().toISOString()
      const seen = new Set()
      for (const b of list){
        if (!b?.startAt || b.startAt<nowISO) continue
        if (seen.has(b.id)) continue
        seen.add(b.id)
        const start=dayjs(b.startAt).tz(EURO_TZ)
        const seg=(b.appointmentSegments||[{}])[0]
        items.push({
          index:items.length+1,
          id:b.id,
          fecha_iso:start.format("YYYY-MM-DD"),
          pretty:fmtES(start),
          sede: locationNice(idToLocKey(b.locationId)||""),
          profesional: staffLabelFromId(seg?.teamMemberId) || "Profesional",
        })
      }
      items.sort((a,b)=> (a.fecha_iso.localeCompare(b.fecha_iso)) || (a.pretty.localeCompare(b.pretty)))
    }catch(e){}
  }
  return items
}
async function executeListAppointments(_params, _sessionData, phone, sock, jid) {
  const appointments = await enumerateCitasByPhone(phone);
  if (!appointments.length) { await sendWithPresence(sock, jid, await aiRewrite("No tienes citas programadas. ¿Quieres agendar una?")); return; }
  const message = `Tus próximas citas (asociadas a tu número):\n\n${appointments.map(apt => 
    `${apt.index}) ${apt.pretty}\n📍 ${apt.sede}\n👩‍💼 ${apt.profesional}\n`
  ).join("\n")}`;
  await sendWithPresence(sock, jid, message);
}
async function executeCancelAppointment(params, sessionData, phone, sock, jid) {
  const appointments = await enumerateCitasByPhone(phone);
  if (!appointments.length) { await sendWithPresence(sock, jid, await aiRewrite("No encuentro citas futuras asociadas a tu número. ¿Quieres que te ayude a reservar?")); return; }
  const appointmentIndex = params?.appointmentIndex;
  if (!appointmentIndex) {
    sessionData.cancelList = appointments
    sessionData.stage = "awaiting_cancel"
    saveSession(phone, sessionData)
    const message = `Estas son tus próximas citas (por tu número). ¿Cuál quieres cancelar?\n\n${appointments.map(apt => 
      `${apt.index}) ${apt.pretty} - ${apt.sede}`
    ).join("\n")}\n\nResponde con el número`
    await sendWithPresence(sock, jid, await aiRewrite(message));
    return;
  }
  const appointment = appointments.find(apt => apt.index === appointmentIndex);
  if (!appointment) { await sendWithPresence(sock, jid, await aiRewrite("No encontré esa cita. ¿Puedes verificar el número?")); return; }
  const success = await cancelBooking(appointment.id);
  if (success) { await sendWithPresence(sock, jid, await aiRewrite(`✅ Cita cancelada: ${appointment.pretty} en ${appointment.sede}`)) }
  else { await sendWithPresence(sock, jid, await aiRewrite("No pude cancelar la cita. Por favor contacta directamente al salón.")) }
  delete sessionData.cancelList
  sessionData.stage = null
  saveSession(phone, sessionData)
}

// ============== SESIONES ==============
function loadSession(phone){
  const row = db.prepare(`SELECT data_json FROM sessions WHERE phone=@phone`).get({phone})
  if (!row?.data_json) return null
  const s = JSON.parse(row.data_json)
  if (Array.isArray(s.lastHours_ms)) s.lastHours = s.lastHours_ms.map(ms=>dayjs.tz(ms,EURO_TZ))
  if (s.pendingDateTime_ms) s.pendingDateTime = dayjs.tz(s.pendingDateTime_ms,EURO_TZ)
  if (Array.isArray(s.lastDays_ms)) s.lastDays = s.lastDays_ms.map(ms=>dayjs.tz(ms,EURO_TZ))
  if (s.chosenDayISO_ms) s.chosenDayISO = dayjs.tz(s.chosenDayISO_ms,EURO_TZ).toISOString()
  return s
}
function saveSession(phone,s){
  const c={...s}
  c.lastHours_ms = Array.isArray(s.lastHours)? s.lastHours.map(d=>dayjs.isDayjs(d)?d.valueOf():null).filter(Boolean):[]
  c.lastDays_ms = Array.isArray(s.lastDays)? s.lastDays.map(d=>dayjs.isDayjs(d)?d.valueOf():null).filter(Boolean):[]
  c.pendingDateTime_ms = s.pendingDateTime? (dayjs.isDayjs(s.pendingDateTime)? s.pendingDateTime.valueOf() : dayjs(s.pendingDateTime).valueOf()) : null
  c.chosenDayISO_ms = s.chosenDayISO ? dayjs(s.chosenDayISO).valueOf() : null
  delete c.lastHours; delete c.pendingDateTime; delete c.lastDays
  const j=JSON.stringify(c)
  const up=db.prepare(`UPDATE sessions SET data_json=@j, updated_at=@u WHERE phone=@p`).run({j,u:new Date().toISOString(),p:phone})
  if (up.changes===0) db.prepare(`INSERT INTO sessions (phone,data_json,updated_at) VALUES (@p,@j,@u)`).run({p:phone,j,u:new Date().toISOString()})
}
function clearSession(phone){ db.prepare(`DELETE FROM sessions WHERE phone=@phone`).run({phone}) }

// ============== COLA / ENVÍO ==============
let RECONNECT_SCHEDULED = false
let RECONNECT_ATTEMPTS = 0
const QUEUE=new Map()
function enqueue(key,job){
  const prev=QUEUE.get(key)||Promise.resolve()
  const next=prev.then(job,job).finally(()=>{ if (QUEUE.get(key)===next) QUEUE.delete(key) })
  QUEUE.set(key,next); return next
}
async function sendWithPresence(sock, jid, text){
  try{ await sock.sendPresenceUpdate("composing", jid) }catch{}
  await new Promise(r=>setTimeout(r, 600+Math.random()*600))
  return sock.sendMessage(jid, { text })
}

// ============== CHAT HELPERS ==============
function isCancelIntent(text){
  const lower = norm(text)
  return /\b(cancelar|anular|borrar)\b/.test(lower) && /\b(cita|reserva|pr[oó]xima|mi)\b/.test(lower)
}
function parseSede(text){
  const t=norm(text||"")
  if (/\b(la luz|luz|malaga|málaga)\b/.test(t)) return "la_luz"
  if (/\b(torre|torremolinos)\b/.test(t)) return "torremolinos"
  return null
}
function parseNameEmailFromText(txt){
  const emailMatch = String(txt||"").match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i)
  const email = emailMatch ? emailMatch[0] : null
  const name = String(txt||"").replace(email||"", "").replace(/(email|correo)[:\s]*/ig,"").trim()
  return { name: name || null, email }
}
function parsePreferredStaffFromText(text){
  const t = norm(text||"")
  const m = t.match(/\bcon\s+([a-zñáéíóú]+)\b/i)
  if (!m) return null
  const token = norm(m[1])
  return findStaffByAliasToken(token)
}

// ============== MINI-WEB + QR ==============
const app=express()
const PORT=process.env.PORT||8080
let lastQR=null, conectado=false

app.get("/", (_req,res)=>{
  const totalAppts = db.prepare(`SELECT COUNT(*) as count FROM appointments`).get()?.count || 0
  const successAppts = db.prepare(`SELECT COUNT(*) as count FROM appointments WHERE status = 'confirmed'`).get()?.count || 0
  const failedAppts = db.prepare(`SELECT COUNT(*) as count FROM appointments WHERE status = 'failed'`).get()?.count || 0

  res.send(`<!doctype html><meta charset="utf-8">
  <meta http-equiv="refresh" content="6">
  <style>
  body{font-family:system-ui;display:grid;place-items:center;min-height:100vh;background:#f6f7fb;margin:0}
  .card{max-width:720px;width:90vw;padding:28px;border-radius:20px;box-shadow:0 8px 32px rgba(2,6,23,.12);background:white}
  .row{display:flex;gap:16px;align-items:center;flex-wrap:wrap}
  .status{padding:10px 14px;border-radius:10px;margin:10px 0;font-weight:600}
  .success{background:#dcfce7;color:#065f46}
  .error{background:#fee2e2;color:#991b1b}
  .warning{background:#fff7ed;color:#9a3412}
  .stat{display:inline-block;margin:6px 8px;padding:8px 12px;background:#eef2ff;color:#1e3a8a;border-radius:10px}
  .qr{display:grid;place-items:center;margin:16px 0}
  .note{color:#475569;font-size:14px}
  code{background:#0f172a;color:#e2e8f0;padding:2px 6px;border-radius:6px}
  </style>
  <div class="card">
    <h1>🩷 Gapink Nails Bot <small style="font-size:14px;color:#64748b">v29.0.0</small></h1>
    <div class="row">
      <div class="status ${conectado ? 'success' : 'error'}">WhatsApp: ${conectado ? "✅ Conectado" : "❌ Desconectado"}</div>
      <div class="status warning">Modo: ${DRY_RUN ? "🧪 Simulación" : "🚀 Producción"}</div>
    </div>
    ${!conectado&&lastQR?`<div class="qr"><img src="/qr.png" width="300" height="300" style="border-radius:12px;box-shadow:0 6px 24px rgba(2,6,23,.15)"></div>
    <div class="note">Escanea con WhatsApp > <b>Dispositivos vinculados</b>. Esta página refresca sola.</div>`:
    (!conectado?`<div class="note">Esperando QR…</div>`:`<div class="note">Tu sesión está activa. ✅</div>`)}
    <h3>📊 Estadísticas</h3>
    <div><span class="stat">📅 Total: ${totalAppts}</span><span class="stat">✅ Exitosas: ${successAppts}</span><span class="stat">❌ Fallidas: ${failedAppts}</span></div>
    <p class="note">Tip: si necesitas re-vincular, borra <code>auth_info/</code> y refresca esta página.</p>
  </div>`)
})
app.get("/qr.png", async (_req,res)=>{
  if(!lastQR) return res.status(404).send("No QR")
  try{
    const png = await qrcode.toBuffer(lastQR, { type:"png", width:512, margin:1 })
    res.set("Content-Type","image/png").send(png)
  }catch(e){
    res.status(500).send("QR error")
  }
})
app.get("/qr.txt", (_req,res)=>{
  if(!lastQR) return res.status(404).send("No QR")
  res.type("text/plain").send(lastQR)
})
app.get("/logs", (_req,res)=>{
  const recent = db.prepare(`SELECT * FROM square_logs ORDER BY timestamp DESC LIMIT 50`).all()
  res.json({ logs: recent })
})
app.get("/health", (_req,res)=>res.json({ ok:true, connected:conectado, hasQR: !!lastQR }))

// ============== BAILEYS (WhatsApp) ==============
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

// ============== ARRANQUE DEL BOT ==============
async function startBot(){
  try{
    const { makeWASocket, useMultiFileAuthState, fetchLatestBaileysVersion, Browsers } = await loadBaileys()

    // ⚠️ FIX: crear carpeta auth correctamente (typo arreglado)
    if (!fs.existsSync("auth_info")) fs.mkdirSync("auth_info",{recursive:true})

    const { state, saveCreds } = await useMultiFileAuthState("auth_info")
    const { version } = await fetchLatestBaileysVersion().catch(()=>({version:[2,3000,0]}))
    const sock = makeWASocket({
      logger:pino({level:"silent"}),
      printQRInTerminal:false, // lo pintamos nosotros
      auth:state,
      version,
      browser:Browsers.macOS("Desktop"),
      syncFullHistory:false
    })
    globalThis.sock=sock

    sock.ev.on("connection.update", ({connection,qr,lastDisconnect})=>{
      if (qr){
        lastQR=qr; conectado=false
        try{ qrcodeTerminal.generate(qr,{small:true}) }catch{}
      }
      if (connection==="open"){
        lastQR=null; conectado=true; RECONNECT_ATTEMPTS=0; RECONNECT_SCHEDULED=false
        if (BOT_DEBUG) console.log("WhatsApp conectado ✅")
      }
      if (connection==="close"){
        conectado=false
        if (BOT_DEBUG) console.log("WhatsApp desconectado ❌", lastDisconnect?.error?.message||"")
        if (!RECONNECT_SCHEDULED){
          RECONNECT_SCHEDULED = true
          const delay = Math.min(30000, 1500 * Math.pow(2, RECONNECT_ATTEMPTS++))
          setTimeout(()=>{ RECONNECT_SCHEDULED=false; startBot().catch(console.error) }, delay)
        }
      }
    })
    sock.ev.on("creds.update", saveCreds)

    // ========== MENSAJES ==========
    sock.ev.on("messages.upsert", async ({messages})=>{
      const m=messages?.[0]; 
      if (!m?.message) return
      const jid = m.key.remoteJid
      // Ignora grupos y estados
      if (/@g\.us$/.test(jid) || /status@broadcast$/.test(jid)) return

      const isFromMe = !!m.key.fromMe
      const phone = normalizePhoneES((jid||"").split("@")[0]||"") || (jid||"").split("@")[0]
      const textRaw = (m.message.conversation || m.message.extendedTextMessage?.text || m.message?.imageMessage?.caption || "").trim()
      if (!textRaw) return

      await enqueue(phone, async ()=>{
        try {
          let sessionData = loadSession(phone) || {
            greeted: false, sede: null, selectedServiceEnvKey: null, selectedServiceLabel: null,
            preferredStaffId: null, preferredStaffLabel: null, pendingDateTime: null,
            name: null, email: null, last_msg_id: null, lastStaffByIso: {},
            lastProposeUsedPreferred: false, stage: null, cancelList: null,
            serviceChoices: null, identityChoices: null, pendingCategory: null,
            lastStaffNamesById: null, snooze_until_ms: null,
            identityResolvedCustomerId: null, category: null,
            lastDays: null, chosenDayISO: null
          }
          if (sessionData.last_msg_id === m.key.id) return
          sessionData.last_msg_id = m.key.id

          // Silenciar con "."
          const trimmed = textRaw.trim()
          const nowEU = dayjs().tz(EURO_TZ)
          if (trimmed === ".") {
            sessionData.snooze_until_ms = nowEU.add(6, "hour").valueOf()
            saveSession(phone, sessionData)
            return
          }
          if (sessionData.snooze_until_ms && nowEU.valueOf() < sessionData.snooze_until_ms) {
            saveSession(phone, sessionData)
            return
          }
          if (isFromMe) { saveSession(phone, sessionData); return }

          const lower = norm(textRaw)
          const numMatch = lower.match(/^(?:opcion|opción)?\s*([1-9]\d*)\b/)

          // Preferencia “con {nombre}”
          const maybeStaff = parsePreferredStaffFromText(textRaw)
          if (maybeStaff) {
            sessionData.preferredStaffId = maybeStaff.id
            sessionData.preferredStaffLabel = staffLabelFromId(maybeStaff.id)
            saveSession(phone, sessionData)
            // Si estamos eligiendo hora, re-proponemos horas solo con esa profesional
            if (sessionData.stage === "awaiting_time"){
              if (!isStaffAllowedInLocation(maybeStaff.id, sessionData.sede)){
                const names = allowedStaffNamesForSede(sessionData.sede)
                await sendWithPresence(sock, jid, await aiRewrite(`Esa profesional no atiende en ${locationNice(sessionData.sede)}. En esa sede están: ${names.join(", ")}. Dime con quién prefieres.`))
                return
              }
              await sendWithPresence(sock, jid, await aiRewrite(`Perfecto, te muestro horas con ${sessionData.preferredStaffLabel} ese día:`))
              await proposeHoursForDay(sessionData, phone, sock, jid, { forcePreferred:true })
              return
            }
          }

          // ====== GUARDIAS DE ETAPA ======

          // Sede para listar servicios
          if (sessionData.stage === "awaiting_sede_for_services") {
            const sede = parseSede(textRaw)
            if (!sede){
              await sendWithPresence(sock, jid, await aiRewrite("¿Prefieres *Torremolinos* o *La Luz*?"))
              saveSession(phone, sessionData); return
            }
            sessionData.sede = sede
            sessionData.stage = null
            saveSession(phone, sessionData)
            await executeChooseService({ category: sessionData.pendingCategory || sessionData.category, candidates: [] }, sessionData, phone, sock, jid, textRaw)
            return
          }

          // Categoría primero
          if (sessionData.stage === "awaiting_category"){
            const cat0 = detectCategory(textRaw)
            if (!cat0){
              await sendWithPresence(sock, jid, await aiRewrite("¿Qué te quieres hacer: *uñas*, *pestañas*, *cejas* o *depilación*?"))
              saveSession(phone, sessionData)
              return
            }
            sessionData.category = cat0
            sessionData.stage = null
            saveSession(phone, sessionData)
            await executeChooseService({ category: cat0, candidates: [] }, sessionData, phone, sock, jid, textRaw)
            return
          }

          // Identidad: varias fichas
          if (sessionData.stage==="awaiting_identity_pick"){
            if (!numMatch){ await sendWithPresence(sock, jid, await aiRewrite("Responde con el número de tu ficha (1, 2, …).")); return }
            const n = Number(numMatch[1])
            const choice = (sessionData.identityChoices||[]).find(c=>c.index===n)
            if (!choice){ await sendWithPresence(sock, jid, await aiRewrite("No encontré esa opción. Prueba con un número de la lista.")); return }
            sessionData.identityResolvedCustomerId = choice.id
            sessionData.stage = null
            saveSession(phone, sessionData)
            await sendWithPresence(sock, jid, await aiRewrite("¡Gracias! Finalizo tu reserva…"))
            await executeCreateBooking({}, sessionData, phone, sock, jid)
            return
          }

          // Identidad: crear nueva
          if (sessionData.stage==="awaiting_identity"){
            const { name, email } = parseNameEmailFromText(textRaw)
            if (!name && !email){ 
              await sendWithPresence(sock, jid, await aiRewrite("Dime tu *nombre completo* y, si quieres, tu *email* 😊"))
              return
            }
            if (name) sessionData.name = name
            if (email) sessionData.email = email
            const created = await findOrCreateCustomerWithRetry({ name: sessionData.name, email: sessionData.email, phone })
            if (!created){
              await sendWithPresence(sock, jid, await aiRewrite("No pude crear tu ficha. ¿Puedes repetir tu *nombre* y (opcional) tu *email*?"))
              return
            }
            sessionData.identityResolvedCustomerId = created.id
            sessionData.stage = null
            saveSession(phone, sessionData)
            await sendWithPresence(sock, jid, await aiRewrite("¡Gracias! Finalizo tu reserva…"))
            await executeCreateBooking({}, sessionData, phone, sock, jid)
            return
          }

          // Selección de servicio por número
          if (sessionData.stage==="awaiting_service_choice" && Array.isArray(sessionData.serviceChoices) && sessionData.serviceChoices.length){
            if (!numMatch){
              await sendWithPresence(sock, jid, await aiRewrite("Responde con el *número* del servicio, por ejemplo: 1, 2 o 3."))
              return
            }
            const n = Number(numMatch[1])
            const pick = sessionData.serviceChoices.find(it=>it.index===n)
            if (!pick){
              await sendWithPresence(sock, jid, await aiRewrite("No encontré esa opción. Prueba con uno de los números de la lista."))
              return
            }
            const ek = resolveEnvKeyFromLabelAndSede(pick.label, sessionData.sede)
            if (!ek){
              await sendWithPresence(sock, jid, await aiRewrite("No puedo vincular ese servicio ahora mismo. ¿Puedes decirme el *nombre exacto* del servicio?"))
              return
            }
            sessionData.selectedServiceLabel = pick.label
            sessionData.selectedServiceEnvKey = ek
            sessionData.stage = null
            saveSession(phone, sessionData)
            await startPickDayFlow(sessionData, phone, sock, jid)
            return
          }

          // Elección de día
          if (sessionData.stage === "awaiting_day_pick" && Array.isArray(sessionData.lastDays) && sessionData.lastDays.length) {
            if (!numMatch) { await sendWithPresence(sock, jid, await aiRewrite("Elige un *día* (1, 2 o 3).")); return }
            const idx = Number(numMatch[1]) - 1
            const day = sessionData.lastDays[idx]
            if (!day) { await sendWithPresence(sock, jid, await aiRewrite("Número inválido. Prueba de nuevo con 1, 2 o 3.")); return }
            sessionData.chosenDayISO = day.toISOString()
            sessionData.stage = null
            saveSession(phone, sessionData)
            await proposeHoursForDay(sessionData, phone, sock, jid)
            return
          }

          // Selección de horario
          if (sessionData.stage === "awaiting_time") {
            if (!numMatch) { await sendWithPresence(sock, jid, await aiRewrite("Elige una *hora* (1, 2 o 3).")); return }
            const idx = Number(numMatch[1]) - 1
            const pick = Array.isArray(sessionData.lastHours) ? sessionData.lastHours[idx] : null
            if (!dayjs.isDayjs(pick)) { await sendWithPresence(sock, jid, await aiRewrite("Esa opción ya no está disponible. Te paso nuevas horas.")); await proposeHoursForDay(sessionData, phone, sock, jid); return }
            const isoH = pick.format("YYYY-MM-DDTHH:mm")
            const staffFromIso = sessionData?.lastStaffByIso?.[isoH] || null
            if (staffFromIso && !isStaffAllowedInLocation(staffFromIso, sessionData.sede)) {
              await sendWithPresence(sock, jid, await aiRewrite("Esa hora ya no está con esa profesional en esa sede. Te enseño alternativas 👇"))
              await proposeHoursForDay(sessionData, phone, sock, jid); return
            }
            sessionData.pendingDateTime = pick.tz(EURO_TZ).toISOString()
            if (staffFromIso) {
              sessionData.preferredStaffId = staffFromIso
              sessionData.preferredStaffLabel = staffLabelFromId(staffFromIso)
            }
            sessionData.stage = null
            saveSession(phone, sessionData)
            await executeCreateBooking({}, sessionData, phone, sock, jid)
            return
          }

          // ====== INTENCIONES RÁPIDAS ======
          if (isCancelIntent(textRaw) && sessionData.stage!=="awaiting_cancel"){
            await executeCancelAppointment({}, sessionData, phone, sock, jid)
            return
          }

          // ====== FLUJO PRINCIPAL ======
          const catDetected = detectCategory(textRaw)
          if (!sessionData.category && !catDetected && !sessionData.selectedServiceEnvKey){
            sessionData.stage = "awaiting_category"
            saveSession(phone, sessionData)
            await sendWithPresence(sock, jid, await aiRewrite("¿Qué te quieres hacer: *uñas*, *pestañas*, *cejas* o *depilación*?"))
            return
          }

          if (catDetected && !sessionData.category){
            sessionData.category = catDetected
            saveSession(phone, sessionData)
          }

          if (!sessionData.sede){
            const sede = parseSede(textRaw)
            if (!sede){
              sessionData.stage = "awaiting_sede_for_services"
              saveSession(phone, sessionData)
              await sendWithPresence(sock, jid, await aiRewrite(`Para ${sessionData.category || "el servicio"}, ¿prefieres *Torremolinos* o *La Luz*?`))
              return
            }
            sessionData.sede = sede
            saveSession(phone, sessionData)
          }

          if (!sessionData.selectedServiceEnvKey){
            await executeChooseService({ category: sessionData.category || catDetected, candidates: [] }, sessionData, phone, sock, jid, textRaw)
            return
          }

          // Preferencia de profesional no válida para la sede → lista TODAS las válidas
          if (sessionData.preferredStaffId && !isStaffAllowedInLocation(sessionData.preferredStaffId, sessionData.sede)){
            const names = allowedStaffNamesForSede(sessionData.sede)
            await sendWithPresence(sock, jid, await aiRewrite(`Esa profesional no atiende en ${locationNice(sessionData.sede)}. En esa sede están: ${names.join(", ")}. Dime con quién prefieres.`))
            return
          }

          // Ya hay servicio → pedir día
          await startPickDayFlow(sessionData, phone, sock, jid)

        } catch (error) {
          if (BOT_DEBUG) console.error(error)
          await sendWithPresence(sock, jid, await aiRewrite("Disculpa, hubo un error técnico. ¿Puedes repetir tu mensaje?"))
        }
      })
    })
  }catch(e){ 
    if (BOT_DEBUG) console.error("startBot error:", e?.message||e)
    setTimeout(() => startBot().catch(console.error), 5000) 
  }
}

// ============== ARRANQUE SERVIDOR ==============
let serverStarted = false
function safeListen(){
  if (serverStarted) return
  try{
    app.listen(PORT, ()=>{ serverStarted = true; startBot().catch(console.error) })
  }catch(e){
    if (String(e?.message||"").includes("EADDRINUSE")){
      console.error("⚠️ Puerto ocupado, continúo sin levantar Express duplicado")
      startBot().catch(console.error)
    } else {
      console.error("💥 Error al escuchar:", e?.message||e)
      startBot().catch(console.error)
    }
  }
}

console.log(`🩷 Gapink Nails Bot v29.0.0`)
safeListen()

process.on("uncaughtException", (e)=>{ console.error("💥 uncaughtException:", e?.stack||e?.message||e) })
process.on("unhandledRejection", (e)=>{ console.error("💥 unhandledRejection:", e) })
process.on("SIGTERM", ()=>{ process.exit(0) })
process.on("SIGINT", ()=>{ process.exit(0) })
