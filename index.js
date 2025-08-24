// index.js — Gapink Nails · v28.0.0
// Features:
// • Categorías (uñas, depilación, micropigmentación, pestañas, cejas, facial, masaje).
// • Staff inteligente: alias, nombres compuestos, sedes forzadas por tu lista.
// • “Con {nombre}” → SOLO huecos con esa persona, si no hay: “equipo u otro día”.
// • “viernes / mañana / 25/08” → repropone ese día (sin perder profesional/sede/servicio).
// • Siempre respeta staff permitido en cada sede (no ofrece slots imposibles).
// • Correcciones de tildes/ñ en etiquetas, normalización y Title Case español.
// • Fix DB: inserciones con mismas columnas/valores. Fixs de sintaxis previos.
// • Modo silencioso “.” (6 h).

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
import customParseFormat from "dayjs/plugin/customParseFormat.js"
import "dayjs/locale/es.js"
import { webcrypto, createHash } from "crypto"
import { createRequire } from "module"
import { Client, Environment } from "square"

if (!globalThis.crypto) globalThis.crypto = webcrypto
dayjs.extend(utc); dayjs.extend(tz); dayjs.extend(customParseFormat); dayjs.locale("es")
const EURO_TZ = "Europe/Madrid"

// ====== Config horario
const WORK_DAYS = [1,2,3,4,5] // Lun-Vie
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

// ====== IA (DeepSeek por defecto; puedes alternar a OpenAI si quieres)
const AI_API_KEY = process.env.DEEPSEEK_API_KEY || process.env.OPENAI_API_KEY || ""
const AI_MODEL = process.env.AI_MODEL || process.env.DEEPSEEK_MODEL || "deepseek-chat"
const AI_URL = process.env.DEEPSEEK_API_URL || process.env.OPENAI_API_URL || "https://api.deepseek.com/v1/chat/completions"
const AI_MAX_RETRIES = Number(process.env.AI_MAX_RETRIES || 3)
const AI_TIMEOUT_MS = Number(process.env.AI_TIMEOUT_MS || 15000)
const sleep = ms => new Promise(r=>setTimeout(r, ms))

// ====== Utils básicos
const onlyDigits = s => String(s||"").replace(/\D+/g,"")
// Quita diacríticos solo para comparaciones; no para mostrar
const rm = s => String(s||"").normalize("NFD").replace(/\p{Diacritic}/gu,"")
// Normaliza para matching laxo
const norm = s => rm(s).toLowerCase().replace(/[+.,;:()/_-]/g," ").replace(/[^\p{Letter}\p{Number}\s]/gu," ").replace(/\s+/g," ").trim()

// 👉 Restaurar tildes/ñ y arreglar faltas comunes SOLO para mostrar
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
  x = x.replace(/\bdepilacion\b/gi, m => m[0]==='D' ? 'Depilación' : 'depilación')
  x = x.replace(/\bfotodepilacion\b/gi, m => m[0]==='F' ? 'Fotodepilación' : 'fotodepilación')
  x = x.replace(/\bdiseno\b/gi, m => m[0]==='D' ? 'Diseño' : 'diseño')
  x = x.replace(/\bcejas\b/gi, m => m[0]==='C' ? 'Cejas' : 'cejas')
  x = x.replace(/\blabios\b/gi, m => m[0]==='L' ? 'Labios' : 'labios')
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
function fmtES(d){
  const dias=["domingo","lunes","martes","miércoles","jueves","viernes","sábado"]
  const t=(dayjs.isDayjs(d)?d:dayjs(d)).tz(EURO_TZ)
  return `${dias[t.day()]} ${String(t.date()).padStart(2,"0")}/${String(t.month()+1).padStart(2,"0")} ${String(t.hour()).padStart(2,"0")}:${String(t.minute()).padStart(2,"0")}`
}
function enumerateHours(list){ return list.map((d,i)=>({ index:i+1, iso:d.format("YYYY-MM-DDTHH:mm"), pretty:fmtES(d) })) }
function stableKey(parts){ const raw=Object.values(parts).join("|"); return createHash("sha256").update(raw).digest("hex").slice(0,48) }

// ====== Parse fechas: “viernes / mañana / 25/08 / 25-08-2025”
const DOW = ["domingo","lunes","martes","miércoles","jueves","viernes","sábado"]
function parseRequestedDayFromText(text, nowEU){
  const t = norm(text)
  if (/\bhoy\b/.test(t)) return nowEU.clone().hour(OPEN.start).minute(0).second(0).millisecond(0)
  if (/\bmañana\b/.test(t)) return nowEU.clone().add(1,"day").hour(OPEN.start).minute(0).second(0).millisecond(0)
  // día de la semana
  for (let i=0;i<7;i++){
    if (t.includes(DOW[i])){
      let d = nowEU.clone()
      while (d.day()!==i) d = d.add(1,"day")
      return d.hour(OPEN.start).minute(0).second(0).millisecond(0)
    }
  }
  // fechas tipo 25/08[/2025] o 25-08-2025
  const m = t.match(/\b(\d{1,2})[\/\-](\d{1,2})(?:[\/\-](\d{2,4}))?\b/)
  if (m){
    let dd=Number(m[1]), mm=Number(m[2])-1, yy=m[3]?Number(m[3]):nowEU.year()
    if (yy<100) yy += 2000
    const d = dayjs.tz({ year:yy, month:mm, date:dd, hour:OPEN.start, minute:0 }, EURO_TZ)
    if (d.isValid()) return d
  }
  return null
}

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

// ====== Empleadas y servicios
function deriveLabelsFromEnvKey(envKey){
  const raw = envKey.replace(/^SQ_EMP_/, "")
  const toks = raw.split("_").map(t=>norm(t)).filter(Boolean)
  const uniq = Array.from(new Set(toks))
  const labels = [...uniq]
  if (uniq.length>1) labels.push(uniq.join(" "))
  return labels.map(s => s.replace(/\bpatri\b/,'patricia')) // ayuda a “Patri/Patricia”
}
function parseEmployees(){
  const out=[]
  for (const [k,v] of Object.entries(process.env)) {
    if (!k.startsWith("SQ_EMP_")) continue
    const [id, book, locs] = String(v||"").split("|")
    if (!id) continue
    const bookable = (book||"").toUpperCase()==="BOOKABLE"
    let allow = (locs||"").split(",").map(s=>s.trim()).filter(Boolean)
    // Override por EMP_CENTER_*
    const empKey = "EMP_CENTER_" + k.replace(/^SQ_EMP_/, "")
    const empVal = process.env[empKey]
    if (empVal) {
      const centers = String(empVal).split(",").map(s=>s.trim().toLowerCase()).filter(Boolean)
      if (centers.some(c => c === "all")) {
        allow = ["ALL"]
      } else {
        const normCenter = c => (c==="la luz" ? "la_luz" : c)
        const ids = centers
          .map(c => normCenter(c))
          .map(centerKey => locationToId(centerKey))
          .filter(Boolean)
        if (ids.length) allow = ids
      }
    }
    const labels = deriveLabelsFromEnvKey(k)
    out.push({ envKey:k, id, bookable, allow, labels })
  }
  return out
}

let EMPLOYEES = parseEmployees()

// Overrides manuales (tu lista)
function overrideStaffCentersFromList(){
  const overrideMap = {
    // Málaga – La Luz
    "rocio": ["la_luz"],
    "rocio chica": ["la_luz","torremolinos"], // en tu lista aparece en ambas
    "carmen belen": ["la_luz"],
    "patri": ["la_luz"],
    "patricia": ["la_luz"],
    "ganna": ["la_luz"],
    "maria": ["la_luz"],
    "anaira": ["la_luz"],
    "cristi": ["la_luz","torremolinos"], // aparece en ambas
    "cristina": ["la_luz","torremolinos"],

    // Torremolinos
    "ginna": ["torremolinos"],
    "daniela": ["torremolinos"],
    "desi": ["torremolinos"],
    "jamaica": ["torremolinos"],
    "johana": ["torremolinos"],
    "edurne": ["torremolinos"],
    "sudemis": ["torremolinos"],
    "tania": ["torremolinos"],
    "chabely": ["torremolinos"],
    "elisabeth": ["torremolinos"]
  }
  for (const e of EMPLOYEES){
    const allAliases = e.labels.map(l => norm(l))
    const hit = Object.keys(overrideMap).find(name => allAliases.some(a => a.includes(name)))
    if (hit){
      const centers = overrideMap[hit]
      if (centers && centers.length){
        e.allow = centers.map(c => locationToId(c)).filter(Boolean)
      }
    }
  }
}
overrideStaffCentersFromList()

function staffLabelFromId(id){
  const e = EMPLOYEES.find(x=>x.id===id)
  // Mostrar primer alias bonito
  const lbl = e?.labels?.[0] || (id ? `Prof. ${String(id).slice(-4)}` : null)
  if (!lbl) return null
  return lbl.split(" ").map(w => w[0]?.toUpperCase()+w.slice(1)).join(" ")
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

// ====== Servicios
function titleCase(str){
  return String(str||"").toLowerCase().replace(/\b([a-záéíóúñ])/g, (m)=>m.toUpperCase())
}
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

// ====== Categorías
const CAT = {
  UNAS: "uñas",
  DEP: "depilación",
  MICRO: "micropigmentación",
  PEST: "pestañas",
  CEJAS: "cejas",
  FACIAL: "facial",
  MASAJE: "masaje",
  OTROS: "otros"
}
function detectCategoryFromText(msg){
  const u = norm(msg||"")
  if (/\bdepil|fotodepil|axilas|ingles|labio|piernas|nasales\b/.test(u)) return CAT.DEP
  if (/\bmicropig|microblading|aquarela|eyeliner|cejas.*(polvo|shading)\b/.test(u)) return CAT.MICRO
  if (/\bpestañas|pestanas|lifting\b/.test(u)) return CAT.PEST
  if (/\bcejas|laminacion|laminación\b/.test(u)) return CAT.CEJAS
  if (/\blimpieza|facial|dermapen|hydra|vitamina|acne|manchas|endosphere|endosfere|oro|jade\b/.test(u)) return CAT.FACIAL
  if (/\bmasaje|maderoterapia\b/.test(u)) return CAT.MASAJE
  if (/\buñ|unas|manicura|gel|acrilic|acrílic|press|tips|francesa|nivelaci|pedicur|pies\b/.test(u)) return CAT.UNAS
  return null
}
function categorizeService(labelNorm){
  const s = labelNorm
  if (/\b(foto)?depil|axilas|ingles|labio|piernas|nasales\b/.test(s)) return CAT.DEP
  if (/microblading|aquarela|eyeliner|polvo|microshading/.test(s)) return CAT.MICRO
  if (/pestañ|pestanas|lifting/.test(s)) return CAT.PEST
  if (/cejas\b/.test(s) && !/micro/.test(s)) return CAT.CEJAS
  if (/facial|dermapen|hydra|vitamina|acne|manchas|endosphere|oro|jade/.test(s)) return CAT.FACIAL
  if (/masaje|maderoterapia/.test(s)) return CAT.MASAJE
  if (/uña|unas|manicura|gel|acril|press|tips|francesa|nivelación|pedicura|pies/.test(s)) return CAT.UNAS
  return CAT.OTROS
}
function servicesByCategoryForSede(sedeKey, category){
  const list = servicesForSedeKeyRaw(sedeKey)
  const filtered = list.filter(s => categorizeService(s.norm)===category)
  const uniq = new Map()
  for (const s of filtered){
    const key = s.label.toLowerCase()
    if (!uniq.has(key)) uniq.set(key, s)
  }
  return Array.from(uniq.values())
}
function scoreServiceRelevance(userMsg, label){
  const u = norm(userMsg), l = norm(label); let score = 0
  if (/\b(uñas|unas|manicura)\b/.test(u) && /\b(uñas|unas|manicura)\b/.test(l)) score += 3
  if (/\bmanicura\b/.test(u) && /\bmanicura\b/.test(l)) score += 3
  if (/\b(acrilic|acrilico|acrílico)\b/.test(u) && l.includes("acril")) score += 2.5
  if (/\bgel\b/.test(u) && l.includes("gel")) score += 2.5
  if (/\bsemi|semipermanente\b/.test(u) && l.includes("semi")) score += 2
  if (/\bpedicur|pies\b/.test(u) && /\bpedicur|pies\b/.test(l)) score += 2
  if (/\bdepil|fotodepil|axilas|ingles|labio|piernas|nasales\b/.test(u) && /\bdepil|fotodepil|axilas|ingles|labio|piernas|nasales\b/.test(l)) score += 3
  if (/microblading|aquarela|eyeliner|microshading/.test(u) && /microblading|aquarela|eyeliner|microshading/.test(l)) score += 3
  const tokens = ["natural","francesa","frances","decoracion","diseño","extra","exprés","express","completa","nivelacion","nivelación"]
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

// ====== Clasificación de “uñas” (compatibilidad con prompts viejos)
function shouldIncludePedicure(userMsg){ return /\b(pedicur|pies|pie)\b/i.test(String(userMsg||"")) }

// ====== Square helpers (identidad por teléfono)
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
    await sock.sendMessage(jid, { text: "Para terminar, no encuentro tu ficha por este número. Dime tu *nombre completo* y, si quieres, tu *email* para crearte 😊" })
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
  await sock.sendMessage(jid, { text: `Para terminar, he encontrado varias fichas con tu número. ¿Cuál eres?\n\n${lines}\n\nResponde con el número.` })
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

// ====== Square booking helpers
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

// ====== DISPONIBILIDAD
async function searchAvailabilityForStaff({ locationKey, envServiceKey, staffId, fromEU, days=14, n=3, distinctDays=false }){
  try{
    const sv = await getServiceIdAndVersion(envServiceKey)
    if (!sv?.id || !staffId) return []
    // Ajuste a comienzo del día si exact day
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
      if (!isStaffAllowedInLocation(staffId, locationKey)) continue
      if (distinctDays){
        const key=d.format("YYYY-MM-DD"); if (seenDays.has(key)) continue; seenDays.add(key)
      }
      slots.push({ date:d, staffId })
      if (slots.length>=n) break
    }
    return slots
  }catch{ return [] }
}
async function searchAvailabilityGeneric({ locationKey, envServiceKey, fromEU, days=14, n=3, distinctDays=false }){
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
      if (tm && !isStaffAllowedInLocation(tm, locationKey)) continue
      if (distinctDays){
        const key=d.format("YYYY-MM-DD"); if (seenDays.has(key)) continue; seenDays.add(key)
      }
      slots.push({ date:d, staffId: tm || null })
      if (slots.length>=n) break
    }
    return slots
  }catch{ return [] }
}

// ====== IA (prompts con reglas)
async function callAIOnce(messages, systemPrompt = "") {
  const controller = new AbortController()
  const timeoutId = setTimeout(() => controller.abort(), AI_TIMEOUT_MS)
  try {
    const allMessages = systemPrompt ? [{ role: "system", content: systemPrompt }, ...messages] : messages
    const response = await fetch(AI_URL, {
      method: "POST",
      headers: { "Content-Type": "application/json", "Authorization": `Bearer ${AI_API_KEY}` },
      body: JSON.stringify({ model: AI_MODEL, messages: allMessages, max_tokens: 1500, temperature: 0.6, stream: false }),
      signal: controller.signal
    });
    clearTimeout(timeoutId)
    if (!response.ok) return null
    const data = await response.json();
    return data?.choices?.[0]?.message?.content ?? null
  } catch { clearTimeout(timeoutId); return null }
}
async function callAIWithRetries(messages, systemPrompt=""){
  for (let i=0;i<=AI_MAX_RETRIES;i++){
    const res = await callAIOnce(messages, systemPrompt)
    if (res && typeof res==="string" && res.trim()) return res
    if (i < AI_MAX_RETRIES) await sleep(Math.min(5000, 500 * Math.pow(2, i)))
  }
  return null
}

function staffRosterForPrompt(){
  return EMPLOYEES.map(e=>{
    const locs = e.allow.map(id=> id===LOC_TORRE?"torremolinos" : id===LOC_LUZ?"la_luz" : id).join(",")
    return `• ID:${e.id} | Nombres:[${e.labels.join(", ")}] | Sedes:[${locs||"ALL"}] | Reservable:${e.bookable}`
  }).join("\n")
}

function buildSystemPrompt() {
  const nowEU = dayjs().tz(EURO_TZ);
  const staffLines = staffRosterForPrompt()
  // No meto todo el catálogo raw para no inflar el prompt; la IA decide con la categoría
  return `Eres el asistente de WhatsApp para Gapink Nails. Devuelves SOLO JSON válido.

INFORMACIÓN:
- Fecha/hora actual: ${nowEU.format("dddd DD/MM/YYYY HH:mm")} (Madrid)
- Estado: PRODUCCIÓN

SEDES:
- Torremolinos: ${ADDRESS_TORRE}
- Málaga – La Luz: ${ADDRESS_LUZ}

HORARIOS:
- L-V 09:00-20:00; S/D cerrado; Festivos: ${HOLIDAYS_EXTRA.join(", ")}

PROFESIONALES (con aliases y sedes permitidas):
${staffLines}

CATEGORÍAS: uñas, depilación, micropigmentación, pestañas, cejas, facial, masaje.

REGLAS:
1) Primero entiende intención: reservar / listar / cancelar. Para reservar: pide CATEGORÍA y SEDE si faltan. Luego servicio concreto.
2) “con {nombre}” o si el nombre aparece en el texto → preferredStaffId. Si esa profesional no atiende en la sede, dilo y propone otras válidas. NO inventes IDs.
3) Proponer horas: si hay preferida → SOLO con ella. Si no hay huecos → pregunta “¿equipo u otro día?” (no cambies sola).
4) Al elegir hora, conserva el teamMemberId exacto si vino del slot; si no coincide con preferida, pide confirmación para cambiar a equipo.
5) Identidad: por teléfono (match único). Si 0 o 2+, pide datos al final (nombre/email) o que elija ficha.
6) Mensajes cortos y claros; devuelve SOLO JSON con campos:
{"message":"...","action":"propose_times|create_booking|list_appointments|cancel_appointment|choose_category|choose_service|need_info|none","session_updates":{...},"action_params":{...}}`
}

async function getAIResponse(userMessage, sessionData, phone) {
  const systemPrompt = buildSystemPrompt();
  const recent = db.prepare(`SELECT user_message, ai_response FROM ai_conversations WHERE phone = ? ORDER BY timestamp DESC LIMIT 6`).all(phone);
  const conversationHistory = recent.reverse().map(msg => [
    { role: "user", content: msg.user_message },
    { role: "assistant", content: msg.ai_response }
  ]).flat();

  const sessionContext = `
ESTADO:
- Sede: ${sessionData?.sede || 'no seleccionada'}
- Categoría: ${sessionData?.category || 'no seleccionada'}
- Servicio: ${sessionData?.selectedServiceLabel || 'no seleccionado'} (${sessionData?.selectedServiceEnvKey || 'no_key'})
- Profesional preferida: ${sessionData?.preferredStaffLabel || 'ninguna'}
- Fecha/hora pendiente: ${sessionData?.pendingDateTime ? fmtES(parseToEU(sessionData.pendingDateTime)) : 'no seleccionada'}
- Etapa: ${sessionData?.stage || 'inicial'}
- Últimas horas propuestas: ${Array.isArray(sessionData?.lastHours) ? sessionData.lastHours.length + ' opciones' : 'ninguna'}
`;

  const messages = [
    ...conversationHistory,
    { role: "user", content: `MENSAJE DEL CLIENTE: "${userMessage}"\n\n${sessionContext}\n\nINSTRUCCIÓN: Devuelve SOLO JSON siguiendo las reglas.` }
  ];

  const aiText = await callAIWithRetries(messages, systemPrompt)
  if (!aiText || /^error de conexión/i.test(aiText.trim())) return { message:"¿Quieres reservar, cancelar o ver tus citas? Dime categoría (uñas, depilación, micropigmentación, pestañas, cejas, facial o masaje) y sede.", action:"need_info", session_updates:{}, action_params:{} }

  const cleaned = aiText.replace(/```json\s*/gi, "").replace(/```\s*/g, "").replace(/^[^{]*/, "").replace(/[^}]*$/, "").trim()
  try { return JSON.parse(cleaned) } catch { return { message:"Ok, ¿uñas, depilación, micropigmentación, pestañas, cejas, facial o masaje? y la sede (Torremolinos/La Luz).", action:"need_info", session_updates:{}, action_params:{} } }
}

// ====== Staff detector pro
function findEmployeeByAliasLoose(token){
  const t = norm(token)
  for (const e of EMPLOYEES){
    for (const raw of e.labels){
      const ln = norm(raw)
      if (ln===t || ln.startsWith(t) || t.startsWith(ln) || ln.includes(t) || t.includes(ln)) return e
    }
  }
  return null
}
const NAME_SYNONYMS = [
  ["patricia","patri"],
  ["rocio chica","rocío chica","rociochica","rocíochica","chica"],
  ["carmen belen","carmen belén","belen","belén"],
  ["cristina","cristi"],
]
function expandWithSynonyms(text){
  const t = norm(text)
  const out = new Set([t])
  for (const group of NAME_SYNONYMS){
    for (const g of group){
      if (t.includes(g)) group.forEach(x=>out.add(x))
    }
  }
  return Array.from(out)
}
function parsePreferredStaffFromText(text){
  const candidates = new Set()
  const t = ` ${norm(text)} `
  const m = t.match(/\bcon\s+([a-zñáéíóú]+(?:\s+[a-zñáéíóú]+){0,2})\b/i)
  if (m) candidates.add(norm(m[1]))
  for (const e of EMPLOYEES){
    for (const raw of e.labels){
      const lbl = ` ${norm(raw)} `
      if (t.includes(lbl)) candidates.add(norm(raw))
    }
  }
  const expanded = new Set()
  for (const c of candidates) expandWithSynonyms(c).forEach(x=>expanded.add(x))
  for (const c of Array.from(expanded)){
    const e = findEmployeeByAliasLoose(c)
    if (e) return e
  }
  return null
}

// ====== Menús por categoría
function buildServiceChoiceListBySedeCategory(sedeKey, category, userMsg, aiCandidates){
  const itemsBase = servicesByCategoryForSede(sedeKey, category)
  const localScores = new Map()
  for (const s of itemsBase){ localScores.set(s.label, scoreServiceRelevance(userMsg||"", s.label)) }
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
  const inAI = itemsBase.filter(s=>aiMap.has(s.label)).sort((a,b)=> (aiMap.get(b.label)-aiMap.get(a.label)) || ((localScores.get(b.label)||0)-(localScores.get(a.label)||0)))
  const rest = itemsBase.filter(s=>!aiMap.has(s.label)).sort((a,b)=> (localScores.get(b.label)||0)-(localScores.get(a.label)||0))
  const final = [...inAI, ...rest]
  return final.map((s,i)=>({ index:i+1, label:s.label }))
}

async function executeChooseCategory(_params, sessionData, phone, sock, jid){
  sessionData.stage = "awaiting_category"
  saveSession(phone, sessionData)
  await sock.sendMessage(jid, { text: "¿Qué categoría quieres? *Uñas, Depilación, Micropigmentación, Pestañas, Cejas, Facial o Masaje*." })
}

async function executeChooseService(params, sessionData, phone, sock, jid, userMsg){
  if (!sessionData.sede){
    sessionData.stage = "awaiting_sede_for_services"
    saveSession(phone, sessionData)
    await sock.sendMessage(jid, { text: "¿En qué sede te viene mejor, Torremolinos o La Luz?" })
    return
  }
  if (!sessionData.category){
    await executeChooseCategory({}, sessionData, phone, sock, jid)
    return
  }
  const aiCands = Array.isArray(params?.candidates) ? params.candidates : []
  const items = buildServiceChoiceListBySedeCategory(sessionData.sede, sessionData.category, userMsg||"", aiCands)
  if (!items.length){
    await sock.sendMessage(jid, { text: `Ahora mismo no tengo servicios de *${sessionData.category}* configurados en ${locationNice(sessionData.sede)}.` })
    return
  }
  sessionData.serviceChoices = items
  sessionData.stage = "awaiting_service_choice"
  saveSession(phone, sessionData)
  const lines = items.map(it=> {
    const star = aiCands.find(c=>cleanDisplayLabel(String(c.label||"")).toLowerCase()===it.label.toLowerCase()) ? " ⭐" : ""
    return `${it.index}) ${applySpanishDiacritics(it.label)}${star}`
  }).join("\n")
  await sock.sendMessage(jid, { text: `Opciones de *${sessionData.category}* en ${locationNice(sessionData.sede)}:\n\n${lines}\n\nResponde con el número.` })
}

// ====== Proponer horas
function proposeSlots({ fromEU, durationMin=60, n=3 }){
  const out=[]
  let t = ceilToSlotEU(fromEU.clone())
  while (out.length<n){
    if (insideBusinessHours(t, durationMin)) out.push(t.clone())
    t = t.add(SLOT_MIN, "minute")
    if (t.hour()>=OPEN.end) { t = nextOpeningFrom(t) }
  }
  return out
}

async function executeProposeTime(params, sessionData, phone, sock, jid) {
  const nowEU = dayjs().tz(EURO_TZ);
  const base0 = params?.fromISO ? parseToEU(params.fromISO) : nowEU.clone().add(NOW_MIN_OFFSET_MIN,"minute")
  const baseFrom = params?.exactDayOnly
    ? base0.clone().hour(OPEN.start).minute(0).second(0).millisecond(0)
    : nextOpeningFrom(base0.add(NOW_MIN_OFFSET_MIN,"minute"))

  if (!sessionData.sede || !sessionData.selectedServiceEnvKey) { await sock.sendMessage(jid, { text:"Necesito la sede y el servicio primero." }); return; }

  let slots = []
  let usedPreferred = false
  const daysRange = params?.exactDayOnly ? 1 : 14

  const ignorePreferred = !!sessionData.overrideTeam
  if (sessionData.preferredStaffId && !ignorePreferred && isStaffAllowedInLocation(sessionData.preferredStaffId, sessionData.sede)) {
    const staffSlots = await searchAvailabilityForStaff({
      locationKey: sessionData.sede,
      envServiceKey: sessionData.selectedServiceEnvKey,
      staffId: sessionData.preferredStaffId,
      fromEU: baseFrom, n: 6, days: daysRange
    })
    if (staffSlots.length){
      slots = staffSlots.slice(0,3); usedPreferred = true
    } else {
      sessionData.lastRequestedBaseISO = baseFrom.toISOString()
      sessionData.lastRequestedExactDay = !!params?.exactDayOnly
      sessionData.stage = "awaiting_preferred_fallback_choice"
      saveSession(phone, sessionData)
      const who = sessionData.preferredStaffLabel || "esa profesional"
      await sock.sendMessage(jid, { text: `No veo huecos con ${who} en ese periodo. ¿Quieres ver opciones con el *equipo* o prefieres *otro día* con ${who}?` })
      return
    }
  }

  if (!slots.length) {
    const generic = await searchAvailabilityGeneric({ locationKey: sessionData.sede, envServiceKey: sessionData.selectedServiceEnvKey, fromEU: baseFrom, n: 6, days: daysRange })
    slots = generic.slice(0,3)
  }
  if (!slots.length) {
    const fallback = proposeSlots({ fromEU: baseFrom, durationMin: 60, n: 3 });
    slots = fallback.map(d => ({ date: d, staffId: null }))
  }
  slots = slots.filter(s => !s.staffId || isStaffAllowedInLocation(s.staffId, sessionData.sede))
  if (!slots.length){ await sock.sendMessage(jid, { text:"No encuentro horarios disponibles. ¿Otro día?" }); return }

  const hoursEnum = enumerateHours(slots.map(s => s.date))
  const map = {}; for (const s of slots) map[s.date.format("YYYY-MM-DDTHH:mm")] = s.staffId || null

  const nameMap = {}
  Object.values(map).forEach(sid => { if (sid) nameMap[sid] = staffLabelFromId(sid) })
  sessionData.lastStaffNamesById = nameMap

  sessionData.lastHours = slots.map(s => s.date)
  sessionData.lastStaffByIso = map
  sessionData.lastProposeUsedPreferred = usedPreferred
  sessionData.stage = "awaiting_time"
  saveSession(phone, sessionData)

  const lines = hoursEnum.map(h => {
    const sid = map[h.iso]
    const tag = sid ? ` — ${staffLabelFromId(sid)}` : ""
    return `${h.index}) ${h.pretty}${tag}`
  }).join("\n")
  const header = usedPreferred
    ? `Horarios disponibles con ${sessionData.preferredStaffLabel || "tu profesional"}:`
    : `Horarios disponibles (nuestro equipo):${sessionData.preferredStaffLabel ? `\nNota: no veo huecos con ${sessionData.preferredStaffLabel} en los próximos días; te muestro alternativas.`:""}`
  await sock.sendMessage(jid, { text: `${header}\n${lines}\n\nResponde con el número (1, 2 o 3)` })
}

// ====== Crear reserva
async function executeCreateBooking(_params, sessionData, phone, sock, jid) {
  if (!sessionData.sede) { await sock.sendMessage(jid, { text:"Falta seleccionar la sede (Torremolinos o La Luz)" }); return; }
  if (!sessionData.selectedServiceEnvKey) { await sock.sendMessage(jid, { text:"Falta seleccionar el servicio" }); return; }
  if (!sessionData.pendingDateTime) { await sock.sendMessage(jid, { text:"Falta seleccionar la fecha y hora" }); return; }

  const startEU = parseToEU(sessionData.pendingDateTime)
  if (!insideBusinessHours(startEU, 60)) { await sock.sendMessage(jid, { text:"Esa hora está fuera del horario (L-V 09:00–20:00)" }); return; }

  const iso = startEU.format("YYYY-MM-DDTHH:mm")
  const wantedId = sessionData.preferredStaffId || null
  const isoStaff = sessionData?.lastStaffByIso?.[iso] || null

  if (wantedId && !sessionData.overrideTeam && isoStaff && isoStaff !== wantedId){
    const nameWanted = sessionData.preferredStaffLabel || staffLabelFromId(wantedId) || "tu profesional"
    await sock.sendMessage(jid, { text: `Esa hora no es con ${nameWanted}. ¿Quieres confirmar con el *equipo* o prefieres que te pase horas con ${nameWanted}?` })
    sessionData.stage = "awaiting_preferred_fallback_choice"
    saveSession(phone, sessionData)
    return
  }
  if (wantedId && !sessionData.overrideTeam && (!isoStaff || isoStaff===null)){
    const probe = await searchAvailabilityForStaff({
      locationKey: sessionData.sede,
      envServiceKey: sessionData.selectedServiceEnvKey,
      staffId: wantedId,
      fromEU: startEU.clone().subtract(1, "minute"), days: 1, n: 30
    })
    const match = probe.find(x => x.date.isSame(startEU, "minute"))
    if (!match){
      const nameWanted = sessionData.preferredStaffLabel || staffLabelFromId(wantedId) || "tu profesional"
      await sock.sendMessage(jid, { text: `A esa hora no veo hueco con ${nameWanted}. ¿Quieres ver *otro día* con ${nameWanted} o confirmar con el *equipo*?` })
      sessionData.stage = "awaiting_preferred_fallback_choice"
      saveSession(phone, sessionData)
      return
    }
  }

  let staffId = null
  if (wantedId && !sessionData.overrideTeam) {
    staffId = wantedId
  } else {
    staffId = sessionData.lastProposeUsedPreferred ? (sessionData.preferredStaffId || sessionData.lastStaffByIso?.[iso] || null)
                                                   : (sessionData.lastStaffByIso?.[iso] || sessionData.preferredStaffId || null)
  }

  if (staffId && !isStaffAllowedInLocation(staffId, sessionData.sede)) {
    staffId = null
  }
  if (!staffId) {
    const probe = await searchAvailabilityGeneric({ locationKey: sessionData.sede, envServiceKey: sessionData.selectedServiceEnvKey, fromEU: startEU.clone().subtract(1, "minute"), days: 1, n: 10 })
    const match = probe.find(x => x.date.isSame(startEU, "minute"))
    if (match?.staffId && isStaffAllowedInLocation(match.staffId, sessionData.sede)) staffId = match.staffId
  }
  if (!staffId) staffId = pickStaffForLocation(sessionData.sede, null)
  if (!staffId) { await sock.sendMessage(jid, { text:"No hay profesionales disponibles en esa sede" }); return; }

  // Identidad
  let customerId = sessionData.identityResolvedCustomerId || null
  if (!customerId){
    const { status, customer } = await getUniqueCustomerByPhoneOrPrompt(phone, sessionData, sock, jid) || {}
    if (status === "need_new" || status === "need_pick") return
    customerId = customer?.id || null
  }
  if (!customerId && (sessionData.name || sessionData.email)){
    const created = await findOrCreateCustomerWithRetry({ name: sessionData.name, email: sessionData.email, phone })
    if (created) customerId = created.id
  }
  if (!customerId){
    sessionData.stage = "awaiting_identity"
    saveSession(phone, sessionData)
    await sock.sendMessage(jid, { text: "Para terminar, dime tu *nombre* y (opcional) tu *email* para crear tu ficha 😊" })
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
    await sock.sendMessage(jid, { text:"No pude crear la reserva ahora. ¿Quieres que te proponga otro horario?" })
    return
  }

  if (result.booking.__sim) { await sock.sendMessage(jid, { text:"🧪 SIMULACIÓN: Reserva creada exitosamente (modo prueba)" }); clearSession(phone); return }

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

🧾 ${svcLabel}
👩‍💼 ${applySpanishDiacritics(staffName)}
📅 ${fmtES(startEU)}

Ref: ${result.booking.id}

¡Te esperamos!`
  await sock.sendMessage(jid, { text: confirmMessage });
  clearSession(phone);
}

// ====== Listar/cancelar por teléfono
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
  if (!appointments.length) { await sock.sendMessage(jid, { text:"No tienes citas programadas. ¿Quieres agendar una?" }); return; }
  const message = `Tus próximas citas (asociadas a tu número):\n\n${appointments.map(apt => 
    `${apt.index}) ${apt.pretty}\n📍 ${apt.sede}\n👩‍💼 ${apt.profesional}\n`
  ).join("\n")}`;
  await sock.sendMessage(jid, { text: message });
}
async function executeCancelAppointment(params, sessionData, phone, sock, jid) {
  const appointments = await enumerateCitasByPhone(phone);
  if (!appointments.length) { await sock.sendMessage(jid, { text:"No encuentro citas futuras asociadas a tu número. ¿Quieres que te ayude a reservar?" }); return; }
  const appointmentIndex = params?.appointmentIndex;
  if (!appointmentIndex) {
    sessionData.cancelList = appointments
    sessionData.stage = "awaiting_cancel"
    saveSession(phone, sessionData)
    const message = `Estas son tus próximas citas (por tu número). ¿Cuál quieres cancelar?\n\n${appointments.map(apt => 
      `${apt.index}) ${apt.pretty} - ${apt.sede}`
    ).join("\n")}\n\nResponde con el número`
    await sock.sendMessage(jid, { text: message });
    return;
  }
  const appointment = appointments.find(apt => apt.index === appointmentIndex);
  if (!appointment) { await sock.sendMessage(jid, { text:"No encontré esa cita. ¿Puedes verificar el número?" }); return; }
  const success = await cancelBooking(appointment.id);
  if (success) { await sock.sendMessage(jid, { text:`✅ Cita cancelada: ${appointment.pretty} en ${appointment.sede}` }) }
  else { await sock.sendMessage(jid, { text:"No pude cancelar la cita. Por favor contacta directamente al salón." }) }
  delete sessionData.cancelList
  sessionData.stage = null
  saveSession(phone, sessionData)
}

// ====== Mini-web + QR
const app=express()
const PORT=process.env.PORT||8080
let lastQR=null, conectado=false
app.get("/", (_req,res)=>{
  const totalAppts = db.prepare(`SELECT COUNT(*) as count FROM appointments`).get()?.count || 0
  const successAppts = db.prepare(`SELECT COUNT(*) as count FROM appointments WHERE status = 'confirmed'`).get()?.count || 0
  const failedAppts = db.prepare(`SELECT COUNT(*) as count FROM appointments WHERE status = 'failed'`).get()?.count || 0
  res.send(`<!doctype html><meta charset="utf-8"><style>
  body{font-family:system-ui;display:grid;place-items:center;min-height:100vh;background:#f8f9fa}
  .card{max-width:640px;padding:32px;border-radius:20px;box-shadow:0 8px 32px rgba(0,0,0,.1);background:white}
  .status{padding:12px;border-radius:8px;margin:8px 0}
  .success{background:#d4edda;color:#155724}
  .error{background:#f8d7da;color:#721c24}
  .warning{background:#fff3cd;color:#856404}
  .stat{display:inline-block;margin:0 16px;padding:8px 12px;background:#e9ecef;border-radius:6px}
  </style><div class="card">
  <h1>🩷 Gapink Nails Bot v28.0.0</h1>
  <div class="status ${conectado ? 'success' : 'error'}">Estado WhatsApp: ${conectado ? "✅ Conectado" : "❌ Desconectado"}</div>
  ${!conectado&&lastQR?`<div style="text-align:center;margin:20px 0"><img src="/qr.png" width="300" style="border-radius:8px"></div>`:""}
  <div class="status warning">Modo: ${DRY_RUN ? "🧪 Simulación" : "🚀 Producción"}</div>
  <h3>📊 Estadísticas</h3>
  <div><span class="stat">📅 Total: ${totalAppts}</span><span class="stat">✅ Exitosas: ${successAppts}</span><span class="stat">❌ Fallidas: ${failedAppts}</span></div>
  <div style="margin-top:24px;padding:16px;background:#e3f2fd;border-radius:8px;font-size:14px">
    <strong>🚀 Mejoras v28:</strong><br>
    • Categorías y staff preferido con fallback “equipo u otro día”.<br>
    • Repropuesta por día (viernes / mañana / 25/08).<br>
    • Slots sólo de staff permitido en sede.<br>
  </div>
  </div>`)
})
app.get("/qr.png", async (_req,res)=>{
  if(!lastQR) return res.status(404).send("No QR")
  const png = await qrcode.toBuffer(lastQR, { type:"png", width:512, margin:1 })
  res.set("Content-Type","image/png").send(png)
})
app.get("/logs", (_req,res)=>{
  const recent = db.prepare(`SELECT * FROM square_logs ORDER BY timestamp DESC LIMIT 50`).all()
  res.json({ logs: recent })
})

// ====== Baileys
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

function parseSede(text){
  const t=norm(text)
  if (/\b(luz|la luz)\b/.test(t)) return "la_luz"
  if (/\b(torre|torremolinos)\b/.test(t)) return "torremolinos"
  return null
}

// 👉 parse básico para nombre/email en texto libre
function parseNameEmailFromText(txt){
  const emailMatch = String(txt||"").match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i)
  const email = emailMatch ? emailMatch[0] : null
  const name = String(txt||"").replace(email||"", "").replace(/(email|correo)[:\s]*/ig,"").trim()
  return { name: name || null, email }
}

function isCancelIntent(text){
  const lower = norm(text)
  return /\b(cancelar|anular|borrar)\b/.test(lower) && /\b(cita|reserva|pr[oó]xima|mi)\b/.test(lower)
}

// ====== Router IA
async function routeAIResult(aiObj, sessionData, textRaw, m, phone, sock, jid){
  if (aiObj?.session_updates) {
    Object.keys(aiObj.session_updates).forEach(key => {
      if (aiObj.session_updates[key] !== null && aiObj.session_updates[key] !== undefined) {
        sessionData[key] = aiObj.session_updates[key]
      }
    })
  }

  // Staff preferido desde texto (si no lo puso la IA)
  if (!sessionData.preferredStaffId){
    const maybe = parsePreferredStaffFromText(textRaw)
    if (maybe){
      sessionData.preferredStaffId = maybe.id
      sessionData.preferredStaffLabel = staffLabelFromId(maybe.id)
    }
  }

  // Resolver envKey desde label si falta
  if (sessionData.sede && sessionData.selectedServiceLabel && !sessionData.selectedServiceEnvKey){
    const ek = resolveEnvKeyFromLabelAndSede(sessionData.selectedServiceLabel, sessionData.sede)
    if (ek) sessionData.selectedServiceEnvKey = ek
  }

  const fallbackUsedBool = !!aiObj?.__fallback_used
  insertAIConversation.run({
    phone, message_id: m.key.id, user_message: textRaw,
    ai_response: safeJSONStringify(aiObj || {}), timestamp: new Date().toISOString(),
    session_data: safeJSONStringify(sessionData),
    ai_error: null,
    fallback_used: Number(fallbackUsedBool)
  })
  saveSession(phone, sessionData)

  switch (aiObj?.action) {
    case "choose_category":
      await executeChooseCategory(aiObj.action_params, sessionData, phone, sock, jid); break
    case "choose_service":
      await executeChooseService(aiObj.action_params, sessionData, phone, sock, jid, textRaw); break
    case "propose_times":
      await executeProposeTime(aiObj.action_params, sessionData, phone, sock, jid); break
    case "create_booking":
      await executeCreateBooking(aiObj.action_params, sessionData, phone, sock, jid); break
    case "list_appointments":
      await executeListAppointments(aiObj.action_params, sessionData, phone, sock, jid); break
    case "cancel_appointment":
      await executeCancelAppointment(aiObj.action_params, sessionData, phone, sock, jid); break
    case "need_info":
    case "none":
    default: {
      // Si tenemos sede+cat pero no servicio → mostrar menú
      if (sessionData.sede && sessionData.category && !sessionData.selectedServiceEnvKey){
        await executeChooseService({ candidates: aiObj?.action_params?.candidates || [] }, sessionData, phone, sock, jid, textRaw)
      } else if (!sessionData.sede || !sessionData.category) {
        await sock.sendMessage(jid, { text: "Para reservar dime *categoría* (uñas, depilación, micropigmentación, pestañas, cejas, facial o masaje) y *sede* (Torremolinos o La Luz)." })
      } else {
        await sock.sendMessage(jid, { text: aiObj?.message || "¿Puedes repetirlo, por favor?" })
      }
    }
  }
}

// ====== Bot principal
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
  await new Promise(r=>setTimeout(r, 800+Math.random()*1200))
  return sock.sendMessage(jid, { text })
}

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
      if (connection==="open"){ lastQR=null; conectado=true; RECONNECT_ATTEMPTS=0; RECONNECT_SCHEDULED=false; }
      if (connection==="close"){ 
        conectado=false; 
        if (!RECONNECT_SCHEDULED){
          RECONNECT_SCHEDULED = true
          const delay = Math.min(30000, 1500 * Math.pow(2, RECONNECT_ATTEMPTS++))
          setTimeout(()=>{ RECONNECT_SCHEDULED=false; startBot().catch(console.error) }, delay)
        }
      }
    })
    sock.ev.on("creds.update", saveCreds)

    sock.ev.on("messages.upsert", async ({messages})=>{
      const m=messages?.[0]; 
      if (!m?.message) return
      const jid = m.key.remoteJid
      const isFromMe = !!m.key.fromMe
      const phone = normalizePhoneES((jid||"").split("@")[0]||"") || (jid||"").split("@")[0]
      const textRaw = (m.message.conversation || m.message.extendedTextMessage?.text || m.message?.imageMessage?.caption || "").trim()
      if (!textRaw) return

      await enqueue(phone, async ()=>{
        try {
          let sessionData = loadSession(phone) || {
            greeted: false, sede: null, category: null,
            selectedServiceEnvKey: null, selectedServiceLabel: null,
            preferredStaffId: null, preferredStaffLabel: null, overrideTeam: false,
            pendingDateTime: null, name: null, email: null, last_msg_id: null,
            lastStaffByIso: {}, lastProposeUsedPreferred: false, stage: null, cancelList: null,
            serviceChoices: null, identityChoices: null, pendingCategory: null, lastStaffNamesById: null,
            snooze_until_ms: null, identityResolvedCustomerId: null,
            lastRequestedBaseISO: null, lastRequestedExactDay: false
          }
          if (sessionData.last_msg_id === m.key.id) return
          sessionData.last_msg_id = m.key.id

          // Silencio con "."
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

          // ===== Etapas especiales =====

          // Elegir ficha (identidad duplicada)
          if (sessionData.stage==="awaiting_identity_pick"){
            if (!numMatch){
              await sendWithPresence(sock, jid, "Responde con el número de tu ficha (1, 2, ...).")
              return
            }
            const n = Number(numMatch[1])
            const choice = (sessionData.identityChoices||[]).find(c=>c.index===n)
            if (!choice){
              await sendWithPresence(sock, jid, "No encontré esa opción. Prueba con el número de la lista.")
              return
            }
            sessionData.identityResolvedCustomerId = choice.id
            sessionData.stage = null
            saveSession(phone, sessionData)
            await sendWithPresence(sock, jid, "¡Gracias! Finalizo tu reserva…")
            await executeCreateBooking({}, sessionData, phone, sock, jid)
            return
          }

          // Crear ficha (identidad nueva)
          if (sessionData.stage==="awaiting_identity"){
            const { name, email } = parseNameEmailFromText(textRaw)
            if (!name && !email){
              await sendWithPresence(sock, jid, "Dime tu *nombre completo* y, si quieres, tu *email* 😊")
              return
            }
            if (name) sessionData.name = name
            if (email) sessionData.email = email
            const created = await findOrCreateCustomerWithRetry({ name: sessionData.name, email: sessionData.email, phone })
            if (!created){
              await sendWithPresence(sock, jid, "No pude crear tu ficha. ¿Puedes repetir tu *nombre* y (opcional) tu *email*?")
              return
            }
            sessionData.identityResolvedCustomerId = created.id
            sessionData.stage = null
            saveSession(phone, sessionData)
            await sendWithPresence(sock, jid, "¡Gracias! Finalizo tu reserva…")
            await executeCreateBooking({}, sessionData, phone, sock, jid)
            return
          }

          // Elegir sede cuando estábamos esperando para mostrar servicios
          if (sessionData.stage==="awaiting_sede_for_services"){
            const sede = parseSede(textRaw)
            if (sede){
              sessionData.sede = sede
              sessionData.stage = null
              saveSession(phone, sessionData)
              await executeChooseService({ candidates: [] }, sessionData, phone, sock, jid, textRaw)
              return
            }
          }

          // “equipo u otro día” (fallback cuando no hay con preferida)
          if (sessionData.stage === "awaiting_preferred_fallback_choice") {
            const t = norm(textRaw)
            if (/\b(equipo|cualquiera|me da igual|ok equipo|vale equipo)\b/.test(t)) {
              sessionData.overrideTeam = true
              sessionData.stage = null
              saveSession(phone, sessionData)
              await executeProposeTime({
                fromISO: sessionData.lastRequestedBaseISO || undefined,
                exactDayOnly: !!sessionData.lastRequestedExactDay
              }, sessionData, phone, sock, jid)
              return
            }
            const req = parseRequestedDayFromText(textRaw, nowEU)
            if (req){
              sessionData.overrideTeam = false
              sessionData.stage = null
              saveSession(phone, sessionData)
              await executeProposeTime({ fromISO: req.toISOString(), exactDayOnly: true }, sessionData, phone, sock, jid)
              return
            }
            await sendWithPresence(sock, jid, "Escribe *equipo* para ver con cualquier profesional, o dime un *día* (p. ej. viernes, 25/08, mañana) para seguir con tu profesional.")
            return
          }

          // Número → elegir hora (si hay lastHours)
          if (numMatch && Array.isArray(sessionData.lastHours) && sessionData.lastHours.length && (!sessionData.stage || sessionData.stage==="awaiting_time")){
            const idx = Number(numMatch[1]) - 1
            const pick = sessionData.lastHours[idx]
            if (dayjs.isDayjs(pick)){
              const iso = pick.format("YYYY-MM-DDTHH:mm")
              const staffFromIso = sessionData?.lastStaffByIso?.[iso] || null
              if (staffFromIso && !isStaffAllowedInLocation(staffFromIso, sessionData.sede)) {
                await sendWithPresence(sock, jid, "Esa hora ya no está disponible con esa profesional en esa sede. Te paso otras opciones 👇")
                await executeProposeTime({}, sessionData, phone, sock, jid)
                return
              }
              sessionData.pendingDateTime = pick.tz(EURO_TZ).toISOString()
              if (staffFromIso){ sessionData.preferredStaffId = staffFromIso; sessionData.preferredStaffLabel = staffLabelFromId(staffFromIso) }
              saveSession(phone, sessionData)
              await routeAIResult({ message:"Perfecto, confirmo tu cita ✨", action:"create_booking", session_updates:{}, action_params:{} }, sessionData, textRaw, m, phone, sock, jid)
              return
            }
          }

          // Número → cancelar (si estábamos esperando)
          if (numMatch && sessionData.stage==="awaiting_cancel" && Array.isArray(sessionData.cancelList) && sessionData.cancelList.length){
            const n = Number(numMatch[1])
            const chosen = sessionData.cancelList.find(apt=>apt.index===n)
            if (chosen){
              const success = await cancelBooking(chosen.id)
              if (success) await sendWithPresence(sock, jid, `✅ Cita cancelada: ${chosen.pretty} en ${chosen.sede}`)
              else await sendWithPresence(sock, jid, "No pude cancelar la cita. Por favor contacta directamente al salón.")
              delete sessionData.cancelList
              sessionData.stage = null
              saveSession(phone, sessionData)
              return
            }
          }

          // Detectar preferida del texto en cualquier momento
          const maybePro = parsePreferredStaffFromText(textRaw)
          if (maybePro){
            sessionData.preferredStaffId = maybePro.id
            sessionData.preferredStaffLabel = staffLabelFromId(maybePro.id)
            sessionData.overrideTeam = false
            saveSession(phone, sessionData)
          }

          // “viernes/mañana/25-08” → si ya tenemos sede+servicio, repropone
          const reqDay = parseRequestedDayFromText(textRaw, nowEU)
          if (reqDay && sessionData.sede && sessionData.selectedServiceEnvKey){
            await executeProposeTime({ fromISO: reqDay.toISOString(), exactDayOnly: true }, sessionData, phone, sock, jid)
            return
          }

          if (isCancelIntent(textRaw) && sessionData.stage!=="awaiting_cancel"){
            await executeCancelAppointment({}, sessionData, phone, sock, jid)
            return
          }

          // ===== IA normal =====
          // Rellenar categoría si se deduce del texto
          if (!sessionData.category){
            const cat = detectCategoryFromText(textRaw)
            if (cat) sessionData.category = cat
          }
          // Rellenar sede si se detecta
          const autoSede = parseSede(textRaw)
          if (autoSede && !sessionData.sede) sessionData.sede = autoSede

          const aiObj = await getAIResponse(textRaw, sessionData, phone)

          // Si la IA marcó selectedServiceLabel, resolver envKey
          if (aiObj?.session_updates?.selectedServiceLabel && sessionData.sede){
            const ek = resolveEnvKeyFromLabelAndSede(aiObj.session_updates.selectedServiceLabel, sessionData.sede)
            if (ek) aiObj.session_updates.selectedServiceEnvKey = ek
          }

          await routeAIResult(aiObj, sessionData, textRaw, m, phone, sock, jid)

        } catch (error) {
          if (BOT_DEBUG) console.error(error)
          await sendWithPresence(sock, jid, "Disculpa, hubo un error técnico. ¿Puedes repetir tu mensaje?")
        }
      })
    })
  }catch(e){ setTimeout(() => startBot().catch(console.error), 5000) }
}

// ====== Arranque
console.log(`🩷 Gapink Nails Bot v28.0.0`)
app.listen(PORT, ()=>{ startBot().catch(console.error) })
process.on("uncaughtException", (e)=>{ console.error("💥 uncaughtException:", e?.stack||e?.message||e) })
process.on("unhandledRejection", (e)=>{ console.error("💥 unhandledRejection:", e) })
process.on("SIGTERM", ()=>{ process.exit(0) })
process.on("SIGINT", ()=>{ process.exit(0) })
