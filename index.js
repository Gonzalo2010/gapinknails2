// index.js — Gapink Assistant · v30.0.0
// Gonzalo edition 🤙
// - IA full-stack: categoría → sede → servicio → staff → día/franja → reserva Square.
// - Staff por centro con aliases (cristi/cristina, carmen belén/carmen, rocio chica/…)
// - Busca huecos reales en Square (14 días, configurable), etiqueta cada slot con la profesional real.
// - “¿El viernes por la tarde?” funciona aunque falten datos (se guardan día/franja y se piden los que falten).
// - Listas por categoría (uñas, depilación, micropigmentación, faciales, pestañas), nada de mezclar.
// - Fixes: sin funciones duplicadas, SQL columns OK, paréntesis OK, robust JSON, idempotencia booking.

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
const SEARCH_WINDOW_DAYS = Number(process.env.BOT_SEARCH_WINDOW_DAYS || 14)
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
const AI_API_KEY = process.env.DEEPSEEK_API_KEY || process.env.OPENAI_API_KEY || ""
const AI_MODEL = process.env.AI_MODEL || process.env.DEEPSEEK_MODEL || "deepseek-chat"
const AI_MAX_RETRIES = Number(process.env.AI_MAX_RETRIES || 3)
const AI_TIMEOUT_MS = Number(process.env.AI_TIMEOUT_MS || 15000)
const sleep = ms => new Promise(r=>setTimeout(r, ms))

// ====== Utils básicos
const onlyDigits = s => String(s||"").replace(/\D+/g,"")
// Quita diacríticos solo para comparaciones
const rm = s => String(s||"").normalize("NFD").replace(/\p{Diacritic}/gu,"")
// Normaliza para matching laxo
const norm = s => rm(s).toLowerCase().replace(/[+.,;:()/_-]/g," ").replace(/[^\p{Letter}\p{Number}\s]/gu," ").replace(/\s+/g," ").trim()
const titleCase = (str) => String(str||"").toLowerCase().replace(/\b([a-záéíóúñ])/g, m=>m.toUpperCase())
function applySpanishDiacritics(label){
  let x = String(label||"")
  x = x.replace(/\bunias\b/gi, "uñas").replace(/\bunas\b/gi,"uñas")
  x = x.replace(/\bpestan(as?|)\b/gi, (m)=> (m[0]==='P'?'Pestañ':'pestañ') + (m.endsWith('as')?'as':'a'))
  x = x.replace(/\bnivelacion\b/gi, "nivelación")
  x = x.replace(/\bacrilic[oa]s?\b/gi, (m)=>{
    const fem = /a$/i.test(m); const pl=/s$/i.test(m)
    const base = fem ? "acrílica":"acrílico"
    return base + (pl?"s":"")
  })
  x = x.replace(/\bfrancesa?\b/gi, (m)=> (/[A-Z]/.test(m[0])?"Francés":"francés").replace("ésa","esa"))
  x = x.replace(/\bsemi ?permanente\b/gi, "semipermanente")
  x = x.replace(/\bmicroblading\b/gi, "Microblading").replace(/\bmicroshading\b/gi,"Microshading")
  x = x.replace(/\bfotodepilacion\b/gi,"Fotodepilación")
  x = x.replace(/\bdiseno\b/gi,"diseño")
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
function parseToEU(input){
  if (dayjs.isDayjs(input)) return input.clone().tz(EURO_TZ)
  const s = String(input||"")
  if (/[Zz]|[+\-]\d{2}:?\d{2}$/.test(s)) return dayjs(s).tz(EURO_TZ)
  return dayjs.tz(s, EURO_TZ)
}

// Daypart
function parseDaypart(text){
  const t=norm(text)
  if (/\b(ma[nñ]ana|morning|temprano)\b/.test(t)) return "morning"      // 09-13
  if (/\b(tarde|afternoon)\b/.test(t)) return "afternoon"               // 13-18
  if (/\b(noche|tardi[oa]|despues de|después de)\b/.test(t)) return "evening" // 18-20
  return null
}
function inDaypart(d, part){
  const h = d.hour()
  if (part==="morning") return h>=9 && h<13
  if (part==="afternoon") return h>=13 && h<18
  if (part==="evening") return h>=18 && h<20
  return true
}
function parseRequestedDayFromText(text, base=dayjs().tz(EURO_TZ)){
  const t=norm(text)
  const m = t.match(/\b(\d{1,2})[\/\-](\d{1,2})(?:[\/\-](\d{2,4}))?\b/)
  if (m){
    const dd = Number(m[1]), mm = Number(m[2])-1, yyyy = m[3]? Number(m[3].length===2 ? 2000+Number(m[3]) : m[3]) : base.year()
    const d = dayjs.tz({ year:yyyy, month:mm, day:dd }, EURO_TZ)
    if (d.isValid()) return d
  }
  const daysMap = { "lunes":1,"martes":2,"miercoles":3,"miércoles":3,"jueves":4,"viernes":5,"sabado":6,"sábado":6,"domingo":0,"hoy":"hoy","mañana":"mañana" }
  const dm = t.match(/\b(hoy|ma[nñ]ana|lunes|martes|mi[eé]rcoles|jueves|viernes|s[áa]bado|domingo)\b/i)
  if (dm){
    const key = dm[1].toLowerCase()
    if (key==="hoy") return base
    if (key==="mañana" || key==="manana") return base.clone().add(1,"day")
    const target = daysMap[key.normalize("NFD").replace(/\p{Diacritic}/gu,"")] ?? null
    if (typeof target==="number"){
      let d = base.clone().startOf("day")
      while (d.day() !== target) d = d.add(1,"day")
      return d
    }
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

// ====== DB
const db=new Database("gapink.db"); db.pragma("journal_mode = WAL")
db.exec(`
CREATE TABLE IF NOT EXISTS appointments (
  id TEXT PRIMARY KEY,
  customer_name TEXT,
  customer_phone TEXT,
  customer_square_id TEXT,
  location_key TEXT,
  category TEXT,
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
  fallback_used INTEGER DEFAULT 0,
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
  success INTEGER
);
`)
const insertAppt = db.prepare(`INSERT INTO appointments
(id,customer_name,customer_phone,customer_square_id,location_key,category,service_env_key,service_label,duration_min,start_iso,end_iso,staff_id,status,created_at,square_booking_id,square_error,retry_count)
VALUES (@id,@customer_name,@customer_phone,@customer_square_id,@location_key,@category,@service_env_key,@service_label,@duration_min,@start_iso,@end_iso,@staff_id,@status,@created_at,@square_booking_id,@square_error,@retry_count)`)

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
  if (s.dateContextISO) s.dateContextISO = s.dateContextISO
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
  return labels
}
function parseEmployees(){
  const out=[]
  for (const [k,v] of Object.entries(process.env)) {
    if (!k.startsWith("SQ_EMP_")) continue
    const [id, book, _locs] = String(v||"").split("|")
    if (!id) continue
    const bookable = (book||"").toUpperCase()==="BOOKABLE"
    let allow = (_locs||"").split(",").map(s=>s.trim()).filter(Boolean)
    // EMP_CENTER_* override
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
    const labels = deriveLabelsFromEnvKey(k).map(s=>applySpanishDiacritics(titleCase(s)))
    out.push({ envKey:k, id, bookable, allow, labels })
  }
  return out
}
const EMPLOYEES = parseEmployees()

// Aliases manuales extra (mejora matching por nombre coloquial)
const EXTRA_STAFF_ALIASES = {
  "CRISTINA": ["cristi","cristina","cristy","cristy","cristine","cristí"],
  "CARMEN_BELEN": ["carmen","carmen belen","carmen belén","belen","belén"],
  "ROCIO_CHICA": ["rocio chica","rocío chica","rocio c","rociochica","chica"],
  "ROCIO": ["rocio","rocío"],
  "PATRICIA": ["patri","patricia"],
  "DANIELA": ["daniela","dani"],
  "GINNA": ["ginna","gina"],
  "JOHANA": ["johana","yojana","yohana"],
  "ELISABETH": ["elisabeth","eli"],
  "DESI": ["desi","desiree","desirée"],
  "TANIA": ["tania"],
  "ANAIRA": ["anaira","ana ira","anai"],
  "MARIA": ["maria","maría"],
  "GANNA": ["ganna","hana","hanna"],
  "CHABELI": ["chabeli","chabely","chabelí","chabely"]
}
function staffLabelFromId(id){
  const e = EMPLOYEES.find(x=>x.id===id)
  return e?.labels?.[0] || (id ? `Profesional ${String(id).slice(-4)}` : null)
}
function isStaffAllowedInLocation(staffId, locKey){
  const e = EMPLOYEES.find(x=>x.id===staffId)
  if (!e || !e.bookable) return false
  const locId = locationToId(locKey)
  return e.allow.includes("ALL") || e.allow.includes(locId)
}
function allAllowedStaffForLocation(locKey){
  const locId = locationToId(locKey)
  return EMPLOYEES.filter(e => e.bookable && (e.allow.includes("ALL") || e.allow.includes(locId)))
}
function matchStaffByName(text, locKey=null){
  const t = norm(text)
  // 1) Busca por labels
  let candidates = EMPLOYEES.filter(e => e.bookable && e.labels.some(lbl=> norm(lbl).split(" ").some(tok=> t.includes(tok) )))
  // 2) Busca por aliases manuales
  for (const [key, arr] of Object.entries(EXTRA_STAFF_ALIASES)){
    if (arr.some(a => t.includes(norm(a)))){
      const hit = EMPLOYEES.find(e => norm(e.envKey).includes(norm(key)))
      if (hit && hit.bookable) candidates.push(hit)
    }
  }
  // unique
  const seen = new Set(); candidates = candidates.filter(c => { if (seen.has(c.id)) return false; seen.add(c.id); return true })
  if (locKey) candidates = candidates.filter(c => isStaffAllowedInLocation(c.id, locKey))
  return candidates
}

// ====== Servicios y categorías
function servicesForSedeKeyRaw(sedeKey){
  const prefix = (sedeKey==="la_luz") ? "SQ_SVC_luz_" : "SQ_SVC_"
  const out=[]
  for (const [k,v] of Object.entries(process.env)){
    if (!k.startsWith(prefix)) continue
    const [id] = String(v||"").split("|"); if (!id) continue
    const raw = k.replace(prefix,"").replaceAll("_"," ")
    let label = titleCase(raw)
    label = applySpanishDiacritics(label)
    out.push({ sedeKey, key:k, id, rawKey:k, label, norm: norm(label) })
  }
  return out
}
function allServices(){ return [...servicesForSedeKeyRaw("torremolinos"), ...servicesForSedeKeyRaw("la_luz")] }
function serviceLabelFromEnvKey(envKey){
  if (!envKey) return null
  const all = allServices()
  return all.find(s=>s.key===envKey)?.label || null
}

// Categorización
const CAT_RULES = {
  unas: {
    pos: ["uña","uñas","manicura","esculp","gel","semipermanente","press","tips","francés","frances","nivelación","nivelacion","esmalt"],
    neg: ["pestañ","ceja","depil","facial","micro","labios","eyeliner","laser","láser"]
  },
  depilacion: {
    pos: ["depil","fotodepil","axilas","piernas","ingles","pubis","perianal","labio","hilo","cejas","láser","laser","fosas nasales"],
    neg: []
  },
  micropigmentacion: {
    pos: ["microblading","microshading","efecto polvo","labios efecto aquarela","eyeliner","retoque"],
    neg: []
  },
  faciales: {
    pos: ["facial","dermapen","hydra","vitamina c","limpieza","carbon peel","jade","reafirmante","colageno","colágeno","anti acne","anti manchas","oro","endosphere","endospheres"],
    neg: []
  },
  pestanas: {
    pos: ["pestañ","lifting","extensiones","2d","3d","pelo a pelo","relleno pestañas","quitar extensiones"],
    neg: []
  }
}
function detectCategoryFromMessage(msg){
  const u = norm(msg)
  const score = (cat) => {
    let s=0
    CAT_RULES[cat].pos.forEach(p=>{ if (u.includes(norm(p))) s+=2 })
    CAT_RULES[cat].neg.forEach(n=>{ if (u.includes(norm(n))) s-=3 })
    return s
  }
  const cats = Object.keys(CAT_RULES).map(c=>({c, s:score(c)})).sort((a,b)=>b.s-a.s)
  return cats[0].s>0 ? cats[0].c : null
}
function filterServicesByCategory(sedeKey, category){
  const list = servicesForSedeKeyRaw(sedeKey)
  const rules = CAT_RULES[category]
  if (!rules) return list
  return list.filter(s => {
    const n = s.norm
    const ok = rules.pos.some(p => n.includes(norm(p)))
    const bad = rules.neg.some(nn => n.includes(norm(nn)))
    return ok && !bad
  })
}
function scoreServiceRelevance(userMsg, label){
  const u = norm(userMsg), l = norm(label); let score = 0
  const bonus = ["natural","francesa","frances","diseño","nivelacion","nivelación","axilas","piernas","ingles","pubis","cejas","labio","hilo","facial","dermapen","hydra","vitamina","microblading","microshading","eyeliner"]
  bonus.forEach(t=>{ if (u.includes(norm(t)) && l.includes(norm(t))) score += 0.5 })
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
    await sock.sendMessage(jid, { text: "Para terminar, no encuentro tu ficha por este número. Dime tu *nombre completo* y, si quieres, tu *email* 😊" })
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

// ====== DISPONIBILIDAD (full-scan + por staff + por día/franja)
function slotPassesFilters(d, { exactDayOnly=false, dayISO=null, daypart=null }){
  if (exactDayOnly && dayISO){
    const ref = dayjs(dayISO).tz(EURO_TZ)
    if (!d.isSame(ref,"day")) return false
  }
  if (daypart && !inDaypart(d, daypart)) return false
  return insideBusinessHours(d,60)
}
async function searchAvailabilityForStaff({ locationKey, envServiceKey, staffId, fromEU, days=SEARCH_WINDOW_DAYS, limitPerDay=6, exactDayOnly=false, dayISO=null, daypart=null }){
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
    const byDay = new Map()
    for (const a of avail){
      if (!a?.startAt) continue
      const d = dayjs(a.startAt).tz(EURO_TZ)
      if (!slotPassesFilters(d, { exactDayOnly, dayISO, daypart })) continue
      if (!isStaffAllowedInLocation(staffId, locationKey)) continue
      const key = d.format("YYYY-MM-DD")
      if (!byDay.has(key)) byDay.set(key, [])
      if (byDay.get(key).length < limitPerDay) byDay.get(key).push({ date:d, staffId })
    }
    const out=[]
    for (const [_, arr] of byDay) out.push(...arr)
    return out.sort((a,b)=> a.date.valueOf()-b.date.valueOf())
  }catch{ return [] }
}
async function searchAvailabilityGeneric({ locationKey, envServiceKey, fromEU, days=SEARCH_WINDOW_DAYS, limitPerDay=6, exactDayOnly=false, dayISO=null, daypart=null }){
  try{
    const sv = await getServiceIdAndVersion(envServiceKey)
    if (!sv?.id) return []
    const startAt = fromEU.tz("UTC").toISOString()
    const endAt = fromEU.clone().add(days,"day").tz("UTC").toISOString()
    const locationId = locationToId(locationKey)
    const body = { query:{ filter:{ startAtRange:{ startAt, endAt }, locationId, segmentFilters:[{ serviceVariationId: sv.id }] } } }
    const resp = await square.bookingsApi.searchAvailability(body)
    const avail = resp?.result?.availabilities || []
    const byDay = new Map()
    const slots=[]
    for (const a of avail){
      if (!a?.startAt) continue
      const d = dayjs(a.startAt).tz(EURO_TZ)
      let tm = null
      const segs = Array.isArray(a.appointmentSegments) ? a.appointmentSegments
                 : Array.isArray(a.segments) ? a.segments
                 : []
      if (segs[0]?.teamMemberId) tm = segs[0].teamMemberId
      if (tm && !isStaffAllowedInLocation(tm, locationKey)) continue
      if (!slotPassesFilters(d, { exactDayOnly, dayISO, daypart })) continue
      const key = d.format("YYYY-MM-DD")
      if (!byDay.has(key)) byDay.set(key, 0)
      if (byDay.get(key) < limitPerDay){
        byDay.set(key, byDay.get(key)+1)
        slots.push({ date:d, staffId: tm || null })
      }
    }
    return slots.sort((a,b)=> a.date.valueOf()-b.date.valueOf())
  }catch{ return [] }
}

// ====== IA (prompts con reglas)
function staffRosterForPrompt(){
  return EMPLOYEES.map(e=>{
    const locs = e.allow.map(id=> id===LOC_TORRE?"torremolinos" : id===LOC_LUZ?"la_luz" : id).join(",")
    return `• ID:${e.id} | Nombres:[${e.labels.join(", ")}] | Sedes:[${locs||"ALL"}] | Reservable:${e.bookable}`
  }).join("\n")
}
function buildSystemPrompt() {
  const nowEU = dayjs().tz(EURO_TZ);
  const staffLines = staffRosterForPrompt()
  const cats = Object.keys(CAT_RULES).map(c=>`- ${c}`).join(", ")

  return `Eres el asistente de WhatsApp para Gapink. Devuelves SOLO JSON válido.

AHORA: ${nowEU.format("dddd DD/MM/YYYY HH:mm")} (Madrid)
SEDES: torremolinos="${ADDRESS_TORRE}" | la_luz="${ADDRESS_LUZ}"
HORARIO: L-V 09:00-20:00; S/D cerrado; Festivos: ${HOLIDAYS_EXTRA.join(", ")}

CATEGORIAS: ${cats}

STAFF:
${staffLines}

REGLAS:
1) No muestres lista de servicios hasta conocer la categoría (uñas, depilación, micropigmentación, faciales, pestañas).
2) Si el cliente dice “con {nombre}”, mapea a staff y valida que atienda en la sede elegida. Si no atiende, propón alternativas válidas.
3) Propuestas de horas: usa Square para huecos reales; si se pide “viernes / por la tarde”, filtra ese día/franja.
4) Selección por número (1/2/3…): aplica a la última lista de horas o de servicios.
5) Identidad: por teléfono (buscar cliente). Solo pide nombre/email si 0 o varios matches.
6) Al confirmar, crea reserva real en Square, con *teamMemberId* exacto del slot.
7) Acciones: "choose_category" | "choose_service" | "propose_times" | "create_booking" | "list_appointments" | "cancel_appointment" | "need_info" | "none"

FORMATO:
{"message":"...","action":"...","session_updates":{...},"action_params":{...}}`
}
async function callAIOnce(messages, systemPrompt = "") {
  const controller = new AbortController()
  const timeoutId = setTimeout(() => controller.abort(), AI_TIMEOUT_MS)
  try {
    const allMessages = systemPrompt ? [{ role: "system", content: systemPrompt }, ...messages] : messages
    const url = process.env.DEEPSEEK_API_URL || process.env.OPENAI_API_URL || "https://api.deepseek.com/v1/chat/completions"
    const response = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json", "Authorization": `Bearer ${AI_API_KEY}` },
      body: JSON.stringify({ model: AI_MODEL, messages: allMessages, max_tokens: 1400, temperature: 0.4, stream: false }),
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
async function getAIResponse(userMessage, sessionData, phone) {
  const systemPrompt = buildSystemPrompt();
  const recent = db.prepare(`SELECT user_message, ai_response FROM ai_conversations WHERE phone = ? ORDER BY timestamp DESC LIMIT 6`).all(phone);
  const conversationHistory = recent.reverse().map(msg => [
    { role: "user", content: msg.user_message },
    { role: "assistant", content: msg.ai_response }
  ]).flat();

  const sessionContext = `
ESTADO:
- Categoría: ${sessionData?.category || '—'}
- Sede: ${sessionData?.sede || '—'}
- Servicio: ${sessionData?.selectedServiceLabel || '—'} (${sessionData?.selectedServiceEnvKey || 'no_key'})
- Profesional pref.: ${sessionData?.preferredStaffLabel || '—'}
- Fecha/hora pendiente: ${sessionData?.pendingDateTime ? fmtES(parseToEU(sessionData.pendingDateTime)) : '—'}
- Día contexto: ${sessionData?.dateContextISO ? dayjs(sessionData.dateContextISO).tz(EURO_TZ).format("DD/MM") : '—'}
- Franja: ${sessionData?.preferredDaypart || '—'}
- Etapa: ${sessionData?.stage || '—'}
- Últimas horas: ${Array.isArray(sessionData?.lastHours) ? sessionData.lastHours.length + ' opciones' : '—'}
`;

  const messages = [
    ...conversationHistory,
    { role: "user", content: `MENSAJE DEL CLIENTE: "${userMessage}"\n\n${sessionContext}\n\nINSTRUCCIÓN: Devuelve SOLO JSON siguiendo las reglas.` }
  ];

  const aiText = await callAIWithRetries(messages, systemPrompt)
  if (!aiText || /^error de conexión/i.test(aiText.trim())) return buildLocalFallback(userMessage, sessionData)

  const cleaned = aiText.replace(/```json\s*/gi, "").replace(/```\s*/g, "").replace(/^[^{]*/, "").replace(/[^}]*$/, "").trim()
  try { return JSON.parse(cleaned) } catch { return buildLocalFallback(userMessage, sessionData) }
}

// ====== Fallback local mínimo (por si la IA falla)
function buildLocalFallback(userMessage, sessionData){
  const msg = String(userMessage||"").trim()
  const lower = norm(msg)
  const numMatch = lower.match(/^(?:opcion|opción)?\s*([1-9]\d*)\b/)
  const yesMatch = /\b(si|sí|ok|vale|confirmo|de\ acuerdo)\b/i.test(msg)

  const hasCore = (s)=> s?.sede && s?.selectedServiceEnvKey && (s?.pendingDateTime || s?.dateContextISO)

  if (numMatch && Array.isArray(sessionData?.lastHours) && sessionData.lastHours.length){
    const idx = Number(numMatch[1]) - 1
    const pick = sessionData.lastHours[idx]
    if (dayjs.isDayjs(pick)){
      const iso = pick.format("YYYY-MM-DDTHH:mm")
      const staffFromIso = sessionData?.lastStaffByIso?.[iso] || null
      const updates = { pendingDateTime: pick.tz(EURO_TZ).toISOString() }
      if (staffFromIso) { updates.preferredStaffId = staffFromIso; updates.preferredStaffLabel = null }
      const okToCreate = hasCore({...sessionData, ...updates})
      return { message: okToCreate ? "Perfecto, voy a confirmar esa hora 👍" : "Genial. Me falta algún dato.", action: okToCreate ? "create_booking" : "need_info", session_updates: updates, action_params: {} }
    }
  }
  if (yesMatch){
    if (hasCore(sessionData)){
      return { message:"¡Voy a crear la reserva! ✨", action:"create_booking", session_updates:{}, action_params:{} }
    } else {
      const faltan=[]; if (!sessionData?.category) faltan.push("categoría"); if (!sessionData?.sede) faltan.push("sede"); if (!sessionData?.selectedServiceEnvKey) faltan.push("servicio"); if (!sessionData?.pendingDateTime && !sessionData?.dateContextISO) faltan.push("día/hora")
      return { message:`Para proponerte horas dime: ${faltan.join(" y ")}.`, action:"need_info", session_updates:{}, action_params:{} }
    }
  }

  // Señal de categoría
  const cat = detectCategoryFromMessage(msg)
  if (!sessionData?.category && cat){
    return { message:`Ok, ${cat}. ¿Qué sede prefieres, Torremolinos o La Luz?`, action:"choose_service", session_updates:{ category:cat }, action_params:{} }
  }
  return { message:"¿Quieres reservar, cancelar o ver tus citas? Dime categoría (uñas, depilación…), sede y si tienes preferencia de profesional.", action:"none", session_updates:{}, action_params:{} }
}

// ====== Helpers chat
function parseSede(text){
  const t=norm(text)
  if (/\b(luz|la luz)\b/.test(t)) return "la_luz"
  if (/\b(torre|torremolinos)\b/.test(t)) return "torremolinos"
  return null
}
function cleanDisplayLabel(label){
  const s = String(label||"").replace(/^\s*(luz|la\s*luz)\s+/i,"").trim()
  return applySpanishDiacritics(s)
}

// ====== Menús
function buildServiceChoiceListByCategory(sedeKey, userMsg, category){
  const items = filterServicesByCategory(sedeKey, category)
  const scored = items.map(s => ({ ...s, _score: scoreServiceRelevance(userMsg, s.label) }))
  scored.sort((a,b)=> b._score - a._score || a.label.localeCompare(b.label))
  return scored.map((s,i)=>({ index:i+1, label:s.label }))
}

async function executeChooseService(params, sessionData, phone, sock, jid, userMsg){
  if (!sessionData.category){
    sessionData.stage = "awaiting_category"
    saveSession(phone, sessionData)
    await sock.sendMessage(jid, { text: "¿Qué categoría necesitas? *Uñas*, *Depilación*, *Micropigmentación*, *Faciales* o *Pestañas*." })
    return
  }
  if (!sessionData.sede){
    sessionData.stage = "awaiting_sede_for_services"
    saveSession(phone, sessionData)
    await sock.sendMessage(jid, { text: "¿En qué sede? Torremolinos o La Luz." })
    return
  }
  const items = buildServiceChoiceListByCategory(sessionData.sede, userMsg||"", sessionData.category)
  if (!items.length){
    await sock.sendMessage(jid, { text: `No tengo servicios de *${sessionData.category}* configurados en ${locationNice(sessionData.sede)}.` })
    return
  }
  sessionData.serviceChoices = items
  sessionData.stage = "awaiting_service_choice"
  saveSession(phone, sessionData)
  const lines = items.slice(0, 25).map(it=> `${it.index}) ${applySpanishDiacritics(it.label)}`).join("\n")
  await sock.sendMessage(jid, { text: `Opciones de *${sessionData.category}* en ${locationNice(sessionData.sede)}:\n\n${lines}\n\nResponde con el número.` })
}

// ====== Proponer horas (full scan, staff-aware, day/daypart)
async function executeProposeTime(params, sessionData, phone, sock, jid) {
  const nowEU = dayjs().tz(EURO_TZ)
  const baseFrom = params?.fromISO ? parseToEU(params.fromISO) : nextOpeningFrom(nowEU.add(NOW_MIN_OFFSET_MIN, "minute"))
  const exactDayOnly = !!params?.exactDayOnly
  const dayISO = exactDayOnly ? (params?.fromISO || sessionData?.dateContextISO || null) : null
  const daypart = params?.daypart || sessionData.preferredDaypart || null

  if (!sessionData.sede || !sessionData.selectedServiceEnvKey) { await sock.sendMessage(jid, { text: "Necesito la sede y el servicio primero." }); return; }

  let slots = []
  let usedPreferred = false

  const wantStaffId = sessionData.preferredStaffId || params?.staffId || null
  if (wantStaffId && isStaffAllowedInLocation(wantStaffId, sessionData.sede)) {
    const staffSlots = await searchAvailabilityForStaff({
      locationKey: sessionData.sede,
      envServiceKey: sessionData.selectedServiceEnvKey,
      staffId: wantStaffId,
      fromEU: baseFrom, days: SEARCH_WINDOW_DAYS, limitPerDay: 6,
      exactDayOnly, dayISO, daypart
    })
    if (staffSlots.length){ slots = staffSlots; usedPreferred = true }
  }
  if (!slots.length) {
    const generic = await searchAvailabilityGeneric({
      locationKey: sessionData.sede,
      envServiceKey: sessionData.selectedServiceEnvKey,
      fromEU: baseFrom, days: SEARCH_WINDOW_DAYS, limitPerDay: 6,
      exactDayOnly, dayISO, daypart
    })
    slots = generic
  }
  if (!slots.length) {
    await sock.sendMessage(jid, { text: "No veo huecos disponibles con esos filtros. ¿Otra fecha u otra franja?" })
    return
  }

  // Mapear staff por ISO
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

  const lines = hoursEnum.slice(0, 20).map(h => {
    const sid = map[h.iso]
    const tag = sid ? ` — ${staffLabelFromId(sid)}` : ""
    return `${h.index}) ${h.pretty}${tag}`
  }).join("\n")
  const staffNote = sessionData.preferredStaffLabel
    ? (usedPreferred
       ? `Horarios disponibles con ${sessionData.preferredStaffLabel}:`
       : `Horarios disponibles (nuestro equipo):\nNota: no veo huecos con ${sessionData.preferredStaffLabel} en los próximos días; te muestro alternativas.`)
    : `Horarios disponibles (nuestro equipo):`
  await sock.sendMessage(jid, { text: `${staffNote}\n${lines}\n\nResponde con el número.` })
}

// ====== Crear reserva
async function executeCreateBooking(_params, sessionData, phone, sock, jid) {
  if (!sessionData.sede) { await sock.sendMessage(jid, { text: "Falta seleccionar la sede (Torremolinos o La Luz)" }); return; }
  if (!sessionData.selectedServiceEnvKey) { await sock.sendMessage(jid, { text: "Falta seleccionar el servicio" }); return; }

  const startEU = sessionData.pendingDateTime
    ? parseToEU(sessionData.pendingDateTime)
    : (sessionData.dateContextISO ? parseToEU(sessionData.dateContextISO).hour(OPEN.start).minute(0) : null)

  if (!startEU) { await sock.sendMessage(jid, { text: "Falta seleccionar la fecha y hora" }); return; }
  if (!insideBusinessHours(startEU, 60)) { await sock.sendMessage(jid, { text: "Esa hora está fuera del horario (L-V 09:00–20:00)" }); return; }

  const iso = startEU.format("YYYY-MM-DDTHH:mm")
  let staffId = sessionData.lastProposeUsedPreferred ? (sessionData.preferredStaffId || sessionData.lastStaffByIso?.[iso] || null)
                                                    : (sessionData.lastStaffByIso?.[iso] || sessionData.preferredStaffId || null)
  if (staffId && !isStaffAllowedInLocation(staffId, sessionData.sede)) staffId = null
  if (!staffId) {
    // intentamos fijar staff del slot real
    const probe = await searchAvailabilityGeneric({
      locationKey: sessionData.sede,
      envServiceKey: sessionData.selectedServiceEnvKey,
      fromEU: startEU.clone().subtract(1, "minute"),
      days: 1, limitPerDay: 20, exactDayOnly: true, dayISO: startEU.toISOString()
    })
    const match = probe.find(x => x.date.isSame(startEU, "minute"))
    if (match?.staffId && isStaffAllowedInLocation(match.staffId, sessionData.sede)) staffId = match.staffId
  }
  if (!staffId) {
    const allowed = allAllowedStaffForLocation(sessionData.sede)
    staffId = allowed[0]?.id || null
  }
  if (!staffId) { await sock.sendMessage(jid, { text: "No hay profesionales disponibles en esa sede" }); return; }

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

  const result = await createBookingWithRetry({
    startEU, locationKey: sessionData.sede, envServiceKey: sessionData.selectedServiceEnvKey,
    durationMin: 60, customerId, teamMemberId: staffId, phone
  })
  if (!result.success) {
    const aptId = `apt_failed_${Math.random().toString(36).slice(2,8)}${Date.now().toString(36).slice(-4)}`
    insertAppt.run({
      id: aptId, customer_name: sessionData?.name || null, customer_phone: phone,
      customer_square_id: customerId, location_key: sessionData.sede, category: sessionData?.category || null,
      service_env_key: sessionData.selectedServiceEnvKey, service_label: sessionData.selectedServiceLabel || serviceLabelFromEnvKey(sessionData.selectedServiceEnvKey) || "Servicio",
      duration_min: 60, start_iso: startEU.tz("UTC").toISOString(),
      end_iso: startEU.clone().add(60, "minute").tz("UTC").toISOString(),
      staff_id: staffId, status: "failed", created_at: new Date().toISOString(),
      square_booking_id: null, square_error: result.error, retry_count: SQUARE_MAX_RETRIES
    })
    await sock.sendMessage(jid, { text: "No pude crear la reserva ahora. ¿Quieres que te proponga otro horario?" })
    return
  }

  if (result.booking.__sim) { await sock.sendMessage(jid, { text: "🧪 SIMULACIÓN: Reserva creada exitosamente (modo prueba)" }); clearSession(phone); return }

  const aptId = `apt_${Math.random().toString(36).slice(2,8)}${Date.now().toString(36).slice(-4)}`
  insertAppt.run({
    id: aptId, customer_name: sessionData?.name || null, customer_phone: phone,
    customer_square_id: customerId, location_key: sessionData.sede, category: sessionData?.category || null,
    service_env_key: sessionData.selectedServiceEnvKey, service_label: sessionData.selectedServiceLabel || serviceLabelFromEnvKey(sessionData.selectedServiceEnvKey) || "Servicio",
    duration_min: 60, start_iso: startEU.tz("UTC").toISOString(),
    end_iso: startEU.clone().add(60, "minute").tz("UTC").toISOString(),
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
  if (!appointments.length) { await sock.sendMessage(jid, { text: "No tienes citas programadas. ¿Quieres agendar una?" }); return; }
  const message = `Tus próximas citas (asociadas a tu número):\n\n${appointments.map(apt => 
    `${apt.index}) ${apt.pretty}\n📍 ${apt.sede}\n👩‍💼 ${apt.profesional}\n`
  ).join("\n")}`;
  await sock.sendMessage(jid, { text: message });
}
async function executeCancelAppointment(params, sessionData, phone, sock, jid) {
  const appointments = await enumerateCitasByPhone(phone);
  if (!appointments.length) { await sock.sendMessage(jid, { text: "No encuentro citas futuras asociadas a tu número. ¿Quieres que te ayude a reservar?" }); return; }
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
  if (!appointment) { await sock.sendMessage(jid, { text: "No encontré esa cita. ¿Puedes verificar el número?" }); return; }
  const success = await cancelBooking(appointment.id);
  if (success) { await sock.sendMessage(jid, { text: `✅ Cita cancelada: ${appointment.pretty} en ${appointment.sede}` }) }
  else { await sock.sendMessage(jid, { text: "No pude cancelar la cita. Por favor contacta directamente al salón." }) }
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
  .card{max-width:680px;padding:32px;border-radius:20px;box-shadow:0 8px 32px rgba(0,0,0,.1);background:white}
  .status{padding:12px;border-radius:8px;margin:8px 0}
  .success{background:#d4edda;color:#155724}
  .error{background:#f8d7da;color:#721c24}
  .warning{background:#fff3cd;color:#856404}
  .stat{display:inline-block;margin:0 16px;padding:8px 12px;background:#e9ecef;border-radius:6px}
  </style><div class="card">
  <h1>🩷 Gapink Bot v30.0.0</h1>
  <div class="status ${conectado ? 'success' : 'error'}">Estado WhatsApp: ${conectado ? "✅ Conectado" : "❌ Desconectado"}</div>
  ${!conectado&&lastQR?`<div style="text-align:center;margin:20px 0"><img src="/qr.png" width="300" style="border-radius:8px"></div>`:""}
  <div class="status warning">Modo: ${DRY_RUN ? "🧪 Simulación" : "🚀 Producción"}</div>
  <h3>📊 Estadísticas</h3>
  <div><span class="stat">📅 Total: ${totalAppts}</span><span class="stat">✅ Exitosas: ${successAppts}</span><span class="stat">❌ Fallidas: ${failedAppts}</span></div>
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
let RECONNECT_SCHEDULED = false
let RECONNECT_ATTEMPTS = 0
const QUEUE=new Map()
function enqueue(key,job){
  const prev=QUEUE.get(key)||Promise.resolve()
  const next=prev.then(job,job).finally(()=>{ if (QUEUE.get(key)===next) QUEUE.delete(key) })
  QUEUE.set(key,next); return next
}
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

function parsePreferredStaffFromText(text, locKey=null){
  const candidates = matchStaffByName(text, locKey)
  if (candidates.length) return candidates[0]
  return null
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
            greeted: false, category:null, sede: null, selectedServiceEnvKey: null, selectedServiceLabel: null,
            preferredStaffId: null, preferredStaffLabel: null, pendingDateTime: null,
            name: null, email: null, last_msg_id: null, lastStaffByIso: {},
            lastProposeUsedPreferred: false, stage: null, cancelList: null,
            serviceChoices: null, identityChoices: null,
            lastStaffNamesById: null, dateContextISO: null, preferredDaypart: null,
            snooze_until_ms: null, identityResolvedCustomerId: null
          }
          if (sessionData.last_msg_id === m.key.id) return
          sessionData.last_msg_id = m.key.id

          const nowEU = dayjs().tz(EURO_TZ)
          const lower = norm(textRaw)
          const numMatch = lower.match(/^(?:opcion|opción)?\s*([1-9]\d*)\b/)

          if (isFromMe) { saveSession(phone, sessionData); return }

          // === "." silencia 6h
          if (textRaw === ".") {
            sessionData.snooze_until_ms = nowEU.add(6, "hour").valueOf()
            saveSession(phone, sessionData)
            return
          }
          if (sessionData.snooze_until_ms && nowEU.valueOf() < sessionData.snooze_until_ms) {
            saveSession(phone, sessionData)
            return
          }

          // === PRE: identidad pick
          if (sessionData.stage==="awaiting_identity_pick"){
            if (!numMatch){ await sock.sendMessage(jid, { text: "Responde con el número de tu ficha (1, 2, ...)." }); return }
            const n = Number(numMatch[1])
            const choice = (sessionData.identityChoices||[]).find(c=>c.index===n)
            if (!choice){ await sock.sendMessage(jid, { text: "No encontré esa opción. Prueba con el número de la lista." }); return }
            sessionData.identityResolvedCustomerId = choice.id
            sessionData.stage = null
            saveSession(phone, sessionData)
            await sock.sendMessage(jid, { text: "¡Gracias! Finalizo tu reserva…" })
            await executeCreateBooking({}, sessionData, phone, sock, jid)
            return
          }

          // === PRE: identidad crear
          if (sessionData.stage==="awaiting_identity"){
            const emailMatch = String(textRaw||"").match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i)
            const email = emailMatch ? emailMatch[0] : null
            const name = String(textRaw||"").replace(email||"", "").replace(/(email|correo)[:\s]*/ig,"").trim()
            if (name) sessionData.name = name
            if (email) sessionData.email = email
            const created = await findOrCreateCustomerWithRetry({ name: sessionData.name, email: sessionData.email, phone })
            if (!created){ await sock.sendMessage(jid, { text: "No pude crear tu ficha. ¿Puedes repetir tu *nombre* y (opcional) tu *email*?" }); return }
            sessionData.identityResolvedCustomerId = created.id
            sessionData.stage = null
            saveSession(phone, sessionData)
            await sock.sendMessage(jid, { text: "¡Gracias! Finalizo tu reserva…" })
            await executeCreateBooking({}, sessionData, phone, sock, jid)
            return
          }

          // === PRE: elegir sede si estamos esperando para servicios
          if (sessionData.stage==="awaiting_sede_for_services"){
            const sede = parseSede(textRaw)
            if (sede){
              sessionData.sede = sede
              sessionData.stage = null
              saveSession(phone, sessionData)
              await executeChooseService({ }, sessionData, phone, sock, jid, textRaw)
              return
            }
          }

          // === PRE: selección de servicio (por número)
          if (numMatch && sessionData.stage==="awaiting_service_choice" && Array.isArray(sessionData.serviceChoices) && sessionData.serviceChoices.length){
            const idx = Number(numMatch[1]) - 1
            const pick = sessionData.serviceChoices[idx]
            if (!pick){ await sock.sendMessage(jid, { text:"No encontré esa opción. Usa un número de la lista." }); return }
            sessionData.selectedServiceLabel = applySpanishDiacritics(pick.label)
            if (sessionData.sede){
              const ek = resolveEnvKeyFromLabelAndSede(pick.label, sessionData.sede)
              if (ek) sessionData.selectedServiceEnvKey = ek
            }
            sessionData.stage = null
            saveSession(phone, sessionData)
            await executeProposeTime({ exactDayOnly: !!sessionData.dateContextISO, fromISO: sessionData.dateContextISO || null, daypart: sessionData.preferredDaypart || null }, sessionData, phone, sock, jid)
            return
          }

          // === PRE: selección de horario (por número)
          if (numMatch && Array.isArray(sessionData.lastHours) && sessionData.lastHours.length && (!sessionData.stage || sessionData.stage==="awaiting_time")){
            const idx = Number(numMatch[1]) - 1
            const pick = sessionData.lastHours[idx]
            if (dayjs.isDayjs(pick)){
              const iso = pick.format("YYYY-MM-DDTHH:mm")
              const staffFromIso = sessionData?.lastStaffByIso?.[iso] || null
              if (staffFromIso && !isStaffAllowedInLocation(staffFromIso, sessionData.sede)) {
                await sock.sendMessage(jid, { text: "Esa hora ya no está disponible con esa profesional en esa sede. Te paso otras opciones 👇" })
                await executeProposeTime({}, sessionData, phone, sock, jid)
                return
              }
              sessionData.pendingDateTime = pick.tz(EURO_TZ).toISOString()
              if (staffFromIso){ sessionData.preferredStaffId = staffFromIso; sessionData.preferredStaffLabel = null }
              saveSession(phone, sessionData)
              const aiObj = { message:"Perfecto, confirmo tu cita ✨", action:"create_booking", session_updates:{}, action_params:{} }
              await routeAIResult(aiObj, sessionData, textRaw, m, phone, sock, jid)
              return
            }
          }

          // === PRE: cancelar (intención)
          if (/\b(cancelar|anular)\b/.test(lower) && /\b(cita|reserva)\b/.test(lower) && sessionData.stage!=="awaiting_cancel"){
            await executeCancelAppointment({}, sessionData, phone, sock, jid)
            return
          }

          // === “viernes / por la tarde / mañana”: guardar y actuar cuando haya datos
          {
            const base = sessionData?.dateContextISO ? dayjs(sessionData.dateContextISO).tz(EURO_TZ) : dayjs().tz(EURO_TZ)
            const reqDay = parseRequestedDayFromText(textRaw, base)
            const reqPart = parseDaypart(textRaw)
            let saved=false
            if (reqDay){ sessionData.dateContextISO = reqDay.clone().startOf("day").toISOString(); saved=true }
            if (reqPart){ sessionData.preferredDaypart = reqPart; saved=true }
            if (saved){ saveSession(phone, sessionData) }
            if (saved){
              if (sessionData.sede && sessionData.selectedServiceEnvKey){
                const dayToUse = reqDay ? reqDay : (sessionData.dateContextISO ? dayjs(sessionData.dateContextISO).tz(EURO_TZ) : null)
                await executeProposeTime({ fromISO: dayToUse ? dayToUse.toISOString() : null, exactDayOnly: !!dayToUse, daypart: sessionData.preferredDaypart || null }, sessionData, phone, sock, jid)
                return
              } else {
                const missing=[]
                if (!sessionData.category) missing.push("categoría")
                if (!sessionData.sede) missing.push("sede")
                if (!sessionData.selectedServiceEnvKey) missing.push("servicio")
                const resumen = `${reqDay? fmtES(reqDay.clone().hour(9).minute(0)):""} ${reqPart? (reqPart==="morning"?"(mañana)":reqPart==="afternoon"?"(tarde)":"(noche)"):""}`.trim()
                await sock.sendMessage(jid, { text: `${resumen?`Perfecto, apunto *${resumen}*.\n`:``}Para buscar huecos necesito ${missing.join(" y ")}.` })
                return
              }
            }
          }

          // === “con {nombre}” -> marcar preferencia staff
          {
            const maybe = parsePreferredStaffFromText(textRaw, sessionData.sede || parseSede(textRaw) || null)
            if (maybe){
              // validar sede
              const loc = sessionData.sede || parseSede(textRaw) || null
              if (loc && !isStaffAllowedInLocation(maybe.id, loc)){
                const alternatives = allAllowedStaffForLocation(loc).slice(0,6).map(e=>applySpanishDiacritics(e.labels[0])).join(", ")
                await sock.sendMessage(jid, { text: `${applySpanishDiacritics(maybe.labels[0])} no atiende en ${locationNice(loc)}. Disponibles: ${alternatives}.` })
              } else {
                sessionData.preferredStaffId = maybe.id
                sessionData.preferredStaffLabel = applySpanishDiacritics(maybe.labels[0])
                saveSession(phone, sessionData)
                // si ya hay sede+servicio, proponemos
                if (sessionData.sede && sessionData.selectedServiceEnvKey){
                  await executeProposeTime({ staffId: maybe.id, exactDayOnly: !!sessionData.dateContextISO, fromISO: sessionData.dateContextISO || null, daypart: sessionData.preferredDaypart || null }, sessionData, phone, sock, jid)
                  return
                }
              }
            }
          }

          // ===== IA normal
          const aiObj = await getAIResponse(textRaw, sessionData, phone)

          // Aplicar updates de sesión
          if (aiObj?.session_updates) {
            Object.keys(aiObj.session_updates).forEach(key => {
              if (aiObj.session_updates[key] !== null && aiObj.session_updates[key] !== undefined) {
                sessionData[key] = aiObj.session_updates[key]
              }
            })
          }
          // Sede inferida → set envKey
          if (sessionData.sede && sessionData.selectedServiceLabel && !sessionData.selectedServiceEnvKey){
            const ek = resolveEnvKeyFromLabelAndSede(sessionData.selectedServiceLabel, sessionData.sede)
            if (ek) sessionData.selectedServiceEnvKey = ek
          }

          // Persistir conversación IA
          const fallbackUsedBool = !!aiObj.__fallback_used
          insertAIConversation.run({
            phone, message_id: m.key.id, user_message: textRaw,
            ai_response: safeJSONStringify(aiObj), timestamp: new Date().toISOString(),
            session_data: safeJSONStringify(sessionData),
            ai_error: (typeof aiObj.__ai_error === "string" || aiObj.__ai_error == null) ? (aiObj.__ai_error ?? null) : safeJSONStringify(aiObj.__ai_error),
            fallback_used: Number(fallbackUsedBool)
          })
          saveSession(phone, sessionData)

          // Route
          switch (aiObj.action) {
            case "choose_category":
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
            default:
              if (!sessionData.selectedServiceEnvKey && sessionData.category){
                await executeChooseService({ }, sessionData, phone, sock, jid, textRaw)
              } else {
                await sock.sendMessage(jid, { text: aiObj.message || "¿Puedes repetirlo, por favor?" })
              }
          }

        } catch (error) {
          if (BOT_DEBUG) console.error(error)
          await sock.sendMessage(jid, { text: "Disculpa, hubo un error técnico. ¿Puedes repetir tu mensaje?" })
        }
      })
    })
  }catch(e){ setTimeout(() => startBot().catch(console.error), 5000) }
}

async function routeAIResult(aiObj, sessionData, textRaw, m, phone, sock, jid){
  // (ya manejado en el switch dentro del upsert)
  return
}

// ====== Arranque
console.log(`🩷 Gapink Bot v30.0.0`)
app.listen(PORT, ()=>{ startBot().catch(console.error) })
process.on("uncaughtException", (e)=>{ console.error("💥 uncaughtException:", e?.stack||e?.message||e) })
process.on("unhandledRejection", (e)=>{ console.error("💥 unhandledRejection:", e) })
process.on("SIGTERM", ()=>{ process.exit(0) })
process.on("SIGINT", ()=>{ process.exit(0) })
