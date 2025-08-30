// index.js — Gapink Nails · v31.5.0 (empleados sin ubicación)
// - Staff global (sin mapeo por salón en ENV).
// - Disponibilidad filtrada por locationId en Square (según salón elegido).
// - Si un staff no tiene huecos en ese salón/rango, se cae a equipo automáticamente.
// - Cambios v31.5.0: consultas de info/editar/cancelar cita → redirigen a email/SMS; logging exhaustivo.

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
const WORK_DAYS = [1,2,3,4,5]        // L–V (sábado NO)
const SLOT_MIN = 30
const OPEN = { start: 9, end: 20 }
const NOW_MIN_OFFSET_MIN = Number(process.env.BOT_NOW_OFFSET_MIN || 30)
const SEARCH_WINDOW_DAYS = Number(process.env.BOT_SEARCH_WINDOW_DAYS || 30)
const HOLIDAYS_EXTRA = (process.env.HOLIDAYS_EXTRA || "06/01,28/02,15/08,12/10,01/11,06/12,08/12,25/12")
  .split(",").map(s=>s.trim()).filter(Boolean)
// — límite de resultados mostrados (top N)
const SHOW_TOP_N = Number(process.env.SHOW_TOP_N || 5)

// ====== Flags
const BOT_DEBUG = /^true$/i.test(process.env.BOT_DEBUG || "")
const DRY_RUN = /^true$/i.test(process.env.DRY_RUN || "")
const SQUARE_MAX_RETRIES = Number(process.env.SQUARE_MAX_RETRIES || 3)
const LOG_LEVEL = process.env.LOG_LEVEL || "info"

// ====== Logger (consola)
const logger = pino({ level: LOG_LEVEL })

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
        body: JSON.stringify({ model: DEEPSEEK_MODEL, messages, temperature:0.2, max_tokens:1000 }),
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
        body: JSON.stringify({ model: OPENAI_MODEL, messages, temperature:0.2, max_tokens:1000 }),
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

// ====== Utils básicos
const onlyDigits = s => String(s||"").replace(/\D+/g,"")
const rm = s => String(s||"").normalize("NFD").replace(/\p{Diacritic}/gu,"")
const norm = s => rm(s).toLowerCase().replace(/[+.,;:()/_-]/g," ").replace(/[^\p{Letter}\p{Number}\s]/gu," ").replace(/\s+/g," ").trim()
function stableKey(parts){ const raw=Object.values(parts).join("|"); return createHash("sha256").update(raw).digest("hex").slice(0,48) }

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
CREATE TABLE IF NOT EXISTS bot_logs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  phone TEXT,
  direction TEXT,         -- 'in' | 'out' | 'sys'
  intent TEXT,            -- intención detectada
  action TEXT,            -- acción/resolvedor
  stage TEXT,             -- etapa sesión
  raw_text TEXT,          -- mensaje recibido
  reply_text TEXT,        -- respuesta enviada
  timestamp TEXT,
  extra TEXT              -- JSON con detalles
);
`)
const insertAppt = db.prepare(`INSERT INTO appointments
(id,customer_name,customer_phone,customer_square_id,location_key,service_env_key,service_label,duration_min,start_iso,end_iso,staff_id,status,created_at,square_booking_id,square_error,retry_count)
VALUES (@id,@customer_name,@customer_phone,@customer_square_id,@location_key,@service_env_key,@service_label,@duration_min,@start_iso,@end_iso,@staff_id,@status,@created_at,@square_booking_id,@square_error,@retry_count)`)

const insertSquareLog = db.prepare(`INSERT INTO square_logs
(phone, action, request_data, response_data, error_data, timestamp, success)
VALUES (@phone, @action, @request_data, @response_data, @error_data, @timestamp, @success)`)

const insertBotLog = db.prepare(`INSERT INTO bot_logs
(phone, direction, intent, action, stage, raw_text, reply_text, timestamp, extra)
VALUES (@phone, @direction, @intent, @action, @stage, @raw_text, @reply_text, @timestamp, @extra)`)

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

// ====== Logging helpers
function logBot({ phone, direction, intent=null, action=null, stage=null, raw_text=null, reply_text=null, extra=null }){
  const payload = {
    phone: phone || 'unknown',
    direction: direction || 'sys',
    intent, action, stage,
    raw_text, reply_text,
    timestamp: new Date().toISOString(),
    extra: extra ? safeJSONStringify(extra) : null
  }
  try { insertBotLog.run(payload) } catch(e){ /* swallow */ }
  const c = { ...payload, extra: extra || undefined }
  if (direction === 'in') logger.info({ msg:"[IN] hemos recibido este mensaje", ...c })
  else if (direction === 'out') logger.info({ msg:"[OUT] le vamos a responder", ...c })
  else logger.info({ msg:"[SYS] evento", ...c })
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

// ====== Empleadas (sin ubicación en ENV)
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
// Ahora solo comprobamos si es bookable, ignorando el salón
function isStaffAllowedInLocation(staffId, _locKey){
  const e = EMPLOYEES.find(x=>x.id===staffId)
  return !!(e && e.bookable)
}
// Elegimos cualquier bookable (o el preferido)
function pickStaffForLocation(_locKey, preferId=null){
  if (preferId){
    const e = EMPLOYEES.find(x=>x.id===preferId && x.bookable)
    if (e) return e.id
  }
  const found = EMPLOYEES.find(e=>e.bookable)
  return found?.id || null
}

// Aliases para nombres
const NAME_ALIASES = [
  ["patri","patricia"],["patricia","patri"],
  ["cristi","cristina","cristy"],
  ["rocio chica","rociochica","rocio  chica","rocio c","rocio chica"],["rocio","rosio"],
  ["carmen belen","carmen","belen"],["tania","tani"],["johana","joana","yohana"],["ganna","gana"],
  ["ginna","gina"],["chabely","chabeli","chabelí"],["elisabeth","elisabet","elis"],
  ["desi","desiree","desirée"],["daniela","dani"],["jamaica","jahmaica"],["edurne","edur"],
  ["sudemis","sude"],["maria","maría"],["anaira","an aira"],["thalia","thalía","talia","talía"]
]
function fuzzyStaffFromText(text){
  const tnorm = norm(text)
  if (/\b(con el equipo|me da igual|cualquiera|con quien sea|lo que haya)\b/i.test(tnorm)) return { anyTeam:true }
  const t = " " + tnorm + " "
  const m = t.match(/\scon\s+([a-zñáéíóú ]{2,})\b/i)
  let token = m ? norm(m[1]).trim() : null
  if (!token){
    const nm = t.match(/\b(patri|patricia|cristi|cristina|rocio chica|rocio|carmen belen|carmen|belen|ganna|maria|anaira|ginna|daniela|desi|jamaica|johana|edurne|sudemis|tania|chabely|elisabeth|thalia|thalía|talia|talía)\b/i)
    if (nm) token = norm(nm[0])
  }
  if (!token) return null
  for (const arr of NAME_ALIASES){ if (arr.some(a=>token.includes(a))) { token = arr[0]; break } }
  for (const e of EMPLOYEES){ for (const lbl of e.labels){ if (norm(lbl).includes(token)) return e } }
  return null
}
function staffRosterForPrompt(){
  return EMPLOYEES.map(e=>{
    return `• ID:${e.id} | Nombres:[${e.labels.join(", ")}] | Reservable:${e.bookable}`
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

// ====== Categorías y filtros
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

// ====== Square helpers
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
    await botReply(sock, jid, phone, { text:"Para terminar, no encuentro tu ficha por este número. Dime tu *nombre completo* y, si quieres, tu *email* para crearte 😊" }, { intent:"identity_new_prompt", action:"ask_identity" }, sessionData)
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
  await botReply(sock, jid, phone, { text:`Para terminar, he encontrado varias fichas con tu número. ¿Cuál eres?\n\n${lines}\n\nResponde con el número.` }, { intent:"identity_pick_prompt", action:"ask_identity_pick" }, sessionData)
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
async function getServiceIdAndVersion(envServiceKey){
  const raw = process.env[envServiceKey]; if (!raw) return null
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

// ====== DISPONIBILIDAD (staff global, Square filtra por locationId)
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
  if (targetDay!=null){
    let d = now.clone()
    while (d.day() !== targetDay) d = d.add(1,"day")
    when = d
  }
  let part = null
  if (/\bpor la manana\b/.test(t) || (/\bmanana\b/.test(t) && !when)) part="mañana"
  if (/\btarde\b/.test(t)) part="tarde"
  if (/\bnoche\b/.test(t)) part="noche"

  const nextWeek = /\b(pr[oó]xima\s+semana|semana\s+que\s+viene)\b/i.test(t)
  return { when, part, nextWeek }
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
    if (part){
      const { start, end } = partOfDayWindow(d, part)
      if (!(d.isSame(start,"day") && d.isAfter(start.subtract(1,"minute")) && d.isBefore(end.add(1,"minute")))) continue
    }
    out.push({ date:d, staffId: tm || null })
    if (out.length>=limit) break
  }
  return out
}

// ====== Conversación determinista/IA
function parseSede(text){ // “salón”
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

Profesionales (IDs y aliases, staff global):
${staffLines}

Servicios TORREMOLINOS:
${torremolinos_services.map(s => `- ${s.label} (Clave: ${s.key})`).join("\n")}

Servicios LA LUZ:
${laluz_services.map(s => `- ${s.label} (Clave: ${s.key})`).join("\n")}

REGLAS:
- Si el cliente escribe números para elegir, NO intervengas. Tú solo interpretas lenguaje natural.
- Mapea nombres de profesionales (alias) a IDs listados.
- Antes de listar servicios, identifica *categoría* y *salón*.
- Si pide “horario”, “esta/está semana” o “próxima semana”, devuelve {action:"weekly_schedule", next_week:boolean, staff_name?:string}.
- Si dice “otro día / viernes tarde…”, devuelve {action:"propose_times", date_hint, part_of_day}.
- Para reservar hace falta: salón + servicio + fecha/hora. La identidad se resuelve por teléfono.
- Si pregunta por su cita (hora/fecha/detalles), devuelve {action:"appointment_info"}.
- Acciones: set_salon (alias set_sede), set_category, set_staff, choose_service_label, propose_times, weekly_schedule, create_booking, list_appointments, cancel_appointment, edit_appointment, appointment_info, none.

FORMATO:
{"message":"...","action":"set_salon|set_sede|set_category|set_staff|choose_service_label|propose_times|weekly_schedule|create_booking|list_appointments|cancel_appointment|edit_appointment|appointment_info|none","params":{ ... } }`
}

async function aiInterpret(textRaw, session){
  if (AI_PROVIDER==="none") return null
  const sys = buildSystemPrompt(session)
  const ctx = `Estado actual:
- Salón: ${session.sede||"—"}
- Categoría: ${session.category||"—"}
- Servicio: ${session.selectedServiceLabel||"—"}
- Profesional: ${session.preferredStaffLabel||"—"}`
  const out = await aiChat(sys, `Mensaje cliente: "${textRaw}"\n${ctx}\nDevuelve SOLO JSON (sin explicaciones).`)
  const obj = stripToJSON(out)
  return obj
}

// ====== Intent helpers (detectar info sobre cita)
function isAppointmentInfoQuery(tNorm){
  // Palabras clave sobre "mi cita/reserva" + (cuando/hora/detalle/ver/confirmar/etc.)
  const aboutAppt = /\b(mi\s+(cita|reserva)|tengo\s+(cita|reserva)|la\s+cita|mi\s+booking|mi\s+turno|ver\s+(mi\s+)?(cita|reserva))\b/i.test(tNorm)
  const infoWords = /\b(cu[aá]ndo|cuando|hora|a que hora|a qué hora|detalle|detalles|ver|consultar|confirmar|comprobar|d[oó]nde|donde)\b/i.test(tNorm)
  return aboutAppt || (/\b(cita|reserva)\b/i.test(tNorm) && infoWords)
}

// ====== Proponer horas (top N con fallback a próxima semana)
async function proposeTimes(sessionData, phone, sock, jid, opts={}){
  const nowEU = dayjs().tz(EURO_TZ); 
  const baseFrom = nextOpeningFrom(nowEU.add(NOW_MIN_OFFSET_MIN, "minute"))
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
    await botReply(sock, jid, phone, { text:"Necesito primero *salón* y *servicio* para proponerte horas." }, { intent:"propose_times_missing", action:"proposeTimes" }, sessionData)
    return
  }

  const rawSlots = await searchAvailWindow({
    locationKey: sessionData.sede,
    envServiceKey: sessionData.selectedServiceEnvKey,
    startEU, endEU, limit: 200, part
  })

  let slots = rawSlots
  let usedPreferred = false
  if (sessionData.preferredStaffId){
    slots = rawSlots.filter(s => s.staffId === sessionData.preferredStaffId)
    usedPreferred = true
    if (!slots.length){ slots = rawSlots; usedPreferred = false }
  }

  // Fallback: próxima semana
  if (!slots.length){
    const startNext = startEU.clone().add(7, "day")
    const endNext   = endEU.clone().add(7, "day")
    const rawNext = await searchAvailWindow({
      locationKey: sessionData.sede,
      envServiceKey: sessionData.selectedServiceEnvKey,
      startEU: startNext, endEU: endNext, limit: 200, part
    })
    let nextSlots = rawNext
    let nextUsedPreferred = false
    if (sessionData.preferredStaffId){
      nextSlots = rawNext.filter(s => s.staffId === sessionData.preferredStaffId)
      nextUsedPreferred = true
      if (!nextSlots.length){ nextSlots = rawNext; nextUsedPreferred = false }
    }
    if (nextSlots.length){
      const shown = nextSlots.slice(0, SHOW_TOP_N)
      const mapN={}; for (const s of shown) mapN[s.date.format("YYYY-MM-DDTHH:mm")] = s.staffId || null
      const { lines } = proposeLines(shown, mapN)
      sessionData.lastHours = shown.map(s => s.date)
      sessionData.lastStaffByIso = mapN
      sessionData.lastProposeUsedPreferred = nextUsedPreferred
      sessionData.stage = "awaiting_time"
      saveSession(phone, sessionData)
      await botReply(sock, jid, phone, { text:`No había huecos en los próximos ${days} días. *La próxima semana* sí hay (primeras ${SHOW_TOP_N}):\n${lines}\n\nResponde con el número.` }, { intent:"propose_times", action:"fallback_next_week" }, sessionData)
      return
    }
  }

  if (!slots.length){
    const msg = when
      ? `No veo huecos para ese día${part?` por la ${part}`:""}. ¿Otra fecha o franja?`
      : `No encuentro huecos en los próximos ${days} días. ¿Otra fecha/franja (ej. “viernes por la tarde”)?`
    await botReply(sock, jid, phone, { text: msg }, { intent:"propose_times_no_slots", action:"proposeTimes" }, sessionData)
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
    ? `Horarios disponibles con ${sessionData.preferredStaffLabel || "tu profesional"} (primeras ${SHOW_TOP_N}):`
    : `Horarios disponibles (equipo) — primeras ${SHOW_TOP_N}:${sessionData.preferredStaffLabel ? `\nNota: no veo huecos con ${sessionData.preferredStaffLabel} en este rango; te muestro alternativas.`:""}`
  await botReply(sock, jid, phone, { text:`${header}\n${lines}\n\nResponde con el número.` }, { intent:"propose_times", action:"list_slots" }, sessionData)
}

// ====== HORARIO SEMANAL (7 días o próxima semana) — top N
function nextMondayEU(base){ return base.clone().add(1,"week").isoWeekday(1).hour(OPEN.start).minute(0).second(0).millisecond(0) }
async function weeklySchedule(sessionData, phone, sock, jid, opts={}){
  if (!sessionData.sede){
    await botReply(sock, jid, phone, { text:"¿En qué *salón* te viene mejor? *Torremolinos* o *La Luz*." }, { intent:"weekly_missing_sede", action:"ask_sede" }, sessionData)
    return
  }
  if (!sessionData.selectedServiceEnvKey){
    await botReply(sock, jid, phone, { text:"Dime el *servicio* (o la *categoría* para listarte opciones) y te muestro el horario semanal." }, { intent:"weekly_missing_service", action:"ask_service" }, sessionData)
    return
  }
  const nowEU = dayjs().tz(EURO_TZ)
  let startEU = nextOpeningFrom(nowEU.add(NOW_MIN_OFFSET_MIN,"minute"))
  if (opts.nextWeek){ startEU = nextMondayEU(nowEU) }
  const endEU = startEU.clone().add(7,"day").hour(OPEN.end).minute(0)

  const rawSlots = await searchAvailWindow({
    locationKey: sessionData.sede,
    envServiceKey: sessionData.selectedServiceEnvKey,
    startEU, endEU, limit: 500
  })

  if (!rawSlots.length){
    await botReply(sock, jid, phone, { text:`No encuentro huecos en ese rango. ¿Quieres que mire otra semana o cambiar de franja?` }, { intent:"weekly_no_slots", action:"weeklySchedule" }, sessionData)
    return
  }

  let staffIdFilter = null
  if (opts.staffName){
    const fz = fuzzyStaffFromText("con " + opts.staffName)
    if (fz && !fz.anyTeam) staffIdFilter = fz.id
  } else if (opts.usePreferred && sessionData.preferredStaffId){
    staffIdFilter = sessionData.preferredStaffId
  }

  let slots = rawSlots
  if (staffIdFilter){
    slots = rawSlots.filter(s => s.staffId === staffIdFilter)
    if (!slots.length){
      await botReply(sock, jid, phone, { text:`No veo huecos con ${staffLabelFromId(staffIdFilter)} en ese rango. Te muestro el *horario del equipo*:` }, { intent:"weekly_staff_no_slots", action:"weeklySchedule_staffFallback" }, sessionData)
      slots = rawSlots
      staffIdFilter = null
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
  await botReply(sock, jid, phone, { text:`${header}${lines.join("\n")}\n\nResponde con el *número* para reservar ese hueco.` }, { intent:"weeklySchedule", action:"list_week" }, sessionData)
}

// ====== Crear reserva
async function executeCreateBooking(sessionData, phone, sock, jid){
  if (!sessionData.sede) { await botReply(sock, jid, phone, {text:"Falta el *salón* (Torremolinos o La Luz)"}, { intent:"create_missing", action:"missing_sede" }, sessionData); return }
  if (!sessionData.selectedServiceEnvKey) { await botReply(sock, jid, phone, {text:"Falta el *servicio*"}, { intent:"create_missing", action:"missing_service" }, sessionData); return }
  if (!sessionData.pendingDateTime) { await botReply(sock, jid, phone, {text:"Falta la *fecha y hora*"}, { intent:"create_missing", action:"missing_datetime" }, sessionData); return }

  const startEU = parseToEU(sessionData.pendingDateTime)
  if (!insideBusinessHours(startEU, 60)) { await botReply(sock, jid, phone, {text:"Esa hora está fuera del horario (L–V 09:00–20:00)"}, { intent:"create_invalid_time", action:"validate_bh" }, sessionData); return }

  const iso = startEU.format("YYYY-MM-DDTHH:mm")
  let staffId = sessionData.lastProposeUsedPreferred ? (sessionData.preferredStaffId || sessionData.lastStaffByIso?.[iso] || null)
                                                    : (sessionData.lastStaffByIso?.[iso] || sessionData.preferredStaffId || null)

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
  if (!staffId) { await botReply(sock, jid, phone, {text:"No hay profesionales disponibles ahora mismo"}, { intent:"create_no_staff", action:"pick_staff" }, sessionData); return }

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
    await botReply(sock, jid, phone, {text:"Para terminar, dime tu *nombre* y (opcional) tu *email* para crear tu ficha 😊"}, { intent:"create_need_identity", action:"ask_identity" }, sessionData)
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
      customer_square_id: customerId, location_key: sessionData.sede, service_env_key: sessionData.selectedServiceEnvKey,
      service_label: sessionData.selectedServiceLabel || serviceLabelFromEnvKey(sessionData.selectedServiceEnvKey) || "Servicio", duration_min: 60,
      start_iso: startEU.tz("UTC").toISOString(), end_iso: startEU.clone().add(60, "minute").tz("UTC").toISOString(),
      staff_id: staffId, status: "failed", created_at: new Date().toISOString(),
      square_booking_id: null, square_error: result.error, retry_count: SQUARE_MAX_RETRIES
    })
    await botReply(sock, jid, phone, {text:"No pude crear la reserva ahora. ¿Quieres que te proponga otro horario?"}, { intent:"create_failed", action:"create_booking" }, sessionData)
    return
  }

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
👩‍💼 ${staffName}
📅 ${fmtES(startEU)}

Ref: ${result.booking.id}

¡Te esperamos!`
  await botReply(sock, jid, phone, { text: confirmMessage }, { intent:"create_success", action:"send_confirmation" }, sessionData)
  clearSession(phone);
}

// ====== Listar/cancelar/editar/info (modificado: todas redirigen a email/SMS)
async function enumerateCitasByPhone(phone){
  // Se mantiene por compatibilidad, ya no se usa para responder al cliente.
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
          salon: locationNice(idToLocKey(b.locationId)||""),
          profesional: staffLabelFromId(seg?.teamMemberId) || "Profesional",
        })
      }
      items.sort((a,b)=> (a.fecha_iso.localeCompare(b.fecha_iso)) || (a.pretty.localeCompare(b.pretty)))
    }catch(e){}
  }
  return items
}

const POLICY_MSG_CANCEL = "Para *cancelar* tu cita debes hacerlo desde el *enlace del email/SMS* de confirmación que recibiste al reservar. Ábrelo y gestiona la cancelación allí. 🙏"
const POLICY_MSG_EDIT   = "Para *editar o reprogramar* tu cita, usa el *enlace del email/SMS* de confirmación. Desde ese enlace puedes cambiar fecha u hora directamente. 📩📲"
const POLICY_MSG_INFO   = "Para *consultar detalles (fecha/hora)* de tu cita, entra en el *enlace del email/SMS* de confirmación que recibiste al reservar. Ahí verás toda la información actualizada. 🔎"

async function executeListAppointments(sessionData, phone, sock, jid){
  // Ahora NO listamos nada: redirigimos al email/SMS
  sessionData.stage = null
  saveSession(phone, sessionData)
  await botReply(sock, jid, phone, { text: POLICY_MSG_INFO }, { intent:"appointment_info", action:"redirect_email_sms" }, sessionData)
}
async function executeCancelAppointment(sessionData, phone, sock, jid){
  sessionData.cancelList = null
  sessionData.stage = null
  saveSession(phone, sessionData)
  await botReply(sock, jid, phone, { text: POLICY_MSG_CANCEL }, { intent:"cancel_appointment", action:"redirect_email_sms" }, sessionData)
}
async function executeEditAppointment(sessionData, phone, sock, jid){
  sessionData.stage = null
  saveSession(phone, sessionData)
  await botReply(sock, jid, phone, { text: POLICY_MSG_EDIT }, { intent:"edit_appointment", action:"redirect_email_sms" }, sessionData)
}
async function executeAppointmentInfo(sessionData, phone, sock, jid){
  sessionData.stage = null
  saveSession(phone, sessionData)
  await botReply(sock, jid, phone, { text: POLICY_MSG_INFO }, { intent:"appointment_info", action:"redirect_email_sms" }, sessionData)
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
  <h1>🩷 Gapink Nails Bot v31.5.0 — Top ${SHOW_TOP_N}</h1>
  <div class="status ${conectado ? 'success' : 'error'}">WhatsApp: ${conectado ? "✅ Conectado" : "❌ Desconectado"}</div>
  ${!conectado&&lastQR?`<div style="text-align:center;margin:20px 0"><img src="/qr.png" width="300" style="border-radius:8px"></div>`:""}
  <div class="status warning">Modo: ${DRY_RUN ? "🧪 Simulación" : "🚀 Producción"} | IA: ${AI_PROVIDER.toUpperCase()}</div>
  <p>Staff global (sin ubicación en ENV). La disponibilidad ya se filtra por salón usando Square.</p>
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

// ====== Wrapper de respuesta con logging
async function botReply(sock, jid, phone, payload, meta={}, session=null){
  const stage = session?.stage || null
  const text = payload?.text || ""
  logBot({ phone, direction:'out', intent: meta.intent||null, action: meta.action||null, stage, raw_text:null, reply_text:text, extra:{ payload } })
  try{ await sock.sendMessage(jid, payload) }catch(e){ if (BOT_DEBUG) console.error(e) }
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
            greeted:false, sede:null, category:null,
            selectedServiceEnvKey:null, selectedServiceLabel:null,
            preferredStaffId:null, preferredStaffLabel:null,
            pendingDateTime:null, lastHours:null, lastStaffByIso:{},
            lastProposeUsedPreferred:false, stage:null,
            identityChoices:null, identityResolvedCustomerId:null,
            cancelList:null,
            snooze_until_ms:null, name:null, email:null,
            // saludo 24h
            lastGreetAt_ms:null
          }

          const nowEU = dayjs().tz(EURO_TZ)
          logBot({ phone, direction:'in', intent:null, action:null, stage:session.stage||null, raw_text:textRaw, reply_text:null, extra:{ isFromMe } })

          // Pausa 6h con "."
          if (textRaw.trim()==="."){
            session.snooze_until_ms = nowEU.add(6,"hour").valueOf()
            saveSession(phone, session)
            logBot({ phone, direction:'sys', intent:"snooze_command", action:"pause_6h", stage:session.stage, raw_text:textRaw, reply_text:null, extra:{ until: session.snooze_until_ms } })
            return
          }

          // Ignorar otros mensajes propios
          if (isFromMe) { saveSession(phone, session); return }

          // En pausa
          if (session.snooze_until_ms && nowEU.valueOf() < session.snooze_until_ms) { 
            logBot({ phone, direction:'sys', intent:"snoozed", action:"ignore_in_window", stage:session.stage, raw_text:textRaw })
            saveSession(phone, session); 
            return 
          }

          // Saludo si toca (24h) y luego seguimos normal
          if (!session.lastGreetAt_ms || (nowEU.valueOf() - session.lastGreetAt_ms) >= 24*60*60*1000){
            await botReply(sock, jid, phone, {text:buildGreeting()}, { intent:"greeting_24h", action:"send_greeting" }, session)
            session.lastGreetAt_ms = nowEU.valueOf()
            saveSession(phone, session)
          }

          const t = norm(textRaw)
          const numMatch = t.match(/^\s*([1-9]\d*)\b/)
          const sedeMention = parseSede(textRaw)
          const catMention = parseCategory(textRaw)
          const temporal = parseTemporalPreference(textRaw)

          // Detección explícita de info/cancel/editar cita (antes de todo)
          if (isAppointmentInfoQuery(t)){
            logBot({ phone, direction:'sys', intent:"appointment_info_detected", action:"redirect_email_sms", stage:session.stage, raw_text:textRaw })
            await executeAppointmentInfo(session, phone, sock, jid)
            return
          }
          if (/\b(editar|modificar|cambiar|reprogramar|mover)\b/.test(t) && /\b(cita|reserva|hora|turno)\b/.test(t)){
            logBot({ phone, direction:'sys', intent:"edit_detected", action:"redirect_email_sms", stage:session.stage, raw_text:textRaw })
            await executeEditAppointment(session, phone, sock, jid)
            return
          }
          if (/\b(cancelar|anular)\b/.test(t) && /\b(cita|reserva|hora|turno)\b/.test(t)){
            logBot({ phone, direction:'sys', intent:"cancel_detected", action:"redirect_email_sms", stage:session.stage, raw_text:textRaw })
            await executeCancelAppointment(session, phone, sock, jid)
            return
          }

          // “con el equipo / me da igual / cualquiera”
          if (/\b(con el equipo|me da igual|cualquiera|con quien sea|lo que haya)\b/i.test(t)){
            session.preferredStaffId = null
            session.preferredStaffLabel = null
            saveSession(phone, session)
            logBot({ phone, direction:'sys', intent:"team_any", action:"clear_preferred_staff", stage:session.stage, raw_text:textRaw })
            if (session.sede && session.selectedServiceEnvKey){
              await proposeTimes(session, phone, sock, jid, { text:textRaw })
            } else {
              const faltan=[]
              if (!session.sede) faltan.push("salón")
              if (!session.category) faltan.push("categoría")
              if (!session.selectedServiceEnvKey) faltan.push("servicio")
              await botReply(sock, jid, phone, {text:`Perfecto, te propongo huecos del equipo en cuanto me digas ${faltan.join(", ")}.`}, { intent:"team_any_prompt_missing", action:"ask_missing" }, session)
            }
            return
          }

          // ====== BLOQUE DETERMINISTA POR NÚMERO ======
          if (session.stage==="awaiting_identity_pick" && numMatch){
            const n = Number(numMatch[1])
            const choice = (session.identityChoices||[]).find(c=>c.index===n)
            if (!choice){ await botReply(sock, jid, phone, {text:"No encontré esa opción. Responde con el número de tu ficha."}, { intent:"identity_pick_bad", action:"ask_retry" }, session); return }
            session.identityResolvedCustomerId = choice.id
            session.stage = null
            saveSession(phone, session)
            await botReply(sock, jid, phone, {text:"¡Gracias! Finalizo tu reserva…"}, { intent:"identity_ok", action:"proceed_booking" }, session)
            await executeCreateBooking(session, phone, sock, jid)
            return
          }
          if (session.stage==="awaiting_identity"){
            const {name,email} = parseNameEmailFromText(textRaw)
            if (!name && !email){ await botReply(sock, jid, phone, {text:"Dime tu *nombre completo* y opcionalmente tu *email* 😊"}, { intent:"identity_need_both", action:"ask_identity" }, session); return }
            if (name) session.name = name
            if (email) session.email = email
            const created = await findOrCreateCustomerWithRetry({ name: session.name, email: session.email, phone })
            if (!created){ await botReply(sock, jid, phone, {text:"No pude crear tu ficha. ¿Puedes repetir nombre y (opcional) email?"}, { intent:"identity_create_failed", action:"ask_retry" }, session); return }
            session.identityResolvedCustomerId = created.id
            session.stage = null
            saveSession(phone, session)
            await botReply(sock, jid, phone, {text:"¡Gracias! Finalizo tu reserva…"}, { intent:"identity_created", action:"proceed_booking" }, session)
            await executeCreateBooking(session, phone, sock, jid)
            return
          }
          if (session.stage==="awaiting_cancel" && numMatch && Array.isArray(session.cancelList)){
            logBot({ phone, direction:'sys', intent:"cancel_number_ignored", action:"redirect_email_sms", stage:session.stage, raw_text:textRaw })
            await executeCancelAppointment(session, phone, sock, jid)
            return
          }
          if (session.stage==="awaiting_service_choice" && numMatch && Array.isArray(session.serviceChoices) && session.serviceChoices.length){
            const n = Number(numMatch[1])
            const choice = session.serviceChoices.find(it=>it.index===n)
            if (!choice){ await botReply(sock, jid, phone, {text:"No encontré esa opción. Responde con el número de la lista."}, { intent:"service_pick_bad", action:"ask_retry" }, session); return }
            session.selectedServiceEnvKey = choice.key
            session.selectedServiceLabel = choice.label
            session.stage = null
            saveSession(phone, session)
            logBot({ phone, direction:'sys', intent:"service_selected", action:"store_service", stage:session.stage, raw_text:textRaw, extra:{ key:choice.key, label:choice.label } })
            if (session.preferredStaffId){
              await proposeTimes(session, phone, sock, jid, { text:"" })
            } else {
              await botReply(sock, jid, phone, {text:`Perfecto, ${choice.label} en ${locationNice(session.sede)}. ¿Lo quieres *con alguna profesional*? (por ejemplo “con Patri”). Si no, te paso huecos del equipo.`}, { intent:"service_selected_prompt_staff", action:"ask_staff" }, session)
            }
            return
          }
          if ((!session.stage || session.stage==="awaiting_time") && numMatch && Array.isArray(session.lastHours) && session.lastHours.length){
            const idx = Number(numMatch[1]) - 1
            const pick = session.lastHours[idx]
            if (!dayjs.isDayjs(pick)){ await botReply(sock, jid, phone, {text:"No encontré esa opción. Responde con el número válido."}, { intent:"time_pick_bad", action:"ask_retry" }, session); return }
            const iso = pick.format("YYYY-MM-DDTHH:mm")
            const staffFromIso = session?.lastStaffByIso?.[iso] || null
            session.pendingDateTime = pick.tz(EURO_TZ).toISOString()
            if (staffFromIso){ session.preferredStaffId = staffFromIso; session.preferredStaffLabel = staffLabelFromId(staffFromIso) }
            saveSession(phone, session)
            await botReply(sock, jid, phone, {text:"¡Perfecto! Creo la reserva…"}, { intent:"time_selected", action:"create_booking" }, session)
            await executeCreateBooking(session, phone, sock, jid)
            return
          }
          // ====== FIN BLOQUE POR NÚMERO ======

          if (sedeMention) { session.sede = sedeMention; saveSession(phone, session); logBot({ phone, direction:'sys', intent:"sede_detected", action:"store_sede", stage:session.stage, raw_text:textRaw, extra:{ sede:sedeMention } }) }
          if (catMention)  { session.category = catMention; saveSession(phone, session); logBot({ phone, direction:'sys', intent:"category_detected", action:"store_category", stage:session.stage, raw_text:textRaw, extra:{ category:catMention } }) }

          // Fuzzy staff (sin bloquear por salón)
          const fuzzy = fuzzyStaffFromText(textRaw)
          if (fuzzy){
            if (fuzzy.anyTeam){
              session.preferredStaffId = null
              session.preferredStaffLabel = null
              saveSession(phone, session)
              logBot({ phone, direction:'sys', intent:"staff_any", action:"clear_preferred", stage:session.stage })
              if (session.sede && session.selectedServiceEnvKey){
                await proposeTimes(session, phone, sock, jid, { text:textRaw })
              }else{
                const faltan=[]; if (!session.sede) faltan.push("salón"); if (!session.category) faltan.push("categoría"); if (!session.selectedServiceEnvKey) faltan.push("servicio")
                await botReply(sock, jid, phone, {text:`Perfecto, te propongo huecos del equipo en cuanto me digas ${faltan.join(", ")}.`}, { intent:"staff_any_prompt_missing", action:"ask_missing" }, session)
              }
              return
            }
            session.preferredStaffId = fuzzy.id
            session.preferredStaffLabel = staffLabelFromId(fuzzy.id)
            saveSession(phone, session)
            logBot({ phone, direction:'sys', intent:"staff_detected", action:"store_staff", stage:session.stage, extra:{ id:fuzzy.id, label:session.preferredStaffLabel } })
          } else {
            const unknownNameMatch = /(?:^|\s)con\s+([a-zñáéíóúüï\s]{2,})\??$/i.exec(textRaw)
            if (unknownNameMatch){
              const alternativas = Array.from(new Set(
                EMPLOYEES.filter(e=> e.bookable).map(e=>e.labels[0])
              )).slice(0,6)
              await botReply(sock, jid, phone, { text:`No tengo a *${unknownNameMatch[1].trim()}* en el equipo. Disponibles: ${alternativas.join(", ")}. ¿Con quién prefieres?` }, { intent:"staff_unknown", action:"suggest_staff" }, session)
              return
            }
          }

          // Disparadores “horario”
          if (/\b(horario|agenda|est[áa]\s+semana|esta\s+semana|pr[oó]xima\s+semana|semana\s+que\s+viene|7\s+d[ií]as|siete\s+d[ií]as)\b/i.test(t)){
            if (!session.selectedServiceEnvKey){
              if (!session.category){
                await botReply(sock, jid, phone, { text:"Antes del horario, dime *categoría* (Uñas / Depilación / Micropigmentación / Faciales / Pestañas)." }, { intent:"weekly_missing_category", action:"ask_category" }, session)
                return
              }
              if (!session.sede){
                await botReply(sock, jid, phone, { text:"¿En qué *salón* te viene mejor? *Torremolinos* o *La Luz*." }, { intent:"weekly_missing_sede", action:"ask_sede" }, session)
                return
              }
              const itemsRaw = listServicesByCategory(session.sede, session.category, textRaw)
              if (!itemsRaw.length){ await botReply(sock, jid, phone, {text:`No tengo servicios de *${session.category}* en ${locationNice(session.sede)}.`}, { intent:"weekly_no_services", action:"none" }, session); return }
              const list = itemsRaw.slice(0,22).map((s,i)=>({ index:i+1, key:s.key, label:s.label }))
              session.serviceChoices = list
              session.stage = "awaiting_service_choice"
              saveSession(phone, session)
              const lines = list.map(it=> `${it.index}) ${it.label}`).join("\n")
              await botReply(sock, jid, phone, {text:`Elige el *servicio* para mostrarte el horario semanal en ${locationNice(session.sede)}:\n\n${lines}\n\nResponde con el número.`}, { intent:"weekly_service_choice", action:"ask_service_number" }, session)
              return
            }
            await weeklySchedule(session, phone, sock, jid, {
              nextWeek: temporal.nextWeek,
              staffName: null,
              usePreferred: true
            })
            return
          }

          // Si ya hay salón+servicio y el texto menciona día/franja → proponer directamente
          if (session.sede && session.selectedServiceEnvKey && /\botro dia\b|\botro día\b|\bhoy\b|\bmanana\b|\bpasado\b|\blunes\b|\bmartes\b|\bmiercoles\b|\bjueves\b|\bviernes\b|\btarde\b|\bpor la manana\b|\bnoche\b/i.test(t)){
            await proposeTimes(session, phone, sock, jid, { text:textRaw })
            return
          }

          // IA para el resto
          const aiObj = await aiInterpret(textRaw, session)
          logBot({ phone, direction:'sys', intent:"ai_interpretation", action: aiObj?.action || "none", stage:session.stage, raw_text:textRaw, extra:{ aiObj } })

          if (aiObj && typeof aiObj==="object"){
            const action = aiObj.action
            const p = aiObj.params || {}

            if ((action==="set_salon" || action==="set_sede") && p.sede){
              const lk = parseSede(String(p.sede))
              if (lk){ session.sede = lk; saveSession(phone, session) }
              if (!session.category){
                await botReply(sock, jid, phone, {text:"¿Qué *categoría* necesitas? *Uñas*, *Depilación*, *Micropigmentación*, *Faciales* o *Pestañas*."}, { intent:"ask_category", action:"set_sede" }, session)
                return
              }
            }

            if (action==="set_category" && p.category){
              const cm = parseCategory(String(p.category))
              if (cm){ session.category = cm; saveSession(phone, session) }
              if (!session.sede){
                await botReply(sock, jid, phone, {text:"¿En qué *salón* te viene mejor? *Torremolinos* o *La Luz*."}, { intent:"ask_sede", action:"set_category" }, session)
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
                  await botReply(sock, jid, phone, {text:`¿En qué *salón* prefieres con ${session.preferredStaffLabel}? Torremolinos o La Luz.`}, { intent:"ask_sede", action:"set_staff" }, session)
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
                await botReply(sock, jid, phone, {text:`Perfecto, ${p.label} en ${locationNice(session.sede)}.`}, { intent:"service_selected", action:"confirm_service" }, session)
                await proposeTimes(session, phone, sock, jid, { text:textRaw })
                return
              }
            }

            if (action==="weekly_schedule"){
              if (!session.selectedServiceEnvKey){
                await botReply(sock, jid, phone, { text:"Dime el *servicio* y te muestro el horario semanal." }, { intent:"ask_service", action:"weekly_schedule" }, session)
                return
              }
              await weeklySchedule(session, phone, sock, jid, {
                nextWeek: !!p.next_week,
                staffName: p.staff_name || null,
                usePreferred: !p.staff_name
              })
              return
            }

            if (action==="propose_times"){
              if (!session.sede){
                await botReply(sock, jid, phone, {text:"¿En qué *salón* te viene mejor? *Torremolinos* o *La Luz*."}, { intent:"ask_sede", action:"propose_times" }, session); return
              }
              if (!session.category){
                await botReply(sock, jid, phone, {text:"¿Qué *categoría* necesitas? *Uñas*, *Depilación*, *Micropigmentación*, *Faciales* o *Pestañas*."}, { intent:"ask_category", action:"propose_times" }, session); return
              }
              if (!session.selectedServiceEnvKey){
                const itemsRaw = listServicesByCategory(session.sede, session.category, textRaw)
                if (!itemsRaw.length){ await botReply(sock, jid, phone, {text:`No tengo servicios de *${session.category}* en ${locationNice(session.sede)}.`}, { intent:"no_services", action:"propose_times" }, session); return }
                const list = itemsRaw.slice(0,22).map((s,i)=>({ index:i+1, key:s.key, label:s.label }))
                session.serviceChoices = list
                session.stage = "awaiting_service_choice"
                saveSession(phone, session)
                const lines = list.map(it=> `${it.index}) ${it.label}`).join("\n")
                await botReply(sock, jid, phone, {text:`Opciones de *${session.category}* en ${locationNice(session.sede)}:\n\n${lines}\n\nResponde con el número.`}, { intent:"ask_service_number", action:"propose_times" }, session)
                return
              }
              await proposeTimes(session, phone, sock, jid, { date_hint:p.date_hint, part_of_day:p.part_of_day, text:textRaw })
              return
            }

            if (action==="list_appointments"){ await executeListAppointments(session, phone, sock, jid); return }
            if (action==="cancel_appointment"){ await executeCancelAppointment(session, phone, sock, jid); return }
            if (action==="edit_appointment"){ await executeEditAppointment(session, phone, sock, jid); return }
            if (action==="appointment_info"){ await executeAppointmentInfo(session, phone, sock, jid); return }
          } // fin IA válida

          // Si faltan datos, guía
          if (!session.sede){
            session.stage="awaiting_sede"; saveSession(phone, session)
            await botReply(sock, jid, phone, {text:"¿En qué *salón* te viene mejor? *Torremolinos* o *La Luz*."}, { intent:"ask_sede", action:"guide" }, session)
            return
          }
          if (!session.category){
            session.stage="awaiting_category"; saveSession(phone, session)
            await botReply(sock, jid, phone, {text:"¿Qué *categoría* necesitas? *Uñas*, *Depilación*, *Micropigmentación*, *Faciales* o *Pestañas*."}, { intent:"ask_category", action:"guide" }, session)
            return
          }
          if (!session.selectedServiceEnvKey){
            const itemsRaw = listServicesByCategory(session.sede, session.category, textRaw)
            if (!itemsRaw.length){ await botReply(sock, jid, phone, {text:`No tengo servicios de *${session.category}* en ${locationNice(session.sede)}.`}, { intent:"no_services", action:"guide" }, session); return }
            const list = itemsRaw.slice(0,22).map((s,i)=>({ index:i+1, key:s.key, label:s.label }))
            session.serviceChoices = list
            session.stage = "awaiting_service_choice"
            saveSession(phone, session)
            const lines = list.map(it=> `${it.index}) ${it.label}`).join("\n")
            await botReply(sock, jid, phone, {text:`Opciones de *${session.category}* en ${locationNice(session.sede)}:\n\n${lines}\n\nResponde con el número.`}, { intent:"ask_service_number", action:"guide" }, session)
            return
          }
          if (/\botro dia\b|\botro día\b|\bhoy\b|\bmanana\b|\bpasado\b|\blunes\b|\bmartes\b|\bmiercoles\b|\bjueves\b|\bviernes\b|\btarde\b|\bpor la manana\b|\bnoche\b/i.test(t)){
            await proposeTimes(session, phone, sock, jid, { text:textRaw })
            return
          }
          await botReply(sock, jid, phone, {text:buildGreeting()}, { intent:"fallback_greeting", action:"greet" }, session)
        }catch(err){
          if (BOT_DEBUG) console.error(err)
          logBot({ phone, direction:'sys', intent:"error", action:"catch", stage:null, raw_text:null, extra:{ message: err?.message, stack: err?.stack } })
          await botReply(sock, jid, phone, {text:"Ups, error técnico. ¿Puedes repetirlo, porfa?"}, { intent:"error_reply", action:"send_error" }, null)
        }
      })
      QUEUE.set(phone, job.finally(()=>{ if (QUEUE.get(phone)===job) QUEUE.delete(phone) }))
    })
  }catch(e){ setTimeout(() => startBot().catch(console.error), 5000) }
}

// ====== Arranque
console.log(`🩷 Gapink Nails Bot v31.5.0 — Top ${SHOW_TOP_N} (L–V)`)
const appListen = app.listen(PORT, ()=>{ startBot().catch(console.error) })
process.on("uncaughtException", (e)=>{ console.error("💥 uncaughtException:", e?.stack||e?.message||e) })
process.on("unhandledRejection", (e)=>{ console.error("💥 unhandledRejection:", e) })
process.on("SIGTERM", ()=>{ try{ appListen.close(()=>process.exit(0)) }catch{ process.exit(0) } })
process.on("SIGINT", ()=>{ try{ appListen.close(()=>process.exit(0)) }catch{ process.exit(0) } })
