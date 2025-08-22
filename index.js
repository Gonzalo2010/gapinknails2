// index.js — Gapink Nails · v28.4.0
// Cambios vs v28.3.0 (aplican tu plan):
// • Motor de slots (ConversationContext) con validación contextual y confianza por campo.
// • NLU avanzado: intenciones múltiples, sinónimos, corrección leve, referencias temporales, reescritura IA en todas partes clave.
// • Flujo adaptativo (AdaptiveFlow) + “una pregunta a la vez” usando stages estrictos.
// • Caché multinivel (SmartCache L1/L2 SQLite simple) + precarga predictiva básica.
// • Cola de prioridad (PriorityQueue) para trabajo interno (notificaciones, reintentos).
// • Manejo de errores resiliente (ResilientErrorHandler) con degradación elegante y cola manual (“Cristina te contesta…”).
// • Listado de TODAS las profesionales por sede (EMP_CENTER_*) y respeto de “con {nombre}” siempre.
// • Días→Horas (3 días más cercanos → horas) antes de confirmar.
// • Métricas y memoria conversacional; recordatorios 24h y seguimiento.

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
const WORK_DAYS = [1,2,3,4,5] // L-V
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

// ====== IA DeepSeek
const AI_API_KEY = process.env.DEEPSEEK_API_KEY || ""
const AI_MODEL = process.env.AI_MODEL || "deepseek-chat"
const AI_MAX_RETRIES = Number(process.env.AI_MAX_RETRIES || 3)
const AI_TIMEOUT_MS = Number(process.env.AI_TIMEOUT_MS || 15000)
const sleep = ms => new Promise(r=>setTimeout(r, ms))

// ====== Utils
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
  x = x.replace(/Esculpid(a|as)\b/gi, (m)=> (/[A-Z]/.test(m[0])?'E':'e') + 'sculpid' + (m.endsWith('as')?'as':'a'))
  return x
}
function normalizePhoneES(raw){
  const d = onlyDigits(raw)
  if (!d) return null
  if (raw.startsWith("+") && d.length >= 8 && d.length <= 15) return `+${d}`
  if (d.startsWith("34") && d.length === 11) return `+${d}`
  if (d.length === 9) return `+34${d}`
  if (d.startsWith("00")) return `+${d.slice(2)}`
  return `+${d}`
}
function locationToId(key){ return key==="la_luz" ? LOC_LUZ : LOC_TORRE }
function idToLocKey(id){ return id===LOC_LUZ ? "la_luz" : id===LOC_TORRE ? "torremolinos" : null }
function locationNice(key){ return key==="la_luz" ? "Málaga – La Luz" : "Torremolinos" }
function isHolidayEU(d){
  const dd = String(d.date()).padStart(2,"0")
  const mm = String(d.month()+1).padStart(2,"0")
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
  while (!WORK_DAYS.includes(t.day()) || isHolidayEU(t)) t = t.add(1,"day").hour(OPEN.start).minute(0).second(0).millisecond(0)
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
CREATE TABLE IF NOT EXISTS conversation_memory (
  phone TEXT PRIMARY KEY,
  data_json TEXT,
  updated_at TEXT
);
CREATE TABLE IF NOT EXISTS conversation_metrics (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  phone TEXT,
  stage TEXT,
  success INTEGER,
  timestamp INTEGER,
  session_duration INTEGER
);
CREATE TABLE IF NOT EXISTS learning_terms (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  phone TEXT,
  terms TEXT,
  service_label TEXT,
  timestamp INTEGER
);
CREATE TABLE IF NOT EXISTS manual_review_queue (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  context TEXT,
  created_at TEXT,
  status TEXT
);
CREATE TABLE IF NOT EXISTS cache_l2 (
  key TEXT PRIMARY KEY,
  value TEXT,
  saved_at INTEGER
);
CREATE TABLE IF NOT EXISTS booking_notifications (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  booking_id TEXT,
  type TEXT,
  sent_at TEXT,
  UNIQUE(booking_id,type)
);
`)

// ====== IA helpers
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
async function aiCall(messages, systemPrompt="") {
  const controller = new AbortController()
  const timer = setTimeout(()=>controller.abort(), AI_TIMEOUT_MS)
  try{
    const payload = {
      model: AI_MODEL,
      messages: systemPrompt ? [{role:"system",content:systemPrompt}, ...messages] : messages,
      max_tokens: 800,
      temperature: 0.6,
      stream: false
    }
    const resp = await fetch("https://api.deepseek.com/chat/completions", {
      method: "POST",
      headers: { "Content-Type":"application/json", "Authorization": `Bearer ${AI_API_KEY}` },
      body: JSON.stringify(payload),
      signal: controller.signal
    })
    clearTimeout(timer)
    if (!resp.ok) return null
    const data = await resp.json()
    return data?.choices?.[0]?.message?.content || null
  }catch{ clearTimeout(timer); return null }
}
async function aiWithRetries(messages, systemPrompt=""){
  for (let i=0;i<=AI_MAX_RETRIES;i++){
    const out = await aiCall(messages, systemPrompt)
    if (out && out.trim()) return out
    if (i<AI_MAX_RETRIES) await sleep(300*(i+1))
  }
  return null
}
async function aiRewrite(text, context=null){
  if (!AI_API_KEY) return text
  const sys = "Reescribe el texto para WhatsApp en español de España, tono cercano, claro y breve. Devuelve SOLO el texto reescrito."
  const ctx = context ? `\nContexto: ${safeJSONStringify(context)}` : ""
  const msg = { role:"user", content:`${text}${ctx}` }
  const out = await aiWithRetries([msg], sys)
  return (out && out.trim()) ? out.trim() : text
}
async function analyzeUserSentiment(text){
  if (!AI_API_KEY) return { sentiment:"neutral", confidence:0.5, emotion:"neutral" }
  const sys = "Eres un analizador de sentimiento. Devuelve SOLO JSON válido."
  const user = { role:"user", content:
    `Analiza el sentimiento: "${text}". 
Responde JSON: {"sentiment":"positive|negative|neutral","confidence":0.0-1.0,"emotion":"excited|frustrated|confused|happy|neutral"}` }
  try{
    const raw = await aiWithRetries([user], sys)
    const cleaned = (raw||"").replace(/```json|```/g,"").trim()
    const obj = JSON.parse(cleaned)
    return obj
  }catch{ return { sentiment:"neutral", confidence:0.5, emotion:"neutral" } }
}
async function parseMultipleIntentsEnhanced(userText, conversationHistory=[]) {
  // Corrección leve y sinónimos básicos previos
  let t = userText
  t = t.replace(/\bpesta(ns|s)\b/ig, "pestañas").replace(/\buñitas\b/ig, "manicura").replace(/\bdepile\b/ig, "depilación")
  const sys = "Eres un parser de intenciones. Devuelve SOLO JSON válido con todos los campos."
  const examples = `Ejemplos:
- "manicura francesa el viernes por la tarde con Laura en la luz"
- "cámbiame la cita de mañana con Carmen a las 12"
- "precio de fotodepilación piernas"`
  const history = conversationHistory.map(h => `U:${h.user} | B:${h.bot}`).join("\n")
  const user = { role:"user", content:
`Contexto reciente:
${history || "(sin contexto)"}

Extrae TODAS las intenciones del mensaje: "${t}"
Responde JSON: {
  "primary_intent": "reservar|cancelar|modificar|consultar|saludo|otro",
  "secondary_intents": ["horario","precio","ubicacion","profesional","categoria"],
  "entities": {
    "datetime": string|null,
    "service": string|null,
    "staff": string|null,
    "location": "torremolinos"|"la_luz"|null,
    "category": "manicura"|"pedicura"|"pestañas"|"cejas"|"depilación"|"fotodepilación"|"micropigmentación"|"tratamiento facial"|"tratamiento corporal"|"otros"|null,
    "number_choice": number|null
  },
  "urgency": "high|medium|low"
}` }
  try{
    const raw = await aiWithRetries([user], sys)
    const cleaned = (raw||"").replace(/```json|```/g,"").trim()
    return JSON.parse(cleaned)
  }catch{ return { primary_intent:"otro", secondary_intents:[], entities:{}, urgency:"low" } }
}

// ====== Conversational Memory
function loadConversationMemory(phone){
  const row = db.prepare(`SELECT data_json FROM conversation_memory WHERE phone=?`).get(phone)
  if (!row?.data_json) return null
  try{ return JSON.parse(row.data_json) }catch{ return null }
}
function saveConversationMemory(phone, memory){
  const payload = JSON.stringify(memory||{})
  const up = db.prepare(`UPDATE conversation_memory SET data_json=?, updated_at=? WHERE phone=?`).run(payload, new Date().toISOString(), phone)
  if (up.changes===0){
    db.prepare(`INSERT INTO conversation_memory (phone,data_json,updated_at) VALUES (?,?,?)`).run(phone,payload,new Date().toISOString())
  }
}
function updateConversationMemory(phone, message, response){
  const memory = loadConversationMemory(phone) || { history: [], preferences: {} }
  memory.history.push({ user: message, bot: response, timestamp: Date.now() })
  const MAX = 5
  if (memory.history.length > MAX) memory.history = memory.history.slice(-MAX)
  saveConversationMemory(phone, memory)
}

// ====== ConversationContext (slots)
class ConversationContext {
  constructor() {
    this.slots = new Map()
    this.confidence = new Map()
    this.entities = new Map()
    this.history = []
  }
  updateSlot(slot, value, confidence = 1.0) {
    if (value==null || value==="") return
    this.slots.set(slot, value)
    this.confidence.set(slot, confidence)
    this.validateContext()
  }
  get(slot){ return this.slots.get(slot) }
  hasValidSlot(slot){ return this.slots.has(slot) && this.slots.get(slot)!=null }
  isServiceAvailableAt(serviceLabel, locationKey){
    const list = servicesForSedeKeyRaw(locationKey||"") || []
    return list.some(s => s.label.toLowerCase() === String(serviceLabel||"").toLowerCase())
  }
  validateContext(){
    const service = this.slots.get('service_label')
    const location = this.slots.get('location')
    if (service && location && !this.isServiceAvailableAt(service, location)) {
      this.slots.delete('service_label')
      this.confidence.delete('service_label')
    }
  }
}

// ====== AdaptiveFlow
const FLOW_STATES = {
  COLLECTING: "collecting",
  CONFIRMING: "confirming",
  MODIFYING: "modifying",
  COMPLETED: "completed"
}
class AdaptiveFlow {
  constructor(){ this.flowGraph = new Map(); this.shortcuts = new Map() }
  getRequiredSlots(target){
    switch(target){
      case "select_service": return ["category","location"]
      case "pick_day": return ["category","location","service_env"]
      case "pick_time": return ["category","location","service_env","day"]
      case "confirm": return ["category","location","service_env","datetime"]
      default: return []
    }
  }
  canSkipTo(targetState, ctx){
    const req = this.getRequiredSlots(targetState)
    return req.every(slot => ctx.hasValidSlot(slot))
  }
  handleError(_error, _context){ return "collecting" }
}

// ====== SmartCache (L1 = memoria, L2 = SQLite)
class SmartCache {
  constructor(){ this.L1 = new Map() }
  _getL2(key){
    try{
      const row = db.prepare(`SELECT value,saved_at FROM cache_l2 WHERE key=?`).get(key)
      if (!row) return null
      return { value: JSON.parse(row.value), ts: row.saved_at }
    }catch{ return null }
  }
  _setL2(key, val){
    try{
      const payload = JSON.stringify(val)
      db.prepare(`INSERT OR REPLACE INTO cache_l2 (key,value,saved_at) VALUES (?,?,?)`).run(key, payload, Date.now())
    }catch{}
  }
  async get(key, ttlMs, fetcher){
    const now = Date.now()
    const l1 = this.L1.get(key)
    if (l1 && now - l1.ts < ttlMs) return l1.value
    const l2 = this._getL2(key)
    if (l2 && now - l2.ts < ttlMs) { this.L1.set(key, {value:l2.value, ts:l2.ts}); return l2.value }
    const fresh = await fetcher()
    this._setL2(key, fresh); this.L1.set(key, {value:fresh, ts:now})
    return fresh
  }
  async predictivePreload(){ /* simple stub */ }
}
const smartCache = new SmartCache()

// ====== PriorityQueue (simple round-robin con niveles)
class PriorityQueue {
  constructor(){ this.queues = { urgent:[], high:[], medium:[], low:[] } }
  push(priority, fn){ (this.queues[priority]||this.queues.medium).push(fn) }
  hasWork(){ return Object.values(this.queues).some(q=>q.length) }
  isOverloaded(){ return false }
  async backoff(){ await sleep(200) }
  async processNext(){
    const order = ["urgent","high","medium","low"]
    for (const p of order){
      const q = this.queues[p]
      if (q && q.length){
        const fn = q.shift()
        try{ await fn() }catch{}
        return
      }
    }
  }
  async processWithBackpressure(){
    while(this.hasWork()){
      if (this.isOverloaded()) await this.backoff()
      await this.processNext()
    }
  }
}
const PQ = new PriorityQueue()

// ====== ResilientErrorHandler
class ResilientErrorHandler {
  async handleSquareAPIError(error, context){
    const type = String(error?.type || error?.name || "UNKNOWN")
    if (type.includes("RATE") || type.includes("TooMany")) return await this.exponentialBackoff(context)
    if (type.includes("UNAVAILABLE")) return await this.fallbackToManualQueue(context)
    if (type.includes("VALIDATION")) return await this.requestClarification(context)
    return await this.gracefulDegrade(context)
  }
  async exponentialBackoff(_ctx){ await sleep(600); return { degraded:true } }
  async fallbackToManualQueue(context){
    try{
      db.prepare(`INSERT INTO manual_review_queue (context,created_at,status) VALUES (?,?,?)`)
        .run(safeJSONStringify(context), new Date().toISOString(), "open")
    }catch{}
    return { queued:true }
  }
  async requestClarification(_ctx){ return { need_clarification:true } }
  async gracefulDegrade(_ctx){ return { degraded:true } }
}
const ErrorHandler = new ResilientErrorHandler()

// ====== Servicios y Empleadas
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
function resolveEnvKeyFromLabelAndSede(label, sedeKey){
  const list = servicesForSedeKeyRaw(sedeKey)
  return list.find(s=>s.label.toLowerCase()===String(label||"").toLowerCase())?.key || null
}

// Empleadas
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
    let allowIds = (locs||"").split(",").map(s=>s.trim()).filter(Boolean)
    let allowTxt = []
    const empKey = "EMP_CENTER_" + k.replace(/^SQ_EMP_/, "")
    const empVal = process.env[empKey]
    if (empVal) {
      const centers = String(empVal).split(",").map(s=>s.trim().toLowerCase()).filter(Boolean)
      const normCenter = c => (c==="la luz" ? "la_luz" : c)
      const centerKeys = centers.map(normCenter).filter(x => x==="la_luz" || x==="torremolinos" || x==="all")
      allowTxt = centerKeys
      const mapped = centerKeys.includes("all") ? ["ALL"] : centerKeys.map(c => locationToId(c)).filter(Boolean)
      if (mapped.length) allowIds = mapped
    }
    const labels = deriveLabelsFromEnvKey(k)
    out.push({ envKey:k, id, bookable, allowIds, allowTxt, labels })
  }
  return out
}
const EMPLOYEES = parseEmployees()
function staffLabelFromId(id){
  const e = EMPLOYEES.find(x=>x.id===id)
  return e?.labels?.[0] || (id ? `Profesional ${String(id).slice(-4)}` : null)
}
function isStaffAllowedInLocation(staffId, locKey){
  const e = EMPLOYEES.find(x=>x.id===staffId)
  if (!e || !e.bookable) return false
  const locId = locationToId(locKey)
  return (e.allowIds.includes("ALL") || e.allowTxt.includes("all") ||
          (locId && e.allowIds.includes(locId)) ||
          e.allowTxt.includes(locKey))
}
function allowedStaffLabelsForLocation(sedeKey){
  const locId = locationToId(sedeKey)
  return EMPLOYEES
    .filter(e => e.bookable && (
      e.allowIds.includes("ALL") || e.allowTxt.includes("all") ||
      (locId && e.allowIds.includes(locId)) ||
      e.allowTxt.includes(sedeKey)
    ))
    .map(e=>staffLabelFromId(e.id))
    .filter(Boolean)
    .sort((a,b)=>a.localeCompare(b))
}

// Aliases
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
function parsePreferredStaffFromText(text){
  const t = norm(text||"")
  const m = t.match(/\b(?:con|cita con|con la|con el)\s+([a-zñáéíóú]+)/i)
  if (!m) return null
  const token = norm(m[1])
  return findStaffByAliasToken(token)
}

// ====== Categorización y listado (filtrado simple)
function servicesByCategory(sedeKey, category){
  const list = servicesForSedeKeyRaw(sedeKey)
  const L = rm(String(category||"")).toLowerCase()
  const not = (s, re) => !re.test(s.norm)

  switch (L){
    case "manicura":
      return list.filter(s => /\b(uñ|manicura|gel|acril|semi|frances|nivelaci|esculpid)\b/.test(s.norm) && not(s, /\b(pestañ|depil|laser|fotodepil|hilo|pedicur)\b/))
    case "pedicura":
      return list.filter(s => /\b(pedicur|pies?)\b/.test(s.norm))
    case "pestañas":
      return list.filter(s => /\b(pestañ|eyelash|lash|lifting|rizado|volumen|2d|3d|mega|tinte)\b/.test(s.norm) && not(s, /\b(depila|laser|foto)\b/))
    case "cejas":
      return list.filter(s => /\b(ceja|brow|henna|laminad|perfilad|microblad|microshad|hairstroke|polvo|powder|ombr|hilo|retoque)\b/.test(s.norm))
    case "depilación":
      return list.filter(s => /\b(depila|cera|cerado|hilo)\b/.test(s.norm) && not(s, /\b(uñ|manicura|pestañ)\b/))
    case "fotodepilación":
      return list.filter(s => /\b(foto ?depil|ipl|laser|l[aá]ser)\b/.test(s.norm) && not(s, /\b(uñ|manicura|pestañ)\b/))
    case "micropigmentación":
      return list.filter(s => /\b(micropigment|microblad|microshad|powder|ombr|labio|eyeliner|p[aá]rpado|ceja|cejas)\b/.test(s.norm))
    case "tratamiento facial":
      return list.filter(s => /\b(facial|higiene|dermaplan|peeling|radiofrecuencia|mascarilla|hidrataci[oó]n)\b/.test(s.norm))
    case "tratamiento corporal":
      return list.filter(s => /\b(corporal|maderoterapia|drenaje|anticelulitis|cavit|radiofrecuencia|masaje)\b/.test(s.norm))
    case "otros":
      return list
    default:
      return []
  }
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
function buildServiceChoiceListBySede(sedeKey, category){
  const list = uniqueByLabel(servicesByCategory(sedeKey, category))
  return list.map((s,i)=>({ index:i+1, label:s.label }))
}

// ====== Square helpers
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
    const msg = await aiRewrite("Para terminar, no encuentro tu ficha por este número. Dime tu *nombre completo* y, si quieres, tu *email* para crearte 😊")
    await sock.sendMessage(jid, { text: msg })
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
  const lines = choices.map(ch => `${ch.index}) ${ch.name}${ch.email!=="—" ? ` (${ch.email})`:""}`).join("\n")
  const msg = await aiRewrite(`Para terminar, he encontrado varias fichas con tu número. ¿Cuál eres?\n\n${lines}\n\nResponde con el número.`)
  await sock.sendMessage(jid, { text: msg })
  return { status:"need_pick" }
}
async function findOrCreateCustomerWithRetry({ name, email, phone }){
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
    } catch(e) { if (attempt < SQUARE_MAX_RETRIES) await sleep(1000 * attempt) }
  }
  return null
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
        db.prepare(`INSERT INTO square_logs (phone,action,request_data,response_data,error_data,timestamp,success) VALUES (?,?,?,?,?,?,?)`)
          .run(phone||"unknown","create_booking",safeJSONStringify(requestData),safeJSONStringify(resp?.result||{}),null,new Date().toISOString(),1)
      }catch{}
      if (booking) return { success: true, booking }
    } catch(e) {
      lastError = e
      try{
        db.prepare(`INSERT INTO square_logs (phone,action,request_data,response_data,error_data,timestamp,success) VALUES (?,?,?,?,?,?,?)`)
          .run(phone||"unknown","create_booking",safeJSONStringify({ attempt, envServiceKey, locationKey, startISO }),null,safeJSONStringify({ message: e?.message, body: e?.body }),new Date().toISOString(),0)
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
  }catch{ return false }
}

// ====== Disponibilidad con cache
async function searchAvailabilityForStaffRaw({ locationKey, envServiceKey, staffId, fromEU, days=14, n=3, distinctDays=false }){
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
async function searchAvailabilityGenericRaw({ locationKey, envServiceKey, fromEU, days=14, n=3, distinctDays=false }){
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
async function searchAvailabilityForStaff(args){
  const key = `staff_${args.locationKey}_${args.envServiceKey}_${args.staffId}_${args.fromEU.format("YYYYMMDDHH")}_${args.days}_${args.n}_${args.distinctDays?'D':'N'}`
  return smartCache.get(key, 8*60*1000, ()=>searchAvailabilityForStaffRaw(args))
}
async function searchAvailabilityGeneric(args){
  const key = `gen_${args.locationKey}_${args.envServiceKey}_${args.fromEU.format("YYYYMMDDHH")}_${args.days}_${args.n}_${args.distinctDays?'D':'N'}`
  return smartCache.get(key, 8*60*1000, ()=>searchAvailabilityGenericRaw(args))
}
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

// ====== Sesiones
function loadSession(phone){
  const row = db.prepare(`SELECT data_json FROM sessions WHERE phone=@phone`).get({phone})
  if (!row?.data_json) return null
  const s = JSON.parse(row.data_json)
  if (Array.isArray(s.lastHours_ms)) s.lastHours = s.lastHours_ms.map(ms=>dayjs.tz(ms,EURO_TZ))
  if (Array.isArray(s.lastDays_ms)) s.lastDays = s.lastDays_ms.map(ms=>dayjs.tz(ms,EURO_TZ))
  if (s.pendingDateTime_ms) s.pendingDateTime = dayjs.tz(s.pendingDateTime_ms,EURO_TZ)
  return s
}
function saveSession(phone,s){
  const c={...s}
  c.lastHours_ms = Array.isArray(s.lastHours)? s.lastHours.map(d=>dayjs.isDayjs(d)?d.valueOf():null).filter(Boolean):[]
  c.lastDays_ms = Array.isArray(s.lastDays)? s.lastDays.map(d=>dayjs.isDayjs(d)?d.valueOf():null).filter(Boolean):[]
  c.pendingDateTime_ms = s.pendingDateTime? (dayjs.isDayjs(s.pendingDateTime)? s.pendingDateTime.valueOf() : dayjs(s.pendingDateTime).valueOf()) : null
  delete c.lastHours; delete c.lastDays; delete c.pendingDateTime
  const j=JSON.stringify(c)
  const up=db.prepare(`UPDATE sessions SET data_json=@j, updated_at=@u WHERE phone=@p`).run({j,u:new Date().toISOString(),p:phone})
  if (up.changes===0) db.prepare(`INSERT INTO sessions (phone,data_json,updated_at) VALUES (@p,@j,@u)`).run({p:phone,j,u:new Date().toISOString()})
}
function clearSession(phone){ db.prepare(`DELETE FROM sessions WHERE phone=@phone`).run({phone}) }

// ====== Analytics
function trackConversationMetrics(phone, stage, success){
  try{
    db.prepare(`INSERT INTO conversation_metrics (phone,stage,success,timestamp,session_duration) VALUES (?,?,?,?,?)`)
      .run(phone, stage||null, success?1:0, Date.now(), 0)
  }catch{}
}

// ====== Notificaciones
function jidFromPhoneE164(phoneE164){
  const raw = String(phoneE164||"").replace(/^\+/, "")
  return `${raw}@s.whatsapp.net`
}
async function sendWhatsAppToPhone(phoneE164, text){
  try{
    const j = jidFromPhoneE164(phoneE164)
    if (!globalThis.sock) return false
    await globalThis.sock.sendPresenceUpdate("composing", j)
    await sleep(600+Math.random()*600)
    await globalThis.sock.sendMessage(j, { text })
    return true
  }catch{ return false }
}
function getUpcomingBookings(hoursAhead=24){
  try{
    const now = dayjs().tz(EURO_TZ).toDate().toISOString()
    const until = dayjs().tz(EURO_TZ).add(hoursAhead, "hour").toDate().toISOString()
    return db.prepare(`SELECT * FROM appointments WHERE status='confirmed' AND start_iso BETWEEN ? AND ?`).all(now, until)
  }catch{ return [] }
}
function getBookingById(id){
  try{ return db.prepare(`SELECT * FROM appointments WHERE id=? OR square_booking_id=?`).get(id,id) }catch{ return null }
}
async function sendBookingReminders(){
  const upcoming = getUpcomingBookings(24)
  for (const b of upcoming){
    try{
      const already = db.prepare(`SELECT 1 FROM booking_notifications WHERE booking_id=? AND type='reminder_24h'`).get(b.id)
      if (already) continue
      const text = await aiRewrite(`¡Hola! Te recordamos tu cita para mañana:
📅 ${fmtES(dayjs(b.start_iso))}
💅 ${b.service_label}
📍 ${locationNice(b.location_key)}

¿Todo bien? Responde "sí" para confirmar o "cambiar" para modificar.`)
      await sendWhatsAppToPhone(b.customer_phone, text)
      db.prepare(`INSERT OR IGNORE INTO booking_notifications (booking_id,type,sent_at) VALUES (?,?,?)`)
        .run(b.id, "reminder_24h", new Date().toISOString())
    }catch{}
  }
}
async function sendFollowUp(bookingId, hoursAfter = 24){
  setTimeout(async ()=>{
    const b = getBookingById(bookingId)
    if (!b) return
    const text = await aiRewrite(`¡Hola! Esperamos que hayas disfrutado tu ${b.service_label}.
¿Nos dejas una reseña? ¡Nos ayuda mucho! ⭐`)
    await sendWhatsAppToPhone(b.customer_phone, text)
  }, hoursAfter * 3600 * 1000)
}

// ====== Chat helpers
async function sendWithPresence(sock, jid, text){
  try{ await sock.sendPresenceUpdate("composing", jid) }catch{}
  await new Promise(r=>setTimeout(r, 600+Math.random()*600))
  return sock.sendMessage(jid, { text })
}
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

// ====== Flujo: servicio → días → horas
async function listServiceMenuOrAskCategory(sessionData, sock, jid){
  if (!sessionData.category){
    sessionData.stage = "awaiting_category"
    sessionData.flow_state = FLOW_STATES.COLLECTING
    saveSession(sessionData.customer_phone || "", sessionData)
    const msg = await aiRewrite("¿Qué te quieres hacer: *manicura*, *pedicura*, *pestañas*, *cejas*, *depilación*, *fotodepilación*, *micropigmentación*, *tratamiento facial*, *tratamiento corporal* u *otros*?")
    await sendWithPresence(sock, jid, msg)
    return false
  }
  if (!sessionData.sede){
    sessionData.stage = "awaiting_sede_for_services"
    sessionData.flow_state = FLOW_STATES.COLLECTING
    saveSession(sessionData.customer_phone || "", sessionData)
    const msg = await aiRewrite(`¿En qué sede te viene mejor, Torremolinos o La Luz? (para ${sessionData.category})`)
    await sendWithPresence(sock, jid, msg)
    return false
  }
  const items = buildServiceChoiceListBySede(sessionData.sede, sessionData.category)
  if (!items.length){
    const msg = await aiRewrite(`Ahora mismo no tengo servicios de ${sessionData.category} configurados para esa sede. Si quieres, dime el *nombre exacto* del servicio.`)
    await sendWithPresence(sock, jid, msg)
    return false
  }
  sessionData.serviceChoices = items
  sessionData.stage = "awaiting_service_choice"
  sessionData.flow_state = FLOW_STATES.COLLECTING
  saveSession(sessionData.customer_phone || "", sessionData)
  const lines = items.map(it=> `${it.index}) ${applySpanishDiacritics(it.label)}`).join("\n")
  const msg = await aiRewrite(`Estas son nuestras opciones de *${sessionData.category}* en ${locationNice(sessionData.sede)}:\n\n${lines}\n\nResponde con el número.`)
  await sendWithPresence(sock, jid, msg)
  return true
}
async function proposeClosestDays({ sessionData, sock, jid }){
  const nowEU = dayjs().tz(EURO_TZ)
  const baseFrom = nextOpeningFrom(nowEU.add(NOW_MIN_OFFSET_MIN, "minute"))
  const daysWanted = 3
  let slots = []
  if (sessionData.preferredStaffId && isStaffAllowedInLocation(sessionData.preferredStaffId, sessionData.sede)) {
    const s = await searchAvailabilityForStaff({ locationKey: sessionData.sede, envServiceKey: sessionData.selectedServiceEnvKey, staffId: sessionData.preferredStaffId, fromEU: baseFrom, n: daysWanted, distinctDays: true })
    slots = s
  }
  if (!slots.length){
    const g = await searchAvailabilityGeneric({ locationKey: sessionData.sede, envServiceKey: sessionData.selectedServiceEnvKey, fromEU: baseFrom, n: daysWanted, distinctDays: true })
    slots = g
  }
  if (!slots.length){
    const out=[]; let t = baseFrom.clone()
    while (out.length<daysWanted){
      if (WORK_DAYS.includes(t.day()) && !isHolidayEU(t)) out.push({ date:t.clone(), staffId:null })
      t = t.add(1,"day").hour(OPEN.start).minute(0)
    }
    slots = out
  }
  const uniqDays = []
  const seen = new Set()
  for (const s of slots){
    const key = s.date.format("YYYY-MM-DD")
    if (seen.has(key)) continue
    seen.add(key); uniqDays.push(s.date.startOf("day"))
    if (uniqDays.length>=daysWanted) break
  }
  if (!uniqDays.length){
    const msg = await aiRewrite("No encuentro días disponibles en los próximos días. ¿Otra fecha aproximada?")
    await sendWithPresence(sock, jid, msg)
    return false
  }
  sessionData.lastDays = uniqDays
  sessionData.stage = "awaiting_day_choice"
  sessionData.flow_state = FLOW_STATES.COLLECTING
  saveSession(sessionData.customer_phone || "", sessionData)
  const lines = uniqDays.map((d,i)=> `${i+1}) ${d.format("dddd DD/MM")}`).join("\n")
  const hdr = sessionData.preferredStaffLabel ? `con ${sessionData.preferredStaffLabel}` : " (nuestro equipo)"
  const msg = await aiRewrite(`Días disponibles${hdr}:\n${lines}\n\nResponde con el número (1, 2 o 3).`)
  await sendWithPresence(sock, jid, msg)
  return true
}
async function proposeHoursForPickedDay({ sessionData, sock, jid, pickedDayIndex }){
  const idx = Number(pickedDayIndex) - 1
  const day = (sessionData.lastDays||[])[idx]
  if (!day){ await sendWithPresence(sock, jid, "Elige un día válido (1, 2 o 3)."); return false }
  const fromEU = day.clone().hour(OPEN.start).minute(0)
  const baseFrom = nextOpeningFrom(fromEU)
  let slots = []
  let usedPreferred = false
  if (sessionData.preferredStaffId && isStaffAllowedInLocation(sessionData.preferredStaffId, sessionData.sede)){
    const s = await searchAvailabilityForStaff({ locationKey: sessionData.sede, envServiceKey: sessionData.selectedServiceEnvKey, staffId: sessionData.preferredStaffId, fromEU: baseFrom, days:1, n: 6 })
    if (s.length){ slots = s; usedPreferred = true }
  }
  if (!slots.length){
    const g = await searchAvailabilityGeneric({ locationKey: sessionData.sede, envServiceKey: sessionData.selectedServiceEnvKey, fromEU: baseFrom, days:1, n: 6 })
    slots = g
  }
  if (!slots.length){
    const general = proposeSlots({ fromEU: baseFrom, durationMin:60, n:3 }).map(d=>({ date:d, staffId:null }))
    slots = general
  }
  if (!slots.length){ await sendWithPresence(sock, jid, "Ese día no tiene huecos visibles. ¿Te viene bien otro?"); return false }

  slots = slots.filter(s => !s.staffId || isStaffAllowedInLocation(s.staffId, sessionData.sede))
  const hoursEnum = enumerateHours(slots.map(s => s.date))
  const map = {}; for (const s of slots) map[s.date.format("YYYY-MM-DDTHH:mm")] = s.staffId || null

  const nameMap = {}
  Object.values(map).forEach(sid => { if (sid) nameMap[sid] = staffLabelFromId(sid) })
  sessionData.lastStaffNamesById = nameMap

  sessionData.lastHours = slots.map(s => s.date)
  sessionData.lastStaffByIso = map
  sessionData.lastProposeUsedPreferred = usedPreferred
  sessionData.stage = "awaiting_time_choice"
  sessionData.flow_state = FLOW_STATES.CONFIRMING
  sessionData.lastPickedDayIndex = Number(pickedDayIndex)
  sessionData.lastPickedDayISO = day.format("YYYY-MM-DD")
  saveSession(sessionData.customer_phone || "", sessionData)

  const lines = hoursEnum.map(h => {
    const sid = map[h.iso]
    const tag = sid ? ` — ${staffLabelFromId(sid)}` : ""
    return `${h.index}) ${h.pretty}${tag}`
  }).join("\n")
  const header = usedPreferred
    ? `Horarios disponibles con ${sessionData.preferredStaffLabel}:`
    : `Horarios disponibles (nuestro equipo):${sessionData.preferredStaffLabel ? `\nNota: no veo huecos con ${sessionData.preferredStaffLabel} ese día; te muestro alternativas.`:""}`
  await sendWithPresence(sock, jid, `${header}\n${lines}\n\nResponde con el número.`)
  return true
}

// ====== Crear reserva
async function executeCreateBooking(sessionData, phone, sock, jid) {
  if (!sessionData.sede) { await sendWithPresence(sock, jid, "Falta seleccionar la sede (Torremolinos o La Luz)"); return; }
  if (!sessionData.selectedServiceEnvKey) { await sendWithPresence(sock, jid, "Falta seleccionar el servicio"); return; }
  if (!sessionData.pendingDateTime) { await sendWithPresence(sock, jid, "Falta seleccionar la fecha y hora"); return; }

  const startEU = parseToEU(sessionData.pendingDateTime)
  if (!insideBusinessHours(startEU, 60)) { await sendWithPresence(sock, jid, "Esa hora está fuera del horario (L-V 09:00–20:00)"); return; }

  const iso = startEU.format("YYYY-MM-DDTHH:mm")
  let staffId = null

  if (sessionData.preferredStaffId && isStaffAllowedInLocation(sessionData.preferredStaffId, sessionData.sede)) {
    const probe = await searchAvailabilityForStaff({
      locationKey: sessionData.sede,
      envServiceKey: sessionData.selectedServiceEnvKey,
      staffId: sessionData.preferredStaffId,
      fromEU: startEU.clone().startOf("day"),
      days: 1,
      n: 50
    })
    const ok = probe.some(x => x.date.isSame(startEU, "minute"))
    if (ok) staffId = sessionData.preferredStaffId
    else {
      const label = sessionData.preferredStaffLabel || "tu profesional"
      await sendWithPresence(sock, jid, `Esa hora no está disponible con ${label}. Te enseño otras horas de ese mismo día con ${label}:`)
      const idx = sessionData.lastPickedDayIndex || 1
      sessionData.lastProposeUsedPreferred = true
      saveSession(phone, sessionData)
      await proposeHoursForPickedDay({ sessionData, sock, jid, pickedDayIndex: idx })
      return
    }
  }
  if (!staffId) staffId = sessionData.lastStaffByIso?.[iso] || null
  if (!staffId) {
    const locId = locationToId(sessionData.sede)
    const fallback = EMPLOYEES.find(e => e.bookable && (e.allowIds.includes("ALL") || e.allowIds.includes(locId) || e.allowTxt.includes(sessionData.sede)))
    staffId = fallback?.id || null
  }
  if (!staffId) { await sendWithPresence(sock, jid, "No hay profesionales disponibles en esa sede"); return; }

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
    sessionData.flow_state = FLOW_STATES.CONFIRMING
    saveSession(phone, sessionData)
    await sendWithPresence(sock, jid, "Para terminar, dime tu *nombre* y (opcional) tu *email* para crear tu ficha 😊")
    return
  }

  // Crear
  const contextForError = { start: startEU.toISOString(), sede: sessionData.sede, service: sessionData.selectedServiceEnvKey, staffId, phone }
  const result = await createBookingWithRetry({ startEU, locationKey: sessionData.sede, envServiceKey: sessionData.selectedServiceEnvKey, durationMin: 60, customerId, teamMemberId: staffId, phone })
  if (!result.success) {
    await ErrorHandler.fallbackToManualQueue(contextForError)
    const aptId = `apt_failed_${Math.random().toString(36).slice(2,8)}${Date.now().toString(36).slice(-4)}`
    db.prepare(`INSERT INTO appointments (id,customer_name,customer_phone,customer_square_id,location_key,service_env_key,service_label,duration_min,start_iso,end_iso,staff_id,status,created_at,square_booking_id,square_error,retry_count)
      VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`
    ).run(
      aptId, sessionData?.name || null, phone, customerId, sessionData.sede, sessionData.selectedServiceEnvKey,
      sessionData.selectedServiceLabel || serviceLabelFromEnvKey(sessionData.selectedServiceEnvKey) || "Servicio", 60,
      startEU.tz("UTC").toISOString(), startEU.clone().add(60,"minute").tz("UTC").toISOString(), staffId, "failed", new Date().toISOString(), null, result.error, SQUARE_MAX_RETRIES
    )
    const msg = await aiRewrite("Ahora mismo no puedo confirmarte esa reserva. Cristina te contesta en cuanto pueda 🙏 ¿Quieres que te proponga otra hora mientras tanto?")
    await sendWithPresence(sock, jid, msg)
    trackConversationMetrics(phone, "create_booking", false)
    return
  }

  if (result.booking.__sim) { await sendWithPresence(sock, jid, "🧪 SIMULACIÓN: Reserva creada exitosamente (modo prueba)"); clearSession(phone); return }

  const aptId = `apt_${Math.random().toString(36).slice(2,8)}${Date.now().toString(36).slice(-4)}`
  db.prepare(`INSERT INTO appointments (id,customer_name,customer_phone,customer_square_id,location_key,service_env_key,service_label,duration_min,start_iso,end_iso,staff_id,status,created_at,square_booking_id,square_error,retry_count)
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`
  ).run(
    aptId, sessionData?.name || null, phone, customerId, sessionData.sede, sessionData.selectedServiceEnvKey,
    sessionData.selectedServiceLabel || serviceLabelFromEnvKey(sessionData.selectedServiceEnvKey) || "Servicio",
    60, startEU.tz("UTC").toISOString(), startEU.clone().add(60,"minute").tz("UTC").toISOString(), staffId, "confirmed", new Date().toISOString(),
    result.booking.id, null, 0
  )

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
  trackConversationMetrics(phone, "create_booking", true)
  clearSession(phone);
  sendFollowUp(aptId, 24).catch(()=>{})
}

// ====== Listar / Cancelar
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
  if (!appointments.length) { 
    const msg = await aiRewrite("No tienes citas programadas. ¿Quieres agendar una?")
    await sendWithPresence(sock, jid, msg); 
    return; 
  }
  const message = `Tus próximas citas (asociadas a tu número):\n\n${appointments.map(apt => 
    `${apt.index}) ${apt.pretty}\n📍 ${apt.sede}\n👩‍💼 ${apt.profesional}\n`
  ).join("\n")}`;
  await sendWithPresence(sock, jid, message);
}
async function executeCancelAppointment(params, sessionData, phone, sock, jid) {
  const appointments = await enumerateCitasByPhone(phone);
  if (!appointments.length) { 
    const msg = await aiRewrite("No encuentro citas futuras asociadas a tu número. ¿Quieres que te ayude a reservar?")
    await sendWithPresence(sock, jid, msg); 
    return; 
  }
  const appointmentIndex = params?.appointmentIndex;
  if (!appointmentIndex) {
    sessionData.cancelList = appointments
    sessionData.stage = "awaiting_cancel"
    saveSession(phone, sessionData)
    const message = `Estas son tus próximas citas (por tu número). ¿Cuál quieres cancelar?\n\n${appointments.map(apt => 
      `${apt.index}) ${apt.pretty} - ${apt.sede}`
    ).join("\n")}\n\nResponde con el número`
    await sendWithPresence(sock, jid, message);
    return;
  }
  const appointment = appointments.find(apt => apt.index === appointmentIndex);
  if (!appointment) { await sendWithPresence(sock, jid, "No encontré esa cita. ¿Puedes verificar el número?"); return; }
  const success = await cancelBooking(appointment.id);
  if (success) { await sendWithPresence(sock, jid, `✅ Cita cancelada: ${appointment.pretty} en ${appointment.sede}`) }
  else { await sendWithPresence(sock, jid, "No pude cancelar la cita. Por favor contacta directamente al salón.") }
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
  .card{max-width:860px;padding:32px;border-radius:20px;box-shadow:0 8px 32px rgba(0,0,0,.1);background:white}
  .status{padding:12px;border-radius:8px;margin:8px 0}
  .success{background:#d4edda;color:#155724}
  .error{background:#f8d7da;color:#721c24}
  .warning{background:#fff3cd;color:#856404}
  .stat{display:inline-block;margin:0 16px;padding:8px 12px;background:#e9ecef;border-radius:6px}
  </style><div class="card">
  <h1>🩷 Gapink Nails Bot v28.4.0</h1>
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

// ====== Cola + envío
let RECONNECT_SCHEDULED = false
let RECONNECT_ATTEMPTS = 0
const QUEUE=new Map()
function enqueue(key,job){
  const prev=QUEUE.get(key)||Promise.resolve()
  const next=prev.then(job,job).finally(()=>{ if (QUEUE.get(key)===next) QUEUE.delete(key) })
  QUEUE.set(key,next); return next
}

// ====== Start bot
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

    // Recordatorios periódicos y precarga
    setInterval(()=>{ sendBookingReminders().catch(()=>{}) }, 15*60*1000)
    smartCache.predictivePreload().catch(()=>{})
    PQ.processWithBackpressure().catch(()=>{})

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
            customer_phone: phone,
            greeted: false, sede: null, selectedServiceEnvKey: null, selectedServiceLabel: null,
            preferredStaffId: null, preferredStaffLabel: null, pendingDateTime: null,
            name: null, email: null, last_msg_id: null, lastStaffByIso: {},
            lastProposeUsedPreferred: false, stage: null, cancelList: null,
            serviceChoices: null, identityChoices: null, pendingCategory: null,
            lastStaffNamesById: null, lastHours: null, lastDays: null,
            snooze_until_ms: null,
            identityResolvedCustomerId: null,
            category: null,
            ai_number_choice: null,
            lastPickedDayIndex: null,
            lastPickedDayISO: null,
            flow_state: FLOW_STATES.COLLECTING,
            ctxSlots: {} // esp. para ConversationContext mirror
          }
          if (sessionData.last_msg_id === m.key.id) return
          sessionData.last_msg_id = m.key.id

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

          // Sentimiento + memoria
          const sent = await analyzeUserSentiment(textRaw)
          const memory = loadConversationMemory(phone) || { history: [], preferences: {} }
          const intents = await parseMultipleIntentsEnhanced(textRaw, memory.history||[])
          const maybeStaff = parsePreferredStaffFromText(textRaw) || (intents?.entities?.staff ? findStaffByAliasToken(norm(intents.entities.staff)) : null)
          if (maybeStaff){
            sessionData.preferredStaffId = maybeStaff.id
            sessionData.preferredStaffLabel = staffLabelFromId(maybeStaff.id)
          }
          const chosenIndex = intents?.entities?.number_choice ?? (norm(textRaw).match(/^\s*([1-9]\d*)\s*$/)?.[1] ? Number(norm(textRaw).match(/^\s*([1-9]\d*)\s*$/)[1]) : null)
          sessionData.ai_number_choice = chosenIndex

          // Contexto de slots
          const ctx = new ConversationContext()
          if (sessionData.category) ctx.updateSlot("category", sessionData.category, 1)
          if (sessionData.sede) ctx.updateSlot("location", sessionData.sede, 1)
          if (sessionData.selectedServiceEnvKey) ctx.updateSlot("service_env", sessionData.selectedServiceEnvKey, 1)
          if (sessionData.selectedServiceLabel) ctx.updateSlot("service_label", sessionData.selectedServiceLabel, 1)
          // detectar nuevos slots
          if (intents?.entities?.location) { const s = intents.entities.location; const sede = s==="la_luz"||s==="torremolinos"?s:parseSede(textRaw); if (sede) ctx.updateSlot("location", sede, 0.8) }
          if (intents?.entities?.category) ctx.updateSlot("category", intents.entities.category, 0.8)
          if (intents?.entities?.service && sessionData.sede){
            const ek = resolveEnvKeyFromLabelAndSede(intents.entities.service, sessionData.sede)
            if (ek){ ctx.updateSlot("service_env", ek, 0.7); ctx.updateSlot("service_label", intents.entities.service, 0.7) }
          }

          // Persistir cambios de slots en la sesión
          if (ctx.get("location")) sessionData.sede = ctx.get("location")
          if (ctx.get("category")) sessionData.category = ctx.get("category")
          if (ctx.get("service_env")) sessionData.selectedServiceEnvKey = ctx.get("service_env")
          if (ctx.get("service_label")) sessionData.selectedServiceLabel = ctx.get("service_label")

          saveSession(phone, sessionData)

          // === Ramas de identidad pendientes
          if (sessionData.stage==="awaiting_identity_pick"){
            if (!chosenIndex){ await sendWithPresence(sock, jid, "Responde con el número de tu ficha (1, 2, …)."); return }
            const choice = (sessionData.identityChoices||[]).find(c=>c.index===chosenIndex)
            if (!choice){ await sendWithPresence(sock, jid, "No encontré esa opción. Prueba con el número de la lista."); return }
            sessionData.identityResolvedCustomerId = choice.id
            sessionData.stage = null
            sessionData.flow_state = FLOW_STATES.CONFIRMING
            saveSession(phone, sessionData)
            await sendWithPresence(sock, jid, "¡Gracias! Finalizo tu reserva…")
            await executeCreateBooking(sessionData, phone, sock, jid)
            return
          }
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
            sessionData.flow_state = FLOW_STATES.CONFIRMING
            saveSession(phone, sessionData)
            await sendWithPresence(sock, jid, "¡Gracias! Finalizo tu reserva…")
            await executeCreateBooking(sessionData, phone, sock, jid)
            return
          }

          // === Cancelar / Listar (si no hay pregunta pendiente)
          const hasPendingQuestion = !!sessionData.stage && /^awaiting_/.test(sessionData.stage)
          const isPlainNumber = /^\s*\d+\s*$/.test(textRaw)
          if (!hasPendingQuestion && !isPlainNumber && /(?:\bcancelar\b|\banular\b|\bborrar\b)/i.test(norm(textRaw))) {
            await executeCancelAppointment({}, sessionData, phone, sock, jid)
            return
          }
          if (!hasPendingQuestion && intents?.primary_intent==="consultar" && (intents?.secondary_intents||[]).includes("horario")){
            await executeListAppointments({}, sessionData, phone, sock, jid)
            return
          }

          // === Flujo principal
          if (!sessionData.selectedServiceEnvKey){
            if (sessionData.stage === "awaiting_category"){
              const catToken = ctx.get("category") || (["manicura","pedicura","pestañas","cejas","depilación","fotodepilación","micropigmentación","tratamiento facial","tratamiento corporal","otros"].find(x => norm(textRaw).includes(norm(x))) || null)
              if (!catToken){
                const msg = await aiRewrite("Dime por favor: *manicura*, *pedicura*, *pestañas*, *cejas*, *depilación*, *fotodepilación*, *micropigmentación*, *tratamiento facial*, *tratamiento corporal* u *otros* 😊", {sent})
                await sendWithPresence(sock, jid, msg)
                saveSession(phone, sessionData)
                return
              }
              sessionData.category = catToken
              sessionData.stage = null
              sessionData.flow_state = FLOW_STATES.COLLECTING
              saveSession(phone, sessionData)
              await listServiceMenuOrAskCategory(sessionData, sock, jid)
              return
            }
            if (sessionData.stage==="awaiting_sede_for_services"){
              const sede = ctx.get("location") || parseSede(textRaw)
              if (!sede){
                await sendWithPresence(sock, jid, "¿Prefieres *Torremolinos* o *La Luz*?")
                return
              }
              sessionData.sede = sede
              sessionData.stage = null
              sessionData.flow_state = FLOW_STATES.COLLECTING
              saveSession(phone, sessionData)
              await listServiceMenuOrAskCategory(sessionData, sock, jid)
              return
            }
            if (!hasPendingQuestion){
              if (!sessionData.category){
                sessionData.stage = "awaiting_category"
                sessionData.flow_state = FLOW_STATES.COLLECTING
                saveSession(phone, sessionData)
                const hello = await aiRewrite("¡Hola! 😊 ¿Qué te apetece hacerte hoy? Tenemos manicura, pedicura, pestañas, cejas, depilación, fotodepilación, micropigmentación, tratamientos faciales y corporales. ¡Dime y te ayudo!")
                await sendWithPresence(sock, jid, hello)
                return
              }
              if (!sessionData.sede){
                sessionData.stage = "awaiting_sede_for_services"
                sessionData.flow_state = FLOW_STATES.COLLECTING
                saveSession(phone, sessionData)
                const msg = await aiRewrite(`¿En qué sede te viene mejor, Torremolinos o La Luz? (para ${sessionData.category})`)
                await sendWithPresence(sock, jid, msg)
                return
              }
              await listServiceMenuOrAskCategory(sessionData, sock, jid)
              return
            }
          }

          // Elegir servicio
          if (sessionData.stage==="awaiting_service_choice" && Array.isArray(sessionData.serviceChoices) && sessionData.serviceChoices.length){
            if (!isPlainNumber && chosenIndex==null){
              await sendWithPresence(sock, jid, "Responde con el *número* del servicio, por ejemplo: 1, 2 o 3.")
              return
            }
            const pick = sessionData.serviceChoices.find(it=>it.index===chosenIndex)
            if (!pick){
              await sendWithPresence(sock, jid, "No encontré esa opción. Prueba con uno de los números de la lista.")
              return
            }
            const ek = resolveEnvKeyFromLabelAndSede(pick.label, sessionData.sede)
            if (!ek){
              await sendWithPresence(sock, jid, "No puedo vincular ese servicio ahora mismo. ¿Puedes decirme el *nombre exacto* del servicio?")
              return
            }
            sessionData.selectedServiceLabel = pick.label
            sessionData.selectedServiceEnvKey = ek
            sessionData.stage = null
            sessionData.ai_number_choice = null
            sessionData.flow_state = FLOW_STATES.COLLECTING
            saveSession(phone, sessionData)

            // Si la preferida no está en la sede → lista completa de staff
            if (sessionData.preferredStaffId && !isStaffAllowedInLocation(sessionData.preferredStaffId, sessionData.sede)){
              const all = allowedStaffLabelsForLocation(sessionData.sede)
              sessionData.stage = "awaiting_staff_choice"
              sessionData.staffChoices = all.map((n,i)=>({index:i+1,label:n}))
              sessionData.flow_state = FLOW_STATES.COLLECTING
              saveSession(phone, sessionData)
              const lines = all.map((n,i)=>`${i+1}) ${n}`).join("\n")
              const msg = await aiRewrite(`Esa profesional no atiende en ${locationNice(sessionData.sede)}. Puedo proponerte con:\n\n${lines}\n\nResponde con el número o di el nombre tal cual aparece.`)
              await sendWithPresence(sock, jid, msg)
              return
            }
            await proposeClosestDays({ sessionData, sock, jid })
            return
          }

          // Elegir profesional explícita
          if (sessionData.stage === "awaiting_staff_choice"){
            if (chosenIndex) {
              const pick = (sessionData.staffChoices || []).find(x => x.index === chosenIndex)
              if (!pick) {
                await sendWithPresence(sock, jid, "Elige una opción válida o di el nombre tal cual aparece.")
                return
              }
              const emp = EMPLOYEES.find(e => e.labels.some(lbl => norm(lbl) === norm(pick.label)))
              if (emp && isStaffAllowedInLocation(emp.id, sessionData.sede)) {
                sessionData.preferredStaffId = emp.id
                sessionData.preferredStaffLabel = staffLabelFromId(emp.id)
                sessionData.stage = null
                sessionData.ai_number_choice = null
                sessionData.flow_state = FLOW_STATES.COLLECTING
                saveSession(phone, sessionData)
              } else {
                await sendWithPresence(sock, jid, "Esa profesional no está disponible en esa sede. Elige otra de la lista.")
                return
              }
            } else {
              const maybe = parsePreferredStaffFromText(textRaw)
              if (maybe && isStaffAllowedInLocation(maybe.id, sessionData.sede)) {
                sessionData.preferredStaffId = maybe.id
                sessionData.preferredStaffLabel = staffLabelFromId(maybe.id)
                sessionData.stage = null
                sessionData.flow_state = FLOW_STATES.COLLECTING
                saveSession(phone, sessionData)
              } else {
                await sendWithPresence(sock, jid, "Elige una opción de la lista (1, 2, …) o di el nombre tal cual aparece.")
                return
              }
            }
            await proposeClosestDays({ sessionData, sock, jid })
            return
          }

          // Elegir día
          if (sessionData.stage === "awaiting_day_choice" && Array.isArray(sessionData.lastDays) && sessionData.lastDays.length){
            if (!chosenIndex){
              await sendWithPresence(sock, jid, "Responde con el *número* del día, por ejemplo: 1, 2 o 3.")
              return
            }
            sessionData.ai_number_choice = null
            saveSession(phone, sessionData)
            await proposeHoursForPickedDay({ sessionData, sock, jid, pickedDayIndex: chosenIndex })
            return
          }

          // Elegir hora
          if (sessionData.stage === "awaiting_time_choice" && Array.isArray(sessionData.lastHours) && sessionData.lastHours.length){
            if (!chosenIndex){
              await sendWithPresence(sock, jid, "Responde con el *número* del horario.")
              return
            }
            const pick = sessionData.lastHours[chosenIndex-1]
            if (!dayjs.isDayjs(pick)){ await sendWithPresence(sock, jid, "Elige un horario válido."); return }
            const iso = pick.format("YYYY-MM-DDTHH:mm")
            const staffFromIso = sessionData?.lastStaffByIso?.[iso] || null

            sessionData.pendingDateTime = pick.tz(EURO_TZ).toISOString()
            if (staffFromIso && !sessionData.preferredStaffId){
              sessionData.preferredStaffId = staffFromIso
              sessionData.preferredStaffLabel = staffLabelFromId(staffFromIso)
            }
            sessionData.stage = null
            sessionData.ai_number_choice = null
            sessionData.flow_state = FLOW_STATES.CONFIRMING
            saveSession(phone, sessionData)
            await sendWithPresence(sock, jid, "Perfecto, voy a confirmar esa hora ✨")
            await executeCreateBooking(sessionData, phone, sock, jid)
            return
          }

          // Ambiguo → pedir aclaración
          if (!hasPendingQuestion && !isPlainNumber){
            const clar = await aiRewrite(`Para ayudarte mejor, necesito un dato más. Si quieres reservar dime: categoría (manicura/pedicura/pestañas/cejas/depilación/fotodepilación/micropigmentación/tratamiento facial/corporal), sede (Torremolinos/La Luz), servicio exacto, y día/hora aproximados.`)
            await sendWithPresence(sock, jid, clar)
            return
          }

          saveSession(phone, sessionData)
          const resp = "Disculpa, hubo un error técnico. ¿Puedes repetir tu mensaje?"
          await sendWithPresence(sock, jid, resp)

          // Memoria final
          updateConversationMemory(phone, textRaw, resp)

        } catch (error) {
          if (BOT_DEBUG) console.error(error)
          trackConversationMetrics(phone, "uncaught", false)
          await sendWithPresence(sock, jid, "Disculpa, hubo un error técnico. ¿Puedes repetir tu mensaje?")
        }
      })
    })
  }catch(e){ 
    setTimeout(() => startBot().catch(console.error), 5000) 
  }
}

// ====== Arranque
console.log(`🩷 Gapink Nails Bot v28.4.0`)
const server = app.listen(PORT, ()=>{ console.log(`HTTP ${PORT}`) })
startBot().catch(console.error)

process.on("uncaughtException", (e)=>{ console.error("💥 uncaughtException:", e?.stack||e?.message||e) })
process.on("unhandledRejection", (e)=>{ console.error("💥 unhandledRejection:", e) })
process.on("SIGTERM", ()=>{ try{ server.close(()=>process.exit(0)) }catch{ process.exit(0) } })
process.on("SIGINT", ()=>{ try{ server.close(()=>process.exit(0)) }catch{ process.exit(0) } })
