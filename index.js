// index.js — Gapink Nails · v28.0.0
// Cambios clave:
// • Flujo guiado por etapas: categoría → sede → servicio → día → hora → identidad → confirmación.
// • IA DeepSeek en: parseo de intención, reescritura de respuestas y fallback.
// • Nueva lógica de días: primero 3 días distintos con huecos, luego horas del día elegido.
// • Menús por categoría (manicura, pedicura, pestañas, cejas, depilación, fotodepilación, micropigmentación, tratamiento facial, tratamiento corporal, otros).
// • “con {nombre}” persiste y valida por sede; si no, lista TODAS las profesionales de la sede.
// • No se lanzan listados/cancelaciones mientras hay una pregunta pendiente (awaiting_*).
// • Arreglos de estilo, template strings y únicos app.listen/startBot.

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
async function aiParseUserMessage(userText, session){
  const sys = "Eres un parser de intención muy estricto. Responde SOLO JSON válido, sin texto adicional."
  const prompt = {
    role:"user",
    content:
`Extrae entidades del mensaje y del contexto. Devuelve JSON:
{"category":null|"manicura"|"pedicura"|"pestañas"|"cejas"|"depilación"|"fotodepilación"|"micropigmentación"|"tratamiento facial"|"tratamiento corporal"|"otros",
 "sede":null|"torremolinos"|"la_luz",
 "intent":"reservar"|"cancelar"|"listar"|"saludo"|"otro",
 "staff_token":string|null,
 "service_hint":string|null,
 "number_choice":1|2|3|null}
Reglas:
- Si el mensaje es SOLO un número (1..3), pon intent="otro" y ese número en number_choice; NO pongas "listar" ni "cancelar".
- Si detectas “con {nombre}” o “con {alias}”, rellena staff_token con ese nombre/alias.
- No inventes datos. category solo si el mensaje lo sugiere con claridad.
Mensaje: "${userText}"
Contexto: ${safeJSONStringify({ sede: session?.sede||null, category: session?.category||null, stage: session?.stage||null })}`
  }
  try{
    const raw = await aiWithRetries([prompt], sys)
    if (!raw) return { intent:"otro", number_choice:null }
    const cleaned = raw.replace(/```json|```/g,"").trim()
    const obj = JSON.parse(cleaned)
    return obj
  }catch{ return { intent:"otro", number_choice:null } }
}
async function aiRewrite(text){
  if (!AI_API_KEY) return text
  const sys = "Reescribe el texto para WhatsApp en español europeo, tono cercano, claro, sin emojis si ya hay demasiados. Devuelve SOLO el texto reescrito."
  const msg = { role:"user", content:text }
  const out = await aiWithRetries([msg], sys)
  return (out && out.trim()) ? out.trim() : text
}

// ====== Utils básicos / tiempo
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

// ====== Personal & sedes
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
    let allow = (locs||"").split(",").map(s=>s.trim()).filter(Boolean) // ids Square o location ids

    // Prefer mapping por EMP_CENTER_* (más legible)
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
  return e?.labels?.[0] || (id ? `Profesional ${String(id).slice(-4)}` : null)
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
function allowedStaffLabelsForLocation(sedeKey){
  const locId = locationToId(sedeKey)
  return EMPLOYEES
    .filter(e => e.bookable && (e.allow.includes("ALL") || e.allow.includes(locId)))
    .map(e=>staffLabelFromId(e.id))
    .filter(Boolean)
}

// ====== Aliases staff
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
function parsePreferredStaffFromText(text){
  const t = norm(text||"")
  // patrones: "con desi", "con cristina", "cita con patri"
  const m = t.match(/\b(?:con|cita con|con la|con el)\s+([a-zñáéíóú]+)/i)
  if (!m) return null
  const token = norm(m[1])
  return findStaffByAliasToken(token)
}

// ====== Servicios: carga por env
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

// ====== Clasificación por categorías (filtros)
function servicesByCategory(sedeKey, category, userMsg){
  const list = servicesForSedeKeyRaw(sedeKey)
  const L = category?.toLowerCase() || ""
  const msg = norm(userMsg||"")
  const has = (s, re) => re.test(s.norm)
  const not = (s, re) => !re.test(s.norm)

  // Reglas generales por categoría
  switch (L){
    case "manicura":
      return list.filter(s => /\b(uñ|manicura|gel|acril|semi|frances|nivelaci|esculpid)\b/.test(s.norm) && not(s, /\b(pestañ|depil|laser|fotodepil|hilo|pedicur)\b/)).map(x=>x)
    case "pedicura":
      return list.filter(s => /\b(pedicur|pies?)\b/.test(s.norm))
    case "pestañas":
      return list.filter(s => /\b(pestañ|eyelash|lash|lifting|rizado|volumen|2d|3d|mega|tinte)\b/.test(s.norm) && not(s, /\b(depila|laser|foto)\b/))
    case "cejas":
      return list.filter(s => /\b(ceja|brow|henna|laminad|perfilad|microblad|microshad|hairstroke|polvo|powder|ombr|hilo|retoque)\b/.test(s.norm)
        && not(s, /\b(pierna|axila|pubis|ingle|bikini|braz|espalda|facial completo|piernas completas|medias piernas)\b/))
    case "depilación":
      return list.filter(s => /\b(depila|cera|cerado|hilo)\b/.test(s.norm) && not(s, /\b(uñ|manicura|pestañ)\b/))
    case "fotodepilación":
      return list.filter(s => /\b(foto ?depil|ipl|laser|l[aá]ser)\b/.test(s.norm) && not(s, /\b(uñ|manicura|pestañ)\b/))
    case "micropigmentación":
      return list.filter(s => {
        const l = s.norm
        const mp = /\b(micropigment|microblad|microshad|powder|ombr|labio|eyeliner|p[aá]rpado|ceja|cejas)\b/.test(l)
        const dep = /\b(depil|hilo|wax|foto ?depil|laser|l[aá]ser|ipl)\b/.test(l)
        return mp && !dep
      })
    case "tratamiento facial":
      return list.filter(s => /\b(facial|higiene|dermaplan|peeling|radiofrecuencia|mascarilla|hidrataci[oó]n)\b/.test(s.norm))
    case "tratamiento corporal":
      return list.filter(s => /\b(corporal|maderoterapia|drenaje|anticelulitis|cavit|radiofrecuencia|masaje)\b/.test(s.norm))
    case "otros":
      return list // fallback general
    default:
      // Si no hay categoría, no listar nada aún
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
function scoreServiceRelevance(userMsg, label){
  const u = norm(userMsg||""), l = norm(label); let score = 0
  const tokens = ["natural","francesa","frances","decoracion","diseño","extra","express","completa","nivelacion","nivelación","henna","lamin","láser","laser","cera","depil","pedicura","facial","corporal","micro"]
  for (const t of tokens){ if (u.includes(norm(t)) && l.includes(norm(t))) score += 0.4 }
  const utoks = new Set(u.split(" ").filter(Boolean))
  const ltoks = new Set(l.split(" ").filter(Boolean))
  let overlap=0; for (const t of utoks){ if (ltoks.has(t)) overlap++ }
  score += Math.min(overlap,3)*0.25
  return score
}
function buildServiceChoiceListBySede(sedeKey, userMsg, category){
  const list = uniqueByLabel(servicesByCategory(sedeKey, category, userMsg))
  const scored = list.map(s=>({label:s.label, score:scoreServiceRelevance(userMsg, s.label)}))
  scored.sort((a,b)=>b.score-a.score)
  return scored.map((s,i)=>({ index:i+1, label:s.label }))
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

// ====== Square booking helpers & disponibilidad
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
async function searchAvailabilityForStaff({ locationKey, envServiceKey, staffId, fromEU, days=14, n=3, distinctDays=false }){
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

// ====== Cola & envío
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

// ====== Helpers chat
function isCancelIntent(text){
  const lower = norm(text)
  return /\b(cancelar|anular|borrar)\b/.test(lower) && /\b(cita|reserva|pr[oó]xima|mi)\b/.test(lower)
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

// ====== Etapas: servicio → días → horas
async function listServiceMenuOrAskCategory(sessionData, sock, jid, userMsg){
  // Si no hay categoría, pedirla
  if (!sessionData.category){
    sessionData.stage = "awaiting_category"
    saveSession(sessionData.customer_phone || "", sessionData)
    const msg = await aiRewrite("¿Qué te quieres hacer: *manicura*, *pedicura*, *pestañas*, *cejas*, *depilación*, *fotodepilación*, *micropigmentación*, *tratamiento facial*, *tratamiento corporal* u *otros*?")
    await sendWithPresence(sock, jid, msg)
    return false
  }
  // Si no hay sede, pedirla
  if (!sessionData.sede){
    sessionData.stage = "awaiting_sede_for_services"
    saveSession(sessionData.customer_phone || "", sessionData)
    const msg = await aiRewrite(`¿En qué sede te viene mejor, Torremolinos o La Luz? (para ${sessionData.category})`)
    await sendWithPresence(sock, jid, msg)
    return false
  }
  // Mostrar servicios de la categoría/sede
  const items = buildServiceChoiceListBySede(sessionData.sede, userMsg||"", sessionData.category)
  if (!items.length){
    const msg = await aiRewrite(`Ahora mismo no tengo servicios de ${sessionData.category} configurados para esa sede. Si quieres, dime el *nombre exacto* del servicio.`)
    await sendWithPresence(sock, jid, msg)
    return false
  }
  sessionData.serviceChoices = items
  sessionData.stage = "awaiting_service_choice"
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
  // Si hay profesional preferida válida, intentar por staff (distinctDays)
  if (sessionData.preferredStaffId && isStaffAllowedInLocation(sessionData.preferredStaffId, sessionData.sede)) {
    const s = await searchAvailabilityForStaff({ locationKey: sessionData.sede, envServiceKey: sessionData.selectedServiceEnvKey, staffId: sessionData.preferredStaffId, fromEU: baseFrom, n: daysWanted, distinctDays: true })
    slots = s
  }
  if (!slots.length){
    const g = await searchAvailabilityGeneric({ locationKey: sessionData.sede, envServiceKey: sessionData.selectedServiceEnvKey, fromEU: baseFrom, n: daysWanted, distinctDays: true })
    slots = g
  }
  if (!slots.length){
    // fallback: próximos 3 días hábiles
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
  let staffId = sessionData.lastProposeUsedPreferred ? (sessionData.preferredStaffId || sessionData.lastStaffByIso?.[iso] || null)
                                                    : (sessionData.lastStaffByIso?.[iso] || sessionData.preferredStaffId || null)

  if (staffId && !isStaffAllowedInLocation(staffId, sessionData.sede)) {
    staffId = null
  }
  if (!staffId) {
    const probe = await searchAvailabilityGeneric({ locationKey: sessionData.sede, envServiceKey: sessionData.selectedServiceEnvKey, fromEU: startEU.clone().subtract(1, "minute"), days: 1, n: 10 })
    const match = probe.find(x => x.date.isSame(startEU, "minute"))
    if (match?.staffId && isStaffAllowedInLocation(match.staffId, sessionData.sede)) staffId = match.staffId
  }
  if (!staffId) staffId = pickStaffForLocation(sessionData.sede, null)
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
    saveSession(phone, sessionData)
    await sendWithPresence(sock, jid, "Para terminar, dime tu *nombre* y (opcional) tu *email* para crear tu ficha 😊")
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
    const msg = await aiRewrite("No pude crear la reserva ahora. Nuestro equipo te contactará. ¿Quieres que te proponga otro horario?")
    await sendWithPresence(sock, jid, msg)
    return
  }

  if (result.booking.__sim) { await sendWithPresence(sock, jid, "🧪 SIMULACIÓN: Reserva creada exitosamente (modo prueba)"); clearSession(phone); return }

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

// ====== Mini-web + QR (solo una vez)
const app=express()
const PORT=process.env.PORT||8080
let lastQR=null, conectado=false
app.get("/", (_req,res)=>{
  const totalAppts = db.prepare(`SELECT COUNT(*) as count FROM appointments`).get()?.count || 0
  const successAppts = db.prepare(`SELECT COUNT(*) as count FROM appointments WHERE status = 'confirmed'`).get()?.count || 0
  const failedAppts = db.prepare(`SELECT COUNT(*) as count FROM appointments WHERE status = 'failed'`).get()?.count || 0
  res.send(`<!doctype html><meta charset="utf-8"><style>
  body{font-family:system-ui;display:grid;place-items:center;min-height:100vh;background:#f8f9fa}
  .card{max-width:720px;padding:32px;border-radius:20px;box-shadow:0 8px 32px rgba(0,0,0,.1);background:white}
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
    • Depilación, fotodepilación y micropigmentación separados.<br>
    • Días → horas (3 días más cercanos).<br>
    • "con {nombre}" persistente, sedes y lista completa de profesionales por centro.<br>
    • IA en todos los pasos.
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

// ====== Arranque del bot (una vez)
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
            ai_number_choice: null
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

          // ===== IA parse (pero no romper números si hay etapa pendiente)
          const aiParsed = await aiParseUserMessage(textRaw, sessionData)
          const chosenIndex = (()=>{ 
            if (aiParsed?.number_choice!=null) return Number(aiParsed.number_choice)
            const mnum = norm(textRaw).match(/^\s*([1-9]\d*)\s*$/); return mnum ? Number(mnum[1]) : null
          })()
          sessionData.ai_number_choice = chosenIndex
          // staff por texto libre
          const maybeStaff = parsePreferredStaffFromText(textRaw)
          if (maybeStaff){
            sessionData.preferredStaffId = maybeStaff.id
            sessionData.preferredStaffLabel = staffLabelFromId(maybeStaff.id)
          }
          // categ por IA si no hay
          if (!sessionData.category && aiParsed?.category) sessionData.category = aiParsed.category
          // sede por IA si no hay
          if (!sessionData.sede && aiParsed?.sede) sessionData.sede = aiParsed.sede

          // ===== Prioridades de etapas (no hacer dos preguntas a la vez)
          const hasPendingQuestion = !!sessionData.stage && /^awaiting_/.test(sessionData.stage)
          const isPlainNumber = /^\s*\d+\s*$/.test(textRaw)

          // Identidad: varias fichas
          if (sessionData.stage==="awaiting_identity_pick"){
            if (!chosenIndex){ await sendWithPresence(sock, jid, "Responde con el número de tu ficha (1, 2, …)."); return }
            const choice = (sessionData.identityChoices||[]).find(c=>c.index===chosenIndex)
            if (!choice){ await sendWithPresence(sock, jid, "No encontré esa opción. Prueba con el número de la lista."); return }
            sessionData.identityResolvedCustomerId = choice.id
            sessionData.stage = null
            saveSession(phone, sessionData)
            await sendWithPresence(sock, jid, "¡Gracias! Finalizo tu reserva…")
            await executeCreateBooking(sessionData, phone, sock, jid)
            return
          }
          // Identidad: crear nueva
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
            await executeCreateBooking(sessionData, phone, sock, jid)
            return
          }

          // Cancel intent directo (solo si no hay pregunta pendiente)
          if (!hasPendingQuestion && !isPlainNumber && isCancelIntent(textRaw)) {
            await executeCancelAppointment({}, sessionData, phone, sock, jid)
            return
          }

          // Intent listar (solo si no hay pregunta pendiente ni número suelto)
          if (!hasPendingQuestion && !isPlainNumber && aiParsed?.intent==="listar"){
            await executeListAppointments({}, sessionData, phone, sock, jid)
            return
          }

          // === Etapa: esperando categoría
          if (sessionData.stage === "awaiting_category"){
            const catToken = aiParsed?.category || (["manicura","pedicura","pestañas","cejas","depilación","fotodepilación","micropigmentación","tratamiento facial","tratamiento corporal","otros"].find(x => norm(textRaw).includes(norm(x))) || null)
            if (!catToken){
              const msg = await aiRewrite("Dime por favor: *manicura*, *pedicura*, *pestañas*, *cejas*, *depilación*, *fotodepilación*, *micropigmentación*, *tratamiento facial*, *tratamiento corporal* u *otros* 😊")
              await sendWithPresence(sock, jid, msg)
              saveSession(phone, sessionData)
              return
            }
            sessionData.category = catToken
            sessionData.stage = null
            saveSession(phone, sessionData)
            await listServiceMenuOrAskCategory(sessionData, sock, jid, textRaw)
            return
          }

          // === Etapa: esperando sede para servicios
          if (sessionData.stage==="awaiting_sede_for_services"){
            const sede = parseSede(textRaw) || aiParsed?.sede
            if (!sede){
              await sendWithPresence(sock, jid, "¿Prefieres *Torremolinos* o *La Luz*?")
              return
            }
            sessionData.sede = sede
            sessionData.stage = null
            saveSession(phone, sessionData)
            await listServiceMenuOrAskCategory(sessionData, sock, jid, textRaw)
            return
          }

          // === Etapa: esperando elección de servicio
          if (sessionData.stage==="awaiting_service_choice" && Array.isArray(sessionData.serviceChoices) && sessionData.serviceChoices.length){
            if (!chosenIndex){
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
            saveSession(phone, sessionData)

            // Si ya hay preferencia “con {nombre}” pero no es válida en sede, ofrecer lista completa
            if (sessionData.preferredStaffId && !isStaffAllowedInLocation(sessionData.preferredStaffId, sessionData.sede)){
              const all = Array.from(new Set(allowedStaffLabelsForLocation(sessionData.sede))).sort((a,b)=>a.localeCompare(b))
              sessionData.stage = "awaiting_staff_choice"
              sessionData.staffChoices = all.map((n,i)=>({index:i+1,label:n}))
              saveSession(phone, sessionData)
              const lines = all.map((n,i)=>`${i+1}) ${n}`).join("\n")
              const msg = await aiRewrite(`Esa profesional no atiende en ${locationNice(sessionData.sede)}. Puedo proponerte con:\n\n${lines}\n\nResponde con el número o di el nombre tal cual aparece.`)
              await sendWithPresence(sock, jid, msg)
              return
            }

            // Pedir días
            await proposeClosestDays({ sessionData, sock, jid })
            return
          }

          // === Etapa: elección de profesional explícita
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
                saveSession(phone, sessionData)
              } else {
                await sendWithPresence(sock, jid, "Esa profesional no está disponible en esa sede. Elige otra de la lista.")
                return
              }
            } else {
              // permitir “con {nombre}”
              const maybe = parsePreferredStaffFromText(textRaw)
              if (maybe && isStaffAllowedInLocation(maybe.id, sessionData.sede)) {
                sessionData.preferredStaffId = maybe.id
                sessionData.preferredStaffLabel = staffLabelFromId(maybe.id)
                sessionData.stage = null
                saveSession(phone, sessionData)
              } else {
                await sendWithPresence(sock, jid, "Elige una opción de la lista (1, 2, …) o di el nombre tal cual aparece.")
                return
              }
            }
            // tras fijar profesional, pedir días
            await proposeClosestDays({ sessionData, sock, jid })
            return
          }

          // === Etapa: elección de día
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

          // === Etapa: elección de hora
          if (sessionData.stage === "awaiting_time_choice" && Array.isArray(sessionData.lastHours) && sessionData.lastHours.length){
            if (!chosenIndex){
              await sendWithPresence(sock, jid, "Responde con el *número* del horario.")
              return
            }
            const pick = sessionData.lastHours[chosenIndex-1]
            if (!dayjs.isDayjs(pick)){ await sendWithPresence(sock, jid, "Elige un horario válido."); return }
            const iso = pick.format("YYYY-MM-DDTHH:mm")
            const staffFromIso = sessionData?.lastStaffByIso?.[iso] || null
            if (staffFromIso && !isStaffAllowedInLocation(staffFromIso, sessionData.sede)) {
              await sendWithPresence(sock, jid, "Esa hora ya no está disponible con esa profesional. Te paso otras opciones 👇")
              await proposeHoursForPickedDay({ sessionData, sock, jid, pickedDayIndex: 1 })
              return
            }
            sessionData.pendingDateTime = pick.tz(EURO_TZ).toISOString()
            if (staffFromIso){ 
              sessionData.preferredStaffId = staffFromIso
              sessionData.preferredStaffLabel = staffLabelFromId(staffFromIso)
            }
            sessionData.stage = null
            sessionData.ai_number_choice = null
            saveSession(phone, sessionData)
            await sendWithPresence(sock, jid, "Perfecto, voy a confirmar esa hora ✨")
            await executeCreateBooking(sessionData, phone, sock, jid)
            return
          }

          // === Si aún no hay categoría/servicio: pregunta inicial cercana
          if (!sessionData.selectedServiceEnvKey){
            // Si el usuario dijo que quiere con X y no hay sede o categoría, primero completar eso
            if (!hasPendingQuestion){
              // Si no categoría → preguntar categoría
              if (!sessionData.category){
                sessionData.stage = "awaiting_category"
                saveSession(phone, sessionData)
                const txt = "¡Hola! 😊 ¿Qué te apetece hacerte hoy? Tenemos manicura, pedicura, pestañas, cejas, depilación, fotodepilación, micropigmentación, tratamientos faciales y corporales. ¡Dime y te ayudo!"
                await sendWithPresence(sock, jid, txt)
                return
              }
              // Si no sede → preguntar sede
              if (!sessionData.sede){
                sessionData.stage = "awaiting_sede_for_services"
                saveSession(phone, sessionData)
                const msg = await aiRewrite(`¿En qué sede te viene mejor, Torremolinos o La Luz? (para ${sessionData.category})`)
                await sendWithPresence(sock, jid, msg)
                return
              }
              // Mostrar menú de servicios
              await listServiceMenuOrAskCategory(sessionData, sock, jid, textRaw)
              return
            }
          }

          // === Dudas no clasificables
          if (!hasPendingQuestion && aiParsed?.intent==="otro" && !isPlainNumber){
            const msg = await aiRewrite("Ahora mismo no puedo confirmarte eso. Cristina te contesta en cuanto pueda 🙏")
            await sendWithPresence(sock, jid, msg)
            return
          }

        } catch (error) {
          if (BOT_DEBUG) console.error(error)
          await sendWithPresence(sock, jid, "Disculpa, hubo un error técnico. ¿Puedes repetir tu mensaje?")
        }
      })
    })
  }catch(e){ 
    setTimeout(() => startBot().catch(console.error), 5000) 
  }
}

// ====== Arranque único
console.log(`🩷 Gapink Nails Bot v28.0.0`)
app.listen(PORT, ()=>{ console.log(`HTTP ${PORT}`) })
startBot().catch(console.error)

process.on("uncaughtException", (e)=>{ console.error("💥 uncaughtException:", e?.stack||e?.message||e) })
process.on("unhandledRejection", (e)=>{ console.error("💥 unhandledRejection:", e) })
process.on("SIGTERM", ()=>{ process.exit(0) })
process.on("SIGINT", ()=>{ process.exit(0) })
