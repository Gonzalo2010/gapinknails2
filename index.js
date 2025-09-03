// index.js — Gapink Nails · v32.2.0-smart (backend-first, “para tontos”, staff-first & auto-slots)
// Claves:
// - Si el user dice “con <nombre>” → detecta staff y MUESTRA HUECOS directamente.
// - Si no dice staff → MUESTRA HUECOS igualmente (equipo).
// - Si faltan sede/servicio → inferimos: IA mini barata + defaults (categoría “uñas”, “Semipermanente”).
// - Puede listar huecos de AMBOS salones si aún no hay sede (anotando la sede en cada línea).
// - Reserva = SOLO en DB con status “requested” (bloquea el hueco para otros).
// - Editar/Cancelar/Otras gestiones → mensaje corto y silenciar 6h.
// - Filtro de disponibilidad: Square Availability – pero filtrado por bloqueos en SQLite.
// - Elección por número; guardamos meta (sede/envKey/staff) por slot para poder bloquear bien.
//
// Nota: IA mini = clasificador simple para elegir 1 servicio de una lista. Temp 0.1, max_tokens 200.

// ====== Imports
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

// ====== Square (solo disponibilidad)
const square = new Client({
  accessToken: process.env.SQUARE_ACCESS_TOKEN,
  environment: (process.env.SQUARE_ENV==="production") ? Environment.Production : Environment.Sandbox
})
const LOC_TORRE = (process.env.SQUARE_LOCATION_ID_TORREMOLINOS || "").trim()
const LOC_LUZ   = (process.env.SQUARE_LOCATION_ID_LA_LUZ || "").trim()
const ADDRESS_TORRE = process.env.ADDRESS_TORREMOLINOS || "Av. de Benyamina 18, Torremolinos"
const ADDRESS_LUZ   = process.env.ADDRESS_LA_LUZ || "Málaga – Barrio de La Luz"

// ====== IA mini (barata)
const AI_PROVIDER = (process.env.AI_PROVIDER || (process.env.DEEPSEEK_API_KEY? "deepseek" : process.env.OPENAI_API_KEY? "openai" : "none")).toLowerCase()
const DEEPSEEK_API_KEY = process.env.DEEPSEEK_API_KEY || ""
const DEEPSEEK_MODEL = process.env.DEEPSEEK_MODEL || "deepseek-chat"
const OPENAI_API_KEY = process.env.OPENAI_API_KEY || ""
const OPENAI_MODEL = process.env.OPENAI_MODEL || "gpt-4o-mini"
const AI_TIMEOUT_MS = Number(process.env.AI_TIMEOUT_MS || 8000)
const aiTemp = 0.1
const aiMaxTokens = 200
const sleep = ms => new Promise(r=>setTimeout(r, ms))

async function aiChatMini(system, user){
  if (AI_PROVIDER==="none") return null
  const controller = new AbortController()
  const timeout = setTimeout(()=>controller.abort(), AI_TIMEOUT_MS)
  try{
    const messages = [
      system ? { role:"system", content: system } : null,
      { role:"user", content: user }
    ].filter(Boolean)
    if (AI_PROVIDER==="deepseek"){
      const resp = await fetch("https://api.deepseek.com/chat/completions",{
        method:"POST",
        headers:{ "Content-Type":"application/json", "Authorization":`Bearer ${DEEPSEEK_API_KEY}` },
        body: JSON.stringify({ model: DEEPSEEK_MODEL, messages, temperature:aiTemp, max_tokens:aiMaxTokens }),
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
        body: JSON.stringify({ model: OPENAI_MODEL, messages, temperature:aiTemp, max_tokens:aiMaxTokens }),
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
  let s = text.trim().replace(/```json/gi,"```")
  if (s.startsWith("```")) s = s.slice(3)
  if (s.endsWith("```")) s = s.slice(0,-3)
  s = s.trim()
  const i = s.indexOf("{"), j = s.lastIndexOf("}")
  if (i>=0 && j>i) s = s.slice(i, j+1)
  try{ return JSON.parse(s) }catch{ return null }
}

// Clasificador IA mini: elegir opción de una lista
async function aiPickFromList(userText, options/* array {index,label} */){
  if (!options?.length) return null
  const sys = `Clasifica y devuelve SOLO JSON {"pick":<n>|null}. Sin explicaciones.`
  const listado = options.map(o=>`${o.index}) ${o.label}`).join("\n")
  const prompt = `Mensaje: "${userText}"\nOpciones:\n${listado}\nJSON:`
  const out = await aiChatMini(sys, prompt)
  const obj = stripToJSON(out)
  if (obj && Number.isInteger(obj.pick)) return obj.pick
  return null
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
  location_key TEXT,
  service_env_key TEXT,
  service_label TEXT,
  duration_min INTEGER,
  start_iso TEXT,
  end_iso TEXT,
  staff_id TEXT,
  status TEXT,                 -- requested | confirmed | held | cancelled | failed
  created_at TEXT,
  note TEXT
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
CREATE INDEX IF NOT EXISTS idx_appt_time_loc ON appointments(location_key, start_iso);
CREATE INDEX IF NOT EXISTS idx_appt_status ON appointments(status);
`)
const insertAppt = db.prepare(`INSERT INTO appointments
(id,customer_name,customer_phone,location_key,service_env_key,service_label,duration_min,start_iso,end_iso,staff_id,status,created_at,note)
VALUES (@id,@customer_name,@customer_phone,@location_key,@service_env_key,@service_label,@duration_min,@start_iso,@end_iso,@staff_id,@status,@created_at,@note)`)

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

// ¿Está bloqueado este start en esta sede?
function isSlotTaken({ locationKey, startEU, durationMin=60 }){
  const startUtc = startEU.tz("UTC").toISOString()
  const endUtc = startEU.clone().add(durationMin,"minute").tz("UTC").toISOString()
  const rows = db.prepare(`
    SELECT 1 FROM appointments
    WHERE location_key=@k
      AND status IN ('requested','confirmed','held')
      AND NOT (end_iso <= @start OR start_iso >= @end)
    LIMIT 1
  `).all({ k:locationKey, start:startUtc, end:endUtc })
  return rows.length>0
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
  const msg = direction==="in" ? "hemos recibido este mensaje"
            : direction==="out" ? "le vamos a responder"
            : "evento"
  const payload = { action: action||null, direction, phone, intent, stage, raw_text, reply_text, time: Date.now(), extra }
  try{ console.log(JSON.stringify({ message:`${tag} ${msg}`, attributes:{...payload, level:"info", hostname:process.env.HOSTNAME||"local", pid:process.pid, timestamp:new Date().toISOString()}})) }catch{}
}

// ====== send wrapper
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

// ====== Empleadas
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
function isStaffAllowedInLocation(staffId){
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

// ====== Fuzzy staff
const NAME_ALIASES = [
  ["patri","patricia"],["patricia","patri"],
  ["cristi","cristina","cristy"],
  ["rocio chica","rociochica","rocio  chica","rocio c","rocio chica"],["rocio","rosio"],
  ["carmen belen","carmen","belen"],["tania","tani"],["johana","joana","yohana"],
  ["ganna","gana","ana","anna"], ["ginna","gina"],["chabely","chabeli","chabelí"],
  ["elisabeth","elisabet","elis"],["desi","desiree","desirée"],["daniela","dani"],
  ["jamaica","jahmaica"],["edurne","edur"],["sudemis","sude"],["maria","maría"],
  ["anaira","an aira"],["thalia","thalía","talia","talía"]
]
function findAliasCluster(token){ for (const arr of NAME_ALIASES){ if (arr.some(a=>token===a)) return arr } return null }
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

// ====== Categorías y defaults “para tontos”
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
const CAT_ALIASES = { "unas":"uñas","unias":"uñas","unyas":"uñas","depilacion":"depilación","micro":"micropigmentación","micropigmentacion":"micropigmentación","facial":"faciales","pestanas":"pestañas" }
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
// Default: si no hay pista, asumimos “uñas”, y dentro semipermanente
function pickDefaultServiceLabel(sedeKey, category="uñas"){
  const listAll = servicesForSedeKeyRaw(sedeKey).filter(s => (CATS[category] ? CATS[category](s, "") : true))
  if (!listAll.length) return null
  const prefer = [/semipermanente/i,/manicura/i,/pedicura/i,/gel/i,/acril/i]
  for (const rx of prefer){
    const f = listAll.find(s => rx.test(s.label))
    if (f) return f.label
  }
  return listAll[0].label
}

// ====== Availability (filtrando bloqueos locales)
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
    // filtro por bloqueos locales
    if (isSlotTaken({ locationKey, startEU:d, durationMin:60 })) continue
    const segs = Array.isArray(a.appointmentSegments) ? a.appointmentSegments
                 : Array.isArray(a.segments) ? a.segments : []
    const tm = segs[0]?.teamMemberId || null
    if (part){
      const start = d.clone().hour(part==="tarde"?15:part==="noche"?18:OPEN.start).minute(0)
      const end   = d.clone().hour(part==="mañana"?13:OPEN.end).minute(0)
      if (!(d.isSame(start,"day") && d.isAfter(start.subtract(1,"minute")) && d.isBefore(end.add(1,"minute")))) continue
    }
    out.push({ date:d, staffId: tm })
    if (out.length>=limit) break
  }
  out.sort((a,b)=>a.date.valueOf()-b.date.valueOf())
  return out
}

// ====== Parse temporal rápido
function parseTemporalPreference(text){
  const t = norm(text)
  const now = dayjs().tz(EURO_TZ)
  let when = null, part = null
  if (/\bhoy\b/.test(t)) when = now
  else if (/\bmanana\b/.test(t)) when = now.add(1,"day")
  else if (/\bpasado\b/.test(t)) when = now.add(2,"day")
  if (/\btarde\b/.test(t)) part="tarde"; else if (/\bnoche\b/.test(t)) part="noche"; else if (/\bpor la manana\b/.test(t)) part="mañana"
  const nextWeek = /\b(pr[oó]xima\s+semana|semana\s+que\s+viene)\b/i.test(t)
  return { when, part, nextWeek }
}

// ====== Propuestas de horas
function proposeLines(slots, ctxMap){ // ctxMap[iso] = {sede,staffId,envKey}
  const hoursEnum = enumerateHours(slots.map(s=>s.date))
  const lines = hoursEnum.map(h => {
    const c = ctxMap[h.iso] || {}
    const tagStaff = c.staffId ? ` — ${staffLabelFromId(c.staffId)}` : ""
    const tagSede  = c.sede ? ` · ${locationNice(c.sede)}` : ""
    return `${h.index}) ${h.pretty}${tagStaff}${tagSede}`
  }).join("\n")
  return { lines, hoursEnum }
}

// Proponer con sede+servicio ya resueltos (clásico)
async function proposeTimes(session, phone, sock, jid, {text=""}={}){
  const now = nowEU()
  const { when, part } = parseTemporalPreference(text)
  const baseFrom = nextOpeningFrom(now.add(NOW_MIN_OFFSET_MIN, "minute"))
  const startEU = (when||baseFrom).clone().hour(when?OPEN.start:baseFrom.hour()).minute(when?0:baseFrom.minute())
  const endEU   = when ? when.clone().hour(OPEN.end).minute(0) : baseFrom.clone().add(SEARCH_WINDOW_DAYS,"day")

  const rawSlots = await searchAvailWindow({
    locationKey: session.sede,
    envServiceKey: session.selectedServiceEnvKey,
    startEU, endEU, limit: 500, part
  })

  let slots = rawSlots
  let usedPreferred = false
  if (session.preferredStaffId){
    slots = rawSlots.filter(s => s.staffId === session.preferredStaffId)
    if (!slots.length) { slots = rawSlots; usedPreferred=false } else usedPreferred=true
  }
  if (!slots.length){
    await sendWithLog(sock, jid, `No veo huecos en ese rango. Prueba otra fecha o franja (p.ej. “viernes tarde”).`, {phone, intent:"no_slots", action:"guide"})
    return
  }
  const shown = slots.slice(0, SHOW_TOP_N)
  const ctx = {}
  for (const s of shown){
    const iso = s.date.format("YYYY-MM-DDTHH:mm")
    ctx[iso] = { sede: session.sede, staffId: s.staffId||null, envKey: session.selectedServiceEnvKey }
  }
  session.lastHours = shown.map(s=>s.date)
  session.lastMetaByIso = ctx
  session.lastProposeUsedPreferred = usedPreferred
  session.stage = "awaiting_time"
  saveSession(phone, session)

  const { lines } = proposeLines(shown, ctx)
  const header = usedPreferred
    ? `Huecos con ${session.preferredStaffLabel || "tu profe"} (top ${SHOW_TOP_N}):`
    : `Huecos del equipo (top ${SHOW_TOP_N}):`
  await sendWithLog(sock, jid, `${header}\n${lines}\n\nResponde con el número.`, {phone, intent:"times_list", action:"guide", stage:session.stage})
}

// Proponer aunque FALTE sede o envKey: busca en ambos salones con el label del servicio
async function proposeTimesSmart(session, phone, sock, jid, {text=""}={}){
  const now = nowEU()
  const { when, part } = parseTemporalPreference(text)
  const baseFrom = nextOpeningFrom(now.add(NOW_MIN_OFFSET_MIN, "minute"))
  const startEU = (when||baseFrom).clone().hour(when?OPEN.start:baseFrom.hour()).minute(when?0:baseFrom.minute())
  const endEU   = when ? when.clone().hour(OPEN.end).minute(0) : baseFrom.clone().add(SEARCH_WINDOW_DAYS,"day")

  // Si ya tenemos todo, usa la clásica
  if (session.sede && session.selectedServiceEnvKey){
    await proposeTimes(session, phone, sock, jid, {text})
    return
  }

  // Necesitamos al menos un label de servicio; si no, intenta inferir
  if (!session.selectedServiceLabel){
    const cat = session.category || parseCategory(text) || "uñas"
    session.category = cat
    // Intentamos IA global (labels únicos entre sedes)
    const labelsUniq = Array.from(new Set(
      allServices()
        .filter(s=> (CATS[cat]?CATS[cat](s, text):true))
        .map(s=>s.label)
    ))
    if (labelsUniq.length){
      const opts = labelsUniq.slice(0,22).map((lbl,i)=>({index:i+1,label:lbl}))
      const pick = await aiPickFromList(text||"servicio basico", opts)
      session.selectedServiceLabel = (pick ? opts.find(o=>o.index===pick)?.label : null)
        || pickDefaultServiceLabel("torremolinos", cat) || pickDefaultServiceLabel("la_luz", cat)
    } else {
      session.selectedServiceLabel = pickDefaultServiceLabel("torremolinos", cat) || pickDefaultServiceLabel("la_luz", cat)
    }
    saveSession(phone, session)
  }

  // Construir slots sumando sedes donde exista el servicio
  const sedeKeys = session.sede ? [session.sede] : ["torremolinos","la_luz"]
  const found = []
  for (const sedeKey of sedeKeys){
    let envKey = resolveEnvKeyFromLabelAndSede(session.selectedServiceLabel, sedeKey)
    if (!envKey){
      // fallback por default del category en esa sede
      const lblDef = pickDefaultServiceLabel(sedeKey, session.category||"uñas")
      if (lblDef) envKey = resolveEnvKeyFromLabelAndSede(lblDef, sedeKey)
    }
    if (!envKey) continue
    const raw = await searchAvailWindow({ locationKey:sedeKey, envServiceKey:envKey, startEU, endEU, limit:500, part })
    let list = raw
    if (session.preferredStaffId) list = raw.filter(s=>s.staffId===session.preferredStaffId)
    for (const s of list){
      found.push({ sede:sedeKey, envKey, date:s.date, staffId:s.staffId||null })
    }
  }

  if (!found.length){
    await sendWithLog(sock, jid, `No veo huecos ahora mismo. Dime otra fecha/franja (ej. “mañana tarde”).`, {phone, intent:"no_slots_smart", action:"guide"})
    return
  }
  found.sort((a,b)=>a.date.valueOf()-b.date.valueOf())
  const shown = found.slice(0, SHOW_TOP_N)
  const ctx = {}
  const dates = []
  for (const s of shown){
    const iso = s.date.format("YYYY-MM-DDTHH:mm")
    ctx[iso] = { sede:s.sede, staffId:s.staffId, envKey:s.envKey }
    dates.push(s.date)
  }
  session.lastHours = dates
  session.lastMetaByIso = ctx
  session.lastProposeUsedPreferred = !!session.preferredStaffId
  session.stage = "awaiting_time"
  saveSession(phone, session)

  const { lines } = proposeLines(shown.map(x=>({date:x.date})), ctx)
  const who = session.preferredStaffLabel ? ` con ${session.preferredStaffLabel}` : ""
  await sendWithLog(sock, jid, `Huecos${who} (top ${SHOW_TOP_N}):\n${lines}\n\nResponde con el número.`, {phone, intent:"times_list_smart", action:"guide", stage:session.stage})
}

// ====== Crear “reserva” (solo backend + bloqueo)
async function executeCreateBooking(session, phone, sock, jid){
  // Recuperar contexto del slot si vino de ambos salones
  const startEU = parseToEU(session.pendingDateTime)
  const iso = startEU.format("YYYY-MM-DDTHH:mm")
  const meta = (session.lastMetaByIso||{})[iso] || {}

  // Completar faltantes
  if (!session.sede && meta.sede) session.sede = meta.sede
  if (!session.selectedServiceEnvKey && meta.envKey) session.selectedServiceEnvKey = meta.envKey

  if (!session.sede){ await sendWithLog(sock, jid, "Falta *salón* (Torremolinos o La Luz).", {phone, intent:"missing_sede", action:"guide"}); return }
  if (!session.selectedServiceEnvKey){
    await sendWithLog(sock, jid, "Falta *servicio*. Dime qué quieres hacer (p. ej., “Semipermanente”).", {phone, intent:"missing_service", action:"guide"})
    return
  }
  if (!session.pendingDateTime){ await sendWithLog(sock, jid, "Falta *fecha y hora*.", {phone, intent:"missing_datetime", action:"guide"}); return }
  if (!insideBusinessHours(startEU, 60)){
    await sendWithLog(sock, jid, "Esa hora está fuera del horario (L–V 09:00–20:00).", {phone, intent:"outside_hours", action:"guide"}); return
  }

  // Bloqueo local
  if (isSlotTaken({ locationKey: session.sede, startEU, durationMin:60 })){
    await sendWithLog(sock, jid, "Ese hueco se acaba de ocupar. Te paso otras horas 👇", {phone, intent:"slot_taken", action:"guide"})
    await proposeTimesSmart(session, phone, sock, jid, { text:"" })
    return
  }

  let staffId = meta.staffId || session.preferredStaffId || pickStaffForLocation(session.sede, null)
  if (staffId && !isStaffAllowedInLocation(staffId)) staffId = pickStaffForLocation(session.sede, null)
  if (!staffId){ await sendWithLog(sock, jid,"No hay profesionales disponibles ahora mismo.",{phone, intent:"no_staff", action:"guide"}); return }

  const aptId = `apt_${Math.random().toString(36).slice(2,8)}${Date.now().toString(36).slice(-4)}`
  insertAppt.run({
    id: aptId,
    customer_name: session?.name || null,
    customer_phone: phone,
    location_key: session.sede,
    service_env_key: session.selectedServiceEnvKey,
    service_label: serviceLabelFromEnvKey(session.selectedServiceEnvKey) || session.selectedServiceLabel || "Servicio",
    duration_min: 60,
    start_iso: startEU.tz("UTC").toISOString(),
    end_iso: startEU.clone().add(60,"minute").tz("UTC").toISOString(),
    staff_id: staffId,
    status: "requested",
    created_at: new Date().toISOString(),
    note: null
  })

  const address = session.sede === "la_luz" ? ADDRESS_LUZ : ADDRESS_TORRE
  const svcLabel = serviceLabelFromEnvKey(session.selectedServiceEnvKey) || session.selectedServiceLabel || "Servicio"
  const staffLabel = staffLabelFromId(staffId) || "Equipo"

  const resumen = `📝 Solicitud de cita
📍 ${locationNice(session.sede)} — ${address}
🧾 ${svcLabel}
👩‍💼 ${staffLabel}
📅 ${fmtES(startEU)}
📞 ${phone}${session?.name?`\n👤 ${session.name}`:""}`

  await sendWithLog(sock, jid, `Listo ✅\nHe guardado tu *solicitud de cita*. Una empleada te confirmará en breve.\n\n${resumen}`, {phone, intent:"booking_requested", action:"confirm"})
  clearSession(phone)
}

// ====== Mensajes tipo / handoff
const HUMAN_WILL_HANDLE = "Ok. *Una empleada se pone con ello en un momento*. Gracias 🙏"

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

// ====== Flow WhatsApp
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
            greetedAt_ms:null, greeted:false,
            sede:null, category:null,
            selectedServiceEnvKey:null, selectedServiceLabel:null,
            preferredStaffId:null, preferredStaffLabel:null,
            pendingDateTime:null, lastHours:null,
            lastProposeUsedPreferred:false, stage:null,
            serviceChoices:null,
            lastMetaByIso:{},
            snooze_until_ms:null, name:null, email:null,
            lastUserText:null
          }

          const now = nowEU()

          // Silencios manuales
          if (isFromMe && /^[\s.·•⋅]+$/.test(textRaw)){
            session.snooze_until_ms = now.add(6,"hour").valueOf()
            saveSession(phone, session)
            logEvent({direction:"sys", action:"admin_snooze_6h", phone, raw_text:textRaw})
            return
          }
          if (isFromMe) { saveSession(phone, session); return }

          // IN log
          logEvent({direction:"in", action:"message", phone, raw_text:textRaw, stage:session.stage, extra:{isFromMe:false}})
          session.lastUserText = textRaw
          saveSession(phone, session)

          // Silencio 6h si el CLIENTE manda solo puntitos
          if (/^[\s.·•⋅]+$/.test(textRaw)){
            session.snooze_until_ms = now.add(6,"hour").valueOf()
            saveSession(phone, session)
            logEvent({direction:"sys", action:"snooze_6h", phone, raw_text:textRaw})
            return
          }
          if (session.snooze_until_ms && now.valueOf() < session.snooze_until_ms) { saveSession(phone, session); logEvent({direction:"sys", action:"snoozing_skip", phone}); return }

          // Saludo 24h
          const lastGreetAt = session.greetedAt_ms || 0
          if (!session.greeted || (Date.now()-lastGreetAt) > 24*60*60*1000){
            session.greeted=true; session.greetedAt_ms = Date.now(); saveSession(phone, session)
            await sendWithLog(sock, jid, "¡Hola! Soy el asistente de Gapink Nails 💅 Dime qué quieres y te paso huecos. Puedes decir “con Patri”, “mañana tarde”… y listo.", {phone, intent:"greeting_24h", action:"send_greeting"})
          }

          // Handoff a humano (editar/cancelar/otras)
          const tnorm=norm(textRaw)
          if ((/\b(cancel|anul|cambiar|mover|reprogram|editar|modificar)\b/.test(tnorm) && /\bcita\b/.test(tnorm))
              || /\b(queja|reclamo|factura|devoluci[oó]n|pago|presupuesto|precio|sugerencia|hablar|llamar|telefono|tel[eé]fono|email)\b/.test(tnorm)) {
            session.snooze_until_ms = now.add(6,"hour").valueOf()
            saveSession(phone, session)
            await sendWithLog(sock, jid, "Ok. *Una empleada se pone con ello en un momento* 🙏", {phone, intent:"human_management", action:"handoff"})
            return
          }

          // Staff → set y proponemos YA (smart)
          const fuzzy = fuzzyStaffFromText(textRaw)
          if (fuzzy){
            if (!fuzzy.anyTeam){
              session.preferredStaffId = fuzzy.id
              session.preferredStaffLabel = staffLabelFromId(fuzzy.id)
            } else {
              session.preferredStaffId = null
              session.preferredStaffLabel = null
            }
            saveSession(phone, session)
            await proposeTimesSmart(session, phone, sock, jid, { text:textRaw })
            return
          }

          // Si menciona fecha/franja sin más → igualmente proponemos (smart)
          if (/\bhoy\b|\bmanana\b|\bpasado\b|\blunes\b|\bmartes\b|\bmiercoles\b|\bjueves\b|\bviernes\b|\btarde\b|\bpor la manana\b|\bnoche\b/i.test(tnorm)){
            await proposeTimesSmart(session, phone, sock, jid, { text:textRaw })
            return
          }

          // Detectar sede/categoría si vienen
          const sedeMention = (/\b(luz|la luz)\b/.test(tnorm)) ? "la_luz" : (/\b(torre|torremolinos)\b/.test(tnorm) ? "torremolinos" : null)
          if (sedeMention){ session.sede = sedeMention; saveSession(phone, session) }
          const catMention = parseCategory(textRaw)
          if (catMention){ session.category = catMention; saveSession(phone, session) }

          // Si ya tenemos servicio y sede → proponer
          if (session.sede && session.selectedServiceEnvKey){
            await proposeTimes(session, phone, sock, jid, { text:textRaw })
            return
          }

          // En cualquier otro caso → proponer en modo smart (elige defaults si faltan cosas)
          await proposeTimesSmart(session, phone, sock, jid, { text:textRaw })

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

// ====== Selección por número (común para cualquier propuesta)
function attachSelectionHandler(){
  // Este helper no puede “enganchar” eventos aquí (lo hace el loop de arriba).
  // La lógica de selección ya está en el propio loop: cuando session.stage==="awaiting_time"
  // se procesa el número y se llama a executeCreateBooking().
}

// ====== Arranque
console.log(`🩷 Gapink Nails Bot v32.2.0-smart — Auto-huecos con/ sin staff, defaults & bloqueos`)
const appListen = app.listen(PORT, ()=>{ startBot().catch(console.error) })
process.on("uncaughtException", (e)=>{ console.error("💥 uncaughtException:", e?.stack||e?.message||e) })
process.on("unhandledRejection", (e)=>{ console.error("💥 unhandledRejection:", e) })
process.on("SIGTERM", ()=>{ try{ appListen.close(()=>process.exit(0)) }catch{ process.exit(0) } })
process.on("SIGINT", ()=>{ try{ appListen.close(()=>process.exit(0)) }catch{ process.exit(0) } })
