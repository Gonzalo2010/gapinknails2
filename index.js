// index.js — Gapink Bot · v33.0.0
// Fixes: NLU micropigmentación (autopick), sede no repetida, ASAP sólido,
// weekday próximo, paginación availability Square, duración real, alias de servicios por .env.
// Node 20+

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

// ===== Boot dayjs/crypto
if (!globalThis.crypto) globalThis.crypto = webcrypto
dayjs.extend(utc); dayjs.extend(tz); dayjs.locale("es")
const EURO_TZ = "Europe/Madrid"

// ===== Horario
const WORK_DAYS = [1,2,3,4,5]
const SLOT_MIN = 30
const OPEN = { start: 9, end: 20 }
const MORNING = { start: 9, end: 14 }
const AFTERNOON = { start: 15, end: 20 }
const NOW_MIN_OFFSET_MIN = Number(process.env.BOT_NOW_OFFSET_MIN || 30)
const HOLIDAYS_EXTRA = (process.env.HOLIDAYS_EXTRA || "06/01,28/02,15/08,12/10,01/11,06/12,08/12,25/12").split(",").map(s=>s.trim()).filter(Boolean)

// ===== Flags
const BOT_DEBUG = /^true$/i.test(process.env.BOT_DEBUG || "")
const SQUARE_MAX_RETRIES = Number(process.env.SQUARE_MAX_RETRIES || 3)
const DRY_RUN = /^true$/i.test(process.env.DRY_RUN || "")

// ===== Square
const square = new Client({
  accessToken: process.env.SQUARE_ACCESS_TOKEN,
  environment: (process.env.SQUARE_ENV==="production") ? Environment.Production : Environment.Sandbox
})
const LOC_TORRE = (process.env.SQUARE_LOCATION_ID_TORREMOLINOS || "").trim()
const LOC_LUZ   = (process.env.SQUARE_LOCATION_ID_LA_LUZ || "").trim()
const ADDRESS_TORRE = process.env.ADDRESS_TORREMOLINOS || "Av. de Benyamina 18, Torremolinos"
const ADDRESS_LUZ   = process.env.ADDRESS_LA_LUZ || "Málaga – Barrio de La Luz"

// ===== IA (DeepSeek)
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
      const body = { model: AI_MODEL, messages: system ? [{ role:"system", content: system }, ...messages] : messages, max_tokens: 500, temperature: 0.35, stream: false }
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
const STYLE_SYSTEM = `Escribe en WhatsApp español, cercano, directo, máx 2 emojis. Pide 1 dato cada vez. Si hay error: disculpa una vez y da alternativa concreta.`
async function aiRewrite(text){
  const out = await aiWithRetries([{ role:"user", content: `Reescribe humano y breve:\n"${text}"`}], STYLE_SYSTEM)
  return out || text
}
async function aiRewriteTone(kind, vars={}){
  const base = ({
    confirm:"Confirma cita alegre.",
    apology:"Disculpa y ofrece alternativa concreta.",
    askOne:"Pide un único dato.",
    offer:"Ofrece 2–3 opciones numeradas.",
    celebrate:"Celebra acción.",
    smalltalk:"Saluda/agradece breve."
  }[kind]) || "Responde cercano."
  const out = await aiWithRetries([{ role:"user", content: `${base}\nContexto:\n${JSON.stringify(vars)}\n1–2 frases, máx 2 emojis.` }], STYLE_SYSTEM)
  return out || (vars.fallback || "¡Listo!")
}

// ===== NLU planner
const NLU_SYSTEM = `
Devuelve SOLO JSON:
{"intent":"greet|smalltalk|thanks|help|choose_category|choose_sede|choose_professional|choose_service_by_index|choose_time_by_index|set_time_prefs|list_appointments|cancel_appointment|provide_identity|pick_identity_index|confirm_alt_yes|confirm_alt_no|direct_booking|asap|pause|unknown","entities":{"category":"uñas|pestañas|cejas|depilación|pedicura|tratamiento facial|tratamiento corporal|dental|null","service_text":"string|null","sede":"torremolinos|la_luz|null","professional_text":"string|null","index":"number|null","part_of_day":"morning|afternoon|null","week_target":"this|next|null","weekday":"0..6|null","date_iso":"YYYY-MM-DD|null","time_24h":"HH:mm|null","name":"string|null","email":"string|null"},"confidence":0..1}
- "micropigmentar/micropigmentación/microblading/ombre/powder/shading/hairstroke/nano" => category:"cejas", service_text:"micropigmentación cejas".
- "lo más pronto posible/asap/urgente/ya" => intent:"asap".
- "el lunes/martes/..." => set_time_prefs con weekday (0=dom..6=sáb).
- "." => pause.
- Si es un número y estamos eligiendo algo => choose_*_by_index.
`.trim()
const tryParseJSON = s => { try { return JSON.parse(s) } catch { return null } }
function repairJSONLoose(s){ if(!s) return null; let t=s.trim(); const i=t.indexOf("{"), j=t.lastIndexOf("}"); if(i>=0&&j>i) t=t.slice(i,j+1); t=t.replace(/[\r\n]/g," "); t=t.replace(/(['"])?([a-zA-Z0-9_]+)(['"])?:/g,'"$2":'); t=t.replace(/'/g,'"'); return tryParseJSON(t) }
async function aiPlanNLU(userText, sessionData){
  const staffHints = EMPLOYEES.map(e=>staffLabelFromId(e.id)).filter(Boolean)
  const ctx = { stage:sessionData?.stage||null, haveSede:!!sessionData?.sede, haveService:!!sessionData?.selectedServiceEnvKey, staff_hints:staffHints }
  const raw = await aiWithRetries([{ role:"user", content: `Usuario: ${userText}\nctx=${JSON.stringify(ctx)}\nJSON:` }], NLU_SYSTEM)
  const parsed = tryParseJSON(raw) || repairJSONLoose(raw)
  if (!parsed || typeof parsed!=="object") return null
  parsed.intent = String(parsed.intent||"unknown").toLowerCase()
  parsed.entities = parsed.entities || {}
  return parsed
}
const NLU_MIN_CONF = 0.4

// ===== Utils
const onlyDigits = s => String(s||"").replace(/\D+/g,"")
const rm = s => String(s||"").normalize("NFD").replace(/\p{Diacritic}/gu,"")
const norm = s => rm(s).toLowerCase().replace(/[+.,;:()/_-]/g," ").replace(/[^\p{Letter}\p{Number}\s]/gu," ").replace(/\s+/g," ").trim()
const clampInt = (n,min,max)=>Number.isFinite(n)?Math.max(min,Math.min(max,Math.trunc(n))):null
function applySpanishDiacritics(label){ let x=String(label||""); x=x.replace(/\bunas\b/gi, m => m[0] === 'U' ? 'Uñas' : 'uñas')
x=x.replace(/\bpestan(as?|)\b/gi, (m, suf) => (m[0]==='P'?'Pestañ':'pestañ') + (suf||'')); x=x.replace(/\bnivelacion\b/gi, m => m[0]==='N' ? 'Nivelación' : 'nivelación')
x=x.replace(/\bacrilic[oa]s?\b/gi, m => { const cap=m[0]===m[0].toUpperCase(); const plural=/s$/.test(m.toLowerCase()); const fem=/a/i.test(m.slice(-1)); const base=fem ? 'acrílica' : 'acrílico'; const out = base + (plural ? 's' : ''); return cap ? out[0].toUpperCase()+out.slice(1) : out })
x=x.replace(/\bfrances\b/gi, m => m[0]==='F' ? 'Francés' : 'francés'); x=x.replace(/\bmas\b/gi, (m) => (m[0]==='M' ? 'Más' : 'más'))
x=x.replace(/\bsemi ?permanente\b/gi, m => /[A-Z]/.test(m[0]) ? 'Semipermanente' : 'semipermanente'); x=x.replace(/\bninas\b/gi, 'niñas')
x=x.replace(/Esculpid(a|as)\b/gi, m=>{const cap=/[A-Z]/.test(m[0]); const suf=m.endsWith('as') ? 'as' : 'a'; return (cap?'E':'e') + 'sculpid' + suf}); return x }
function normalizePhoneES(raw){
  const d = onlyDigits(raw)
  if (!d) return null
  if (raw.startsWith("+") && d.length >= 8 && d.length <= 15) return `+${d}`
  if (d.startsWith("34") && d.length === 11) return `+${d}`
  if (d.length === 9) return `+34${d}`
  if (d.startsWith("00")) return `+${d.slice(2)}`
  return `+${d}`
}
const locationToId = key => key==="la_luz" ? LOC_LUZ : LOC_TORRE
const idToLocKey = id => id===LOC_LUZ ? "la_luz" : id===LOC_TORRE ? "torremolinos" : null
const locationNice = key => key==="la_luz" ? "Málaga – La Luz" : "Torremolinos"
function inPartOfDay(d, part){ const h=d.hour(); if (part==="morning") return h>=MORNING.start && h<MORNING.end; if (part==="afternoon") return h>=AFTERNOON.start && h<AFTERNOON.end; return true }

// ===== Horario helpers
function isHolidayEU(d){ const dd=String(d.date()).padStart(2,"0"); const mm=String(d.month()+1).padStart(2,"0"); return HOLIDAYS_EXTRA.includes(`${dd}/${mm}`) }
function insideBusinessHours(d, dur){
  const t=d.clone()
  if (!WORK_DAYS.includes(t.day())) return false
  if (isHolidayEU(t)) return false
  const end=t.clone().add(dur,"minute")
  if (!t.isSame(end,"day")) return false
  const startMin = t.hour()*60 + t.minute()
  const endMin = end.hour()*60 + end.minute()
  const openMin = OPEN.start*60
  const closeMin = OPEN.end*60
  return startMin >= openMin && endMin <= closeMin
}
function nextOpeningFrom(d){
  let t=d.clone()
  const nowMin=t.hour()*60 + t.minute()
  const openMin=OPEN.start*60
  const closeMin=OPEN.end*60
  if (nowMin < openMin) t = t.hour(OPEN.start).minute(0).second(0).millisecond(0)
  if (nowMin >= closeMin) t = t.add(1,"day").hour(OPEN.start).minute(0).second(0).millisecond(0)
  while (!WORK_DAYS.includes(t.day()) || isHolidayEU(t)) t = t.add(1,"day").hour(OPEN.start).minute(0).second(0).millisecond(0)
  return t
}
const fmtES = d => { const dias=["domingo","lunes","martes","miércoles","jueves","viernes","sábado"]; const t=(dayjs.isDayjs(d)?d:dayjs(d)).tz(EURO_TZ); return `${dias[t.day()]} ${String(t.date()).padStart(2,"0")}/${String(t.month()+1).padStart(2,"0")} ${String(t.hour()).padStart(2,"0")}:${String(t.minute()).padStart(2,"0")}` }
const enumerateHours = list => list.map((d,i)=>({ index:i+1, iso:d.format("YYYY-MM-DDTHH:mm"), pretty:fmtES(d) }))
const stableKey = parts => createHash("sha256").update(Object.values(parts).join("|")).digest("hex").slice(0,48)
function parseToEU(input){ if (dayjs.isDayjs(input)) return input.clone().tz(EURO_TZ); const s=String(input||""); if (/[Zz]|[+\-]\d{2}:?\d{2}$/.test(s)) return dayjs(s).tz(EURO_TZ); return dayjs.tz(s, EURO_TZ) }
const startOfNextWeekEU = base => base.clone().startOf("week").add(1,"day").add(7,"day").hour(OPEN.start).minute(0).second(0).millisecond(0)
const endOfWeekEU = from => from.clone().endOf("week")
function nextOccurrenceOfWeekday(baseEU, weekday){ // 0..6
  let t=baseEU.clone()
  const today=t.day()
  if (weekday===today){
    if (t.hour()>=OPEN.end) t=t.add(7,"day")
  } else {
    while (t.day()!==weekday) t=t.add(1,"day")
  }
  return t.startOf("day").hour(OPEN.start).minute(0).second(0).millisecond(0)
}

// ===== DB
const db=new Database("gapink.db"); db.pragma("journal_mode = WAL")
db.exec(`
CREATE TABLE IF NOT EXISTS appointments (id TEXT PRIMARY KEY, customer_name TEXT, customer_phone TEXT, customer_square_id TEXT, location_key TEXT, service_env_key TEXT, service_label TEXT, duration_min INTEGER, start_iso TEXT, end_iso TEXT, staff_id TEXT, status TEXT, created_at TEXT, square_booking_id TEXT, square_error TEXT, retry_count INTEGER DEFAULT 0);
CREATE TABLE IF NOT EXISTS sessions (phone TEXT PRIMARY KEY, data_json TEXT, updated_at TEXT);
CREATE TABLE IF NOT EXISTS ai_conversations (phone TEXT, message_id TEXT, user_message TEXT, ai_response TEXT, timestamp TEXT, session_data TEXT, ai_error TEXT, fallback_used BOOLEAN DEFAULT 0, PRIMARY KEY (phone, message_id));
CREATE TABLE IF NOT EXISTS square_logs (id INTEGER PRIMARY KEY AUTOINCREMENT, phone TEXT, action TEXT, request_data TEXT, response_data TEXT, error_data TEXT, timestamp TEXT, success BOOLEAN);
CREATE TABLE IF NOT EXISTS staff_aliases (alias_norm TEXT PRIMARY KEY, staff_id TEXT);
`)

// ===== Staff
function deriveLabelsFromEnvKey(envKey){
  const raw = envKey.replace(/^SQ_EMP_/,"")
  const toks = raw.split("_").map(t=>norm(t)).filter(Boolean)
  const uniq = [...new Set(toks)]
  const labels = [...uniq]; if (uniq.length>1) labels.push(uniq.join(" "))
  return labels
}
function parseEmployees(){
  const out=[]
  for (const [k,v] of Object.entries(process.env)){
    if (!k.startsWith("SQ_EMP_")) continue
    const [id,book,locs] = String(v||"").split("|")
    if (!id) continue
    const bookable = (book||"").toUpperCase()==="BOOKABLE"
    let allow = (locs||"").split(",").map(s=>s.trim()).filter(Boolean)
    const empKey = "EMP_CENTER_" + k.replace(/^SQ_EMP_/, "")
    const empVal = process.env[empKey]
    if (empVal){
      const centers = String(empVal).split(",").map(s=>s.trim().toLowerCase()).filter(Boolean)
      if (centers.some(c=>c==="all")) allow = ["ALL"]
      else {
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
const staffLabelFromId = id => EMPLOYEES.find(x=>x.id===id)?.labels?.[0] || (id?`Profesional ${String(id).slice(-4)}`:null)
function isStaffAllowedInLocation(staffId, locKey){ const e=EMPLOYEES.find(x=>x.id===staffId); if(!e||!e.bookable) return false; const locId=locationToId(locKey); return e.allow.includes("ALL")||e.allow.includes(locId) }
function pickStaffForLocation(locKey, preferId=null){
  const locId=locationToId(locKey)
  const isAllowed=e=>e.bookable&&(e.allow.includes("ALL")||e.allow.includes(locId))
  if (preferId){ const e=EMPLOYEES.find(x=>x.id===preferId); if (e&&isAllowed(e)) return e.id }
  const found = EMPLOYEES.find(isAllowed)
  return found?.id || null
}
function allowedStaffNamesForSede(locKey){
  const locId=locationToId(locKey)
  return EMPLOYEES.filter(e=>e.bookable&&(e.allow.includes("ALL")||e.allow.includes(locId))).map(e=>staffLabelFromId(e.id)).filter(Boolean)
}
function parseStaffAliasesFromEnv(){
  const map=new Map()
  const raw=(process.env.STAFF_ALIASES||"").trim()
  if (!raw) return map
  try{
    const obj = raw.startsWith("{") ? JSON.parse(raw) : {}
    for (const [a,id] of Object.entries(obj||{})){ if(a&&id) map.set(norm(a), String(id).trim()) }
  }catch{}
  return map
}
const STAFF_ENV_ALIAS_MAP = parseStaffAliasesFromEnv()
function buildStaffAliasIndex(){
  const map=new Map([...STAFF_ENV_ALIAS_MAP])
  const push=(alias,id)=>{ const a=norm(alias); if(a) map.set(a,id) }
  const shorty=s=>s.length>5?s.slice(0,5):s
  for (const e of EMPLOYEES){
    for (const lbl of e.labels){
      push(lbl,e.id)
      const parts=norm(lbl).split(" ").filter(Boolean)
      if(parts.length>1) push(parts.join(" "),e.id)
      for (const p of parts){ push(p,e.id); push(shorty(p),e.id) }
      if(/cristina|cristian|crist/.test(norm(lbl))) push("cris",e.id)
      if(/maria/.test(norm(lbl))){ push("maria",e.id); push("maría",e.id); push("mary",e.id) }
      if(/jose|josé/.test(norm(lbl))){ push("jose",e.id); push("josé",e.id); push("pepe",e.id) }
    }
  }
  const rows=db.prepare(`SELECT alias_norm,staff_id FROM staff_aliases`).all()
  for (const r of rows) map.set(r.alias_norm, r.staff_id)
  return map
}
let STAFF_ALIAS_INDEX = buildStaffAliasIndex()
function learnAlias(aliasRaw, staffId){
  const a=norm(aliasRaw); if(!a) return
  try{ db.prepare(`INSERT OR REPLACE INTO staff_aliases (alias_norm,staff_id) VALUES (@a,@id)`).run({a,id:staffId}) }catch{}
  STAFF_ALIAS_INDEX = buildStaffAliasIndex()
}
function lev(a,b){a=a||"";b=b||"";const m=a.length,n=b.length,dp=Array.from({length:m+1},()=>Array(n+1).fill(0));for(let i=0;i<=m;i++)dp[i][0]=i;for(let j=0;j<=n;j++)dp[0][j]=j;for(let i=1;i<=m;i++){for(let j=1;j<=n;j++){const c=a[i-1]===b[j-1]?0:1;dp[i][j]=Math.min(dp[i-1][j]+1,dp[i][j-1]+1,dp[i-1][j-1]+c)}}return dp[m][n]}
function findStaffByAliasToken(tokenNorm){
  const envId = STAFF_ALIAS_INDEX.get(tokenNorm)
  if (envId){
    const e = EMPLOYEES.find(x=>x.id===envId)
    if (e) return e
  }
  let best=null,score=99
  for (const e of EMPLOYEES){ for (const lbl of e.labels){ const s=lev(tokenNorm, norm(lbl)); if (s<score){score=s; best=e} } }
  return score<=2?best:null
}
function findStaffByFreeText(free){
  const t=norm(free||""); if(!t) return null
  const direct=findStaffByAliasToken(t); if(direct) return direct
  const tokens=t.split(" ").filter(Boolean).slice(0,3)
  for (const tok of tokens){ const h=findStaffByAliasToken(tok); if(h) return h }
  return null
}

// ===== Servicios + Aliases por .env
const titleCase = str => String(str||"").toLowerCase().replace(/\b([a-z])/g, m=>m.toUpperCase())
const cleanDisplayLabel = label => applySpanishDiacritics(String(label||"").replace(/^\s*(luz|la\s*luz)\s+/i,"").trim())
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

// Aliases por .env (flex): 
// SVC_ALIAS_JSON = {"torremolinos":{"micropigmentar":"SQ_SVC_MICRO_TORRE"},"la_luz":{"micropigmentar":"SQ_SVC_luz_MICRO"}}
// ó plano: {"micropigmentar":"SQ_SVC_MICRO_TORRE"} (se aplica a ambas si existe).
let SVC_ALIAS = {}
try{ SVC_ALIAS = JSON.parse(process.env.SVC_ALIAS_JSON||"{}") }catch{ SVC_ALIAS = {} }
function resolveServiceByEnvAlias(sedeKey, userMsg){
  const t = norm(userMsg||"")
  const local = (typeof SVC_ALIAS[sedeKey]==="object") ? SVC_ALIAS[sedeKey] : {}
  // primero sede → envKey
  for (const [alias, envKey] of Object.entries(local)){ if (t.includes(norm(alias))) return envKey }
  // luego plano
  if (typeof SVC_ALIAS==="object"){
    for (const [alias, envKey] of Object.entries(SVC_ALIAS)){ if (typeof envKey==="string" && t.includes(norm(alias))) return envKey }
  }
  return null
}

// — Categorización y ranking cejas
const DEPIL_POS = ["depil","cera","cerado","fotodepil","láser","laser","ipl","fotodep","hilo","wax"]
const DEPIL_ALIAS_ZONES = ["pierna","piernas","axila","axilas","pubis","perianal","ingle","ingles","bikini","brazos","espalda","labio","facial","ceja","cejas","mentón","patillas","abdomen","pecho","hombros","nuca","glúteos"]
function isDepilLabel(lbl){ const has=DEPIL_POS.some(a=>lbl.includes(norm(a))); if(!has) return false; if(/\buñ|manicura|gel|acril|pestañ|eyelash|lash\b/.test(lbl)) return false; const zone=DEPIL_ALIAS_ZONES.some(z=>lbl.includes(norm(z))); return has||zone }
function depilacionServicesForSede(sedeKey){ const list=servicesForSedeKeyRaw(sedeKey); return uniqueByLabel(list.filter(s=>isDepilLabel(s.norm))) }
const POS_NAIL_ANCHORS = ["uña","unas","uñas","manicura","gel","acrilic","acrilico","acrílico","semi","semipermanente","esculpida","esculpidas","press on","press-on","tips","francesa","frances","baby boomer","encapsulado","encapsulados","nivelacion","nivelación","esmaltado","esmalte"]
const NEG_NOT_NAILS   = ["pesta","pestañ","ceja","cejas","ojos","pelo a pelo","eyelash"]
const PEDI_RE = /\b(pedicur\w*|pies?)\b/i
function detectCategory(text){
  const t=norm(text||"")
  if (/\b(blanqueamient|dental|dentista|odontolog)\b/.test(t)) return "dental"
  if (/\b(ceja|cejas|brow|henna|laminad|perfilad|microblad|microshad|hairstroke|polvo|powder|ombr[eé]|micropigment|pigmentaci[oó]n|nano)\b/.test(t)) return "cejas"
  if (/\b(pesta|pestañ|eyelash|lifting|lash|volumen|2d|3d|mega|megavolumen|tinte|rizado)\b/.test(t)) return "pestañas"
  if (/(^|\W)(depil|depilar|depilarme|fotodepil|laser|láser|ipl|cera)(\W|$)/.test(t)) return "depilación"
  if (/\b(pedicur|pies)\b/.test(t)) return "pedicura"
  if (/\b(tratamiento facial|higiene facial|radiofrecuencia|peeling|facial|limpieza facial)\b/.test(t)) return "tratamiento facial"
  if (/\b(tratamiento corporal|maderoterapia|drenaje|corporal|anticelulit|reafirmante)\b/.test(t)) return "tratamiento corporal"
  if (POS_NAIL_ANCHORS.some(a=>t.includes(norm(a))) || /\buñas?\b/.test(t)) return "uñas"
  return null
}
const shouldIncludePedicure = userMsg => PEDI_RE.test(String(userMsg||""))
function isNailsLabel(labelNorm, allowPed){ if (NEG_NOT_NAILS.some(n=>labelNorm.includes(norm(n)))) return false; const has = POS_NAIL_ANCHORS.some(p=>labelNorm.includes(norm(p))) || /uñ|manicura|gel|acril|semi/.test(labelNorm); const isPedi = PEDI_RE.test(labelNorm); if (isPedi && !allowPed) return false; return has }
function uniqueByLabel(arr){ const seen=new Set(), out=[]; for (const s of arr){ const k=s.label.toLowerCase(); if (seen.has(k)) continue; seen.add(k); out.push(s) } return out }

const MICRO_TOKENS = ["micropigment","pigmentaci","microblad","ombre","powder","shad","hairstroke","nano"]
function queryScoreCejas(label, userMsg){
  const L=norm(label), Q=norm(userMsg||"")
  let score=0
  for (const tk of MICRO_TOKENS){ if (L.includes(tk)) score+=3; if (Q.includes(tk)) score+=6 }
  if (/depil/.test(L)) score-=2
  return score
}
function resolveEnvKeyFromLabelAndSede(label, sedeKey){
  const list = servicesForSedeKeyRaw(sedeKey)
  return list.find(s=>s.label.toLowerCase()===String(label||"").toLowerCase())?.key || null
}
function resolveEnvKeyFuzzy(label, sedeKey){
  const list = servicesForSedeKeyRaw(sedeKey)
  const L = norm(label)
  const hit = list.find(s => s.norm.includes(L) || norm(s.label).includes(L))
  return hit?.key || null
}
function servicesByCategory(sedeKey, category, userMsg){
  const c=(category||"").toLowerCase()
  switch (c){
    case "depilación": {
      const filtered = depilacionServicesForSede(sedeKey)
      return filtered.map((s,i)=>({ index:i+1, label:s.label }))
    }
    case "uñas": {
      const list = servicesForSedeKeyRaw(sedeKey)
      const filtered = list.filter(s=>isNailsLabel(s.norm, shouldIncludePedicure(userMsg)))
      return uniqueByLabel(filtered).map((s,i)=>({ index:i+1, label:s.label }))
    }
    case "cejas": {
      const list = uniqueByLabel(servicesForSedeKeyRaw(sedeKey))
      // rankea micropigmentación arriba
      const sorted = [...list].sort((a,b)=>queryScoreCejas(b.label,userMsg)-queryScoreCejas(a.label,userMsg))
      return sorted.map((s,i)=>({ index:i+1, label:s.label }))
    }
    case "pedicura":
    case "pestañas":
    case "tratamiento facial":
    case "tratamiento corporal":
    case "dental": {
      const list = uniqueByLabel(servicesForSedeKeyRaw(sedeKey))
      return list.map((s,i)=>({ index:i+1, label:s.label }))
    }
    default: return []
  }
}

// ===== Square helpers
function safeJSONStringify(value){
  const seen = new WeakSet()
  try{
    return JSON.stringify(value, (_k,v)=>{
      if (typeof v === "bigint") return v.toString()
      if (typeof v === "object" && v!==null){ if (seen.has(v)) return "[Circular]"; seen.add(v) }
      return v
    })
  }catch{ try { return String(value) }catch{ return "[Unserializable]" } }
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
    await sock.sendMessage(jid, { text: await aiRewriteTone("askOne", { need:"tu *nombre completo* y (opcional) tu *email*", fallback:"Para terminar, dime tu *nombre completo* y (opcional) tu *email* 😊" }) })
    return { status:"need_new" }
  }
  const choices = matches.map((c,i)=>({ index:i+1, id:c.id, name:c?.givenName || "Sin nombre", email:c?.emailAddress || "—" }))
  sessionData.identityChoices = choices
  sessionData.stage = "awaiting_identity_pick"
  saveSession(phone, sessionData)
  const lines = choices.map(ch => `${ch.index}) ${ch.name} ${ch.email!=="—" ? `(${ch.email})`:""}`).join("\n")
  await sock.sendMessage(jid, { text: await aiRewriteTone("offer", { options: `He encontrado varias fichas con tu número.\n\n${lines}\n\nResponde con el número.`, fallback:`He encontrado varias fichas con tu número.\n\n${lines}\n\nResponde con el número.` }) })
  return { status:"need_pick" }
}
async function findOrCreateCustomerWithRetry({ name, email, phone }){
  let last=null
  for (let attempt=1; attempt<=SQUARE_MAX_RETRIES; attempt++){
    try{
      const e164=normalizePhoneES(phone); if(!e164) return null
      const got=await square.customersApi.searchCustomers({ query:{ filter:{ phoneNumber:{ exact:e164 } } } })
      const c=(got?.result?.customers||[])[0]; if (c) return c
      const created = await square.customersApi.createCustomer({
        idempotencyKey:`cust_${Date.now()}_${Math.random().toString(36).slice(2,6)}`,
        givenName:name||undefined, emailAddress:email||undefined, phoneNumber:e164||undefined
      })
      const newCustomer = created?.result?.customer || null
      if (newCustomer) return newCustomer
    }catch(e){ last=e; if (attempt<SQUARE_MAX_RETRIES) await sleep(1000*attempt) }
  }
  if (BOT_DEBUG) console.error("findOrCreateCustomerWithRetry failed", last?.message)
  return null
}
async function getServiceIdAndVersion(envKey){
  const raw = process.env[envKey]; if (!raw) return null
  let [id, ver] = String(raw).split("|"); ver=ver?Number(ver):null
  if (!id) return null
  if (!ver){
    try{ const resp=await square.catalogApi.retrieveCatalogObject(id,true); const vRaw=resp?.result?.object?.version; ver = vRaw!=null ? Number(vRaw) : 1 }catch(e){ ver=1 }
  }
  return { id, version: ver||1 }
}

// == Square availability con paginación y duración real
function extractSegments(obj){
  // Square puede devolver "appointmentSegments" o "segments"
  if (Array.isArray(obj?.appointmentSegments)) return obj.appointmentSegments
  if (Array.isArray(obj?.segments)) return obj.segments
  return []
}
async function searchAvailabilityAPI(body, limit=50, maxPages=6){
  let cursor=null, all=[]
  for (let i=0;i<maxPages;i++){
    const req={ query:{...body.query}, limit }
    if (cursor) req.cursor = cursor
    const resp = await square.bookingsApi.searchAvailability(req)
    const avail = resp?.result?.availabilities || []
    all = all.concat(avail)
    cursor = resp?.result?.cursor || null
    if (!cursor || all.length >= 400) break
  }
  return all
}
function segDurationOrDefault(av){ // minutos
  const segs = extractSegments(av)
  const d = Number(segs?.[0]?.durationMinutes||segs?.[0]?.duration_minutes||0)
  return d>0 ? d : 60
}

async function searchAvailabilityForStaff({ locationKey, envServiceKey, staffId, fromEU, days=14, n=6, distinctDays=false }){
  try{
    const sv = await getServiceIdAndVersion(envServiceKey); if (!sv?.id||!staffId) return []
    const startAt = fromEU.tz("UTC").toISOString()
    const endAt = fromEU.clone().add(days,"day").tz("UTC").toISOString()
    const locationId = locationToId(locationKey)
    const body = { filter:{ startAtRange:{ startAt, endAt }, locationId, segmentFilters:[{ serviceVariationId: sv.id, teamMemberIdFilter:{ any:[ staffId ] } }] } }
    const avail = await searchAvailabilityAPI({ query: body }, 75, 8)
    const slots=[], seenDays=new Set()
    for (const a of avail){
      if (!a?.startAt) continue
      const d = dayjs(a.startAt).tz(EURO_TZ)
      const dur = segDurationOrDefault(a)
      if (!insideBusinessHours(d, dur)) continue
      if (distinctDays){ const key=d.format("YYYY-MM-DD"); if (seenDays.has(key)) continue; seenDays.add(key) }
      if (!isStaffAllowedInLocation(staffId, locationKey)) continue
      slots.push({ date:d, staffId }); if (slots.length>=n) break
    }
    return slots
  }catch{ return [] }
}
async function searchAvailabilityGeneric({ locationKey, envServiceKey, fromEU, days=14, n=6, distinctDays=false }){
  try{
    const sv = await getServiceIdAndVersion(envServiceKey); if (!sv?.id) return []
    const startAt = fromEU.tz("UTC").toISOString()
    const endAt = fromEU.clone().add(days,"day").tz("UTC").toISOString()
    const locationId = locationToId(locationKey)
    const body = { filter:{ startAtRange:{ startAt, endAt }, locationId, segmentFilters:[{ serviceVariationId: sv.id }] } }
    const avail = await searchAvailabilityAPI({ query: body }, 75, 8)
    const slots=[], seenDays=new Set()
    for (const a of avail){
      if (!a?.startAt) continue
      const d = dayjs(a.startAt).tz(EURO_TZ)
      const segs = extractSegments(a)
      const dur = segDurationOrDefault(a)
      if (!insideBusinessHours(d, dur)) continue
      let tm = segs?.[0]?.teamMemberId || null
      if (distinctDays){ const key=d.format("YYYY-MM-DD"); if (seenDays.has(key)) continue; seenDays.add(key) }
      if (tm && !isStaffAllowedInLocation(tm, locationKey)) continue
      slots.push({ date:d, staffId: tm || null }); if (slots.length>=n) break
    }
    return slots
  }catch{ return [] }
}
async function searchNearby({ locationKey, envServiceKey, targetEU, staffId=null, windowMin=90, max=4 }){
  const dayStart = targetEU.clone().startOf("day")
  const cand = staffId
    ? await searchAvailabilityForStaff({ locationKey, envServiceKey, staffId, fromEU:dayStart, days:1, n:80 })
    : await searchAvailabilityGeneric({ locationKey, envServiceKey, fromEU:dayStart, days:1, n:120 })
  return cand.filter(s=>Math.abs(s.date.diff(targetEU,"minute"))<=windowMin)
             .sort((a,b)=>Math.abs(a.date.diff(targetEU))-Math.abs(b.date.diff(a.date)))
             .slice(0, max)
}

// ===== Parse natural fallback
function parsePartOfDay(text){ const t=norm(text); if(/\b(mañana|manana|por la manana|por la mañana|primeras horas)\b/.test(t)) return "morning"; if(/\b(tarde|por la tarde|después de comer|despues de comer)\b/.test(t)) return "afternoon"; return null }
function parseWeekTarget(text){ const t=norm(text); if(/\b(semana que viene|proxima semana|pr[oó]xima semana|la proxima|la próxima)\b/.test(t)) return "next"; if(/\b(esta semana|esta|hoy|manana|mañana)\b/.test(t)) return "this"; return null }
function parseWeekday(text){ const t=norm(text); const map={"domingo":0,"lunes":1,"martes":2,"miercoles":3,"miércoles":3,"jueves":4,"viernes":5,"sabado":6,"sábado":6}; for (const [k,v] of Object.entries(map)) if (new RegExp(`\\b${k}\\b`).test(t)) return v; return null }
const isNegativeIntent = text => /\b(me viene mal|no puedo|esa no|me va fatal|no me cuadra)\b/.test(norm(text))
const isYes = text => /\b(si|sí|vale|ok|confirm|confirmo|perfecto|me vale|me viene bien)\b/.test(norm(text))
const isNo  = text => /\b(no|prefiero otra|mejor no|cambiar|otra hora|otra pro|ver huecos)\b/.test(norm(text))
function parseSede(text){ const t=norm(text||""); if (/\b(la luz|luz|malaga|málaga)\b/.test(t)) return "la_luz"; if (/\b(torre|torremolinos)\b/.test(t)) return "torremolinos"; return null }
function parseNameEmailFromText(txt){ const m=String(txt||"").match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i); const email=m?m[0]:null; const name=String(txt||"").replace(email||"","").replace(/(email|correo)[:\s]*/ig,"").trim(); return { name: name || null, email } }
function parsePreferredStaffFromText(text){ const t=norm(text||""); const m=t.match(/\bcon\s+([a-zñáéíóú]+(?:\s+[a-zñáéíóú]+){0,2})\b/i); if(!m) return null; let phrase=m[1]||""; phrase=phrase.replace(/\b(la|el)\b\s+/g,"").trim(); return findStaffByFreeText(phrase) }
const parseASAP = text => /\b(cuanto antes|lo mas pronto|lo más pronto|asap|lo antes posible|urgente|ya|ahora mismo)\b/.test(norm(text))

// ===== Menús/propuestas
async function executeChooseService(params, sessionData, phone, sock, jid, userMsg){
  const incomingCat = params?.category || sessionData.category || sessionData.pendingCategory || detectCategory(userMsg)
  const VALID = ["uñas","pestañas","cejas","depilación","pedicura","tratamiento facial","tratamiento corporal","dental"]
  if (!incomingCat || !VALID.includes(incomingCat)){
    sessionData.stage = "awaiting_category"; saveSession(phone, sessionData)
    await sendWithPresence(sock, jid, await aiRewriteTone("askOne", { need: "¿qué te quieres hacer: *uñas*, *pestañas*, *cejas*, *depilación* o *blanqueamiento dental*?" }))
    return
  }
  if (!sessionData.sede){
    sessionData.pendingCategory = incomingCat; sessionData.stage = "awaiting_sede_for_services"; saveSession(phone, sessionData)
    await sendWithPresence(sock, jid, await aiRewriteTone("askOne", { need: "¿*Torremolinos* o *La Luz*?" }))
    return
  }

  // Autopick por alias de servicio (p. ej. micropigmentar)
  let aliasEK = resolveServiceByEnvAlias(sessionData.sede, userMsg||incomingCat)
  if (!aliasEK && incomingCat==="cejas"){
    // si el texto suena a micropigmentación, intenta el mejor match del catálogo
    const list = servicesForSedeKeyRaw(sessionData.sede)
    const scored = list.map(s=>({ s, score: queryScoreCejas(s.label, userMsg||"") })).sort((a,b)=>b.score-a.score)
    if (scored[0]?.score >= 6) aliasEK = scored[0].s.key
  }
  if (aliasEK){
    sessionData.selectedServiceEnvKey = aliasEK
    sessionData.selectedServiceLabel = serviceLabelFromEnvKey(aliasEK) || "Servicio"
    sessionData.stage = "awaiting_time_prefs"
    saveSession(phone, sessionData)
    await sendWithPresence(sock, jid, await aiRewriteTone("askOne", { need:"¿te viene mejor *mañana* o *tarde*? ¿Y *esta semana* o *la próxima*?" }))
    return
  }

  let items = servicesByCategory(sessionData.sede, incomingCat, userMsg||"")
  if (!items.length && incomingCat==="depilación"){
    const DEFAULT_DEPIL=["Depilación Cejas","Depilación Labio Superior","Depilación Axilas","Depilación Ingles","Depilación Piernas Medias","Depilación Piernas Completas"]
    items = DEFAULT_DEPIL.map((label,i)=>({ index:i+1, label }))
  }
  if (!items.length){
    const all = servicesForSedeKeyRaw(sessionData.sede)
    items = uniqueByLabel(all).slice(0, 20).map((s,i)=>({ index:i+1, label:s.label }))
  }
  if (!items.length){
    await sendWithPresence(sock, jid, await aiRewriteTone("apology", { fallback:"No puedo listar el menú ahora. Si quieres, dime el *nombre exacto* del servicio." }))
    return
  }

  sessionData.category = incomingCat
  sessionData.serviceChoices = items
  sessionData.stage = "awaiting_service_choice"
  saveSession(phone, sessionData)

  const lines = items.map(it=>`${it.index}) ${applySpanishDiacritics(it.label)}`).join("\n")
  await sendWithPresence(sock, jid, await aiRewriteTone("offer", { options:`Opciones de **${incomingCat}** en ${locationNice(sessionData.sede)}:\n\n${lines}\n\nResponde con el número.` }))
}

async function proposeWeekOptions(sessionData, phone, sock, jid, { forcePreferred=false, maxOptions=3 } = {}){
  const nowEU = dayjs().tz(EURO_TZ)
  const weekTarget = sessionData.targetWeek || "this"
  const fromEU = (weekTarget==="next") ? startOfNextWeekEU(nowEU) : nextOpeningFrom(nowEU.add(NOW_MIN_OFFSET_MIN,"minute"))
  const endEU = endOfWeekEU(fromEU)
  const baseDays = endEU.diff(fromEU, "day") + 1
  const days = Math.min(14, Math.max(1, baseDays))
  let slots=[]; let usedPreferred=false

  const filterPart = arr => sessionData.preferredPartOfDay ? arr.filter(s=>inPartOfDay(s.date, sessionData.preferredPartOfDay)) : arr
  const filterWeekday = arr => Number.isInteger(sessionData.preferredWeekday) ? arr.filter(s=>s.date.day()===sessionData.preferredWeekday) : arr

  if (sessionData.preferredStaffId && isStaffAllowedInLocation(sessionData.preferredStaffId, sessionData.sede)){
    const withPro = await searchAvailabilityForStaff({ locationKey: sessionData.sede, envServiceKey: sessionData.selectedServiceEnvKey, staffId: sessionData.preferredStaffId, fromEU, days, n: 40, distinctDays:false })
    let filt = filterWeekday(filterPart(withPro))
    if (!filt.length && forcePreferred) filt = filterWeekday(withPro)
    if (filt.length){ slots = filt.slice(0, maxOptions); usedPreferred = true }
    else if (forcePreferred){
      await sendWithPresence(sock, jid, await aiRewriteTone("apology", { fallback:`No veo huecos con ${sessionData.preferredStaffLabel} ${weekTarget==="next"?"la próxima semana":"esta semana"}. ¿Te muestro *cualquier profesional* o miramos *otra semana*?` }))
      return
    }
  }

  if (!slots.length && !forcePreferred){
    const generic = await searchAvailabilityGeneric({ locationKey: sessionData.sede, envServiceKey: sessionData.selectedServiceEnvKey, fromEU, days, n: 80, distinctDays:false })
    const filt = filterWeekday(filterPart(generic))
    slots = (filt.length?filt:generic).slice(0, maxOptions)
  }

  if (!slots.length){
    await sendWithPresence(sock, jid, await aiRewriteTone("apology", { fallback:`No veo huecos ${weekTarget==="next"?"la próxima semana":"esta semana"} en esa franja. ¿Probamos *otra franja* u *otra semana*?` }))
    return
  }

  const hoursEnum = enumerateHours(slots.map(s=>s.date))
  const map = {}; for (const s of slots) map[s.date.format("YYYY-MM-DDTHH:mm")] = s.staffId || null
  sessionData.lastHours = slots.map(s=>s.date)
  sessionData.lastStaffByIso = map
  sessionData.lastProposeUsedPreferred = usedPreferred
  sessionData.lastProposeScope = "week"
  sessionData.lastWeekFromISO = fromEU.toISOString()
  sessionData.stage = "awaiting_time"
  saveSession(phone, sessionData)

  const lines = hoursEnum.map(h=>`${h.index}) ${h.pretty}${map[h.iso]?` — ${staffLabelFromId(map[h.iso])}`:""}`).join("\n")
  const hdr = usedPreferred ? `Huecos de ${sessionData.preferredStaffLabel} ${weekTarget==="next"?"(próxima semana)":""}:` : `Tengo disponibles ${weekTarget==="next"?"la próxima semana":"esta semana"}:`
  await sendWithPresence(sock, jid, await aiRewriteTone("offer", { options:`${hdr}\n${lines}\n\nResponde solo con el número.` }))
}
async function proposeEarliestOptions(sessionData, phone, sock, jid, { maxOptions=3 }={}){
  const fromEU = nextOpeningFrom(dayjs().tz(EURO_TZ).add(NOW_MIN_OFFSET_MIN,"minute"))
  let slots=[]
  if (sessionData.preferredStaffId && isStaffAllowedInLocation(sessionData.preferredStaffId, sessionData.sede)){
    slots = await searchAvailabilityForStaff({ locationKey: sessionData.sede, envServiceKey: sessionData.selectedServiceEnvKey, staffId: sessionData.preferredStaffId, fromEU, days:14, n:120 })
  }
  if (!slots.length){
    slots = await searchAvailabilityGeneric({ locationKey: sessionData.sede, envServiceKey: sessionData.selectedServiceEnvKey, fromEU, days:14, n:120 })
  }
  if (!slots.length){
    await sendWithPresence(sock, jid, await aiRewriteTone("apology", { fallback:"No veo huecos inmediatos. Te muestro esta semana:" }))
    await proposeWeekOptions(sessionData, phone, sock, jid, { forcePreferred:!!sessionData.preferredStaffId, maxOptions })
    return
  }
  slots = slots.slice(0, maxOptions)
  const map = {}; for (const s of slots) map[s.date.format("YYYY-MM-DDTHH:mm")] = s.staffId || null
  sessionData.lastHours = slots.map(s=>s.date)
  sessionData.lastStaffByIso = map
  sessionData.stage = "awaiting_time"
  sessionData.lastProposeUsedPreferred = !!sessionData.preferredStaffId
  saveSession(phone, sessionData)

  const lines = enumerateHours(slots.map(s=>s.date)).map(h=>`${h.index}) ${h.pretty}${map[h.iso]?` — ${staffLabelFromId(map[h.iso])}`:""}`).join("\n")
  await sendWithPresence(sock, jid, await aiRewriteTone("offer", { options:`Lo más pronto que tengo:\n${lines}\n\nResponde con el número.` }))
}
async function proposeDayOptions(sessionData, phone, sock, jid, weekday, { maxOptions=3 }={}){
  if (typeof weekday!=="number") return proposeWeekOptions(sessionData, phone, sock, jid, { forcePreferred:!!sessionData.preferredStaffId, maxOptions })
  const base = dayjs().tz(EURO_TZ)
  const startDay = nextOccurrenceOfWeekday(base, weekday)
  let slots=[]
  if (sessionData.preferredStaffId && isStaffAllowedInLocation(sessionData.preferredStaffId, sessionData.sede)){
    slots = await searchAvailabilityForStaff({ locationKey: sessionData.sede, envServiceKey: sessionData.selectedServiceEnvKey, staffId: sessionData.preferredStaffId, fromEU:startDay, days:1, n:80 })
  }
  if (!slots.length){
    slots = await searchAvailabilityGeneric({ locationKey: sessionData.sede, envServiceKey: sessionData.selectedServiceEnvKey, fromEU:startDay, days:1, n:120 })
  }
  if (!slots.length){
    await sendWithPresence(sock, jid, await aiRewriteTone("apology", { fallback:"Ese día está completo en esa franja. Te paso otras opciones de la semana:" }))
    await proposeWeekOptions(sessionData, phone, sock, jid, { forcePreferred:!!sessionData.preferredStaffId, maxOptions })
    return
  }
  slots = slots.slice(0, maxOptions)
  const map = {}; for (const s of slots) map[s.date.format("YYYY-MM-DDTHH:mm")] = s.staffId || null
  sessionData.lastHours = slots.map(s=>s.date)
  sessionData.lastStaffByIso = map
  sessionData.stage = "awaiting_time"
  sessionData.lastProposeUsedPreferred = !!sessionData.preferredStaffId
  saveSession(phone, sessionData)

  const lines = enumerateHours(slots.map(s=>s.date)).map(h=>`${h.index}) ${h.pretty}${map[h.iso]?` — ${staffLabelFromId(map[h.iso])}`:""}`).join("\n")
  await sendWithPresence(sock, jid, await aiRewriteTone("offer", { options:`Para ese día tengo:\n${lines}\n\nResponde con el número.` }))
}

// ===== Crear reserva
async function createBookingWithRetry({ startEU, locationKey, envServiceKey, durationMin, customerId, teamMemberId, phone }){
  if (!envServiceKey) return { success:false, error:"No se especificó servicio" }
  if (!teamMemberId || typeof teamMemberId!=="string" || !teamMemberId.trim()) return { success:false, error:"teamMemberId requerido" }
  if (DRY_RUN) return { success:true, booking:{ id:`TEST_SIM_${Date.now()}`, __sim:true } }
  const sv = await getServiceIdAndVersion(envServiceKey); if (!sv?.id||!sv?.version) return { success:false, error:`No se pudo obtener servicio ${envServiceKey}` }
  const startISO = startEU.tz("UTC").toISOString()
  const idempotencyKey = stableKey({ loc:locationToId(locationKey), sv:sv.id, startISO, customerId, teamMemberId })
  let lastError=null
  for (let attempt=1; attempt<=SQUARE_MAX_RETRIES; attempt++){
    try{
      const requestData = {
        idempotencyKey,
        booking:{
          locationId: locationToId(locationKey),
          startAt: startISO,
          customerId,
          appointmentSegments:[{ teamMemberId, serviceVariationId: sv.id, serviceVariationVersion: Number(sv.version), durationMinutes: durationMin||60 }]
        }
      }
      const resp = await square.bookingsApi.createBooking(requestData)
      const booking = resp?.result?.booking || null
      try{ insertSquareLog.run({ phone: phone||'unknown', action:'create_booking', request_data:safeJSONStringify(requestData), response_data:safeJSONStringify(resp?.result||{}), error_data:null, timestamp:new Date().toISOString(), success:1 }) }catch{}
      if (booking) return { success:true, booking }
    }catch(e){
      lastError = e
      try{ insertSquareLog.run({ phone: phone||'unknown', action:'create_booking', request_data:safeJSONStringify({ attempt, envServiceKey, locationKey, startISO }), response_data:null, error_data:safeJSONStringify({ message:e?.message, body:e?.body }), timestamp:new Date().toISOString(), success:0 }) }catch{}
      if (attempt<SQUARE_MAX_RETRIES) await sleep(2000*attempt)
    }
  }
  return { success:false, error:`No se pudo crear reserva: ${lastError?.message||'Error desconocido'}`, lastError }
}

async function executeCreateBooking(_params, sessionData, phone, sock, jid){
  if (!sessionData.sede){ await sendWithPresence(sock, jid, await aiRewriteTone("askOne", { need:"elegir sede (Torremolinos o La Luz)" })); return }
  if (!sessionData.selectedServiceEnvKey){
    // Intentar alias por texto (micropigmentación etc.)
    const ek = resolveServiceByEnvAlias(sessionData.sede, sessionData.selectedServiceLabel||sessionData.category||"")
           || resolveEnvKeyFuzzy(sessionData.selectedServiceLabel||"", sessionData.sede)
    if (ek){ sessionData.selectedServiceEnvKey=ek; sessionData.selectedServiceLabel=serviceLabelFromEnvKey(ek) }
    if (!sessionData.selectedServiceEnvKey){ await sendWithPresence(sock, jid, await aiRewriteTone("askOne", { need:"elegir servicio" })); return }
  }
  if (!sessionData.pendingDateTime){ await sendWithPresence(sock, jid, await aiRewriteTone("askOne", { need:"elegir fecha y hora" })); return }

  const startEU = parseToEU(sessionData.pendingDateTime)
  if (!insideBusinessHours(startEU, 60)){
    await sendWithPresence(sock, jid, await aiRewriteTone("apology", { fallback:"Esa hora cae fuera del horario (L–V 09:00–20:00). Te paso opciones." }))
    await proposeWeekOptions(sessionData, phone, sock, jid, { forcePreferred:!!sessionData.preferredStaffId, maxOptions:3 })
    return
  }
  const iso = startEU.format("YYYY-MM-DDTHH:mm")

  const preferredId=sessionData.preferredStaffId||null, preferredLabel=sessionData.preferredStaffLabel||(preferredId?staffLabelFromId(preferredId):null)
  let staffId=null
  const staffFromIso = sessionData?.lastStaffByIso?.[iso] || null
  if (staffFromIso && isStaffAllowedInLocation(staffFromIso, sessionData.sede)) staffId = staffFromIso

  if (preferredId){
    if (!isStaffAllowedInLocation(preferredId, sessionData.sede)){
      const names=allowedStaffNamesForSede(sessionData.sede)
      await sendWithPresence(sock, jid, await aiRewriteTone("apology", { fallback:`${preferredLabel} no atiende en ${locationNice(sessionData.sede)}. En esa sede están: ${names.join(", ")}. Dime con quién prefieres.` }))
      return
    }
    const probe = await searchAvailabilityForStaff({ locationKey: sessionData.sede, envServiceKey: sessionData.selectedServiceEnvKey, staffId: preferredId, fromEU:startEU.clone().subtract(1,"minute"), days:1, n:200 })
    const matchPreferred = probe.find(x=>x.date.isSame(startEU,"minute"))
    if (matchPreferred) staffId = preferredId
    else if (staffFromIso && staffFromIso!==preferredId){
      sessionData.stage="awaiting_alt_staff_confirm"; sessionData.altCandidate={ startISO:startEU.toISOString(), staffId:staffFromIso }; saveSession(phone, sessionData)
      const otherName=staffLabelFromId(staffFromIso)||"otra profesional"
      const msg=`Para *${fmtES(startEU)}* tengo hueco con *${otherName}*, pero no con *${preferredLabel}*.\n1) Confirmar con ${otherName}\n2) Ver huecos con ${preferredLabel}`
      await sendWithPresence(sock, jid, await aiRewriteTone("offer", { options:msg }))
      return
    } else if (!staffFromIso){
      await sendWithPresence(sock, jid, await aiRewriteTone("apology", { fallback:`A esa hora no veo hueco con ${preferredLabel}. Te paso otras opciones con ${preferredLabel} 👇` }))
      await proposeWeekOptions(sessionData, phone, sock, jid, { forcePreferred:true, maxOptions:3 })
      return
    }
  }
  if (!staffId){
    const probe = await searchAvailabilityGeneric({ locationKey: sessionData.sede, envServiceKey: sessionData.selectedServiceEnvKey, fromEU:startEU.clone().subtract(1,"minute"), days:1, n:120 })
    const match = probe.find(x=>x.date.isSame(startEU,"minute"))
    if (match?.staffId && isStaffAllowedInLocation(match.staffId, sessionData.sede)) staffId = match.staffId
  }
  if (!staffId) staffId = pickStaffForLocation(sessionData.sede, preferredId)
  if (!staffId){ await sendWithPresence(sock, jid, await aiRewriteTone("apology", { fallback:"No hay profesionales disponibles en esa sede." })); return }

  // Identidad
  let customerId = sessionData.identityResolvedCustomerId || null
  if (!customerId){
    const { status, customer } = await getUniqueCustomerByPhoneOrPrompt(phone, sessionData, sock, jid) || {}
    if (status==="need_new" || status==="need_pick") return
    customerId = customer?.id || null
  }
  if (!customerId && (sessionData.name || sessionData.email)){
    const created = await findOrCreateCustomerWithRetry({ name:sessionData.name, email:sessionData.email, phone })
    if (created) customerId = created.id
  }
  if (!customerId){
    sessionData.stage = "awaiting_identity"; saveSession(phone, sessionData)
    await sendWithPresence(sock, jid, await aiRewriteTone("askOne", { need:"tu *nombre completo* y (opcional) tu *email*" }))
    return
  }

  const result = await createBookingWithRetry({ startEU, locationKey: sessionData.sede, envServiceKey: sessionData.selectedServiceEnvKey, durationMin: 60, customerId, teamMemberId: staffId, phone })
  if (!result.success){
    const aptId = `apt_failed_${Math.random().toString(36).slice(2,8)}${Date.now().toString(36).slice(-4)}`
    db.prepare(`INSERT INTO appointments VALUES (@id,@customer_name,@customer_phone,@customer_square_id,@location_key,@service_env_key,@service_label,@duration_min,@start_iso,@end_iso,@staff_id,@status,@created_at,@square_booking_id,@square_error,@retry_count)`)
      .run({ id:aptId, customer_name:sessionData?.name||null, customer_phone:phone, customer_square_id:customerId, location_key:sessionData.sede, service_env_key:sessionData.selectedServiceEnvKey, service_label:sessionData.selectedServiceLabel||serviceLabelFromEnvKey(sessionData.selectedServiceEnvKey)||"Servicio", duration_min:60, start_iso:startEU.tz("UTC").toISOString(), end_iso:startEU.clone().add(60,"minute").tz("UTC").toISOString(), staff_id:staffId, status:"failed", created_at:new Date().toISOString(), square_booking_id:null, square_error:result.error, retry_count:SQUARE_MAX_RETRIES })
    await sendWithPresence(sock, jid, await aiRewriteTone("apology", { fallback:"No pude crear la reserva ahora. ¿Quieres que te proponga otro horario?" }))
    return
  }
  if (result.booking.__sim){ await sendWithPresence(sock, jid, await aiRewriteTone("celebrate", { fallback:"🧪 SIMULACIÓN: Reserva creada (modo prueba)" })); clearSession(phone); return }

  const staffName = staffLabelFromId(staffId) || sessionData.preferredStaffLabel || "nuestro equipo"
  const address = sessionData.sede === "la_luz" ? ADDRESS_LUZ : ADDRESS_TORRE
  const svcLabel = serviceLabelFromEnvKey(sessionData.selectedServiceEnvKey) || sessionData.selectedServiceLabel || "Servicio"

  await sendWithPresence(sock, jid, `🎉 ¡Cita confirmada!\n\n📍 ${locationNice(sessionData.sede)}\n${address}\n\n💼 ${svcLabel}\n👤 ${staffName}\n🗓️ ${fmtES(startEU)}\n\nRef.: ${result.booking.id}\n\nTe recordaremos por aquí. ¡Nos vemos!`)
  clearSession(phone)
}

// ===== Listar / Cancelar
async function enumerateCitasByPhone(phone){
  const items=[]; let cid=null
  try{ const e164=normalizePhoneES(phone); const s=await square.customersApi.searchCustomers({ query:{ filter:{ phoneNumber:{ exact:e164 } } } }); cid=(s?.result?.customers||[])[0]?.id||null }catch{}
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
        items.push({ index:items.length+1, id:b.id, fecha_iso:start.format("YYYY-MM-DD"), pretty:fmtES(start), sede: locationNice(idToLocKey(b.locationId)||""), profesional: staffLabelFromId(seg?.teamMemberId) || "Profesional" })
      }
      items.sort((a,b)=> (a.fecha_iso.localeCompare(b.fecha_iso)) || (a.pretty.localeCompare(b.pretty)))
    }catch(e){}
  }
  return items
}
async function executeListAppointments(_p,_s,phone,sock,jid){
  const ap=await enumerateCitasByPhone(phone)
  if (!ap.length){ await sendWithPresence(sock, jid, await aiRewriteTone("smalltalk", { fallback:"No tienes citas programadas. ¿Quieres agendar una?" })); return }
  const msg=`Tus próximas citas:\n\n${ap.map(apt=>`${apt.index}) ${apt.pretty}\n📍 ${apt.sede}\n👤 ${apt.profesional}\n`).join("\n")}`
  await sendWithPresence(sock, jid, msg)
}
async function executeCancelAppointment(params, sessionData, phone, sock, jid){
  const ap=await enumerateCitasByPhone(phone)
  if (!ap.length){ await sendWithPresence(sock, jid, await aiRewriteTone("smalltalk", { fallback:"No encuentro citas futuras por tu número. ¿Quieres que te ayude a reservar?" })); return }
  const idx=params?.appointmentIndex
  if (!idx){
    sessionData.cancelList = ap
    sessionData.stage = "awaiting_cancel"
    saveSession(phone, sessionData)
    const msg = `¿Cuál quieres cancelar?\n\n${ap.map(a=>`${a.index}) ${a.pretty} - ${a.sede}`).join("\n")}\n\nResponde con el número`
    await sendWithPresence(sock, jid, await aiRewriteTone("offer", { options:msg }))
    return
  }
  const pick = ap.find(a=>a.index===idx)
  if (!pick){ await sendWithPresence(sock, jid, await aiRewriteTone("smalltalk", { fallback:"No encontré esa cita. ¿Puedes revisar el número?" })); return }
  const ok = await cancelBooking(pick.id)
  if (ok) await sendWithPresence(sock, jid, await aiRewriteTone("celebrate", { fallback:`✅ Cita cancelada: ${pick.pretty} en ${pick.sede}` }))
  else await sendWithPresence(sock, jid, await aiRewriteTone("apology", { fallback:"No pude cancelar la cita. Por favor, contáctanos directamente." }))
  delete sessionData.cancelList
  sessionData.stage = null
  saveSession(phone, sessionData)
}

// ===== Sesiones
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
  c.pendingDateTime_ms = s.pendingDateTime ? (dayjs.isDayjs(s.pendingDateTime) ? s.pendingDateTime.valueOf() : dayjs(s.pendingDateTime).valueOf()) : null
  c.chosenDayISO_ms = s.chosenDayISO ? dayjs(s.chosenDayISO).valueOf() : null
  delete c.lastHours; delete c.pendingDateTime; delete c.lastDays
  const j=JSON.stringify(c)
  const up=db.prepare(`UPDATE sessions SET data_json=@j, updated_at=@u WHERE phone=@p`).run({j,u:new Date().toISOString(),p:phone})
  if (up.changes===0) db.prepare(`INSERT INTO sessions (phone,data_json,updated_at) VALUES (@p,@j,@u)`).run({p:phone,j,u:new Date().toISOString()})
}
function clearSession(phone){ db.prepare(`DELETE FROM sessions WHERE phone=@phone`).run({phone}) }

// ===== Cola / envío
const QUEUE=new Map()
function enqueue(key,job){
  const prev=QUEUE.get(key)||Promise.resolve()
  const next=prev.then(job,job).finally(()=>{ if (QUEUE.get(key)===next) QUEUE.delete(key) })
  QUEUE.set(key,next); return next
}
async function sendWithPresence(sock, jid, text){
  try{ await sock.sendPresenceUpdate("composing", jid) }catch{}
  await new Promise(r=>setTimeout(r, 550+Math.random()*650))
  return sock.sendMessage(jid, { text })
}

// ===== Router (IA + heurísticas)
function fallbackHeuristics(textRaw, sessionData){
  const lower=norm(textRaw)
  if (textRaw.trim()===".") return { intent:"pause" }
  const numMatch = lower.match(/^(?:opcion|opción)?\s*([1-9]\d*)\b/)
  if (sessionData.stage==="awaiting_service_choice" && numMatch) return { intent:"choose_service_by_index", entities:{ index:Number(numMatch[1]) } }
  if (sessionData.stage==="awaiting_time" && numMatch) return { intent:"choose_time_by_index", entities:{ index:Number(numMatch[1]) } }
  if (sessionData.stage==="awaiting_identity_pick" && numMatch) return { intent:"pick_identity_index", entities:{ index:Number(numMatch[1]) } }
  if (sessionData.stage==="awaiting_alt_staff_confirm"){
    if (/(^|\b)1(\b|$)|\bconfirm/.test(lower) || isYes(lower)) return { intent:"confirm_alt_yes" }
    if (/(^|\b)2(\b|$)|ver huecos|otra/.test(lower) || isNo(lower)) return { intent:"confirm_alt_no" }
  }
  if (/\bcancel(ar)?\b/.test(lower) && /\bcita|reserva/.test(lower)) return { intent:"cancel_appointment" }
  if (/\bmis citas|ver citas|tengo cita|pr[oó]ximas citas\b/.test(lower)) return { intent:"list_appointments" }
  if (/\b(gracias|thanks|perfecto|genial)\b/.test(lower)) return { intent:"thanks" }
  if (/\b(hola|buenas|qué tal|que tal)\b/.test(lower)) return { intent:"greet" }
  if (parseASAP(textRaw)) return { intent:"asap" }
  const sede=parseSede(textRaw); if (sede) return { intent:"choose_sede", entities:{ sede } }
  const cat=detectCategory(textRaw); if (cat) return { intent:"choose_category", entities:{ category:cat } }
  const pro=parsePreferredStaffFromText(textRaw); if (pro) return { intent:"choose_professional", entities:{ professional_text:textRaw } }
  const { name, email } = parseNameEmailFromText(textRaw); if (name||email) return { intent:"provide_identity", entities:{ name, email } }
  const pod=parsePartOfDay(textRaw), wk=parseWeekTarget(textRaw), wd=parseWeekday(textRaw); if (pod||wk||wd) return { intent:"set_time_prefs", entities:{ part_of_day:pod||null, week_target:wk||null, weekday:wd||null } }
  return { intent:"unknown" }
}
async function routeWithAI(textRaw, sessionData){
  const plan = await aiPlanNLU(textRaw, sessionData)
  if (!plan || Number(plan.confidence||0) < NLU_MIN_CONF) return null
  const ent = plan.entities || {}
  const normEnt = {
    category: ent.category||null, service_text:ent.service_text||null, sede:ent.sede||null,
    professional_text: ent.professional_text||null, index: ent.index!=null?clampInt(Number(ent.index),1,999):null,
    part_of_day: ent.part_of_day||null, week_target:ent.week_target||null, weekday: ent.weekday!=null?clampInt(Number(ent.weekday),0,6):null,
    date_iso:ent.date_iso||null, time_24h:ent.time_24h||null, name:ent.name||null, email:ent.email||null
  }
  return { intent:plan.intent, entities:normEnt, confidence:Number(plan.confidence||0) }
}

// ===== Mini-web + QR
const app=express()
const PORT=process.env.PORT||8080
let lastQR=null, conectado=false
app.get("/", (_req,res)=>{
  const total = db.prepare(`SELECT COUNT(*) c FROM appointments`).get()?.c || 0
  const ok    = db.prepare(`SELECT COUNT(*) c FROM appointments WHERE status='confirmed'`).get()?.c || 0
  const fail  = db.prepare(`SELECT COUNT(*) c FROM appointments WHERE status='failed'`).get()?.c || 0
  res.send(`<!doctype html><meta charset="utf-8"><meta http-equiv="refresh" content="6"><style>
  body{font-family:system-ui;display:grid;place-items:center;min-height:100vh;background:#f8f9fa}
  .card{max-width:640px;padding:32px;border-radius:20px;box-shadow:0 8px 32px rgba(0,0,0,.1);background:white}
  .status{padding:12px;border-radius:8px;margin:8px 0}
  .success{background:#d4edda;color:#155724}
  .error{background:#f8d7da;color:#721c24}
  .warning{background:#fff3cd;color:#856404}
  .stat{display:inline-block;margin:0 16px;padding:8px 12px;background:#e9ecef;border-radius:6px}
  </style><div class="card">
  <h1>🩷 Bot v33.0.0</h1>
  <div class="status ${conectado ? 'success' : 'error'}">WhatsApp: ${conectado ? "✅ Conectado" : "❌ Desconectado"}</div>
  ${!conectado&&lastQR?`<div style="text-align:center;margin:20px 0"><img src="/qr.png" width="300" style="border-radius:8px"></div>`:""}
  <div class="status warning">Modo: ${DRY_RUN ? "🧪 Simulación" : "🚀 Producción"}</div>
  <h3>📊 Stats</h3>
  <div><span class="stat">📅 Total: ${total}</span><span class="stat">✅ Exitosas: ${ok}</span><span class="stat">❌ Fallidas: ${fail}</span></div>
  </div>`)
})
app.get("/qr.png", async (_req,res)=>{ if(!lastQR) return res.status(404).send("No QR"); const png=await qrcode.toBuffer(lastQR,{type:"png",width:512,margin:1}); res.set("Content-Type","image/png").send(png) })
app.get("/logs", (_req,res)=>{ const recent=db.prepare(`SELECT * FROM square_logs ORDER BY timestamp DESC LIMIT 50`).all(); res.json({ logs: recent }) })

// ===== Baileys
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

// ===== Arranque del bot
async function startBot(){
  try{
    const { makeWASocket, useMultiFileAuthState, fetchLatestBaileysVersion, Browsers } = await loadBaileys()
    if (!fs.existsSync("auth_info")) fs.mkdirSync("auth_info",{recursive:true})
    const { state, saveCreds } = await useMultiFileAuthState("auth_info")
    const { version } = await fetchLatestBaileysVersion().catch(()=>({version:[2,3000,0]}))
    const sock = makeWASocket({ logger:pino({level:"silent"}), printQRInTerminal:false, auth:state, version, browser:Browsers.macOS("Desktop"), syncFullHistory:false })
    globalThis.sock=sock

    sock.ev.on("connection.update", ({connection,qr})=>{
      if (qr){ lastQR=qr; conectado=false; try{ qrcodeTerminal.generate(qr,{small:true}) }catch{} }
      if (connection==="open"){ lastQR=null; conectado=true }
      if (connection==="close"){ conectado=false; setTimeout(()=>{ startBot().catch(console.error) }, 1500) }
    })
    sock.ev.on("creds.update", saveCreds)

    sock.ev.on("messages.upsert", async ({messages})=>{
      const m=messages?.[0]; if (!m?.message) return
      const jid = m.key.remoteJid
      const isFromMe = !!m.key.fromMe
      const phone = normalizePhoneES((jid||"").split("@")[0]||"") || (jid||"").split("@")[0]
      const textRaw = (m.message.conversation || m.message.extendedTextMessage?.text || m.message?.imageMessage?.caption || "").trim()
      if (!textRaw) return

      await enqueue(phone, async ()=>{
        try {
          let s = loadSession(phone) || {
            greeted:false, sede:null, selectedServiceEnvKey:null, selectedServiceLabel:null,
            preferredStaffId:null, preferredStaffLabel:null, pendingDateTime:null,
            name:null, email:null, last_msg_id:null, lastStaffByIso:{},
            lastProposeUsedPreferred:false, stage:null, cancelList:null,
            serviceChoices:null, identityChoices:null, pendingCategory:null,
            snooze_until_ms:null, identityResolvedCustomerId:null, category:null,
            lastDays:null, chosenDayISO:null,
            preferredPartOfDay:null, targetWeek:null, preferredWeekday:null,
            lastProposeScope:null, lastWeekFromISO:null, altCandidate:null
          }
          if (s.last_msg_id === m.key.id) return
          s.last_msg_id = m.key.id

          const nowEU = dayjs().tz(EURO_TZ)
          if (textRaw.trim() === "."){ s.snooze_until_ms = nowEU.add(6,"hour").valueOf(); saveSession(phone,s); return }
          if (s.snooze_until_ms && nowEU.valueOf() < s.snooze_until_ms){ saveSession(phone,s); return }
          if (isFromMe){ saveSession(phone,s); return }

          // Si estamos esperando sede y el user la da, avanzamos sin repetir
          if (s.stage==="awaiting_sede_for_services"){
            const sedeG = parseSede(textRaw)
            if (sedeG){ s.sede=sedeG; s.stage=null; saveSession(phone,s); await executeChooseService({ category:s.pendingCategory||s.category||detectCategory(textRaw) }, s, phone, sock, jid, textRaw); return }
          }

          // === NLU
          let aiRoute = await routeWithAI(textRaw, s)
          if (!aiRoute) aiRoute = fallbackHeuristics(textRaw, s)
          const intent = aiRoute?.intent || "unknown"
          const ent = aiRoute?.entities || {}

          // Side-effects por entidades
          if (ent.sede) s.sede = ent.sede
          if (ent.part_of_day) s.preferredPartOfDay = ent.part_of_day
          if (ent.week_target) s.targetWeek = ent.week_target
          if (Number.isInteger(ent.weekday)) s.preferredWeekday = ent.weekday
          if (ent.category) s.category = ent.category
          if (ent.name) s.name = ent.name
          if (ent.email) s.email = ent.email

          // Profesional
          if ((intent==="choose_professional"||intent==="direct_booking") && ent.professional_text){
            const e=findStaffByFreeText(ent.professional_text)
            if (e){ s.preferredStaffId=e.id; s.preferredStaffLabel=staffLabelFromId(e.id); learnAlias(ent.professional_text, e.id) }
          }

          // Router
          switch (intent) {
            case "pause": s.snooze_until_ms=nowEU.add(6,"hour").valueOf(); saveSession(phone,s); return

            case "greet":
            case "smalltalk":
            case "thanks":
              await sendWithPresence(sock, jid, await aiRewriteTone("smalltalk", { fallback: intent==="thanks" ? "¡A ti! 😊" : "¡Hola! ¿En qué te ayudo?" }))
              return

            case "help":
              await sendWithPresence(sock, jid, await aiRewriteTone("smalltalk", { fallback:"Puedo reservarte cita. Dime qué quieres, la sede y si prefieres mañana o tarde. 😉" }))
              return

            case "choose_sede":
              // si ya tenemos sede, no repitas
              if (!s.sede) s.sede = ent.sede || parseSede(textRaw) || s.sede
              saveSession(phone, s)
              await executeChooseService({ category:s.category||detectCategory(textRaw) }, s, phone, sock, jid, textRaw)
              return

            case "choose_category":
              await executeChooseService({ category: ent.category||detectCategory(textRaw) }, s, phone, sock, jid, textRaw)
              return

            case "choose_service_by_index":{
              if (!(s.stage==="awaiting_service_choice" && Array.isArray(s.serviceChoices))){
                await executeChooseService({ category:s.category||detectCategory(textRaw) }, s, phone, sock, jid, textRaw)
                return
              }
              const n=Number(ent.index||0)
              const pick = s.serviceChoices.find(it=>it.index===n)
              if (!pick){ await sendWithPresence(sock, jid, await aiRewriteTone("smalltalk",{ fallback:"No encontré esa opción. Prueba con un número de la lista." })); return }
              let ek = resolveEnvKeyFromLabelAndSede(pick.label, s.sede) || resolveEnvKeyFuzzy(pick.label, s.sede)
              if (!ek) ek = resolveServiceByEnvAlias(s.sede, pick.label)
              if (!ek){ await sendWithPresence(sock, jid, await aiRewriteTone("askOne", { need:"el nombre exacto como en el menú o la zona (p. ej. Micropigmentación Cejas / Ombré / Powder)" })); return }
              s.selectedServiceLabel = pick.label
              s.selectedServiceEnvKey = ek
              s.stage = "awaiting_time_prefs"
              saveSession(phone, s)
              await sendWithPresence(sock, jid, await aiRewriteTone("askOne", { need:"¿te viene mejor *mañana* o *tarde*? ¿Y *esta semana* o *la próxima*?" }))
              return
            }

            case "set_time_prefs":
              if (!s.selectedServiceEnvKey){
                const ek2 = resolveServiceByEnvAlias(s.sede, textRaw)
                        || resolveEnvKeyFuzzy(s.selectedServiceLabel||"", s.sede)
                if (ek2){ s.selectedServiceEnvKey=ek2; s.selectedServiceLabel=serviceLabelFromEnvKey(ek2) }
                if (!s.selectedServiceEnvKey){ await executeChooseService({ category:s.category||detectCategory(textRaw) }, s, phone, sock, jid, textRaw); return }
              }
              if (Number.isInteger(ent.weekday)){ await proposeDayOptions(s, phone, sock, jid, ent.weekday, { maxOptions:3 }); return }
              await proposeWeekOptions(s, phone, sock, jid, { forcePreferred:!!s.preferredStaffId, maxOptions:3 })
              return

            case "asap":
              if (!s.selectedServiceEnvKey){
                const ek3 = resolveServiceByEnvAlias(s.sede, textRaw)
                        || resolveEnvKeyFuzzy(s.selectedServiceLabel||"", s.sede)
                if (ek3){ s.selectedServiceEnvKey=ek3; s.selectedServiceLabel=serviceLabelFromEnvKey(ek3) }
                if (!s.selectedServiceEnvKey){ await executeChooseService({ category:s.category||detectCategory(textRaw) }, s, phone, sock, jid, textRaw); return }
              }
              await proposeEarliestOptions(s, phone, sock, jid, { maxOptions:3 })
              return

            case "choose_time_by_index":{
              if (parseASAP(textRaw)){ await proposeEarliestOptions(s, phone, sock, jid, { maxOptions:3 }); return }
              if (s.stage!=="awaiting_time"){ await proposeWeekOptions(s, phone, sock, jid, { forcePreferred:!!s.preferredStaffId, maxOptions:3 }); return }
              const idx = Number(ent.index||0)-1
              const pick = Array.isArray(s.lastHours) ? s.lastHours[idx] : null
              if (!dayjs.isDayjs(pick)){
                await sendWithPresence(sock, jid, await aiRewriteTone("apology", { fallback:"Esa opción ya no está. Te paso nuevas:" }))
                await proposeWeekOptions(s, phone, sock, jid, { forcePreferred:!!s.preferredStaffId, maxOptions:3 })
                return
              }
              const isoH=pick.format("YYYY-MM-DDTHH:mm")
              s.pendingDateTime = pick.tz(EURO_TZ).toISOString()
              const slotStaff = s?.lastStaffByIso?.[isoH] || null
              if (slotStaff){ s.preferredStaffId=slotStaff; s.preferredStaffLabel=staffLabelFromId(slotStaff) }
              s.stage = null
              saveSession(phone, s)
              await executeCreateBooking({}, s, phone, sock, jid)
              return
            }

            case "confirm_alt_yes":
            case "confirm_alt_no":
              if (s.stage==="awaiting_alt_staff_confirm" && s.altCandidate){
                if (intent==="confirm_alt_yes"){
                  const otherId = s.altCandidate.staffId
                  s.pendingDateTime = s.altCandidate.startISO
                  s.stage = null
                  if (!s.identityResolvedCustomerId){
                    const { status, customer } = await getUniqueCustomerByPhoneOrPrompt(phone, s, sock, jid) || {}
                    if (status==="need_new" || status==="need_pick") return
                    s.identityResolvedCustomerId = customer?.id || null
                  }
                  const startEU = parseToEU(s.pendingDateTime)
                  const result = await createBookingWithRetry({ startEU, locationKey:s.sede, envServiceKey:s.selectedServiceEnvKey, durationMin:60, customerId:s.identityResolvedCustomerId, teamMemberId:otherId, phone })
                  if (!result.success){ await sendWithPresence(sock, jid, await aiRewriteTone("apology", { fallback:"No pude crearla ahora mismo. ¿Te enseño otros horarios?" })); s.stage="awaiting_time_prefs"; saveSession(phone,s); return }
                  if (result.booking.__sim){ await sendWithPresence(sock, jid, await aiRewriteTone("celebrate", { fallback:"🧪 SIMULACIÓN: Reserva creada (modo prueba)" })); clearSession(phone); return }
                  const staffName=staffLabelFromId(otherId)||"nuestro equipo", address=s.sede==="la_luz"?ADDRESS_LUZ:ADDRESS_TORRE, svcLabel=serviceLabelFromEnvKey(s.selectedServiceEnvKey)||s.selectedServiceLabel||"Servicio"
                  await sendWithPresence(sock, jid, `🎉 ¡Cita confirmada!\n\n📍 ${locationNice(s.sede)}\n${address}\n\n💼 ${svcLabel}\n👤 ${staffName}\n🗓️ ${fmtES(startEU)}\n\nRef.: ${result.booking.id}`)
                  clearSession(phone)
                  return
                } else {
                  s.stage=null; saveSession(phone,s)
                  await proposeWeekOptions(s, phone, sock, jid, { forcePreferred:true, maxOptions:3 })
                  return
                }
              }
              break

            case "provide_identity":
              if (s.stage!=="awaiting_identity"){
                if (ent.name||ent.email){
                  const created = await findOrCreateCustomerWithRetry({ name:ent.name||s.name, email:ent.email||s.email, phone })
                  if (created) s.identityResolvedCustomerId = created.id
                  saveSession(phone, s)
                }
              } else {
                if (!(ent.name||ent.email)){ await sendWithPresence(sock, jid, await aiRewriteTone("askOne", { need:"tu nombre completo y (opcional) email" })); return }
                const created = await findOrCreateCustomerWithRetry({ name:ent.name||s.name, email:ent.email||s.email, phone })
                if (!created){ await sendWithPresence(sock, jid, await aiRewriteTone("apology", { fallback:"No pude crear tu ficha. ¿Repites tu *nombre* y (opcional) tu *email*?" })); return }
                s.identityResolvedCustomerId = created.id
                s.stage = null
                saveSession(phone, s)
                await sendWithPresence(sock, jid, await aiRewriteTone("smalltalk", { fallback:"¡Gracias! Finalizo tu reserva…" }))
                await executeCreateBooking({}, s, phone, sock, jid)
                return
              }
              break

            case "pick_identity_index":
              if (s.stage==="awaiting_identity_pick"){
                const n=Number(ent.index||0)
                const choice=(s.identityChoices||[]).find(c=>c.index===n)
                if (!choice){ await sendWithPresence(sock, jid, await aiRewriteTone("smalltalk",{ fallback:"No encontré esa opción. Prueba con un número de la lista." })); return }
                s.identityResolvedCustomerId = choice.id
                s.stage = null; saveSession(phone, s)
                await sendWithPresence(sock, jid, await aiRewriteTone("smalltalk",{ fallback:"¡Gracias! Finalizo tu reserva…" }))
                await executeCreateBooking({}, s, phone, sock, jid)
                return
              }
              break

            case "list_appointments": await executeListAppointments({}, s, phone, sock, jid); return
            case "cancel_appointment": await executeCancelAppointment({}, s, phone, sock, jid); return

            default:
              // Flujo auto
              if (!s.category && !s.selectedServiceEnvKey){
                s.stage="awaiting_category"; saveSession(phone,s)
                await sendWithPresence(sock, jid, await aiRewriteTone("askOne", { need:"¿qué te quieres hacer: *uñas*, *pestañas*, *cejas*, *depilación* o *blanqueamiento dental*?" }))
                return
              }
              if (!s.sede){
                s.stage="awaiting_sede_for_services"; saveSession(phone,s)
                await sendWithPresence(sock, jid, await aiRewriteTone("askOne", { need:"¿*Torremolinos* o *La Luz*?" }))
                return
              }
              if (!s.selectedServiceEnvKey){
                await executeChooseService({ category:s.category||detectCategory(textRaw) }, s, phone, sock, jid, textRaw)
                return
              }
              if (parseASAP(textRaw)){ await proposeEarliestOptions(s, phone, sock, jid, { maxOptions:3 }); return }
              if (s.stage!=="awaiting_time_prefs"){
                s.stage="awaiting_time_prefs"; saveSession(phone,s)
                await sendWithPresence(sock, jid, await aiRewriteTone("askOne", { need:"¿mañana o tarde? ¿esta semana o la próxima?" }))
                return
              }
              await proposeWeekOptions(s, phone, sock, jid, { forcePreferred:!!s.preferredStaffId, maxOptions:3 })
              return
          }

        } catch (error) {
          if (BOT_DEBUG) console.error(error)
          await sendWithPresence(sock, jid, await aiRewriteTone("apology", { fallback:"Uy, hubo un problema técnico. ¿Puedes repetir tu mensaje? 🙏" }))
        }
      })
    })
  }catch(e){
    setTimeout(()=>{ startBot().catch(console.error) }, 5000)
  }
}

// ===== Arranque servidor
let serverStarted=false
function safeListen(){
  if (serverStarted) return
  try{
    app.listen(PORT, ()=>{ serverStarted=true; startBot().catch(console.error) })
  }catch(e){
    if (String(e?.message||"").includes("EADDRINUSE")){
      console.error("⚠️ Puerto ocupado, continúo sin Express duplicado")
      startBot().catch(console.error)
    } else {
      console.error("💥 Error al escuchar:", e?.message||e)
      startBot().catch(console.error)
    }
  }
}
console.log("🩷 Bot v33.0.0")
safeListen()
process.on("uncaughtException", e=>{ console.error("💥 uncaughtException:", e?.stack||e?.message||e) })
process.on("unhandledRejection", e=>{ console.error("💥 unhandledRejection:", e) })
process.on("SIGTERM", ()=>{ process.exit(0) })
process.on("SIGINT", ()=>{ process.exit(0) })
