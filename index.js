// index.js — Gapink Nails · MULTI-LOCAL + IA + Square (v9.2 “Torremolinos + La Luz”)
// Requisitos: Node 20+, npm i express @whiskeysockets/baileys pino qrcode qrcode-terminal better-sqlite3 dayjs dotenv @square/square

import express from "express"
import makeWASocket, {
  useMultiFileAuthState,
  fetchLatestBaileysVersion,
  Browsers
} from "@whiskeysockets/baileys"
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
import isSameOrAfter from "dayjs/plugin/isSameOrAfter.js"
import isSameOrBefore from "dayjs/plugin/isSameOrBefore.js"
import "dayjs/locale/es.js"
import { Client, Environment } from "square"

// ============= Dayjs
dayjs.extend(utc); dayjs.extend(tz); dayjs.extend(isoWeek); dayjs.extend(isSameOrAfter); dayjs.extend(isSameOrBefore)
dayjs.locale("es")
const EURO_TZ = "Europe/Madrid"

// ============= Config negocio
const WORK_DAYS = [1,2,3,4,5] // L–V
const SHIFT_A_START=10, SHIFT_A_END=14
const SHIFT_B_START=16, SHIFT_B_END=20
const SLOT_MIN=30
const SEARCH_WINDOW_DAYS = Number(process.env.BOT_SEARCH_WINDOW_DAYS || 14)
const NOW_MIN_OFFSET_MIN = Number(process.env.BOT_NOW_OFFSET_MIN || 30)

// Festivos (nacionales+Andalucía) + extras por .env (dd/mm)
const HOLI_BASE = new Set([
  "01/01","06/01","28/02","01/05","15/08","12/10","01/11","06/12","08/12","25/12"
])
;(String(process.env.HOLIDAYS_EXTRA||"").split(",").map(s=>s.trim()).filter(Boolean)).forEach(x=>HOLI_BASE.add(x))

// ============= Square
const square = new Client({
  accessToken: process.env.SQUARE_ACCESS_TOKEN,
  environment: process.env.SQUARE_ENV==="production" ? Environment.Production : Environment.Sandbox
})
const LOCATION_IDS = {
  TORREMOLINOS: process.env.SQUARE_LOCATION_ID_TORREMOLINOS || process.env.SQUARE_LOCATION_ID || "",
  LA_LUZ: process.env.SQUARE_LOCATION_ID_LA_LUZ || ""
}
const LOCATION_NAMES = {
  [LOCATION_IDS.TORREMOLINOS]: "Torremolinos",
  [LOCATION_IDS.LA_LUZ]: "Málaga – La Luz"
}

// ============= Mensaje bienvenida (SIEMPRE primero)
const WELCOME_TEXT =
`Gracias por comunicarte con Gapink Nails. Por favor, haznos saber cómo podemos ayudarte.

Solo atenderemos por WhatsApp y llamadas en horario de lunes a viernes de 10 a 14:00 y de 16:00 a 20:00 

Si quieres reservar una cita puedes hacerlo a través de este link:

https://gapinknails.square.site/

Y si quieres modificarla puedes hacerlo a través del link del sms que llega con su cita! 

Para cualquier otra consulta, déjenos saber y en el horario establecido le responderemos.
Gracias 😘`

// ============= OpenAI (para redacción amable y aclaraciones, sin inventar)
const OPENAI_API_KEY  = process.env.OPENAI_API_KEY
const OPENAI_API_URL  = process.env.OPENAI_API_URL || "https://api.openai.com/v1/chat/completions"
const OPENAI_MODEL    = process.env.OPENAI_MODEL || "gpt-4o-mini"

async function aiChat(messages, temperature=0.25){
  if(!OPENAI_API_KEY) return ""
  try{
    const r = await fetch(OPENAI_API_URL, {
      method:"POST",
      headers:{ "Content-Type":"application/json", "Authorization":`Bearer ${OPENAI_API_KEY}` },
      body: JSON.stringify({ model: OPENAI_MODEL, messages, temperature })
    })
    if(!r.ok) throw new Error(`OpenAI ${r.status}`)
    const j = await r.json()
    return (j?.choices?.[0]?.message?.content||"").trim()
  }catch(e){ console.error("OpenAI:", e?.message||e); return "" }
}

const SYS_TONE = `Eres recepcionista WhatsApp de Gapink Nails (España). Responde breve, cálida y clara, sin emojis. No inventes jamás horarios ni precios. Si falta un dato clave (local o servicio), pregunta solo eso. Español.`

// ============= Helpers
const rm = (s="") => s.normalize("NFD").replace(/\p{Diacritic}/gu,"")
const norm = (s="") => rm(String(s).toLowerCase()).replace(/[^a-z0-9]+/g," ").trim()
const onlyDigits = (s="") => (s||"").replace(/\D+/g,"")
const EURO = (d)=>dayjs(d).tz(EURO_TZ)
const fmtES=(d)=>{
  const t = EURO(d)
  const dias=["domingo","lunes","martes","miércoles","jueves","viernes","sábado"]
  const DD=String(t.date()).padStart(2,"0"), MM=String(t.month()+1).padStart(2,"0")
  const HH=String(t.hour()).padStart(2,"0"), mm=String(t.minute()).padStart(2,"0")
  return `${dias[t.day()]} ${DD}/${MM} ${HH}:${mm}`
}
const isHolidayEU=(d)=>{
  const dd=String(d.date()).padStart(2,"0"), mm=String(d.month()+1).padStart(2,"0")
  return HOLI_BASE.has(`${dd}/${mm}`) || !WORK_DAYS.includes(d.day())
}
const insideBusinessHours=(d)=>{
  if(isHolidayEU(d)) return false
  const h = d.hour() + d.minute()/60
  const okAM = h>=SHIFT_A_START && h<=SHIFT_A_END
  const okPM = h>=SHIFT_B_START && h<=SHIFT_B_END
  return okAM || okPM
}
const ceilToSlot=(t)=>{
  const r=t.minute()%SLOT_MIN; return r? t.add(SLOT_MIN-r,"minute").second(0).millisecond(0) : t.second(0).millisecond(0)
}
const clampFuture=(t)=>ceilToSlot(EURO(t.isValid?t:dayjs(t)).add(NOW_MIN_OFFSET_MIN,"minute"))
const normalizePhoneES=(raw)=>{
  const d=onlyDigits(raw||""); if(!d) return null
  if (raw.startsWith("+") && d.length>=8 && d.length<=15) return `+${d}`
  if (d.startsWith("34") && d.length===11) return `+${d}`
  if (d.length===9) return `+34${d}`
  if (d.startsWith("00")) return `+${d.slice(2)}`
  return `+${d}`
}

// ============= NLP: local + servicio + fecha/hora
const LOC_SYNON = {
  TORREMOLINOS:["torremolinos","playamar","benyamina","benyamaina","benyamina 18"],
  LA_LUZ:["la luz","malaga","málaga","centro","cruz de humilladero","huelin","carretera cadiz","carretera cádiz","barrio de la luz"]
}
function detectLocationFromText(text){
  const low=rm(String(text||"").toLowerCase())
  for(const [k,arr] of Object.entries(LOC_SYNON)) if(arr.some(w=>low.includes(rm(w)))) return k
  return null
}

// Catálogo (duraciones por defecto si no aparece aquí: 60)
const SERVICE = {
  MANICURA_CON_ESMALTE_NORMAL:{ name:"Manicura con esmalte normal", dur:30 },
  MANICURA_SEMIPERMANENTE:{ name:"Manicura semipermanente", dur:30 },
  MANICURA_SEMIPERMANENTE_QUITAR:{ name:"Manicura semipermanente + quitar", dur:40 },
  MANICURA_SEMIPERMANETE_CON_NIVELACION:{ name:"Manicura semipermanente con nivelación", dur:60 },
  MANICURA_RUSA_CON_NIVELACION:{ name:"Manicura rusa con nivelación", dur:90 },
  UNAS_NUEVAS_ESCULPIDAS:{ name:"Uñas nuevas esculpidas", dur:90 },
  RELLENO_UNAS_ESCULPIDAS:{ name:"Relleno uñas esculpidas", dur:60 },
  ESMALTADO_SEMIPERMANETE_PIES:{ name:"Esmaltado semipermanente pies", dur:30 },
  PEDICURA_SPA_CON_ESMALTE_SEMIPERMANENTE:{ name:"Pedicura spa (semipermanente)", dur:60 },
  PEDICURA_SPA_CON_ESMALTE_NORMAL:{ name:"Pedicura spa (normal)", dur:60 },
  DISENO_DE_CEJAS_CON_HENNA_Y_DEPILACION:{ name:"Diseño de cejas con henna y depilación", dur:30 },
  DEPILACION_CEJAS_CON_HILO:{ name:"Depilación cejas con hilo", dur:15 },
  LIFITNG_DE_PESTANAS_Y_TINTE:{ name:"Lifting de pestañas y tinte", dur:60 },
  EXTENSIONES_DE_PESTANAS_NUEVAS_PELO_A_PELO:{ name:"Extensiones nuevas pelo a pelo", dur:120 },
  EXTENSIONES_PESTANAS_NUEVAS_2D:{ name:"Extensiones nuevas 2D", dur:120 },
  EXTENSIONES_PESTANAS_NUEVAS_3D:{ name:"Extensiones nuevas 3D", dur:120 },
  RELLENO_EXTENSIONES_PESTANAS_PELO_A_PELO:{ name:"Relleno pestañas pelo a pelo", dur:90 },
  RELLENO_PESTANAS_2D:{ name:"Relleno pestañas 2D", dur:90 },
  RELLENO_PESTANAS_3D:{ name:"Relleno pestañas 3D", dur:90 },
  LIMPIEZA_HYDRA_FACIAL:{ name:"Limpieza hydra facial", dur:90 },
  LIMPIEZA_FACIAL_CON_PUNTA_DE_DIAMANTE:{ name:"Limpieza facial con punta de diamante", dur:90 },
  MICROBLADING:{ name:"Microblading", dur:120 },
  DERMAPEN:{ name:"Dermapen", dur:60 },
  MASAJE_RELAJANTE:{ name:"Masaje relajante", dur:60 },
  // (tienes más servicios en .env; si los detectas por texto y no están aquí, dur=60 por defecto)
}
const SVC_KEYS = Object.keys(SERVICE)
const SERVICE_SYNONYMS = [
  // uñas manos
  ["manicura semipermanente","MANICURA_SEMIPERMANENTE",["semi","esmaltado gel","gel color","permanente"]],
  ["manicura semipermanente quitar","MANICURA_SEMIPERMANENTE_QUITAR",["retirar semi","quitar gel","retirar gel"]],
  ["manicura rusa","MANICURA_RUSA_CON_NIVELACION",["russian","rusa nivelacion","rusa nivelación"]],
  ["manicura normal","MANICURA_CON_ESMALTE_NORMAL",["esmaltado normal","manicura básica","manicura basica"]],
  ["manicura nivelacion","MANICURA_SEMIPERMANETE_CON_NIVELACION",["nivelación","nivelacion"]],
  ["uñas nuevas","UNAS_NUEVAS_ESCULPIDAS",["acrílicas nuevas","acrilicas nuevas","esculpidas nuevas"]],
  ["relleno uñas","RELLENO_UNAS_ESCULPIDAS",["relleno acrílicas","relleno acrilicas","relleno esculpidas"]],
  // pies
  ["pedicura semi","PEDICURA_SPA_CON_ESMALTE_SEMIPERMANENTE",["semi pies","gel pies"]],
  ["pedicura normal","PEDICURA_SPA_CON_ESMALTE_NORMAL",["pedicura básica","pedicura basica"]],
  ["esmaltado semi pies","ESMALTADO_SEMIPERMANETE_PIES",["semi pies"]],
  // cejas/pestañas
  ["hilo cejas","DEPILACION_CEJAS_CON_HILO",["threading","depilacion cejas hilo"]],
  ["diseño cejas henna","DISENO_DE_CEJAS_CON_HENNA_Y_DEPILACION",["diseño cejas","cejas henna"]],
  ["lifting pestañas tinte","LIFITNG_DE_PESTANAS_Y_TINTE",["lash lift","lifting pestañas"]],
  ["pelo a pelo","EXTENSIONES_DE_PESTANAS_NUEVAS_PELO_A_PELO",["extensiones clásicas","extensiones clasicas","classic lashes"]],
  ["2d","EXTENSIONES_PESTANAS_NUEVAS_2D",[]],
  ["3d","EXTENSIONES_PESTANAS_NUEVAS_3D",[]],
  ["relleno pelo a pelo","RELLENO_EXTENSIONES_PESTANAS_PELO_A_PELO",["relleno clásicas","relleno clasicas"]],
  ["relleno 2d","RELLENO_PESTANAS_2D",[]],
  ["relleno 3d","RELLENO_PESTANAS_3D",[]],
  // facial
  ["hydra","LIMPIEZA_HYDRA_FACIAL",["hydrafacial","hydra facial"]],
  ["punta diamante","LIMPIEZA_FACIAL_CON_PUNTA_DE_DIAMANTE",["diamond tip"]],
  ["microblading","MICROBLADING",[]],
  ["dermapen","DERMAPEN",[]],
  ["masaje","MASAJE_RELAJANTE",[]]
]
function detectServiceKey(text){
  const low = rm(String(text||"").toLowerCase())
  for(const [label,key,extra] of SERVICE_SYNONYMS){
    const tag=rm(label)
    if(low.includes(tag) || (extra||[]).some(x=>low.includes(rm(x)))) return key
  }
  // Fuzzy tokens
  const toks = norm(low).split(/\s+/).filter(Boolean)
  let best=null,score=0
  for(const k of SVC_KEYS){
    const words = norm(SERVICE[k].name).split(/\s+/)
    const s = toks.filter(t=>words.includes(t)).length
    if(s>score){score=s; best=k}
  }
  // Caso "manicura" genérica → default a semi
  if(!best && /\bmanicura\b/.test(low)) return "MANICURA_SEMIPERMANENTE"
  if(!best && /\bpedicura\b/.test(low)) return "PEDICURA_SPA_CON_ESMALTE_SEMIPERMANENTE"
  return score>=1?best:null
}

// Fecha/hora multiidioma (si viene, la usamos como preferencia; si no, buscamos la primera libre)
const DOW = {lunes:1,martes:2,miercoles:3,miércoles:3,jueves:4,viernes:5,monday:1,tuesday:2,wednesday:3,thursday:4,friday:5}
function parseDateTimeMulti(text){
  if(!text) return null
  const t=rm(String(text||"").toLowerCase())
  // dd/mm(/yy)
  const m=t.match(/\b(\d{1,2})[\/\-](\d{1,2})(?:[\/\-](\d{2,4}))?\b/)
  let base=null
  if(m){
    let dd=+m[1], mm=+m[2], yy=m[3]?+m[3]:dayjs().year()
    if(yy<100) yy+=2000
    base=EURO(`${yy}-${String(mm).padStart(2,"0")}-${String(dd).padStart(2,"0")} 00:00`)
  }else{
    for(const [w,dow] of Object.entries(DOW)){
      if(t.includes(w)){
        const now=EURO(dayjs()); let delta=(dow-now.day()+7)%7
        if(delta===0 && now.hour()>=SHIFT_B_END) delta=7
        base=now.startOf("day").add(delta,"day"); break
      }
    }
    if(!base && /\b(hoy|today)\b/.test(t)) base=EURO(dayjs().startOf("day"))
    if(!base && /\b(ma(n|ñ)ana|tomorrow)\b/.test(t)) base=EURO(dayjs().add(1,"day").startOf("day"))
    if(!base) base=EURO(dayjs().startOf("day"))
  }
  const hm=t.match(/(?:a\s+las\s+)?(\d{1,2})(?::(\d{2}))?\s*(am|pm)?\b/)
  if(!hm) return null
  let h=+hm[1], m2=hm[2]?+hm[2]:0; const ap=hm[3]
  if(ap==="pm" && h<12) h+=12
  if(ap==="am" && h===12) h=0
  return clampFuture(base.hour(h).minute(m2))
}

// ============= ENV servicios por local
function pickServiceEnvPair(serviceKey, locationId){
  const envName = (locationId===LOCATION_IDS.LA_LUZ) ? `SQ_SVC_luz_${serviceKey}` : `SQ_SVC_${serviceKey}`
  const raw = process.env[envName]
  if(!raw) return null
  const [id, verStr] = raw.split("|")
  const duration = SERVICE[serviceKey]?.dur ?? 60
  const name = SERVICE[serviceKey]?.name ?? serviceKey
  return { id, version: verStr?Number(verStr):undefined, duration, name }
}

// ============= Empleadas por local
function loadTeamByLocation(){
  const byLoc = { [LOCATION_IDS.TORREMOLINOS]:[], [LOCATION_IDS.LA_LUZ]:[] }
  for(const [k,v] of Object.entries(process.env)){
    if(!k.startsWith("SQ_EMP_")) continue
    const [teamId, bookable, locs] = String(v).split("|")
    if(bookable!=="BOOKABLE") continue
    if(locs==="ALL"){ byLoc[LOCATION_IDS.TORREMOLINOS].push(teamId); byLoc[LOCATION_IDS.LA_LUZ].push(teamId); continue }
    (locs||"").split(",").map(s=>s.trim()).forEach(L=>{ if(byLoc[L]) byLoc[L].push(teamId) })
  }
  return byLoc
}
const TEAM_BY_LOC = loadTeamByLocation()

// ============= Square helpers
async function getServiceVariationVersion(id){
  try{
    const r = await square.catalogApi.retrieveCatalogObject(id, true)
    return r?.result?.object?.version
  }catch(e){ console.error("catalog version:", e?.message||e); return undefined }
}

async function findOrCreateCustomer({ name, email, phone }){
  try{
    const phoneE164 = normalizePhoneES(phone)
    if(phoneE164){
      const s = await square.customersApi.searchCustomers({ query:{ filter:{ phoneNumber:{ exact: phoneE164 } } } })
      const found=(s?.result?.customers||[])[0]; if(found) return found
    }
  }catch(e){ /* ignore */ }
  try{
    const resp = await square.customersApi.createCustomer({
      idempotencyKey:`cust_${Date.now()}_${Math.random().toString(36).slice(2,8)}`,
      givenName: name || undefined,
      emailAddress: email || undefined,
      phoneNumber: normalizePhoneES(phone) || undefined,
      note: "Creado por bot WhatsApp Gapink Nails"
    })
    return resp?.result?.customer||null
  }catch(e){ console.error("createCustomer:", e?.message||e); return null }
}

async function searchNextAvailability({ locationId, serviceKey, afterEU, preferredTeamId=null }){
  const pair = pickServiceEnvPair(serviceKey, locationId)
  if(!pair?.id) return null
  const startEU = clampFuture(afterEU || EURO(dayjs()))
  const endEU   = startEU.clone().add(SEARCH_WINDOW_DAYS,"day").endOf("day")
  const teamAny = preferredTeamId ? [preferredTeamId] : (TEAM_BY_LOC[locationId]||[])
  try{
    const body = {
      query:{
        filter:{
          startAtRange:{ startAt: startEU.tz("UTC").toISOString(), endAt: endEU.tz("UTC").toISOString() },
          locationId,
          segmentFilters:[{
            serviceVariationId: pair.id,
            ...(teamAny.length?{ teamMemberIdFilter:{ any: teamAny } }:{})
          }]
        }
      }
    }
    const resp = await square.bookingsApi.searchAvailability(body)
    const list = (resp?.result?.availabilities||[])
      .filter(a => {
        const t = EURO(a.startAt)
        return insideBusinessHours(t)
      })
      .map(a => ({
        startEU: EURO(a.startAt),
        teamId: a.appointmentSegments?.[0]?.teamMemberId || null,
        duration: a.appointmentSegments?.[0]?.durationMinutes || pair.duration,
        svId: a.appointmentSegments?.[0]?.serviceVariationId || pair.id,
        svVersion: a.appointmentSegments?.[0]?.serviceVariationVersion || pair.version
      }))
    return list[0] || null
  }catch(e){ console.error("searchAvailability:", e?.message||e); return null }
}

async function createBookingSquare({ locationId, customerId, serviceKey, startEU, teamMemberId }){
  const pair = pickServiceEnvPair(serviceKey, locationId)
  if(!pair?.id) return null
  const version = pair.version || await getServiceVariationVersion(pair.id)
  if(!version) return null
  try{
    const body = {
      idempotencyKey: `bk_${locationId}_${pair.id}_${startEU.valueOf()}_${teamMemberId||"any"}`,
      booking:{
        locationId,
        startAt: startEU.tz("UTC").toISOString(),
        customerId,
        appointmentSegments:[{
          teamMemberId: teamMemberId || undefined,
          serviceVariationId: pair.id,
          serviceVariationVersion: Number(version),
          durationMinutes: pair.duration
        }]
      }
    }
    const r = await square.bookingsApi.createBooking(body)
    return r?.result?.booking || null
  }catch(e){ console.error("createBooking:", e?.message||e); return null }
}

async function cancelBookingSquare(bookingId){
  try{
    const g = await square.bookingsApi.retrieveBooking(bookingId)
    const ver = g?.result?.booking?.version
    if(!ver) return false
    const r = await square.bookingsApi.cancelBooking(bookingId, { idempotencyKey:`cancel_${bookingId}_${Date.now()}`, bookingVersion: ver })
    return !!r?.result?.booking?.id
  }catch(e){ console.error("cancelBooking:", e?.message||e); return false }
}

// ============= DB mínima
const db=new Database("gapink.db"); db.pragma("journal_mode = WAL")
db.exec(`
CREATE TABLE IF NOT EXISTS appointments(
  id TEXT PRIMARY KEY,
  phone TEXT, name TEXT, email TEXT,
  location_id TEXT,
  service_key TEXT, service_name TEXT, duration_min INTEGER,
  staff_id TEXT,
  start_iso TEXT,
  square_booking_id TEXT,
  status TEXT,
  created_at TEXT
);
CREATE TABLE IF NOT EXISTS sessions(
  phone TEXT PRIMARY KEY,
  json TEXT,
  updated_at TEXT
);
CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v TEXT);
`)
const sessGet = db.prepare(`SELECT json FROM sessions WHERE phone=@phone`)
const sessSet = db.prepare(`INSERT INTO sessions(phone,json,updated_at) VALUES(@phone,@json,@u) ON CONFLICT(phone) DO UPDATE SET json=excluded.json, updated_at=excluded.u`)
const sessDel = db.prepare(`DELETE FROM sessions WHERE phone=@phone`)
const apptInsert = db.prepare(`INSERT INTO appointments(id,phone,name,email,location_id,service_key,service_name,duration_min,staff_id,start_iso,square_booking_id,status,created_at)
VALUES(@id,@phone,@name,@email,@location_id,@service_key,@service_name,@duration_min,@staff_id,@start_iso,@square_booking_id,@status,@created_at)`)

function loadSession(phone){
  const r=sessGet.get({phone}); if(!r?.json) return null
  const d=JSON.parse(r.json); if(d.startEU_ms) d.startEU = EURO(Number(d.startEU_ms))
  return d
}
function saveSession(phone,data){
  const d={...data}; d.startEU_ms = data.startEU?.valueOf?.() ?? null; delete d.startEU
  sessSet.run({ phone, json: JSON.stringify(d), u: new Date().toISOString() })
}

// ============= Mini web (QR)
const app=express()
const PORT=process.env.PORT||8080
let lastQR=null, conectado=false
app.get("/",(_req,res)=>res.send(`<!doctype html><meta charset="utf-8"><style>body{font-family:system-ui;display:grid;place-items:center;min-height:100vh;background:#fff0f6} .c{background:#fff;padding:24px;border-radius:16px;box-shadow:0 10px 30px rgba(0,0,0,.08);}</style><div class="c"><h1>Gapink Nails</h1><p>${conectado?"✅ WhatsApp conectado":"❌ No conectado"}</p>${!conectado&&lastQR?`<img src="/qr.png" width="320"/>`:""}</div>`))
app.get("/qr.png",async(_req,res)=>{ if(!lastQR) return res.status(404).end(); const png=await qrcode.toBuffer(lastQR,{type:"png",width:512,margin:1}); res.set("Content-Type","image/png").send(png) })
app.listen(PORT,()=>startBot().catch(console.error))

// ============= WhatsApp bot
const wait=(ms)=>new Promise(r=>setTimeout(r,ms))
async function startBot(){
  try{
    if(!fs.existsSync("auth_info")) fs.mkdirSync("auth_info",{recursive:true})
    const { state, saveCreds } = await useMultiFileAuthState("auth_info")
    const { version } = await fetchLatestBaileysVersion()
    const sock = makeWASocket({ logger:pino({level:"silent"}), auth:state, version, browser:Browsers.macOS("Desktop"), printQRInTerminal:false })

    sock.ev.on("connection.update",({connection,qr})=>{
      if(qr){ lastQR=qr; try{ qrcodeTerminal.generate(qr,{small:true}) }catch{} }
      if(connection==="open"){ conectado=true; lastQR=null; console.log("✅ WA conectado") }
      if(connection==="close"){ conectado=false; console.log("❌ WA desconectado, reintento…"); setTimeout(()=>startBot().catch(console.error), 2000) }
    })
    sock.ev.on("creds.update",saveCreds)

    sock.ev.on("messages.upsert", async ({ messages })=>{
      const m = messages?.[0]; if(!m?.message || m.key.fromMe) return
      const from = m.key.remoteJid
      const phone = normalizePhoneES((from||"").split("@")[0]||"")||(from||"").split("@")[0]
      const txt = (m.message.conversation || m.message.extendedTextMessage?.text || m.message?.imageMessage?.caption || "").trim()
      if(!txt) return

      // Sesión
      let s = loadSession(phone) || { welcomeSent:false, locationKey:null, serviceKey:null, startEU:null, durationMin:null, staffId:null, name:null, email:null, confirmPending:false }
      const low = rm(txt.toLowerCase())

      // Bienvenida (siempre primero una única vez)
      if(!s.welcomeSent){
        await sock.sendMessage(from,{ text: WELCOME_TEXT })
        s.welcomeSent = true; saveSession(phone,s)
        // seguimos para procesar el propio mensaje del cliente
      }

      // Horario de atención por WhatsApp (si está fuera, no seguimos, para no “atender”)
      const now=EURO(dayjs())
      const openNow = insideBusinessHours(now)
      if(!openNow){
        // Si preguntan explícitamente por horario/abierto, respondemos seguro:
        if(/\b(abiertos?|open|horario|hours?)\b/i.test(low)){
          await sock.sendMessage(from,{ text:"Atendemos por WhatsApp L–V 10:00–14:00 y 16:00–20:00. Puedes reservar por la web en cualquier momento: https://gapinknails.square.site/" })
        }
        return
      }

      // Rápido: cancelar
      if(/\b(cancela(r|me|la)?|anula(r|me|la)?|borra(r|me|la)?|delete|cancel)\b/.test(low)){
        const upc = db.prepare(`SELECT * FROM appointments WHERE phone=@phone AND status='confirmed' AND start_iso>@now ORDER BY start_iso ASC LIMIT 1`).get({ phone, now: dayjs().utc().toISOString() })
        if(upc?.square_booking_id){
          const ok = await cancelBookingSquare(upc.square_booking_id)
          if(ok){ db.prepare(`UPDATE appointments SET status='cancelled' WHERE id=@id`).run({ id: upc.id }); sessDel.run({ phone }); await sock.sendMessage(from,{ text:`He cancelado tu cita del ${fmtES(dayjs(upc.start_iso))}.` }); return }
        }
        await sock.sendMessage(from,{ text:"No veo ninguna cita futura a tu nombre para cancelar ahora mismo." })
        return
      }

      // Detectar local/servicio/datetime si vienen
      const locK = detectLocationFromText(txt); if(locK) { s.locationKey = locK; saveSession(phone,s) }
      const svcK = detectServiceKey(txt); if(svcK){ s.serviceKey=svcK; s.durationMin = SERVICE[svcK]?.dur ?? 60; saveSession(phone,s) }
      const dtPref = parseDateTimeMulti(txt)

      // Si piden precios → IA sin inventar números
      if(/\b(precio|cu[aá]nto|tarifa|vale|coste|costos?)\b/.test(low)){
        const svcName = s.serviceKey ? SERVICE[s.serviceKey].name : "el servicio que te interese"
        const msg = await aiChat([
          { role:"system", content: SYS_TONE },
          { role:"user", content:`Cliente pregunta precio sobre "${svcName}". No inventes cantidades: di que los precios pueden variar según técnica y que en recepción lo confirman. Ofrece coger cita directamente.`}
        ])
        await sock.sendMessage(from,{ text: msg || `Los precios pueden variar según la técnica. Si quieres te cojo cita directamente y allí te lo confirman sin compromiso.` })
        return
      }

      // Si hay servicio pero falta local → pedirlo (no pedimos hora)
      if(s.serviceKey && !s.locationKey){
        await sock.sendMessage(from,{ text:`¿En qué salón te viene mejor, *Málaga – La Luz* o *Torremolinos*?` })
        return
      }

      // Si hay dudas y hablan de uñas/pies genérico sin servicio → menú corto
      if(!s.serviceKey && /\b(u[nñ]as|manicura|pedicura|pies|manos)\b/.test(low)){
        await sock.sendMessage(from,{ text:
`¿Qué necesitas exactamente?
• Manicura semipermanente (o con nivelación / rusa)
• Uñas esculpidas (nuevas) o Relleno
• Pedicura (normal o semipermanente)
• Solo esmaltado en pies

Dime una opción y te cojo la cita sin pedirte hora.` })
        return
      }

      // Si ya hay servicio + local → buscar hueco y PROPONER (no pedir hora)
      if(s.serviceKey && s.locationKey){
        const locationId = LOCATION_IDS[s.locationKey]
        const pair = pickServiceEnvPair(s.serviceKey, locationId)
        if(!pair?.id){ /* por seguridad, no respondemos si el mapeo no existe */ return }

        const seed = (dtPref && insideBusinessHours(dtPref)) ? dtPref : now
        const slot = await searchNextAvailability({ locationId, serviceKey: s.serviceKey, afterEU: seed, preferredTeamId: s.staffId||null })
        if(slot){
          s.startEU = slot.startEU; s.durationMin = slot.duration; s.staffId = slot.teamId || s.staffId || null; s.confirmPending=true; saveSession(phone,s)
          const locName = LOCATION_NAMES[locationId]
          // Si el usuario dijo “manicura” genérica, mostramos “Manicura” (pero internamente es semi)
          const genericMani = /\bmanicura\b/.test(low) && !/rusa|semi|nivel/i.test(low)
          const visibleSvcName = genericMani ? "Manicura" : (SERVICE[s.serviceKey].name)
          await sock.sendMessage(from,{ text:`Te puedo ofrecer *${fmtES(slot.startEU)}* en *${locName}* para *${visibleSvcName}*. ¿Confirmo? (Pago en persona)` })
          return
        } else {
          await sock.sendMessage(from,{ text:`No veo huecos seguros ahora mismo para ese servicio en ${LOCATION_NAMES[locationId]}. Si te vale, te reservo el primer hueco libre en cuanto se abra.` })
          return
        }
      }

      // Confirmaciones
      if(s.confirmPending && /\b(si|sí|vale|ok|okay|confirmo|dale|de acuerdo|perfecto)\b/.test(low)){
        // Pedir nombre/email si faltan
        if(!s.name){ await sock.sendMessage(from,{ text:`Para cerrar, dime tu *nombre y apellidos*.` }); return }
        if(!s.email || !/^[^\s@]+@[^\s@]+\.[^\s@]{2,}$/.test(s.email)){ 
          if(/@/.test(txt) && /^[^\s@]+@[^\s@]+\.[^\s@]{2,}$/.test(txt.trim())){ s.email = txt.trim(); saveSession(phone,s) }
          if(!s.email){ await sock.sendMessage(from,{ text:`Genial. Ahora tu email (tipo: nombre@correo.com).` }); return }
        }
        const locationId = LOCATION_IDS[s.locationKey]
        const cust = await findOrCreateCustomer({ name:s.name, email:s.email, phone })
        if(!cust){ await sock.sendMessage(from,{ text:`No pude crear la ficha con ese email. Envíame uno válido y seguimos.` }); return }
        const booking = await createBookingSquare({ locationId, customerId:cust.id, serviceKey:s.serviceKey, startEU:s.startEU, teamMemberId:s.staffId||undefined })
        if(!booking){ await sock.sendMessage(from,{ text:`Uy, justo han cogido ese hueco. Te busco el siguiente disponible y te lo propongo ahora.` }); s.confirmPending=false; saveSession(phone,s); return }

        // Guardar
        apptInsert.run({
          id:`apt_${Date.now().toString(36)}_${Math.random().toString(36).slice(2,7)}`,
          phone, name:s.name, email:s.email,
          location_id: locationId,
          service_key: s.serviceKey, service_name: SERVICE[s.serviceKey]?.name || s.serviceKey, duration_min: s.durationMin,
          staff_id: s.staffId || null,
          start_iso: s.startEU.tz("UTC").toISOString(),
          square_booking_id: booking.id || null,
          status:"confirmed",
          created_at: new Date().toISOString()
        })
        await sock.sendMessage(from,{ text:
`Reserva confirmada 🎉
Servicio: ${SERVICE[s.serviceKey]?.name || s.serviceKey}
Fecha: ${fmtES(s.startEU)}
Local: ${LOCATION_NAMES[locationId]}
Duración: ${s.durationMin} min
Pago en persona.` })
        sessDel.run({ phone })
        return
      }

      // Captura pasiva de nombre/email si lo mandan sueltos
      if(!s.name && /^[a-zA-ZÁÉÍÓÚÜÑáéíóúüñ]+/.test(txt.trim()) && txt.trim().split(" ").length>=2){
        s.name = txt.trim(); saveSession(phone,s)
        await sock.sendMessage(from,{ text:`Gracias, ${s.name}. Dime el *local* (Málaga – La Luz o Torremolinos) y el *servicio* y te cojo la cita al primer hueco libre.` })
        return
      }
      if(!s.email && /\@/.test(low) && /^[^\s@]+@[^\s@]+\.[^\s@]{2,}$/.test(txt.trim())){
        s.email = txt.trim(); saveSession(phone,s)
        await sock.sendMessage(from,{ text:`Perfecto. ¿La cita la cogemos en *Málaga – La Luz* o en *Torremolinos*? Dime también el servicio.` })
        return
      }

      // Fallback ultraseguro (si la IA no está 100% segura, no inventa nada)
      const fb = await aiChat([
        { role:"system", content: SYS_TONE },
        { role:"user", content:`Mensaje del cliente: "${txt}". Si falta local o servicio, pide SOLO eso en una frase. No des horarios ni precios.` }
      ])
      await sock.sendMessage(from,{ text: fb || "¿Te viene mejor *Málaga – La Luz* o *Torremolinos*? Y dime el servicio (ej.: “manicura semipermanente”)." })
    })
  }catch(e){ console.error("startBot:", e) }
}
