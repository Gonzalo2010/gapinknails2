// index.js — Gapink Nails · IA + Square + Baileys (v11)
// Node 20+. Paquetes: express pino qrcode qrcode-terminal better-sqlite3 dayjs dotenv @square/square @whiskeysockets/baileys

import express from "express"
import pino from "pino"
import qrcode from "qrcode"
import qrcodeTerminal from "qrcode-terminal"
import "dotenv/config"
import fs from "fs"
import { createRequire } from "module"
import Database from "better-sqlite3"
import dayjs from "dayjs"
import utc from "dayjs/plugin/utc.js"
import tz from "dayjs/plugin/timezone.js"
import isoWeek from "dayjs/plugin/isoWeek.js"
import isSameOrAfter from "dayjs/plugin/isSameOrAfter.js"
import isSameOrBefore from "dayjs/plugin/isSameOrBefore.js"
import "dayjs/locale/es.js"
import { Client, Environment } from "square"
import crypto from "crypto"

dayjs.extend(utc); dayjs.extend(tz); dayjs.extend(isoWeek); dayjs.extend(isSameOrAfter); dayjs.extend(isSameOrBefore)
dayjs.locale("es")
const EURO_TZ = "Europe/Madrid"

// ====== Ajustes
const WORK_DAYS = [1,2,3,4,5]
const SHIFT_A_START=10, SHIFT_A_END=14
const SHIFT_B_START=16, SHIFT_B_END=20
const SLOT_MIN=30
const SEARCH_WINDOW_DAYS = +process.env.BOT_SEARCH_WINDOW_DAYS || 14
const NOW_OFFSET_MIN = +process.env.BOT_NOW_OFFSET_MIN || 30
const WELCOME_MUTE_MS = 90_000

const HOLI = new Set(["01/01","06/01","28/02","01/05","15/08","12/10","01/11","06/12","08/12","25/12"])
;(String(process.env.HOLIDAYS_EXTRA||"").split(",").map(s=>s.trim()).filter(Boolean)).forEach(x=>HOLI.add(x))

// ====== Square
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

// ====== Mensaje de bienvenida
const WELCOME_TEXT =
`Gracias por comunicarte con Gapink Nails. Por favor, haznos saber cómo podemos ayudarte.

Solo atenderemos por WhatsApp y llamadas en horario de lunes a viernes de 10 a 14:00 y de 16:00 a 20:00

Si quieres reservar una cita puedes hacerlo a través de este link:

https://gapinknails.square.site/

Y si quieres modificarla puedes hacerlo a través del link del sms que llega con su cita.

Para cualquier otra consulta, déjenos saber y en el horario establecido le responderemos.
Gracias 😘`

// ====== OpenAI (IA)
const OPENAI_MODEL = process.env.OPENAI_MODEL || "gpt-4o-mini"
const OPENAI_URL   = process.env.OPENAI_API_URL || "https://api.openai.com/v1/chat/completions"
async function aiReply(userText, ctx={}){
  if(!process.env.OPENAI_API_KEY) return ""
  const sys = `Eres recepcionista de Gapink Nails (España). Español. Breve, cálida, clara.
No inventes horarios/precios ni huecos. Si no tienes 100% la info, pide el dato.
ESTADO: local=${ctx.haveLoc?"sí":"no"}, servicio=${ctx.haveSvc?"sí":"no"}, pestañas=${ctx.lash?"sí":"no"}.
REGLAS:
- Si el cliente pide "pestañas" sin especificar, pregunta el tipo: "Lifting + tinte", "Extensiones NUEVAS (pelo a pelo/2D/3D)" o "Relleno (pelo a pelo/2D/3D)" o "Quitar extensiones". 
- Si ya hay local, NO lo vuelvas a pedir; solo pide la variante de pestañas.
- Si ya hay variante, confirma día/hora preferidos.`
  try{
    const r = await fetch(OPENAI_URL, {
      method:"POST",
      headers:{ "Content-Type":"application/json", "Authorization":`Bearer ${process.env.OPENAI_API_KEY}` },
      body: JSON.stringify({
        model: OPENAI_MODEL,
        temperature: 0.2,
        messages:[
          { role:"system", content: sys },
          { role:"user", content: userText }
        ]
      })
    })
    const j = await r.json()
    return (j?.choices?.[0]?.message?.content||"").trim()
  }catch{ return "" }
}

// ====== Utils
const rm = s => String(s||"").normalize("NFD").replace(/\p{Diacritic}/gu,"")
const norm = s => rm(s).toLowerCase()
const EURO = d => dayjs(d).tz(EURO_TZ)
const fmtES = d => {
  const t=EURO(d); const dias=["domingo","lunes","martes","miércoles","jueves","viernes","sábado"]
  const DD=String(t.date()).padStart(2,"0"), MM=String(t.month()+1).padStart(2,"0"), HH=String(t.hour()).padStart(2,"0"), mm=String(t.minute()).padStart(2,"0")
  return `${dias[t.day()]} ${DD}/${MM} ${HH}:${mm}`
}
const isHoliday = d => {
  const dd=String(d.date()).padStart(2,"0"), mm=String(d.month()+1).padStart(2,"0")
  return HOLI.has(`${dd}/${mm}`) || !WORK_DAYS.includes(d.day())
}
const withinBusiness = d=>{
  if(isHoliday(d)) return false
  const h=d.hour()+d.minute()/60
  return (h>=SHIFT_A_START&&h<=SHIFT_A_END) || (h>=SHIFT_B_START&&h<=SHIFT_B_END)
}
const ceilSlot = t=>{
  const r=t.minute()%SLOT_MIN; return r? t.add(SLOT_MIN-r,"minute").second(0).millisecond(0):t.second(0).millisecond(0)
}
const safeFuture = t=>ceilSlot(EURO(t||dayjs()).add(NOW_OFFSET_MIN,"minute"))
const phoneE164 = raw=>{
  const d=(raw||"").replace(/\D+/g,""); if(!d) return null
  if(raw.startsWith("+")) return `+${d}`
  if(d.startsWith("34") && d.length===11) return `+${d}`
  if(d.length===9) return `+34${d}`
  return `+${d}`
}

// ====== NLP local/servicio/fecha
const LOC_SYNON = {
  TORREMOLINOS:["torremolinos","playamar","benyamina","benyamaina","benyamina 18"],
  LA_LUZ:["la luz","malaga","málaga","centro","huelin","carretera cadiz","carretera cádiz","barrio de la luz","cruz de humilladero"]
}
function detectLocation(text){
  const t=norm(text)
  for(const [k,arr] of Object.entries(LOC_SYNON)) if(arr.some(w=>t.includes(norm(w)))) return k
  return null
}

const SERVICE = {
  MANICURA_SEMIPERMANENTE:{ name:"Manicura semipermanente", dur:30 },
  MANICURA_SEMIPERMANENTE_QUITAR:{ name:"Manicura semipermanente + quitar", dur:40 },
  MANICURA_SEMIPERMANETE_CON_NIVELACION:{ name:"Manicura semipermanente con nivelación", dur:60 },
  MANICURA_RUSA_CON_NIVELACION:{ name:"Manicura rusa con nivelación", dur:90 },
  MANICURA_CON_ESMALTE_NORMAL:{ name:"Manicura con esmalte normal", dur:30 },
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
  MICROBLADING:{ name:"Microblading", dur:120 },
  DERMAPEN:{ name:"Dermapen", dur:60 },
  MASAJE_RELAJANTE:{ name:"Masaje relajante", dur:60 }
}
const SVC_SYNON=[
  ["manicura semipermanente","MANICURA_SEMIPERMANENTE",["semi","gel color","permanente"]],
  ["quitar semi","MANICURA_SEMIPERMANENTE_QUITAR",["retirar semi","quitar gel"]],
  ["manicura rusa","MANICURA_RUSA_CON_NIVELACION",["russian"]],
  ["manicura normal","MANICURA_CON_ESMALTE_NORMAL",[]],
  ["nivelacion","MANICURA_SEMIPERMANETE_CON_NIVELACION",["nivelación"]],
  ["uñas nuevas","UNAS_NUEVAS_ESCULPIDAS",["acrílicas nuevas","acrilicas nuevas"]],
  ["relleno uñas","RELLENO_UNAS_ESCULPIDAS",["relleno acrílicas"]],
  ["pedicura semi","PEDICURA_SPA_CON_ESMALTE_SEMIPERMANENTE",["semi pies"]],
  ["pedicura normal","PEDICURA_SPA_CON_ESMALTE_NORMAL",[]],
  ["hilo cejas","DEPILACION_CEJAS_CON_HILO",["threading"]],
  ["diseño cejas henna","DISENO_DE_CEJAS_CON_HENNA_Y_DEPILACION",[]],
  ["lifting pestañas","LIFITNG_DE_PESTANAS_Y_TINTE",["lash lift","lifting de pestañas","lifting y tinte"]],
  ["pelo a pelo","EXTENSIONES_DE_PESTANAS_NUEVAS_PELO_A_PELO",["clásicas","clasicas","classic"]],
  ["extensiones 2d","EXTENSIONES_PESTANAS_NUEVAS_2D",["2d nuevas","2 d"]],
  ["extensiones 3d","EXTENSIONES_PESTANAS_NUEVAS_3D",["3d nuevas","3 d"]],
  ["relleno pelo a pelo","RELLENO_EXTENSIONES_PESTANAS_PELO_A_PELO",[]],
  ["relleno 2d","RELLENO_PESTANAS_2D",[]],
  ["relleno 3d","RELLENO_PESTANAS_3D",[]],
  ["microblading","MICROBLADING",[]],
  ["dermapen","DERMAPEN",[]],
  ["masaje","MASAJE_RELAJANTE",[]]
]
function detectService(text){
  const t=norm(text)
  for(const [label,key,extra] of SVC_SYNON)
    if(t.includes(norm(label)) || (extra||[]).some(x=>t.includes(norm(x)))) return key
  // extensiones genéricas => aún falta variante
  return null
}
const DOW = {lunes:1,martes:2,miercoles:3,miércoles:3,jueves:4,viernes:5}
function parseDateTime(text){
  if(!text) return null
  const t=norm(text)
  const m=t.match(/\b(\d{1,2})[\/\-](\d{1,2})(?:[\/\-](\d{2,4}))?\b/)
  let base=null
  if(m){
    let dd=+m[1], mm=+m[2], yy=m[3]?+m[3]:dayjs().year(); if(yy<100) yy+=2000
    base=EURO(`${yy}-${String(mm).padStart(2,"0")}-${String(dd).padStart(2,"0")} 00:00`)
  }else{
    for(const [w,d] of Object.entries(DOW)) if(t.includes(w)){ 
      let delta=(d-EURO(dayjs()).day()+7)%7; if(delta===0 && EURO(dayjs()).hour()>=SHIFT_B_END) delta=7
      base=EURO(dayjs()).startOf("day").add(delta,"day"); break
    }
    if(!base && t.includes("hoy")) base=EURO(dayjs().startOf("day"))
    if(!base && (t.includes("mañana")||t.includes("manana"))) base=EURO(dayjs().add(1,"day").startOf("day"))
    if(!base) base=EURO(dayjs().startOf("day"))
  }
  const hm=t.match(/(?:a\s+las\s+)?(\d{1,2})(?::(\d{2}))?\s*(am|pm)?\b/)
  if(!hm) return null
  let h=+hm[1], m2=hm[2]?+hm[2]:0; const ap=hm[3]
  if(ap==="pm"&&h<12) h+=12; if(ap==="am"&&h===12) h=0
  return safeFuture(base.hour(h).minute(m2))
}

// ====== ENV servicios por local (prefijo luz_)
function svcEnvPair(serviceKey, locationId){
  const name = (locationId===LOCATION_IDS.LA_LUZ)? `SQ_SVC_luz_${serviceKey}` : `SQ_SVC_${serviceKey}`
  const raw = process.env[name]; if(!raw) return null
  const [id, verStr] = raw.split("|")
  const duration = SERVICE[serviceKey]?.dur ?? 60
  return { id, version: verStr?Number(verStr):undefined, duration, name: SERVICE[serviceKey]?.name||serviceKey }
}

// ====== Square helpers
function explainSquareError(e){
  try{
    const err = e?.errors || e?.result?.errors || e?.response?.body || e
    console.error("Square error:", JSON.stringify(err,null,2))
  }catch{}
}
async function getServiceVersion(id){
  try{
    const r = await square.catalogApi.retrieveCatalogObject(id, true)
    return r?.result?.object?.version
  }catch(e){ explainSquareError(e); return undefined }
}
async function findOrCreateCustomer({ name, email, phone }){
  try{
    const pn = phoneE164(phone)
    if(pn){
      const s = await square.customersApi.searchCustomers({ query:{ filter:{ phoneNumber:{ exact: pn } } } })
      const c=(s?.result?.customers||[])[0]; if(c) return c
    }
  }catch(e){}
  try{
    const r = await square.customersApi.createCustomer({
      idempotencyKey: crypto.randomUUID(),
      givenName: name||undefined,
      emailAddress: email||undefined,
      phoneNumber: phoneE164(phone)||undefined,
      note: "Creado por bot WhatsApp"
    })
    return r?.result?.customer||null
  }catch(e){ explainSquareError(e); return null }
}
async function searchNextAvailability({ locationId, serviceKey, afterEU, preferredTeamId=null }){
  const pair = svcEnvPair(serviceKey, locationId)
  if(!pair?.id) return null
  const startEU = safeFuture(afterEU||dayjs())
  const endEU = startEU.clone().add(SEARCH_WINDOW_DAYS,"day").endOf("day")
  const baseFilter = {
    startAtRange:{ startAt:startEU.tz("UTC").toISOString(), endAt:endEU.tz("UTC").toISOString() },
    locationId,
    segmentFilters:[{ serviceVariationId: pair.id }]
  }
  try{
    const r = await square.bookingsApi.searchAvailability({ query:{ filter: baseFilter } })
    const list = (r?.result?.availabilities||[])
      .filter(a=>withinBusiness(EURO(a.startAt)))
      .map(a=>({
        startEU: EURO(a.startAt),
        teamId: a.appointmentSegments?.[0]?.teamMemberId||null,
        duration: a.appointmentSegments?.[0]?.durationMinutes||pair.duration,
        svId: a.appointmentSegments?.[0]?.serviceVariationId||pair.id,
        svVersion: a.appointmentSegments?.[0]?.serviceVariationVersion||pair.version
      }))
    if(list.length) return list[0]
  }catch(e){
    console.error("searchAvailability(ANY) error"); explainSquareError(e)
  }
  if(preferredTeamId){
    try{
      const r2 = await square.bookingsApi.searchAvailability({
        query:{ filter:{ ...baseFilter, segmentFilters:[{ serviceVariationId: pair.id, teamMemberIdFilter:{ any:[preferredTeamId] } }] } }
      })
      const li=(r2?.result?.availabilities||[]).filter(a=>withinBusiness(EURO(a.startAt)))
      if(li.length) return {
        startEU: EURO(li[0].startAt),
        teamId: li[0].appointmentSegments?.[0]?.teamMemberId||preferredTeamId,
        duration: li[0].appointmentSegments?.[0]?.durationMinutes||pair.duration,
        svId: pair.id,
        svVersion: li[0].appointmentSegments?.[0]?.serviceVariationVersion||pair.version
      }
    }catch(e){ console.error("searchAvailability(PREFERRED) error"); explainSquareError(e) }
  }
  return null
}
async function createBooking({ locationId, customerId, serviceKey, startEU, teamMemberId }){
  const pair = svcEnvPair(serviceKey, locationId); if(!pair?.id) return null
  const version = pair.version || await getServiceVersion(pair.id); if(!version) return null
  try{
    const r = await square.bookingsApi.createBooking({
      idempotencyKey: crypto.randomUUID(),
      booking:{
        locationId,
        startAt: startEU.tz("UTC").toISOString(),
        customerId,
        appointmentSegments:[{
          teamMemberId: teamMemberId||undefined,
          serviceVariationId: pair.id,
          serviceVariationVersion: Number(version),
          durationMinutes: pair.duration
        }]
      }
    })
    return r?.result?.booking||null
  }catch(e){ explainSquareError(e); return null }
}
async function cancelBooking(bookingId){
  try{
    const g = await square.bookingsApi.retrieveBooking(bookingId)
    const ver = g?.result?.booking?.version
    if(!ver) return false
    const r = await square.bookingsApi.cancelBooking(bookingId, { idempotencyKey: crypto.randomUUID(), bookingVersion: ver })
    return !!r?.result?.booking?.id
  }catch(e){ explainSquareError(e); return false }
}

// ====== DB
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
CREATE TABLE IF NOT EXISTS sessions(phone TEXT PRIMARY KEY, json TEXT, updated_at TEXT);
`)
const sessGet=db.prepare(`SELECT json FROM sessions WHERE phone=?`)
const sessSet=db.prepare(`INSERT INTO sessions(phone,json,updated_at) VALUES(?,?,?)
ON CONFLICT(phone) DO UPDATE SET json=excluded.json, updated_at=excluded.updated_at`)
const sessDel=db.prepare(`DELETE FROM sessions WHERE phone=?`)
const apptInsert=db.prepare(`INSERT INTO appointments(id,phone,name,email,location_id,service_key,service_name,duration_min,staff_id,start_iso,square_booking_id,status,created_at)
VALUES(@id,@phone,@name,@email,@location_id,@service_key,@service_name,@duration_min,@staff_id,@start_iso,@square_booking_id,@status,@created_at)`)

// ====== Mini web (QR)
const app=express(); const PORT=process.env.PORT||8080
let lastQR=null, conectado=false
app.get("/",(_req,res)=>res.send(`<!doctype html><meta charset="utf-8"><style>body{font-family:system-ui;display:grid;place-items:center;min-height:100vh;background:#fff0f6} .c{background:#fff;padding:24px;border-radius:16px;box-shadow:0 10px 30px rgba(0,0,0,.08)}</style><div class="c"><h1>Gapink Nails</h1><p>${conectado?"✅ WhatsApp conectado":"❌ No conectado"}</p>${!conectado&&lastQR?`<img src="/qr.png" width="320"/>`:""}</div>`))
app.get("/qr.png",async(_req,res)=>{ if(!lastQR) return res.status(404).end(); const png=await qrcode.toBuffer(lastQR,{type:"png",width:512,margin:1}); res.set("Content-Type","image/png").send(png) })
app.listen(PORT,()=>startBot().catch(console.error))

// ====== Baileys loader
async function loadBaileys(){
  const require = createRequire(import.meta.url)
  let mod=null; try{ mod=require("@whiskeysockets/baileys") }catch{}
  if(!mod) try{ mod=await import("@whiskeysockets/baileys") }catch{}
  if(!mod) throw new Error("Falta @whiskeysockets/baileys")
  const def = mod.default
  const makeWASocket = mod.makeWASocket || (typeof def==="function"?def:def?.makeWASocket)
  const useMultiFileAuthState = mod.useMultiFileAuthState || def?.useMultiFileAuthState
  const fetchLatestBaileysVersion = mod.fetchLatestBaileysVersion || def?.fetchLatestBaileysVersion
  const Browsers = mod.Browsers || def?.Browsers || { macOS:(n="Desktop")=>["MacOS",n,"121.0.0"] }
  if(typeof makeWASocket!=="function"||typeof useMultiFileAuthState!=="function") throw new Error("Baileys incompatible")
  return { makeWASocket, useMultiFileAuthState, fetchLatestBaileysVersion, Browsers }
}

// ====== Bot helpers (pestañas)
const ASK_LASH_TEXT = (haveLoc)=>
`${haveLoc?"" : "¿En qué salón te viene mejor, *Málaga – La Luz* o *Torremolinos*?\n\n"}¿Qué servicio de *pestañas* necesitas?
• *Lifting + tinte*
• *Extensiones nuevas*: pelo a pelo (clásicas) / 2D / 3D
• *Relleno*: pelo a pelo / 2D / 3D
• *Quitar* extensiones

Escribe por ejemplo: "Extensiones 2D", "Relleno pelo a pelo" o "Lifting + tinte".`

const BOOKING_INTENT = /\b(cita|reserv|hora|pesta(?:n|ñ)as|extensi|lifting|lash|u(?:n|ñ)as|manicura|pedicura|cejas|microblading|facial|limpieza|masaje|depilaci(?:o|ó)n|laser|l[aá]ser)\b/i

async function startBot(){
  try{
    const { makeWASocket, useMultiFileAuthState, fetchLatestBaileysVersion, Browsers } = await loadBaileys()
    if(!fs.existsSync("auth_info")) fs.mkdirSync("auth_info",{recursive:true})
    const { state, saveCreds } = await useMultiFileAuthState("auth_info")
    const { version } = await fetchLatestBaileysVersion().catch(()=>({version:[2,3000,0]}))
    const sock = makeWASocket({ logger:pino({level:"silent"}), auth:state, version, browser: Browsers.macOS("Chrome"), printQRInTerminal:false, syncFullHistory:false })
    sock.ev.on("connection.update",({connection,qr})=>{
      if(qr){ lastQR=qr; try{ qrcodeTerminal.generate(qr,{small:true}) }catch{} }
      if(connection==="open"){ conectado=true; lastQR=null; console.log("✅ WA conectado") }
      if(connection==="close"){ conectado=false; console.log("❌ WA desconectado, reintento…"); setTimeout(()=>startBot().catch(console.error),2000) }
    })
    sock.ev.on("creds.update", saveCreds)

    // ====== Sesiones
    const sessGet=db.prepare(`SELECT json FROM sessions WHERE phone=?`)
    const loadSession=(phone)=>{ const r=sessGet.get(phone); if(!r?.json) return null; const d=JSON.parse(r.json); if(d.startEU_ms) d.startEU=EURO(Number(d.startEU_ms)); return d }
    const sessSet=db.prepare(`INSERT INTO sessions(phone,json,updated_at) VALUES(?,?,?)
    ON CONFLICT(phone) DO UPDATE SET json=excluded.json, updated_at=excluded.updated_at`)
    const saveSession=(phone,data)=>{ const d={...data}; d.startEU_ms=data.startEU?.valueOf?.()??null; delete d.startEU; sessSet.run(phone, JSON.stringify(d), new Date().toISOString()) }
    const sessDel=db.prepare(`DELETE FROM sessions WHERE phone=?`)

    // ====== Mensajes
    sock.ev.on("messages.upsert", async ({ messages })=>{
      const m = messages?.[0]; if(!m?.message || m.key.fromMe) return
      const from = m.key.remoteJid
      const phone = phoneE164((from||"").split("@")[0]||"")||(from||"").split("@")[0]
      const text = (m.message.conversation || m.message.extendedTextMessage?.text || m.message?.imageMessage?.caption || "").trim()
      if(!text) return

      let s = loadSession(phone) || { welcomeSent:false, welcomeAtMs:0, locationKey:null, serviceKey:null, startEU:null, durationMin:null, staffId:null, name:null, email:null, confirm:false }
      const tlow = norm(text)

      // bienvenida
      if(!s.welcomeSent){
        await sock.sendMessage(from,{ text: WELCOME_TEXT })
        s.welcomeSent=true; s.welcomeAtMs=Date.now(); saveSession(phone,s)
      }

      const now=EURO(dayjs()); const openNow=withinBusiness(now)
      const dtPref = parseDateTime(text)
      const locTxt = detectLocation(text); if(locTxt){ s.locationKey=locTxt; saveSession(phone,s) }
      const svcTxt = detectService(text); if(svcTxt){ s.serviceKey=svcTxt; s.durationMin = SERVICE[svcTxt]?.dur||60; saveSession(phone,s) }

      const mentionsLash = /\bpesta(?:n|ñ)as\b|lash|lifting/.test(tlow)
      const mentionsExt  = /extensi(?:o|ó)nes/.test(tlow)
      const mentionsRell = /\brelleno\b/.test(tlow)
      const hasVariant   = /(pelo a pelo|2d|3d|lifting)/.test(tlow)

      const inFlow = !!(s.locationKey || s.serviceKey || dtPref || s.confirm || mentionsLash)
      const intent = BOOKING_INTENT.test(text) || inFlow

      // fuera de horario si no hay intención
      if(!openNow && !intent){
        if(!s.welcomeAtMs || (Date.now()-s.welcomeAtMs)>WELCOME_MUTE_MS){
          await sock.sendMessage(from,{ text:"Ahora estamos fuera de horario. Si necesitas una cita, dímela y te la gestiono igual 😊 (o usa https://gapinknails.square.site/)." })
          s.welcomeAtMs=Date.now(); saveSession(phone,s)
        }
        return
      }

      // cancelar
      if(/\b(cancela|anula|cancel|borra)\b/i.test(tlow)){
        const upc = db.prepare(`SELECT * FROM appointments WHERE phone=? AND status='confirmed' AND start_iso>? ORDER BY start_iso ASC LIMIT 1`).get(phone, dayjs().utc().toISOString())
        if(upc?.square_booking_id){
          const ok = await cancelBooking(upc.square_booking_id)
          if(ok){ db.prepare(`UPDATE appointments SET status='cancelled' WHERE id=?`).run(upc.id); sessDel.run(phone); await sock.sendMessage(from,{ text:`He cancelado tu cita del ${fmtES(upc.start_iso)}.` }); return }
        }
        await sock.sendMessage(from,{ text:"No veo ninguna cita futura a tu nombre para cancelar ahora mismo." })
        return
      }

      // ——— PREGUNTA ESPECÍFICA DE PESTAÑAS ———
      // 1) Dice pestañas sin variante
      if(mentionsLash && !s.serviceKey && !hasVariant){
        await sock.sendMessage(from,{ text: ASK_LASH_TEXT(!!s.locationKey) })
        return
      }
      // 2) Dice extensiones o relleno sin 1D/2D/3D
      if((mentionsExt || mentionsRell) && !hasVariant && !s.serviceKey){
        await sock.sendMessage(from,{ text: ASK_LASH_TEXT(!!s.locationKey) })
        return
      }
      // 3) Dice "naturales" sin variante
      if(mentionsLash && /naturales?/.test(tlow) && !s.serviceKey){
        await sock.sendMessage(from,{ text:`Para un acabado natural solemos recomendar *pelo a pelo* o *3D con pelo muy fino*. ¿Cuál prefieres?` })
        return
      }

      // pedir datos que falten (no insistir en local si ya lo dijo)
      if(!s.serviceKey && /\b(uñas|manicura|pedicura|pesta|cejas|facial|masaje)\b/i.test(tlow) && !mentionsLash){
        const prefix = s.locationKey ? "" : "¿En qué salón te viene mejor, *Málaga – La Luz* o *Torremolinos*?\n\n"
        await sock.sendMessage(from,{ text:
`${prefix}¿Qué necesitas exactamente?
• Manicura semipermanente (con o sin nivelación / rusa)
• Uñas esculpidas (nuevas) o Relleno
• Pedicura (normal o semipermanente)
• Lifting de pestañas o Extensiones (pelo a pelo / 2D / 3D)
• Cejas (diseño con henna / hilo)

Dime una opción.` })
        return
      }
      if(s.serviceKey && !s.locationKey){
        await sock.sendMessage(from,{ text:`¿En qué salón te viene mejor, *Málaga – La Luz* o *Torremolinos*?` })
        return
      }
      // Si tiene local pero pidió pestañas genérico, volver a menú de pestañas
      if(s.locationKey && !s.serviceKey && mentionsLash){
        await sock.sendMessage(from,{ text: ASK_LASH_TEXT(true) })
        return
      }

      // propuesta de hueco
      if(s.serviceKey && s.locationKey){
        const locId = LOCATION_IDS[s.locationKey]
        const pair = svcEnvPair(s.serviceKey, locId)
        if(!pair?.id){ await sock.sendMessage(from,{ text:`Ese servicio no está disponible en ese salón ahora mismo.` }); return }
        const seed = (dtPref && withinBusiness(dtPref)) ? dtPref : now
        const slot = await searchNextAvailability({ locationId:locId, serviceKey:s.serviceKey, afterEU:seed, preferredTeamId:s.staffId||null })
        if(slot){
          s.startEU=slot.startEU; s.durationMin=slot.duration; s.staffId=slot.teamId||null; s.confirm=true; saveSession(phone,s)
          await sock.sendMessage(from,{ text:`Te puedo ofrecer *${fmtES(slot.startEU)}* en *${LOCATION_NAMES[locId]}* para *${SERVICE[s.serviceKey].name}*. ¿Confirmo? (Pago en persona)` })
          return
        }else{
          await sock.sendMessage(from,{ text:`No puedo ver huecos seguros ahora mismo. Dime un *día y hora exactos* que te vengan bien y te lo intento coger manualmente.` })
          return
        }
      }

      // confirmación
      if(s.confirm && /\b(si|sí|vale|ok|okay|confirmo|perfecto|de acuerdo)\b/i.test(tlow)){
        if(!s.name){
          if(text.trim().split(" ").length>=2){ s.name=text.trim(); saveSession(phone,s) }
          else { await sock.sendMessage(from,{ text:`Para cerrar, dime tu *nombre y apellidos*.` }); return }
        }
        if(!s.email || !/^[^\s@]+@[^\s@]+\.[^\s@]{2,}$/.test(s.email)){
          if(/@/.test(text) && /^[^\s@]+@[^\s@]+\.[^\s@]{2,}$/.test(text.trim())){ s.email=text.trim(); saveSession(phone,s) }
          else { await sock.sendMessage(from,{ text:`Genial. Ahora tu *email* (tipo: nombre@correo.com).` }); return }
        }
        const locId = LOCATION_IDS[s.locationKey]
        const cust = await findOrCreateCustomer({ name:s.name, email:s.email, phone })
        if(!cust){ await sock.sendMessage(from,{ text:`No pude crear la ficha con ese email. Envíame uno válido y seguimos.` }); return }
        const booking = await createBooking({ locationId:locId, customerId:cust.id, serviceKey:s.serviceKey, startEU:s.startEU, teamMemberId:s.staffId||undefined })
        if(!booking){ await sock.sendMessage(from,{ text:`Uy, justo han cogido ese hueco. Te busco otro y te digo.` }); s.confirm=false; saveSession(phone,s); return }
        apptInsert.run({
          id:`apt_${Date.now().toString(36)}_${Math.random().toString(36).slice(2,7)}`,
          phone, name:s.name, email:s.email,
          location_id: locId, service_key: s.serviceKey, service_name: SERVICE[s.serviceKey].name, duration_min: s.durationMin,
          staff_id: s.staffId||null, start_iso: s.startEU.tz("UTC").toISOString(),
          square_booking_id: booking.id||null, status:"confirmed", created_at: new Date().toISOString()
        })
        await sock.sendMessage(from,{ text:
`Reserva confirmada 🎉
Servicio: ${SERVICE[s.serviceKey].name}
Fecha: ${fmtES(s.startEU)}
Local: ${LOCATION_NAMES[locId]}
Duración: ${s.durationMin} min
Pago en persona.` })
        sessDel.run(phone); return
      }

      // captura pasiva nombre/email
      if(!s.name && /^[A-Za-zÁÉÍÓÚÜÑáéíóúüñ]+/.test(text.trim()) && text.trim().split(" ").length>=2){
        s.name=text.trim(); saveSession(phone,s)
        await sock.sendMessage(from,{ text:`Gracias, ${s.name}. Dime *local* (Málaga – La Luz o Torremolinos) y el *servicio* y te cojo hueco.` })
        return
      }
      if(!s.email && /@/.test(text) && /^[^\s@]+@[^\s@]+\.[^\s@]{2,}$/.test(text.trim())){
        s.email=text.trim(); saveSession(phone,s)
        await sock.sendMessage(from,{ text:`Perfecto. ¿La cita la cogemos en *Málaga – La Luz* o en *Torremolinos*? Dime también el servicio.` })
        return
      }

      // fallback IA con contexto (pregunta pestañas si toca)
      const ai = await aiReply(
        `Mensaje del cliente: "${text}". Ya tengo local=${!!s.locationKey}, servicio=${!!s.serviceKey}.`,
        { haveLoc:!!s.locationKey, haveSvc:!!s.serviceKey, lash:mentionsLash }
      )
      await sock.sendMessage(from,{ text: ai || (mentionsLash? ASK_LASH_TEXT(!!s.locationKey) : "¿Te viene mejor *Málaga – La Luz* o *Torremolinos*? Y dime el servicio (ej.: “manicura semipermanente”).") })
    })
  }catch(e){ console.error("startBot:", e?.message||e) }
}
