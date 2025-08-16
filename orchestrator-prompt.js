// index.js — Gapink Nails · v17 “Cálido y cercano” (DeepSeek + Memoria + Microcopy Humana)
// - IA siempre on (DeepSeek)
// - Menús pegajosos 20 min (servicios/horas/confirmación/citas)
// - Motor de tono (sin tokens extra) y presence typing
// - Bienvenida/OOH con throttle para que no suene a contestador
//
// Requisitos: node 18+, @whiskeysockets/baileys, express, pino, qrcode, qrcode-terminal,
// dotenv, better-sqlite3, dayjs, square.

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

// ====== Day.js
if (!globalThis.crypto) globalThis.crypto = webcrypto
dayjs.extend(utc); dayjs.extend(tz); dayjs.locale("es")
const EURO_TZ = "Europe/Madrid"

// ====== Horarios negocio
const WORK_DAYS = [1,2,3,4,5] // L–V
const SLOT_MIN = 30
const MORNING = { start:10, end:14 }
const AFTERNOON = { start:16, end:20 }
const NOW_MIN_OFFSET_MIN = Number(process.env.BOT_NOW_OFFSET_MIN || 30)
const HOLIDAYS_EXTRA = (process.env.HOLIDAYS_EXTRA || "06/01,28/02,15/08,12/10,01/11,06/12,08/12,25/12")
  .split(",").map(s=>s.trim()).filter(Boolean)

// ====== Square
const square = new Client({
  accessToken: process.env.SQUARE_ACCESS_TOKEN,
  environment: (process.env.SQUARE_ENV==="production") ? Environment.Production : Environment.Sandbox
})
const LOC_TORRE = (process.env.SQUARE_LOCATION_ID_TORREMOLINOS || "").trim()
const LOC_LUZ   = (process.env.SQUARE_LOCATION_ID_LA_LUZ || "").trim()
const ADDRESS_TORRE = process.env.ADDRESS_TORREMOLINOS || "Av. de Benyamina 18, Torremolinos"
const ADDRESS_LUZ   = process.env.ADDRESS_LA_LUZ || "Málaga – Barrio de La Luz"
const DRY_RUN = /^true$/i.test(process.env.DRY_RUN || "")

// ====== DeepSeek (Chat Completions compatible)
const LLM_API_KEY = process.env.DEEPSEEK_API_KEY || ""
const LLM_MODEL   = process.env.DEEPSEEK_MODEL   || "deepseek-chat"
const LLM_URL     = process.env.DEEPSEEK_API_URL || "https://api.deepseek.com/v1/chat/completions"

import { SYSTEM_PROMPT } from "./orchestrator-prompt.js"

async function aiChat(messages, { temperature=0.2, retries=3 } = {}){
  if (!LLM_API_KEY) return ""
  let err=null
  for (let i=0;i<=retries;i++){
    try{
      const r = await fetch(LLM_URL, {
        method:"POST",
        headers:{ "Content-Type":"application/json", "Authorization":`Bearer ${LLM_API_KEY}` },
        body: JSON.stringify({ model: LLM_MODEL, messages, temperature })
      })
      if (!r.ok) throw new Error(`HTTP ${r.status}`)
      const j = await r.json()
      return (j?.choices?.[0]?.message?.content || "").trim()
    }catch(e){
      err=e
      const wait=300*Math.pow(2,i)
      await new Promise(r=>setTimeout(r,wait))
    }
  }
  console.error("aiChat failed:", err?.message||err); return ""
}

// ====== Utils
const onlyDigits = s => String(s||"").replace(/\D+/g,"")
const rm = s => String(s||"").normalize("NFD").replace(/\p{Diacritic}/gu, "")
const norm = s => rm(s).toLowerCase().replace(/[+.,;:()/_-]/g," ").replace(/[^\p{Letter}\p{Number}\s]/gu," ").replace(/\s+/g," ").trim()
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
const LOC_SYNON = {
  la_luz:[/\bluz\b/i,/\bmalaga\b/i,/\bmálaga\b/i,/\bvelazquez\b/i,/\bvelázquez\b/i],
  torremolinos:[/\btorre\b/i,/\btorremolinos\b/i]
}
function detectSedeFromText(t){ const low=norm(t); for (const [k,rs] of Object.entries(LOC_SYNON)) if (rs.some(r=>r.test(low))) return k; return null }

// ====== Horario helpers
function isHolidayEU(d){
  const dd=String(d.date()).padStart(2,"0"), mm=String(d.month()+1).padStart(2,"0")
  return HOLIDAYS_EXTRA.includes(`${dd}/${mm}`)
}
function insideBlock(d,b){ return d.hour()>=b.start && d.hour()<b.end }
function insideBusinessHours(d,dur){
  const t=d.clone(); if (!WORK_DAYS.includes(t.day())) return false; if (isHolidayEU(t)) return false
  const end=t.clone().add(dur,"minute")
  return (insideBlock(t,MORNING)&&insideBlock(end,MORNING)&&t.isSame(end,"day"))||
         (insideBlock(t,AFTERNOON)&&insideBlock(end,AFTERNOON)&&t.isSame(end,"day"))
}
function nextOpeningFrom(d){
  let t=d.clone()
  if (t.hour()>=AFTERNOON.end) t=t.add(1,"day").hour(MORNING.start).minute(0).second(0).millisecond(0)
  else if (t.hour()>=MORNING.end && t.hour()<AFTERNOON.start) t=t.hour(AFTERNOON.start).minute(0).second(0).millisecond(0)
  else if (t.hour()<MORNING.start) t=t.hour(MORNING.start).minute(0).second(0).millisecond(0)
  while (!WORK_DAYS.includes(t.day()) || isHolidayEU(t)) t=t.add(1,"day").hour(MORNING.start).minute(0).second(0).millisecond(0)
  return t
}
function ceilToSlotEU(t){ const m=t.minute(), rem=m%SLOT_MIN; return rem===0 ? t.second(0).millisecond(0) : t.add(SLOT_MIN-rem,"minute").second(0).millisecond(0) }
function toDayjsEU(x){ if (!x) return null; if (dayjs.isDayjs(x)) return x.tz(EURO_TZ); if (typeof x==="number") return dayjs.tz(x,EURO_TZ); if (typeof x==="string") return dayjs.tz(x,EURO_TZ); return null }
function fmtES(d){
  const dias=["domingo","lunes","martes","miércoles","jueves","viernes","sábado"]; const t=(dayjs.isDayjs(d)?d:dayjs(d)).tz(EURO_TZ)
  return `${dias[t.day()]} ${String(t.date()).padStart(2,"0")}/${String(t.month()+1).padStart(2,"0")} ${String(t.hour()).padStart(2,"0")}:${String(t.minute()).padStart(2,"0")}`
}
function proposeSlots({ fromEU, durationMin=60, n=3 }){
  const out=[]; let t=ceilToSlotEU(fromEU.clone()); t=nextOpeningFrom(t)
  while (out.length<n){
    if (insideBusinessHours(t,durationMin)){ out.push(t.clone()); t=t.add(SLOT_MIN,"minute") }
    else {
      if (t.hour()>=AFTERNOON.end) t=t.add(1,"day").hour(MORNING.start).minute(0)
      else if (t.hour()>=MORNING.end && t.hour()<AFTERNOON.start) t=t.hour(AFTERNOON.start).minute(0)
      else t=t.add(SLOT_MIN,"minute")
      while (!WORK_DAYS.includes(t.day()) || isHolidayEU(t)) t=t.add(1,"day").hour(MORNING.start).minute(0)
    }
  }
  return out
}
function enumerateHours(list){ return list.map((d,i)=>({ index:i+1, iso:d.format("YYYY-MM-DDTHH:mm"), pretty:fmtES(d) })) }
function stableKey(parts){ const raw=Object.values(parts).join("|"); return createHash("sha256").update(raw).digest("hex").slice(0,48) }

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
  square_booking_id TEXT
);
CREATE TABLE IF NOT EXISTS sessions (
  phone TEXT PRIMARY KEY,
  data_json TEXT,
  updated_at TEXT
);
`)
const insertAppt = db.prepare(`INSERT INTO appointments
(id,customer_name,customer_phone,customer_square_id,location_key,service_env_key,service_label,duration_min,start_iso,end_iso,staff_id,status,created_at,square_booking_id)
VALUES (@id,@customer_name,@customer_phone,@customer_square_id,@location_key,@service_env_key,@service_label,@duration_min,@start_iso,@end_iso,@staff_id,@status,@created_at,@square_booking_id)`)

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
  c.pendingDateTime_ms = s.pendingDateTime? s.pendingDateTime.valueOf(): null
  delete c.lastHours; delete c.pendingDateTime
  const j=JSON.stringify(c)
  const up=db.prepare(`UPDATE sessions SET data_json=@j, updated_at=@u WHERE phone=@p`).run({j,u:new Date().toISOString(),p:phone})
  if (up.changes===0) db.prepare(`INSERT INTO sessions (phone,data_json,updated_at) VALUES (@p,@j,@u)`).run({p:phone,j,u:new Date().toISOString()})
}
function clearSession(phone){ db.prepare(`DELETE FROM sessions WHERE phone=@phone`).run({phone}) }

// ====== Servicios (.env → lista)
function servicesForSedeKey(sedeKey){
  const prefix = (sedeKey==="la_luz") ? "SQ_SVC_luz_" : "SQ_SVC_"
  const out=[]
  for (const [k,v] of Object.entries(process.env)){
    if (!k.startsWith(prefix)) continue
    const [id] = String(v||"").split("|"); if (!id) continue
    const label = k.replace(prefix,"").replaceAll("_"," ").replace(/\b([a-z])/g,m=>m.toUpperCase()).replace("Pestan","Pestañ")
    out.push({ index: out.length+1, label, key:k })
  }
  return out
}
function buildLashMenu(sedeKey){
  const p=(sedeKey==="la_luz")?"SQ_SVC_luz_":"SQ_SVC_"
  const want = [
    [p+"EXTENSIONES_DE_PESTANAS_NUEVAS_PELO_A_PELO","Extensiones de pestañas nuevas pelo a pelo"],
    [p+"EXTENSIONES_PESTANAS_NUEVAS_2D","Extensiones pestañas nuevas 2D"],
    [p+"EXTENSIONES_PESTANAS_NUEVAS_3D","Extensiones pestañas nuevas 3D"],
    [p+"LIFITNG_DE_PESTANAS_Y_TINTE","Lifting de pestañas y tinte"]
  ]
  const out=[]
  for (const [key,label] of want){
    const [id] = String(process.env[key]||"").split("|")
    if (id) out.push({ index: out.length+1, label, key })
  }
  return out
}

// ====== Square helpers
async function getServiceIdAndVersion(envKey){
  const raw = process.env[envKey]; if (!raw) return null
  let [id, ver] = String(raw).split("|"); ver=ver?Number(ver):null
  if (!id) return null
  if (!ver){
    try{ const resp=await square.catalogApi.retrieveCatalogObject(id,true); ver=resp?.result?.object?.version?Number(resp.result.object.version):1 }catch{ ver=1 }
  }
  return {id,version:ver||1}
}
async function findOrCreateCustomer({ name, email, phone }){
  try{
    const e164=normalizePhoneES(phone); if(!e164) return null
    const got = await square.customersApi.searchCustomers({ query:{ filter:{ phoneNumber:{ exact:e164 } } } })
    const c=(got?.result?.customers||[])[0]; if (c) return c
  }catch{}
  try{
    const created = await square.customersApi.createCustomer({
      idempotencyKey:`cust_${Date.now()}_${Math.random().toString(36).slice(2,6)}`,
      givenName:name||undefined,
      emailAddress:email||undefined,
      phoneNumber:normalizePhoneES(phone)||undefined
    })
    return created?.result?.customer||null
  }catch{ return null }
}
async function createBooking({ startEU, locationKey, envServiceKey, durationMin, customerId, teamMemberId }){
  if (!envServiceKey) return null
  if (DRY_RUN) return { id:`TEST_${Date.now()}` }
  const sv = await getServiceIdAndVersion(envServiceKey); if (!sv?.id || !sv?.version) return null
  const startISO = startEU.tz("UTC").toISOString()
  const idempotencyKey = stableKey({ loc:locationToId(locationKey), sv:sv.id, startISO, customerId, teamMemberId })
  try{
    const resp = await square.bookingsApi.createBooking({
      idempotencyKey,
      booking:{
        locationId: locationToId(locationKey),
        startAt: startISO,
        customerId,
        appointmentSegments:[{
          teamMemberId: teamMemberId || undefined,
          serviceVariationId: sv.id,
          serviceVariationVersion: Number(sv.version),
          durationMinutes: durationMin||60
        }]
      }
    })
    return resp?.result?.booking || null
  }catch(e){ console.error("createBooking:", e?.message||e); return null }
}
async function cancelBooking(bookingId){
  if (DRY_RUN) return true
  try{
    const body = { idempotencyKey:`cancel_${bookingId}_${Date.now()}` }
    const resp = await square.bookingsApi.cancelBooking(bookingId, body)
    return !!resp?.result?.booking
  }catch(e){ console.error("cancelBooking:", e?.message||e); return false }
}
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
      const list=resp?.result?.bookings||[]; const nowISO=new Date().toISOString()
      for (const b of list){
        if (!b?.startAt || b.startAt<nowISO) continue
        const start=dayjs.tz(b.startAt,EURO_TZ)
        const seg=(b.appointmentSegments||[{}])[0]
        items.push({
          index:items.length+1,
          id:b.id,
          fecha_iso:start.format("YYYY-MM-DD"),
          pretty:fmtES(start),
          sede: locationNice(idToLocKey(b.locationId)||""),
          profesional: seg?.teamMemberId?`Prof. ${seg.teamMemberId.slice(-4)}`:null,
          servicio: "Servicio"
        })
      }
    }catch{}
  }
  return items
}

// ====== Menús pegajosos (20 min)
const MENU_TTL_MS = 20*60*1000
function parseIndexFromText(t){
  const x=norm(t)
  if (/\b(primero|primera|1ro|1ª|1a|1º)\b/.test(x)) return 1
  if (/\b(segundo|segunda|2do|2ª|2a|2º)\b/.test(x)) return 2
  if (/\b(tercero|tercera|3ro|3ª|3a|3º)\b/.test(x)) return 3
  const m=x.match(/\b([1-9])\b/); if (m) return Number(m[1])
  return null
}
function setPendingMenu(s, type, items){ s.pendingMenu = { type, items, createdAt: Date.now() } }
function getPendingMenu(s){
  if (!s?.pendingMenu) return null
  if (Date.now() - (s.pendingMenu.createdAt||0) > MENU_TTL_MS){ s.pendingMenu=null; return null }
  return s.pendingMenu
}

// ====== Microcopy humano (sin tokens extra)
const EMOJI = ["😊","✨","😉","🗓️","💅","👍","🎉","🙌"]
function pick(arr){ return arr[Math.floor(Math.random()*arr.length)] }
function timeGreeting(){
  const h = dayjs().tz(EURO_TZ).hour()
  if (h<12) return "¡Buenos días!"
  if (h<20) return "¡Buenas!"
  return "¡Buenas noches!"
}
function mirrorHello(userText){
  const t=norm(userText)
  if (/\b(hol[ae]|\bwenas|buenas)\b/.test(t)) return userText.match(/^[^\n]{1,18}/)?.[0] || null
  return null
}
function soften(text){
  // reglas mínimas: frases cortas, evita “por favor” repetitivo, añade emojis suaves
  let s = String(text||"").trim()
  s = s.replace(/\s+/g," ")
  // evita mayúsculas largas
  if (s.length>180) s = s.slice(0, 180) + "…"
  // añade un emoji al final si no hay
  if (!/[!?…]$/.test(s)) s += "."
  s += " " + pick(EMOJI)
  return s
}
function buildFriendly(msg, ctx){
  // ctx: { name, sedeNice, serviceLabel, timePretty, preferStaff }
  // Intenta no sonar a plantilla; usa aperturas contextuales y espeja saludo
  const hello = mirrorHello(ctx.userText) || timeGreeting()
  const tail = ctx.tail || ""
  // Si el mensaje ya es de una línea clara, solo suaviza:
  if (msg && msg.length<140) return soften(msg)
  // Si es más largo o genérico, lo reescribimos en 1–2 líneas:
  const bits=[]
  bits.push(hello)
  if (msg) bits.push(msg)
  if (tail) bits.push(tail)
  return soften(bits.join(" "))
}

// ====== Bienvenida / OOH (throttle)
const WELCOME_COOLDOWN_MS = 6*60*60*1000 // cada 6h máx.
const OOH_COOLDOWN_MS = 4*60*60*1000     // cada 4h máx.
function canSend(ts, cooldown){ return !ts || (Date.now()-ts>cooldown) }

// ====== Mini-web + QR
const app=express()
const PORT=process.env.PORT||8080
let lastQR=null, conectado=false
app.get("/", (_req,res)=>{
  res.send(`<!doctype html><meta charset="utf-8"><style>
  body{font-family:system-ui;display:grid;place-items:center;min-height:100vh}
  .card{max-width:560px;padding:24px;border-radius:16px;box-shadow:0 6px 24px rgba(0,0,0,.08)}
  </style><div class="card"><h1>Gapink Nails</h1>
  <p>Estado: ${conectado?"✅ Conectado":"❌ Desconectado"}</p>
  ${!conectado&&lastQR?`<img src="/qr.png" width="300">`:""}
  <p style="opacity:.7">Modo: ${DRY_RUN?"Simulación (no toca Square)":"Producción"}</p>
  </div>`)
})
app.get("/qr.png", async (_req,res)=>{
  if(!lastQR) return res.status(404).send("No QR")
  const png = await qrcode.toBuffer(lastQR, { type:"png", width:512, margin:1 })
  res.set("Content-Type","image/png").send(png)
})
app.listen(PORT, ()=>{ console.log("🌐 Web", PORT); startBot().catch(console.error) })

// ====== Carga robusta Baileys
async function loadBaileys(){
  const require = createRequire(import.meta.url); let mod=null
  try{ mod=require("@whiskeysockets/baileys") }catch{}; if(!mod){ try{ mod=await import("@whiskeysockets/baileys") }catch{} }
  if(!mod) throw new Error("No se pudo cargar @whiskeysockets/baileys")
  const makeWASocket = mod.makeWASocket || mod.default?.makeWASocket || (typeof mod.default==="function"?mod.default:undefined)
  const useMultiFileAuthState = mod.useMultiFileAuthState || mod.default?.useMultiFileAuthState
  const fetchLatestBaileysVersion = mod.fetchLatestBaileysVersion || mod.default?.fetchLatestBaileysVersion || (async()=>({version:[2,3000,0]}))
  const Browsers = mod.Browsers || mod.default?.Browsers || { macOS:(n="Desktop")=>["MacOS",n,"121.0.0"] }
  if (typeof makeWASocket!=="function" || typeof useMultiFileAuthState!=="function") throw new Error("Baileys incompatible")
  return { makeWASocket, useMultiFileAuthState, fetchLatestBaileysVersion, Browsers }
}

// ====== Cola por usuario
const QUEUE=new Map()
function enqueue(key,job){
  const prev=QUEUE.get(key)||Promise.resolve()
  const next=prev.then(job,job).finally(()=>{ if (QUEUE.get(key)===next) QUEUE.delete(key) })
  QUEUE.set(key,next); return next
}

// ====== Presence typing helper
async function sendWithPresence(sock, jid, text){
  try{ await sock.sendPresenceUpdate("composing", jid); }catch{}
  await new Promise(r=>setTimeout(r, 600+Math.random()*1000))
  return sock.sendMessage(jid, { text })
}

// ====== Bot
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
      if (connection==="open"){ lastQR=null; conectado=true; console.log("✅ WhatsApp listo") }
      if (connection==="close"){ conectado=false; console.log("❌ Conexión cerrada. Reintentando…"); setTimeout(()=>startBot().catch(console.error),2500) }
    })
    sock.ev.on("creds.update", saveCreds)

    sock.ev.on("messages.upsert", async ({messages})=>{
      const m=messages?.[0]; if (!m?.message || m.key.fromMe) return
      const jid = m.key.remoteJid
      const phone = normalizePhoneES((jid||"").split("@")[0]||"") || (jid||"").split("@")[0]
      const textRaw = (m.message.conversation || m.message.extendedTextMessage?.text || m.message?.imageMessage?.caption || "").trim()
      if (!textRaw) return

      await enqueue(phone, async ()=>{
        let s = loadSession(phone) || {
          greeted:false, lastWelcomeAt:null, lastOOHAt:null,
          lastOOHDay:null,
          sede:null,
          name:null, email:null,
          selectedServiceEnvKey:null, selectedServiceLabel:null,
          pendingDateTime:null,
          preferredStaffLabel:null,
          lastHours:[],
          last_msg_id:null,
          pendingMenu:null,
          appointmentIndexLocal:null
        }

        // Evita procesar duplicados
        if (s.last_msg_id===m.key.id){ return } s.last_msg_id=m.key.id

        const nowEU=dayjs().tz(EURO_TZ)

        // Bienvenida con throttle (cada 6h máx)
        if (!s.greeted || canSend(s.lastWelcomeAt, WELCOME_COOLDOWN_MS)){
          const hello = buildFriendly("Gracias por escribir a Gapink Nails. ¿Cómo te ayudamos?", { userText:textRaw })
          await sendWithPresence(sock, jid, hello + "\n\nReserva: https://gapinknails.square.site/\nCambios: link del SMS de tu cita")
          s.greeted=true; s.lastWelcomeAt=Date.now(); saveSession(phone,s)
        }

        // Fuera de horario (una vez/4h)
        const inHours=insideBusinessHours(nowEU.clone(),15)
        if (!inHours && canSend(s.lastOOHAt, OOH_COOLDOWN_MS)){
          const msg = buildFriendly("Ahora estamos fuera de horario. Si quieres dime día y hora y lo gestiono igual (o usa el link).", { userText:textRaw })
          await sendWithPresence(sock, jid, msg.replace("link","https://gapinknails.square.site/"))
          s.lastOOHAt=Date.now(); saveSession(phone,s)
        }

        // Detecciones rápidas
        const maybeSede=detectSedeFromText(textRaw); if (maybeSede) s.sede=maybeSede
        if (/\bdesi\b/i.test(textRaw)) s.preferredStaffLabel="Desi"

        // ====== Menú pegajoso corto-circuito
        const pending = getPendingMenu(s)
        const idxPick = parseIndexFromText(textRaw)
        let shortCircuitHandled=false, localConfirmIdx=null

        if (pending && idxPick){
          const item = pending.items.find(x=>x.index===idxPick)
          if (item){
            if (pending.type==="services"){
              s.selectedServiceEnvKey=item.key; s.selectedServiceLabel=item.label
              await sendWithPresence(sock,jid, buildFriendly(`Perfecto, apunto *${item.label}*${s.preferredStaffLabel?` (preferencia *${s.preferredStaffLabel}*)`:""}.`, { userText:textRaw }))
              s.pendingMenu=null; shortCircuitHandled=true
            }
            if (pending.type==="hours"){
              s.pendingDateTime = dayjs.tz(item.iso, EURO_TZ)
              await sendWithPresence(sock,jid, buildFriendly(`Genial, reservo para *${fmtES(s.pendingDateTime)}*.`, { userText:textRaw }))
              s.pendingMenu=null; shortCircuitHandled=true
            }
            if (pending.type==="confirm"){
              localConfirmIdx = idxPick; s.pendingMenu=null; shortCircuitHandled=false
            }
            if (pending.type==="appointments"){
              s.appointmentIndexLocal = idxPick; shortCircuitHandled=false
            }
            saveSession(phone,s)
          }
        }

        // ====== Menú de pestañas directo si procede
        const mentionsLash = /\bpesta(?:n|ñ)as\b/i.test(norm(textRaw)) || /lifting/.test(norm(textRaw))
        if (mentionsLash && s.sede && !s.selectedServiceEnvKey){
          const lash = buildLashMenu(s.sede)
          if (lash.length){
            const msg = `Para *pestañas* en *${locationNice(s.sede)}*, tengo estas opciones:\n`+
              lash.map(x=>`${x.index}. ${x.label}`).join("\n")+
              `\n\n¿Con cuál te quedas? (número)`
            setPendingMenu(s,"services",lash); saveSession(phone,s)
            await sendWithPresence(sock,jid, buildFriendly(msg,{ userText:textRaw }))
            return
          }
        }

        // ====== Enumeraciones para IA
        const servicios = s.sede ? servicesForSedeKey(s.sede) : null
        let hoursList = []
        if (s.sede){
          const base = nextOpeningFrom(nowEU.add(NOW_MIN_OFFSET_MIN,"minute"))
          s.lastHours = proposeSlots({ fromEU: base, durationMin:60, n:3 })
          hoursList = enumerateHours(s.lastHours)
        }
        const citas = await enumerateCitasByPhone(phone)
        const confirmChoices = [{index:1,label:"sí"},{index:2,label:"no"}]

        // ====== IA (orquestador)
        const payload = {
          user_message: textRaw,
          sede_actual: s.sede,
          servicios_enumerados: servicios || null,
          horas_enumeradas: hoursList.length?hoursList:null,
          citas_enumeradas: citas.length?citas:null,
          fechas_enumeradas: null,
          confirm_choices: confirmChoices
        }
        const aiRaw = await aiChat([
          { role:"system", content: SYSTEM_PROMPT },
          { role:"user", content: JSON.stringify(payload, null, 2) }
        ])

        function safeParseJSON(txt){ try{ const a=txt.indexOf("{"), b=txt.lastIndexOf("}"); if (a>=0&&b>a) txt=txt.slice(a,b+1); return JSON.parse(txt) }catch{return null} }
        function sanitize(dec){
          const base = { intent:5, needs_clarification:true, requires_confirmation:false,
            slots:{ sede:s.sede||null, service_index:null, appointment_index:null, date_iso:null,time_iso:null,datetime_iso:null,profesional:null,notes:null },
            selection:{ time_index:null, date_index:null, confirm_index:null }, client_message:"" }
          if (!dec||typeof dec!=="object") return base
          const out = structuredClone(base)
          const clamp=(n,max)=>Number.isInteger(n)&&n>=1&&(max? n<=max:true)?n:null
          out.intent = [1,2,3,4,5].includes(Number(dec.intent)) ? Number(dec.intent) : base.intent
          out.needs_clarification=!!dec.needs_clarification
          out.requires_confirmation=!!dec.requires_confirmation
          out.client_message = String(dec.client_message||"")
          const sev=dec.slots||{}
          out.slots.sede = (sev.sede==="torremolinos"||sev.sede==="la_luz")?sev.sede:base.slots.sede
          out.slots.service_index = clamp(sev.service_index,(servicios||[]).length)
          out.slots.appointment_index = clamp(sev.appointment_index,citas.length)
          out.slots.datetime_iso = sev.datetime_iso||null
          const sel=dec.selection||{}
          out.selection.time_index = clamp(sel.time_index,hoursList.length)
          out.selection.confirm_index = [1,2].includes(Number(sel.confirm_index))?Number(sel.confirm_index):null
          return out
        }
        const decision = sanitize(safeParseJSON(aiRaw))

        // ====== Fusión memoria + IA
        const srvMap = new Map((servicios||[]).map(x=>[x.index,x]))
        const hrsMap = new Map(hoursList.map(h=>[h.index,h]))
        const citasMap = new Map(citas.map(c=>[c.index,c]))

        let chosenServiceEnvKey = s.selectedServiceEnvKey
        let chosenServiceLabel  = s.selectedServiceLabel
        if (!chosenServiceEnvKey && decision.slots.service_index && srvMap.has(decision.slots.service_index)){
          const row=srvMap.get(decision.slots.service_index); chosenServiceEnvKey=row.key; chosenServiceLabel=row.label
          s.selectedServiceEnvKey=chosenServiceEnvKey; s.selectedServiceLabel=chosenServiceLabel; saveSession(phone,s)
        }
        if (!s.pendingDateTime && decision.selection.time_index && hrsMap.has(decision.selection.time_index)){
          s.pendingDateTime = dayjs.tz(hrsMap.get(decision.selection.time_index).iso, EURO_TZ); saveSession(phone,s)
        }
        const confirmIdx = localConfirmIdx || decision.selection.confirm_index || null

        // Menús a fijar si la IA los solicita
        if (decision.intent===1){
          if (!chosenServiceEnvKey && servicios?.length){ setPendingMenu(s,"services",servicios); saveSession(phone,s) }
          if (!s.pendingDateTime && hoursList.length){ setPendingMenu(s,"hours",hoursList); saveSession(phone,s) }
          if (decision.requires_confirmation && confirmIdx==null){ setPendingMenu(s,"confirm",confirmChoices); saveSession(phone,s) }
        }
        if (decision.intent===2 && citas.length && decision.slots.appointment_index==null){
          setPendingMenu(s,"appointments",citas); saveSession(phone,s)
        }

        // ====== Enviar SIEMPRE el mensaje (humanizado)
        const ctx = {
          userText: textRaw,
          sedeNice: s.sede ? locationNice(s.sede) : null,
          serviceLabel: chosenServiceLabel || null,
          timePretty: s.pendingDateTime ? fmtES(s.pendingDateTime) : null,
          preferStaff: s.preferredStaffLabel || null
        }
        const outMsg = decision.client_message?.trim()
          ? buildFriendly(decision.client_message.trim(), ctx)
          : buildFriendly("Perfecto, te ayudo con eso ahora mismo.", ctx)
        await sendWithPresence(sock, jid, outMsg)

        // ====== Acciones (crear/cancelar/reprogramar)
        async function closeCreate(){
          if (!s.sede){ await sendWithPresence(sock,jid, buildFriendly("¿Te viene mejor *Torremolinos* o *Málaga – La Luz*?", { userText:textRaw })); return }
          if (!chosenServiceEnvKey){ await sendWithPresence(sock,jid, buildFriendly("Elige el *servicio* (puedes responder con el *número*).", { userText:textRaw })); return }
          if (!s.pendingDateTime){ await sendWithPresence(sock,jid, buildFriendly("Elige una *hora* (1/2/3) o dime otra.", { userText:textRaw })); return }

          const startEU = ceilToSlotEU(s.pendingDateTime.clone())
          if (!insideBusinessHours(startEU,60)){
            s.pendingDateTime=null; saveSession(phone,s)
            await sendWithPresence(sock,jid, buildFriendly("Esa hora cae fuera de L–V 10–14 / 16–20. Dime otra, porfa.", { userText:textRaw }))
            return
          }

          const customer = await findOrCreateCustomer({ name:s.name, email:s.email, phone })
          if (!customer){
            await sendWithPresence(sock,jid, buildFriendly("Para cerrar, pásame un *nombre* o *email* y lo grabo.", { userText:textRaw }))
            return
          }

          const booking = await createBooking({ startEU, locationKey:s.sede, envServiceKey:chosenServiceEnvKey, durationMin:60, customerId:customer.id, teamMemberId:null })
          if (!booking){
            await sendWithPresence(sock,jid, buildFriendly("No pude reservar ese hueco. ¿Te paso otras horas o prefieres usar el link?", { userText:textRaw, tail:"https://gapinknails.square.site/" }))
            return
          }

          const aptId = `apt_${Math.random().toString(36).slice(2,8)}${Date.now().toString(36).slice(-4)}`
          insertAppt.run({
            id:aptId, customer_name:customer?.givenName||null, customer_phone:phone, customer_square_id:customer.id,
            location_key:s.sede, service_env_key:chosenServiceEnvKey, service_label:chosenServiceLabel||"Servicio",
            duration_min:60, start_iso:startEU.tz("UTC").toISOString(), end_iso:startEU.clone().add(60,"minute").tz("UTC").toISOString(),
            staff_id:null, status:"confirmed", created_at:new Date().toISOString(), square_booking_id:booking.id
          })

          const confirmText =
`Reserva confirmada ${pick(["🎉","🙌","✨"])}
${locationNice(s.sede)} — ${s.sede==="la_luz"?ADDRESS_LUZ:ADDRESS_TORRE}
Servicio: ${chosenServiceLabel||"—"}${s.preferredStaffLabel?`\nPreferencia: ${s.preferredStaffLabel}`:""}
Fecha: ${fmtES(startEU)}
Duración: 60 min

¡Te esperamos! ${pick(["💅","😊"])}`
          await sendWithPresence(sock, jid, confirmText)
          clearSession(phone)
        }

        if (decision.intent===1){
          if (decision.requires_confirmation){
            if (confirmIdx===1) await closeCreate()
            else if (confirmIdx===2) await sendWithPresence(sock,jid, buildFriendly("Sin problema, dime otra hora o servicio y te paso opciones.", { userText:textRaw }))
          } else if (shortCircuitHandled || s.selectedServiceEnvKey || s.pendingDateTime){
            await closeCreate()
          }
          return
        }

        if (decision.intent===2){
          const aidx = decision.slots.appointment_index || s.appointmentIndexLocal || null
          if (!aidx || !citasMap.has(aidx)) return
          if (decision.requires_confirmation){
            if (confirmIdx===1){
              const ok = await cancelBooking(citasMap.get(aidx).id)
              await sendWithPresence(sock,jid, ok? buildFriendly("He cancelado la cita. Si necesitas otra hora, te paso huequitos.", { userText:textRaw })
                                        : buildFriendly("No pude cancelarla. Prueba con el enlace del SMS o dime y lo intento de nuevo.", { userText:textRaw }))
            } else if (confirmIdx===2){
              await sendWithPresence(sock,jid, buildFriendly("Vale, no cancelo. Si quieres moverla, te paso horas.", { userText:textRaw }))
            }
          }
          return
        }

        if (decision.intent===3){
          const aidx = decision.slots.appointment_index || s.appointmentIndexLocal || null
          if (!aidx || !citasMap.has(aidx)) return
          if (decision.requires_confirmation && confirmIdx===2){
            await sendWithPresence(sock,jid, buildFriendly("Ok, no reprogramo.", { userText:textRaw })); return
          }
          if ((decision.requires_confirmation && confirmIdx===1) || (!decision.requires_confirmation && s.pendingDateTime)){
            const old=citasMap.get(aidx)
            const ok=await cancelBooking(old.id)
            if (!ok){ await sendWithPresence(sock,jid, buildFriendly("No pude reprogramar (falló cancelar). Te paso otras horas si quieres.", { userText:textRaw })); return }
            const envKey = s.selectedServiceEnvKey || (servicesForSedeKey(s.sede||idToLocKey(old.locationId)).find(x=>x.label===old.servicio)?.key) || null
            const customer = await findOrCreateCustomer({ name:s.name, email:s.email, phone })
            if (!customer){ await sendWithPresence(sock,jid, buildFriendly("Me falta un nombre/email para cerrar.", { userText:textRaw })); return }
            const bk = await createBooking({ startEU:s.pendingDateTime, locationKey:s.sede||idToLocKey(old.locationId), envServiceKey:envKey, durationMin:60, customerId:customer.id })
            if (!bk){ await sendWithPresence(sock,jid, buildFriendly("No pude crear la nueva cita. ¿Te paso otras horas?", { userText:textRaw })); return }
            await sendWithPresence(sock,jid, buildFriendly(`Listo, movida a ${fmtES(s.pendingDateTime)} ✅`, { userText:textRaw }))
            clearSession(phone)
          }
          return
        }

        // 4/5: saludo/info ya enviado en cercano
        saveSession(phone,s)
      })
    })
  }catch(e){ console.error("startBot:", e?.message||e) }
}
