// index.js — Gapink Nails · v15.1 (orquestador JSON + hardening)
// “Sesiones seguras · anti-duplicados · validación de decisiones · reintentos Square”
//
// Requiere: node 18+, @whiskeysockets/baileys, express, pino, qrcode, qrcode-terminal,
// dotenv, better-sqlite3, dayjs, square.
//
// .env: ver el que ya usas (SQUARE_LOCATION_ID_TORREMOLINOS, SQUARE_LOCATION_ID_LA_LUZ, etc.)

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

// ===== Day.js base
if (!globalThis.crypto) globalThis.crypto = webcrypto
dayjs.extend(utc); dayjs.extend(tz); dayjs.locale("es")
const EURO_TZ = "Europe/Madrid"

// ===== Horarios negocio
const WORK_DAYS = [1,2,3,4,5]            // L–V
const SLOT_MIN = 30
const MORNING = { start: 10, end: 14 }
const AFTERNOON = { start: 16, end: 20 }
const NOW_MIN_OFFSET_MIN = Number(process.env.BOT_NOW_OFFSET_MIN || 30)
const HOLIDAYS_EXTRA = (process.env.HOLIDAYS_EXTRA || "06/01,28/02,15/08,12/10,01/11,06/12,08/12,25/12")
  .split(",").map(s => s.trim()).filter(Boolean)

// ===== Square
const square = new Client({
  accessToken: process.env.SQUARE_ACCESS_TOKEN,
  environment: (process.env.SQUARE_ENV==="production") ? Environment.Production : Environment.Sandbox
})
const LOC_TORRE = (process.env.SQUARE_LOCATION_ID_TORREMOLINOS || "").trim()
const LOC_LUZ   = (process.env.SQUARE_LOCATION_ID_LA_LUZ || "").trim()
const ADDRESS_TORRE = process.env.ADDRESS_TORREMOLINOS || "Av. de Benyamina 18, Torremolinos"
const ADDRESS_LUZ   = process.env.ADDRESS_LA_LUZ || "Málaga – Barrio de La Luz"
const DRY_RUN = /^true$/i.test(process.env.DRY_RUN || "")

// ===== OpenAI (orquestador)
import { SYSTEM_PROMPT } from "./orchestrator-prompt.js"
const OPENAI_API_KEY = process.env.OPENAI_API_KEY || ""
const OPENAI_MODEL   = process.env.OPENAI_MODEL || "gpt-4o-mini"
const OPENAI_API_URL = process.env.OPENAI_API_URL || "https://api.openai.com/v1/chat/completions"

async function aiChat(messages, { temperature=0.2, retries=2 } = {}) {
  if (!OPENAI_API_KEY) return ""
  let attempt=0, lastErr=null
  while (attempt<=retries){
    try{
      const r = await fetch(OPENAI_API_URL, {
        method: "POST",
        headers: { "Content-Type":"application/json", "Authorization":`Bearer ${OPENAI_API_KEY}` },
        body: JSON.stringify({ model: OPENAI_MODEL, messages, temperature })
      })
      if (!r.ok) throw new Error(`OpenAI ${r.status}`)
      const j = await r.json()
      return (j?.choices?.[0]?.message?.content || "").trim()
    }catch(e){ lastErr=e; await new Promise(res=>setTimeout(res, 300*(attempt+1))); attempt++ }
  }
  console.error("aiChat failed:", lastErr?.message||lastErr)
  return ""
}

// ===== Utils
const onlyDigits = s => String(s||"").replace(/\D+/g,"")
const rm = s => String(s||"").normalize("NFD").replace(/\p{Diacritic}/gu, "")
const norm = s => rm(s).toLowerCase()
  .replace(/[+.,;:()/_-]/g, " ")
  .replace(/[^\p{Letter}\p{Number}\s]/gu, " ")
  .replace(/\s+/g, " ").trim()
function normalizePhoneES(raw){
  const d=onlyDigits(raw); if(!d) return null
  if (raw.startsWith("+") && d.length>=8 && d.length<=15) return `+${d}`
  if (d.startsWith("34") && d.length===11) return `+${d}`
  if (d.length===9) return `+34${d}`
  if (d.startsWith("00")) return `+${d.slice(2)}`
  return `+${d}`
}
function locationToId(key){ return key==="la_luz" ? LOC_LUZ : LOC_TORRE }
function idToLocationKey(locId){ return locId===LOC_LUZ ? "la_luz" : (locId===LOC_TORRE ? "torremolinos" : null) }
function locationNice(key){ return key==="la_luz" ? "Málaga – La Luz" : "Torremolinos" }

const LOC_SYNONYMS = {
  la_luz: [/(\bluz\b|\bmalaga\b|\bmálaga\b)/i, /\bvelazquez\b/i, /\bvelázquez\b/i, /\bbarri[oa]\s+de\s+la\s+luz\b/i],
  torremolinos: [/\btorre\b/i, /\btorremolinos\b/i]
}
function detectSedeFromText(t){
  const low = norm(t)
  for (const [key, regs] of Object.entries(LOC_SYNONYMS)){
    if (regs.some(r=>r.test(low))) return key
  }
  return null
}

// Festivos y horario
function isHolidayEU(d){
  const dd = String(d.date()).padStart(2,"0")
  const mm = String(d.month()+1).padStart(2,"0")
  return HOLIDAYS_EXTRA.includes(`${dd}/${mm}`)
}
function insideBlock(d, block){ return d.hour()>=block.start && d.hour()<block.end }
function insideBusinessHours(d, durMin){
  const t=d.clone()
  if (!WORK_DAYS.includes(t.day())) return false
  if (isHolidayEU(t)) return false
  const end=t.clone().add(durMin,"minute")
  const ok = (insideBlock(t, MORNING) && insideBlock(end, MORNING) && t.isSame(end,"day")) ||
             (insideBlock(t, AFTERNOON) && insideBlock(end, AFTERNOON) && t.isSame(end,"day"))
  return ok
}
function nextOpeningFrom(d){
  let t=d.clone()
  if (t.hour()>=AFTERNOON.end) t = t.add(1,"day").hour(MORNING.start).minute(0).second(0).millisecond(0)
  else if (t.hour()>=MORNING.end && t.hour()<AFTERNOON.start) t = t.hour(AFTERNOON.start).minute(0).second(0).millisecond(0)
  else if (t.hour()<MORNING.start) t = t.hour(MORNING.start).minute(0).second(0).millisecond(0)
  while (!WORK_DAYS.includes(t.day()) || isHolidayEU(t)) {
    t=t.add(1,"day").hour(MORNING.start).minute(0).second(0).millisecond(0)
  }
  return t
}
function ceilToSlotEU(t){
  const m=t.minute(), rem=m%SLOT_MIN
  return rem===0 ? t.second(0).millisecond(0) : t.add(SLOT_MIN-rem,"minute").second(0).millisecond(0)
}
function toDayjsEU(x){
  if (!x) return null
  if (dayjs.isDayjs(x)) return x.tz(EURO_TZ)
  if (typeof x === "number") return dayjs.tz(x, EURO_TZ)
  if (typeof x === "string") return dayjs.tz(x, EURO_TZ)
  return null
}
function fmtES(d){
  const dias=["domingo","lunes","martes","miércoles","jueves","viernes","sábado"]
  const t=(dayjs.isDayjs(d)?d:dayjs(d)).tz(EURO_TZ)
  const DD=String(t.date()).padStart(2,"0")
  const MM=String(t.month()+1).padStart(2,"0")
  const HH=String(t.hour()).padStart(2,"0")
  const mm=String(t.minute()).padStart(2,"0")
  return `${dias[t.day()]} ${DD}/${MM} ${HH}:${mm}`
}
function proposeSlots({ fromEU, durationMin=60, n=3 }){
  const out=[]
  let t = ceilToSlotEU(fromEU.clone())
  t = nextOpeningFrom(t)
  while (out.length<n){
    if (insideBusinessHours(t,durationMin)) {
      out.push(t.clone())
      t = t.add(SLOT_MIN,"minute")
    } else {
      if (t.hour()>=AFTERNOON.end) t = t.add(1,"day").hour(MORNING.start).minute(0)
      else if (t.hour()>=MORNING.end && t.hour()<AFTERNOON.start) t = t.hour(AFTERNOON.start).minute(0)
      else t = t.add(SLOT_MIN,"minute")
      while (!WORK_DAYS.includes(t.day()) || isHolidayEU(t)) {
        t=t.add(1,"day").hour(MORNING.start).minute(0)
      }
    }
  }
  return out
}
function enumerateHours(list){ return list.map((d,i)=>({ index:i+1, iso:d.format("YYYY-MM-DDTHH:mm"), pretty:fmtES(d) })) }
function stableKey(parts){
  const raw = Object.values(parts).join("|")
  return createHash("sha256").update(raw).digest("hex").slice(0,48)
}

// ===== DB (sesiones + log)
const db=new Database("gapink.db");db.pragma("journal_mode = WAL")
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
(id, customer_name, customer_phone, customer_square_id, location_key, service_env_key, service_label, duration_min, start_iso, end_iso, staff_id, status, created_at, square_booking_id)
VALUES (@id,@customer_name,@customer_phone,@customer_square_id,@location_key,@service_env_key,@service_label,@duration_min,@start_iso,@end_iso,@staff_id,@status,@created_at,@square_booking_id)`)
const updateAppt = db.prepare(`UPDATE appointments SET status=@status, square_booking_id=@square_booking_id WHERE id=@id`)

function loadSession(phone){
  const row = db.prepare(`SELECT data_json FROM sessions WHERE phone=@phone`).get({phone})
  if (!row?.data_json) return null
  const s = JSON.parse(row.data_json)
  if (Array.isArray(s.lastHours_ms)) s.lastHours = s.lastHours_ms.map(ms => dayjs.tz(ms, EURO_TZ))
  if (s.pendingDateTime_ms) s.pendingDateTime = dayjs.tz(s.pendingDateTime_ms, EURO_TZ)
  return s
}
function saveSession(phone, s){
  const c={...s}
  c.lastHours_ms = Array.isArray(s.lastHours) ? s.lastHours.map(d=>dayjs.isDayjs(d)?d.valueOf():null).filter(Boolean) : []
  c.pendingDateTime_ms = s.pendingDateTime ? s.pendingDateTime.valueOf() : null
  delete c.lastHours; delete c.pendingDateTime
  const j = JSON.stringify(c)
  const upd = db.prepare(`UPDATE sessions SET data_json=@data_json, updated_at=@u WHERE phone=@p`)
  const res = upd.run({ p:phone, data_json:j, u:new Date().toISOString() })
  if (res.changes===0){
    db.prepare(`INSERT INTO sessions (phone,data_json,updated_at) VALUES (@p,@data_json,@u)`)
      .run({ p:phone, data_json:j, u:new Date().toISOString() })
  }
}
function clearSession(phone){ db.prepare(`DELETE FROM sessions WHERE phone=@phone`).run({phone}) }

// ===== Servicios enumerados desde .env
function servicesForSedeKey(sedeKey){
  const prefix = (sedeKey==="la_luz") ? "SQ_SVC_luz_" : "SQ_SVC_"
  const out=[]
  for (const [k,v] of Object.entries(process.env)) {
    if (!k.startsWith(prefix)) continue
    const label = k.replace(prefix,"")
      .replaceAll("_"," ")
      .replace(/\b([a-z])/g, m => m.toUpperCase())
      .replace("Pestan", "Pestañ")
    const [id] = String(v||"").split("|"); if (!id) continue
    out.push({ index: out.length+1, label, key: k })
  }
  return out.sort((a,b)=> a.label.localeCompare(b.label, "es"))
}

// ===== Citas enumeradas (Square + fallback DB)
async function enumerateCitasByPhone(phone){
  const items=[]
  let cid=null
  try{
    const e164 = normalizePhoneES(phone)
    const search = await square.customersApi.searchCustomers({ query:{ filter:{ phoneNumber:{ exact:e164 } } } })
    cid = (search?.result?.customers||[])[0]?.id || null
  }catch{}
  if (cid){
    try{
      const resp = await square.bookingsApi.listBookings(undefined, undefined, cid)
      const list = resp?.result?.bookings || []
      const nowISO = new Date().toISOString()
      for (const b of list){
        if (b?.startAt && b.startAt >= nowISO){
          const start = dayjs.tz(b.startAt, EURO_TZ)
          const locKey = idToLocationKey(b.locationId)
          const seg = (b.appointmentSegments||[{}])[0]
          const labelSvc = seg?.serviceVariationId ? `Servicio ${seg.serviceVariationId.slice(-6)}` : "Servicio"
          items.push({
            index: items.length+1,
            id: b.id,
            fecha_iso: start.format("YYYY-MM-DD"),
            pretty: fmtES(start),
            sede: locKey ? locationNice(locKey) : "—",
            profesional: seg?.teamMemberId ? `Prof. ${seg.teamMemberId.slice(-4)}` : null,
            servicio: labelSvc
          })
        }
      }
    }catch{}
  }
  if (!items.length){
    const nowISO = new Date().toISOString()
    const rows = db.prepare(`SELECT * FROM appointments WHERE customer_phone=@p AND start_iso>=@n AND status='confirmed' ORDER BY start_iso ASC`).all({p:phone, n:nowISO})
    for (const r of rows){
      const start = dayjs.tz(r.start_iso, EURO_TZ)
      items.push({
        index: items.length+1,
        id: r.square_booking_id || r.id,
        fecha_iso: start.format("YYYY-MM-DD"),
        pretty: fmtES(start),
        sede: locationNice(r.location_key),
        profesional: r.staff_id ? `Prof. ${r.staff_id.slice(-4)}` : null,
        servicio: r.service_label
      })
    }
  }
  return items
}

// ===== Square helpers
async function getServiceIdAndVersion(envKey){
  const raw = process.env[envKey]; if (!raw) return null
  let [id, ver] = String(raw).split("|"); ver = ver?Number(ver):null
  if (!id) return null
  if (!ver){
    try{
      const resp = await square.catalogApi.retrieveCatalogObject(id, true)
      ver = resp?.result?.object?.version ? Number(resp.result.object.version) : null
    }catch{}
  }
  return ver ? { id, version:ver } : { id, version:null }
}
async function findOrCreateCustomer({ name, email, phone }){
  try{
    const e164=normalizePhoneES(phone); if(!e164) return null
    const got = await square.customersApi.searchCustomers({ query:{ filter:{ phoneNumber:{ exact:e164 } } } })
    const c = (got?.result?.customers||[])[0]; if (c) return c
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
  if (DRY_RUN) return { id:`TEST_${Date.now()}` }
  const sid = await getServiceIdAndVersion(envServiceKey)
  if (!sid?.id) return null
  const startISO = startEU.tz("UTC").toISOString()
  const idempotencyKey = stableKey({ loc:locationToId(locationKey), sv:sid.id, startISO, customerId, teamMemberId })
  const body = {
    idempotencyKey,
    booking: {
      locationId: locationToId(locationKey),
      startAt: startISO,
      customerId,
      appointmentSegments: [{
        teamMemberId: teamMemberId || undefined,
        serviceVariationId: sid.id,
        serviceVariationVersion: Number(sid.version || 1),
        durationMinutes: durationMin || 60
      }]
    }
  }
  try{
    const resp = await square.bookingsApi.createBooking(body)
    return resp?.result?.booking || null
  }catch(e){
    console.error("createBooking:", e?.message||e); return null
  }
}
async function cancelBooking(bookingId){
  if (DRY_RUN) return true
  try{
    const body = { idempotencyKey:`cancel_${bookingId}_${Date.now()}` }
    const resp = await square.bookingsApi.cancelBooking(bookingId, body)
    return !!resp?.result?.booking
  }catch(e){ console.error("cancelBooking:", e?.message||e); return false }
}

// ===== Validación de la decisión de la IA (anti-JSON loco)
function sanitizeDecision(dec, payload){
  const base = {
    intent: 5,
    needs_clarification: true,
    requires_confirmation: false,
    slots:{
      sede: payload?.sede_actual ?? null,
      service_index: null,
      appointment_index: null,
      date_iso: null, time_iso: null, datetime_iso: null,
      profesional: null, notes: null
    },
    selection:{ time_index:null, date_index:null, confirm_index:null },
    client_message:"Lo miro y te digo en un momento."
  }
  if (!dec || typeof dec!=="object") return base
  const out = structuredClone(base)
  const clampIdx = (n, max) => (Number.isInteger(n) && n>=1 && (!max || n<=max)) ? n : null
  out.intent = [1,2,3,4,5].includes(Number(dec.intent)) ? Number(dec.intent) : base.intent
  out.needs_clarification = !!dec.needs_clarification
  out.requires_confirmation = !!dec.requires_confirmation
  out.client_message = String(dec.client_message||base.client_message).slice(0, 1000)

  const sev = dec.slots||{}
  out.slots.sede = (sev.sede==="torremolinos"||sev.sede==="la_luz") ? sev.sede : base.slots.sede
  out.slots.service_index = clampIdx(sev.service_index, (payload?.servicios_enumerados||[]).length)
  out.slots.appointment_index = clampIdx(sev.appointment_index, (payload?.citas_enumeradas||[]).length)
  out.slots.date_iso = sev.date_iso || null
  out.slots.time_iso = sev.time_iso || null
  out.slots.datetime_iso = sev.datetime_iso || null
  out.slots.profesional = sev.profesional ? String(sev.profesional).slice(0,80) : null
  out.slots.notes = sev.notes ? String(sev.notes).slice(0,120) : null

  const sel = dec.selection||{}
  out.selection.time_index = clampIdx(sel.time_index, (payload?.horas_enumeradas||[]).length)
  out.selection.date_index = clampIdx(sel.date_index, (payload?.fechas_enumeradas||[]).length)
  out.selection.confirm_index = [1,2].includes(Number(sel.confirm_index)) ? Number(sel.confirm_index): null
  return out
}
function safeParseJSON(txt){
  try{
    const start = txt.indexOf("{"), end = txt.lastIndexOf("}")
    if (start>=0 && end>start) txt = txt.slice(start, end+1)
    return JSON.parse(txt)
  }catch{ return null }
}

// ===== Mini-web + WhatsApp
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

// ===== Carga robusta de Baileys
async function loadBaileys(){
  const require = createRequire(import.meta.url)
  let mod = null
  try { mod = require("@whiskeysockets/baileys") } catch {}
  if (!mod) { try { mod = await import("@whiskeysockets/baileys") } catch {} }
  if (!mod) throw new Error("No se pudo cargar @whiskeysockets/baileys")
  const makeWASocket =
    mod.makeWASocket ||
    mod.default?.makeWASocket ||
    (typeof mod.default === "function" ? mod.default : undefined)
  const useMultiFileAuthState =
    mod.useMultiFileAuthState ||
    mod.default?.useMultiFileAuthState
  const fetchLatestBaileysVersion =
    mod.fetchLatestBaileysVersion ||
    mod.default?.fetchLatestBaileysVersion ||
    (async()=>({ version:[2,3000,0] }))
  const Browsers =
    mod.Browsers ||
    mod.default?.Browsers || { macOS:(n="Desktop")=>["MacOS",n,"121.0.0"] }
  if (typeof makeWASocket !== "function" || typeof useMultiFileAuthState !== "function") {
    throw new Error("Baileys incompatible. Instala @whiskeysockets/baileys ^6.x")
  }
  return { makeWASocket, useMultiFileAuthState, fetchLatestBaileysVersion, Browsers }
}

// ===== Micromensajes
const WELCOME_MSG =
`Gracias por comunicarte con Gapink Nails. ¿Cómo podemos ayudarte?

Solo atenderemos por WhatsApp y llamadas de lunes a viernes de 10 a 14:00 y de 16:00 a 20:00.

Reserva: https://gapinknails.square.site/
Para cambios usa el enlace del SMS de tu cita.
¡Cuéntanos!`
const OOH_MSG = (link) => `Ahora estamos fuera de horario. Dime día y hora y te lo gestiono igual 😉 (o usa ${link}).`

// ===== Anti duplicados + cola por usuario
const QUEUE = new Map()
function enqueue(key, job){
  const prev = QUEUE.get(key) || Promise.resolve()
  const next = prev.then(job, job).finally(()=>{ if (QUEUE.get(key)===next) QUEUE.delete(key) })
  QUEUE.set(key, next)
  return next
}

async function startBot(){
  try{
    const { makeWASocket, useMultiFileAuthState, fetchLatestBaileysVersion, Browsers } = await loadBaileys()
    if(!fs.existsSync("auth_info")) fs.mkdirSync("auth_info",{recursive:true})
    const { state, saveCreds } = await useMultiFileAuthState("auth_info")
    const { version } = await fetchLatestBaileysVersion().catch(()=>({version:[2,3000,0]}))

    const sock = makeWASocket({
      logger: pino({ level:"silent" }),
      printQRInTerminal: false,
      auth: state,
      version,
      browser: Browsers.macOS("Desktop"),
      syncFullHistory: false
    })
    globalThis.sock = sock

    sock.ev.on("connection.update", ({connection, qr})=>{
      if (qr){ lastQR=qr; conectado=false; try{ qrcodeTerminal.generate(qr,{small:true}) }catch{} }
      if (connection==="open"){ lastQR=null; conectado=true; console.log("✅ WhatsApp listo") }
      if (connection==="close"){ conectado=false; console.log("❌ Conexión cerrada. Reintentando…"); setTimeout(()=>startBot().catch(console.error), 2500) }
    })
    sock.ev.on("creds.update", saveCreds)

    sock.ev.on("messages.upsert", async ({messages})=>{
      const m=messages?.[0]; if (!m?.message || m.key.fromMe) return
      const jid = m.key.remoteJid
      const phone = normalizePhoneES((jid||"").split("@")[0]||"") || (jid||"").split("@")[0]
      const textRaw  = (m.message.conversation || m.message.extendedTextMessage?.text || m.message?.imageMessage?.caption || "").trim()
      if (!textRaw) return

      await enqueue(phone, async ()=>{
        // cargar sesión
        let s = loadSession(phone) || {
          greeted:false,
          lastOOHDay:null,
          sede:null,
          lastServices:[],
          lastHours:[],
          lastCitas:[],
          lastDates:[],
          pendingAction:null,   // {type:"create"|"cancel"|"edit"}
          pendingDateTime:null,
          name:null, email:null,
          last_msg_id:null
        }

        // anti-duplicados
        if (s.last_msg_id === m.key.id) return
        s.last_msg_id = m.key.id

        const nowEU = dayjs().tz(EURO_TZ)

        // Bienvenida 1 vez
        if (!s.greeted){
          await sock.sendMessage(jid, { text: WELCOME_MSG })
          s.greeted = true; saveSession(phone,s)
        }

        // Fuera de horario ⇒ una vez al día
        const inHours = insideBusinessHours(nowEU.clone(), 15)
        const todayKey = nowEU.format("YYYY-MM-DD")
        if (!inHours && s.lastOOHDay !== todayKey){
          await sock.sendMessage(jid, { text: OOH_MSG("https://gapinknails.square.site/") })
          s.lastOOHDay = todayKey; saveSession(phone,s)
        }

        // Actualiza sede por texto libre (“Velázquez”, “Málaga”, “Torre…”)
        const maybeSede = detectSedeFromText(textRaw)
        if (maybeSede) s.sede = maybeSede

        // Enumeraciones actuales
        const servicios = s.sede ? servicesForSedeKey(s.sede) : null
        const hoursList = (() => {
          if (!s.sede) return []
          const base = nextOpeningFrom(nowEU.add(NOW_MIN_OFFSET_MIN,"minute"))
          const offers = proposeSlots({ fromEU: base, durationMin:60, n:3 })
          s.lastHours = offers
          return enumerateHours(offers)
        })()
        const citas = await enumerateCitasByPhone(phone); s.lastCitas = citas
        const confirmChoices = [{index:1,label:"sí"},{index:2,label:"no"}]

        // Orquestador
        const payload = {
          user_message: textRaw,
          sede_actual: s.sede,
          servicios_enumerados: servicios || null,
          horas_enumeradas: hoursList.length ? hoursList : null,
          citas_enumeradas: citas.length ? citas : null,
          fechas_enumeradas: null,
          confirm_choices: confirmChoices
        }
        const aiRaw = await aiChat([
          { role:"system", content:SYSTEM_PROMPT },
          { role:"user", content: JSON.stringify(payload, null, 2) }
        ])
        const decision = sanitizeDecision(safeParseJSON(aiRaw), payload)

        // Persistir posibles cambios
        if (decision?.slots?.sede) s.sede = decision.slots.sede

        // Mapas para búsquedas por índice
        const srvMap = new Map((servicios||[]).map(x=>[x.index,x]))
        const hrsMap = new Map(hoursList.map(h=>[h.index,h]))
        const citasMap = new Map(citas.map(c=>[c.index,c]))

        // Selecciones del JSON
        let chosenServiceEnvKey=null, chosenServiceLabel=null
        if (decision?.slots?.service_index && srvMap.has(decision.slots.service_index)){
          const row = srvMap.get(decision.slots.service_index)
          chosenServiceEnvKey = row.key; chosenServiceLabel = row.label
        }
        if (decision?.selection?.time_index && hrsMap.has(decision.selection.time_index)){
          s.pendingDateTime = dayjs.tz(hrsMap.get(decision.selection.time_index).iso, EURO_TZ)
        } else if (decision?.slots?.datetime_iso){
          const dt = toDayjsEU(decision.slots.datetime_iso)
          if (dt) s.pendingDateTime = ceilToSlotEU(dt)
        }

        // Enviar SIEMPRE el mensaje al cliente
        const msgOut = String(decision.client_message||"").trim() || "Perfecto 👍"
        await sock.sendMessage(jid, { text: msgOut })

        // Acciones según intención
        const intent = Number(decision.intent||0)
        const confirmIdx = decision?.selection?.confirm_index

        async function closeCreate(){
          if (!s.sede || !chosenServiceEnvKey || !s.pendingDateTime){
            // faltan datos: el orquestador ya habrá preguntado
            return
          }
          const startEU = ceilToSlotEU(s.pendingDateTime.clone())
          if (!insideBusinessHours(startEU, 60)){
            await sock.sendMessage(jid, { text:"Esa hora cae fuera de horario (L–V 10–14 / 16–20). Dime otra dentro de ese rango 🕒" })
            s.pendingDateTime=null; saveSession(phone,s); return
          }
          const customer = await findOrCreateCustomer({ name:s.name, email:s.email, phone })
          if (!customer){
            await sock.sendMessage(jid, { text:"Necesito un email o nombre para completar el perfil. Si quieres, me lo pasas y seguimos 💌" })
            return
          }
          const booking = await createBooking({
            startEU, locationKey:s.sede, envServiceKey:chosenServiceEnvKey,
            durationMin:60, customerId:customer.id, teamMemberId:null
          })
          if (!booking){
            await sock.sendMessage(jid, { text:"No pude reservar ese hueco 😕. Dime otra hora o usa https://gapinknails.square.site/" })
            return
          }
          const aptId = `apt_${Math.random().toString(36).slice(2,8)}${Date.now().toString(36).slice(-4)}`
          insertAppt.run({
            id:aptId,
            customer_name: customer?.givenName||null,
            customer_phone: phone,
            customer_square_id: customer.id,
            location_key: s.sede,
            service_env_key: chosenServiceEnvKey,
            service_label: chosenServiceLabel || "Servicio",
            duration_min: 60,
            start_iso: startEU.tz("UTC").toISOString(),
            end_iso: startEU.clone().add(60,"minute").tz("UTC").toISOString(),
            staff_id: null,
            status: "confirmed",
            created_at: new Date().toISOString(),
            square_booking_id: booking.id
          })
          await sock.sendMessage(jid, { text:
`Reserva confirmada 🎉
Salón: ${locationNice(s.sede)}
Dirección: ${s.sede==="la_luz"?ADDRESS_LUZ:ADDRESS_TORRE}
Servicio: ${chosenServiceLabel||"—"}
Fecha: ${fmtES(startEU)}
Duración: 60 min

¡Te esperamos!`
          })
          clearSession(phone)
        }

        if (intent===1){ // Concertar
          if (decision.requires_confirmation){
            if (confirmIdx===1) await closeCreate()
            else if (confirmIdx===2) await sock.sendMessage(jid, { text:"Sin problema, dime otra hora o día y te paso huequitos 🗓️" })
          } else {
            await closeCreate()
          }
          saveSession(phone,s); return
        }

        if (intent===2){ // Cancelar
          const aidx = decision?.slots?.appointment_index
          if (!aidx || !citasMap.has(aidx)){ saveSession(phone,s); return }
          if (decision.requires_confirmation){
            if (confirmIdx===1){
              const ok = await cancelBooking(citasMap.get(aidx).id)
              await sock.sendMessage(jid, { text: ok?`He cancelado la cita (opción ${aidx}) ✅`:`No pude cancelarla. Prueba con el enlace del SMS o dime y lo intento otra vez.` })
            } else if (confirmIdx===2){
              await sock.sendMessage(jid, { text:"Cancelación anulada. Si necesitas otra cosa, dime 😉" })
            }
          }
          saveSession(phone,s); return
        }

        if (intent===3){ // Editar (reprogramar sencillo)
          const aidx = decision?.slots?.appointment_index
          if (!aidx || !citasMap.has(aidx)){ saveSession(phone,s); return }
          if (decision.requires_confirmation && confirmIdx===2){
            await sock.sendMessage(jid, { text:"Ok, no reprogramo. Si quieres te paso otras horas 😊" })
            saveSession(phone,s); return
          }
          if ((decision.requires_confirmation && confirmIdx===1) || (!decision.requires_confirmation && s.pendingDateTime)){
            const old = citasMap.get(aidx)
            const canceled = await cancelBooking(old.id)
            if (!canceled){ await sock.sendMessage(jid, { text:"No pude reprogramar (falló cancelar). Pruébalo de nuevo o usa el enlace del SMS 🙏" }); return }
            // Para el nuevo servicio, usa el mismo de DB si no se eligió otro
            let envServiceKey = chosenServiceEnvKey
            if (!envServiceKey){
              const row = db.prepare(`SELECT * FROM appointments WHERE square_booking_id=@id`).get({id:old.id})
              envServiceKey = row?.service_env_key || null
              chosenServiceLabel = row?.service_label || chosenServiceLabel
              s.sede = row?.location_key || s.sede
            }
            const customer = await findOrCreateCustomer({ name:s.name, email:s.email, phone })
            if (!customer){ await sock.sendMessage(jid, { text:"Me falta un email/nombre para cerrar la reprogramación." }); return }
            const booking = await createBooking({
              startEU:s.pendingDateTime, locationKey:s.sede, envServiceKey, durationMin:60, customerId:customer.id, teamMemberId:null
            })
            if (!booking){ await sock.sendMessage(jid, { text:"No pude crear la nueva cita. ¿Te paso otras horas?" }); return }
            await sock.sendMessage(jid, { text:`Listo, reprogramada a ${fmtES(s.pendingDateTime)} ✅` })
            clearSession(phone)
          }
          return
        }

        // Intención 4/5 u otras: ya contestó el orquestador
        saveSession(phone,s)
      })
    })
  }catch(e){
    console.error("startBot:", e?.message||e)
  }
}
