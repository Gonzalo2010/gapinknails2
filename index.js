// Gapink Nails WhatsApp Bot — 1 servicio: "uñas acrílicas"
// IA: interpreta + elige acciones + redacta.
// Sistema: crea/edita/cancela en Square, sugiere antes/próximo, elige staff con menos carga.
// Fallback: si falta la variación en Square, confirma local y te avisa para poner SQ_SV_UNAS_ACRILICAS.

import express from "express"
import baileys from "@whiskeysockets/baileys"
import pino from "pino"
import qrcode from "qrcode"
import qrcodeTerminal from "qrcode-terminal"
import "dotenv/config"
import fs from "fs"
import { webcrypto, createHash } from "crypto"
import Database from "better-sqlite3"
import dayjs from "dayjs"
import utc from "dayjs/plugin/utc.js"
import tz from "dayjs/plugin/timezone.js"
import "dayjs/locale/es.js"
import { Client, Environment } from "square"

if (!globalThis.crypto) globalThis.crypto = webcrypto
dayjs.extend(utc); dayjs.extend(tz); dayjs.locale("es")
const EURO_TZ = "Europe/Madrid"
const { makeWASocket, useMultiFileAuthState, fetchLatestBaileysVersion, Browsers } = baileys

// ───────────────── Config negocio
const SERVICE_NAME = "uñas acrílicas"
const SERVICE_MIN  = Number(process.env.SERVICE_MIN || 90) // duración por defecto 90
const WORK_DAYS = [1,2,3,4,5,6] // Lunes-Sábado
const OPEN_HOUR  = 10
const CLOSE_HOUR = 20
const SLOT_MIN   = 30

// ───────────────── ENV / Flags
const DEEPSEEK_API_KEY = process.env.DEEPSEEK_API_KEY
const DEEPSEEK_API_URL = process.env.DEEPSEEK_API_URL || "https://api.deepseek.com/chat/completions"
const DEEPSEEK_MODEL   = process.env.DEEPSEEK_MODEL   || "deepseek-chat"

const SQUARE_ACCESS_TOKEN = process.env.SQUARE_ACCESS_TOKEN
const SQUARE_ENV = process.env.SQUARE_ENV === "production" ? Environment.Production : Environment.Sandbox
const SQUARE_LOCATION_ID = process.env.SQUARE_LOCATION_ID
const SQUARE_BOOKINGS = String(process.env.SQUARE_BOOKINGS || "on").toLowerCase() !== "off"

// Staff permitido y mapeo “con Gonzalo”
const TEAM_MEMBER_IDS = (process.env.SQ_TEAM_IDS || "").split(",").map(s=>s.trim()).filter(Boolean)
const STAFF_NAME_MAP = (() => {
  const raw = (process.env.SQ_STAFF_MAP || "").trim() // "gonzalo:TM1,maría:TM2"
  const map = {}
  raw.split(",").map(s=>s.trim()).filter(Boolean).forEach(pair=>{
    const [name,id] = pair.split(":").map(x=>x?.trim())
    if (name && id) map[name.toLowerCase()] = id
  })
  return map
})()

// ───────────────── Square client
const square = new Client({ accessToken: SQUARE_ACCESS_TOKEN, environment: SQUARE_ENV })
let LOCATION_TZ = EURO_TZ

// ───────────────── Servicio único
const SERVICES = { [SERVICE_NAME]: SERVICE_MIN }
const SERVICE_VARIATIONS = { [SERVICE_NAME]: process.env.SQ_SV_UNAS_ACRILICAS || null }
const SERVICE_KEYWORDS = { [SERVICE_NAME]: ["uñas acrílicas","acrilicas","acrílicas","nails","manicura","uñas"] }

// ───────────────── Utilidades
const onlyDigits = (s="") => (s||"").replace(/\D+/g,"")
const rmDiacritics = (s="") => s.normalize("NFD").replace(/\p{Diacritic}/gu,"")
const YES_RE = /\b(si|sí|ok|vale|confirmo|confirmar|de acuerdo|perfecto)\b/i
const NO_RE  = /\b(no|otra|cambia|no confirmo|mejor mas tarde|mejor más tarde|no puedo)\b/i
const wait = (ms) => new Promise(r => setTimeout(r, ms))

function normalizePhoneES(raw) {
  const d = onlyDigits(raw)
  if (!d) return null
  if (raw.startsWith("+") && d.length >= 8 && d.length <= 15) return `+${d}`
  if (d.startsWith("34") && d.length === 11) return `+${d}`
  if (d.length === 9) return `+34${d}`
  if (d.startsWith("00")) return `+${d.slice(2)}`
  return `+${d}`
}
function fmtES(d) {
  const t = d.tz(EURO_TZ)
  const dias = ["domingo","lunes","martes","miércoles","jueves","viernes","sábado"]
  return `${dias[t.day()]} ${String(t.date()).padStart(2,"0")}/${String(t.month()+1).padStart(2,"0")} ${String(t.hour()).padStart(2,"0")}:${String(t.minute()).padStart(2,"0")}`
}
function parseDateTimeES(dtText) {
  if (!dtText) return null
  const t = rmDiacritics(dtText.toLowerCase())
  let base = null
  if (/\bhoy\b/.test(t)) base = dayjs().tz(EURO_TZ)
  else if (/\bmanana\b/.test(t)) base = dayjs().tz(EURO_TZ).add(1,"day")
  if (!base) {
    const M = {enero:1,febrero:2,marzo:3,abril:4,mayo:5,junio:6,julio:7,agosto:8,septiembre:9,setiembre:9,octubre:10,noviembre:11,diciembre:12,ene:1,feb:2,mar:3,abr:4,may:5,jun:6,jul:7,ago:8,sep:9,oct:10,nov:11,dic:12}
    const m1 = t.match(/\b(\d{1,2})\s+de\s+(enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|setiembre|octubre|noviembre|diciembre|ene|feb|mar|abr|may|jun|jul|ago|sep|oct|nov|dic)\b(?:\s+de\s+(\d{4}))?/)
    if (m1) {
      const dd = +m1[1], mm = M[m1[2]], yy = m1[3] ? +m1[3] : dayjs().tz(EURO_TZ).year()
      base = dayjs.tz(`${yy}-${String(mm).padStart(2,"0")}-${String(dd).padStart(2,"0")} 00:00`, EURO_TZ)
    }
  }
  if (!base) {
    const m2 = t.match(/\b(\d{1,2})[\/\-](\d{1,2})(?:[\/\-](\d{2,4}))?\b/)
    if (m2) {
      let yy = m2[3] ? +m2[3] : dayjs().tz(EURO_TZ).year()
      if (yy < 100) yy += 2000
      base = dayjs.tz(`${yy}-${String(+m2[2]).padStart(2,"0")}-${String(+m2[1]).padStart(2,"0")} 00:00`, EURO_TZ)
    }
  }
  if (!base) base = dayjs().tz(EURO_TZ)
  let hour = null, minute = 0
  const hm = t.match(/(\d{1,2})(?::|h)?(\d{2})?\s*(am|pm)?\b/)
  if (hm) {
    hour = +hm[1]; minute = hm[2] ? +hm[2] : 0
    const ap = hm[3]
    if (ap === "pm" && hour < 12) hour += 12
    if (ap === "am" && hour === 12) hour = 0
  }
  if (hour === null) return null
  return base.hour(hour).minute(minute).second(0).millisecond(0)
}
function detectPreferredStaff(text="") {
  const low = rmDiacritics(text.toLowerCase())
  for (const name in STAFF_NAME_MAP) if (low.includes(rmDiacritics(name))) return STAFF_NAME_MAP[name]
  return null
}
function detectService(text="") {
  const low = rmDiacritics(text.toLowerCase())
  for (const kw of SERVICE_KEYWORDS[SERVICE_NAME]) if (low.includes(rmDiacritics(kw))) return SERVICE_NAME
  // Solo hay un servicio → por defecto ese si la intención es reservar/cambiar
  if (/\b(reserv|cita|agenda|coger|pilla|poner|cambiar|mover|reprogram)/i.test(low)) return SERVICE_NAME
  return null
}

// ───────────────── DeepSeek (NLU + decisiones)
async function dsChat(messages, temperature=0.2, timeoutMs=15000) {
  const controller = new AbortController()
  const t = setTimeout(()=>controller.abort(), timeoutMs)
  try {
    const r = await fetch(DEEPSEEK_API_URL, {
      method:"POST",
      headers:{ "Content-Type":"application/json", "Authorization":`Bearer ${DEEPSEEK_API_KEY}` },
      body: JSON.stringify({ model: DEEPSEEK_MODEL, messages, temperature }),
      signal: controller.signal
    })
    clearTimeout(t)
    if (!r.ok) throw new Error(`DeepSeek ${r.status}: ${await r.text()}`)
    const j = await r.json()
    return (j?.choices?.[0]?.message?.content || "").trim()
  } catch(e) {
    console.error("DeepSeek:", e?.message || e); return ""
  }
}

async function nluExtract(message) {
  const schema = `Devuelve SOLO JSON válido:
{
  "intent": "create|reschedule|cancel|info",
  "datetime_text": "texto con fecha/hora si aparece",
  "staff_hint": "nombre si aparece",
  "confirm": "yes|no|unknown",
  "name": "si aparece",
  "email": "si aparece"
}`
  const content = await dsChat([
    {role:"system", content:`Eres un orquestador. Extrae intención y campos. Español de España. ${schema}`},
    {role:"user", content: message}
  ], 0.1)
  try { return JSON.parse(content.replace(/^```(json)?/i,"").replace(/```$/,"")) } catch { return { intent:"info" } }
}

async function aiReply(facts, allowedActions) {
  const schema = `Devuelve SOLO JSON:
{ "action":"${allowedActions.join("|")}", "reply":"mensaje breve, natural, sin emojis" }`
  const msg = await dsChat([
    {role:"system", content:`Te pasan HECHOS. Elige UNA acción válida y redacta respuesta. ${schema}`},
    {role:"user", content: JSON.stringify(facts)}
  ], 0.2)
  try {
    const j = JSON.parse(msg.replace(/^```(json)?/i,"").replace(/```$/,""))
    if (!allowedActions.includes(j.action)) throw new Error("acción no válida")
    return j
  } catch {
    return { action: allowedActions[0], reply: "¿Me confirmas el día y la hora que te viene bien?" }
  }
}

// ───────────────── Square helpers
async function squareInit() {
  try {
    const locs = await square.locationsApi.listLocations()
    const loc = (locs.result.locations||[]).find(l=>l.id===SQUARE_LOCATION_ID) || (locs.result.locations||[])[0]
    if (loc?.timezone) LOCATION_TZ = loc.timezone
  } catch(e){ console.error("Square init:", e?.message||e) }
}
async function squareFindCustomerByPhone(phoneE164) {
  try {
    const resp = await square.customersApi.searchCustomers({ query: { filter: { phoneNumber: { exact: phoneE164 } } } })
    return resp?.result?.customers || []
  } catch(e){ console.error("searchCustomers:", e?.message||e); return [] }
}
async function squareCreateOrPickCustomer({givenName, emailAddress, phoneNumber}, candidates) {
  if (candidates.length===1) return candidates[0]
  if (candidates.length>1) return null
  try {
    const resp = await square.customersApi.createCustomer({
      idempotencyKey: `cust_${Date.now()}_${Math.random().toString(36).slice(2,8)}`,
      givenName, emailAddress, phoneNumber
    })
    return resp?.result?.customer || null
  } catch(e){ console.error("createCustomer:", e?.message||e); return null }
}
async function getServiceVariationVersion(id) {
  if (!id) return undefined
  try {
    const resp = await square.catalogApi.retrieveCatalogObject(id, true)
    return resp?.result?.object?.version
  } catch { return undefined }
}
function idempoKeyBooking({locationId, serviceVariationId, startISO, customerId}) {
  const raw = `${locationId}|${serviceVariationId}|${startISO}|${customerId}`
  return createHash("sha256").update(raw).digest("hex").slice(0,48)
}
async function squareCreateBooking({startEU, customerId, teamMemberId}) {
  const serviceVariationId = SERVICE_VARIATIONS[SERVICE_NAME]
  if (!SQUARE_BOOKINGS || !SQUARE_LOCATION_ID || !teamMemberId || !serviceVariationId) return null
  const version = await getServiceVariationVersion(serviceVariationId)
  if (!version) return null
  const startISO = startEU.tz("UTC").toISOString()
  const body = {
    idempotencyKey: idempoKeyBooking({locationId:SQUARE_LOCATION_ID, serviceVariationId, startISO, customerId}),
    booking: {
      locationId: SQUARE_LOCATION_ID,
      startAt: startISO,
      customerId,
      appointmentSegments: [{
        teamMemberId,
        serviceVariationId,
        serviceVariationVersion: Number(version),
        durationMinutes: SERVICE_MIN
      }]
    }
  }
  try {
    const resp = await square.bookingsApi.createBooking(body)
    return resp?.result?.booking || null
  } catch(e){ console.error("createBooking:", e?.message||e); return null }
}
async function squareCancelBooking(bookingId) {
  try { await square.bookingsApi.cancelBooking(bookingId); return true }
  catch(e){ console.error("cancelBooking:", e?.message||e); return false }
}

// ───────────────── DB local
const db = new Database("gapink.db")
db.pragma("journal_mode=WAL")
db.exec(`
CREATE TABLE IF NOT EXISTS appointments (
  id TEXT PRIMARY KEY,
  customer_name TEXT,
  customer_phone TEXT,
  customer_square_id TEXT,
  service TEXT,
  duration_min INTEGER,
  start_iso TEXT,
  end_iso TEXT,
  staff_id TEXT,
  status TEXT,            -- pending|confirmed|cancelled
  created_at TEXT,
  square_booking_id TEXT
);
CREATE TABLE IF NOT EXISTS sessions (
  phone TEXT PRIMARY KEY,
  data_json TEXT,
  updated_at TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_slot ON appointments(staff_id, start_iso)
WHERE status IN ('pending','confirmed');
CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_customer_slot ON appointments(customer_phone, start_iso)
WHERE status IN ('pending','confirmed');
`)
const insertAppt = db.prepare(`INSERT INTO appointments
(id, customer_name, customer_phone, customer_square_id, service, duration_min, start_iso, end_iso, staff_id, status, created_at, square_booking_id)
VALUES (@id, @customer_name, @customer_phone, @customer_square_id, @service, @duration_min, @start_iso, @end_iso, @staff_id, @status, @created_at, @square_booking_id)`)
const updateAppt = db.prepare(`UPDATE appointments SET status=@status, square_booking_id=@square_booking_id WHERE id=@id`)
const deleteAppt = db.prepare(`DELETE FROM appointments WHERE id=@id`)
const getSessionRow = db.prepare(`SELECT * FROM sessions WHERE phone=@phone`)
const upsertSession = db.prepare(`INSERT INTO sessions (phone, data_json, updated_at)
VALUES (@phone, @data_json, @updated_at)
ON CONFLICT(phone) DO UPDATE SET data_json=excluded.data_json, updated_at=excluded.updated_at`)
const clearSession = db.prepare(`DELETE FROM sessions WHERE phone=@phone`)

function loadSession(phone) {
  const row = getSessionRow.get({phone})
  if (!row?.data_json) return null
  const raw = JSON.parse(row.data_json)
  const data = {...raw}
  if (raw.startEUISO) data.startEU = dayjs.tz(raw.startEUISO, EURO_TZ)
  return data
}
function saveSession(phone, data) {
  const s = {...data}
  s.startEUISO = data.startEU?.toISOString ? data.startEU.toISOString() : (data.startEUISO || null)
  delete s.startEU
  upsertSession.run({ phone, data_json: JSON.stringify(s), updated_at: new Date().toISOString() })
}

// ───────────────── Disponibilidad y staff
function getBookedIntervals(fromIso, toIso) {
  const rows = db.prepare(`SELECT start_iso, end_iso, staff_id, customer_phone FROM appointments 
    WHERE status IN ('pending','confirmed') AND start_iso < @to AND end_iso > @from`)
    .all({from: fromIso, to: toIso})
  return rows.map(r => ({ start: dayjs(r.start_iso), end: dayjs(r.end_iso), staff_id: r.staff_id, customer_phone: r.customer_phone }))
}
function countBookingsForStaffDay(staffId, ymd) {
  const a = dayjs.tz(`${ymd} 00:00`, EURO_TZ).tz("UTC").toISOString()
  const b = dayjs.tz(`${ymd} 23:59`, EURO_TZ).tz("UTC").toISOString()
  const row = db.prepare(`SELECT COUNT(*) c FROM appointments WHERE staff_id=@id AND start_iso BETWEEN @a AND @b AND status IN ('pending','confirmed')`)
    .get({id:staffId, a, b})
  return row?.c || 0
}
function chooseStaffForDateEU(dateEU) {
  const ids = TEAM_MEMBER_IDS.length ? TEAM_MEMBER_IDS : ["any"]
  if (ids.length===1) return ids[0]
  const ymd = dateEU.format("YYYY-MM-DD")
  return ids.map(id=>({id,c:countBookingsForStaffDay(id, ymd)})).sort((x,y)=>x.c-y.c)[0].id
}
function hasFree(intervals, startUTC, endUTC, staffId) {
  const busy = intervals.filter(i=>i.staff_id===staffId).some(i => (startUTC < i.end) && (i.start < endUTC))
  return !busy
}
function searchSlot(startEU, preferredStaffId=null) {
  const durationMin = SERVICE_MIN
  const now = dayjs().tz(EURO_TZ).add(15,"minute").second(0).millisecond(0)
  const fromIso = now.tz("UTC").toISOString()
  const toIso = now.add(21,"day").tz("UTC").toISOString()
  const intervals = getBookedIntervals(fromIso, toIso)
  const endEU = startEU.clone().add(durationMin,"minute")
  const dow = startEU.day()===0 ? 7 : startEU.day()
  const inHours = startEU.hour()>=OPEN_HOUR && (endEU.hour()<CLOSE_HOUR || (endEU.hour()===CLOSE_HOUR && endEU.minute()===0))
  const staffList = TEAM_MEMBER_IDS.length ? TEAM_MEMBER_IDS : ["any"]

  const can = (tEU, staffId) => {
    if (tEU.isBefore(now)) return null
    const startUTC = tEU.tz("UTC"), endUTC = tEU.clone().add(durationMin,"minute").tz("UTC")
    if (staffId==="any") {
      for (const id of staffList) if (hasFree(intervals, startUTC, endUTC, id)) return id
      return null
    }
    return hasFree(intervals, startUTC, endUTC, staffId) ? staffId : null
  }

  // exacto con preferido o con el de menor carga
  if (inHours && WORK_DAYS.includes(dow)) {
    if (preferredStaffId) {
      const ok = can(startEU, preferredStaffId)
      if (ok) return { type:"exact", when:startEU, staffId: ok }
    }
    const auto = chooseStaffForDateEU(startEU)
    const ok2 = can(startEU, auto)
    if (ok2) return { type:"exact", when:startEU, staffId: ok2 }
  }

  // antes, mismo día (cualquier staff)
  const dayStart = startEU.clone().hour(OPEN_HOUR).minute(0)
  for (let t=dayStart.clone(); t.isBefore(startEU); t=t.add(SLOT_MIN,"minute")) {
    const ok = can(t, "any")
    if (ok) return { type:"earlier", when:t, staffId: ok }
  }

  // más próximo, mismo día (cualquier staff)
  const dayEnd = startEU.clone().hour(CLOSE_HOUR).minute(0)
  for (let t=startEU.clone(); !t.isAfter(dayEnd); t=t.add(SLOT_MIN,"minute")) {
    const ok = can(t, "any")
    if (ok) return { type:"nearest", when:t, staffId: ok }
  }

  // otros días
  for (let d=1; d<=14; d++) {
    const base = startEU.clone().add(d,"day")
    const ds = base.clone().hour(OPEN_HOUR).minute(0)
    const de = base.clone().hour(CLOSE_HOUR).minute(0)
    for (let t=ds.clone(); !t.isAfter(de); t=t.add(SLOT_MIN,"minute")) {
      const ok = can(t, "any")
      if (ok) return { type:"nearest", when:t, staffId: ok }
    }
  }

  return { type:"none" }
}

// ───────────────── Web mínima
const app = express()
const PORT = process.env.PORT || 8080
let lastQR = null, conectado = false

app.get("/", (_req,res)=>{
  res.send(`<!doctype html><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<style>body{font-family:system-ui;display:grid;place-items:center;min-height:100vh;background:linear-gradient(135deg,#fce4ec,#f8bbd0);margin:0;color:#4a148c}
.card{background:#fff;padding:24px;border-radius:16px;box-shadow:0 6px 24px rgba(0,0,0,.08);text-align:center;max-width:560px;margin:20px}
.status{padding:8px 16px;border-radius:8px;font-weight:bold;margin:16px 0}
.connected{background:#e8f5e8;color:#2e7d32}.disconnected{background:#ffebee;color:#c62828}
</style>
<div class="card">
<h1>🌸 Gapink Nails Bot</h1>
<div class="status ${conectado?'connected':'disconnected'}">${conectado?'✅ Conectado':'❌ Desconectado'}</div>
<p>Servicio activo: <b>${SERVICE_NAME}</b> (${SERVICE_MIN} min)</p>
${!conectado && lastQR ? `<img src="/qr.png" width="320">` : ""}
</div>`)
})
app.get("/qr.png", async(_req,res)=>{
  if (!lastQR) return res.status(404).send("No hay QR")
  const png = await qrcode.toBuffer(lastQR, {type:"png",width:512,margin:1})
  res.set("Content-Type","image/png").send(png)
})
app.get("/health", (_req,res)=> res.json({ok:true, connected:conectado, service:SERVICE_NAME, duration:SERVICE_MIN, squareBookings:SQUARE_BOOKINGS, variation:!!SERVICE_VARIATIONS[SERVICE_NAME]}))

// ───────────────── Bot WhatsApp
async function startBot() {
  console.log("🚀 Bot arrancando…")
  try {
    if (!fs.existsSync("auth_info")) fs.mkdirSync("auth_info", {recursive:true})
    const { state, saveCreds } = await useMultiFileAuthState("auth_info")
    const { version } = await fetchLatestBaileysVersion()
    let isOpen=false, reconnecting=false

    const sock = makeWASocket({
      logger: pino({level:"silent"}), printQRInTerminal:false,
      auth: state, version, browser: Browsers.macOS("Desktop"),
      syncFullHistory:false, connectTimeoutMs:30000, defaultQueryTimeoutMs:20000
    })

    const outbox=[]; let sending=false
    const SAFE_SEND = (jid, content)=> new Promise((resolve,reject)=>{ outbox.push({jid,content,resolve,reject}); processOutbox().catch(console.error) })
    async function processOutbox(){
      if (sending) return; sending=true
      while(outbox.length){
        const {jid,content,resolve,reject} = outbox.shift()
        let guard=0; while(!isOpen && guard<60){ await wait(1000); guard++ }
        if (!isOpen){ reject(new Error("WA not connected")); continue }
        let ok=false, err=null
        for (let a=1;a<=4;a++){
          try { await sock.sendMessage(jid, content); ok=true; break }
          catch(e){ err=e; if(/Timed Out|Boom/.test(e?.message||"")){ await wait(400*a); continue } await wait(300) }
        }
        ok ? resolve(true) : reject(err)
      }
      sending=false
    }

    sock.ev.on("connection.update", async({connection,lastDisconnect,qr})=>{
      if (qr){ lastQR=qr; conectado=false; try{ qrcodeTerminal.generate(qr,{small:true}); console.log("📱 Escanea el QR") }catch{} }
      if (connection==="open"){ lastQR=null; conectado=true; isOpen=true; console.log("✅ Conectado a WhatsApp"); processOutbox().catch(console.error) }
      if (connection==="close"){
        conectado=false; isOpen=false
        console.log("❌ Conexión cerrada:", lastDisconnect?.error?.message||String(lastDisconnect?.error||""))
        if(!reconnecting){ reconnecting=true; await wait(1500); try{ await startBot() }catch(e){ console.error("Restart:",e) } finally{ reconnecting=false } }
      }
    })
    sock.ev.on("creds.update", saveCreds)

    sock.ev.on("messages.upsert", async({messages})=>{
      try{
        const m = messages?.[0]; if(!m?.message || m.key.fromMe) return
        const from = m.key.remoteJid
        const phone = normalizePhoneES((from||"").split("@")[0]||"") || (from||"").split("@")[0] || ""
        const body = m.message.conversation || m.message.extendedTextMessage?.text || m.message?.imageMessage?.caption || ""
        const text = (body||"").trim()
        if (!text) return

        console.log(`📱 ${phone}: ${text}`)
        let s = loadSession(phone) || {
          intent:null, startEU:null, staffId:null, name:null, email:null,
          confirmAsked:false, confirmApproved:false, customerCandidates:null,
          proposed:null, bookingInFlight:false
        }

        // ── NLU
        const nlu = await nluExtract(text)
        if (!s.intent) s.intent = nlu.intent
        const dt = nlu.datetime_text || null
        if (dt){ const parsed = parseDateTimeES(dt); if (parsed) s.startEU = parsed }
        const pref = detectPreferredStaff(text) || (nlu.staff_hint ? STAFF_NAME_MAP[nlu.staff_hint.toLowerCase()] : null)
        if (pref) s.staffId = pref
        if (!s.name && nlu.name) s.name = nlu.name
        if (!s.email && nlu.email) s.email = nlu.email
        if (YES_RE.test(text) || nlu.confirm==="yes") s.confirmApproved = true
        if (NO_RE .test(text) || nlu.confirm==="no")  { s.confirmApproved=false; s.confirmAsked=false }

        // Servicio único → siempre SERVICE_NAME si intención es reservar/cambiar
        const service = detectService(text) || SERVICE_NAME

        // Cliente por teléfono
        const e164 = normalizePhoneES(phone)
        const candidates = await squareFindCustomerByPhone(e164)
        s.customerCandidates = candidates.map(c=>({id:c.id, name:c.givenName||"", email:c.emailAddress||""}))

        // Desambiguación / creación
        if (s.customerCandidates.length!==1) {
          const needData = (s.customerCandidates.length===0)
          const resp = await aiReply({step:"customer_lookup", candidates:s.customerCandidates, need_data:needData}, ["ASK_MISSING"])
          await SAFE_SEND(from, { text: resp.reply || (needData ? "Dime tu nombre y email para crear tu ficha." : "Tengo varias fichas con tu teléfono. ¿Cuál es tu nombre y email?") })
          s.intent = s.intent || "create"
          saveSession(phone, s)
          return
        }
        const customer = s.customerCandidates[0]

        // Normaliza intención
        if (!s.intent) s.intent = "create"

        // ── CANCEL
        if (s.intent==="cancel") {
          const nowIso = dayjs().tz("UTC").toISOString()
          const local = db.prepare(`SELECT * FROM appointments WHERE customer_phone=@p AND start_iso>=@n AND status IN ('pending','confirmed') ORDER BY start_iso ASC LIMIT 1`)
            .get({p:e164, n:nowIso})
          if (!local || !local.square_booking_id) {
            const r = await aiReply({step:"cancel", result:"no_active"}, ["ASK_MISSING"])
            await SAFE_SEND(from, { text: r.reply || "No veo citas futuras a tu nombre." })
            clearSession.run({ phone }); return
          }
          const ok = await squareCancelBooking(local.square_booking_id)
          if (ok) {
            updateAppt.run({ id: local.id, status:"cancelled", square_booking_id: local.square_booking_id })
            await SAFE_SEND(from, { text: `✅ He cancelado tu cita del ${fmtES(dayjs(local.start_iso))}.` })
          } else {
            await SAFE_SEND(from, { text: "No he podido cancelar ahora. Lo intento en un rato." })
          }
          clearSession.run({ phone }); return
        }

        // ── RESCHEDULE
        if (s.intent==="reschedule") {
          if (!s.startEU) {
            const r = await aiReply({step:"reschedule", missing:["día y hora"]}, ["ASK_MISSING"])
            await SAFE_SEND(from, { text: r.reply || "¿Para cuándo quieres mover la cita?" })
            saveSession(phone,s); return
          }
          const nowIso = dayjs().tz("UTC").toISOString()
          const current = db.prepare(`SELECT * FROM appointments WHERE customer_phone=@p AND start_iso>=@n AND status IN ('pending','confirmed') ORDER BY start_iso ASC LIMIT 1`)
            .get({p:e164, n:nowIso})
          if (!current || !current.square_booking_id) {
            const r = await aiReply({step:"reschedule", result:"no_current"}, ["ASK_MISSING"])
            await SAFE_SEND(from, { text: r.reply || "No tengo una cita futura tuya. Si quieres, la creo nueva: dime día y hora." })
            s.intent="create"; saveSession(phone,s); return
          }
          const prefStaff = s.staffId || current.staff_id || null
          const found = searchSlot(s.startEU, prefStaff)
          if (found.type==="none") {
            const r = await aiReply({step:"reschedule", result:"no_slots"}, ["ASK_MISSING"])
            await SAFE_SEND(from, { text: r.reply || "No veo huecos cercanos. Dime otra hora o día y te digo." })
            saveSession(phone,s); return
          }
          if ((found.type==="earlier" || found.type==="nearest") && !s.confirmApproved) {
            const r = await aiReply({step:"reschedule", suggest:found.type, when:fmtES(found.when)}, ["ASK_MISSING"])
            await SAFE_SEND(from, { text: r.reply || `Puedo ofrecerte ${found.type==="earlier"?"antes":"el hueco más próximo"}: ${fmtES(found.when)}. ¿Confirmo?` })
            s.proposed = found; s.confirmAsked=true; saveSession(phone,s); return
          }
          // mover: cancelar actual + crear nuevo
          await squareCancelBooking(current.square_booking_id)
          updateAppt.run({ id: current.id, status:"cancelled", square_booking_id: current.square_booking_id })
          const sq = await squareCreateBooking({ startEU: (s.proposed?.when || found.when), customerId: customer.id, teamMemberId: (s.proposed?.staffId || found.staffId) })
          if (!sq) {
            await SAFE_SEND(from,{ text:"No he podido cerrar el cambio ahora. Lo intento de nuevo." })
            saveSession(phone,s); return
          }
          const newWhen = s.proposed?.when || found.when
          insertAppt.run({
            id: `apt_${Math.random().toString(36).slice(2,8)}${Date.now().toString(36).slice(-4)}`,
            customer_name: customer.name || s.name || "",
            customer_phone: e164,
            customer_square_id: customer.id,
            service: SERVICE_NAME,
            duration_min: SERVICE_MIN,
            start_iso: newWhen.tz("UTC").toISOString(),
            end_iso: newWhen.clone().add(SERVICE_MIN,"minute").tz("UTC").toISOString(),
            staff_id: s.proposed?.staffId || found.staffId,
            status: "confirmed",
            created_at: new Date().toISOString(),
            square_booking_id: sq.id
          })
          clearSession.run({ phone })
          await SAFE_SEND(from, { text: `✅ He movido tu cita a ${fmtES(newWhen)} para ${SERVICE_NAME}. ¡Hecho!` })
          return
        }

        // ── CREATE
        if (!s.startEU) {
          const r = await aiReply({step:"create", missing:["día y hora"]}, ["ASK_MISSING"])
          await SAFE_SEND(from, { text: r.reply || "¿Qué día y a qué hora te viene bien?" })
          saveSession(phone,s); return
        }
        if (!s.staffId) s.staffId = chooseStaffForDateEU(s.startEU)

        const found = searchSlot(s.startEU, s.staffId)
        if (found.type==="none") {
          const r = await aiReply({step:"create", result:"no_slots"}, ["ASK_MISSING"])
          await SAFE_SEND(from, { text: r.reply || "No veo huecos cercanos. Pásame otra hora o día y te miro." })
          saveSession(phone,s); return
        }
        if ((found.type==="earlier" || found.type==="nearest" || found.type==="exact") && !s.confirmApproved) {
          const key = found.type==="exact" ? "exact" : (found.type==="earlier" ? "earlier" : "nearest")
          const r = await aiReply({step:"create", slot:key, when:fmtES(found.when)}, ["ASK_MISSING"])
          const txt = found.type==="exact"
            ? `Tengo libre ${fmtES(found.when)}. ¿Confirmo la cita?`
            : (found.type==="earlier"
              ? `Te puedo dar antes: ${fmtES(found.when)}. ¿Confirmo?`
              : `No está libre esa hora; te propongo ${fmtES(found.when)}. ¿Confirmo?`)
          await SAFE_SEND(from, { text: r.reply || txt })
          s.proposed = found; s.confirmAsked=true; saveSession(phone,s); return
        }

        // Crear YA (aceptó propuesta o ya venía con confirmación)
        const chosen = (s.confirmApproved && s.proposed?.when) ? s.proposed : found
        const whenEU = chosen.when
        const staffId = chosen.staffId

        // Candado local (anti-doble)
        const startUTC = whenEU.tz("UTC"), endUTC = whenEU.clone().add(SERVICE_MIN,"minute").tz("UTC")
        try {
          insertAppt.run({
            id: `apt_${Math.random().toString(36).slice(2,8)}${Date.now().toString(36).slice(-4)}`,
            customer_name: customer.name || s.name || "",
            customer_phone: e164,
            customer_square_id: customer.id,
            service: SERVICE_NAME,
            duration_min: SERVICE_MIN,
            start_iso: startUTC.toISOString(),
            end_iso: endUTC.toISOString(),
            staff_id: staffId,
            status: "pending",
            created_at: new Date().toISOString(),
            square_booking_id: null
          })
        } catch(e) {
          if (String(e?.message||"").includes("UNIQUE")) {
            await SAFE_SEND(from, { text: "Ese hueco se acaba de ocupar. Dime otra hora y te digo la siguiente libre." })
            saveSession(phone,s); return
          }
          console.error("insertAppt:", e?.message||e)
        }

        // Square booking
        const sq = await squareCreateBooking({ startEU: whenEU, customerId: customer.id, teamMemberId: staffId })
        if (!sq) {
          // Confirmamos local y avisamos admin (no hay variación o error)
          db.prepare(`UPDATE appointments SET status='confirmed' WHERE customer_phone=@p AND start_iso=@s AND status='pending'`)
            .run({ p: e164, s: startUTC.toISOString() })
          await SAFE_SEND(from, { text:
`✅ Reserva confirmada

📅 ${fmtES(whenEU)}
💅 ${SERVICE_NAME}
⏱️ ${SERVICE_MIN} min
📍 Gapink Nails — Pago en persona

*(Nota interna: configura SQ_SV_UNAS_ACRILICAS con la variación de Square para registrar automáticamente)*`
          })
          clearSession.run({ phone })
          return
        }

        // Confirmar local con booking id
        db.prepare(`UPDATE appointments SET status='confirmed', square_booking_id=@b WHERE customer_phone=@p AND start_iso=@s AND status='pending'`)
          .run({ b: sq.id, p: e164, s: startUTC.toISOString() })

        clearSession.run({ phone })
        await SAFE_SEND(from, { text:
`✅ Reserva confirmada

📅 ${fmtES(whenEU)}
💅 ${SERVICE_NAME}
⏱️ ${SERVICE_MIN} min
📍 Gapink Nails — Pago en persona

¡Te esperamos!`
        })

      } catch(e){ console.error("messages.upsert:", e) }
    })
  } catch(e) {
    console.error("startBot:", e)
    setTimeout(()=>startBot().catch(console.error), 1500)
  }
}

// ───────────────── Boot
const app = express()
const PORT = process.env.PORT || 8080

app.listen(PORT, async()=>{
  console.log(`🌐 Web servidor iniciado en puerto ${PORT}`)
  const missing = ['DEEPSEEK_API_KEY','SQUARE_ACCESS_TOKEN','SQUARE_LOCATION_ID'].filter(k=>!process.env[k])
  if (missing.length) { console.error("⛔ Faltan env:", missing.join(", ")); process.exit(1) }
  await squareInit()
  startBot().catch(console.error)
})

// ───────────────── Señales
process.on("uncaughtException", e=>console.error("Uncaught:",e))
process.on("unhandledRejection", (r,p)=>console.error("Unhandled:",r))
process.on("SIGTERM", ()=>{ console.log("SIGTERM, bye"); db.close(); process.exit(0) })
process.on("SIGINT",  ()=>{ console.log("SIGINT, bye");  db.close(); process.exit(0) })
