// index.js — Gapink Nails WhatsApp Bot
// OpenAI gpt-4o-mini + extracción JSON + TZ Europe/Madrid
// Silencioso ante errores (no enviar mensajes de error al cliente)
// Confirmación SOLO si el mensaje ACTUAL contiene “sí/confirmo/ok/vale”
// Persistencia de hora en ms (sin UTC shift) + validaciones
// Disponibilidad forward-first + cancelar/editar reales en Square
// FIXES:
//  - Acepta SQ_FORCE_SERVICE_ID como fallback de servicio
//  - Autodescubre team_member_id si no se define SQ_TEAM_IDS
//  - Logs detallados de errores Square (para depurar por qué no “entra”)
//  - Aviso: en SANDBOX no hay SMS reales; en producción activa Communications

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
import { Client, Environment, ApiError } from "square"

if (!globalThis.crypto) globalThis.crypto = webcrypto
dayjs.extend(utc); dayjs.extend(tz); dayjs.locale("es")
const EURO_TZ = "Europe/Madrid"

const { makeWASocket, useMultiFileAuthState, fetchLatestBaileysVersion, Browsers } = baileys

// ===== Negocio
const WORK_DAYS = [1,2,3,4,5,6]          // L-S (domingo cerrado)
const OPEN_HOUR  = 10
const CLOSE_HOUR = 20
const SLOT_MIN   = 30
const SERVICES = { "uñas acrílicas": 90 }
// Acepta dos nombres de variable para el mismo servicio (evita “no entra a Square” por ID vacío)
const SERVICE_VARIATIONS = {
  "uñas acrílicas": process.env.SQ_SV_UNAS_ACRILICAS || process.env.SQ_FORCE_SERVICE_ID || ""
}
// TEAM MEMBERS (si vienen vacíos, los descubrimos vía API)
let TEAM_MEMBER_IDS = (process.env.SQ_TEAM_IDS || "").split(",").map(s=>s.trim()).filter(Boolean)

// ===== OpenAI
const OPENAI_API_KEY  = process.env.OPENAI_API_KEY
const OPENAI_API_URL  = process.env.OPENAI_API_URL || "https://api.openai.com/v1/chat/completions"
const OPENAI_MODEL    = process.env.OPENAI_MODEL || "gpt-4o-mini"

async function aiChat(messages, { temperature=0.4 } = {}) {
  if (!OPENAI_API_KEY) return ""
  try {
    const r = await fetch(OPENAI_API_URL, {
      method: "POST",
      headers: { "Content-Type":"application/json", "Authorization":`Bearer ${OPENAI_API_KEY}` },
      body: JSON.stringify({ model: OPENAI_MODEL, messages, temperature })
    })
    if (!r.ok) throw new Error(`OpenAI ${r.status}`)
    const j = await r.json()
    return (j?.choices?.[0]?.message?.content || "").trim()
  } catch (e) { console.error("OpenAI error:", e?.message || e); return "" }
}

const SYS_TONE = `Eres el asistente de WhatsApp de Gapink Nails (España).
Habla natural, breve y sin emojis. No digas que eres IA.
Si la hora pedida está libre, ofrécela tal cual; si no, propone la más cercana y pide confirmación.
No ofrezcas elegir profesional. Pago siempre en persona.`

async function extractFromText(userText="") {
  const schema = `
Devuelve SOLO un JSON válido (omite claves que no apliquen):
{
  "intent": "greeting|booking|cancel|reschedule|other",
  "service": "uñas acrílicas|…",
  "datetime_text": "texto con fecha/hora si lo hay",
  "confirm": "yes|no|unknown",
  "name": "si aparece",
  "email": "si aparece",
  "polite_reply": "respuesta breve y natural para avanzar"
}`
  const content = await aiChat([
    { role: "system", content: `${SYS_TONE}\n${schema}\nUsa español de España.` },
    { role: "user", content: userText }
  ], { temperature: 0.2 })
  try {
    const jsonStr = content.trim().replace(/^```(json)?/i,"").replace(/```$/,"")
    return JSON.parse(jsonStr)
  } catch { return { intent:"other", polite_reply:"" } }
}
async function aiSay(contextSummary) {
  return await aiChat([
    { role:"system", content: SYS_TONE },
    { role:"user", content: contextSummary }
  ], { temperature: 0.35 })
}

// ===== Helpers
const onlyDigits = (s="") => (s||"").replace(/\D+/g,"")
const rmDiacritics = (s="") => s.normalize("NFD").replace(/\p{Diacritic}/gu,"")
const YES_RE = /\b(si|sí|ok|vale|confirmo|confirmar|de acuerdo|perfecto)\b/i
const NO_RE  = /\b(no|otra|cambia|no confirmo|mejor mas tarde|mejor más tarde|anula|cancela)\b/i
const RESCH_RE = /\b(cambia|cambiar|modifica|mover|reprograma|reprogramar|edita|mejor)\b/i
function normalizePhoneES(raw){const d=onlyDigits(raw);if(!d)return null;if(raw.startsWith("+")&&d.length>=8&&d.length<=15)return`+${d}`;if(d.startsWith("34")&&d.length===11)return`+${d}`;if(d.length===9)return`+34${d}`;if(d.startsWith("00"))return`+${d.slice(2)}`;return`+${d}`}
function detectServiceFree(text=""){const low=rmDiacritics(text.toLowerCase());const map={"unas acrilicas":"uñas acrílicas","uñas acrilicas":"uñas acrílicas","uñas acrílicas":"uñas acrílicas"};for(const k of Object.keys(map))if(low.includes(rmDiacritics(k)))return map[k];for(const k of Object.keys(SERVICES))if(low.includes(rmDiacritics(k)))return k;return null}
function parseDateTimeES(dtText){if(!dtText)return null;const t=rmDiacritics(dtText.toLowerCase());let base=null;if(/\bhoy\b/.test(t))base=dayjs().tz(EURO_TZ);else if(/\bmanana\b/.test(t))base=dayjs().tz(EURO_TZ).add(1,"day");if(!base){const M={enero:1,febrero:2,marzo:3,abril:4,mayo:5,junio:6,julio:7,agosto:8,septiembre:9,setiembre:9,octubre:10,noviembre:11,diciembre:12,ene:1,feb:2,mar:3,abr:4,may:5,jun:6,jul:7,ago:8,sep:9,oct:10,nov:11,dic:12};const m=t.match(/\b(\d{1,2})\s+de\s+(enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|setiembre|octubre|noviembre|diciembre|ene|feb|mar|abr|may|jun|jul|ago|sep|oct|nov|dic)\b(?:\s+de\s+(\d{4}))?/);if(m){const dd=+m[1],mm=M[m[2]],yy=m[3]?+m[3]:dayjs().tz(EURO_TZ).year();base=dayjs.tz(`${yy}-${String(mm).padStart(2,"0")}-${String(dd).padStart(2,"0")} 00:00`,EURO_TZ)}}if(!base){const m=t.match(/\b(\d{1,2})[\/\-](\d{1,2})(?:[\/\-](\d{2,4}))?\b/);if(m){let yy=m[3]?+m[3]:dayjs().tz(EURO_TZ).year();if(yy<100)yy+=2000;base=dayjs.tz(`${yy}-${String(+m[2]).padStart(2,"0")}-${String(+m[1]).padStart(2,"0")} 00:00`,EURO_TZ)}}if(!base)base=dayjs().tz(EURO_TZ);let hour=null,minute=0;const hm=t.match(/(\d{1,2})(?::|h)?(\d{2})?\s*(am|pm)?\b/);if(hm){hour=+hm[1];minute=hm[2]?+hm[2]:0;const ap=hm[3];if(ap==="pm"&&hour<12)hour+=12;if(ap==="am"&&hour===12)hour=0}if(hour===null)return null;return base.hour(hour).minute(minute).second(0).millisecond(0)}
const fmtES=(d)=>{const t=(dayjs.isDayjs(d)?d:dayjs(d)).tz(EURO_TZ);const dias=["domingo","lunes","martes","miércoles","jueves","viernes","sábado"];const DD=String(t.date()).padStart(2,"0"),MM=String(t.month()+1).padStart(2,"0"),HH=String(t.hour()).padStart(2,"0"),mm=String(t.minute()).padStart(2,"0");return `${dias[t.day()]} ${DD}/${MM} ${HH}:${mm}`}
// Redondeo a slot (ceil)
function ceilToSlotEU(t){const m=t.minute();const rem=m%SLOT_MIN;if(rem===0)return t.second(0).millisecond(0);return t.add(SLOT_MIN-rem,"minute").second(0).millisecond(0)}

// ===== Square
const square = new Client({ accessToken: process.env.SQUARE_ACCESS_TOKEN, environment: process.env.SQUARE_ENV==="production"?Environment.Production:Environment.Sandbox })
const locationId = process.env.SQUARE_LOCATION_ID
let LOCATION_TZ = EURO_TZ

function prettyApiError(e){
  if (e instanceof ApiError) {
    const code = e.statusCode
    const errs = e.result?.errors || []
    return `Square ${code}: ${errs.map(x=>`${x.category||''}/${x.code||''} ${x.detail||''}`.trim()).join(" | ")}`
  }
  return e?.message || String(e)
}

async function squareCheckCredentials(){
  try{
    const locs=await square.locationsApi.listLocations()
    const loc=(locs.result.locations||[]).find(l=>l.id===locationId)||(locs.result.locations||[])[0]
    if(loc?.timezone) LOCATION_TZ=loc.timezone
    console.log(`✅ Square listo. Location ${locationId||loc?.id}, TZ=${LOCATION_TZ}`)
    // Validación servicio:
    const sv = SERVICE_VARIATIONS["uñas acrílicas"]
    if(!sv){ console.error("⛔ Falta ID de servicio. Define SQ_SV_UNAS_ACRILICAS o SQ_FORCE_SERVICE_ID"); }
    // Autodescubre team members si no hay env:
    if (!TEAM_MEMBER_IDS.length) {
      const ids = await discoverTeamMembers()
      TEAM_MEMBER_IDS = ids
      console.log(`👥 TEAM_MEMBER_IDS autodescubiertos: ${TEAM_MEMBER_IDS.join(", ")}`)
    } else {
      console.log(`👥 TEAM_MEMBER_IDS (env): ${TEAM_MEMBER_IDS.join(", ")}`)
    }
  }catch(e){
    console.error("⛔ Square init:", prettyApiError(e))
  }
}

async function discoverTeamMembers() {
  try{
    // bookable_only + location para asegurarnos que sirven para reservas
    const r = await square.bookingsApi.listTeamMemberBookingProfiles({
      bookableOnly: true,
      locationId: locationId
    })
    const list = r?.result?.teamMemberBookingProfiles || []
    const ids = list.filter(x=>x?.isBookable!==false && x?.teamMemberId).map(x=>x.teamMemberId)
    if (!ids.length) console.error("⚠️ No hay team members bookables en esta location.")
    return ids
  }catch(e){
    console.error("listTeamMemberBookingProfiles:", prettyApiError(e))
    return []
  }
}

async function squareFindCustomerByPhone(phoneRaw){
  try{
    const e164=normalizePhoneES(phoneRaw)
    if(!e164||!e164.startsWith("+")||e164.length<8||e164.length>16) return null
    const resp=await square.customersApi.searchCustomers({query:{filter:{phoneNumber:{exact:e164}}}})
    return (resp?.result?.customers||[])[0]||null
  }catch(e){ console.error("Square search:", prettyApiError(e)); return null }
}
async function squareCreateCustomer({givenName,emailAddress,phoneNumber}){
  try{
    const phone=normalizePhoneES(phoneNumber)
    const resp=await square.customersApi.createCustomer({
      idempotencyKey:`cust_${Date.now()}_${Math.random().toString(36).slice(2,8)}`,
      givenName,emailAddress,phoneNumber:phone||undefined,
      note:"Creado desde bot WhatsApp Gapink Nails"
    })
    return resp?.result?.customer||null
  }catch(e){ console.error("Square create:", prettyApiError(e)); return null }
}
async function getServiceVariationVersion(id){
  try{
    const resp=await square.catalogApi.retrieveCatalogObject(id,true)
    return resp?.result?.object?.version
  }catch(e){ console.error("getServiceVariationVersion:", prettyApiError(e)); return undefined }
}
function stableKey({locationId,serviceVariationId,startISO,customerId,teamMemberId}){
  const raw=`${locationId}|${serviceVariationId}|${startISO}|${customerId}|${teamMemberId}`
  return createHash("sha256").update(raw).digest("hex").slice(0,48)
}
async function createSquareBooking({startEU,serviceKey,customerId,teamMemberId}){
  try{
    const serviceVariationId=SERVICE_VARIATIONS[serviceKey]
    if(!serviceVariationId){ console.error("createSquareBooking: falta serviceVariationId"); return null }
    if(!teamMemberId){ console.error("createSquareBooking: falta teamMemberId"); return null }
    if(!locationId){ console.error("createSquareBooking: falta locationId"); return null }

    const version=await getServiceVariationVersion(serviceVariationId)
    if(!version){ console.error("createSquareBooking: no pude obtener serviceVariationVersion"); return null }

    const startISO=startEU.tz("UTC").toISOString()
    const body={
      idempotencyKey:stableKey({locationId,serviceVariationId,startISO,customerId,teamMemberId}),
      booking:{
        locationId,
        startAt:startISO,
        customerId,
        // Si quieres mostrar una nota en Square:
        // customerNote: "Reserva creada por WhatsApp Bot",
        appointmentSegments:[{
          teamMemberId,
          serviceVariationId,
          serviceVariationVersion:Number(version),
          durationMinutes:SERVICES[serviceKey] // seller-level: RW
        }]
      }
    }
    const resp=await square.bookingsApi.createBooking(body)
    return resp?.result?.booking||null
  }catch(e){
    console.error("createSquareBooking:", prettyApiError(e))
    return null
  }
}
async function cancelSquareBooking(bookingId){
  try{
    const r=await square.bookingsApi.cancelBooking(bookingId)
    return !!r?.result?.booking?.id
  }catch(e){ console.error("cancelSquareBooking:", prettyApiError(e)); return false }
}
async function updateSquareBooking(bookingId,{startEU,serviceKey,customerId,teamMemberId}){
  try{
    const get=await square.bookingsApi.retrieveBooking(bookingId)
    const booking=get?.result?.booking
    if(!booking) return null
    const serviceVariationId=SERVICE_VARIATIONS[serviceKey]
    const version=await getServiceVariationVersion(serviceVariationId)
    const startISO=startEU.tz("UTC").toISOString()
    const body={
      idempotencyKey:stableKey({locationId,serviceVariationId,startISO,customerId,teamMemberId}),
      booking:{
        id:bookingId, version:booking.version,
        locationId, customerId, startAt:startISO,
        appointmentSegments:[{
          teamMemberId, serviceVariationId,
          serviceVariationVersion:Number(version),
          durationMinutes:SERVICES[serviceKey]
        }]
      }
    }
    const resp=await square.bookingsApi.updateBooking(bookingId, body)
    return resp?.result?.booking||null
  }catch(e){ console.error("updateSquareBooking:", prettyApiError(e)); return null }
}

// ===== DB & sesiones
const db=new Database("gapink.db");db.pragma("journal_mode = WAL")
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
  status TEXT,
  created_at TEXT,
  square_booking_id TEXT
);
CREATE TABLE IF NOT EXISTS sessions (
  phone TEXT PRIMARY KEY,
  data_json TEXT,
  updated_at TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_slot
ON appointments(staff_id, start_iso)
WHERE status IN ('pending','confirmed');
`)
const insertAppt=db.prepare(`INSERT INTO appointments
(id, customer_name, customer_phone, customer_square_id, service, duration_min, start_iso, end_iso, staff_id, status, created_at, square_booking_id)
VALUES (@id, @customer_name, @customer_phone, @customer_square_id, @service, @duration_min, @start_iso, @end_iso, @staff_id, @status, @created_at, @square_booking_id)`)
const updateAppt=db.prepare(`UPDATE appointments SET status=@status, square_booking_id=@square_booking_id WHERE id=@id`)
const updateApptTimes=db.prepare(`UPDATE appointments SET start_iso=@start_iso, end_iso=@end_iso, staff_id=@staff_id WHERE id=@id`)
const markCancelled=db.prepare(`UPDATE appointments SET status='cancelled' WHERE id=@id`)
const deleteAppt=db.prepare(`DELETE FROM appointments WHERE id=@id`)
const getSessionRow=db.prepare(`SELECT * FROM sessions WHERE phone=@phone`)
const upsertSession=db.prepare(`INSERT INTO sessions (phone, data_json, updated_at)
VALUES (@phone, @data_json, @updated_at)
ON CONFLICT(phone) DO UPDATE SET data_json=excluded.data_json, updated_at=excluded.updated_at`)
const clearSession=db.prepare(`DELETE FROM sessions WHERE phone=@phone`)
const getUpcomingByPhone=db.prepare(`SELECT * FROM appointments WHERE customer_phone=@phone AND status='confirmed' AND start_iso > @now ORDER BY start_iso ASC LIMIT 1`)

// Persistimos MILLISECONDS locales para evitar desplazamientos TZ
function loadSession(phone){
  const row=getSessionRow.get({phone}); if(!row?.data_json) return null
  const raw=JSON.parse(row.data_json); const data={...raw}
  if (raw.startEU_ms) data.startEU = dayjs.tz(raw.startEU_ms, EURO_TZ)
  return data
}
function saveSession(phone,data){
  const s={...data}; s.startEU_ms = data.startEU?.valueOf?.() ?? data.startEU_ms ?? null; delete s.startEU
  upsertSession.run({phone, data_json:JSON.stringify(s), updated_at:new Date().toISOString()})
}

// ===== Disponibilidad (usa solo DB local; Square puede rechazar por solape si hay citas externas)
function getBookedIntervals(fromIso,toIso){
  const rows=db.prepare(`SELECT start_iso,end_iso,staff_id FROM appointments WHERE status IN ('pending','confirmed') AND start_iso < @to AND end_iso > @from`).all({from:fromIso,to:toIso})
  return rows.map(r=>({start:dayjs(r.start_iso),end:dayjs(r.end_iso),staff_id:r.staff_id}))
}
function findFreeStaff(intervals,start,end,preferred){
  const base=TEAM_MEMBER_IDS.length?TEAM_MEMBER_IDS:["__MISSING__"]
  const ids = preferred && base.includes(preferred) ? [preferred, ...base.filter(x=>x!==preferred)] : base
  for(const id of ids){
    const busy = intervals.filter(i=>i.staff_id===id).some(i => (start<i.end) && (i.start<end))
    if(!busy) return id
  }
  return null
}
function suggestOrExact(startEU,durationMin,preferredStaffId=null){
  const now=dayjs().tz(EURO_TZ).add(30,"minute").second(0).millisecond(0)
  const from=now.tz("UTC").toISOString(), to=now.add(14,"day").tz("UTC").toISOString()
  const intervals=getBookedIntervals(from,to)
  const dayStart=startEU.clone().hour(OPEN_HOUR).minute(0).second(0), dayEnd=startEU.clone().hour(CLOSE_HOUR).minute(0).second(0)
  let req=startEU.clone(); if(req.isBefore(dayStart)) req=dayStart; if(req.isAfter(dayEnd)) req=dayEnd
  const dow=req.day()===0?7:req.day(); const endEU=req.clone().add(durationMin,"minute")
  const insideHours=req.hour()>=OPEN_HOUR && (endEU.hour()<CLOSE_HOUR || (endEU.hour()===CLOSE_HOUR && endEU.minute()===0))
  if (dow===7||!WORK_DAYS.includes(dow)||!insideHours) return { exact:null, suggestion:null, staffId:null }
  const scanStart = ceilToSlotEU(req.isBefore(now)? now.clone(): req.clone())
  const exactId = findFreeStaff(intervals, scanStart.tz("UTC"), scanStart.clone().add(durationMin,"minute").tz("UTC"), preferredStaffId)
  if (exactId && scanStart.valueOf()===startEU.valueOf()) return { exact: scanStart, suggestion:null, staffId: exactId }
  for (let t=scanStart.clone(); !t.isAfter(dayEnd); t=t.add(SLOT_MIN,"minute")) {
    const e=t.clone().add(durationMin,"minute"); if(t.isBefore(now)) continue
    if (t.isAfter(dayEnd) || e.isAfter(dayEnd)) break
    const id=findFreeStaff(intervals,t.tz("UTC"),e.tz("UTC"),preferredStaffId); if(id) return { exact:null, suggestion:t, staffId:id }
  }
  for (let t=ceilToSlotEU(startEU.clone()).subtract(SLOT_MIN,"minute"); !t.isBefore(dayStart); t=t.subtract(SLOT_MIN,"minute")) {
    const e=t.clone().add(durationMin,"minute"); const id=findFreeStaff(intervals,t.tz("UTC"),e.tz("UTC"),preferredStaffId)
    if(id) return { exact:null, suggestion:t, staffId:id }
  }
  return { exact:null, suggestion:null, staffId:null }
}

// ===== Mini web
const app=express()
const PORT=process.env.PORT||8080
let lastQR=null,conectado=false
app.get("/",(_req,res)=>{res.send(`<!doctype html><meta charset="utf-8"><style>body{font-family:system-ui;display:grid;place-items:center;min-height:100vh;background:linear-gradient(135deg,#fce4ec,#f8bbd0);color:#4a148c} .card{background:#fff;padding:24px;border-radius:16px;box-shadow:0 6px 24px rgba(0,0,0,.08);text-align:center;max-width:520px}</style><div class="card"><h1>Gapink Nails</h1><p>Estado: ${conectado?"✅ Conectado":"❌ Desconectado"}</p>${!conectado&&lastQR?`<img src="/qr.png" width="320" />`:``}<p><small>Desarrollado por Gonzalo</small></p><p style="font-size:12px;color:#6b7280">Recuerda: para SMS/emails al cliente, activa Appointments → Settings → Communications.</p></div>`)})
app.get("/qr.png",async(_req,res)=>{if(!lastQR)return res.status(404).send("No hay QR");const png=await qrcode.toBuffer(lastQR,{type:"png",width:512,margin:1});res.set("Content-Type","image/png").send(png)})

// ===== Cola envío Baileys
const wait=(ms)=>new Promise(r=>setTimeout(r,ms))
app.listen(PORT,async()=>{
  console.log(`🌐 Web en puerto ${PORT}`)
  await squareCheckCredentials()
  startBot().catch(console.error)
})

async function startBot(){
  console.log("🚀 Bot arrancando…")
  try{
    if(!fs.existsSync("auth_info"))fs.mkdirSync("auth_info",{recursive:true})
    const { state, saveCreds } = await useMultiFileAuthState("auth_info")
    const { version } = await fetchLatestBaileysVersion()
    let isOpen=false,reconnecting=false
    const sock=makeWASocket({logger:pino({level:"silent"}),printQRInTerminal:false,auth:state,version,browser:Browsers.macOS("Desktop"),syncFullHistory:false,connectTimeoutMs:30000})

    const outbox=[]; let sending=false
    const __SAFE_SEND__=(jid,content)=>new Promise((resolve,reject)=>{outbox.push({jid,content,resolve,reject});processOutbox().catch(console.error)})
    async function processOutbox(){if(sending)return;sending=true;while(outbox.length){const {jid,content,resolve,reject}=outbox.shift();let guard=0;while(!isOpen&&guard<60){await wait(1000);guard++}if(!isOpen){reject(new Error("WA not connected"));continue}let ok=false,err=null;for(let a=1;a<=4;a++){try{await sock.sendMessage(jid,content);ok=true;break}catch(e){err=e;const msg=e?.data?.stack||e?.message||String(e);if(/Timed Out/i.test(msg)||/Boom/i.test(msg)){await wait(500*a);continue}await wait(400)}}if(ok)resolve(true);else{console.error("sendMessage failed:",err?.message||err);reject(err);try{await sock.ws.close()}catch{}}}sending=false}

    sock.ev.on("connection.update",async({connection,lastDisconnect,qr})=>{
      if(qr){lastQR=qr;conectado=false;try{qrcodeTerminal.generate(qr,{small:true})}catch{}}
      if(connection==="open"){lastQR=null;conectado=true;isOpen=true;console.log("✅ Conectado a WhatsApp");processOutbox().catch(console.error)}
      if(connection==="close"){conectado=false;isOpen=false;const reason=lastDisconnect?.error?.message||String(lastDisconnect?.error||"");console.log("❌ Conexión cerrada:",reason);if(!reconnecting){reconnecting=true;await wait(2000);try{await startBot()}finally{reconnecting=false}}}
    })
    sock.ev.on("creds.update",saveCreds)

    // ===== Mensajes
    sock.ev.on("messages.upsert",async({messages})=>{
      try{
        const m=messages?.[0]; if (!m?.message || m.key.fromMe) return
        const from=m.key.remoteJid
        const phone=normalizePhoneES((from||"").split("@")[0]||"")||(from||"").split("@")[0]||""
        const body=m.message.conversation||m.message.extendedTextMessage?.text||m.message?.imageMessage?.caption||""
        const textRaw=(body||"").trim()

        // Sesión
        let data=loadSession(phone)||{
          service:null,startEU:null,durationMin:null,name:null,email:null,
          confirmApproved:false,confirmAsked:false,bookingInFlight:false,
          lastUserDtText:null,lastService:null, selectedStaffId:null,
          editBookingId:null
        }

        // IA: extracción (confirmación NO se fía de la IA)
        const extra=await extractFromText(textRaw)
        const incomingService = extra.service || detectServiceFree(textRaw)
        const incomingDt = extra.datetime_text || null

        // Cancelación explícita
        if (extra.intent==="cancel" || /\b(cancel|anul|borra|elimina)r?\b/i.test(textRaw)) {
          const upc = getUpcomingByPhone.get({ phone, now: dayjs().utc().toISOString() })
          if (upc) {
            const ok = upc.square_booking_id ? await cancelSquareBooking(upc.square_booking_id) : true
            if (ok) { markCancelled.run({ id: upc.id }); clearSession.run({ phone }); await __SAFE_SEND__(from,{ text:`He cancelado tu cita del ${fmtES(dayjs(upc.start_iso))}.` }) }
          } else {
            await __SAFE_SEND__(from,{ text:"No veo ninguna cita futura tuya. Si quieres, dime fecha y hora y te doy hueco." })
          }
          return
        }

        // ¿Pide reprogramar?
        if (extra.intent==="reschedule" || RESCH_RE.test(textRaw)) {
          const upc = getUpcomingByPhone.get({ phone, now: dayjs().utc().toISOString() })
          if (upc) { data.editBookingId = upc.id; data.service = upc.service; data.durationMin = upc.duration_min; data.selectedStaffId = upc.staff_id }
        }

        // Reset confirm si cambió servicio/fecha
        if ((incomingService && incomingService !== data.lastService) || (incomingDt && incomingDt !== data.lastUserDtText)) {
          data.confirmApproved = false; data.confirmAsked = false
        }
        if (!data.service) data.service = incomingService || data.service
        data.lastService = data.service || data.lastService

        if (!data.name && extra.name) data.name = extra.name
        if (!data.email && extra.email) data.email = extra.email

        // Confirmación basada SOLO en el mensaje actual
        const userSaysYes = YES_RE.test(textRaw)
        const userSaysNo  = NO_RE.test(textRaw)
        if (userSaysYes) data.confirmApproved = true
        if (userSaysNo)  { data.confirmApproved=false; data.confirmAsked=false }

        // Fecha/hora
        if (incomingDt) data.lastUserDtText = incomingDt
        const parsed = parseDateTimeES(incomingDt ? incomingDt : textRaw)
        if (parsed) data.startEU = parsed

        if (data.service && !data.durationMin) data.durationMin = SERVICES[data.service] || 60
        saveSession(phone, data)

        // Cierre inmediato SOLO si el mensaje actual es afirmativo
        if (data.confirmAsked && userSaysYes && data.service && data.startEU && data.durationMin) {
          if (data.editBookingId) await finalizeReschedule({ from, phone, data, safeSend: __SAFE_SEND__ })
          else await finalizeBooking({ from, phone, data, safeSend: __SAFE_SEND__ })
          return
        }

        // DISPONIBILIDAD
        if (data.service && data.startEU && data.durationMin) {
          const preferred = data.editBookingId ? data.selectedStaffId : null
          const { exact, suggestion, staffId } = suggestOrExact(data.startEU, data.durationMin, preferred)
          if (exact) {
            data.startEU = exact; data.selectedStaffId = staffId
            if (userSaysYes) { saveSession(phone,data);
              if (data.editBookingId) await finalizeReschedule({ from, phone, data, safeSend: __SAFE_SEND__ })
              else await finalizeBooking({ from, phone, data, safeSend: __SAFE_SEND__ })
              return
            }
            data.confirmAsked = true; saveSession(phone,data)
            await __SAFE_SEND__(from,{ text:`Tengo libre ${fmtES(data.startEU)} para ${data.service}. ¿Confirmo la ${data.editBookingId?"modificación":"cita"}?` })
            return
          }
          if (suggestion) {
            data.startEU = suggestion; data.selectedStaffId = staffId
            data.confirmAsked = true; data.confirmApproved = false; saveSession(phone, data)
            await __SAFE_SEND__(from,{ text:`No tengo ese hueco exacto. Te puedo ofrecer ${fmtES(data.startEU)}. ¿Te viene bien? Si es sí, responde “confirmo”.` })
            return
          }
          data.confirmAsked = false; saveSession(phone,data)
          await __SAFE_SEND__(from,{ text:"No veo hueco en esa franja. Dime otra hora o día y te digo." })
          return
        }

        // Falta nombre/email tras “sí” (solo altas)
        if (userSaysYes && (!data.name || !data.email) && !data.editBookingId) {
          saveSession(phone, data)
          await __SAFE_SEND__(from,{ text:"Para cerrar, dime tu nombre y email (ej: “Ana Pérez, ana@correo.com”)." })
          return
        }
        if (userSaysYes && data.service && data.startEU) {
          if (data.editBookingId) await finalizeReschedule({ from, phone, data, safeSend: __SAFE_SEND__ })
          else if (data.name && data.email) await finalizeBooking({ from, phone, data, safeSend: __SAFE_SEND__ })
          return
        }

        // Mensaje de avance (fallback si falla OpenAI)
        const missing=[]
        if(!data.service) missing.push("servicio")
        if(!data.startEU) missing.push(data.editBookingId?"nueva fecha y hora":"día y hora")
        if(!data.editBookingId && (!data.name||!data.email)) missing.push("nombre y email (si eres nuevo)")
        const prompt=`Contexto:
- Modo: ${data.editBookingId?"edición":"alta"}
- Servicio: ${data.service||"?"}
- Fecha/Hora: ${data.startEU?fmtES(data.startEU):"?"}
- Nombre: ${data.name||"?"}
- Email: ${data.email||"?"}
Escribe un único mensaje corto y humano que avance la ${data.editBookingId?"modificación":"reserva"}, sin emojis.
Si faltan datos (${missing.join(", ")}), pídelo con ejemplo.
Mensaje del cliente: "${textRaw}"`
        let say=await aiSay(prompt)
        if (!say) say = missing.length
          ? `Necesito ${missing.join(", ")}. Ejemplo: "uñas acrílicas, martes 15 a las 11:00, Ana Pérez, ana@correo.com".`
          : "¿Qué día y a qué hora te viene bien?"
        saveSession(phone,data)
        await __SAFE_SEND__(from,{ text: say })
      }catch(e){ console.error("messages.upsert error:", e?.message||e) }
    })
  }catch(e){ console.error("startBot error:", e?.message||e) }
}

// ===== Alta (silenciosa ante errores)
async function finalizeBooking({ from, phone, data, safeSend }) {
  try {
    if (data.bookingInFlight) return
    data.bookingInFlight = true; saveSession(phone, data)

    let customer = await squareFindCustomerByPhone(phone)
    if (!customer) {
      if (!data.name || !data.email) { data.bookingInFlight=false; saveSession(phone,data); return }
      customer = await squareCreateCustomer({ givenName: data.name, emailAddress: data.email, phoneNumber: phone })
    }
    if (!customer) { data.bookingInFlight=false; saveSession(phone,data); return }

    const startEU = dayjs.isDayjs(data.startEU) ? data.startEU : (data.startEU_ms ? dayjs.tz(Number(data.startEU_ms), EURO_TZ) : null)
    if (!startEU || !startEU.isValid()) { data.bookingInFlight=false; saveSession(phone,data); return }

    // Asegurar team member válido:
    let teamMemberId = data.selectedStaffId || TEAM_MEMBER_IDS[0]
    if (!teamMemberId) {
      const found = await discoverTeamMembers()
      TEAM_MEMBER_IDS = found
      teamMemberId = TEAM_MEMBER_IDS[0]
    }
    if (!teamMemberId) { console.error("No hay teamMemberId disponible"); data.bookingInFlight=false; saveSession(phone,data); return }

    const durationMin = SERVICES[data.service]
    const startUTC = startEU.tz("UTC"), endUTC = startUTC.clone().add(durationMin,"minute")

    const aptId = `apt_${Math.random().toString(36).slice(2,8)}${Date.now().toString(36).slice(-4)}`
    try {
      insertAppt.run({
        id: aptId, customer_name: data.name || customer?.givenName || null, customer_phone: phone,
        customer_square_id: customer.id, service: data.service, duration_min: durationMin,
        start_iso: startUTC.toISOString(), end_iso: endUTC.toISOString(),
        staff_id: teamMemberId, status: "pending", created_at: new Date().toISOString(), square_booking_id: null
      })
    } catch (e) {
      if (String(e?.message||"").includes("UNIQUE")) { data.bookingInFlight=false; saveSession(phone,data); return }
      throw e
    }

    const sq = await createSquareBooking({ startEU, serviceKey: data.service, customerId: customer.id, teamMemberId })
    if (!sq) { deleteAppt.run({ id: aptId }); data.bookingInFlight=false; saveSession(phone,data); return }

    updateAppt.run({ id: aptId, status: "confirmed", square_booking_id: sq.id || null })
    clearSession.run({ phone })
    await safeSend(from,{ text:
`Reserva confirmada.
Servicio: ${data.service}
Fecha: ${fmtES(startEU)}
Duración: ${durationMin} min
Pago en persona.` })
  } catch (e) { console.error("finalizeBooking:", prettyApiError(e)) }
  finally { data.bookingInFlight=false; try{ saveSession(phone, data) }catch{} }
}

// ===== Edición (silenciosa ante errores)
async function finalizeReschedule({ from, phone, data, safeSend }) {
  try{
    if (data.bookingInFlight) return
    data.bookingInFlight = true; saveSession(phone, data)

    const upc = getUpcomingByPhone.get({ phone, now: dayjs().utc().toISOString() })
    if (!upc || upc.id !== data.editBookingId) { data.bookingInFlight=false; saveSession(phone,data); return }

    const startEU = dayjs.isDayjs(data.startEU) ? data.startEU : (data.startEU_ms ? dayjs.tz(Number(data.startEU_ms), EURO_TZ) : null)
    if (!startEU || !startEU.isValid()) { data.bookingInFlight=false; saveSession(phone,data); return }

    const startUTC = startEU.tz("UTC"), endUTC = startUTC.clone().add(upc.duration_min,"minute")
    let teamId   = data.selectedStaffId || upc.staff_id || TEAM_MEMBER_IDS[0]
    if (!teamId) {
      const found = await discoverTeamMembers()
      TEAM_MEMBER_IDS = found
      teamId = TEAM_MEMBER_IDS[0]
    }
    if (!teamId) { console.error("No hay teamMemberId para reprogramar"); data.bookingInFlight=false; saveSession(phone,data); return }

    let ok=false
    if (upc.square_booking_id) {
      const sq = await updateSquareBooking(upc.square_booking_id, { startEU, serviceKey: upc.service, customerId: upc.customer_square_id, teamMemberId: teamId })
      if (sq) ok=true
    }
    if (!ok) {
      if (upc.square_booking_id) await cancelSquareBooking(upc.square_booking_id)
      const sqNew = await createSquareBooking({ startEU, serviceKey: upc.service, customerId: upc.customer_square_id, teamMemberId: teamId })
      if (!sqNew) { data.bookingInFlight=false; saveSession(phone,data); return }
      markCancelled.run({ id: upc.id })
      const newId=`apt_${Math.random().toString(36).slice(2,8)}${Date.now().toString(36).slice(-4)}`
      insertAppt.run({
        id:newId, customer_name: upc.customer_name, customer_phone: phone, customer_square_id: upc.customer_square_id,
        service: upc.service, duration_min: upc.duration_min, start_iso: startUTC.toISOString(), end_iso: endUTC.toISOString(),
        staff_id: teamId, status:"confirmed", created_at:new Date().toISOString(), square_booking_id: sqNew.id || null
      })
    } else {
      updateApptTimes.run({ id: upc.id, start_iso: startUTC.toISOString(), end_iso: endUTC.toISOString(), staff_id: teamId })
    }

    clearSession.run({ phone })
    await safeSend(from,{ text:
`Cita actualizada.
Servicio: ${upc.service}
Nueva fecha: ${fmtES(startEU)}
Duración: ${upc.duration_min} min` })
  }catch(e){ console.error("finalizeReschedule:", prettyApiError(e)) }
  finally{ data.bookingInFlight=false; try{ saveSession(phone, data) }catch{} }
}
