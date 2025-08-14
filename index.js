// index.js — Gapink Nails WhatsApp Bot (100% Square, sin DB)
// OpenAI gpt-4o-mini + extracción JSON + TZ Europe/Madrid
// Silencioso ante errores (no enviar mensajes de error al cliente)
// Confirmación SOLO si el mensaje ACTUAL contiene “sí/confirmo/ok/vale”
// Persistencia de hora en ms (sin UTC shift) + validaciones (en memoria)
// Disponibilidad forward-first via Square SearchAvailability
// Cancelar/editar reales en Square
// Cancel/busca citas con teléfono flexible (solo dígitos) y/o fecha objetivo

import express from "express"
import baileys from "@whiskeysockets/baileys"
import pino from "pino"
import qrcode from "qrcode"
import qrcodeTerminal from "qrcode-terminal"
import "dotenv/config"
import fs from "fs"
import { webcrypto, createHash } from "crypto"
import dayjs from "dayjs"
import utc from "dayjs/plugin/utc.js"
import tz from "dayjs/plugin/timezone.js"
import "dayjs/locale/es.js"
import { Client, Environment } from "square"

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
const SERVICE_VARIATIONS = { "uñas acrílicas": process.env.SQ_SV_UNAS_ACRILICAS || "" }
const TEAM_MEMBER_IDS = (process.env.SQ_TEAM_IDS || "").split(",").map(s=>s.trim()).filter(Boolean)

// ===== OpenAI
const OPENAI_API_KEY  = process.env.OPENAI_API_KEY
const OPENAI_API_URL  = process.env.OPENAI_API_URL || "https://api.openai.com/v1/chat/completions"
const OPENAI_MODEL    = process.env.OPENAI_MODEL || "gpt-4o-mini"

async function aiChat(messages, { temperature=0.4 } = {}) {
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
// Redondeo a slot (ceil) — definición única
function ceilToSlotEU(t){const m=t.minute();const rem=m%SLOT_MIN;if(rem===0)return t.second(0).millisecond(0);return t.add(SLOT_MIN-rem,"minute").second(0).millisecond(0)}

// ===== Square
const square = new Client({ accessToken: process.env.SQUARE_ACCESS_TOKEN, environment: process.env.SQUARE_ENV==="production"?Environment.Production:Environment.Sandbox })
const locationId = process.env.SQUARE_LOCATION_ID
let LOCATION_TZ = EURO_TZ
async function squareCheckCredentials(){try{const locs=await square.locationsApi.listLocations();const loc=(locs.result.locations||[]).find(l=>l.id===locationId)||(locs.result.locations||[])[0];if(loc?.timezone)LOCATION_TZ=loc.timezone;console.log(`✅ Square listo. Location ${locationId}, TZ=${LOCATION_TZ}`)}catch(e){console.error("⛔ Square:",e?.message||e)}}
async function squareFindCustomerByPhone(phoneRaw){try{const e164=normalizePhoneES(phoneRaw);if(!e164||!e164.startsWith("+")||e164.length<8||e164.length>16)return null;const resp=await square.customersApi.searchCustomers({query:{filter:{phoneNumber:{exact:e164}}}});return (resp?.result?.customers||[])[0]||null}catch(e){console.error("Square search:",e?.message||e);return null}}
async function squareCreateCustomer({givenName,emailAddress,phoneNumber}){try{const phone=normalizePhoneES(phoneNumber);const resp=await square.customersApi.createCustomer({idempotencyKey:`cust_${Date.now()}_${Math.random().toString(36).slice(2,8)}`,givenName,emailAddress,phoneNumber:phone||undefined,note:"Creado desde bot WhatsApp Gapink Nails"});return resp?.result?.customer||null}catch(e){console.error("Square create:",e?.message||e);return null}}
async function getServiceVariationVersion(id){try{const resp=await square.catalogApi.retrieveCatalogObject(id,true);return resp?.result?.object?.version}catch(e){console.error("getServiceVariationVersion:",e?.message||e);return undefined}}
function stableKey({locationId,serviceVariationId,startISO,customerId,teamMemberId}){const raw=`${locationId}|${serviceVariationId}|${startISO}|${customerId}|${teamMemberId}`;return createHash("sha256").update(raw).digest("hex").slice(0,48)}

// ——— Bookings helpers (100% Square)
async function listUpcomingBookingsForCustomer(customerId,{fromISO,toISO}={}){
  try{
    const nowIso = fromISO || dayjs().utc().toISOString()
    const params = { locationId, customerId, startAtMin: nowIso }
    if (toISO) params.startAtMax = toISO
    let out=[]
    let cursor=null
    do{
      const resp = await square.bookingsApi.listBookings({ ...params, cursor })
      out = out.concat(resp?.result?.bookings || [])
      cursor = resp?.result?.cursor || null
    }while(cursor)
    // sólo futuros
    return out.filter(b => b.startAt && dayjs(b.startAt).isAfter(dayjs()))
              .sort((a,b)=> dayjs(a.startAt).valueOf() - dayjs(b.startAt).valueOf())
  }catch(e){ console.error("listUpcomingBookingsForCustomer:", e?.message||e); return [] }
}

async function cancelSquareBooking(bookingId){
  try{
    const get = await square.bookingsApi.retrieveBooking(bookingId)
    const version = get?.result?.booking?.version
    if (version === undefined || version === null) return false
    const body = { idempotencyKey: `cancel_${bookingId}_${version}_${Date.now()}`, bookingVersion: Number(version) }
    const r = await square.bookingsApi.cancelBooking(bookingId, body)
    return !!r?.result?.booking?.id
  }catch(e){ console.error("cancelSquareBooking:", e?.message||e); return false }
}

async function createSquareBooking({startEU,serviceKey,customerId,teamMemberId}){try{
  const serviceVariationId=SERVICE_VARIATIONS[serviceKey]; if(!serviceVariationId||!teamMemberId||!locationId)return null
  const version=await getServiceVariationVersion(serviceVariationId); if(!version)return null
  const startISO=startEU.tz("UTC").toISOString()
  const body={idempotencyKey:stableKey({locationId,serviceVariationId,startISO,customerId,teamMemberId}),booking:{locationId,startAt:startISO,customerId,appointmentSegments:[{teamMemberId,serviceVariationId,serviceVariationVersion:Number(version),durationMinutes:SERVICES[serviceKey]}]}}
  const resp=await square.bookingsApi.createBooking(body); return resp?.result?.booking||null
}catch(e){console.error("createSquareBooking:",e?.message||e);return null}}

async function updateSquareBooking(bookingId,{startEU,serviceVariationId,customerId,teamMemberId,durationMin}){try{
  const get=await square.bookingsApi.retrieveBooking(bookingId);const booking=get?.result?.booking;if(!booking)return null
  const version=await getServiceVariationVersion(serviceVariationId); if(!version) return null
  const startISO=startEU.tz("UTC").toISOString()
  const body={idempotencyKey:stableKey({locationId,serviceVariationId,startISO,customerId,teamMemberId}),booking:{id:bookingId,version:booking.version,locationId,customerId,startAt:startISO,appointmentSegments:[{teamMemberId,serviceVariationId,serviceVariationVersion:Number(version),durationMinutes:durationMin}]}}
  const resp=await square.bookingsApi.updateBooking(bookingId, body);return resp?.result?.booking||null
}catch(e){console.error("updateSquareBooking:",e?.message||e);return null}}

// ===== Memoria de sesión (en RAM, sin DB)
const SESSIONS = new Map()
function loadSession(phone){const s=SESSIONS.get(phone);if(!s)return null;const d={...s};if(s.startEU_ms) d.startEU=dayjs.tz(s.startEU_ms,EURO_TZ);return d}
function saveSession(phone,data){const c={...data};c.startEU_ms=data.startEU?.valueOf?.() ?? data.startEU_ms ?? null;delete c.startEU;SESSIONS.set(phone,c)}
function clearSession(phone){SESSIONS.delete(phone)}

// ===== Buscar cita futura por teléfono (opcionalmente cerca de una fecha objetivo)
async function findUpcomingForPhone(phone, targetEU=null){
  try{
    const cust = await squareFindCustomerByPhone(phone)
    if (!cust) return null
    const futures = await listUpcomingBookingsForCustomer(cust.id)
    if (!futures.length) return null
    if (targetEU){
      const sameDay = futures.filter(b=>{
        const d=dayjs(b.startAt).tz(EURO_TZ)
        return d.isSame(targetEU,"day")
      })
      if (sameDay.length){
        sameDay.sort((a,b)=> Math.abs(dayjs(a.startAt).diff(targetEU)) - Math.abs(dayjs(b.startAt).diff(targetEU)) )
        return sameDay[0]
      }
    }
    return futures[0]
  }catch(e){ console.error("findUpcomingForPhone:", e?.message||e); return null }
}

// ===== Disponibilidad (Square SearchAvailability, forward-first)
function clampBusinessEU(req){
  const dayStart=req.clone().hour(OPEN_HOUR).minute(0).second(0).millisecond(0)
  const dayEnd=req.clone().hour(CLOSE_HOUR).minute(0).second(0).millisecond(0)
  let t=req.clone()
  if (t.isBefore(dayStart)) t=dayStart
  if (t.isAfter(dayEnd)) t=dayEnd
  return { t, dayStart, dayEnd }
}

async function squareSearchAvail({startEU, serviceKey, preferredTeamId=null, forward=true}) {
  const serviceVariationId = SERVICE_VARIATIONS[serviceKey]
  if (!serviceVariationId || !locationId) return []
  const { t, dayStart, dayEnd } = clampBusinessEU(startEU.clone())
  const now = dayjs().tz(EURO_TZ).add(30,"minute")
  const fromEU = ceilToSlotEU( t.isBefore(now) ? now : t )
  const rangeStart = (forward ? fromEU : dayStart).tz("UTC").toISOString()
  const rangeEnd   = (forward ? dayEnd : fromEU).tz("UTC").toISOString()
  const teamFilter = preferredTeamId ? { all: [preferredTeamId] } :
    (TEAM_MEMBER_IDS.length ? { all: TEAM_MEMBER_IDS } : undefined)
  const query = {
    query: {
      filter: {
        locationId,
        startAtRange: { startAt: rangeStart, endAt: rangeEnd },
        segmentFilters: [{
          serviceVariationId,
          teamMemberIdFilter: teamFilter
        }]
      }
    }
  }
  try{
    const r = await square.bookingsApi.searchAvailability(query)
    const list = (r?.result?.availabilities || []).map(a=>{
      const start = dayjs(a.startAt)
      // primer segmento trae el team asignable
      const seg = (a?.appointmentSegments||[])[0]
      return { start, teamMemberId: seg?.teamMemberId || null }
    })
    // si buscamos atrás, ordenar descendente
    return forward ? list.sort((a,b)=> a.start.valueOf()-b.start.valueOf())
                   : list.sort((a,b)=> b.start.valueOf()-a.start.valueOf())
  }catch(e){ console.error("searchAvailability:", e?.message||e); return [] }
}

async function suggestOrExactSquare(startEU, serviceKey, durationMin, preferredStaffId=null){
  // día permitido + horario + domingo cerrado
  const dow = startEU.day()===0?7:startEU.day()
  if (dow===7 || !WORK_DAYS.includes(dow)) return { exact:null, suggestion:null, staffId:null }
  const { t, dayStart, dayEnd } = clampBusinessEU(startEU)
  const endEU = t.clone().add(durationMin,"minute")
  const inside = t.hour()>=OPEN_HOUR && (endEU.hour()<CLOSE_HOUR || (endEU.hour()===CLOSE_HOUR && endEU.minute()===0))
  if (!inside) return { exact:null, suggestion:null, staffId:null }

  const want = ceilToSlotEU(t)
  // forward-first
  const forward = await squareSearchAvail({ startEU: want, serviceKey, preferredTeamId: preferredStaffId, forward: true })
  const wantUTCiso = want.tz("UTC").toISOString()
  const exact = forward.find(x => x.start.toISOString() === wantUTCiso)
  if (exact) return { exact: want, suggestion: null, staffId: exact.teamMemberId || (preferredStaffId || TEAM_MEMBER_IDS[0] || "any") }

  if (forward.length) {
    const pick = forward[0]
    return { exact:null, suggestion: pick.start.tz(EURO_TZ), staffId: pick.teamMemberId || (preferredStaffId || TEAM_MEMBER_IDS[0] || "any") }
  }

  // si no hay hacia delante, miramos hacia atrás el mismo día
  const backward = await squareSearchAvail({ startEU: want, serviceKey, preferredTeamId: preferredStaffId, forward: false })
  if (backward.length){
    const pick = backward[0]
    return { exact:null, suggestion: pick.start.tz(EURO_TZ), staffId: pick.teamMemberId || (preferredStaffId || TEAM_MEMBER_IDS[0] || "any") }
  }

  return { exact:null, suggestion:null, staffId:null }
}

// ===== Mini web
const app=express()
const PORT=process.env.PORT||8080
let lastQR=null,conectado=false
app.get("/",(_req,res)=>{res.send(`<!doctype html><meta charset="utf-8"><style>body{font-family:system-ui;display:grid;place-items:center;min-height:100vh;background:linear-gradient(135deg,#fce4ec,#f8bbd0);color:#4a148c} .card{background:#fff;padding:24px;border-radius:16px;box-shadow:0 6px 24px rgba(0,0,0,.08);text-align:center;max-width:520px}</style><div class="card"><h1>Gapink Nails</h1><p>Estado: ${conectado?"✅ Conectado":"❌ Desconectado"}</p>${!conectado&&lastQR?`<img src="/qr.png" width="320" />`:``}<p><small>Desarrollado por Gonzalo</small></p></div>`)})
app.get("/qr.png",async(_req,res)=>{if(!lastQR)return res.status(404).send("No hay QR");const png=await qrcode.toBuffer(lastQR,{type:"png",width:512,margin:1});res.set("Content-Type","image/png").send(png)})

// ===== Cola envío Baileys
const wait=(ms)=>new Promise(r=>setTimeout(r,ms))
app.listen(PORT,async()=>{console.log(`🌐 Web en puerto ${PORT}`);await squareCheckCredentials();startBot().catch(console.error)})

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

        // Sesión (en RAM)
        let data=loadSession(phone)||{
          service:null,startEU:null,durationMin:null,name:null,email:null,
          confirmApproved:false,confirmAsked:false,bookingInFlight:false,
          lastUserDtText:null,lastService:null, selectedStaffId:null,
          editBookingId:null, editServiceVarId:null, editDurationMin:null, editCustomerId:null
        }

        // IA: extracción (confirmación NO se fía de la IA)
        const extra=await extractFromText(textRaw)
        const incomingService = extra.service || detectServiceFree(textRaw)
        const incomingDt = extra.datetime_text || null
        const parsedInMessage = parseDateTimeES(incomingDt ? incomingDt : textRaw)

        // Cancelación explícita
        if (extra.intent==="cancel" || /\b(cancel|anul|borra|elimina)r?\b/i.test(textRaw)) {
          const upc = await findUpcomingForPhone(phone, parsedInMessage || null)
          if (upc) {
            const ok = await cancelSquareBooking(upc.id)
            if (ok) { clearSession(phone); await __SAFE_SEND__(from,{ text:`He cancelado tu cita del ${fmtES(dayjs(upc.startAt))}.` }) }
          } else {
            await __SAFE_SEND__(from,{ text:"No veo ninguna cita futura tuya. Si quieres, dime fecha y hora y te doy hueco." })
          }
          return
        }

        // ¿Pide reprogramar?
        if (extra.intent==="reschedule" || RESCH_RE.test(textRaw)) {
          const upc = await findUpcomingForPhone(phone, parsedInMessage || null)
          if (upc) {
            const seg = (upc.appointmentSegments||[])[0] || {}
            data.editBookingId = upc.id
            data.service = incomingService || data.service || Object.keys(SERVICE_VARIATIONS).find(k=>SERVICE_VARIATIONS[k]===seg.serviceVariationId) || "uñas acrílicas"
            data.durationMin = seg?.durationMinutes || SERVICES[data.service] || 60
            data.selectedStaffId = seg?.teamMemberId || null
            data.editServiceVarId = seg?.serviceVariationId || SERVICE_VARIATIONS[data.service]
            data.editDurationMin = data.durationMin
            data.editCustomerId = upc.customerId
          }
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
        if (parsedInMessage) data.startEU = parsedInMessage

        if (data.service && !data.durationMin) data.durationMin = SERVICES[data.service] || 60
        saveSession(phone, data)

        // Cierre inmediato SOLO si el mensaje actual es afirmativo
        if (data.confirmAsked && userSaysYes && data.service && data.startEU && data.durationMin) {
          if (data.editBookingId) await finalizeReschedule({ from, phone, data, safeSend: __SAFE_SEND__ })
          else await finalizeBooking({ from, phone, data, safeSend: __SAFE_SEND__ })
          return
        }

        // DISPONIBILIDAD (via Square)
        if (data.service && data.startEU && data.durationMin) {
          const preferred = data.editBookingId ? data.selectedStaffId : null
          const { exact, suggestion, staffId } = await suggestOrExactSquare(data.startEU, data.service, data.durationMin, preferred)
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
      }catch(e){ console.error("messages.upsert error:",e) }
    })
  }catch(e){ console.error("startBot error:",e) }
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

    const teamMemberId = data.selectedStaffId || TEAM_MEMBER_IDS[0] || "any"

    const sq = await createSquareBooking({ startEU, serviceKey: data.service, customerId: customer.id, teamMemberId })
    if (!sq) { data.bookingInFlight=false; saveSession(phone,data); return }

    clearSession(phone)
    await safeSend(from,{ text:
`Reserva confirmada.
Servicio: ${data.service}
Fecha: ${fmtES(startEU)}
Duración: ${SERVICES[data.service]} min
Pago en persona.` })
  } catch (e) { console.error("finalizeBooking:", e) }
  finally { data.bookingInFlight=false; try{ saveSession(phone, data) }catch{} }
}

// ===== Edición (silenciosa ante errores)
async function finalizeReschedule({ from, phone, data, safeSend }) {
  try{
    if (data.bookingInFlight) return
    data.bookingInFlight = true; saveSession(phone, data)

    const upc = await findUpcomingForPhone(phone, null)
    if (!upc || upc.id !== data.editBookingId) { data.bookingInFlight=false; saveSession(phone,data); return }

    const startEU = dayjs.isDayjs(data.startEU) ? data.startEU : (data.startEU_ms ? dayjs.tz(Number(data.startEU_ms), EURO_TZ) : null)
    if (!startEU || !startEU.isValid()) { data.bookingInFlight=false; saveSession(phone,data); return }

    const seg = (upc.appointmentSegments||[])[0] || {}
    const serviceVarId = data.editServiceVarId || seg.serviceVariationId || SERVICE_VARIATIONS[data.service]
    const durationMin = data.editDurationMin || seg.durationMinutes || SERVICES[data.service] || 60
    const teamId   = data.selectedStaffId || seg.teamMemberId || TEAM_MEMBER_IDS[0] || "any"

    let ok=false
    const sq = await updateSquareBooking(upc.id, { startEU, serviceVariationId: serviceVarId, customerId: upc.customerId, teamMemberId: teamId, durationMin })
    if (sq) ok=true

    if (!ok) { data.bookingInFlight=false; saveSession(phone,data); return }

    clearSession(phone)
    await safeSend(from,{ text:
`Cita actualizada.
Servicio: ${data.service}
Nueva fecha: ${fmtES(startEU)}
Duración: ${durationMin} min` })
  }catch(e){ console.error("finalizeReschedule:", e) }
  finally{ data.bookingInFlight=false; try{ saveSession(phone, data) }catch{} }
}
