// index.js — Gapink Nails WhatsApp Bot (Square-only + Web QR + Botones) — FIX servicios=0
// - Sin DB; todo en Square
// - OpenAI 4o-mini para intención + servicio más parecido
// - Carga de servicios: detecta variaciones con appointmentServiceVariationData (más robusto)
// - Fallback opcional desde env SQ_SERVICES_FALLBACK (JSON: [{name,variationId,durationMin}])
// - Botones "Sí / No" para confirmar reservar / cancelar / editar
// - Web QR para vincular Baileys
// - TZ Europe/Madrid; sin mensajes de error técnicos al cliente

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

// ===== Config negocio
const WORK_DAYS = [1,2,3,4,5,6] // L-S
const OPEN_HOUR = 10
const CLOSE_HOUR = 20
const SLOT_MIN  = 30
const TEAM_MEMBER_IDS = (process.env.SQ_TEAM_IDS || "").split(",").map(s=>s.trim()).filter(Boolean)
const LOCATION_ID = process.env.SQUARE_LOCATION_ID

// ===== OpenAI (4o-mini)
const OPENAI_API_KEY = process.env.OPENAI_API_KEY
const OPENAI_API_URL = process.env.OPENAI_API_URL || "https://api.openai.com/v1/chat/completions"
const OPENAI_MODEL   = process.env.OPENAI_MODEL   || "gpt-4o-mini"

async function aiChat(messages, { temperature=0.3 } = {}) {
  try {
    const r = await fetch(OPENAI_API_URL, {
      method: "POST",
      headers: { "Content-Type":"application/json", "Authorization":`Bearer ${OPENAI_API_KEY}` },
      body: JSON.stringify({ model: OPENAI_MODEL, messages, temperature })
    })
    if (!r.ok) throw new Error(`OpenAI ${r.status}`)
    const j = await r.json()
    return (j?.choices?.[0]?.message?.content || "").trim()
  } catch (e) { console.error("OpenAI:", e?.message || e); return "" }
}

const SYS = `Eres el asistente de WhatsApp de Gapink Nails (España).
Escribe en español (España), natural y breve, sin emojis ni tecnicismos.
No digas que eres IA.`

async function extractFromText(userText="") {
  const schema = `
Devuelve SOLO un JSON válido:
{
  "intent": "booking|cancel|reschedule|greeting|other",
  "service_text": "texto del servicio si aparece",
  "datetime_text": "texto fecha/hora si aparece",
  "confirm": "yes|no|unknown",
  "name": "si aparece",
  "email": "si aparece"
}`
  const content = await aiChat([
    { role:"system", content: `${SYS}\n${schema}` },
    { role:"user", content: userText }
  ], { temperature: 0.1 })
  try {
    const json = content.trim().replace(/^```(json)?/i,"").replace(/```$/,"")
    return JSON.parse(json)
  } catch { return { intent:"other" } }
}

// ===== Utilidades y botones
const onlyDigits = (s="") => (s||"").replace(/\D+/g,"")
function normalizePhoneES(raw){const d=onlyDigits(raw);if(!d)return null;if(raw.startsWith("+")&&d.length>=8&&d.length<=15)return`+${d}`;if(d.startsWith("34")&&d.length===11)return`+${d}`;if(d.length===9)return`+34${d}`;if(d.startsWith("00"))return`+${d.slice(2)}`;return`+${d}`}
const rmDiacritics = (s="") => s.normalize("NFD").replace(/\p{Diacritic}/gu,"")
const YES_RE = /\b(si|sí|ok|vale|confirmo|confirmar|de acuerdo|perfecto)\b/i
const NO_RE  = /\b(no|cancela|anula|otra|cambia)\b/i

function parseDateTimeES(dtText){
  if(!dtText) return null
  const t=rmDiacritics(dtText.toLowerCase())
  let base=null
  if(/\bhoy\b/.test(t)) base=dayjs().tz(EURO_TZ)
  else if(/\bmanana\b/.test(t)) base=dayjs().tz(EURO_TZ).add(1,"day")
  if(!base){
    const M={enero:1,febrero:2,marzo:3,abril:4,mayo:5,junio:6,julio:7,agosto:8,septiembre:9,setiembre:9,octubre:10,noviembre:11,diciembre:12,ene:1,feb:2,mar:3,abr:4,may:5,jun:6,jul:7,ago:8,sep:9,oct:10,nov:11,dic:12}
    const m=t.match(/\b(\d{1,2})\s+de\s+(enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|setiembre|octubre|noviembre|diciembre|ene|feb|mar|abr|may|jun|jul|ago|sep|oct|nov|dic)\b(?:\s+de\s+(\d{4}))?/)
    if(m){const dd=+m[1],mm=M[m[2]],yy=m[3]?+m[3]:dayjs().tz(EURO_TZ).year();base=dayjs.tz(`${yy}-${String(mm).padStart(2,"0")}-${String(dd).padStart(2,"0")} 00:00`,EURO_TZ)}
  }
  if(!base){
    const m=t.match(/\b(\d{1,2})[\/\-](\d{1,2})(?:[\/\-](\d{2,4}))?\b/)
    if(m){let yy=m[3]?+m[3]:dayjs().tz(EURO_TZ).year();if(yy<100)yy+=2000;base=dayjs.tz(`${yy}-${String(+m[2]).padStart(2,"0")}-${String(+m[1]).padStart(2,"0")} 00:00`,EURO_TZ)}
  }
  if(!base) base=dayjs().tz(EURO_TZ)
  let hour=null,minute=0
  const hm=t.match(/(\d{1,2})(?::|h)?(\d{2})?\s*(am|pm)?\b/)
  if(hm){hour=+hm[1];minute=hm[2]?+hm[2]:0;const ap=hm[3];if(ap==="pm"&&hour<12)hour+=12;if(ap==="am"&&hour===12)hour=0}
  if(hour===null) return null
  return base.hour(hour).minute(minute).second(0).millisecond(0)
}
const fmtES=(d)=>{const t=(dayjs.isDayjs(d)?d:dayjs(d)).tz(EURO_TZ);const dias=["domingo","lunes","martes","miércoles","jueves","viernes","sábado"];return `${dias[t.day()]} ${String(t.date()).padStart(2,"0")}/${String(t.month()+1).padStart(2,"0")} ${String(t.hour()).padStart(2,"0")}:${String(t.minute()).padStart(2,"0")}`}

function ceilToSlotEU(t){const m=t.minute();const rem=m%SLOT_MIN;if(rem===0)return t.second(0).millisecond(0);return t.add(SLOT_MIN-rem,"minute").second(0).millisecond(0)}

// ===== Square SDK + REST (para /bookings/search)
const square = new Client({
  accessToken: process.env.SQUARE_ACCESS_TOKEN,
  environment: process.env.SQUARE_ENV==="production" ? Environment.Production : Environment.Sandbox
})
const SQUARE_API_BASE = process.env.SQUARE_ENV==="production"
  ? "https://connect.squareup.com/v2"
  : "https://connect.squareupsandbox.com/v2"
const SQUARE_API_VERSION = process.env.SQUARE_API_VERSION || "2024-06-20"

async function sqREST(path, method="GET", body) {
  try {
    const r = await fetch(`${SQUARE_API_BASE}${path}`, {
      method,
      headers: {
        "Content-Type":"application/json",
        "Authorization":`Bearer ${process.env.SQUARE_ACCESS_TOKEN}`,
        "Square-Version": SQUARE_API_VERSION
      },
      body: body ? JSON.stringify(body) : undefined
    })
    const j = await r.json().catch(()=> ({}))
    if (!r.ok) throw new Error(j?.errors?.[0]?.detail || `${r.status}`)
    return j
  } catch (e) { console.error("Square REST", path, e?.message||e); return null }
}

// === Catálogo de servicios robusto
let SERVICE_BY_NAME = new Map() // name -> { itemId, variationId, durationMin }
let SERVICES_LIST = []          // nombres visibles

async function loadServicesFromSquare(){
  SERVICE_BY_NAME.clear(); SERVICES_LIST=[]
  let cursor=null
  const all = []
  do {
    const j = await sqREST(`/catalog/list?types=ITEM,ITEM_VARIATION${cursor?`&cursor=${encodeURIComponent(cursor)}`:""}`,"GET")
    if (!j) break
    if (Array.isArray(j.objects)) all.push(...j.objects)
    cursor = j.cursor || null
  } while(cursor)

  const items = all.filter(o=>o.type==="ITEM")
  const itemMap = new Map(items.map(i=>[i.id,i]))
  const vars  = all.filter(o=>o.type==="ITEM_VARIATION")

  for (const v of vars){
    const ivd = v.itemVariationData || {}
    const asvd = ivd.appointmentServiceVariationData || v.appointmentServiceVariationData
    if (!asvd) continue // solo las variaciones que realmente son de cita
    const item = itemMap.get(ivd.itemId)
    const baseName = (item?.itemData?.name || "Servicio").trim()
    const varName  = (ivd?.name || "").trim()
    const name = varName && varName.toLowerCase()!=="standard" ? `${baseName} ${varName}`.trim() : baseName
    const durSec = asvd?.serviceDuration ?? v?.serviceDuration ?? null
    const durationMin = durSec ? Math.max(15, Math.round(Number(durSec)/60)) : 60
    SERVICE_BY_NAME.set(name.toLowerCase(), { itemId:item?.id || null, variationId:v.id, durationMin })
    SERVICES_LIST.push(name)
  }

  // Fallback opcional por env si no hay nada en Square
  if (SERVICES_LIST.length===0) {
    try{
      const raw = process.env.SQ_SERVICES_FALLBACK || ""
      if (raw) {
        const list = JSON.parse(raw) // [{name, variationId, durationMin}]
        for (const s of list) {
          if (!s?.name || !s?.variationId) continue
          SERVICE_BY_NAME.set(String(s.name).toLowerCase(), { itemId:null, variationId:String(s.variationId), durationMin: Number(s.durationMin)||60 })
          SERVICES_LIST.push(String(s.name))
        }
      }
    }catch(e){ console.error("SQ_SERVICES_FALLBACK parse:", e?.message||e) }
  }

  console.log(`🗂️ Servicios cargados: ${SERVICES_LIST.length}`)
}

async function matchServiceOrClosest(userText){
  if (!SERVICES_LIST.length) await loadServicesFromSquare()
  if (!SERVICES_LIST.length) {
    console.warn("No hay servicios en Square ni fallback; no puedo mapear servicio.")
    return null
  }
  const lowText = rmDiacritics((userText||"").toLowerCase())

  // 1) match directo por inclusión
  for (const name of SERVICES_LIST){
    const nm = String(name||"")
    if (!nm) continue
    if (lowText.includes(rmDiacritics(nm.toLowerCase()))) {
      const e = SERVICE_BY_NAME.get(nm.toLowerCase())
      if (e) return { name:nm, ...e }
    }
  }

  // 2) IA elige el más parecido (pero protegido)
  const list = SERVICES_LIST.join(" | ")
  const pick = await aiChat([
    { role:"system", content:`${SYS}\nElige de la lista el servicio más parecido al texto del cliente. Responde SOLO con un nombre EXACTO de la lista, sin explicaciones.` },
    { role:"user", content:`Lista de servicios:\n${list}\n\nTexto del cliente: "${userText||""}"` }
  ], { temperature: 0.1 })

  const chosen = (pick||"").split("\n")[0]?.trim?.() || ""
  const key = chosen ? chosen.toLowerCase() : ""
  const e = key ? SERVICE_BY_NAME.get(key) : null
  if (e) return { name: chosen, ...e }

  // 3) fallback: primer servicio disponible (ya no rompe si no hay)
  const fallback = SERVICES_LIST[0]
  if (!fallback) return null
  const ef = SERVICE_BY_NAME.get(fallback.toLowerCase())
  if (!ef) return null
  return { name: fallback, ...ef }
}

// === Clientes y reservas
async function squareFindCustomerByPhone(phoneRaw){
  try{
    const e164=normalizePhoneES(phoneRaw)
    if(!e164) return null
    const r = await square.customersApi.searchCustomers({ query:{ filter:{ phoneNumber:{ exact:e164 } } } })
    return (r?.result?.customers||[])[0]||null
  }catch(e){ console.error("Square customer search:", e?.message||e); return null }
}
async function squareCreateCustomer({ name, email, phone }){
  try{
    const r = await square.customersApi.createCustomer({
      idempotencyKey:`cust_${Date.now()}_${Math.random().toString(36).slice(2,8)}`,
      givenName: name || "Cliente WhatsApp",
      emailAddress: email || undefined,
      phoneNumber: normalizePhoneES(phone) || undefined,
      note: "Creado desde WhatsApp Gapink Nails"
    })
    return r?.result?.customer || null
  }catch(e){ console.error("Square create customer:", e?.message||e); return null }
}
async function getServiceVariationVersion(variationId){
  try{
    const r = await square.catalogApi.retrieveCatalogObject(variationId, true)
    return r?.result?.object?.version
  }catch(e){ console.error("getServiceVariationVersion:", e?.message||e); return undefined }
}
function stableKey(parts){return createHash("sha256").update(Object.values(parts).join("|")).digest("hex").slice(0,48)}

// --- BUSCAR reservas (REST /v2/bookings/search)
async function searchBookings({ customerId, teamMemberIds, startAt, endAt }){
  const body = { limit: 200, query:{ filter:{ locationId: LOCATION_ID } } }
  if (customerId) body.query.filter.customerIds = [customerId]
  if (teamMemberIds?.length) body.query.filter.teamMemberIds = teamMemberIds
  if (startAt || endAt) body.query.filter.startAtRange = { ...(startAt?{startAt}:{}) , ...(endAt?{endAt}:{}) }
  const j = await sqREST("/bookings/search","POST", body)
  const arr = j?.bookings || []
  return arr.filter(b => b.status !== "CANCELLED")
}

// --- Disponibilidad (sin solapes)
function overlaps(aStart, aEnd, bStart, bEnd){ return (aStart < bEnd) && (bStart < aEnd) }
async function firstFreeSlotOrExact(startEU, durationMin){
  const dayStart = startEU.clone().hour(OPEN_HOUR).minute(0).second(0)
  const dayEnd   = startEU.clone().hour(CLOSE_HOUR).minute(0).second(0)
  const nowMin = dayjs().tz(EURO_TZ).add(30,"minute")
  let t = ceilToSlotEU(startEU.isBefore(nowMin)? nowMin : startEU)

  const endWanted = (s)=> s.clone().add(durationMin,"minute")
  const rangeStart = dayStart.tz("UTC").toISOString()
  const rangeEnd   = dayEnd.tz("UTC").toISOString()

  const staffList = TEAM_MEMBER_IDS.length ? TEAM_MEMBER_IDS : ["any"]
  const perStaff = {}
  for (const staffId of staffList) {
    const bookings = await searchBookings({ teamMemberIds: staffId==="any"?undefined:[staffId], startAt: rangeStart, endAt: rangeEnd })
    perStaff[staffId] = bookings.map(b=>({
      start: dayjs(b.startAt),
      end: dayjs(b.startAt).add( (b?.appointmentSegments?.[0]?.durationMinutes||60), "minute" )
    }))
  }

  const desiredEnd = endWanted(t)
  for (const staffId of staffList) {
    const busy = perStaff[staffId].some(iv => overlaps(t, desiredEnd, iv.start, iv.end))
    if (!busy) return { exact: t, staffId }
  }
  for (let s = t.clone().add(SLOT_MIN,"minute"); !s.isAfter(dayEnd); s=s.add(SLOT_MIN,"minute")) {
    const e = endWanted(s)
    for (const staffId of staffList) {
      const busy = perStaff[staffId].some(iv => overlaps(s, e, iv.start, iv.end))
      if (!busy) return { suggestion: s, staffId }
    }
  }
  return { exact:null, suggestion:null, staffId:null }
}

// ===== WhatsApp infra (Baileys) + botones
const SESS = new Map() // phone -> session
function getSession(phone){
  if(!SESS.has(phone)) SESS.set(phone, {
    mode:null,             // "book" | "edit" | "cancel"
    pending:null,          // {action,...} para botones
    service:null,          // {name, variationId, durationMin}
    startEU:null,
    name:null, email:null,
    editBookingId:null
  })
  return SESS.get(phone)
}
function makeCtxId(){ return Math.random().toString(36).slice(2,10) }
function asYesNo(text, ctxId){
  return {
    text,
    footer: "Gapink Nails",
    buttons: [
      { buttonId:`YES|${ctxId}`, buttonText:{displayText:"Sí"}, type:1 },
      { buttonId:`NO|${ctxId}`,  buttonText:{displayText:"No"}, type:1 }
    ],
    headerType: 1
  }
}
const wait=(ms)=>new Promise(r=>setTimeout(r,ms))

// ===== Mini web QR
const app=express()
const PORT=process.env.PORT||8080
let lastQR=null, conectado=false
app.get("/",(_req,res)=>{res.send(`<!doctype html><meta charset="utf-8"><style>body{font-family:system-ui;display:grid;place-items:center;min-height:100vh;background:linear-gradient(135deg,#fce4ec,#f8bbd0);color:#4a148c} .card{background:#fff;padding:24px;border-radius:16px;box-shadow:0 6px 24px rgba(0,0,0,.08);text-align:center;max-width:520px}</style><div class="card"><h1>Gapink Nails</h1><p>Estado: ${conectado?"✅ Conectado":"❌ Desconectado"}</p>${!conectado&&lastQR?`<img src="/qr.png" width="320" />`:``}<p><small>Desarrollado por Gonzalo</small></p></div>`)})
app.get("/qr.png",async(_req,res)=>{try{if(!lastQR)return res.status(404).send("No hay QR");const png=await qrcode.toBuffer(lastQR,{type:"png",width:512,margin:1});res.set("Content-Type","image/png").send(png)}catch{res.status(500).end()}})
app.listen(PORT, async()=>{ console.log(`🌐 Web en puerto ${PORT}`); await loadServicesFromSquare(); startBot().catch(console.error) })

// ===== Bot
async function startBot(){
  console.log("🚀 Bot arrancando…")
  try{
    if(!fs.existsSync("auth_info"))fs.mkdirSync("auth_info",{recursive:true})
    const { state, saveCreds } = await useMultiFileAuthState("auth_info")
    const { version } = await fetchLatestBaileysVersion()
    let isOpen=false,reconnect=false
    const sock=makeWASocket({logger:pino({level:"silent"}),auth:state,version,browser:Browsers.macOS("Desktop"),printQRInTerminal:false})

    // Cola segura
    const outbox=[]; let sending=false
    const safeSend=(jid,content)=>new Promise((resolve)=>{ outbox.push({jid,content,resolve}); processOutbox().catch(console.error) })
    async function processOutbox(){ if(sending) return; sending=true; while(outbox.length){const {jid,content,resolve}=outbox.shift(); let guard=0; while(!isOpen && guard<60){await wait(1000);guard++} try{ await sock.sendMessage(jid,content) }catch(e){ console.error("sendMessage:",e?.message||e) } resolve(true) } sending=false }

    sock.ev.on("connection.update",({connection,lastDisconnect,qr})=>{
      if(qr){ lastQR=qr; conectado=false; try{ qrcodeTerminal.generate(qr,{small:true}) }catch{} }
      if(connection==="open"){ lastQR=null; conectado=true; isOpen=true; console.log("✅ Conectado a WhatsApp"); processOutbox().catch(console.error) }
      if(connection==="close"){ conectado=false; isOpen=false; const reason=lastDisconnect?.error?.message||String(lastDisconnect?.error||""); console.log("❌ Conexión cerrada:",reason); if(!reconnect){ reconnect=true; setTimeout(()=>startBot().catch(console.error),2000) } }
    })
    sock.ev.on("creds.update",saveCreds)

    // === Mensajes
    sock.ev.on("messages.upsert", async ({ messages })=>{
      try{
        const m=messages?.[0]; if(!m?.message || m.key.fromMe) return
        const from=m.key.remoteJid
        const phone=normalizePhoneES((from||"").split("@")[0]||"")||(from||"").split("@")[0]||""
        const msg = m.message
        const body = msg.conversation || msg.extendedTextMessage?.text || msg.imageMessage?.caption || ""
        const text = (body||"").trim()
        const session = getSession(phone)

        // Botón pulsado
        const btnId = msg.buttonsResponseMessage?.selectedButtonId || msg.templateButtonReplyMessage?.selectedId
        if (btnId && session?.pending) {
          const [ans, ctx] = String(btnId).split("|")
          if (ctx === session.pending.ctxId) {
            if (ans === "YES") await onConfirmYes({ from, phone, session, safeSend })
            else await onConfirmNo({ from, phone, session, safeSend })
            return
          }
        }

        // Texto normal → IA
        const extra = await extractFromText(text)
        if (extra.name) session.name = extra.name
        if (extra.email) session.email = extra.email

        // CANCELAR
        if (extra.intent==="cancel" || /cancel|anul|borra/i.test(text)) {
          const customer = await squareFindCustomerByPhone(phone)
          if (!customer?.id) { await safeSend(from,{ text:"No veo ninguna cita futura tuya. Si quieres, dime fecha y hora y te doy hueco."}); return }
          const upcoming = await searchBookings({ customerId: customer.id, startAt: dayjs().utc().toISOString() })
          if (!upcoming.length) { await safeSend(from,{ text:"No veo ninguna cita futura tuya. Si quieres, dime fecha y hora y te doy hueco."}); return }
          const target = upcoming.sort((a,b)=>new Date(a.startAt)-new Date(b.startAt))[0]
          session.mode="cancel"
          session.pending={ action:"cancel", bookingId: target.id, ctxId: makeCtxId(), when: dayjs(target.startAt) }
          await safeSend(from, asYesNo(`¿Seguro que cancelo tu cita del ${fmtES(dayjs(target.startAt))}?`, session.pending.ctxId))
          return
        }

        // EDITAR (pedimos nueva fecha)
        if (extra.intent==="reschedule" || /\b(cambia|cambiar|mover|modifica|reprograma)\b/i.test(text)) {
          const customer = await squareFindCustomerByPhone(phone)
          const upcoming = customer?.id ? await searchBookings({ customerId: customer.id, startAt: dayjs().utc().toISOString() }) : []
          if (!upcoming.length) { await safeSend(from,{ text:"No veo ninguna cita futura tuya. Dime nueva fecha y hora y te doy hueco."}); return }
          const target = upcoming.sort((a,b)=>new Date(a.startAt)-new Date(b.startAt))[0]
          session.mode="edit"
          session.pending=null
          session.editBookingId = target.id
          await safeSend(from,{ text:"Dime la nueva fecha y hora que prefieres (ej: “viernes 15 a las 11:00”)." })
          return
        }

        // Si estamos en modo edición y llega una fecha → proponer
        if (session.mode==="edit" && (extra.datetime_text || parseDateTimeES(text))) {
          const startEU = parseDateTimeES(extra.datetime_text || text)
          if (!startEU) { await safeSend(from,{ text:"No he pillado la hora. Dímela así: “15/08 10:00”." }); return }
          try{
            const get = await sqREST(`/bookings/${session.editBookingId}`,"GET")
            const booking = get?.booking
            const staffId = booking?.appointmentSegments?.[0]?.teamMemberId || TEAM_MEMBER_IDS[0] || "any"
            const durMin = booking?.appointmentSegments?.[0]?.durationMinutes || 60
            const { exact, suggestion, staffId: okStaff } = await firstFreeSlotOrExact(startEU, durMin)
            if (exact) {
              session.pending={ action:"edit", ctxId: makeCtxId(), bookingId: session.editBookingId, startEU: exact, staffId: okStaff, durationMin: durMin }
              await safeSend(from, asYesNo(`Tengo libre ${fmtES(exact)}. ¿Confirmo el cambio de cita?`, session.pending.ctxId))
              return
            }
            if (suggestion) {
              session.pending={ action:"edit", ctxId: makeCtxId(), bookingId: session.editBookingId, startEU: suggestion, staffId: okStaff, durationMin: durMin }
              await safeSend(from, asYesNo(`No tengo ese hueco exacto. Te propongo ${fmtES(suggestion)}. ¿Te viene bien?`, session.pending.ctxId))
              return
            }
            await safeSend(from,{ text:"Ese día lo tengo completo. Dime otra hora o día y te digo." })
          }catch(e){ console.error("prepare edit:", e?.message||e) }
          return
        }

        // RESERVAR
        const when = parseDateTimeES(extra.datetime_text || text)
        let serviceInfo = session.service
        if (extra.service_text || /mani(cura|cure)/i.test(text) || /uñas|unias/i.test(text)) {
          serviceInfo = await matchServiceOrClosest(extra.service_text || text)
          session.service = serviceInfo
        }
        if (when && serviceInfo?.variationId) {
          const { exact, suggestion, staffId } = await firstFreeSlotOrExact(when, serviceInfo.durationMin)
          if (exact) {
            session.mode="book"
            session.pending={ action:"book", ctxId: makeCtxId(), startEU: exact, staffId, service: serviceInfo }
            await safeSend(from, asYesNo(`Tengo libre ${fmtES(exact)} para ${serviceInfo.name}. ¿Confirmo la cita?`, session.pending.ctxId))
            return
          }
          if (suggestion) {
            session.mode="book"
            session.pending={ action:"book", ctxId: makeCtxId(), startEU: suggestion, staffId, service: serviceInfo }
            await safeSend(from, asYesNo(`No tengo ese hueco exacto. Te puedo ofrecer ${fmtES(suggestion)} para ${serviceInfo.name}. ¿Te viene bien?`, session.pending.ctxId))
            return
          }
          await safeSend(from,{ text:"No veo hueco en esa franja. Dime otra hora o día y te digo." })
          return
        }

        // Faltan datos
        if (!serviceInfo) {
          if (!SERVICES_LIST.length) {
            await safeSend(from,{ text:"Ahora mismo no tengo ningún servicio configurado. En cuanto esté, te aviso para reservar." })
          } else {
            await safeSend(from,{ text:`¿Qué servicio quieres? Tengo: ${SERVICES_LIST.slice(0,6).join(", ")}${SERVICES_LIST.length>6?"…":""}.` })
          }
          return
        }
        if (!when) {
          await safeSend(from,{ text:"¿Qué día y a qué hora te viene bien? (ej: “15/08 10:00”)." })
          return
        }
      } catch(e){ console.error("messages.upsert:", e) }
    })

    // === Handlers botones
    async function onConfirmYes({ from, phone, session, safeSend }){
      try{
        const p = session.pending; if(!p) return
        if (p.action==="cancel") {
          try{
            const get = await square.bookingsApi.retrieveBooking(p.bookingId)
            const version = get?.result?.booking?.version
            if (version!==undefined) {
              await square.bookingsApi.cancelBooking(p.bookingId, { idempotencyKey:`cancel_${p.bookingId}_${version}_${Date.now()}`, bookingVersion:Number(version) })
              await safeSend(from,{ text:`He cancelado tu cita del ${fmtES(p.when)}.` })
            }
          }catch(e){ console.error("cancel:",e?.message||e) }
        }
        if (p.action==="edit") {
          try{
            const get = await square.bookingsApi.retrieveBooking(p.bookingId)
            const booking = get?.result?.booking
            const version = booking?.version
            const seg = booking?.appointmentSegments?.[0]
            const serviceVarId = seg?.serviceVariationId
            const servVersion = await getServiceVariationVersion(serviceVarId)
            const body = {
              idempotencyKey: stableKey({loc:LOCATION_ID, sv:serviceVarId, st:p.startEU.tz("UTC").toISOString(), cust:booking?.customerId||"c", tm:p.staffId||seg?.teamMemberId||"any"}),
              booking:{
                id:p.bookingId,
                version,
                locationId: LOCATION_ID,
                customerId: booking?.customerId,
                startAt: p.startEU.tz("UTC").toISOString(),
                appointmentSegments:[
                  {
                    teamMemberId: p.staffId||seg?.teamMemberId,
                    serviceVariationId: serviceVarId,
                    serviceVariationVersion: Number(servVersion||seg?.serviceVariationVersion||1),
                    durationMinutes: p.durationMin||seg?.durationMinutes||60
                  }
                ]
              }
            }
            const up = await square.bookingsApi.updateBooking(p.bookingId, body)
            if (up?.result?.booking?.id){
              await safeSend(from,{ text:`Cita actualizada. Nueva fecha: ${fmtES(p.startEU)}.` })
            }
          }catch(e){ console.error("edit:", e?.message||e) }
        }
        if (p.action==="book") {
          try{
            let customer = await squareFindCustomerByPhone(phone)
            if (!customer) customer = await squareCreateCustomer({ name: session.name, email: session.email, phone })
            if (!customer?.id) return
            const svId = p.service.variationId
            const svVer = await getServiceVariationVersion(svId)
            const startISO = p.startEU.tz("UTC").toISOString()
            const body = {
              idempotencyKey: stableKey({loc:LOCATION_ID, sv:svId, st:startISO, cust:customer.id, tm:p.staffId||"any"}),
              booking:{
                locationId: LOCATION_ID,
                customerId: customer.id,
                startAt: startISO,
                appointmentSegments:[
                  {
                    teamMemberId: p.staffId || TEAM_MEMBER_IDS[0] || "any",
                    serviceVariationId: svId,
                    serviceVariationVersion: Number(svVer||1),
                    durationMinutes: p.service.durationMin
                  }
                ]
              }
            }
            const r = await square.bookingsApi.createBooking(body)
            if (r?.result?.booking?.id){
              await safeSend(from,{ text:
                `Reserva confirmada.\nServicio: ${p.service.name}\nFecha: ${fmtES(p.startEU)}\nDuración: ${p.service.durationMin} min\nPago en persona.` })
            }
          }catch(e){ console.error("book:", e?.message||e) }
        }
      } finally { session.pending=null; session.mode=null }
    }
    async function onConfirmNo({ from, session, safeSend }){
      try{
        const p=session.pending; if(!p) return
        if (p.action==="cancel") await safeSend(from,{ text:"Vale, no cancelo. Si quieres, dime otra cosa." })
        if (p.action==="edit")   await safeSend(from,{ text:"Sin problema. Dime otra hora y lo intento." })
        if (p.action==="book")   await safeSend(from,{ text:"Ok. Dime otra hora o día y te digo." })
      } finally { session.pending=null; session.mode=null }
    }

  }catch(e){ console.error("startBot:", e) }
}
