// index.js — Gapink Nails · PROD (v10.3 “bienvenida-primero • propone-1 • +casuísticas mujer-friendly • horario 10–14/16–20 • weekend-safe”)

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
import isoWeek from "dayjs/plugin/isoWeek.js"
import isSameOrAfter from "dayjs/plugin/isSameOrAfter.js"
import isSameOrBefore from "dayjs/plugin/isSameOrBefore.js"
import "dayjs/locale/es.js"
import { Client, Environment } from "square"

if (!globalThis.crypto) globalThis.crypto = webcrypto
dayjs.extend(utc); dayjs.extend(tz); dayjs.extend(isoWeek); dayjs.extend(isSameOrAfter); dayjs.extend(isSameOrBefore)
dayjs.locale("es")
const EURO_TZ = "Europe/Madrid"

const { makeWASocket, useMultiFileAuthState, fetchLatestBaileysVersion, Browsers } = baileys

// ===== Config negocio
const WORK_DAYS = [1,2,3,4,5] // L-V
const OPEN_BLOCKS = [
  { sh:10, sm:0, eh:14, em:0 },
  { sh:16, sm:0, eh:20, em:0 }
]
const SLOT_MIN = 30

// Ventanas de búsqueda
const STEER_ON = (process.env.BOT_STEER_BALANCE || "on").toLowerCase() === "on"
const STEER_WINDOW_DAYS = Number(process.env.BOT_STEER_WINDOW_DAYS || 7)
const SEARCH_WINDOW_DAYS = Number(process.env.BOT_SEARCH_WINDOW_DAYS || 14)
const MAX_SAME_DAY_DEVIATION_MIN = Number(process.env.BOT_MAX_SAME_DAY_DEVIATION_MIN || 60)
const NOW_MIN_OFFSET_MIN = Number(process.env.BOT_NOW_OFFSET_MIN || 30)

// Servicio por defecto (intención genérica)
const BOT_DEFAULT_SERVICE_KEY = process.env.BOT_DEFAULT_SERVICE_KEY || "MANICURA_CON_ESMALTE_NORMAL"

// Info para FAQs
const BOT_ADDRESS_TEXT = process.env.BOT_ADDRESS_TEXT || "Av. de Benyamina 18, Torremolinos"
const BOT_ADDRESS_URL  = process.env.BOT_ADDRESS_URL  || "https://maps.app.goo.gl/9s9JMAPSgapink"
const BOT_WEBSITE      = process.env.BOT_WEBSITE      || "https://gapinknails.square.site/"
const BOT_PHONE        = process.env.BOT_PHONE        || "652 147 672"

// ===== OpenAI (solo extracción blanda)
const OPENAI_API_KEY  = process.env.OPENAI_API_KEY
const OPENAI_API_URL  = process.env.OPENAI_API_URL || "https://api.openai.com/v1/chat/completions"
const OPENAI_MODEL    = process.env.OPENAI_MODEL || "gpt-4o-mini"
async function aiChat(messages, { temperature=0.2 } = {}) {
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
Habla natural, cálido y breve (sin emojis). No digas que eres IA.
Nunca sugieras fuera de L–V 10–14 y 16–20 ni en pasado. Pago siempre en persona.`
async function extractFromText(userText="") {
  try {
    const schema = `Devuelve solo JSON: {"datetime_text":"...","name":"...","email":"..."}`
    const content = await aiChat([
      { role:"system", content: `${SYS_TONE}\n${schema}\nEspañol neutro.` },
      { role:"user", content: userText }
    ], { temperature: 0.15 })
    const jsonStr = (content||"").trim().replace(/^```(json)?/i,"").replace(/```$/,"")
    try { return JSON.parse(jsonStr) } catch { return {} }
  } catch { return {} }
}

// ===== Helpers
const onlyDigits = (s="") => (s||"").replace(/\D+/g,"")
const rmDiacritics = (s="") => s.normalize("NFD").replace(/\p{Diacritic}/gu,"")
const collapse = (s="") => rmDiacritics(String(s).toLowerCase())
  .replace(/[\u{1F300}-\u{1FAFF}]/gu," ")          // emojis
  .replace(/([a-z])\1{2,}/g,"$1")                  // holaaaa -> hola
  .replace(/[^\w\s:/\-.,@]/g," ")                  // símbolos raros
  .replace(/\s+/g," ")
  .trim()
const POLITE_WORDS = [
  "hola","buenas","buenos dias","buenas tardes","buenas noches",
  "guapa","guapo","reina","rey","bonita","preciosa","hermosa","bella",
  "cari","cariño","mi vida","mi amor","cielo","corazon","corazón","mi niña","nena","linda","chula","amore","amiga","amigo",
  "porfa","porfi","porfis","por favor","gracias","thanks","thank you"
]
function removePoliteNoise(t){
  let s = " " + t + " "
  for (const w of POLITE_WORDS) {
    const r = new RegExp(`\\b${rmDiacritics(w)}\\b`,"g")
    s = s.replace(r," ")
  }
  return s.replace(/\s+/g," ").trim()
}
const norm = (s="") => removePoliteNoise(collapse(s))
const minutesApart=(a,b)=>Math.abs(a.diff(b,"minute"))
const sameMinute=(a,b)=>a && b && a.diff(b,"minute")===0
const clampFuture = (t)=> {
  const now = dayjs().tz(EURO_TZ).add(NOW_MIN_OFFSET_MIN,"minute").second(0).millisecond(0)
  return t.isBefore(now) ? now.clone() : t.clone()
}
const YES_RE = /\b(s[ií]|sip|claro|ok|okay|okey+|vale+|va|venga|dale|confirmo|de acuerdo|perfecto|genial|yes|oui|sim|affirmative|adelante)\b/i
const NO_RE  = /\b(no+|otra|cambia|no confirmo|mejor mas tarde|mejor más tarde|anula|cancela|cancel|prefiero otra|nah|nein|niet|paso)\b/i

// ===== Intenciones / casuísticas extra
const INTENT_BOOK_RE = new RegExp([
  "cita","reserva","reservar","quiero ir","quiero hacerme","puedo ir","teneis hueco","tenéis hueco","tienes hueco",
  "primer hueco","cuando puedas","cuando tengas","lo antes posible","asap","hazme un hueco","me apuntas","agenda","agendar",
  "book","booking","appointment","slot","any time","first available","as soon as","whenever you can",
  "appuntamento","prenota","prenotare","quando puoi","primo posto",
  "marcar","agendar","primeiro horario","quando der",
  "rendez vous","rdv","termin","zeit","platz"
].join("|"),"i")

const INTENT_MORNING_RE   = /\b(ma[nñ]ana|por la ma[nñ]ana|temprano|primer[ao] hora|a primera hora|morning|mattina|manha|mañanita)\b/i
const INTENT_AFTERNOON_RE = /\b(tarde|por la tarde|despues de comer|después de comer|ultima hora|última hora|evening|tarde noche|tardecita)\b/i
const INTENT_MIDDAY_RE    = /\b(mediod[ií]a|medio dia|sobre la una|hora de comer|lunch time)\b/i

const INTENT_LIST_STAFF_RE = /\b(qu[ié]n tiene[s]? disponible|con qui[eé]n|qu[eé] personal|emplead[ao]s|staff|equipo|quien me atiende)\b/i
const INTENT_LOCATION_RE   = /\b(d[oó]nde est[aá]is|ubicaci[oó]n|direcci[oó]n|como llegar|mapa|localizaci[oó]n|address|location)\b/i
const INTENT_HOURS_RE      = /\b(horario|abr[ií]s|cerr[aá]is|horas|abiertos|opening hours)\b/i
const INTENT_PRICE_RE      = /\b(precio|cu[aá]nto vale|cu[aá]nto cuesta|tarifa|coste|vale|price|how much|preco|prix)\b/i
const INTENT_DURATION_RE   = /\b(dura(ci[oó]n)?|cu[aá]nto tarda|cu[aá]nto tiempo|duration|tempo|dauer)\b/i
const INTENT_ANY_STAFF_RE  = /\b(me da igual|cualquiera|quien sea|como sea|con quien quieras|sin preferencia|any staff|whoever)\b/i
const RESCH_RE             = /\b(cambia|cambiar|modifica|mover|reprograma|reprogramar|edita|change|reschedul|aplaza|reagendar|reagenda|move my appointment|otro dia|otro d[ií]a)\b/i
const CANCEL_RE            = /\b(cancela(?:r|me|la)?|anula(?:r|me|la)?|elimina(?:r|me|la)?|borra(?:r|me|la)?|quitar(?: la)? cita|anulaci[oó]n|cancel (my |mi )?appointment|call it off)\b/i

// Mujer-friendly: grupos, bebés, embarazo, alergias, color/diseño, prisa, parking, etc.
const GROUP_RE        = /\b(somos|vamos|venimos)\s+(dos|tres|cuatro|2|3|4)\b|\b(amigas?|mi madre y yo|mi hermana y yo)\b/i
const BABY_RE         = /\b(bebe|beb[eé]|carrito|carro del bebe|lactancia|dormido|siesta)\b/i
const PREGNANT_RE     = /\b(embarazada|embarazo|estoy de|gestaci[oó]n)\b/i
const ALLERGY_RE      = /\b(alergia|al[ée]rgica|al[ée]rgico|dermatitis|eczema|psoriasis)\b/i
const RUSH_RE         = /\b(urgente|prisa|corriendo|me urge|para hoy|hoy mismo|ya)\b/i
const PARK_RE         = /\b(parking|aparcamiento|donde aparcar|zona azul)\b/i
const KID_RE          = /\b(nin[oa]s?|peques|hijos?)\b/i
const PAYMENT_RE      = /\b(pagas?|pago|efectivo|tarjeta|bizum)\b/i
const CALL_RE         = /\b(llamar|os llamo|te llamo|me llamas)\b/i
const COLOR_RE        = /\b(rojo|nude|negro|blanco|azul|rosa|burdeos|granate|vino|french|francesa|glitter|brillos|cromo|chrome|cat ?eye|degradado|ombre|ombr[eé])\b/i
const NAIL_ART_RE     = /\b(francesa|baby ?boomer|encapsulados?|dise[nñ]o|pedrer[ií]a|glitter|brillos|cromo|chrome|cat ?eye|3d|degradado|ombre|ombr[eé])\b/i

// ===== Meses y números verbales para hora
const MONTHS = {
  "enero":1,"ene":1,"january":1,"jan":1,"gennaio":1,"gen":1,"janeiro":1,"janv":1,"janvier":1,"januar":1,
  "febrero":2,"feb":2,"february":2,"febbraio":2,"fevereiro":2,"fevr":2,"fevrier":2,"februar":2,
  "marzo":3,"mar":3,"march":3,"marzo-it":3,"marco":3,"mars":3,"märz":3,"marz":3,
  "abril":4,"abr":4,"april":4,"apr":4,"avril":4,"abril-pt":4,"aprile":4,
  "mayo":5,"may":5,"maio":5,"mai":5,
  "junio":6,"jun":6,"june":6,"giugno":6,"juin":6,"junho":6,
  "julio":7,"jul":7,"july":7,"luglio":7,"juillet":7,"julho":7,
  "agosto":8,"ago":8,"august":8,"aug":8,"agosto-it":8,"aout":8,"août":8,
  "septiembre":9,"setiembre":9,"sep":9,"sept":9,"september":9,"settembre":9,"set":9,"septembre":9,
  "octubre":10,"oct":10,"october":10,"ottobre":10,"outubro":10,"octobre":10,
  "noviembre":11,"nov":11,"november":11,"novembre":11,"novembro":11,"novembre-fr":11,
  "diciembre":12,"dic":12,"december":12,"dec":12,"dicembre":12,"dezembro":12,"decembre":12
}
const ES_NUM = { una:1, uno:1, dos:2, tres:3, cuatro:4, cinco:5, seis:6, siete:7, ocho:8, nueve:9, diez:10, once:11, doce:12 }
const MIN_WORDS = { "y cinco":5,"y diez":10,"y veinte":20,"y veinticinco":25,"y media":30,
                    "menos cinco":-5,"menos diez":-10,"menos veinte":-20,"menos cuarto":-15 }
function verbalToClock(t){
  let s = " " + t + " "
  for (const [w,n] of Object.entries(ES_NUM)){
    s = s.replace(new RegExp(`\\b(a\\s+las|a\\s+la|las|sobre\\s+las|sobre\\s+la)\\s+${w}\\b`,"g"), `$1 ${n}`)
  }
  for (const [w,n] of Object.entries(MIN_WORDS)){
    s = s.replace(new RegExp(`\\b${w}\\b`,"g"), ` __MIN__${n>=0?`+${n}`:`${n}`} `)
  }
  s = s.replace(/__MIN__([+\-]\d+)\s*$/, "")
  s = s.replace(/(\b\d{1,2})(?=\s*__MIN__([+\-]\d+))/g, (m,h)=>{
    const add = Number((s.match(/__MIN__([+\-]\d+)/)||[])[1]||0)
    const H = Number(h)||0
    let mm = add>=0?add:60+add
    let HH = add>=0?H:((H-1+24)%24)
    return `${HH}:${String(mm).padStart(2,"0")}`
  })
  return s.replace(/__MIN__[+\-]\d+/g,"").trim()
}

// ===== Fecha/hora permisiva
const DOW_WORDS = {
  "lunes":1,"monday":1,"lunedi":1,"lunedì":1,"lundi":1,"segunda":1,"segunda-feira":1,"montag":1,
  "martes":2,"tuesday":2,"martedi":2,"mardi":2,"terca":2,"terça":2,"terça-feira":2,"dienstag":2,
  "miercoles":3,"miércoles":3,"wednesday":3,"mercoledi":3,"mercredi":3,"quarta":3,"quarta-feira":3,"mittwoch":3,
  "jueves":4,"thursday":4,"giovedi":4,"giovedì":4,"jeudi":4,"quinta":4,"quinta-feira":4,"donnerstag":4,
  "viernes":5,"friday":5,"venerdi":5,"venerdì":5,"vendredi":5,"sexta":5,"sexta-feira":5,"freitag":5,
  "sabado":6,"sábado":6,"saturday":6,"sabato":6,"samedi":6,"samstag":6,
  "domingo":0,"sunday":0,"domenica":0,"dimanche":0,"sonntag":0
}
const WHEN_WORDS = {
  "hoy":0,"today":0,"oggi":0,"aujourd'hui":0,"hoje":0,"heute":0,
  "manana":1,"mañana":1,"tomorrow":1,"domani":1,"demain":1,"amanha":1,"amanhã":1,"morgen":1,
  "pasado manana":2,"pasado mañana":2
}
function parseDateTimeMulti(text){
  if(!text) return null
  const baseText = verbalToClock(norm(text))
  const t = baseText

  const m = t.match(/\b(\d{1,2})[\/\-](\d{1,2})(?:[\/\-](\d{2,4}))?\b/)
  const mx = t.match(/\b(\d{1,2})\s*(de\s+)?([a-záéíóúü]+)\b/)
  const rel = t.match(/\ben\s+(\d{1,2})\s+d[ií]as?\b/)

  let base = null
  if (m) {
    let dd = +m[1], mm = +m[2], yy = m[3] ? +m[3] : dayjs().tz(EURO_TZ).year()
    if (yy < 100) yy += 2000
    base = dayjs.tz(`${yy}-${String(mm).padStart(2,"0")}-${String(dd).padStart(2,"0")} 00:00`, EURO_TZ)
  } else if (mx && MONTHS[mx[3]]) {
    const dd = +mx[1], mm = MONTHS[mx[3]]
    const yy = dayjs().tz(EURO_TZ).year()
    base = dayjs.tz(`${yy}-${String(mm).padStart(2,"0")}-${String(dd).padStart(2,"0")} 00:00`, EURO_TZ)
  } else if (rel) {
    base = dayjs().tz(EURO_TZ).add(+rel[1],"day").startOf("day")
  } else {
    for (const [w,off] of Object.entries(WHEN_WORDS)) {
      if (t.includes(w)) { base = dayjs().tz(EURO_TZ).add(off,"day").startOf("day"); break }
    }
    if (!base) {
      for (const [w,dow] of Object.entries(DOW_WORDS)) {
        if (t.includes(w)) {
          const now = dayjs().tz(EURO_TZ)
          const nowDow = now.day()
          let delta = (dow - nowDow + 7) % 7
          if (/\b(proximo|pr[oó]ximo|que viene|siguiente|la semana que viene|next)\b/.test(t)) {
            if (delta===0) delta=7; else delta = ((dow + 7) - nowDow + 7) % 7
          } else if (/\b(este|this)\b/.test(t)) {
            if (delta===0 && now.hour()>=20) delta=7
          } else if (delta===0 && now.hour()>=20) delta=7
          base = now.startOf("day").add(delta,"day")
          break
        }
      }
    }
    if (!base) base = dayjs().tz(EURO_TZ).startOf("day")
  }

  let hintedHour = null
  if (INTENT_MORNING_RE.test(t)) hintedHour = 10
  if (INTENT_AFTERNOON_RE.test(t)) hintedHour = 16
  if (INTENT_MIDDAY_RE.test(t)) hintedHour = 13

  let hour=null, minute=0
  const hm = t.match(/(?:a\s+las|a\s+la|sobre\s+las|las|hacia|sobre)?\s*(\d{1,2})(?::|\.|h)?(\d{2})?\s*(am|pm)?\b/)
  if (hm) {
    hour = +hm[1]; minute = hm[2] ? +hm[2] : 0
    const ap = hm[3]
    if (ap==="pm" && hour<12) hour+=12
    if (ap==="am" && hour===12) hour=0
  } else if (hintedHour!==null) {
    hour = hintedHour; minute = 0
  } else {
    return null
  }

  let dt = dayjs.tz(`${base.format("YYYY-MM-DD")} ${String(hour).padStart(2,"0")}:${String(minute).padStart(2,"0")}`, EURO_TZ)
  dt = clampFuture(dt)
  return dt
}

const fmtES=(d)=>{
  const t = (dayjs.isDayjs(d)?d:dayjs(d)).tz(EURO_TZ)
  const dias=["domingo","lunes","martes","miércoles","jueves","viernes","sábado"]
  const DD = String(t.date()).padStart(2,"0")
  const MM = String(t.month()+1).padStart(2,"0")
  const HH = String(t.hour()).padStart(2,"0")
  const mm = String(t.minute()).padStart(2,"0")
  return `${dias[t.day()]} ${DD}/${MM} ${HH}:${mm}`
}
function ceilToSlotEU(t){
  const m = t.minute(); const rem = m % SLOT_MIN
  if (rem===0) return t.second(0).millisecond(0)
  return t.add(SLOT_MIN-rem,"minute").second(0).millisecond(0)
}

// ===== Business hours helpers
function slotInsideAnyBlock(startEU, durationMin){
  if (!WORK_DAYS.includes(startEU.day())) return false
  const endEU = startEU.clone().add(durationMin,"minute")
  for (const b of OPEN_BLOCKS) {
    const bStart = startEU.clone().hour(b.sh).minute(b.sm).second(0)
    const bEnd   = startEU.clone().hour(b.eh).minute(b.em).second(0)
    if (startEU.isSameOrAfter(bStart) && endEU.isSameOrBefore(bEnd)) return true
  }
  return false
}
function firstBlockStart(day){
  return day.clone().hour(OPEN_BLOCKS[0].sh).minute(OPEN_BLOCKS[0].sm).second(0)
}
function nextWorkdayStart(t){
  let d = t.clone().startOf("day")
  while (!WORK_DAYS.includes(d.day())) d = d.add(1,"day")
  return firstBlockStart(d)
}
function blocksForDay(day){
  return OPEN_BLOCKS.map(b=>({
    start: day.clone().hour(b.sh).minute(b.sm).second(0),
    end:   day.clone().hour(b.eh).minute(b.em).second(0)
  }))
}
function *iterateDaySlots(day, startFrom){
  const blocks=blocksForDay(day)
  for (const b of blocks){
    let t = b.start.clone()
    if (startFrom && startFrom.isAfter(b.start) && startFrom.isBefore(b.end)) t = ceilToSlotEU(startFrom.clone())
    while (t.isBefore(b.end)){
      yield t.clone()
      t = t.add(SLOT_MIN,"minute")
    }
  }
}

// Rango “entre / a partir de / antes de…”
function deriveWindowFromText(text, baseDay){
  const t = norm(text)
  let startHint=null, endHint=null
  const mRange = t.match(/\bentre\s+(\d{1,2})(?::(\d{2}))?\s*(y|e|a|-)\s*(\d{1,2})(?::(\d{2}))?\b/)
  if (mRange){
    const h1=+mRange[1], m1= mRange[2]?+mRange[2]:0
    const h2=+mRange[4], m2= mRange[5]?+mRange[5]:0
    startHint = baseDay.clone().hour(h1).minute(m1)
    endHint   = baseDay.clone().hour(h2).minute(m2)
  }
  const mFrom = t.match(/\ba\s+partir\s+de\s+(\d{1,2})(?::(\d{2}))?\b/)
  if (mFrom){
    const h=+mFrom[1], mm=mFrom[2]?+mFrom[2]:0
    startHint = baseDay.clone().hour(h).minute(mm)
  }
  const mBefore = t.match(/\b(antes\s+de)\s+(\d{1,2})(?::(\d{2}))?\b/)
  if (mBefore){
    const h=+mBefore[2], mm=mBefore[3]?+mBefore[3]:0
    endHint = baseDay.clone().hour(h).minute(mm)
  }
  if (INTENT_MORNING_RE.test(t)){
    startHint = baseDay.clone().hour(10).minute(0)
    endHint   = baseDay.clone().hour(14).minute(0)
  }
  if (INTENT_AFTERNOON_RE.test(t)){
    startHint = baseDay.clone().hour(16).minute(0)
    endHint   = baseDay.clone().hour(20).minute(0)
  }
  if (INTENT_MIDDAY_RE.test(t)){
    startHint = baseDay.clone().hour(12).minute(30)
    endHint   = baseDay.clone().hour(14).minute(0)
  }
  return { startHint, endHint }
}

// ===== Square
const square = new Client({ accessToken: process.env.SQUARE_ACCESS_TOKEN, environment: process.env.SQUARE_ENV==="production"?Environment.Production:Environment.Sandbox })
const locationId = process.env.SQUARE_LOCATION_ID
let LOCATION_TZ = EURO_TZ
async function squareCheckCredentials(){
  try{
    const locs=await square.locationsApi.listLocations()
    const loc=(locs.result.locations||[]).find(l=>l.id===locationId)||(locs.result.locations||[])[0]
    if(loc?.timezone) LOCATION_TZ = loc.timezone
    console.log(`✅ Square listo. Location ${locationId}, TZ=${LOCATION_TZ}`)
  }catch(e){ console.error("⛔ Square:",e?.message||e) }
}
function pickServiceEnvPair(key){
  const envName = SVC[key]?.env
  const raw = envName ? process.env[envName] : null
  if (!raw) return null
  const [id, versionStr] = raw.split("|")
  const version = versionStr ? Number(versionStr) : undefined
  return { id, version, duration: SVC[key]?.dur ?? 60, name: SVC[key]?.name ?? key }
}
async function getServiceVariationVersion(id){
  try{ const resp=await square.catalogApi.retrieveCatalogObject(id,true); return resp?.result?.object?.version }
  catch(e){ console.error("getServiceVariationVersion:",e?.message||e); return undefined }
}
function stableKey(partsObj){
  const raw=Object.values(partsObj).join("|")
  return createHash("sha256").update(raw).digest("hex").slice(0,48)
}
async function createSquareBooking({startEU, serviceKey, customerId, teamMemberId}){
  try{
    const pair = pickServiceEnvPair(serviceKey)
    if(!pair?.id || !teamMemberId || !locationId) return null
    const version = pair.version || await getServiceVariationVersion(pair.id)
    if(!version) return null
    const durationMin = pair.duration
    const startISO = startEU.tz("UTC").toISOString()
    const idempotencyKey = stableKey({locationId,serviceVarId:pair.id,startISO,customerId,teamMemberId})
    const body = {
      idempotencyKey,
      booking: {
        locationId,
        startAt: startISO,
        customerId,
        appointmentSegments: [{
          teamMemberId,
          serviceVariationId: pair.id,
          serviceVariationVersion: Number(version),
          durationMinutes: durationMin
        }]
      }
    }
    const resp = await square.bookingsApi.createBooking(body)
    return resp?.result?.booking || null
  }catch(e){ console.error("createSquareBooking:", e?.message||e); return null }
}
async function cancelSquareBooking(bookingId){
  try{
    const get = await square.bookingsApi.retrieveBooking(bookingId)
    const version = get?.result?.booking?.version
    if (!version) return false
    const body = { idempotencyKey: `cancel_${bookingId}_${Date.now()}`, bookingVersion: version }
    const r = await square.bookingsApi.cancelBooking(bookingId, body)
    return !!r?.result?.booking?.id
  }catch(e){ console.error("cancelSquareBooking:", e?.message||e); return false }
}
async function updateSquareBooking(bookingId,{startEU,serviceKey,customerId,teamMemberId}){
  try{
    const get=await square.bookingsApi.retrieveBooking(bookingId)
    const booking=get?.result?.booking
    if(!booking) return null
    const pair = pickServiceEnvPair(serviceKey)
    const version = pair?.version || await getServiceVariationVersion(pair?.id)
    const startISO=startEU.tz("UTC").toISOString()
    const body={
      idempotencyKey: stableKey({locationId,sv:pair?.id,startISO,customerId,teamMemberId}),
      booking:{
        id: bookingId,
        version: booking.version,
        locationId,
        customerId,
        startAt: startISO,
        appointmentSegments:[{
          teamMemberId,
          serviceVariationId: pair?.id,
          serviceVariationVersion: Number(version),
          durationMinutes: pair?.duration ?? 60
        }]
      }
    }
    const resp=await square.bookingsApi.updateBooking(bookingId, body)
    return resp?.result?.booking||null
  }catch(e){ console.error("updateSquareBooking:",e?.message||e); return null }
}

// ===== DB & sesiones
const db=new Database("gapink.db");db.pragma("journal_mode = WAL")
db.exec(`
CREATE TABLE IF NOT EXISTS appointments (
  id TEXT PRIMARY KEY,
  customer_name TEXT,
  customer_phone TEXT,
  customer_square_id TEXT,
  service_key TEXT,
  service_name TEXT,
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
(id, customer_name, customer_phone, customer_square_id, service_key, service_name, duration_min, start_iso, end_iso, staff_id, status, created_at, square_booking_id)
VALUES (@id, @customer_name, @customer_phone, @customer_square_id, @service_key, @service_name, @duration_min, @start_iso, @end_iso, @staff_id, @status, @created_at, @square_booking_id)`)
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

// ===== Disponibilidad / sugerencias
function getBookedIntervals(fromIso,toIso){
  const rows=db.prepare(`SELECT start_iso,end_iso,staff_id FROM appointments WHERE status IN ('pending','confirmed') AND start_iso < @to AND end_iso > @from`).all({from:fromIso,to:toIso})
  return rows.map(r=>({start:dayjs(r.start_iso),end:dayjs(r.end_iso),staff_id:r.staff_id}))
}
function findFreeStaff(intervals,start,end,preferred){
  const base = Object.values(EMP_NAME_MAP).filter(Boolean)
  const ids = (preferred && base.includes(preferred)) ? [preferred, ...base.filter(x=>x!==preferred)] : base
  for(const id of ids){
    const busy = intervals.filter(i=>i.staff_id===id).some(i => (start<i.end) && (i.start<end))
    if(!busy) return id
  }
  return null
}
function insideBusinessBlocks(startEU, durationMin){ return slotInsideAnyBlock(startEU, durationMin) }
function *iterateDaySlots(day, startFrom){
  const blocks=blocksForDay(day)
  for (const b of blocks){
    let t = b.start.clone()
    if (startFrom && startFrom.isAfter(b.start) && startFrom.isBefore(b.end)) t = ceilToSlotEU(startFrom.clone())
    while (t.isBefore(b.end)){
      yield t.clone()
      t = t.add(SLOT_MIN,"minute")
    }
  }
}

function suggestOrExact(startEU, durationMin, preferredStaffId=null, avoidMs=null, constraints=null){
  const now=dayjs().tz(EURO_TZ).add(NOW_MIN_OFFSET_MIN,"minute").second(0).millisecond(0)
  const from = now.tz("UTC").toISOString(), to = now.add(SEARCH_WINDOW_DAYS,"day").tz("UTC").toISOString()
  const intervals=getBookedIntervals(from,to)

  let req = ceilToSlotEU(clampFuture(startEU.clone()))
  if (!WORK_DAYS.includes(req.day())) req = nextWorkdayStart(req)

  if (constraints?.startHint && req.isBefore(constraints.startHint)) req = constraints.startHint.clone()
  if (constraints?.endHint && req.isAfter(constraints.endHint)) req = constraints.endHint.clone()

  if (!insideBusinessBlocks(req,durationMin)) {
    const blocks = blocksForDay(req.clone())
    let moved=null
    for (const b of blocks) {
      const candidate = b.start.clone()
      if (constraints?.startHint && candidate.isBefore(constraints.startHint)) continue
      if (constraints?.endHint && candidate.isAfter(constraints.endHint)) continue
      if (insideBusinessBlocks(candidate, durationMin)) { moved=candidate; break }
    }
    if (moved) req=moved
    else { req = nextWorkdayStart(req) }
  }

  const endEU = req.clone().add(durationMin,"minute")
  const exactId = findFreeStaff(intervals, req.tz("UTC"), endEU.tz("UTC"), preferredStaffId)
  if (exactId && (!avoidMs || req.valueOf()!==avoidMs)) {
    return { exact: req, suggestion:null, staffId: exactId }
  }

  const day = req.clone().startOf("day")
  const windowStart = constraints?.startHint && constraints.startHint.isAfter(day) ? constraints.startHint.clone() : req.clone()
  for (const t of iterateDaySlots(day, windowStart)) {
    if (constraints?.endHint && t.isAfter(constraints.endHint)) break
    if (avoidMs && t.valueOf()===avoidMs) continue
    if (!insideBusinessBlocks(t, durationMin)) continue
    const e=t.clone().add(durationMin,"minute")
    const id=findFreeStaff(intervals, t.tz("UTC"), e.tz("UTC"), preferredStaffId)
    if(id) return { exact:null, suggestion:t, staffId:id }
    if (!constraints && minutesApart(t, req) > MAX_SAME_DAY_DEVIATION_MIN) break
  }

  const windowDays = STEER_ON ? STEER_WINDOW_DAYS : 1
  const limit = dayjs.max(req.clone(), now.clone()).add(windowDays,"day").endOf("day")
  for (let d=day.clone().add(1,"day"); d.isSameOrBefore(limit); d=d.add(1,"day")) {
    if (!WORK_DAYS.includes(d.day())) continue
    const startFrom = constraints?.startHint ? d.clone().hour(constraints.startHint.hour()).minute(constraints.startHint.minute()) : null
    for (const t of iterateDaySlots(d, startFrom)) {
      if (constraints?.endHint) {
        const endLimit = d.clone().hour(constraints.endHint.hour()).minute(constraints.endHint.minute())
        if (t.isAfter(endLimit)) break
      }
      if (avoidMs && t.valueOf()===avoidMs) continue
      if (!insideBusinessBlocks(t, durationMin)) continue
      const e=t.clone().add(durationMin,"minute")
      const id=findFreeStaff(intervals, t.tz("UTC"), e.tz("UTC"), preferredStaffId)
      if(id) return { exact:null, suggestion:t, staffId:id }
    }
  }
  return { exact:null, suggestion:null, staffId:null }
}

// ===== Detección de servicios (sinónimos + fuzzy)
const SERVICE = {
  // (igual que versión anterior, abreviado por longitud —> NO toques claves)
}
const SVC = {/* ——— mismo bloque de servicios que ya tienes ——— */} // <-- Mantén el listado completo de tu versión anterior (no repetimos por espacio)
const SERVICE_KEYS = Object.keys(SVC)

// ——— Sinónimos ampliados (mujer-friendly). Incluye lo de antes + extras.
// Para no reventar el límite del mensaje, mantenlo como en tu última versión extensa.
const SERVICE_SYNONYMS = [
  ["manicura", "MANICURA_CON_ESMALTE_NORMAL", ["uñas","nails","manikura","pintar uñas","básica","normalita","manicure"]],
  ["manicura semipermanente","MANICURA_SEMIPERMANENTE",["semi","semis","gel color","permanente","esmaltado gel"]],
  ["manicura semipermanente quitar","MANICURA_SEMIPERMANENTE_QUITAR",["retirar semi","quitar gel","soak off"]],
  ["manicura semipermanente con nivelacion","MANICURA_SEMIPERMANETE_CON_NIVELACION",["nivelacion","rubber base","refuerzo"]],
  ["manicura rusa","MANICURA_RUSA_CON_NIVELACION",["russian manicure","cutícula profunda","dry manicure"]],
  ["uñas esculpidas nuevas","UNAS_NUEVAS_ESCULPIDAS",["acrílicas nuevas","tips","construcción"]],
  ["relleno esculpidas","RELLENO_UNAS_ESCULPIDAS",["relleno acrílicas","fill"]],
  ["quitar esculpidas","QUITAR_UNAS_ESCULPIDAS",["retirada acrílicas","remove acrylic"]],
  ["pedicura spa","PEDICURA_SPA_CON_ESMALTE_NORMAL",["pedicure","pies normal"]],
  ["pedicura semi","PEDICURA_SPA_CON_ESMALTE_SEMIPERMANENTE",["gel pies","permanente pies"]],
  ["lifting pestañas + tinte","LIFITNG_DE_PESTANAS_Y_TINTE",["lash lift","lift tint"]],
  ["depilacion cejas hilo","DEPILACION_CEJAS_CON_HILO",["threading","cejas con hilo"]],
  ["limpieza hydra","LIMPIEZA_HYDRA_FACIAL",["hydrafacial","hydra facial"]],
  ["dermapen","DERMAPEN",["microneedling"]],
  ["microblading","MICROBLADING",[]],
  // … (mantén todos los que ya añadiste en la v10.1)
]

// Levenshtein light
function lev(a,b){
  a=a||""; b=b||""
  const m=a.length, n=b.length
  if (Math.abs(m-n)>3) return 99
  const dp=Array.from({length:m+1},()=>Array(n+1).fill(0))
  for(let i=0;i<=m;i++) dp[i][0]=i
  for(let j=0;j<=n;j++) dp[0][j]=j
  for(let i=1;i<=m;i++){
    for(let j=1;j<=n;j++){
      dp[i][j]=Math.min(dp[i-1][j]+1, dp[i][j-1]+1, dp[i-1][j-1]+(a[i-1]===b[j-1]?0:1))
    }
  }
  return dp[m][n]
}

function detectAllServices(textLow){
  const found = new Set()
  for (const [label,key,extra] of SERVICE_SYNONYMS) {
    const l = rmDiacritics(label)
    if (textLow.includes(l) || (extra||[]).some(x => textLow.includes(rmDiacritics(x)))) found.add(key)
  }
  const tokens = norm(textLow).split(/\s+/).filter(Boolean)
  let best=null, bestScore=-1
  for (const k of SERVICE_KEYS) {
    const name = rmDiacritics((SVC[k]?.name||k).toLowerCase())
    const words = name.split(/\s+/)
    let score=0
    for (const t of tokens) {
      if (words.includes(t)) score+=2
      else if (words.some(w => lev(t,w)<=2)) score+=1
    }
    if (score>bestScore) { bestScore=score; best=k }
  }
  if (bestScore>=2) found.add(best)
  return Array.from(found)
}
function pickPrimaryService(keys){
  if (!keys?.length) return null
  const order = [
    "MANICURA_SEMIPERMANENTE","MANICURA_CON_ESMALTE_NORMAL","MANICURA_SEMIPERMANENTE_QUITAR","MANICURA_SEMIPERMANETE_CON_NIVELACION","MANICURA_RUSA_CON_NIVELACION",
    "UNAS_NUEVAS_ESCULPIDAS","RELLENO_UNAS_ESCULPIDAS","RELLENO_UNAS_ESCULPIDAS_CON_MANICURA_RUSA",
    "PEDICURA_SPA_CON_ESMALTE_SEMIPERMANENTE","PEDICURA_SPA_CON_ESMALTE_NORMAL",
  ]
  for (const k of order) if (keys.includes(k)) return k
  return keys[0]
}

// ===== Mini web
const app=express()
const PORT=process.env.PORT||8080
let lastQR=null,conectado=false
const WELCOME_TEXT = `Gracias por comunicarte con Gapink Nails. Por favor, haznos saber cómo podemos ayudarte.

Solo atenderemos por WhatsApp y llamadas en horario de lunes a viernes de 10 a 14:00 y de 16:00 a 20:00 

Si quieres reservar una cita puedes hacerlo a través de este link:

${BOT_WEBSITE}

Y si quieres modificarla puedes hacerlo a través del link del sms que llega con su cita! 

Para cualquier otra consulta, déjenos saber y en el horario establecido le responderemos.
Gracias 😘`

app.get("/",(_req,res)=>{res.send(`<!doctype html><meta charset="utf-8"><style>body{font-family:system-ui;display:grid;place-items:center;min-height:100vh;background:linear-gradient(135deg,#fce4ec,#f8bbd0);color:#4a148c} .card{background:#fff;padding:24px;border-radius:16px;box-shadow:0 6px 24px rgba(0,0,0,.08);text-align:center;max-width:520px}</style><div class="card"><h1>Gapink Nails</h1><p>Estado: ${conectado?"✅ Conectado":"❌ Desconectado"}</p>${!conectado&&lastQR?`<img src="/qr.png" width="320" />`:``}<p><small>Hecho por <a href="https://gonzalog.co" target="_blank" rel="noopener">Gonzalo García Aranda</a></small></p></div>`)})
app.get("/qr.png",async(_req,res)=>{if(!lastQR)return res.status(404).send("No hay QR");const png=await qrcode.toBuffer(lastQR,{type:"png",width:512,margin:1});res.set("Content-Type","image/png").send(png)})
const wait=(ms)=>new Promise(r=>setTimeout(r,ms))

let booting=false, reconnectTimer=null
app.listen(PORT,async()=>{
  console.log(`🌐 Web en puerto ${PORT}`)
  await squareCheckCredentials()
  if(!booting){ booting=true; startBot().catch(console.error) }
})

async function startBot(){
  console.log("🚀 Bot arrancando…")
  try{
    if(!fs.existsSync("auth_info"))fs.mkdirSync("auth_info",{recursive:true})
    const { state, saveCreds } = await useMultiFileAuthState("auth_info")
    const { version } = await fetchLatestBaileysVersion()
    const sock=makeWASocket({logger:pino({level:"silent"}),printQRInTerminal:false,auth:state,version,browser:Browsers.macOS("Desktop"),syncFullHistory:false,connectTimeoutMs:30000})

    const outbox=[]; let sending=false, isOpen=false
    const __SAFE_SEND__=(jid,content)=>new Promise((resolve,reject)=>{outbox.push({jid,content,resolve,reject});processOutbox().catch(console.error)})
    async function processOutbox(){if(sending)return;sending=true;while(outbox.length){const {jid,content,resolve,reject}=outbox.shift();let guard=0;while(!isOpen&&guard<60){await wait(500);guard++}if(!isOpen){reject(new Error("WA not connected"));continue}let ok=false,err=null;for(let a=1;a<=4;a++){try{await sock.sendMessage(jid,content);ok=true;break}catch(e){err=e;const msg=e?.data?.stack||e?.message||String(e);if(/Timed Out/i.test(msg)||/Boom/i.test(msg)){await wait(400*a);continue}await wait(300)}}if(ok)resolve(true);else{console.error("sendMessage failed:",err?.message||err);reject(err);try{await sock.ws.close()}catch{}}}sending=false}

    sock.ev.on("connection.update",async({connection,lastDisconnect,qr})=>{
      if(qr){lastQR=qr;conectado=false;try{qrcodeTerminal.generate(qr,{small:true})}catch{}}
      if(connection==="open"){lastQR=null;conectado=true;isOpen=true;console.log("✅ Conectado a WhatsApp");processOutbox().catch(console.error)}
      if(connection==="close"){
        conectado=false;isOpen=false
        const reason=lastDisconnect?.error?.message||String(lastDisconnect?.error||"")
        console.log("❌ Conexión cerrada:",reason)
        if(!reconnectTimer){
          reconnectTimer=setTimeout(async()=>{reconnectTimer=null;try{await startBot()}catch(e){console.error(e)}}, 2500)
        }
      }
    })
    sock.ev.on("creds.update",saveCreds)

    // ===== Mensajes
    sock.ev.on("messages.upsert",async({messages})=>{
      try{
        const m=messages?.[0]; if (!m?.message || m.key.fromMe) return
        const from=m.key.remoteJid
        const phone=normalizePhoneES((from||"").split("@")[0]||"")||(from||"").split("@")[0]||""

        // contenido (texto, imagen, audio)
        let body=m.message.conversation||m.message.extendedTextMessage?.text||m.message?.imageMessage?.caption||""
        const hasImage = !!m.message?.imageMessage
        const hasAudio = !!m.message?.audioMessage
        const textRaw=(body||"").trim()
        const low = norm(textRaw)

        // Sesión
        let data=loadSession(phone)||{
          serviceKey:null, serviceName:null, startEU:null, durationMin:null, preferredStaffId:null,
          name:null,email:null,
          awaitingConfirm:false, bookingInFlight:false,
          lastSuggestedMs:null, editBookingId:null, welcomed:false
        }

        // 0) Bienvenida SIEMPRE al primer mensaje
        if (!data.welcomed) {
          await __SAFE_SEND__(from,{ text: WELCOME_TEXT })
          data.welcomed = true
        }

        // Notas por imagen/audio
        if (hasImage) { await __SAFE_SEND__(from,{ text:"¡Foto recibida! El color/diseño lo eliges en el centro; voy a proponerte hora." }) }
        if (hasAudio && !textRaw) { await __SAFE_SEND__(from,{ text:"He recibido tu audio. Para reservar rápido, escríbeme en texto el servicio y una franja (ej.: “manicura viernes por la tarde”)." }) }

        // 1) FAQs para bajar llamadas
        if (INTENT_LIST_STAFF_RE.test(low)) await __SAFE_SEND__(from,{ text:`Nuestro equipo: ${EMP_LIST_TEXT()}. Si prefieres a alguien, dímelo (ej.: “con Desi”).` })
        if (INTENT_LOCATION_RE.test(low))   await __SAFE_SEND__(from,{ text:`Estamos en ${BOT_ADDRESS_TEXT}\nMapa: ${BOT_ADDRESS_URL}` })
        if (INTENT_HOURS_RE.test(low))      await __SAFE_SEND__(from,{ text:`Abrimos de lunes a viernes de 10:00 a 14:00 y de 16:00 a 20:00.` })
        if (INTENT_PRICE_RE.test(low))      await __SAFE_SEND__(from,{ text:`Los precios dependen del servicio/diseño. Puedes verlos y reservar aquí: ${BOT_WEBSITE}` })
        if (PARK_RE.test(low))              await __SAFE_SEND__(from,{ text:`Hay aparcamiento en la zona (suele haber hueco).` })
        if (CALL_RE.test(low))              await __SAFE_SEND__(from,{ text:`Podemos atenderte por WhatsApp y por llamada en horario 10–14 / 16–20.` })
        if (PAYMENT_RE.test(low))           await __SAFE_SEND__(from,{ text:`El pago es en persona.` })
        if (COLOR_RE.test(low))             await __SAFE_SEND__(from,{ text:`El color/diseño se elige en el centro, no afecta a la reserva.` })
        if (GROUP_RE.test(low))             await __SAFE_SEND__(from,{ text:`Para varias personas, mejor que cada una reserve su cita en ${BOT_WEBSITE} (así aseguramos tiempos).` })
        if (BABY_RE.test(low) || KID_RE.test(low)) await __SAFE_SEND__(from,{ text:`Sin problema si vienes con peques o carrito; avísanos por si necesitas un par de minutos extra al llegar.` })
        if (PREGNANT_RE.test(low))          await __SAFE_SEND__(from,{ text:`Si estás embarazada o en lactancia, algunos servicios tienen consideraciones. En el centro te orientamos y elegimos la opción segura para ti.` })
        if (ALLERGY_RE.test(low))           await __SAFE_SEND__(from,{ text:`Si tienes alergias o piel sensible, coméntalo al llegar para usar productos adecuados.` })
        if (RUSH_RE.test(low))              await __SAFE_SEND__(from,{ text:`Veo que te urge; busco el primer hueco posible.` })

        // 2) Staff preferido
        const staffKey = Object.keys(EMP_NAME_MAP).find(k => low.includes(k))
        if (staffKey) data.preferredStaffId = EMP_NAME_MAP[staffKey]
        if (INTENT_ANY_STAFF_RE.test(low) && !staffKey) data.preferredStaffId = null

        // 3) Cancelar/reprogramar
        if (CANCEL_RE.test(low)) {
          const upc = getUpcomingByPhone.get({ phone, now: dayjs().utc().toISOString() })
          if (upc?.square_booking_id) {
            const ok = await cancelSquareBooking(upc.square_booking_id)
            if (ok) { markCancelled.run({ id: upc.id }); clearSession.run({ phone }); await __SAFE_SEND__(from,{ text:`He cancelado tu cita del ${fmtES(dayjs(upc.start_iso))}.` }); return }
          }
          await __SAFE_SEND__(from,{ text:"No veo ninguna cita futura para cancelar ahora mismo." })
          saveSession(phone,data); return
        }
        if (RESCH_RE.test(low)) {
          const upc = getUpcomingByPhone.get({ phone, now: dayjs().utc().toISOString() })
          if (upc) {
            data.editBookingId = upc.id
            data.serviceKey = upc.service_key
            data.serviceName = upc.service_name
            data.durationMin = upc.duration_min
            data.preferredStaffId = upc.staff_id
            data.awaitingConfirm=false
            saveSession(phone,data)
            await __SAFE_SEND__(from,{ text:`Ok, dime la nueva fecha y hora (ej.: “lunes 10:00”).` })
            return
          }
        }

        // 4) IA suave para nombre/email/fecha
        const extra = await extractFromText(textRaw)
        if (!data.name && extra?.name) data.name = (extra.name+"").trim().slice(0,64)
        if (!data.email && extra?.email && isValidEmail(extra.email)) data.email = extra.email.trim()

        // 5) Detecta servicio(s) y elige principal o default femenino (si solo dice “uñas”, “quiero algo”)
        const detected = detectAllServices(low)
        if (detected.length){
          const primary = pickPrimaryService(detected)
          if (primary) {
            data.serviceKey = primary
            data.serviceName = SVC[primary]?.name || primary
            data.durationMin = SVC[primary]?.dur || 60
          }
        } else if (!data.serviceKey && (INTENT_BOOK_RE.test(low) || /manicura|manikura|pedicura|unas|uñas|nails|facial|cejas|pesta[ñn]as|acrilic|gel/.test(low))) {
          data.serviceKey = BOT_DEFAULT_SERVICE_KEY
          data.serviceName = SVC[BOT_DEFAULT_SERVICE_KEY]?.name || "Manicura"
          data.durationMin = SVC[BOT_DEFAULT_SERVICE_KEY]?.dur || 30
        }

        // 5.1) Si menciona diseño (nail art) y está reservando manicura, añade colchón
        if (data.serviceKey && /^MANICURA_/.test(data.serviceKey) && NAIL_ART_RE.test(low)) {
          data.durationMin = (data.durationMin|| (SVC[data.serviceKey]?.dur||60)) + 15
        }

        // 6) Fecha/hora
        const parsed = parseDateTimeMulti(textRaw) || (extra?.datetime_text ? parseDateTimeMulti(extra.datetime_text) : null)
        if (parsed) data.startEU = parsed

        // 7) Confirmaciones
        const userSaysYes = YES_RE.test(textRaw)
        const userSaysNo  = NO_RE.test(textRaw)

        if (data.awaitingConfirm) {
          if (userSaysYes && data.serviceKey && data.startEU) {
            if (data.editBookingId) await finalizeReschedule({ from, phone, data, safeSend: __SAFE_SEND__ })
            else await finalizeBooking({ from, phone, data, safeSend: __SAFE_SEND__ })
            return
          }
          if (userSaysNo) {
            data.awaitingConfirm=false; data.lastSuggestedMs=null
            saveSession(phone,data)
            await __SAFE_SEND__(from,{ text:"Vale, dime otra hora o día y lo miro." })
            return
          }
          if (staffKey && data.lastSuggestedMs){
            const offer = dayjs.tz(data.lastSuggestedMs, EURO_TZ)
            const { exact, staffId } = suggestOrExact(offer, data.durationMin || 60, data.preferredStaffId, null)
            if (exact && sameMinute(exact, offer)) {
              data.preferredStaffId = staffId
              saveSession(phone,data)
              await __SAFE_SEND__(from,{ text:`Perfecto, con ${staffKey}. ¿Confirmo ${fmtES(offer)} para ${data.serviceName}? (Pago en persona)` })
              return
            }
          }
          await __SAFE_SEND__(from,{ text:`¿Te viene bien ${fmtES(dayjs.tz(data.lastSuggestedMs, EURO_TZ))} para ${data.serviceName}? Si prefieres otra hora/día o con alguien concreto, dime y lo miro.` })
          return
        }

        // 8) Sin hora pero con intención/servicio → propón primer hueco (respetando “mañana/tarde/entre…”)
        if (data.serviceKey && !data.startEU) {
          const baseDay = dayjs().tz(EURO_TZ)
          const constraints = deriveWindowFromText(textRaw, baseDay)
          const base = ceilToSlotEU(baseDay.add(NOW_MIN_OFFSET_MIN,"minute"))
          const { exact, suggestion, staffId } = suggestOrExact(base, data.durationMin || 60, data.preferredStaffId, null, constraints)
          const offer = exact || suggestion
          if (offer) {
            data.startEU = offer
            data.preferredStaffId = staffId
            data.awaitingConfirm = true
            data.lastSuggestedMs = offer.valueOf()
            saveSession(phone,data)
            await __SAFE_SEND__(from,{ text:`Tengo ${fmtES(offer)} para ${data.serviceName}. ¿Confirmo la ${data.editBookingId?"modificación":"cita"}? (Pago en persona)` })
            return
          } else {
            await __SAFE_SEND__(from,{ text:`Ahora mismo no veo huecos. Dime una franja (ej.: “jueves 12:00”, “por la tarde”, “entre 10 y 12”) y pruebo de nuevo.` })
            saveSession(phone,data); return
          }
        }

        // 9) Con hora + servicio → exacto o 1 sugerencia
        if (data.serviceKey && data.startEU) {
          const baseDay = data.startEU.clone()
          const constraints = deriveWindowFromText(textRaw, baseDay)
          const { exact, suggestion, staffId } = suggestOrExact(data.startEU, data.durationMin || 60, data.preferredStaffId, data.lastSuggestedMs, constraints)
          if (exact) {
            data.startEU = exact; data.preferredStaffId = staffId
            data.awaitingConfirm = true; data.lastSuggestedMs = exact.valueOf()
            saveSession(phone,data)
            await __SAFE_SEND__(from,{ text:`Tengo libre ${fmtES(exact)} para ${data.serviceName}. ¿Confirmo la ${data.editBookingId?"modificación":"cita"}? (Pago en persona)` })
            return
          }
          if (suggestion) {
            data.startEU = suggestion; data.preferredStaffId = staffId
            data.awaitingConfirm = true; data.lastSuggestedMs = suggestion.valueOf()
            saveSession(phone,data)
            await __SAFE_SEND__(from,{ text:`No tengo ese hueco exacto. Te puedo ofrecer ${fmtES(suggestion)}. ¿Te viene bien?` })
            return
          }
          await __SAFE_SEND__(from,{ text:`No veo hueco en esa franja. Dime otra hora o día (L–V 10–14 / 16–20) y te digo.` })
          saveSession(phone,data); return
        }

        // 10) Link directo si lo piden
        if (/link|enlace|web|pagina|p[aá]gina|reserva online|online/.test(low)) {
          await __SAFE_SEND__(from,{ text: WELCOME_TEXT })
          saveSession(phone,data); return
        }

        // 11) Fallback amable
        await __SAFE_SEND__(from,{ text:`Puedo reservarte cita. Escríbeme “manicura” o el servicio y te propongo hora; o dime una franja (ej.: “viernes por la tarde”).` })
        saveSession(phone,data)

      }catch(e){ console.error("messages.upsert error:",e) }
    })
  }catch(e){ console.error("startBot error:",e) }
}

// ===== Booking
async function finalizeBooking({ from, phone, data, safeSend }) {
  try {
    if (data.bookingInFlight) return
    data.bookingInFlight = true; saveSession(phone, data)

    let customer = await squareFindCustomerByPhone(phone)
    if (!customer) {
      if (!data.name) { await safeSend(from,{ text:"Para cerrar, dime tu *nombre y apellidos*." }); data.bookingInFlight=false; saveSession(phone,data); return }
      if (!data.email || !isValidEmail(data.email)) { await safeSend(from,{ text:"Genial. Ahora tu email (tipo: nombre@correo.com)." }); data.bookingInFlight=false; saveSession(phone,data); return }
      customer = await squareCreateCustomer({ givenName: data.name, emailAddress: data.email, phoneNumber: phone })
      if (!customer) { await safeSend(from,{ text:"No pude crear el cliente con ese email. Mándame un email válido y seguimos." }); data.bookingInFlight=false; saveSession(phone,data); return }
    }

    const startEU = dayjs.isDayjs(data.startEU) ? data.startEU : (data.startEU_ms ? dayjs.tz(Number(data.startEU_ms), EURO_TZ) : null)
    if (!startEU || !startEU.isValid() || !insideBusinessBlocks(startEU, data.durationMin || (SVC[data.serviceKey]?.dur||60))) { data.bookingInFlight=false; saveSession(phone,data); return }
    const teamMemberId = data.preferredStaffId || Object.values(EMP_NAME_MAP)[0] || null
    if(!teamMemberId){ await safeSend(from,{ text:"Ahora mismo no puedo asignar profesional. Dime si te da igual con quién o prefieres a alguien." }); data.bookingInFlight=false; saveSession(phone,data); return }

    const durationMin = data.durationMin || (SVC[data.serviceKey]?.dur || 60)
    const startUTC = startEU.tz("UTC"), endUTC = startUTC.clone().add(durationMin,"minute")

    const aptId = `apt_${Math.random().toString(36).slice(2,8)}${Date.now().toString(36).slice(-4)}`
    try {
      insertAppt.run({
        id: aptId, customer_name: data.name || customer?.givenName || null, customer_phone: phone,
        customer_square_id: customer.id, service_key: data.serviceKey, service_name: data.serviceName, duration_min: durationMin,
        start_iso: startUTC.toISOString(), end_iso: endUTC.toISOString(),
        staff_id: teamMemberId, status: "pending", created_at: new Date().toISOString(), square_booking_id: null
      })
    } catch (e) {
      if (String(e?.message||"").includes("UNIQUE")) { data.bookingInFlight=false; saveSession(phone,data); return }
      throw e
    }

    const sq = await createSquareBooking({ startEU, serviceKey: data.serviceKey, customerId: customer.id, teamMemberId })
    if (!sq) { deleteAppt.run({ id: aptId }); data.bookingInFlight=false; saveSession(phone,data); return }

    updateAppt.run({ id: aptId, status: "confirmed", square_booking_id: sq.id || null })
    clearSession.run({ phone })
    await safeSend(from,{ text:
`Reserva confirmada 🎉
Servicio: ${data.serviceName}
Fecha: ${fmtES(startEU)}
Duración: ${durationMin} min
Pago en persona.` })
  } catch (e) { console.error("finalizeBooking:", e) }
  finally { data.bookingInFlight=false; try{ saveSession(phone, data) }catch{} }
}

// ===== Reprogramar
async function finalizeReschedule({ from, phone, data, safeSend }) {
  try{
    if (data.bookingInFlight) return
    data.bookingInFlight = true; saveSession(phone, data)

    const upc = getUpcomingByPhone.get({ phone, now: dayjs().utc().toISOString() })
    if (!upc || upc.id !== data.editBookingId) { data.bookingInFlight=false; saveSession(phone,data); return }

    const startEU = dayjs.isDayjs(data.startEU) ? data.startEU : (data.startEU_ms ? dayjs.tz(Number(data.startEU_ms), EURO_TZ) : null)
    if (!startEU || !startEU.isValid() || !insideBusinessBlocks(startEU, upc.duration_min)) { data.bookingInFlight=false; saveSession(phone,data); return }

    const startUTC = startEU.tz("UTC"), endUTC = startUTC.clone().add(upc.duration_min,"minute")
    const teamId   = data.preferredStaffId || upc.staff_id || Object.values(EMP_NAME_MAP)[0] || null
    if (!teamId) { data.bookingInFlight=false; saveSession(phone,data); return }

    let ok=false
    if (upc.square_booking_id) {
      const sq = await updateSquareBooking(upc.square_booking_id, { startEU, serviceKey: upc.service_key, customerId: upc.customer_square_id, teamMemberId: teamId })
      if (sq) ok=true
    }
    if (!ok) {
      if (upc.square_booking_id) await cancelSquareBooking(upc.square_booking_id)
      const sqNew = await createSquareBooking({ startEU, serviceKey: upc.service_key, customerId: upc.customer_square_id, teamMemberId: teamId })
      if (!sqNew) { data.bookingInFlight=false; saveSession(phone,data); return }
      markCancelled.run({ id: upc.id })
      const newId=`apt_${Math.random().toString(36).slice(2,8)}${Date.now().toString(36).slice(-4)}`
      insertAppt.run({
        id:newId, customer_name: upc.customer_name, customer_phone: phone, customer_square_id: upc.customer_square_id,
        service_key: upc.service_key, service_name: upc.service_name, duration_min: upc.duration_min,
        start_iso: startUTC.toISOString(), end_iso: endUTC.toISOString(),
        staff_id: teamId, status:"confirmed", created_at:new Date().toISOString(), square_booking_id: sqNew.id || null
      })
    } else {
      updateApptTimes.run({ id: upc.id, start_iso: startUTC.toISOString(), end_iso: endUTC.toISOString(), staff_id: teamId })
    }

    clearSession.run({ phone })
    await safeSend(from,{ text:
`Cita actualizada ✅
Servicio: ${upc.service_name}
Nueva fecha: ${fmtES(startEU)}
Duración: ${upc.duration_min} min` })
  }catch(e){ console.error("finalizeReschedule:", e) }
  finally{ data.bookingInFlight=false; try{ saveSession(phone, data) }catch{} }
}

// ===== Clientes Square
async function squareFindCustomerByPhone(phoneRaw){
  try{
    const e164=normalizePhoneES(phoneRaw)
    if(!e164||!e164.startsWith("+")||e164.length<8||e164.length>16) return null
    const resp=await square.customersApi.searchCustomers({query:{filter:{phoneNumber:{exact:e164}}}})
    return (resp?.result?.customers||[])[0]||null
  }catch(e){ console.error("Square search:",e?.message||e); return null }
}
async function squareCreateCustomer({givenName,emailAddress,phoneNumber}){
  try{
    if (!isValidEmail(emailAddress)) return null
    const phone=normalizePhoneES(phoneNumber)
    const resp=await square.customersApi.createCustomer({
      idempotencyKey:`cust_${Date.now()}_${Math.random().toString(36).slice(2,8)}`,
      givenName,emailAddress,phoneNumber:phone||undefined,note:"Creado desde bot WhatsApp Gapink Nails"
    })
    return resp?.result?.customer||null
  }catch(e){ console.error("Square create:", e?.message||e); return null }
}
