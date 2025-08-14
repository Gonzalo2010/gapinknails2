// index.js — Gapink Nails WhatsApp Bot (Playamar, robusto + parches)
// - Un solo messages.upsert (sin respuestas dobles)
// - Chequeo de empleadas configuradas (.env) -> aborta si vacío
// - Alias de servicios (corrige typos comunes)
// - Caché de versiones de variaciones de Square (menos fallos / latencia)
// - Limpieza de 'pending' viejos al arrancar
// - Lógica "primer hueco disponible", sin mostrar profesional (steering ON)
// - Pago siempre en persona

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

// ===== Config negocio
const WORK_DAYS = [1,2,3,4,5,6]   // L-S (domingo cerrado)
const OPEN_HOUR  = 10
const CLOSE_HOUR = 20
const SLOT_MIN   = 30

// Steering (equilibrar carga)
const STEER_ON = (process.env.BOT_STEER_BALANCE || "on").toLowerCase() === "on"
const STEER_WINDOW_DAYS = Number(process.env.BOT_STEER_WINDOW_DAYS || 7)   // “esta semana”
const SEARCH_WINDOW_DAYS = Number(process.env.BOT_SEARCH_WINDOW_DAYS || 14)

// ===== Utils
const onlyDigits = (s="") => (s||"").replace(/\D+/g,"")
const rmDiacritics = (s="") => s.normalize("NFD").replace(/\p{Diacritic}/gu,"")
const norm = (s="") => rmDiacritics(String(s).toLowerCase()).replace(/[^a-z0-9]+/g," ").trim()

// YES / NO (ampliado)
const YES_RE = /\b(s[ií]|ok|okay|okey+|vale+|va|venga|dale|confirmo|confirmar|de acuerdo|perfecto|genial)\b/i
const NO_RE  = /\b(no+|otra|cambia|no confirmo|mejor mas tarde|mejor más tarde|anula|cancela)\b/i
const RESCH_RE = /\b(cambia|cambiar|modifica|mover|reprograma|reprogramar|edita|mejor)\b/i

function normalizePhoneES(raw){
  const d=onlyDigits(raw); if(!d) return null
  if (raw.startsWith("+") && d.length>=8 && d.length<=15) return `+${d}`
  if (d.startsWith("34") && d.length===11) return `+${d}`
  if (d.length===9) return `+34${d}`
  if (d.startsWith("00")) return `+${d.slice(2)}`
  return `+${d}` // como último recurso, no “españolizamos” falsos positivos
}
const isValidEmail=(e)=>/^[^\s@]+@[^\s@]+\.[^\s@]{2,}$/.test(String(e||"").trim())

// ===== Empleadas Playamar (por nombre, desde .env). NO se muestran.
const EMPLOYEES = {
  rocio: process.env.SQ_EMP_ROCIO || "",
  cristina: process.env.SQ_EMP_CRISTINA || "",
  sami: process.env.SQ_EMP_SAMI || "",
  elisabeth: process.env.SQ_EMP_ELISABETH || "",
  tania: process.env.SQ_EMP_TANIA || "",
  jamaica: process.env.SQ_EMP_JAMAICA || "",
  johana: process.env.SQ_EMP_JOHANA || "",
  chabeli: process.env.SQ_EMP_CHABELI || "",
  desi: process.env.SQ_EMP_DESI || "",
  martina: process.env.SQ_EMP_MARTINA || "",
  ginna: process.env.SQ_EMP_GINNA || "",
  edurne: process.env.SQ_EMP_EDURNE || "",
}
const EMP_ALIASES = {
  "rocio":"rocio","rocío":"rocio","rosi":"rocio","rocio chica":"rocio","rocío chica":"rocio",
  "cristina":"cristina","cristi":"cristina","cristina jaime":"cristina",
  "sami":"sami",
  "elisabeth":"elisabeth","elisabet":"elisabeth","eli":"elisabeth",
  "tania":"tania",
  "jamaica":"jamaica",
  "johana":"johana","yohana":"johana","yojana":"johana",
  "chabeli":"chabeli","chabela":"chabeli",
  "desi":"desi","desiree":"desi","desirée":"desi",
  "martina":"martina",
  "ginna":"ginna","gina":"ginna",
  "edurne":"edurne"
}
const TEAM_MEMBER_IDS = Object.values(EMPLOYEES).filter(Boolean)
if (!TEAM_MEMBER_IDS.length) {
  console.error("⛔ No hay empleadas configuradas (TEAM_MEMBER_IDS vacío). Revisa .env (SQ_EMP_*).")
  process.exit(1)
}

// ===== Servicios desde .env (SQ_SVC_* = id|version)
function titleCase(s){return s.replace(/\b[a-záéíóúñ0-9]+\b/gi, w => w.charAt(0).toUpperCase()+w.slice(1).toLowerCase())}
function humanizeEnvKey(k){return titleCase(k.replace(/^SQ_SVC_/,"").replace(/_/g," ").trim())}
function loadServiceCatalogFromEnv(){
  const catalog=[]
  for (const [k,v] of Object.entries(process.env)){
    if(!k.startsWith("SQ_SVC_")) continue
    const [variationId, versionRaw] = String(v||"").split("|")
    if(!variationId) continue
    const display = humanizeEnvKey(k)
    const keyNorm = norm(display)
    catalog.push({ envKey:k, displayName:display, variationId, variationVersion:versionRaw?Number(versionRaw):undefined, normName:keyNorm })
  }
  catalog.sort((a,b)=>b.normName.length - a.normName.length)
  return catalog
}
const SERVICE_CATALOG = loadServiceCatalogFromEnv()

// Duraciones (mins). Si no listado, 60.
const DURATION_MIN = {
  "SQ_SVC_BONO_5_SESIONES_MADEROTERAPIA_MAS_5_SESIONES_PUSH_UP":60,
  "SQ_SVC_CARBON_PEEL":60,
  "SQ_SVC_CEJAS_EFECTO_POLVO_MICROSHADING":120,
  "SQ_SVC_CEJAS_HAIRSTROKE":30,
  "SQ_SVC_DEPILACION_CEJAS_CON_HILO":15,
  "SQ_SVC_DEPILACION_CEJAS_Y_LABIO_CON_HILO":20,
  "SQ_SVC_DEPILACION_DE_CEJAS_CON_PINZAS":15,
  "SQ_SVC_DEPILACION_LABIO":15,
  "SQ_SVC_DEPILACION_LABIO_CON_HILO":10,
  "SQ_SVC_DERMAPEN":60,
  "SQ_SVC_DISENO_DE_CEJAS_CON_HENNA_Y_DEPILACION":45,
  "SQ_SVC_ESMALTADO_SEMIPERMANETE_PIES":30,
  "SQ_SVC_EXTENSIONES_DE_PESTANAS_NUEVAS_PELO_A_PELO":120,
  "SQ_SVC_EXTENSIONES_PESTANAS_NUEVAS_2D":120,
  "SQ_SVC_EXTENSIONES_PESTANAS_NUEVAS_3D":120,
  "SQ_SVC_EYELINER":150,
  "SQ_SVC_FOSAS_NASALES":10,
  "SQ_SVC_FOTODEPILACION_AXILAS":30,
  "SQ_SVC_FOTODEPILACION_BRAZOS":30,
  "SQ_SVC_FOTODEPILACION_FACIAL_COMPLETO":30,
  "SQ_SVC_FOTODEPILACION_INGLES":30,
  "SQ_SVC_FOTODEPILACION_LABIO":30,
  "SQ_SVC_FOTODEPILACION_MEDIAS_PIERNAS":30,
  "SQ_SVC_FOTODEPILACION_PIERNAS_COMPLETAS":30,
  "SQ_SVC_FOTODEPILACION_PIERNAS_COMPLETAS_AXILAS_PUBIS_COMPLETO":60,
  "SQ_SVC_FOTODEPILACION_PUBIS_COMPLETO_CON_PERIANAL":30,
  "SQ_SVC_HYDRA_LIPS":60,
  "SQ_SVC_LABIOS_EFECTO_AQUARELA":150,
  "SQ_SVC_LAMINACION_Y_DISENO_DE_CEJAS":30,
  "SQ_SVC_LASER_CEJAS":30,
  "SQ_SVC_LIFITNG_DE_PESTANAS_Y_TINTE":60,
  "SQ_SVC_LIMPIEZA_FACIAL_BASICA":75,
  "SQ_SVC_LIMPIEZA_FACIAL_CON_PUNTA_DE_DIAMANTE":90,
  "SQ_SVC_LIMPIEZA_HYDRA_FACIAL":90,
  "SQ_SVC_MADEROTERAPIA_MAS_PUSH_UP":60,
  "SQ_SVC_MANICURA_CON_ESMALTE_NORMAL":30,
  "SQ_SVC_MANICURA_RUSA_CON_NIVELACION":90,
  "SQ_SVC_MANICURA_SEMIPERMANENTE":30,
  "SQ_SVC_MANICURA_SEMIPERMANENTE_QUITAR":40,
  "SQ_SVC_MANICURA_SEMIPERMANETE_CON_NIVELACION":60,
  "SQ_SVC_MASAJE_RELAJANTE":60,
  "SQ_SVC_MICROBLADING":120,
  "SQ_SVC_PEDICURA_GLAM_JELLY_CON_ESMALTE_NORMAL":60,
  "SQ_SVC_PEDICURA_GLAM_JELLY_CON_ESMALTE_SEMIPERMANENTE":60,
  "SQ_SVC_PEDICURA_SPA_CON_ESMALTE_NORMAL":60,
  "SQ_SVC_PEDICURA_SPA_CON_ESMALTE_SEMIPERMANENTE":60,
  "SQ_SVC_PEDICURA_SPA_CON_ESMALTE_SEMIPERMANENTE_2":60, // si tienes segundo item
  "SQ_SVC_QUITAR_ESMALTADO_SEMIPERMANENTE":30,
  "SQ_SVC_QUITAR_ESMALTADO_SEMIPERMANENTE_PIES":30,
  "SQ_SVC_QUITAR_EXTENSIONES_PESTANAS":30,
  "SQ_SVC_QUITAR_UNAS_ESCULPIDAS":30,
  "SQ_SVC_RECONSTRUCCION_DE_UNA_UNA_PIE":20,
  "SQ_SVC_RELLENO_DE_UNAS_MAS_DE_4_SEMANAS":60,
  "SQ_SVC_RELLENO_EXTENSIONES_PESTANAS_PELO_A_PELO":60,
  "SQ_SVC_RELLENO_PESTANAS_2D":60,
  "SQ_SVC_RELLENO_PESTANAS_3D":60,
  "SQ_SVC_RELLENO_UNAS_ESCULPIDAS":60,
  "SQ_SVC_RELLENO_UNAS_ESCULPIDAS_CON_FRANCESA_CONSTRUIDA_BABY_BOOMER_O_ENCAPSULADOS":75,
  "SQ_SVC_RELLENO_UNAS_ESCULPIDAS_CON_MANICURA_RUSA":75,
  "SQ_SVC_RELLENO_UNAS_ESCULPIDAS_EXTRA_LARGAS":75,
  "SQ_SVC_RETOQUE_ANUAL_CEJAS":60,
  "SQ_SVC_RETOQUE_MES_CEJAS":60,
  "SQ_SVC_SESION_ENDOSPHERE_FACIAL":60,
  "SQ_SVC_SESION_ENDOSPHERE_CORPORAL":60,
  "SQ_SVC_TRATAMIENDO_HIDRATANTE_LAMINAS_DE_ORO":60,
  "SQ_SVC_TRATAMIENTO_ANTI_ACNE":60,
  "SQ_SVC_TRATAMIENTO_FACIAL_ANTI_MANCHAS":60,
  "SQ_SVC_TRATAMIENTO_FACIAL_PIEDRAS_DE_JADE":60,
  "SQ_SVC_TRATAMIENTO_HIDRATANTE_AZAFRAN":60,
  "SQ_SVC_TRATAMIENTO_REAFIRMANTE_CON_VELO_DE_COLAGENO":60,
  "SQ_SVC_TRATAMIENTO_VITAMINA_C":60,
  "SQ_SVC_UNA_ROTA":15,
  "SQ_SVC_UNA_ROTA_DENTRO_DE_RELLENO":15,
  "SQ_SVC_UNA_ROTA_DENTRO_RELLENO":15,
  "SQ_SVC_UNAS_ESCULPIDAS_NUEVAS_EXTRA_LARGAS":90,
  "SQ_SVC_UNAS_NUEVAS_ESCULPIDAS":75,
  "SQ_SVC_UNAS_NUEVAS_ESCULPIDAS_CON_MANICURA_RUSA":90,
  "SQ_SVC_UNAS_NUEVAS_ESCULPIDAS_FRANCESA_BABY_BOOMER_ENCAPSULADOS":90,
  "SQ_SVC_UNAS_NUEVAS_FORMAS_SUPERIORES_Y_MANICURA_RUSA":90,
}
function getDurationForServiceEnvKey(envKey){ return DURATION_MIN[envKey] ?? 60 }

// Alias de servicios (corrige nombres “humanos” a envKey concretos)
const SERVICE_ALIASES = {
  "esmaltado semipermanente pies":"SQ_SVC_ESMALTADO_SEMIPERMANETE_PIES",
  "esmaltado semipermanente en pies":"SQ_SVC_ESMALTADO_SEMIPERMANETE_PIES",
  "lifting de pestanas y tinte":"SQ_SVC_LIFITNG_DE_PESTANAS_Y_TINTE",
  "lifting de pestañas y tinte":"SQ_SVC_LIFITNG_DE_PESTANAS_Y_TINTE",
  "tratamiento hidratante laminas de oro":"SQ_SVC_TRATAMIENDO_HIDRATANTE_LAMINAS_DE_ORO",
  "tratamiento hidratante láminas de oro":"SQ_SVC_TRATAMIENDO_HIDRATANTE_LAMINAS_DE_ORO",
  "laser cejas":"SQ_SVC_LASER_CEJAS",
  "láser cejas":"SQ_SVC_LASER_CEJAS",
  "labios efecto acuarela":"SQ_SVC_LABIOS_EFECTO_AQUARELA",
  "limpieza facial básica":"SQ_SVC_LIMPIEZA_FACIAL_BASICA",
  "limpieza facial basica":"SQ_SVC_LIMPIEZA_FACIAL_BASICA",
  "fotodepilacion ingles":"SQ_SVC_FOTODEPILACION_INGLES",
  "fotodepilación ingles":"SQ_SVC_FOTODEPILACION_INGLES",
}
function getServiceByText(txt){
  const t=norm(txt)
  for(const [alias,envKey] of Object.entries(SERVICE_ALIASES)){
    if(t.includes(norm(alias))){
      const svc=SERVICE_CATALOG.find(s=>s.envKey===envKey)
      if(svc) return svc
    }
  }
  for (const svc of SERVICE_CATALOG){ if (svc.normName && t.includes(svc.normName)) return svc }
  return null
}

// ===== OpenAI (silencioso si peta)
const OPENAI_API_KEY  = process.env.OPENAI_API_KEY
const OPENAI_API_URL  = process.env.OPENAI_API_URL || "https://api.openai.com/v1/chat/completions"
const OPENAI_MODEL    = process.env.OPENAI_MODEL || "gpt-4o-mini"

async function aiChat(messages, { temperature=0.35 } = {}) {
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
Nunca muestres ni ofrezcas elegir profesional.
Si la hora pedida está libre, ofrécela; si no, propone la más cercana y pide confirmación.
Pago siempre en persona.`
async function extractFromText(userText="") {
  const schema = `
Devuelve SOLO un JSON válido (omite claves que no apliquen):
{
  "intent": "greeting|booking|cancel|reschedule|other",
  "datetime_text": "texto con fecha/hora si lo hay",
  "confirm": "yes|no|unknown",
  "name": "si aparece",
  "email": "si aparece",
  "staff_hint": "si menciona 'con rocio', 'con cristina', etc",
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

// ===== Fecha/hora ES
function parseDateTimeES(dtText){if(!dtText)return null;const t=rmDiacritics(dtText.toLowerCase());let base=null;if(/\bhoy\b/.test(t))base=dayjs().tz(EURO_TZ);else if(/\bmanana\b/.test(t))base=dayjs().tz(EURO_TZ).add(1,"day");if(!base){const M={enero:1,febrero:2,marzo:3,abril:4,mayo:5,junio:6,julio:7,agosto:8,septiembre:9,setiembre:9,octubre:10,noviembre:11,diciembre:12,ene:1,feb:2,mar:3,abr:4,may:5,jun:6,jul:7,ago:8,sep:9,oct:10,nov:11,dic:12};const m=t.match(/\b(\d{1,2})\s+de\s+(enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|setiembre|octubre|noviembre|diciembre|ene|feb|mar|abr|may|jun|jul|ago|sep|oct|nov|dic)\b(?:\s+de\s+(\d{4}))?/);if(m){const dd=+m[1],mm=M[m[2]],yy=m[3]?+m[3]:dayjs().tz(EURO_TZ).year();base=dayjs.tz(`${yy}-${String(mm).padStart(2,"0")}-${String(dd).padStart(2,"0")} 00:00`,EURO_TZ)}}if(!base){const m=t.match(/\b(\d{1,2})[\/\-](\d{1,2})(?:[\/\-](\d{2,4}))?\b/);if(m){let yy=m[3]?+m[3]:dayjs().tz(EURO_TZ).year();if(yy<100)yy+=2000;base=dayjs.tz(`${yy}-${String(+m[2]).padStart(2,"0")}-${String(+m[1]).padStart(2,"0")} 00:00`,EURO_TZ)}}if(!base)base=dayjs().tz(EURO_TZ);let hour=null,minute=0;const hm=t.match(/(\d{1,2})(?::|h)?(\d{2})?\s*(am|pm)?\b/);if(hm){hour=+hm[1];minute=hm[2]?+hm[2]:0;const ap=hm[3];if(ap==="pm"&&hour<12)hour+=12;if(ap==="am"&&hour===12)hour=0}if(hour===null)return null;return base.hour(hour).minute(minute).second(0).millisecond(0)}
const fmtES=(d)=>{const t=(dayjs.isDayjs(d)?d:dayjs(d)).tz(EURO_TZ);const dias=["domingo","lunes","martes","miércoles","jueves","viernes","sábado"];const DD=String(t.date()).padStart(2,"0"),MM=String(t.month()+1).padStart(2,"0"),HH=String(t.hour()).padStart(2,"0"),mm=String(t.minute()).padStart(2,"0");return `${dias[t.day()]} ${DD}/${MM} ${HH}:${mm}`}

// ===== Square
const square = new Client({
  accessToken: process.env.SQUARE_ACCESS_TOKEN,
  environment: (process.env.SQUARE_ENV || "sandbox")==="production"?Environment.Production:Environment.Sandbox
})
const locationId = process.env.SQUARE_LOCATION_ID_PLAYAMAR || process.env.SQUARE_LOCATION_ID
let LOCATION_TZ = EURO_TZ
async function squareCheckCredentials(){try{const locs=await square.locationsApi.listLocations();const loc=(locs.result.locations||[]).find(l=>l.id===locationId)||(locs.result.locations||[])[0];if(loc?.timezone)LOCATION_TZ=loc.timezone;console.log(`✅ Square listo. Location ${locationId}, TZ=${LOCATION_TZ}`)}catch(e){console.error("⛔ Square:",e?.message||e)}}
async function squareFindCustomerByPhone(phoneRaw){try{const e164=normalizePhoneES(phoneRaw);if(!e164||!e164.startsWith("+")||e164.length<8||e164.length>16)return null;const resp=await square.customersApi.searchCustomers({query:{filter:{phoneNumber:{exact:e164}}}});return (resp?.result?.customers||[])[0]||null}catch(e){console.error("Square search:",e?.message||e);return null}}
async function squareCreateCustomer({givenName,emailAddress,phoneNumber}){try{
  if(!isValidEmail(emailAddress)) return null
  const phone=normalizePhoneES(phoneNumber)
  const resp=await square.customersApi.createCustomer({
    idempotencyKey:`cust_${Date.now()}_${Math.random().toString(36).slice(2,8)}`,
    givenName,emailAddress,phoneNumber:phone||undefined,
    note:"Creado desde bot WhatsApp Gapink Nails (Playamar)"
  })
  return resp?.result?.customer||null
}catch(e){console.error("Square create:",e?.message||e);return null}}

// Caché de versiones de variaciones
const VAR_VERSION_CACHE = new Map() // variationId -> {version, ts}
const VERSION_TTL_MS = 30*60*1000
async function getServiceVariationVersionRaw(id){const resp=await square.catalogApi.retrieveCatalogObject(id,true);return resp?.result?.object?.version}
async function getServiceVariationVersion(id){
  const cached = VAR_VERSION_CACHE.get(id)
  const now = Date.now()
  if (cached && (now - cached.ts) < VERSION_TTL_MS) return cached.version
  try{
    const v = await getServiceVariationVersionRaw(id)
    if (v) VAR_VERSION_CACHE.set(id, {version:Number(v), ts:now})
    return v
  }catch(e){console.error("getServiceVariationVersion:",e?.message||e);return undefined}
}

function stableKey({locationId,serviceVariationId,startISO,customerId,teamMemberId}){
  const raw=`${locationId}|${serviceVariationId}|${startISO}|${customerId}|${teamMemberId}`
  return createHash("sha256").update(raw).digest("hex").slice(0,48)
}
async function createSquareBooking({startEU,svc,customerId,teamMemberId}){
  try{
    const serviceVariationId = svc.variationId
    let version = svc.variationVersion || await getServiceVariationVersion(serviceVariationId)
    if(!serviceVariationId||!teamMemberId||!locationId||!version) return null
    const startISO=startEU.tz("UTC").toISOString()
    const body={idempotencyKey:stableKey({locationId,serviceVariationId,startISO,customerId,teamMemberId}),
      booking:{locationId,startAt:startISO,customerId,
        appointmentSegments:[{teamMemberId,serviceVariationId,serviceVariationVersion:Number(version),durationMinutes:getDurationForServiceEnvKey(svc.envKey)}]}}
    const resp=await square.bookingsApi.createBooking(body)
    return resp?.result?.booking||null
  }catch(e){console.error("createSquareBooking:",e?.message||e);return null}
}
async function cancelSquareBooking(bookingId){try{const r=await square.bookingsApi.cancelBooking(bookingId);return !!r?.result?.booking?.id}catch(e){console.error("cancelSquareBooking:",e?.message||e);return false}}
async function updateSquareBooking(bookingId,{startEU,svc,customerId,teamMemberId}){
  try{
    const get=await square.bookingsApi.retrieveBooking(bookingId);const booking=get?.result?.booking;if(!booking)return null
    let version = svc.variationVersion || await getServiceVariationVersion(svc.variationId)
    const startISO=startEU.tz("UTC").toISOString()
    const body={idempotencyKey:stableKey({locationId,serviceVariationId:svc.variationId,startISO,customerId,teamMemberId}),
      booking:{id:bookingId,version:booking.version,locationId,customerId,startAt:startISO,
        appointmentSegments:[{teamMemberId,serviceVariationId:svc.variationId,serviceVariationVersion:Number(version),durationMinutes:getDurationForServiceEnvKey(svc.envKey)}]}}
    const resp=await square.bookingsApi.updateBooking(bookingId, body);return resp?.result?.booking||null
  }catch(e){console.error("updateSquareBooking:",e?.message||e);return null}
}

// ===== DB & sesiones
const db=new Database("gapink.db");db.pragma("journal_mode = WAL")
db.exec(`
CREATE TABLE IF NOT EXISTS appointments (
  id TEXT PRIMARY KEY,
  customer_name TEXT,
  customer_phone TEXT,
  customer_square_id TEXT,
  service_env_key TEXT,
  service_display TEXT,
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
(id, customer_name, customer_phone, customer_square_id, service_env_key, service_display, duration_min, start_iso, end_iso, staff_id, status, created_at, square_booking_id)
VALUES (@id, @customer_name, @customer_phone, @customer_square_id, @service_env_key, @service_display, @duration_min, @start_iso, @end_iso, @staff_id, @status, @created_at, @square_booking_id)`)
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
const cleanupPendingOld=db.prepare(`DELETE FROM appointments WHERE status='pending' AND created_at < @cutoff`)

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

// ===== Disponibilidad
function getBookedIntervals(fromIso,toIso){
  const rows=db.prepare(`SELECT start_iso,end_iso,staff_id FROM appointments WHERE status IN ('pending','confirmed') AND start_iso < @to AND end_iso > @from`).all({from:fromIso,to:toIso})
  return rows.map(r=>({start:dayjs(r.start_iso),end:dayjs(r.end_iso),staff_id:r.staff_id}))
}
function isFree(intervals, staffId, startUTC, endUTC){
  return !intervals.filter(i=>i.staff_id===staffId).some(i => (startUTC<i.end) && (i.start<endUTC))
}
function findEarliestAny(startEU, durationMin, daysWindow){
  const now=dayjs().tz(EURO_TZ).add(30,"minute").second(0).millisecond(0)
  const startBase = startEU && startEU.isAfter(now) ? ceilToSlotEU(startEU.clone()) : ceilToSlotEU(now.clone())
  const toEU = startBase.clone().add(daysWindow,"day").hour(CLOSE_HOUR).minute(0).second(0)
  const intervals=getBookedIntervals(startBase.tz("UTC").toISOString(), toEU.tz("UTC").toISOString())
  for(let d=0; d<=daysWindow; d++){
    const day= startBase.clone().add(d,"day")
    const dow=day.day()===0?7:day.day()
    if(!WORK_DAYS.includes(dow)) continue
    const dayStart=day.clone().hour(OPEN_HOUR).minute(0).second(0)
    const firstSlot = d===0 ? startBase.clone().max(dayStart) : dayStart
    const dayEnd=day.clone().hour(CLOSE_HOUR).minute(0).second(0)
    for(let t=ceilToSlotEU(firstSlot.clone()); !t.isAfter(dayEnd); t=t.add(SLOT_MIN,"minute")){
      const e=t.clone().add(durationMin,"minute"); if(e.isAfter(dayEnd)) break
      const tUTC=t.tz("UTC"), eUTC=e.tz("UTC")
      for(const staffId of TEAM_MEMBER_IDS){
        if(isFree(intervals,staffId,tUTC,eUTC)) return { time:t, staffId }
      }
    }
  }
  return null
}
function findEarliestForStaff(startEU, durationMin, daysWindow, staffId){
  if(!staffId) return null
  const now=dayjs().tz(EURO_TZ).add(30,"minute").second(0).millisecond(0)
  const startBase = startEU && startEU.isAfter(now) ? ceilToSlotEU(startEU.clone()) : ceilToSlotEU(now.clone())
  const toEU = startBase.clone().add(daysWindow,"day").hour(CLOSE_HOUR).minute(0).second(0)
  const intervals=getBookedIntervals(startBase.tz("UTC").toISOString(), toEU.tz("UTC").toISOString())
  for(let d=0; d<=daysWindow; d++){
    const day= startBase.clone().add(d,"day")
    const dow=day.day()===0?7:day.day()
    if(!WORK_DAYS.includes(dow)) continue
    const dayStart=day.clone().hour(OPEN_HOUR).minute(0).second(0)
    const firstSlot = d===0 ? startBase.clone().max(dayStart) : dayStart
    const dayEnd=day.clone().hour(CLOSE_HOUR).minute(0).second(0)
    for(let t=ceilToSlotEU(firstSlot.clone()); !t.isAfter(dayEnd); t=t.add(SLOT_MIN,"minute")){
      const e=t.clone().add(durationMin,"minute"); if(e.isAfter(dayEnd)) break
      if(isFree(intervals,staffId,t.tz("UTC"),e.tz("UTC"))) return { time:t, staffId }
    }
  }
  return null
}
function ceilToSlotEU(t){const m=t.minute();const rem=m%SLOT_MIN;if(rem===0)return t.second(0).millisecond(0);return t.add(SLOT_MIN-rem,"minute").second(0).millisecond(0)}

// ===== Mini web
const app=express()
const PORT=process.env.PORT||8080
let lastQR=null,conectado=false
app.get("/",(_req,res)=>{res.send(`<!doctype html><meta charset="utf-8"><style>body{font-family:system-ui;display:grid;place-items:center;min-height:100vh;background:linear-gradient(135deg,#fce4ec,#f8bbd0);color:#4a148c} .card{background:#fff;padding:24px;border-radius:16px;box-shadow:0 6px 24px rgba(0,0,0,.08);text-align:center;max-width:520px}</style><div class="card"><h1>Gapink Nails</h1><p>Estado: ${conectado?"✅ Conectado":"❌ Desconectado"}</p>${!conectado&&lastQR?`<img src="/qr.png" width="320" />`:``}<p><small>Pago en persona · Playamar</small></p></div>`)})
app.get("/qr.png",async(_req,res)=>{if(!lastQR)return res.status(404).send("No hay QR");const png=await qrcode.toBuffer(lastQR,{type:"png",width:512,margin:1});res.set("Content-Type","image/png").send(png)})

// ===== Cola envío Baileys
const wait=(ms)=>new Promise(r=>setTimeout(r,ms))
app.listen(PORT,async()=>{
  console.log(`🌐 Web en puerto ${PORT}`)
  // Limpieza de pending > 30 minutos
  const cutoff = dayjs().subtract(30, "minute").toISOString()
  cleanupPendingOld.run({ cutoff })
  await squareCheckCredentials()
  startBot().catch(console.error)
})

// ===== Bot
const PROCESSED_IDS = new Set()
function markProcessed(id){ PROCESSED_IDS.add(id); if (PROCESSED_IDS.size>2000){ // GC simple
  const arr=[...PROCESSED_IDS]; PROCESSED_IDS.clear(); for(let i=arr.length-1000;i<arr.length;i++) if(i>=0) PROCESSED_IDS.add(arr[i])
}}

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

    // ===== ÚNICO listener de mensajes
    sock.ev.on("messages.upsert",async({messages})=>{
      try{
        const m=messages?.[0]; if (!m?.message || m.key.fromMe) return
        const msgId=m.key.id; if (PROCESSED_IDS.has(msgId)) return; markProcessed(msgId)

        const from=m.key.remoteJid
        const phone=normalizePhoneES((from||"").split("@")[0]||"")||(from||"").split("@")[0]||""
        const body=m.message.conversation||m.message.extendedTextMessage?.text||m.message?.imageMessage?.caption||""
        const textRaw=(body||"").trim()
        const textNorm = norm(textRaw)

        // Sesión
        let data=loadSession(phone)||{
          serviceEnvKey:null, service:null, durationMin:null,
          startEU:null, name:null, email:null,
          requestedStaffId:null, staffInsistCount:0,
          confirmApproved:false,confirmAsked:false,bookingInFlight:false,
          lastUserDtText:null, selectedStaffId:null,
          editBookingId:null
        }

        // IA extracción (silencioso si peta)
        const extra=await extractFromText(textRaw)
        const incomingDt = extra.datetime_text || null

        // Cancelación explícita
        if (extra.intent==="cancel" || /\b(cancel|anul|borra|elimina)r?\b/i.test(textRaw)) {
          const upc = getUpcomingByPhone.get({ phone, now: dayjs().utc().toISOString() })
          if (upc) {
            const ok = upc.square_booking_id ? await cancelSquareBooking(upc.square_booking_id) : true
            if (ok) { markCancelled.run({ id: upc.id }); clearSession.run({ phone }); await __SAFE_SEND__(from,{ text:`He cancelado tu cita del ${fmtES(dayjs(upc.start_iso))}.` }) }
          } else {
            await __SAFE_SEND__(from,{ text:"No veo ninguna cita futura tuya. Si quieres, dime día y hora y te doy hueco." })
          }
          return
        }

        // ¿Pide reprogramar?
        if (extra.intent==="reschedule" || RESCH_RE.test(textRaw)) {
          const upc = getUpcomingByPhone.get({ phone, now: dayjs().utc().toISOString() })
          if (upc) { data.editBookingId = upc.id; data.serviceEnvKey = upc.service_env_key; data.service = upc.service_display; data.durationMin = upc.duration_min; data.selectedStaffId = upc.staff_id }
        }

        // Staff en texto (sin mostrarlo)
        const staffFromText = detectStaffFromText(textNorm, String(extra.staff_hint||"").toLowerCase())
        if (staffFromText?.id) {
          if (data.requestedStaffId && data.requestedStaffId===staffFromText.id) data.staffInsistCount++
          else { data.requestedStaffId = staffFromText.id; data.staffInsistCount = 1 }
        }

        // Servicio detectado
        const svcDetected = getServiceByText(textRaw)
        if (svcDetected) {
          if (svcDetected.envKey!==data.serviceEnvKey) { data.confirmApproved=false; data.confirmAsked=false }
          data.serviceEnvKey = svcDetected.envKey
          data.service = svcDetected.displayName
          data.durationMin = getDurationForServiceEnvKey(svcDetected.envKey)
        }

        // Fecha/hora parsing
        if (incomingDt && incomingDt !== data.lastUserDtText) { data.confirmApproved=false; data.confirmAsked=false }
        if (incomingDt) data.lastUserDtText = incomingDt
        const parsed = parseDateTimeES(incomingDt ? incomingDt : textRaw)
        if (parsed) data.startEU = parsed

        // Nombre/Email si los extrae
        if (!data.name && extra.name) data.name = extra.name.trim()
        if (!data.email && extra.email && isValidEmail(extra.email)) data.email = extra.email.trim()

        saveSession(phone, data)

        // ======= FLUJO por pasos

        // Paso 1: Servicio
        if (!data.serviceEnvKey) {
          await __SAFE_SEND__(from,{ text:"¿Qué te hago? (Ej: “Manicura semipermanente”, “Limpieza facial con punta de diamante”…)" })
          return
        }

        // Paso 2: Fecha/hora
        if (!data.startEU) {
          await __SAFE_SEND__(from,{ text:`Genial, ${data.service}. Dime día y hora (ej: “mañana 10:30” o “15/09 18:00”).` })
          return
        }

        // Paso 3: Disponibilidad con steering
        const duration = data.durationMin || 60
        const earliestAny = findEarliestAny(data.startEU, duration, SEARCH_WINDOW_DAYS)
        const earliestRequested = data.requestedStaffId ? findEarliestForStaff(data.startEU, duration, STEER_WINDOW_DAYS, data.requestedStaffId) : null

        let offer = earliestAny
        let staffToAssign = earliestAny?.staffId || null

        if (STEER_ON && data.requestedStaffId && earliestRequested) {
          if (earliestAny && earliestAny.time.isBefore(earliestRequested.time)) {
            offer = earliestAny
            staffToAssign = earliestAny.staffId
            await __SAFE_SEND__(from,{ text:`No me aparece hueco esta semana con esa opción. Te puedo ofrecer ${fmtES(offer.time)}. ¿Te viene bien? Si es sí, responde “confirmo”.` })
            data.startEU = offer.time; data.selectedStaffId = staffToAssign; data.confirmAsked = true; data.confirmApproved=false; saveSession(phone,data)
            return
          }
        }

        // Si insistió 2+ veces, respetamos la profesional pedida
        if (data.requestedStaffId && data.staffInsistCount >= 2) {
          const forced = findEarliestForStaff(data.startEU, duration, SEARCH_WINDOW_DAYS, data.requestedStaffId)
          if (forced) { offer = forced; staffToAssign = forced.staffId }
        }

        if (offer) {
          data.startEU = offer.time
          data.selectedStaffId = staffToAssign || TEAM_MEMBER_IDS[0]
          if (!data.selectedStaffId) { await __SAFE_SEND__(from,{ text:"Ahora mismo no puedo asignar equipo. Dime otra hora y lo miro." }); return }
          data.confirmAsked = true
          saveSession(phone,data)
          await __SAFE_SEND__(from,{ text:`Puedo darte ${fmtES(offer.time)}. ¿Confirmo la ${data.editBookingId?"modificación":"cita"}?` })
          return
        }

        // Sin huecos
        data.confirmAsked=false; saveSession(phone,data)
        await __SAFE_SEND__(from,{ text:"Ahora mismo no veo huecos en esa franja. Dime otra hora o día y te digo." })
        return

      } catch(e){ console.error("messages.upsert error:",e) }
    })

    // ===== Confirmaciones / capturas dentro del mismo listener (con state)
    sock.ev.on("messages.upsert",async({messages})=>{
      try{
        const m=messages?.[0]; if (!m?.message || m.key.fromMe) return
        const msgId=m.key.id; if (PROCESSED_IDS.has(msgId)) return; markProcessed(msgId)

        const from=m.key.remoteJid
        const phone=normalizePhoneES((from||"").split("@")[0]||"")||(from||"").split("@")[0]||""
        const body=m.message.conversation||m.message.extendedTextMessage?.text||m.message?.imageMessage?.caption||""
        const textRaw=(body||"").trim()

        let data=loadSession(phone); if(!data) return

        const userSaysYes = YES_RE.test(textRaw)
        const userSaysNo  = NO_RE.test(textRaw)
        if (userSaysNo){ data.confirmApproved=false; data.confirmAsked=false; saveSession(phone,data); await __SAFE_SEND__(from,{text:"Sin problema. Dime otra hora o día y lo miro."}); return }

        // Captura paso a paso (solo altas)
        if (!data.editBookingId) {
          if (!data.name) {
            const maybeName = textRaw.replace(/[,;].*/,'').trim()
            if (!userSaysYes && maybeName && !maybeName.match(/@|\d/)) { data.name=maybeName; saveSession(phone,data); await __SAFE_SEND__(from,{text:"Gracias. Ahora tu email (ej: “nombre@correo.com”)."}); return }
            if (userSaysYes && data.confirmAsked) { await __SAFE_SEND__(from,{ text:"Para cerrar, dime tu nombre (ej: “Ana Pérez”)." }); return }
          } else if (!data.email) {
            if (!userSaysYes && isValidEmail(textRaw)) { data.email=textRaw.trim(); saveSession(phone,data); await __SAFE_SEND__(from,{text:"Perfecto. Si estás conforme, responde “confirmo” para cerrar la cita."}); return }
            if (userSaysYes && data.confirmAsked) { await __SAFE_SEND__(from,{ text:"Y ahora tu email (ej: “ana@correo.com”)." }); return }
          }
        }

        // Confirmación final
        if (data.confirmAsked && userSaysYes && data.serviceEnvKey && data.startEU) {
          if (data.editBookingId) await finalizeReschedule({ from, phone, data, safeSend: __SAFE_SEND__ })
          else await finalizeBooking({ from, phone, data, safeSend: __SAFE_SEND__ })
          return
        }
      }catch(e){ console.error("confirm handler error:",e) }
    })

  }catch(e){ console.error("startBot error:",e) }
}

// Detectar staff por texto (sin mostrarlo)
function detectStaffFromText(textNorm, hint){
  const words=[...(textNorm||"").split(/\s+/), (hint||"").toLowerCase()].filter(Boolean)
  for (const w of words){
    const key=EMP_ALIASES[w]
    if (key && EMPLOYEES[key]) return { id: EMPLOYEES[key] }
  }
  return null
}

// ===== Finalizar alta
async function finalizeBooking({ from, phone, data, safeSend }) {
  try {
    if (data.bookingInFlight) return
    data.bookingInFlight = true; saveSession(phone, data)

    // Cliente Square
    let customer = await squareFindCustomerByPhone(phone)
    if (!customer) {
      if (!data.name || !data.email || !isValidEmail(data.email)) {
        data.bookingInFlight=false; saveSession(phone,data);
        await safeSend(from,{text:"Me falta nombre y/o email válido para crear tu ficha. Dime: “Nombre Apellido” y luego “correo@ejemplo.com”"});
        return
      }
      customer = await squareCreateCustomer({ givenName: data.name, emailAddress: data.email, phoneNumber: phone })
    }
    if (!customer) { data.bookingInFlight=false; saveSession(phone,data); await safeSend(from,{text:"No pude crear tu ficha (email no válido). Pásame un email válido por favor."}); return }

    // Servicio
    const svc = SERVICE_CATALOG.find(s=>s.envKey===data.serviceEnvKey)
    if (!svc) { data.bookingInFlight=false; saveSession(phone,data); await safeSend(from,{text:"No encuentro el servicio ahora mismo. Dime de nuevo el servicio, por favor."}); return }

    // Fecha/hora
    const startEU = dayjs.isDayjs(data.startEU) ? data.startEU : (data.startEU_ms ? dayjs.tz(Number(data.startEU_ms), EURO_TZ) : null)
    if (!startEU || !startEU.isValid()) { data.bookingInFlight=false; saveSession(phone,data); return }

    const teamMemberId = data.selectedStaffId || TEAM_MEMBER_IDS[0]
    if(!teamMemberId){ data.bookingInFlight=false; saveSession(phone,data); await safeSend(from,{text:"No puedo asignar equipo ahora mismo. Dime otra hora y lo intento."}); return }
    const durationMin = getDurationForServiceEnvKey(svc.envKey)
    const startUTC = startEU.tz("UTC"), endUTC = startUTC.clone().add(durationMin,"minute")

    // Inserción previa (pending)
    const aptId = `apt_${Math.random().toString(36).slice(2,8)}${Date.now().toString(36).slice(-4)}`
    try {
      insertAppt.run({
        id: aptId, customer_name: data.name || customer?.givenName || null, customer_phone: phone,
        customer_square_id: customer.id, service_env_key: svc.envKey, service_display: svc.displayName,
        duration_min: durationMin, start_iso: startUTC.toISOString(), end_iso: endUTC.toISOString(),
        staff_id: teamMemberId, status: "pending", created_at: new Date().toISOString(), square_booking_id: null
      })
    } catch (e) {
      if (String(e?.message||"").includes("UNIQUE")) { data.bookingInFlight=false; saveSession(phone,data); await safeSend(from,{text:"Ese hueco acaba de ocuparse. Dime otra hora y te doy opción."}); return }
      throw e
    }

    const sq = await createSquareBooking({ startEU, svc, customerId: customer.id, teamMemberId })
    if (!sq) { deleteAppt.run({ id: aptId }); data.bookingInFlight=false; saveSession(phone,data); await safeSend(from,{text:"No pude confirmar con Square ahora mismo. Probamos con otra hora?"}); return }

    updateAppt.run({ id: aptId, status: "confirmed", square_booking_id: sq.id || null })
    clearSession.run({ phone })
    await safeSend(from,{ text:
`Reserva confirmada.
Servicio: ${svc.displayName}
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
    if (!startEU || !startEU.isValid()) { data.bookingInFlight=false; saveSession(phone,data); return }

    const svc = SERVICE_CATALOG.find(s=>s.envKey === (data.serviceEnvKey || upc.service_env_key))
    if (!svc) { data.bookingInFlight=false; saveSession(phone,data); return }

    const startUTC = startEU.tz("UTC"), endUTC = startUTC.clone().add(upc.duration_min,"minute")
    const teamId   = data.selectedStaffId || upc.staff_id || TEAM_MEMBER_IDS[0]

    if(!teamId){ data.bookingInFlight=false; saveSession(phone,data); await safeSend(from,{text:"No puedo asignar equipo ahora mismo. Probamos otro día?"}); return }

    let ok=false
    if (upc.square_booking_id) {
      const sq = await updateSquareBooking(upc.square_booking_id, { startEU, svc, customerId: upc.customer_square_id, teamMemberId: teamId })
      if (sq) ok=true
    }
    if (!ok) {
      if (upc.square_booking_id) await cancelSquareBooking(upc.square_booking_id)
      const sqNew = await createSquareBooking({ startEU, svc, customerId: upc.customer_square_id, teamMemberId: teamId })
      if (!sqNew) { data.bookingInFlight=false; saveSession(phone,data); await safeSend(from,{text:"No pude reprogramar ahora mismo. Probamos otra franja?"}); return }
      markCancelled.run({ id: upc.id })
      const newId=`apt_${Math.random().toString(36).slice(2,8)}${Date.now().toString(36).slice(-4)}`
      insertAppt.run({
        id:newId, customer_name: upc.customer_name, customer_phone: phone, customer_square_id: upc.customer_square_id,
        service_env_key: svc.envKey, service_display: upc.service_display, duration_min: upc.duration_min,
        start_iso: startUTC.toISOString(), end_iso: endUTC.toISOString(),
        staff_id: teamId, status:"confirmed", created_at:new Date().toISOString(), square_booking_id: sqNew.id || null
      })
    } else {
      updateApptTimes.run({ id: upc.id, start_iso: startUTC.toISOString(), end_iso: endUTC.toISOString(), staff_id: teamId })
    }

    clearSession.run({ phone })
    await safeSend(from,{ text:
`Cita actualizada.
Servicio: ${upc.service_display}
Nueva fecha: ${fmtES(startEU)}
Duración: ${upc.duration_min} min
Pago en persona.` })
  }catch(e){ console.error("finalizeReschedule:", e) }
  finally{ data.bookingInFlight=false; try{ saveSession(phone, data) }catch{} }
}

// ==== Lanzar
startBot().catch(console.error)
