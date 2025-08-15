// index.js — Gapink Nails · PROD (v7.4 “L–V 9–20 • multi-idioma • no-pasado • Square-safe”)

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
// L-V (sábado y domingo cerrado)
const WORK_DAYS = [1,2,3,4,5]
const OPEN_HOUR  = 9
const CLOSE_HOUR = 20
const SLOT_MIN   = 30

// Empujar huecos tempranos de la semana (sin ser pesado)
const STEER_ON = (process.env.BOT_STEER_BALANCE || "on").toLowerCase() === "on"
const STEER_WINDOW_DAYS = Number(process.env.BOT_STEER_WINDOW_DAYS || 7)
const SEARCH_WINDOW_DAYS = Number(process.env.BOT_SEARCH_WINDOW_DAYS || 14)
const MAX_SAME_DAY_DEVIATION_MIN = Number(process.env.BOT_MAX_SAME_DAY_DEVIATION_MIN || 60)
const STRICT_YES_DEVIATION_MIN = Number(process.env.BOT_STRICT_YES_DEVIATION_MIN || 45)
const NOW_MIN_OFFSET_MIN = Number(process.env.BOT_NOW_OFFSET_MIN || 30) // no pasado

// ===== OpenAI
const OPENAI_API_KEY  = process.env.OPENAI_API_KEY
const OPENAI_API_URL  = process.env.OPENAI_API_URL || "https://api.openai.com/v1/chat/completions"
const OPENAI_MODEL    = process.env.OPENAI_MODEL || "gpt-4o-mini"

async function aiChat(messages, { temperature=0.4 } = {}) {
  if (!OPENAI_API_KEY) return "" // modo silencioso sin IA
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
Habla natural, cercano y breve (sin emojis). No digas que eres IA.
Si la hora pedida exacta está libre, ofrécela tal cual. Si no, propone la más cercana razonable y pregunta si le viene bien (no impongas).
Nunca sugieras fuera de L–V 09:00–20:00 ni en pasado. Pago siempre en persona.
Si el cliente insiste en una hora concreta, evita repetir la misma sugerencia rechazada.`

async function aiSay(contextSummary) {
  return await aiChat([
    { role:"system", content: SYS_TONE },
    { role:"user", content: contextSummary }
  ], { temperature: 0.35 })
}

// ===== Helpers comunes
const onlyDigits = (s="") => (s||"").replace(/\D+/g,"")
const rmDiacritics = (s="") => s.normalize("NFD").replace(/\p{Diacritic}/gu,"")
const norm = (s="") => rmDiacritics(String(s).toLowerCase()).replace(/[^a-z0-9]+/g," ").trim()
const minutesApart=(a,b)=>Math.abs(a.diff(b,"minute"))
const sameMinute=(a,b)=>a && b && a.diff(b,"minute")===0
const clampFuture = (t)=> {
  const now = dayjs().tz(EURO_TZ).add(NOW_MIN_OFFSET_MIN,"minute").second(0).millisecond(0)
  return t.isBefore(now) ? now.clone() : t.clone()
}

const YES_RE = /\b(s[ií]|ok|okay|okey+|vale+|va|venga|dale|confirmo|confirmar|de acuerdo|perfecto|genial|yes|oui|sim|ja|si claro|ok dale)\b/i
const NO_RE  = /\b(no+|otra|cambia|no confirmo|mejor mas tarde|mejor más tarde|anula|cancela|cancel|annuler|stornare|nein|niet|nie)\b/i
const RESCH_RE = /\b(cambia|cambiar|modifica|mover|reprograma|reprogramar|edita|change|reschedul|verschieb|umbuch|rebook|aplaza|mueva)\b/i
const CANCEL_RE = /\b(cancela(?:r|me|la)?|anula(?:r|me|la)?|elimina(?:r|me|la)?|borra(?:r|me|la)?|quitar(?: la)? cita|anulaci[oó]n|cancel (my |mi )?appointment|annuler|storna|cancelar|delete appointment)\b/i

function normalizePhoneES(raw){
  const d=onlyDigits(raw); if(!d) return null
  if (raw.startsWith("+") && d.length>=8 && d.length<=15) return `+${d}`
  if (d.startsWith("34") && d.length===11) return `+${d}`
  if (d.length===9) return `+34${d}`
  if (d.startsWith("00")) return `+${d.slice(2)}`
  return `+${d}`
}
const isValidEmail=(e)=>/^[^\s@]+@[^\s@]+\.[^\s@]{2,}$/.test(String(e||"").trim())

// ===== Empleadas SOLO Playamar (variables de entorno)
const EMPLOYEES = {
  rocio:     process.env.SQ_EMP_ROCIO     || "",
  cristina:  process.env.SQ_EMP_CRISTINA  || "",
  sami:      process.env.SQ_EMP_SAMI      || "",
  elisabeth: process.env.SQ_EMP_ELISABETH || "",
  tania:     process.env.SQ_EMP_TANIA     || "",
  jamaica:   process.env.SQ_EMP_JAMAICA   || "",
  johana:    process.env.SQ_EMP_JOHANA    || "",
  chabeli:   process.env.SQ_EMP_CHABELI   || "",
  desi:      process.env.SQ_EMP_DESI      || "",
  martina:   process.env.SQ_EMP_MARTINA   || "",
  ginna:     process.env.SQ_EMP_GINNA     || "",
  edurne:    process.env.SQ_EMP_EDURNE    || "",
}
// nombre -> id rápido (ignora tildes)
const EMP_NAME_MAP = Object.fromEntries(Object.entries(EMPLOYEES).map(([k,v]) => [rmDiacritics(k), v]).filter(([_,v])=>!!v))
const EMP_KEYS = Object.keys(EMP_NAME_MAP)

// ===== Servicios (id|version desde .env) + duración (min). Fallback 60.
const SVC = {
  "BONO_5_SESIONES_MADEROTERAPIA_MAS_5_SESIONES_PUSH_UP": { env:"SQ_SVC_BONO_5_SESIONES_MADEROTERAPIA_MAS_5_SESIONES_PUSH_UP", name:"Bono 5 sesiones maderoterapia + 5 push up", dur:60 },
  "CEJAS_EFECTO_POLVO_MICROSHADING": { env:"SQ_SVC_CEJAS_EFECTO_POLVO_MICROSHADING", name:"Cejas efecto polvo microshading", dur:120 },
  "CEJAS_HAIRSTROKE": { env:"SQ_SVC_CEJAS_HAIRSTROKE", name:"Cejas Hairstroke", dur:30 },
  "DEPILACION_CEJAS_CON_HILO": { env:"SQ_SVC_DEPILACION_CEJAS_CON_HILO", name:"Depilación cejas con hilo", dur:15 },
  "DEPILACION_CEJAS_Y_LABIO_CON_HILO": { env:"SQ_SVC_DEPILACION_CEJAS_Y_LABIO_CON_HILO", name:"Depilación cejas y labio con hilo", dur:20 },
  "DEPILACION_DE_CEJAS_CON_PINZAS": { env:"SQ_SVC_DEPILACION_DE_CEJAS_CON_PINZAS", name:"Depilación cejas con pinzas", dur:15 },
  "DEPILACION_LABIO": { env:"SQ_SVC_DEPILACION_LABIO", name:"Depilación labio", dur:10 },
  "DEPILACION_LABIO_CON_HILO": { env:"SQ_SVC_DEPILACION_LABIO_CON_HILO", name:"Depilación labio con hilo", dur:10 },
  "DERMAPEN": { env:"SQ_SVC_DERMAPEN", name:"Dermapen", dur:60 },
  "DISENO_DE_CEJAS_CON_HENNA_Y_DEPILACION": { env:"SQ_SVC_DISENO_DE_CEJAS_CON_HENNA_Y_DEPILACION", name:"Diseño de cejas con henna y depilación", dur:30 },
  "ESMALTADO_SEMIPERMANETE_PIES": { env:"SQ_SVC_ESMALTADO_SEMIPERMANETE_PIES", name:"Esmaltado semipermanente pies", dur:30 },
  "EXTENSIONES_DE_PESTANAS_NUEVAS_PELO_A_PELO": { env:"SQ_SVC_EXTENSIONES_DE_PESTANAS_NUEVAS_PELO_A_PELO", name:"Extensiones nuevas pelo a pelo", dur:120 },
  "EXTENSIONES_PESTANAS_NUEVAS_2D": { env:"SQ_SVC_EXTENSIONES_PESTANAS_NUEVAS_2D", name:"Extensiones nuevas 2D", dur:120 },
  "EXTENSIONES_PESTANAS_NUEVAS_3D": { env:"SQ_SVC_EXTENSIONES_PESTANAS_NUEVAS_3D", name:"Extensiones nuevas 3D", dur:120 },
  "EYELINER": { env:"SQ_SVC_EYELINER", name:"Eyeliner", dur:150 },
  "FOSAS_NASALES": { env:"SQ_SVC_FOSAS_NASALES", name:"Fosas nasales", dur:10 },
  "FOTODEPILACION_AXILAS": { env:"SQ_SVC_FOTODEPILACION_AXILAS", name:"Fotodepilación axilas", dur:30 },
  "FOTODEPILACION_BRAZOS": { env:"SQ_SVC_FOTODEPILACION_BRAZOS", name:"Fotodepilación brazos", dur:30 },
  "FOTODEPILACION_FACIAL_COMPLETO": { env:"SQ_SVC_FOTODEPILACION_FACIAL_COMPLETO", name:"Fotodepilación facial completo", dur:30 },
  "FOTODEPILACION_INGLES": { env:"SQ_SVC_FOTODEPILACION_INGLES", name:"Fotodepilación ingles", dur:30 },
  "FOTODEPILACION_LABIO": { env:"SQ_SVC_FOTODEPILACION_LABIO", name:"Fotodepilación labio", dur:30 },
  "FOTODEPILACION_MEDIAS_PIERNAS": { env:"SQ_SVC_FOTODEPILACION_MEDIAS_PIERNAS", name:"Fotodepilación medias piernas", dur:30 },
  "FOTODEPILACION_PIERNAS_COMPLETAS": { env:"SQ_SVC_FOTODEPILACION_PIERNAS_COMPLETAS", name:"Fotodepilación piernas completas", dur:30 },
  "FOTODEPILACION_PIERNAS_COMPLETAS_AXILAS_PUBIS_COMPLETO": { env:"SQ_SVC_FOTODEPILACION_PIERNAS_COMPLETAS_AXILAS_PUBIS_COMPLETO", name:"Fotodepilación piernas completas, axilas y pubis completo", dur:60 },
  "FOTODEPILACION_PUBIS_COMPLETO_CON_PERIANAL": { env:"SQ_SVC_FOTODEPILACION_PUBIS_COMPLETO_CON_PERIANAL", name:"Fotodepilación pubis completo con perianal", dur:30 },
  "HYDRA_LIPS": { env:"SQ_SVC_HYDRA_LIPS", name:"Hydra lips", dur:60 },
  "LABIOS_EFECTO_AQUARELA": { env:"SQ_SVC_LABIOS_EFECTO_AQUARELA", name:"Labios efecto aquarela", dur:150 },
  "LAMINACION_Y_DISENO_DE_CEJAS": { env:"SQ_SVC_LAMINACION_Y_DISENO_DE_CEJAS", name:"Laminación y diseño de cejas", dur:30 },
  "LASER_CEJAS": { env:"SQ_SVC_LASER_CEJAS", name:"Láser cejas", dur:30 },
  "LIFITNG_DE_PESTANAS_Y_TINTE": { env:"SQ_SVC_LIFITNG_DE_PESTANAS_Y_TINTE", name:"Lifting de pestañas y tinte", dur:60 },
  "LIMPIEZA_FACIAL_BASICA": { env:"SQ_SVC_LIMPIEZA_FACIAL_BASICA", name:"Limpieza facial básica", dur:75 },
  "LIMPIEZA_FACIAL_CON_PUNTA_DE_DIAMANTE": { env:"SQ_SVC_LIMPIEZA_FACIAL_CON_PUNTA_DE_DIAMANTE", name:"Limpieza con punta de diamante", dur:90 },
  "LIMPIEZA_HYDRA_FACIAL": { env:"SQ_SVC_LIMPIEZA_HYDRA_FACIAL", name:"Limpieza hydra facial", dur:90 },
  "MADEROTERAPIA_MAS_PUSH_UP": { env:"SQ_SVC_MADEROTERAPIA_MAS_PUSH_UP", name:"Maderoterapia + push up", dur:60 },
  "MANICURA_CON_ESMALTE_NORMAL": { env:"SQ_SVC_MANICURA_CON_ESMALTE_NORMAL", name:"Manicura con esmalte normal", dur:30 },
  "MANICURA_RUSA_CON_NIVELACION": { env:"SQ_SVC_MANICURA_RUSA_CON_NIVELACION", name:"Manicura rusa con nivelación", dur:90 },
  "MANICURA_SEMIPERMANENTE": { env:"SQ_SVC_MANICURA_SEMIPERMANENTE", name:"Manicura semipermanente", dur:30 },
  "MANICURA_SEMIPERMANENTE_QUITAR": { env:"SQ_SVC_MANICURA_SEMIPERMANENTE_QUITAR", name:"Manicura semipermanente + quitar", dur:40 },
  "MANICURA_SEMIPERMANETE_CON_NIVELACION": { env:"SQ_SVC_MANICURA_SEMIPERMANETE_CON_NIVELACION", name:"Manicura semipermanente con nivelación", dur:60 },
  "MASAJE_RELAJANTE": { env:"SQ_SVC_MASAJE_RELAJANTE", name:"Masaje relajante", dur:60 },
  "MICROBLADING": { env:"SQ_SVC_MICROBLADING", name:"Microblading", dur:120 },
  "PEDICURA_GLAM_JELLY_CON_ESMALTE_NORMAL": { env:"SQ_SVC_PEDICURA_GLAM_JELLY_CON_ESMALTE_NORMAL", name:"Pedicura Glam Jelly (normal)", dur:60 },
  "PEDICURA_GLAM_JELLY_CON_ESMALTE_SEMIPERMANENTE": { env:"SQ_SVC_PEDICURA_GLAM_JELLY_CON_ESMALTE_SEMIPERMANENTE", name:"Pedicura Glam Jelly (semipermanente)", dur:60 },
  "PEDICURA_SPA_CON_ESMALTE_NORMAL": { env:"SQ_SVC_PEDICURA_SPA_CON_ESMALTE_NORMAL", name:"Pedicura spa (normal)", dur:60 },
  "PEDICURA_SPA_CON_ESMALTE_SEMIPERMANENTE": { env:"SQ_SVC_PEDICURA_SPA_CON_ESMALTE_SEMIPERMANENTE", name:"Pedicura spa (semipermanente)", dur:60 },
  "PEDICURA_SPA_CON_ESMALTE_SEMIPERMANENTE_2": { env:"SQ_SVC_PEDICURA_SPA_CON_ESMALTE_SEMIPERMANENTE_2", name:"Pedicura Spa con semipermanente", dur:60 },
  "QUITAR_ESMALTADO_SEMIPERMANENTE": { env:"SQ_SVC_QUITAR_ESMALTADO_SEMIPERMANENTE", name:"Quitar esmaltado semipermanente", dur:30 },
  "QUITAR_ESMALTADO_SEMIPERMANENTE_PIES": { env:"SQ_SVC_QUITAR_ESMALTADO_SEMIPERMANENTE_PIES", name:"Quitar esmaltado semipermanente (pies)", dur:30 },
  "QUITAR_EXTENSIONES_PESTANAS": { env:"SQ_SVC_QUITAR_EXTENSIONES_PESTANAS", name:"Quitar extensiones pestañas", dur:30 },
  "QUITAR_UNAS_ESCULPIDAS": { env:"SQ_SVC_QUITAR_UNAS_ESCULPIDAS", name:"Quitar uñas esculpidas", dur:45 },
  "RECONSTRUCCION_DE_UNA_UNA_PIE": { env:"SQ_SVC_RECONSTRUCCION_DE_UNA_UNA_PIE", name:"Reconstrucción una uña pie", dur:30 },
  "RELLENO_DE_UNAS_MAS_DE_4_SEMANAS": { env:"SQ_SVC_RELLENO_DE_UNAS_MAS_DE_4_SEMANAS", name:"Relleno uñas (más de 4 semanas)", dur:60 },
  "RELLENO_EXTENSIONES_PESTANAS_PELO_A_PELO": { env:"SQ_SVC_RELLENO_EXTENSIONES_PESTANAS_PELO_A_PELO", name:"Relleno pestañas pelo a pelo", dur:90 },
  "RELLENO_PESTANAS_2D": { env:"SQ_SVC_RELLENO_PESTANAS_2D", name:"Relleno pestañas 2D", dur:90 },
  "RELLENO_PESTANAS_3D": { env:"SQ_SVC_RELLENO_PESTANAS_3D", name:"Relleno pestañas 3D", dur:90 },
  "RELLENO_UNAS_ESCULPIDAS": { env:"SQ_SVC_RELLENO_UNAS_ESCULPIDAS", name:"Relleno uñas esculpidas", dur:60 },
  "RELLENO_UNAS_ESCULPIDAS_CON_FRANCESA_CONSTRUIDA_BABY_BOOMER_O_ENCAPSULADOS": { env:"SQ_SVC_RELLENO_UNAS_ESCULPIDAS_CON_FRANCESA_CONSTRUIDA_BABY_BOOMER_O_ENCAPSULADOS", name:"Relleno uñas esculpidas (francesa/baby/encapsulados)", dur:75 },
  "RELLENO_UNAS_ESCULPIDAS_CON_MANICURA_RUSA": { env:"SQ_SVC_RELLENO_UNAS_ESCULPIDAS_CON_MANICURA_RUSA", name:"Relleno uñas esculpidas con manicura rusa", dur:90 },
  "RELLENO_UNAS_ESCULPIDAS_EXTRA_LARGAS": { env:"SQ_SVC_RELLENO_UNAS_ESCULPIDAS_EXTRA_LARGAS", name:"Relleno uñas esculpidas extra largas", dur:90 },
  "RETOQUE_ANUAL_CEJAS": { env:"SQ_SVC_RETOQUE_ANUAL_CEJAS", name:"Retoque anual cejas", dur:90 },
  "RETOQUE_MES_CEJAS": { env:"SQ_SVC_RETOQUE_MES_CEJAS", name:"Retoque mes cejas", dur:60 },
  "SESION_ENDOSPHERE_FACIAL": { env:"SQ_SVC_SESION_ENDOSPHERE_FACIAL", name:"Sesión Endosphere facial", dur:60 },
  "SESION_ENDOSPHERE_CORPORAL": { env:"SQ_SVC_SESION_ENDOSPHERE_CORPORAL", name:"Sesión Endosphere corporal", dur:60 },
  "TRATAMIENDO_HIDRATANTE_LAMINAS_DE_ORO": { env:"SQ_SVC_TRATAMIENDO_HIDRATANTE_LAMINAS_DE_ORO", name:"Tratamiento hidratante láminas de oro", dur:60 },
  "TRATAMIENTO_ANTI_ACNE": { env:"SQ_SVC_TRATAMIENTO_ANTI_ACNE", name:"Tratamiento anti acné", dur:60 },
  "TRATAMIENTO_FACIAL_ANTI_MANCHAS": { env:"SQ_SVC_TRATAMIENTO_FACIAL_ANTI_MANCHAS", name:"Tratamiento facial anti manchas", dur:60 },
  "TRATAMIENTO_FACIAL_PIEDRAS_DE_JADE": { env:"SQ_SVC_TRATAMIENTO_FACIAL_PIEDRAS_DE_JADE", name:"Tratamiento facial piedras de jade", dur:70 },
  "TRATAMIENTO_HIDRATANTE_AZAFRAN": { env:"SQ_SVC_TRATAMIENTO_HIDRATANTE_AZAFRAN", name:"Tratamiento hidratante azafrán", dur:60 },
  "TRATAMIENTO_REAFIRMANTE_CON_VELO_DE_COLAGENO": { env:"SQ_SVC_TRATAMIENTO_REAFIRMANTE_CON_VELO_DE_COLAGENO", name:"Tratamiento reafirmante con velo de colágeno", dur:60 },
  "TRATAMIENTO_VITAMINA_C": { env:"SQ_SVC_TRATAMIENTO_VITAMINA_C", name:"Tratamiento vitamina C", dur:55 },
  "UNA_ROTA": { env:"SQ_SVC_UNA_ROTA", name:"Uña rota", dur:15 },
  "UNA_ROTA_DENTRO_DE_RELLENO": { env:"SQ_SVC_UNA_ROTA_DENTRO_DE_RELLENO", name:"Uña rota dentro de relleno", dur:15 },
  "UNA_ROTA_DENTRO_RELLENO": { env:"SQ_SVC_UNA_ROTA_DENTRO_RELLENO", name:"Uña rota dentro relleno", dur:15 },
  "UNAS_ESCULPIDAS_NUEVAS_EXTRA_LARGAS": { env:"SQ_SVC_UNAS_ESCULPIDAS_NUEVAS_EXTRA_LARGAS", name:"Uñas esculpidas nuevas extra largas", dur:120 },
  "UNAS_NUEVAS_ESCULPIDAS": { env:"SQ_SVC_UNAS_NUEVAS_ESCULPIDAS", name:"Uñas nuevas esculpidas", dur:90 },
  "UNAS_NUEVAS_ESCULPIDAS_CON_MANICURA_RUSA": { env:"SQ_SVC_UNAS_NUEVAS_ESCULPIDAS_CON_MANICURA_RUSA", name:"Uñas nuevas esculpidas con manicura rusa", dur:120 },
  "UNAS_NUEVAS_ESCULPIDAS_FRANCESA_BABY_BOOMER_ENCAPSULADOS": { env:"SQ_SVC_UNAS_NUEVAS_ESCULPIDAS_FRANCESA_BABY_BOOMER_ENCAPSULADOS", name:"Uñas nuevas: francesa/baby boomer/encapsulados", dur:120 },
  "UNAS_NUEVAS_FORMAS_SUPERIORES_Y_MANICURA_RUSA": { env:"SQ_SVC_UNAS_NUEVAS_FORMAS_SUPERIORES_Y_MANICURA_RUSA", name:"Uñas nuevas formas superiores + manicura rusa", dur:120 },
}
const SERVICE_KEYS = Object.keys(SVC)

// ==== Sinónimos multi-idioma (rápidos) -> clave SERVICE_KEYS
const SERVICE_SYNONYMS = [
  ["depilacion cejas hilo","DEPILACION_CEJAS_CON_HILO", ["threading","eyebrow threading","depilacion de cejas","cejas con hilo","depilación cejas"]],
  ["depilacion cejas y labio hilo","DEPILACION_CEJAS_Y_LABIO_CON_HILO", []],
  ["depilacion labio","DEPILACION_LABIO", ["upper lip"]],
  ["dermapen","DERMAPEN", ["microneedling","micro agujas"]],
  ["manicura semipermanente","MANICURA_SEMIPERMANENTE", ["semipermanent nails","semi"]],
  ["manicura semipermanente quitar","MANICURA_SEMIPERMANENTE_QUITAR", ["remove gel","retirar semi"]],
  ["manicura rusa","MANICURA_RUSA_CON_NIVELACION", ["russian manicure"]],
  ["pedicura spa","PEDICURA_SPA_CON_ESMALTE_NORMAL", ["spa pedicure"]],
  ["limpieza punta diamante","LIMPIEZA_FACIAL_CON_PUNTA_DE_DIAMANTE", ["diamond tip"]],
  ["limpieza hydra","LIMPIEZA_HYDRA_FACIAL", ["hydrafacial","hydra facial"]],
  ["lifting pestanas tinte","LIFITNG_DE_PESTANAS_Y_TINTE", ["lash lift tint","lifting de pestañas"]],
  ["extensiones pelo a pelo","EXTENSIONES_DE_PESTANAS_NUEVAS_PELO_A_PELO", ["classic lashes"]],
  ["extensiones 2d","EXTENSIONES_PESTANAS_NUEVAS_2D", []],
  ["extensiones 3d","EXTENSIONES_PESTANAS_NUEVAS_3D", []],
  ["microblading","MICROBLADING", []],
  ["eyeliner","EYELINER", ["delineado"]],
]

// ===== IA extracción mínima (solo para redacción amable)
async function extractFromText(userText="") {
  try {
    const schema = `Devuelve solo JSON: {"service_text":"...","datetime_text":"...","name":"...","email":"..."}`
    const content = await aiChat([
      { role:"system", content: `${SYS_TONE}\n${schema}\nEspañol neutro.` },
      { role:"user", content: userText }
    ], { temperature: 0.2 })
    const jsonStr = (content||"").trim().replace(/^```(json)?/i,"").replace(/```$/,"")
    try { return JSON.parse(jsonStr) } catch { return {} }
  } catch { return {} }
}

// ===== Parse fecha/hora multi-idioma
const DOW_WORDS = {
  // es / en / it / fr / pt / de
  "lunes":1,"monday":1,"lunedi":1,"lunedì":1,"lundi":1,"segunda":1,"segunda-feira":1,"montag":1,
  "martes":2,"tuesday":2,"martedi":2,"mardi":2,"terca":2,"terça":2,"terca-feira":2,"terça-feira":2,"dienstag":2,
  "miercoles":3,"miércoles":3,"wednesday":3,"mercoledi":3,"mercredi":3,"quarta":3,"quarta-feira":3,"mittwoch":3,
  "jueves":4,"thursday":4,"giovedi":4,"giovedì":4,"jeudi":4,"quinta":4,"quinta-feira":4,"donnerstag":4,
  "viernes":5,"friday":5,"venerdi":5,"venerdì":5,"vendredi":5,"sexta":5,"sexta-feira":5,"freitag":5,
  "sabado":6,"sábado":6,"saturday":6,"sabato":6,"samedi":6,"sabado-pt":6,"samstag":6,
  "domingo":0,"sunday":0,"domenica":0,"dimanche":0,"domingo-pt":0,"sonntag":0
}
const WHEN_WORDS = {
  "hoy":0,"today":0,"oggi":0,"aujourd'hui":0,"hoje":0,"heute":0,
  "manana":1,"mañana":1,"tomorrow":1,"domani":1,"demain":1,"amanha":1,"amanhã":1,"morgen":1
}

function parseDateTimeMulti(text){
  if(!text) return null
  const t = rmDiacritics(text.toLowerCase())
  // fecha dd/mm(/yyyy) o dd-mm
  const m = t.match(/\b(\d{1,2})[\/\-](\d{1,2})(?:[\/\-](\d{2,4}))?\b/)
  let base = null
  if (m) {
    let dd = +m[1], mm = +m[2], yy = m[3] ? +m[3] : dayjs().tz(EURO_TZ).year()
    if (yy < 100) yy += 2000
    base = dayjs.tz(`${yy}-${String(mm).padStart(2,"0")}-${String(dd).padStart(2,"0")} 00:00`, EURO_TZ)
  } else {
    // hoy/mañana
    for (const [w,off] of Object.entries(WHEN_WORDS)) {
      if (t.includes(w)) { base = dayjs().tz(EURO_TZ).add(off,"day").startOf("day"); break }
    }
    // día de la semana próximo
    if (!base) {
      for (const [w,dow] of Object.entries(DOW_WORDS)) {
        if (t.includes(w)) {
          const now = dayjs().tz(EURO_TZ)
          let cand = now.startOf("day")
          const nowDow = cand.day()
          let delta = (dow - nowDow + 7) % 7
          if (delta===0 && now.hour() >= CLOSE_HOUR) delta = 7 // si hoy pero ya tarde, pasa a la semana que viene
          cand = cand.add(delta,"day")
          base = cand
          break
        }
      }
    }
    // por defecto, hoy
    if (!base) base = dayjs().tz(EURO_TZ).startOf("day")
  }

  // hora hh(:mm)? + am/pm opcional o “a las 10”
  let hour=null, minute=0
  const hm = t.match(/(?:a\s+las\s+)?(\d{1,2})(?::(\d{2}))?\s*(am|pm)?\b/)
  if (hm) {
    hour = +hm[1]; minute = hm[2] ? +hm[2] : 0
    const ap = hm[3]
    if (ap==="pm" && hour<12) hour+=12
    if (ap==="am" && hour===12) hour=0
  }
  if (hour===null) return null
  let dt = base.hour(hour).minute(minute).second(0).millisecond(0)
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
  try{
    const resp=await square.catalogApi.retrieveCatalogObject(id,true)
    return resp?.result?.object?.version
  }catch(e){ console.error("getServiceVariationVersion:",e?.message||e); return undefined }
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

// ===== Disponibilidad
function getBookedIntervals(fromIso,toIso){
  const rows=db.prepare(`SELECT start_iso,end_iso,staff_id FROM appointments WHERE status IN ('pending','confirmed') AND start_iso < @to AND end_iso > @from`).all({from:fromIso,to:toIso})
  return rows.map(r=>({start:dayjs(r.start_iso),end:dayjs(r.end_iso),staff_id:r.staff_id}))
}
function findFreeStaff(intervals,start,end,preferred){
  const base = Object.values(EMP_NAME_MAP).filter(Boolean)
  const ids = preferred && base.includes(preferred) ? [preferred, ...base.filter(x=>x!==preferred)] : base
  for(const id of ids){
    const busy = intervals.filter(i=>i.staff_id===id).some(i => (start<i.end) && (i.start<end))
    if(!busy) return id
  }
  return null
}
function insideBusinessHours(startEU, durationMin){
  const day = startEU.day()
  if (!WORK_DAYS.includes(day)) return false
  const dayStart=startEU.clone().hour(OPEN_HOUR).minute(0).second(0)
  const dayEnd=startEU.clone().hour(CLOSE_HOUR).minute(0).second(0)
  const endEU = startEU.clone().add(durationMin,"minute")
  return startEU.isSameOrAfter(dayStart) && endEU.isSameOrBefore(dayEnd)
}

function suggestOrExact(startEU, durationMin, preferredStaffId=null, avoidMs=null){
  const now=dayjs().tz(EURO_TZ).add(NOW_MIN_OFFSET_MIN,"minute").second(0).millisecond(0)
  const from = now.tz("UTC").toISOString(), to = now.add(SEARCH_WINDOW_DAYS,"day").tz("UTC").toISOString()
  const intervals=getBookedIntervals(from,to)

  let req = ceilToSlotEU(clampFuture(startEU.clone()))
  if (!insideBusinessHours(req,durationMin)) {
    // si la hora pedida cae fuera, intenta “colarla” al inicio o fin del día
    const dayStart=req.clone().hour(OPEN_HOUR).minute(0).second(0)
    if (insideBusinessHours(dayStart,durationMin)) req = dayStart
    else return { exact:null, suggestion:null, staffId:null }
  }

  const dayStart=req.clone().hour(OPEN_HOUR).minute(0).second(0)
  const dayEnd=req.clone().hour(CLOSE_HOUR).minute(0).second(0)

  const exactId = findFreeStaff(intervals, req.tz("UTC"), req.clone().add(durationMin,"minute").tz("UTC"), preferredStaffId)
  if (exactId && (!avoidMs || req.valueOf()!==avoidMs)) {
    return { exact: req, suggestion:null, staffId: exactId }
  }

  // buscar mismo día, cercanas (hasta MAX_SAME_DAY_DEVIATION_MIN)
  for (let t=req.clone(); t.isSameOrBefore(dayEnd); t=t.add(SLOT_MIN,"minute")) {
    const e=t.clone().add(durationMin,"minute")
    if (!insideBusinessHours(t,durationMin)) break
    if (avoidMs && t.valueOf()===avoidMs) continue
    const id=findFreeStaff(intervals, t.tz("UTC"), e.tz("UTC"), preferredStaffId)
    if(id) return { exact:null, suggestion:t, staffId:id }
    if (minutesApart(t, req) > MAX_SAME_DAY_DEVIATION_MIN) break
  }

  // si nada en el mismo día, buscar hacia adelante (balance de semana)
  const windowDays = STEER_ON ? STEER_WINDOW_DAYS : 1
  const limit = dayjs.max(req.clone(), now.clone()).add(windowDays,"day").endOf("day")
  for (let d=req.clone().add(1,"day").startOf("day"); d.isSameOrBefore(limit); d=d.add(1,"day")) {
    if (!WORK_DAYS.includes(d.day())) continue
    for (let t=d.clone().hour(OPEN_HOUR).minute(0); t.isBefore(d.clone().hour(CLOSE_HOUR)); t=t.add(SLOT_MIN,"minute")) {
      const e=t.clone().add(durationMin,"minute")
      if (avoidMs && t.valueOf()===avoidMs) continue
      const id=findFreeStaff(intervals, t.tz("UTC"), e.tz("UTC"), preferredStaffId)
      if(id) return { exact:null, suggestion:t, staffId:id }
    }
  }
  return { exact:null, suggestion:null, staffId:null }
}

// ===== Mini web
const app=express()
const PORT=process.env.PORT||8080
let lastQR=null,conectado=false
app.get("/",(_req,res)=>{res.send(`<!doctype html><meta charset="utf-8"><style>body{font-family:system-ui;display:grid;place-items:center;min-height:100vh;background:linear-gradient(135deg,#fce4ec,#f8bbd0);color:#4a148c} .card{background:#fff;padding:24px;border-radius:16px;box-shadow:0 6px 24px rgba(0,0,0,.08);text-align:center;max-width:520px}</style><div class="card"><h1>Gapink Nails</h1><p>Estado: ${conectado?"✅ Conectado":"❌ Desconectado"}</p>${!conectado&&lastQR?`<img src="/qr.png" width="320" />`:``}<p><small>Hecho por <a href="https://gonzalog.co" target="_blank" rel="noopener">Gonzalo García Aranda</a></small></p></div>`)})
app.get("/qr.png",async(_req,res)=>{if(!lastQR)return res.status(404).send("No hay QR");const png=await qrcode.toBuffer(lastQR,{type:"png",width:512,margin:1});res.set("Content-Type","image/png").send(png)})

const wait=(ms)=>new Promise(r=>setTimeout(r,ms))

// ===== WhatsApp
let booting=false, sock=null, reconnectTimer=null
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
    sock=makeWASocket({logger:pino({level:"silent"}),printQRInTerminal:false,auth:state,version,browser:Browsers.macOS("Desktop"),syncFullHistory:false,connectTimeoutMs:30000})

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
        const body=m.message.conversation||m.message.extendedTextMessage?.text||m.message?.imageMessage?.caption||""
        const textRaw=(body||"").trim()
        const low = rmDiacritics(textRaw.toLowerCase())

        // Sesión
        let data=loadSession(phone)||{
          serviceKey:null, serviceName:null, startEU:null, durationMin:null, preferredStaffId:null,
          name:null,email:null,
          confirmApproved:false,confirmAsked:false,bookingInFlight:false,
          lastUserDtText:null, lastSuggestedMs:null, insistExact:false,
          editBookingId:null // si está reprogramando
        }

        // idioma/servicio rápido por sinónimos + fuzzy
        let askedServiceKey = detectServiceFromText(low)
        if (askedServiceKey && askedServiceKey !== data.serviceKey) {
          data.serviceKey = askedServiceKey
          data.serviceName = SVC[askedServiceKey]?.name || askedServiceKey
          data.durationMin = SVC[askedServiceKey]?.dur || 60
          data.confirmApproved = false; data.confirmAsked = false
        }

        // ¿profesional preferido?
        const staffKey = EMP_KEYS.find(k => low.includes(k))
        if (staffKey) data.preferredStaffId = EMP_NAME_MAP[staffKey]

        // ¿cancelar o reprogramar?
        if (CANCEL_RE.test(low)) {
          const upc = getUpcomingByPhone.get({ phone, now: dayjs().utc().toISOString() })
          if (upc?.square_booking_id) {
            const ok = await cancelSquareBooking(upc.square_booking_id)
            if (ok) { markCancelled.run({ id: upc.id }); clearSession.run({ phone }); await __SAFE_SEND__(from,{ text:`He cancelado tu cita del ${fmtES(dayjs(upc.start_iso))}.` }); return }
          }
          await __SAFE_SEND__(from,{ text:"No veo ninguna cita futura para cancelar ahora mismo." })
          return
        }
        if (RESCH_RE.test(low)) {
          const upc = getUpcomingByPhone.get({ phone, now: dayjs().utc().toISOString() })
          if (upc) {
            data.editBookingId = upc.id
            data.serviceKey = upc.service_key
            data.serviceName = upc.service_name
            data.durationMin = upc.duration_min
            data.preferredStaffId = upc.staff_id
            saveSession(phone,data)
            await __SAFE_SEND__(from,{ text:`Ok, dime la nueva fecha y hora (ej: “lunes 10:00”).` })
            return
          }
        }

        // extra AI (por si nos da un hint de texto fecha/email/nombre)
        const extra = await extractFromText(textRaw)
        if (!data.name && extra?.name) data.name = (extra.name+"").trim().slice(0,64)
        if (!data.email && extra?.email && isValidEmail(extra.email)) data.email = extra.email.trim()

        // Confirmación explícita del usuario
        const userSaysYes = YES_RE.test(textRaw)
        const userSaysNo  = NO_RE.test(textRaw)
        if (userSaysYes) data.confirmApproved = true
        if (userSaysNo)  { data.confirmApproved=false; data.confirmAsked=false; data.insistExact=true }

        // Fecha/hora
        let incomingDt = extra?.datetime_text || null
        if (!incomingDt) {
          // si el mensaje parece ser solo fecha/hora
          if (/\d{1,2}[\/\-]\d{1,2}/.test(low) || /\b(lunes|martes|miercoles|miércoles|jueves|viernes|monday|tuesday|wednesday|thursday|friday)\b/.test(low) || /\b(\d{1,2})(?::\d{2})?\s*(am|pm)?\b/.test(low)) {
            incomingDt = textRaw
          }
        }
        const parsed = parseDateTimeMulti(incomingDt ? incomingDt : textRaw)
        if (parsed) data.startEU = parsed

        // validar negocio
        if (data.serviceKey && !data.durationMin) data.durationMin = SVC[data.serviceKey]?.dur || 60

        // guardar sesión pronto
        saveSession(phone, data)

        // flujo: falta servicio
        if (!data.serviceKey) {
          await __SAFE_SEND__(from,{ text:`¿Qué te hacemos? (ejemplos: “Manicura semipermanente”, “Depilación cejas con hilo”, “Limpieza hydra facial”)` })
          return
        }

        // flujo: me dieron fecha/hora
        if (data.startEU && data.durationMin) {
          const { exact, suggestion, staffId } = suggestOrExact(data.startEU, data.durationMin, data.preferredStaffId, data.lastSuggestedMs)
          // si hay exacto => ofrecer tal cual
          if (exact) {
            data.startEU = exact; data.preferredStaffId = staffId
            data.confirmAsked = true; data.lastSuggestedMs = exact.valueOf()
            saveSession(phone,data)
            await __SAFE_SEND__(from,{ text:`Tengo libre ${fmtES(data.startEU)} para ${data.serviceName}${staffId? "":""}. ¿Confirmo la ${data.editBookingId?"modificación":"cita"}? (Pago en persona)` })
            return
          }
          // si hay sugerencia => no repetir la misma, preguntar si le cuadra
          if (suggestion) {
            data.startEU = suggestion; data.preferredStaffId = staffId
            data.confirmAsked = true; data.confirmApproved=false; data.lastSuggestedMs = suggestion.valueOf()
            saveSession(phone,data)
            await __SAFE_SEND__(from,{ text:`No tengo ese hueco exacto. Te puedo ofrecer ${fmtES(data.startEU)}${staffId?"":""}. ¿Te viene bien? Si prefieres otra hora/día o con alguien en concreto, dime y lo miro.` })
            return
          }
          // nada: pedir otra franja
          data.confirmAsked=false; saveSession(phone,data)
          await __SAFE_SEND__(from,{ text:`No veo hueco en esa franja. Dime otra hora o día (L–V 09:00–20:00) y te digo.` })
          return
        }

        // Si el usuario dice “sí” y ya tenemos todo -> cerrar
        if (data.confirmAsked && userSaysYes && data.serviceKey && data.startEU && data.durationMin) {
          if (data.editBookingId) await finalizeReschedule({ from, phone, data, safeSend: __SAFE_SEND__ })
          else await finalizeBooking({ from, phone, data, safeSend: __SAFE_SEND__ })
          return
        }

        // Si dijo sí pero faltan nombre/email para alta
        if (userSaysYes && (!data.name || !data.email) && !data.editBookingId) {
          if (!data.name) { await __SAFE_SEND__(from,{ text:"Para cerrar, dime tu *nombre y apellidos*." }); return }
          if (!data.email) { await __SAFE_SEND__(from,{ text:"Genial. Ahora tu email (tipo: nombre@correo.com)." }); return }
        }

        // fallback: pedir fecha/hora
        if (!data.startEU) {
          await __SAFE_SEND__(from,{ text:`Perfecto, ${data.serviceName}. Dime día y hora (ej: “lunes 10:00” o “18/08 10:00”).` })
          return
        }

      }catch(e){ console.error("messages.upsert error:",e) }
    })
  }catch(e){ console.error("startBot error:",e) }
}

// ===== Booking alta
async function finalizeBooking({ from, phone, data, safeSend }) {
  try {
    if (data.bookingInFlight) return
    data.bookingInFlight = true; saveSession(phone, data)

    // cliente
    let customer = await squareFindCustomerByPhone(phone)
    if (!customer) {
      if (!data.name) { await safeSend(from,{ text:"Para cerrar, dime tu *nombre y apellidos*." }); data.bookingInFlight=false; saveSession(phone,data); return }
      if (!data.email || !isValidEmail(data.email)) { await safeSend(from,{ text:"Ok. Ahora tu email válido (tipo: nombre@correo.com)." }); data.bookingInFlight=false; saveSession(phone,data); return }
      customer = await squareCreateCustomer({ givenName: data.name, emailAddress: data.email, phoneNumber: phone })
      // si email malo, Square 400 -> volvemos a pedir
      if (!customer) { await safeSend(from,{ text:"No pude crear el cliente con ese email. Mándame un email válido y seguimos." }); data.bookingInFlight=false; saveSession(phone,data); return }
    }

    const startEU = dayjs.isDayjs(data.startEU) ? data.startEU : (data.startEU_ms ? dayjs.tz(Number(data.startEU_ms), EURO_TZ) : null)
    if (!startEU || !startEU.isValid() || !insideBusinessHours(startEU, data.durationMin)) { data.bookingInFlight=false; saveSession(phone,data); return }
    const teamMemberId = data.preferredStaffId || Object.values(EMP_NAME_MAP)[0] || null
    if(!teamMemberId){ await safeSend(from,{ text:"Ahora mismo no puedo asignar profesional. Dime si te da igual con quién o prefieres a alguien." }); data.bookingInFlight=false; saveSession(phone,data); return }

    const durationMin = data.durationMin
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
Fecha: ${fmtES(startEU)}${teamMemberId?"":""}
Duración: ${durationMin} min
Pago en persona.` })
  } catch (e) { console.error("finalizeBooking:", e) }
  finally { data.bookingInFlight=false; try{ saveSession(phone, data) }catch{} }
}

// ===== Edición (reprogramar)
async function finalizeReschedule({ from, phone, data, safeSend }) {
  try{
    if (data.bookingInFlight) return
    data.bookingInFlight = true; saveSession(phone, data)

    const upc = getUpcomingByPhone.get({ phone, now: dayjs().utc().toISOString() })
    if (!upc || upc.id !== data.editBookingId) { data.bookingInFlight=false; saveSession(phone,data); return }

    const startEU = dayjs.isDayjs(data.startEU) ? data.startEU : (data.startEU_ms ? dayjs.tz(Number(data.startEU_ms), EURO_TZ) : null)
    if (!startEU || !startEU.isValid() || !insideBusinessHours(startEU, upc.duration_min)) { data.bookingInFlight=false; saveSession(phone,data); return }

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

// ===== Square helpers clientes
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

// ===== Servicio: detección por texto
function detectServiceFromText(textLow){
  // 1) atajo por sinónimos
  for (const [label,key,extra] of SERVICE_SYNONYMS) {
    const l = rmDiacritics(label)
    if (textLow.includes(l) || (extra||[]).some(x => textLow.includes(rmDiacritics(x)))) return key
  }
  // 2) fuzzy simple por tokens
  const tokens = norm(textLow).split(/\s+/).filter(Boolean)
  let best=null, bestScore=0
  for (const k of SERVICE_KEYS) {
    const name = rmDiacritics((SVC[k]?.name||k).toLowerCase())
    const words = name.split(/\s+/)
    let score=0
    for (const t of tokens) if (words.includes(t)) score++
    // bonus por subcadena
    if (name.includes(tokens.join(" "))) score+=2
    if (score>bestScore) { bestScore=score; best=k }
  }
  if (bestScore>=1) return best
  return null
}
