// index.js
// ------------------------------
// Gapink Nails – WhatsApp Bot + IA + Square
// ------------------------------
import 'dotenv/config'
import fs from 'fs'
import path from 'path'
import crypto from 'crypto'
import {
  makeWASocket,
  useMultiFileAuthState,
  DisconnectReason,
  fetchLatestBaileysVersion,
  Browsers
} from '@whiskeysockets/baileys'

import dayjsBase from 'dayjs'
import tz from 'dayjs/plugin/timezone.js'
import utc from 'dayjs/plugin/utc.js'
import customParse from 'dayjs/plugin/customParseFormat.js'

dayjsBase.extend(utc)
dayjsBase.extend(tz)
dayjsBase.extend(customParse)

const dayjs = (d) => dayjsBase.tz(d, 'Europe/Madrid')

// ------------------------------
// ENV
// ------------------------------
const {
  OPENAI_API_KEY,
  SQUARE_ACCESS_TOKEN,
  SQUARE_API_VER = '2023-12-13',
  // Locales
  SQUARE_LOCATION_ID_TORREMOLINOS = process.env.SQUARE_LOCATION_ID_TORREMOLINOS || 'LSMNAJFSY1EGS',
  SQUARE_LOCATION_ID_LA_LUZ = process.env.SQUARE_LOCATION_ID_LA_LUZ || 'LF5NK1R8RDMRV',
  ADDRESS_TORREMOLINOS = 'Av. de Benyamina 18, Torremolinos',
  ADDRESS_LA_LUZ = 'Málaga – Barrio de La Luz',
  HOLIDAYS_EXTRA = '06/01,28/02,15/08,12/10,01/11,06/12,08/12,25/12',
  BOT_STEER_BALANCE = 'on',
  BOT_STEER_WINDOW_DAYS = 7,
  BOT_SEARCH_WINDOW_DAYS = 14,
  BOT_MAX_SAME_DAY_DEVIATION_MIN = 60,
  BOT_NOW_OFFSET_MIN = 30
} = process.env

if (!SQUARE_ACCESS_TOKEN) {
  console.warn('⚠️ Falta SQUARE_ACCESS_TOKEN en .env')
}
if (!OPENAI_API_KEY) {
  console.warn('⚠️ Falta OPENAI_API_KEY en .env (la IA seguirá pero solo con reglas)')
}

// ------------------------------
// CATÁLOGO (IDs desde .env)
// Admite claves normales (SQ_SVC_...) y con prefijo _luz_
// ------------------------------
function readServicesFromEnv () {
  const map = {}
  for (const [k, v] of Object.entries(process.env)) {
    if (!/^SQ_SVC_/.test(k)) continue
    const [id, version] = String(v).split('|')
    map[k] = { id, version: version || '' }
  }
  return map
}
const SVC_IDS = readServicesFromEnv()

// Duraciones por defecto (min). Puedes ajustar aquí libremente:
const SVC_DUR = {
  LIFITNG_DE_PESTANAS_Y_TINTE: 75,
  EXTENSIONES_DE_PESTANAS_NUEVAS_PELO_A_PELO: 120,
  EXTENSIONES_PESTANAS_NUEVAS_2D: 120,
  EXTENSIONES_PESTANAS_NUEVAS_3D: 135,
  RELLENO_EXTENSIONES_PESTANAS_PELO_A_PELO: 90,
  RELLENO_PESTANAS_2D: 95,
  RELLENO_PESTANAS_3D: 100,
  QUITAR_EXTENSIONES_PESTANAS: 30,

  MANICURA_SEMIPERMANENTE: 60,
  MANICURA_SEMIPERMANENTE_QUITAR: 25,
  MANICURA_CON_ESMALTE_NORMAL: 45,
  PEDICURA_SPA_CON_ESMALTE_SEMIPERMANENTE: 75
}

// Mapa canónico KEY -> env key real
const KEY_TO_ENV = {
  // Lashes
  LIFITNG_DE_PESTANAS_Y_TINTE: 'SQ_SVC_LIFITNG_DE_PESTANAS_Y_TINTE',
  EXTENSIONES_DE_PESTANAS_NUEVAS_PELO_A_PELO: 'SQ_SVC_EXTENSIONES_DE_PESTANAS_NUEVAS_PELO_A_PELO',
  EXTENSIONES_PESTANAS_NUEVAS_2D: 'SQ_SVC_EXTENSIONES_PESTANAS_NUEVAS_2D',
  EXTENSIONES_PESTANAS_NUEVAS_3D: 'SQ_SVC_EXTENSIONES_PESTANAS_NUEVAS_3D',
  RELLENO_EXTENSIONES_PESTANAS_PELO_A_PELO: 'SQ_SVC_RELLENO_EXTENSIONES_PESTANAS_PELO_A_PELO',
  RELLENO_PESTANAS_2D: 'SQ_SVC_RELLENO_PESTANAS_2D',
  RELLENO_PESTANAS_3D: 'SQ_SVC_RELLENO_PESTANAS_3D',
  QUITAR_EXTENSIONES_PESTANAS: 'SQ_SVC_QUITAR_EXTENSIONES_PESTANAS',
  // Uñas / pies (ejemplos más usados)
  MANICURA_SEMIPERMANENTE: 'SQ_SVC_MANICURA_SEMIPERMANENTE',
  MANICURA_SEMIPERMANENTE_QUITAR: 'SQ_SVC_MANICURA_SEMIPERMANENTE_QUITAR',
  MANICURA_CON_ESMALTE_NORMAL: 'SQ_SVC_MANICURA_CON_ESMALTE_NORMAL',
  PEDICURA_SPA_CON_ESMALTE_SEMIPERMANENTE: 'SQ_SVC_PEDICURA_SPA_CON_ESMALTE_SEMIPERMANENTE'
}

// Empleadas conocidas (desde env que pegaste)
const STAFF_NAME_TO_ID = {
  'carmen belen': 'TM4mRkFCodl_mDWf',
  'ganna': 'TMW_ctTgpIyIOeLa',
  'rocio': 'TMdIMb5NTeGacSTN',
  'rocio chica': 'TMhJsD5RnV9hmEcG',
  'maria': 'TMhpeTVah4qtuUtP',
  'anais': 'TMtlbg7yOf7o0sf0',
  'anaira': 'TMtlbg7yOf7o0sf0',
  'desi': 'TMXSDoPYYoscyMVH', // NO_BOOKABLE en tu dump: la API podría no aceptar; se intentará y si no, sin staff
  'daniela': 'TMTzygZYYb2JNC3z'
}

// ------------------------------
// WhatsApp – helpers
// ------------------------------
const SESSIONS = new Map() // por número

function getS (jid) {
  if (!SESSIONS.has(jid)) {
    SESSIONS.set(jid, {
      step: 'INIT',
      history: [],
      // recogida
      locationKey: null, // TORRE / LUZ
      locationId: null,
      serviceKey: null,
      durationMin: 60,
      staffName: null,
      staffId: null,
      offersISO: [], // ISO strings
      selectedISO: null,
      name: null,
      phone: null
    })
  }
  return SESSIONS.get(jid)
}

function norm (s) {
  return String(s || '')
    .toLowerCase()
    .normalize('NFD')
    .replace(/\p{Diacritic}/gu, '')
    .replace(/\s+/g, ' ')
    .trim()
}

// ------------------------------
// Horario y festivos
// ------------------------------
const EXTRA_HOLIDAYS = (HOLIDAYS_EXTRA || '')
  .split(',')
  .map(s => s.trim())
  .filter(Boolean) // en formato dd/mm

function isHoliday (d = dayjs()) {
  const tag = d.format('DD/MM')
  return EXTRA_HOLIDAYS.includes(tag)
}
function isOpenNow () {
  const now = dayjs()
  if (isHoliday(now)) return false
  const wd = now.day() // 0 Sun .. 6 Sat
  if (wd === 0 || wd === 6) return false
  const hm = now.format('HH:mm')
  return (hm >= '10:00' && hm < '14:00') || (hm >= '16:00' && hm < '20:00')
}
function openingMsg () {
  return `Solo atenderemos por WhatsApp y llamadas en horario de lunes a viernes de 10 a 14:00 y de 16:00 a 20:00`
}

// ------------------------------
// OpenAI – súper cauto
// ------------------------------
async function aiChat (messages) {
  if (!OPENAI_API_KEY) return ''
  const r = await fetch('https://api.openai.com/v1/chat/completions', {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${OPENAI_API_KEY}`,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({
      model: 'gpt-4o-mini',
      temperature: 0.1,
      messages
    })
  })
  if (!r.ok) {
    console.warn('AI HTTP', await r.text())
    return ''
  }
  const j = await r.json()
  return j.choices?.[0]?.message?.content || ''
}

function isValidEmail (s) {
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(String(s || '').trim())
}

// ------------------------------
// Detección de local
// ------------------------------
function detectLocation (text) {
  const t = norm(text)
  if (/\b(torre|torremolinos|beni?amina|benyamina)\b/.test(t)) {
    return { key: 'TORRE', id: SQUARE_LOCATION_ID_TORREMOLINOS, label: `Torremolinos (${ADDRESS_TORREMOLINOS})` }
  }
  if (/\b(luz|malaga|málaga|barrio de la luz|centro|la luz)\b/.test(t)) {
    return { key: 'LUZ', id: SQUARE_LOCATION_ID_LA_LUZ, label: `Málaga – La Luz (${ADDRESS_LA_LUZ})` }
  }
  return null
}

// ------------------------------
// Detección de servicio (reglas + historial + IA)
// ------------------------------
function detectService (text) {
  const t = norm(text)

  // Lashes – quitar/retirar
  if (/\b(quitar(me)?|retirar(me)?|sacar(me)?)\b.*\b(extensi(?:on|ones)|pestanas?|pestañas?)\b/.test(t)) { return 'QUITAR_EXTENSIONES_PESTANAS' }

  // Lashes – lifting + tinte
  if ((/\blifting\b/.test(t) || /\blift\b/.test(t)) && (/\btinte\b/.test(t) || /\btint[ae]\b/.test(t))) { return 'LIFITNG_DE_PESTANAS_Y_TINTE' }

  // Lashes – nuevas
  if (/\b(extensi(?:on|ones)|pestanas?)\b.*\b(2d)\b/.test(t)) return 'EXTENSIONES_PESTANAS_NUEVAS_2D'
  if (/\b(extensi(?:on|ones)|pestanas?)\b.*\b(3d)\b/.test(t)) return 'EXTENSIONES_PESTANAS_NUEVAS_3D'
  if (/\b(extensi(?:on|ones)|pestanas?)\b.*\b(pelo a pelo|clasicas|clásicas|classic)\b/.test(t)) {
    return 'EXTENSIONES_DE_PESTANAS_NUEVAS_PELO_A_PELO'
  }
  // Rellenos
  if (/\brellen[oa]s?\b.*\b(2d)\b/.test(t)) return 'RELLENO_PESTANAS_2D'
  if (/\brellen[oa]s?\b.*\b(3d)\b/.test(t)) return 'RELLENO_PESTANAS_3D'
  if (/\brellen[oa]s?\b.*\b(pelo a pelo|clasicas|clásicas)\b/.test(t)) return 'RELLENO_EXTENSIONES_PESTANAS_PELO_A_PELO'

  // Uñas rápidas
  if (/\bmanicur[ae]\b.*\bsemi\b/.test(t) || /\bsemiperm(?:anente)?\b/.test(t)) return 'MANICURA_SEMIPERMANENTE'
  if (/\bmanicur[ae]\b.*\bquitar\b/.test(t)) return 'MANICURA_SEMIPERMANENTE_QUITAR'
  if (/\bmanicur[ae]\b.*\bnormal\b/.test(t)) return 'MANICURA_CON_ESMALTE_NORMAL'
  if (/\bpedicur[ae]\b.*\bsemi\b/.test(t)) return 'PEDICURA_SPA_CON_ESMALTE_SEMIPERMANENTE'

  return null
}

function detectServiceFromHistory (historyArr = []) {
  const t = norm((historyArr || []).slice(-8).join(' ␟ '))
  if (/\bpestan(?:as|as)\b/.test(t) && /\b(quitar(me)?|retirar(me)?|sacar(me)?)\b/.test(t)) {
    return 'QUITAR_EXTENSIONES_PESTANAS'
  }
  return null
}

async function aiDetectService (historyArr = []) {
  if (!OPENAI_API_KEY) return null
  const last = (historyArr || []).slice(-8).join('\n')
  const allowed = Object.keys(KEY_TO_ENV)
  const sys = `Eres un clasificador muy cauto. Devuelves SOLO JSON: {"service_key": "<KEY|empty>"}.
- KEY debe ser exactamente una de: ${allowed.join(', ')}.
- Si no estás 100% segura, responde {"service_key": ""}.`
  const out = await aiChat([
    { role: 'system', content: sys },
    { role: 'user', content: `Últimos mensajes (cliente):\n${last}` }
  ])
  try {
    const j = JSON.parse(out || '{}')
    return allowed.includes(j?.service_key) ? j.service_key : null
  } catch { return null }
}

// ------------------------------
// Square helpers
// ------------------------------
const SQ_HEADERS = {
  'Authorization': `Bearer ${SQUARE_ACCESS_TOKEN}`,
  'Content-Type': 'application/json',
  'Square-Version': SQUARE_API_VER
}

function getServiceVariationId (serviceKey, locationKey) {
  // si en .env usas prefijo _luz_, respétalo
  let envKey = KEY_TO_ENV[serviceKey]
  if (!envKey) return null
  if (locationKey === 'LUZ') {
    // intenta variante con _luz_ si existe
    const luzKey = envKey.replace('SQ_SVC_', 'SQ_SVC_luz_')
    if (SVC_IDS[luzKey]) envKey = luzKey
  }
  const entry = SVC_IDS[envKey]
  return entry?.id || null
}

async function squareCreateCustomer ({ givenName, familyName, phoneNumber, emailAddress }) {
  const body = {
    idempotency_key: crypto.randomUUID(),
    given_name: givenName || undefined,
    family_name: familyName || undefined,
    phone_number: phoneNumber || undefined,
    email_address: emailAddress || undefined
  }
  const r = await fetch('https://connect.squareup.com/v2/customers', {
    method: 'POST',
    headers: SQ_HEADERS,
    body: JSON.stringify(body)
  })
  if (!r.ok) throw new Error(`createCustomer: ${r.status} ${await r.text()}`)
  const j = await r.json()
  return j.customer?.id
}

async function squareSearchAvailability ({
  locationId,
  serviceVariationId,
  teamMemberId,
  startAt,
  endAt,
  max = 10
}) {
  // Intento 100% API; si devuelve 400, devolvemos [] y dejamos fallback
  const body = {
    query: {
      filter: {
        location_id: locationId,
        start_at_range: { start_at: startAt, end_at: endAt },
        segment_filters: [
          {
            service_variation_id: serviceVariationId,
            team_member_id_filter: teamMemberId ? { any: [teamMemberId] } : undefined
          }
        ]
      }
    },
    limit: max
  }
  const r = await fetch('https://connect.squareup.com/v2/bookings/availability/search', {
    method: 'POST',
    headers: SQ_HEADERS,
    body: JSON.stringify(body)
  })
  if (!r.ok) {
    const txt = await r.text()
    console.warn('searchAvailability:', r.status, txt)
    return []
  }
  const j = await r.json()
  const slots = j.availabilities || []
  return slots.map(a => a.start_at)
}

async function squareCreateBooking ({
  locationId, customerId, serviceVariationId, teamMemberId, startAt, durationMin
}) {
  const body = {
    idempotency_key: crypto.randomUUID(),
    booking: {
      location_id: locationId,
      start_at: startAt,
      customer_id: customerId,
      appointment_segments: [
        {
          duration_minutes: Number(durationMin) || 60,
          service_variation_id: serviceVariationId,
          team_member_id: teamMemberId || undefined
        }
      ]
    }
  }
  const r = await fetch('https://connect.squareup.com/v2/bookings', {
    method: 'POST',
    headers: SQ_HEADERS,
    body: JSON.stringify(body)
  })
  if (!r.ok) throw new Error(`createBooking: ${r.status} ${await r.text()}`)
  const j = await r.json()
  return j.booking
}

// ------------------------------
// Slots fallback (cuando Square 400)
// ------------------------------
function ceilToNextSlot (d) {
  const m = d.minute()
  const add = (m % 30 === 0) ? 0 : (30 - (m % 30))
  return d.add(add, 'minute').second(0).millisecond(0)
}
function generateOffers ({ days = 3, durationMin = 60 }) {
  const res = []
  let cur = ceilToNextSlot(dayjs().add(Number(BOT_NOW_OFFSET_MIN) || 0, 'minute'))
  const end = dayjs().add(days, 'day').endOf('day')

  while (cur.isBefore(end)) {
    const wd = cur.day()
    const hm = cur.format('HH:mm')
    const isOpen = (wd >= 1 && wd <= 5) &&
      ((hm >= '10:00' && hm < '14:00') || (hm >= '16:00' && hm < '20:00')) &&
      !isHoliday(cur)

    if (isOpen) res.push(cur.toISOString())
    cur = cur.add(30, 'minute')
  }
  return res.slice(0, 12)
}

// Emparejar “a las 10” con ofertas ISO
function parseTimeAgainstOffers (text, offersISO, preferDay = null) {
  const t = norm(text)
  const m = t.match(/\b(?:a\s+las?\s+)?(\d{1,2})(?:[:.\s](\d{2}))?\b/)
  if (!m) return null
  const hh = String(m[1]).padStart(2, '0')
  const mm = String(m[2] || '00').padStart(2, '0')
  const target = `${hh}:${mm}`
  // Buscar primero el mismo día si preferDay está
  let list = offersISO
  if (preferDay) {
    list = offersISO.filter(x => dayjs(x).format('YYYY-MM-DD') === preferDay)
      .concat(offersISO.filter(x => dayjs(x).format('YYYY-MM-DD') !== preferDay))
  }
  const hit = list.find(iso => dayjs(iso).format('HH:mm') === target)
  return hit || null
}

// ------------------------------
// Textos
// ------------------------------
const WELCOME =
  `Gracias por comunicarte con Gapink Nails. Por favor, haznos saber cómo podemos ayudarte.\n\n` +
  `${openingMsg()}\n\n` +
  `Si quieres reservar una cita puedes hacerlo a través de este link:\n\n` +
  `https://gapinknails.square.site/\n\n` +
  `Y si quieres modificarla puedes hacerlo a través del link del sms que llega con su cita.\n\n` +
  `Para cualquier otra consulta, déjenos saber y en el horario establecido le responderemos.\nGracias 😘`

const QUESTION_LOCATION = '¿En qué salón te viene mejor, **Málaga – La Luz** o **Torremolinos**?'
const QUESTION_LASH =
  `¿Qué servicio de **pestañas** necesitas?\n` +
  `* Lifting + tinte\n` +
  `* Extensiones nuevas: pelo a pelo (clásicas) / 2D / 3D\n` +
  `* Relleno: pelo a pelo / 2D / 3D\n` +
  `* Quitar extensiones\n\n` +
  `Escribe por ejemplo: "Extensiones 2D", "Relleno pelo a pelo" o "Lifting + tinte".`

function offersToText (offersISO, label, service) {
  if (!offersISO?.length) return 'No tengo huecos en ese rango ahora mismo.'
  const lines = offersISO.slice(0, 3).map(iso =>
    `* ${dayjs(iso).format('dddd DD/MM HH:mm')}`
  )
  return `Tengo estos huecos para **${service}** en **${label}**:\n${lines.join('\n')}\n\n` +
    `¿Te viene bien el primero? Si prefieres otro día/hora, dímelo.`
}

// ------------------------------
// WhatsApp – start
// ------------------------------
async function startBot () {
  const { state, saveCreds } = await useMultiFileAuthState('./auth')
  const { version } = await fetchLatestBaileysVersion()

  const sock = makeWASocket({
    version,
    browser: Browsers.macOS('Safari'),
    auth: state,
    printQRInTerminal: true,
    syncFullHistory: false
  })

  sock.ev.on('creds.update', saveCreds)

  sock.ev.on('connection.update', ({ connection, lastDisconnect }) => {
    if (connection === 'close') {
      const mustReconnect =
        (lastDisconnect?.error)?.output?.statusCode !== DisconnectReason.loggedOut
      console.log(mustReconnect ? '❌ Conexión cerrada. Reintentando…' : '🔴 Sesión cerrada.')
      if (mustReconnect) startBot()
    } else if (connection === 'open') {
      console.log('✅ WhatsApp listo')
    }
  })

  sock.ev.on('messages.upsert', async (m) => {
    const msg = m.messages?.[0]
    if (!msg || msg.key.fromMe) return

    const jid = msg.key.remoteJid
    const text = msg.message?.conversation ||
      msg.message?.extendedTextMessage?.text ||
      msg.message?.ephemeralMessage?.message?.conversation ||
      ''
    if (!text) return

    const s = getS(jid)
    s.history.push(text)
    if (s.history.length > 20) s.history = s.history.slice(-20)

    // 1) Mensaje inicial / fuera de horario
    if (s.step === 'INIT') {
      await sock.sendMessage(jid, { text: WELCOME })
      s.step = 'ASK_LOCATION_OR_SERVICE'
      // no paramos: seguimos a ver si ya dio pistas en el mismo mensaje
    }

    // Cancelar
    if (/cancel(ar|a|acion|ación)/i.test(text)) {
      await sock.sendMessage(jid, { text: `Para cancelar o mover tu cita, usa el enlace del SMS de confirmación. Si no lo tienes, dime **nombre completo** y **fecha** de la cita y lo gestionamos.` })
      return
    }

    // Detecta local si no lo tenemos
    if (!s.locationId) {
      const loc = detectLocation(text)
      if (loc) {
        s.locationKey = loc.key
        s.locationId = loc.id
        s.locationLabel = loc.label
      }
    }

    // Detecta servicio si no lo tenemos
    if (!s.serviceKey) {
      let svc = detectService(text)
      if (!svc) svc = detectServiceFromHistory(s.history)
      if (!svc) svc = await aiDetectService(s.history)
      if (svc) {
        s.serviceKey = svc
        s.durationMin = SVC_DUR[svc] || 60
      }
    }

    // Staff solicitado (ej. "con desi")
    const low = norm(text)
    const staffMatch = low.match(/\bcon\s+([a-z\u00f1]+(?:\s+[a-z\u00f1]+)?)\b/)
    if (staffMatch) {
      const name = staffMatch[1]
      s.staffName = name
      s.staffId = STAFF_NAME_TO_ID[name] || null
    }

    // Preguntas pendientes
    if (!s.locationId) {
      await sock.sendMessage(jid, { text: QUESTION_LOCATION })
      return
    }

    if (!s.serviceKey) {
      // Si habló de pestañas explícitamente, muestra menú de pestañas
      if (/\bpestan(?:as|as)\b/i.test(text)) {
        await sock.sendMessage(jid, { text: QUESTION_LASH })
      } else {
        await sock.sendMessage(jid, {
          text: `¿Qué servicio necesitas? (ej.: “manicura semipermanente”, “Extensiones 2D”, “Lifting + tinte”).`
        })
      }
      return
    }

    // 2) Ya tengo local + servicio -> ofrece huecos
    if (!s.offersISO?.length) {
      const serviceVariationId = getServiceVariationId(s.serviceKey, s.locationKey)
      let offersISO = []
      if (serviceVariationId) {
        const start = dayjs().toISOString()
        const end = dayjs().add(Number(BOT_SEARCH_WINDOW_DAYS) || 14, 'day').toISOString()
        offersISO = await squareSearchAvailability({
          locationId: s.locationId,
          serviceVariationId,
          teamMemberId: s.staffId || undefined,
          startAt: start,
          endAt: end,
          max: 10
        })
      }
      if (!offersISO.length) {
        // fallback local
        offersISO = generateOffers({ days: 5, durationMin: s.durationMin })
      }
      s.offersISO = offersISO
      if (!offersISO.length) {
        await sock.sendMessage(jid, { text: `Ahora mismo no tengo huecos visibles. ¿Te viene bien decirme **un día y hora** aproximados y lo miro?` })
        return
      }
      await sock.sendMessage(jid, { text: offersToText(offersISO, s.locationLabel, prettyService(s.serviceKey)) })
      s.step = 'ASK_TIME_CONFIRM'
      return
    }

    // 3) Esperamos confirmación de hora
    if (s.step === 'ASK_TIME_CONFIRM') {
      // “el primero”
      if (/\b(primer[oa]?|el primero|vale|ok|me vale|perfecto)\b/i.test(text)) {
        s.selectedISO = s.offersISO[0]
      } else {
        // “a las 10”
        const preferDay = dayjs(s.offersISO[0]).format('YYYY-MM-DD')
        s.selectedISO = parseTimeAgainstOffers(text, s.offersISO, preferDay)
      }

      if (!s.selectedISO) {
        await sock.sendMessage(jid, { text: `Dime la hora exacta de las opciones (por ejemplo: “a las ${dayjs(s.offersISO[0]).format('HH:mm')}”).` })
        return
      }

      s.step = 'ASK_NAME'
      await sock.sendMessage(jid, { text: `Para cerrar, dime tu **nombre y apellidos**.` })
      return
    }

    if (s.step === 'ASK_NAME') {
      const name = text.trim()
      if (name.split(' ').length < 2) {
        await sock.sendMessage(jid, { text: `Por favor, dime **nombre y apellidos**.` })
        return
      }
      s.name = name
      s.step = 'BOOKING'
    }

    if (s.step === 'BOOKING') {
      // Crear cliente y reserva en Square
      try {
        const [given, ...rest] = s.name.split(' ')
        const family = rest.join(' ')
        const serviceVariationId = getServiceVariationId(s.serviceKey, s.locationKey)
        const customerId = await squareCreateCustomer({
          givenName: given,
          familyName: family,
          phoneNumber: jid.split('@')[0]
        })

        const booking = await squareCreateBooking({
          locationId: s.locationId,
          customerId,
          serviceVariationId,
          teamMemberId: s.staffId || undefined,
          startAt: s.selectedISO,
          durationMin: s.durationMin
        })

        await sock.sendMessage(jid, {
          text:
`Reserva confirmada 🎉
Salón: ${s.locationLabel}
Servicio: ${prettyService(s.serviceKey)}
Profesional: ${s.staffName ? `${s.staffName} (si está disponible)` : 'cualquiera'}
Fecha: ${dayjs(s.selectedISO).format('dddd DD/MM HH:mm')}
Duración: ${s.durationMin} min
Pago en persona.

Si quieres modificarla, usa el link del SMS que te llegará.`
        })

        // reset básico
        SESSIONS.delete(jid)
      } catch (e) {
        console.error(e)
        await sock.sendMessage(jid, { text: `No he podido confirmar la reserva ahora mismo. ¿Te viene bien que te pase **otros huecos** o prefieres reservar desde:\nhttps://gapinknails.square.site/` })
      }
      return
    }

    // Si no coincidió nada, mensaje de ayuda
    if (!s.selectedISO) {
      await sock.sendMessage(jid, { text: `¿Necesitas **pestañas** (Lifting, extensiones nuevas, relleno o quitar), **uñas** o **pedicura**? Dime además si prefieres **Málaga – La Luz** o **Torremolinos**.` })
    }
  })
}

function prettyService (key) {
  const map = {
    LIFITNG_DE_PESTANAS_Y_TINTE: 'Lifting + tinte',
    EXTENSIONES_DE_PESTANAS_NUEVAS_PELO_A_PELO: 'Extensiones nuevas pelo a pelo',
    EXTENSIONES_PESTANAS_NUEVAS_2D: 'Extensiones nuevas 2D',
    EXTENSIONES_PESTANAS_NUEVAS_3D: 'Extensiones nuevas 3D',
    RELLENO_EXTENSIONES_PESTANAS_PELO_A_PELO: 'Relleno pelo a pelo',
    RELLENO_PESTANAS_2D: 'Relleno 2D',
    RELLENO_PESTANAS_3D: 'Relleno 3D',
    QUITAR_EXTENSIONES_PESTANAS: 'Quitar extensiones de pestañas',
    MANICURA_SEMIPERMANENTE: 'Manicura semipermanente',
    MANICURA_SEMIPERMANENTE_QUITAR: 'Quitar semipermanente',
    MANICURA_CON_ESMALTE_NORMAL: 'Manicura con esmalte normal',
    PEDICURA_SPA_CON_ESMALTE_SEMIPERMANENTE: 'Pedicura SPA semipermanente'
  }
  return map[key] || key.replace(/_/g, ' ').toLowerCase()
}

// ------------------------------
// http tiny server (salud)
// ------------------------------
import http from 'http'
http.createServer((_, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' })
  res.end('Gapink WhatsApp bot OK\n')
}).listen(8080, () => console.log('🌐 Web 8080'))

// Arrancar
startBot().catch(err => {
  console.error(err)
  process.exit(1)
})
