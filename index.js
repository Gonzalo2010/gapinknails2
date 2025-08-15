// index.js — Gapink Nails (WhatsApp + IA + Square) — ESM
import 'dotenv/config'
import http from 'http'
import crypto from 'crypto'

// ===== Baileys (compat con varias versiones) =====
import * as Baileys from '@whiskeysockets/baileys'
const useMultiFileAuthState =
  Baileys.useMultiFileAuthState || Baileys.default?.useMultiFileAuthState
const DisconnectReason =
  Baileys.DisconnectReason || Baileys.default?.DisconnectReason
const Browsers = Baileys.Browsers || Baileys.default?.Browsers
const fetchLatestBaileysVersion =
  Baileys.fetchLatestBaileysVersion ||
  Baileys.default?.fetchLatestBaileysVersion ||
  (async () => ({ version: [2, 3000, 0] }))

const makeWASocket =
  (typeof Baileys.makeWASocket === 'function' && Baileys.makeWASocket) ||
  (typeof Baileys.default === 'function' && Baileys.default) ||
  (Baileys.default && typeof Baileys.default.makeWASocket === 'function' && Baileys.default.makeWASocket)

if (typeof makeWASocket !== 'function' || typeof useMultiFileAuthState !== 'function') {
  throw new Error('No se pudo resolver makeWASocket / useMultiFileAuthState desde @whiskeysockets/baileys.')
}

// ===== Dayjs =====
import dayjsBase from 'dayjs'
import tz from 'dayjs/plugin/timezone.js'
import utc from 'dayjs/plugin/utc.js'
import customParse from 'dayjs/plugin/customParseFormat.js'
dayjsBase.extend(utc); dayjsBase.extend(tz); dayjsBase.extend(customParse)
const dayjs = (d) => dayjsBase.tz(d, 'Europe/Madrid')

// ===== ENV =====
const {
  OPENAI_API_KEY,
  SQUARE_ACCESS_TOKEN,
  SQUARE_API_VER = '2023-12-13',

  // Locations
  SQUARE_LOCATION_ID_TORREMOLINOS = 'LSMNAJFSY1EGS',
  SQUARE_LOCATION_ID_LA_LUZ = 'LF5NK1R8RDMRV',

  // Texto direcciones
  ADDRESS_TORREMOLINOS = 'Av. de Benyamina 18, Torremolinos',
  ADDRESS_LA_LUZ = 'Málaga – Barrio de La Luz',

  // Festivos (DD/MM)
  HOLIDAYS_EXTRA = '06/01,28/02,15/08,12/10,01/11,06/12,08/12,25/12',

  // Bot
  BOT_SEARCH_WINDOW_DAYS = 14,
  BOT_NOW_OFFSET_MIN = 30
} = process.env

// ===== Mapa servicios desde .env (SQ_SVC_* = ID|VERSION) =====
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

// ===== Claves canónicas -> variable .env =====
const KEY_TO_ENV = {
  LIFITNG_DE_PESTANAS_Y_TINTE: 'SQ_SVC_LIFITNG_DE_PESTANAS_Y_TINTE',
  EXTENSIONES_DE_PESTANAS_NUEVAS_PELO_A_PELO: 'SQ_SVC_EXTENSIONES_DE_PESTANAS_NUEVAS_PELO_A_PELO',
  EXTENSIONES_PESTANAS_NUEVAS_2D: 'SQ_SVC_EXTENSIONES_PESTANAS_NUEVAS_2D',
  EXTENSIONES_PESTANAS_NUEVAS_3D: 'SQ_SVC_EXTENSIONES_PESTANAS_NUEVAS_3D',
  RELLENO_EXTENSIONES_PESTANAS_PELO_A_PELO: 'SQ_SVC_RELLENO_EXTENSIONES_PESTANAS_PELO_A_PELO',
  RELLENO_PESTANAS_2D: 'SQ_SVC_RELLENO_PESTANAS_2D',
  RELLENO_PESTANAS_3D: 'SQ_SVC_RELLENO_PESTANAS_3D',
  QUITAR_EXTENSIONES_PESTANAS: 'SQ_SVC_QUITAR_EXTENSIONES_PESTANAS',
  MANICURA_SEMIPERMANENTE: 'SQ_SVC_MANICURA_SEMIPERMANENTE',
  MANICURA_SEMIPERMANENTE_QUITAR: 'SQ_SVC_MANICURA_SEMIPERMANENTE_QUITAR',
  MANICURA_CON_ESMALTE_NORMAL: 'SQ_SVC_MANICURA_CON_ESMALTE_NORMAL',
  PEDICURA_SPA_CON_ESMALTE_SEMIPERMANENTE: 'SQ_SVC_PEDICURA_SPA_CON_ESMALTE_SEMIPERMANENTE'
}

// ===== Duración aprox por servicio =====
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

// ===== Staff (alias -> ID) =====
const STAFF_NAME_TO_ID = {
  'carmen belen': 'TM4mRkFCodl_mDWf',
  'ganna': 'TMW_ctTgpIyIOeLa',
  'rocio': 'TMdIMb5NTeGacSTN',
  'rocio chica': 'TMhJsD5RnV9hmEcG',
  'maria': 'TMhpeTVah4qtuUtP',
  'desi': 'TMXSDoPYYoscyMVH',        // puede no ser bookable
  'daniela': 'TMTzygZYYb2JNC3z',
  'anaira': 'TMtlbg7yOf7o0sf0'
}

// ===== Helpers =====
const norm = (s) => String(s || '').toLowerCase()
  .normalize('NFD').replace(/\p{Diacritic}/gu, '').replace(/\s+/g, ' ').trim()

// Sesiones
const SESSIONS = new Map()
function getS (jid) {
  if (!SESSIONS.has(jid)) {
    SESSIONS.set(jid, {
      step: 'INIT',
      history: [],
      locationKey: null,
      locationId: null,
      locationLabel: null,
      serviceKey: null,
      durationMin: 60,
      staffName: null,
      staffId: null,
      offersISO: [],
      selectedISO: null,
      name: null
    })
  }
  return SESSIONS.get(jid)
}

// Festivos / horario
const EXTRA_HOLIDAYS = (HOLIDAYS_EXTRA || '').split(',').map(s => s.trim()).filter(Boolean)
const isHoliday = (d = dayjs()) => EXTRA_HOLIDAYS.includes(d.format('DD/MM'))

// ===== IA =====
async function aiChat (messages) {
  if (!OPENAI_API_KEY) return ''
  const r = await fetch('https://api.openai.com/v1/chat/completions', {
    method: 'POST',
    headers: { 'Authorization': `Bearer ${OPENAI_API_KEY}`, 'Content-Type': 'application/json' },
    body: JSON.stringify({ model: 'gpt-4o-mini', temperature: 0.1, messages })
  })
  if (!r.ok) return ''
  const j = await r.json()
  return j.choices?.[0]?.message?.content || ''
}

// Detección de local y servicio
function detectLocation (text) {
  const t = norm(text)
  if (/\b(torre|torremolinos|beni?amina|benyamina)\b/.test(t))
    return { key: 'TORRE', id: SQUARE_LOCATION_ID_TORREMOLINOS, label: `Torremolinos (${ADDRESS_TORREMOLINOS})` }
  if (/\b(luz|malaga|málaga|barrio de la luz|centro|la luz)\b/.test(t))
    return { key: 'LUZ', id: SQUARE_LOCATION_ID_LA_LUZ, label: `Málaga – La Luz (${ADDRESS_LA_LUZ})` }
  return null
}
function detectService (text) {
  const t = norm(text)
  if (/\b(quitar|retirar|sacar)\b.*\b(extensi(?:on|ones)|pestan|pestañ)/.test(t)) return 'QUITAR_EXTENSIONES_PESTANAS'
  if ((/\blifting\b/.test(t) || /\blift\b/.test(t)) && (/\btinte\b/.test(t) || /\btint[ae]\b/.test(t))) return 'LIFITNG_DE_PESTANAS_Y_TINTE'
  if (/\b(extensi(?:on|ones)|pestan)\b.*\b2d\b/.test(t)) return 'EXTENSIONES_PESTANAS_NUEVAS_2D'
  if (/\b(extensi(?:on|ones)|pestan)\b.*\b3d\b/.test(t)) return 'EXTENSIONES_PESTANAS_NUEVAS_3D'
  if (/\b(extensi(?:on|ones)|pestan)\b.*\b(pelo a pelo|clasicas|clásicas|classic)\b/.test(t)) return 'EXTENSIONES_DE_PESTANAS_NUEVAS_PELO_A_PELO'
  if (/\brellen[oa]s?\b.*\b2d\b/.test(t)) return 'RELLENO_PESTANAS_2D'
  if (/\brellen[oa]s?\b.*\b3d\b/.test(t)) return 'RELLENO_PESTANAS_3D'
  if (/\brellen[oa]s?\b.*\b(pelo a pelo|clasicas|clásicas)\b/.test(t)) return 'RELLENO_EXTENSIONES_PESTANAS_PELO_A_PELO'
  if (/\bmanicur[ae]\b.*\bsemi\b/.test(t)) return 'MANICURA_SEMIPERMANENTE'
  if (/\bmanicur[ae]\b.*\bquitar\b/.test(t)) return 'MANICURA_SEMIPERMANENTE_QUITAR'
  if (/\bmanicur[ae]\b.*\bnormal\b/.test(t)) return 'MANICURA_CON_ESMALTE_NORMAL'
  if (/\bpedicur[ae]\b.*\bsemi\b/.test(t)) return 'PEDICURA_SPA_CON_ESMALTE_SEMIPERMANENTE'
  return null
}
async function aiDetectService (historyArr = []) {
  if (!OPENAI_API_KEY) return null
  const allowed = Object.keys(KEY_TO_ENV)
  const sys = `Solo responde JSON {"service_key": "<KEY|empty>"}. KEY ∈ {${allowed.join(', ')}}. Si dudas, vacío.`
  const out = await aiChat([
    { role: 'system', content: sys },
    { role: 'user', content: (historyArr || []).slice(-8).join('\n') }
  ])
  try { const j = JSON.parse(out || '{}'); return allowed.includes(j.service_key) ? j.service_key : null } catch { return null }
}

// ===== Square =====
const SQ_HEADERS = {
  'Authorization': `Bearer ${SQUARE_ACCESS_TOKEN}`,
  'Content-Type': 'application/json',
  'Square-Version': SQUARE_API_VER
}
function getServiceVariationId (serviceKey, locationKey) {
  let envKey = KEY_TO_ENV[serviceKey]
  if (!envKey) return null
  if (locationKey === 'LUZ') {
    const luzKey = envKey.replace('SQ_SVC_', 'SQ_SVC_luz_')
    if (SVC_IDS[luzKey]) envKey = luzKey
  }
  return SVC_IDS[envKey]?.id || null
}
async function squareCreateCustomer ({ givenName, familyName, phoneNumber }) {
  const r = await fetch('https://connect.squareup.com/v2/customers', {
    method: 'POST', headers: SQ_HEADERS,
    body: JSON.stringify({ idempotency_key: crypto.randomUUID(), given_name: givenName, family_name: familyName, phone_number: phoneNumber })
  })
  if (!r.ok) throw new Error(`createCustomer: ${r.status} ${await r.text()}`)
  return (await r.json()).customer?.id
}
async function squareSearchAvailability ({ locationId, serviceVariationId, teamMemberId, startAt, endAt, max = 10 }) {
  const body = {
    query: {
      filter: {
        location_id: locationId,
        start_at_range: { start_at: startAt, end_at: endAt },
        segment_filters: [
          { service_variation_id: serviceVariationId, team_member_id_filter: teamMemberId ? { any: [teamMemberId] } : undefined }
        ]
      }
    },
    limit: max
  }
  const r = await fetch('https://connect.squareup.com/v2/bookings/availability/search', {
    method: 'POST', headers: SQ_HEADERS, body: JSON.stringify(body)
  })
  if (!r.ok) { console.warn('searchAvailability:', r.status, await r.text()); return [] }
  const j = await r.json()
  return (j.availabilities || []).map(a => a.start_at)
}
async function squareCreateBooking ({ locationId, customerId, serviceVariationId, teamMemberId, startAt, durationMin }) {
  const body = {
    idempotency_key: crypto.randomUUID(),
    booking: {
      location_id: locationId,
      start_at: startAt,
      customer_id: customerId,
      appointment_segments: [{
        duration_minutes: Number(durationMin) || 60,
        service_variation_id: serviceVariationId,
        team_member_id: teamMemberId || undefined
      }]
    }
  }
  const r = await fetch('https://connect.squareup.com/v2/bookings', {
    method: 'POST', headers: SQ_HEADERS, body: JSON.stringify(body)
  })
  if (!r.ok) throw new Error(`createBooking: ${r.status} ${await r.text()}`)
  return (await r.json()).booking
}

// ===== Ofertas (fallback si Square 400) =====
const ceilToNextSlot = (d) => {
  const m = d.minute()
  const add = (m % 30 === 0) ? 0 : (30 - (m % 30))
  return d.add(add, 'minute').second(0).millisecond(0)
}
function generateOffers ({ days = 5 }) {
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
function parseTimeAgainstOffers (text, offersISO, preferDay = null) {
  const t = norm(text)
  const m = t.match(/\b(?:a\s+las?\s+)?(\d{1,2})(?:[:.\s](\d{2}))?\b/)
  if (!m) return null
  const hh = String(m[1]).padStart(2, '0')
  const mm = String(m[2] || '00').padStart(2, '0')
  const target = `${hh}:${mm}`
  let list = offersISO
  if (preferDay) {
    list = offersISO.filter(x => dayjs(x).format('YYYY-MM-DD') === preferDay)
      .concat(offersISO.filter(x => dayjs(x).format('YYYY-MM-DD') !== preferDay))
  }
  return list.find(iso => dayjs(iso).format('HH:mm') === target) || null
}

// ===== Textos =====
const openingMsg = () => `Solo atenderemos por WhatsApp y llamadas en horario de lunes a viernes de 10 a 14:00 y de 16:00 a 20:00`
const WELCOME =
  `Gracias por comunicarte con Gapink Nails. Por favor, haznos saber cómo podemos ayudarte.\n\n` +
  `${openingMsg()}\n\n` +
  `Si quieres reservar una cita puedes hacerlo a través de este link:\n\nhttps://gapinknails.square.site/\n\n` +
  `Y si quieres modificarla puedes hacerlo a través del link del sms que llega con su cita.\n\n` +
  `Para cualquier otra consulta, déjenos saber y en el horario establecido le responderemos.\nGracias 😘`

const QUESTION_LOCATION = '¿En qué salón te viene mejor, **Málaga – La Luz** o **Torremolinos**?'
const QUESTION_LASH =
  `¿Qué servicio de **pestañas** necesitas?\n` +
  `* Lifting + tinte\n* Extensiones nuevas: pelo a pelo (clásicas) / 2D / 3D\n` +
  `* Relleno: pelo a pelo / 2D / 3D\n* Quitar extensiones\n\n` +
  `Escribe por ejemplo: "Extensiones 2D", "Relleno pelo a pelo" o "Lifting + tinte".`
const prettyService = (k) => ({
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
}[k] || k.replace(/_/g, ' ').toLowerCase())

function offersToText (offersISO, label, service) {
  if (!offersISO?.length) return 'No tengo huecos en ese rango ahora mismo.'
  const lines = offersISO.slice(0, 3).map(iso => `* ${dayjs(iso).format('dddd DD/MM HH:mm')}`)
  return `Tengo estos huecos para **${service}** en **${label}**:\n${lines.join('\n')}\n\n¿Te viene bien el primero? Si prefieres otro día/hora, dímelo.`
}

// ===== BOT =====
async function startBot () {
  const { state, saveCreds } = await useMultiFileAuthState('./auth')
  const { version } = await fetchLatestBaileysVersion()
  const sock = makeWASocket({ version, browser: Browsers.macOS('Safari'), auth: state, printQRInTerminal: true })
  sock.ev.on('creds.update', saveCreds)
  sock.ev.on('connection.update', ({ connection, lastDisconnect }) => {
    if (connection === 'close') {
      const should = (lastDisconnect?.error)?.output?.statusCode !== DisconnectReason.loggedOut
      console.log(should ? '❌ Conexión cerrada. Reintentando…' : '🔴 Sesión cerrada.')
      if (should) startBot()
    }
    if (connection === 'open') console.log('✅ WhatsApp listo')
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
    s.history.push(text); if (s.history.length > 20) s.history = s.history.slice(-20)

    if (s.step === 'INIT') { await sock.sendMessage(jid, { text: WELCOME }); s.step = 'ASK_LOCATION_OR_SERVICE' }

    // Cancelaciones
    if (/cancel(ar|a|acion|ación)/i.test(text)) {
      await sock.sendMessage(jid, { text: `Para cancelar o mover tu cita, usa el enlace del SMS de confirmación. Si no lo tienes, dime **nombre completo** y **fecha** de la cita y lo gestionamos.` })
      return
    }

    // Local
    if (!s.locationId) { const loc = detectLocation(text); if (loc) { s.locationKey = loc.key; s.locationId = loc.id; s.locationLabel = loc.label } }
    // Servicio
    if (!s.serviceKey) {
      let svc = detectService(text)
      if (!svc) svc = await aiDetectService(s.history)
      if (svc) { s.serviceKey = svc; s.durationMin = SVC_DUR[svc] || 60 }
    }
    // Staff (ej. "con desi")
    const staffM = norm(text).match(/\bcon\s+([a-z\u00f1]+(?:\s+[a-z\u00f1]+)?)\b/)
    if (staffM) { s.staffName = staffM[1]; s.staffId = STAFF_NAME_TO_ID[s.staffName] || null }

    if (!s.locationId) { await sock.sendMessage(jid, { text: QUESTION_LOCATION }); return }
    if (!s.serviceKey) {
      if (/\bpestan(?:as|as)\b/i.test(text)) await sock.sendMessage(jid, { text: QUESTION_LASH })
      else await sock.sendMessage(jid, { text: `¿Qué servicio necesitas? (ej.: “manicura semipermanente”, “Extensiones 2D”, “Lifting + tinte”).` })
      return
    }

    // Ofertas
    if (!s.offersISO.length) {
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
      if (!offersISO.length) offersISO = generateOffers({ days: 5 })
      s.offersISO = offersISO
      await sock.sendMessage(jid, { text: offersToText(offersISO, s.locationLabel, prettyService(s.serviceKey)) })
      s.step = 'ASK_TIME_CONFIRM'
      return
    }

    // Confirmación de hora
    if (s.step === 'ASK_TIME_CONFIRM') {
      if (/\b(primer[oa]?|el primero|vale|ok|me vale|perfecto)\b/i.test(text)) s.selectedISO = s.offersISO[0]
      else s.selectedISO = parseTimeAgainstOffers(text, s.offersISO, dayjs(s.offersISO[0]).format('YYYY-MM-DD'))

      if (!s.selectedISO) { await sock.sendMessage(jid, { text: `Dime la hora exacta de las opciones (por ejemplo: “a las ${dayjs(s.offersISO[0]).format('HH:mm')}”).` }); return }
      s.step = 'ASK_NAME'
      await sock.sendMessage(jid, { text: `Para cerrar, dime tu **nombre y apellidos**.` })
      return
    }

    if (s.step === 'ASK_NAME') {
      const name = text.trim()
      if (name.split(' ').length < 2) { await sock.sendMessage(jid, { text: `Por favor, dime **nombre y apellidos**.` }); return }
      s.name = name; s.step = 'BOOKING'
    }

    if (s.step === 'BOOKING') {
      try {
        const [given, ...rest] = s.name.split(' ')
        const customerId = await squareCreateCustomer({ givenName: given, familyName: rest.join(' '), phoneNumber: jid.split('@')[0] })
        const serviceVariationId = getServiceVariationId(s.serviceKey, s.locationKey)
        await squareCreateBooking({
          locationId: s.locationId,
          customerId,
          serviceVariationId,
          teamMemberId: s.staffId || undefined,
          startAt: s.selectedISO,
          durationMin: s.durationMin
        })
        await sock.sendMessage(jid, { text:
`Reserva confirmada 🎉
Salón: ${s.locationLabel}
Servicio: ${prettyService(s.serviceKey)}
Profesional: ${s.staffName ? `${s.staffName} (si está disponible)` : 'cualquiera'}
Fecha: ${dayjs(s.selectedISO).format('dddd DD/MM HH:mm')}
Duración: ${s.durationMin} min
Pago en persona.

Si quieres modificarla, usa el link del SMS que te llegará.` })
        SESSIONS.delete(jid)
      } catch (e) {
        console.error(e)
        await sock.sendMessage(jid, { text: `No he podido confirmar ahora mismo. ¿Prefieres otros huecos o reservar desde https://gapinknails.square.site/ ?` })
      }
    }
  })
}

// ===== HTTP de salud =====
http.createServer((_, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' })
  res.end('Gapink WhatsApp bot OK\n')
}).listen(8080, () => console.log('🌐 Web 8080'))

// ===== Arrancar =====
startBot().catch(err => { console.error(err); process.exit(1) })
