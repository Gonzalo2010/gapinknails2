// index.js — Gapink Nails bot (WhatsApp + IA + Square)  ✅ robusto con Baileys
import 'dotenv/config'
import fs from 'fs'
import http from 'http'
import crypto from 'crypto'

// ---- Baileys: import resistente a versiones (ESM/CJS) ----
import * as BA from '@whiskeysockets/baileys'
const makeWASocket =
  BA.makeWASocket ||
  BA.default?.makeWASocket ||
  BA.default?.default ||
  BA.default
const useMultiFileAuthState =
  BA.useMultiFileAuthState || BA.default?.useMultiFileAuthState
const DisconnectReason =
  BA.DisconnectReason || BA.default?.DisconnectReason
const fetchLatestBaileysVersion =
  BA.fetchLatestBaileysVersion || BA.default?.fetchLatestBaileysVersion
const Browsers =
  BA.Browsers || BA.default?.Browsers

if (typeof makeWASocket !== 'function') {
  console.error('❌ Baileys no expone makeWASocket como función. Revisa @whiskeysockets/baileys.')
  process.exit(1)
}

// ---- Dayjs ----
import dayjsBase from 'dayjs'
import tz from 'dayjs/plugin/timezone.js'
import utc from 'dayjs/plugin/utc.js'
import customParse from 'dayjs/plugin/customParseFormat.js'
dayjsBase.extend(utc)
dayjsBase.extend(tz)
dayjsBase.extend(customParse)
const dayjs = (d) => dayjsBase.tz(d, 'Europe/Madrid')

// ==== ENV ====
const {
  OPENAI_API_KEY,
  SQUARE_ACCESS_TOKEN,
  SQUARE_API_VER = '2023-12-13',
  SQUARE_LOCATION_ID_TORREMOLINOS = 'LSMNAJFSY1EGS',
  SQUARE_LOCATION_ID_LA_LUZ = 'LF5NK1R8RDMRV',
  ADDRESS_TORREMOLINOS = 'Av. de Benyamina 18, Torremolinos',
  ADDRESS_LA_LUZ = 'Málaga – Barrio de La Luz',
  HOLIDAYS_EXTRA = '06/01,28/02,15/08,12/10,01/11,06/12,08/12,25/12',
  BOT_SEARCH_WINDOW_DAYS = 14,
  BOT_NOW_OFFSET_MIN = 30
} = process.env

// ==== Servicios desde .env (normal y *_luz_*) ====
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

// Duraciones típicas
const SVC_DUR = {
  LIFITNG_DE_PESTANAS_Y_TINTE: 75,
  EXTENSIONES_DE_PESTANAS_NUEVAS_PELO_A_PELO: 120,
  EXTENSIONES_PESTANAS_NUEVAS_2D: 120,
  EXTENSIONES_PESTANAS_NUEVAS_3D: 135,
  RELLENO_EXTENSIONES_PESTANAS_PELO_A_PELO: 90,
  RELLENO_PESTANAS_2D: 95,
  RELLENO_PESTANAS_3D: 100,
  QUITAR_EXTENSIONES_PESTANAS: 30
}
// Mapa KEY→env
const KEY_TO_ENV = {
  LIFITNG_DE_PESTANAS_Y_TINTE: 'SQ_SVC_LIFITNG_DE_PESTANAS_Y_TINTE',
  EXTENSIONES_DE_PESTANAS_NUEVAS_PELO_A_PELO: 'SQ_SVC_EXTENSIONES_DE_PESTANAS_NUEVAS_PELO_A_PELO',
  EXTENSIONES_PESTANAS_NUEVAS_2D: 'SQ_SVC_EXTENSIONES_PESTANAS_NUEVAS_2D',
  EXTENSIONES_PESTANAS_NUEVAS_3D: 'SQ_SVC_EXTENSIONES_PESTANAS_NUEVAS_3D',
  RELLENO_EXTENSIONES_PESTANAS_PELO_A_PELO: 'SQ_SVC_RELLENO_EXTENSIONES_PESTANAS_PELO_A_PELO',
  RELLENO_PESTANAS_2D: 'SQ_SVC_RELLENO_PESTANAS_2D',
  RELLENO_PESTANAS_3D: 'SQ_SVC_RELLENO_PESTANAS_3D',
  QUITAR_EXTENSIONES_PESTANAS: 'SQ_SVC_QUITAR_EXTENSIONES_PESTANAS'
}

// Staff (IDs que me pasaste)
const STAFF_NAME_TO_ID = {
  'carmen belen': 'TM4mRkFCodl_mDWf',
  'ganna': 'TMW_ctTgpIyIOeLa',
  'rocio': 'TMdIMb5NTeGacSTN',
  'rocio chica': 'TMhJsD5RnV9hmEcG',
  'maria': 'TMhpeTVah4qtuUtP',
  'anaira': 'TMtlbg7yOf7o0sf0',
  'desi': 'TMXSDoPYYoscyMVH' // si Square no permite asignarla, se creará sin staff
}

// ==== Helpers ====
const SESSIONS = new Map()
const norm = (s) => String(s || '')
  .toLowerCase()
  .normalize('NFD')
  .replace(/\p{Diacritic}/gu, '')
  .replace(/\s+/g, ' ')
  .trim()

const EXTRA_HOLIDAYS = (HOLIDAYS_EXTRA || '').split(',').map(s => s.trim()).filter(Boolean)
const isHoliday = (d = dayjs()) => EXTRA_HOLIDAYS.includes(d.format('DD/MM'))
const isOpenNow = () => {
  const n = dayjs()
  if (isHoliday(n)) return false
  const wd = n.day()
  if (wd === 0 || wd === 6) return false
  const hm = n.format('HH:mm')
  return (hm >= '10:00' && hm < '14:00') || (hm >= '16:00' && hm < '20:00')
}
const openingMsg = () =>
  `Solo atenderemos por WhatsApp y llamadas en horario de lunes a viernes de 10 a 14:00 y de 16:00 a 20:00`

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

// ==== IA (clasificación súper prudente) ====
async function aiChat (messages) {
  if (!OPENAI_API_KEY) return ''
  const r = await fetch('https://api.openai.com/v1/chat/completions', {
    method: 'POST',
    headers: { Authorization: `Bearer ${OPENAI_API_KEY}`, 'Content-Type': 'application/json' },
    body: JSON.stringify({ model: 'gpt-4o-mini', temperature: 0.1, messages })
  })
  if (!r.ok) return ''
  const j = await r.json()
  return j.choices?.[0]?.message?.content || ''
}

function detectLocation (text) {
  const t = norm(text)
  if (/\b(torre|torremolinos|benyamina)\b/.test(t)) {
    return { key: 'TORRE', id: SQUARE_LOCATION_ID_TORREMOLINOS, label: `Torremolinos (${ADDRESS_TORREMOLINOS})` }
  }
  if (/\b(luz|malaga|málaga|barrio de la luz|centro)\b/.test(t)) {
    return { key: 'LUZ', id: SQUARE_LOCATION_ID_LA_LUZ, label: `Málaga – La Luz (${ADDRESS_LA_LUZ})` }
  }
  return null
}

function detectServiceRules (text) {
  const t = norm(text)
  if (/\b(quitar|retirar|sacar)\b.*\b(pestan|extensi)/.test(t)) return 'QUITAR_EXTENSIONES_PESTANAS'
  if ((/\blifting\b/.test(t) || /\blift\b/.test(t)) && (/\btinte\b/.test(t))) return 'LIFITNG_DE_PESTANAS_Y_TINTE'
  if (/\b(extensi|pestan)\b.*\b2d\b/.test(t)) return 'EXTENSIONES_PESTANAS_NUEVAS_2D'
  if (/\b(extensi|pestan)\b.*\b3d\b/.test(t)) return 'EXTENSIONES_PESTANAS_NUEVAS_3D'
  if (/\b(extensi|pestan)\b.*\b(pelo a pelo|clasicas|clásicas|classic)\b/.test(t)) {
    return 'EXTENSIONES_DE_PESTANAS_NUEVAS_PELO_A_PELO'
  }
  if (/\brellen/.test(t) && /\b2d\b/.test(t)) return 'RELLENO_PESTANAS_2D'
  if (/\brellen/.test(t) && /\b3d\b/.test(t)) return 'RELLENO_PESTANAS_3D'
  if (/\brellen/.test(t) && /\b(pelo a pelo|clasicas|clásicas)\b/.test(t)) return 'RELLENO_EXTENSIONES_PESTANAS_PELO_A_PELO'
  return null
}

async function detectServiceIA (history) {
  if (!OPENAI_API_KEY) return null
  const allowed = Object.keys(KEY_TO_ENV)
  const sys = `Devuelve solo JSON {"service_key": "<KEY|empty>"}.
KEY ∈ {${allowed.join(', ')}}
Si no estás 100% segura: {"service_key": ""}.`
  const out = await aiChat([
    { role: 'system', content: sys },
    { role: 'user', content: history.slice(-8).join('\n') }
  ])
  try {
    const j = JSON.parse(out || '{}')
    return allowed.includes(j?.service_key) ? j.service_key : null
  } catch { return null }
}

// ==== Square ====
const SQ_HEADERS = {
  Authorization: `Bearer ${SQUARE_ACCESS_TOKEN}`,
  'Content-Type': 'application/json',
  'Square-Version': SQUARE_API_VER
}

function svcVariationId (serviceKey, locationKey) {
  let envKey = KEY_TO_ENV[serviceKey]
  if (!envKey) return null
  if (locationKey === 'LUZ') {
    const alt = envKey.replace('SQ_SVC_', 'SQ_SVC_luz_')
    if (SVC_IDS[alt]) envKey = alt
  }
  return SVC_IDS[envKey]?.id || null
}

async function sqCreateCustomer ({ given, family, phone }) {
  const r = await fetch('https://connect.squareup.com/v2/customers', {
    method: 'POST',
    headers: SQ_HEADERS,
    body: JSON.stringify({
      idempotency_key: crypto.randomUUID(),
      given_name: given, family_name: family, phone_number: phone
    })
  })
  if (!r.ok) throw new Error(`createCustomer ${r.status}: ${await r.text()}`)
  const j = await r.json()
  return j.customer?.id
}

async function sqSearchAvailability ({ locationId, serviceVariationId, teamMemberId, startAt, endAt, max = 10 }) {
  const body = {
    query: {
      filter: {
        location_id: locationId,
        start_at_range: { start_at: startAt, end_at: endAt },
        segment_filters: [
          { service_variation_id: serviceVariationId,
            team_member_id_filter: teamMemberId ? { any: [teamMemberId] } : undefined }
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

async function sqCreateBooking ({ locationId, customerId, serviceVariationId, teamMemberId, startAt, durationMin }) {
  const r = await fetch('https://connect.squareup.com/v2/bookings', {
    method: 'POST',
    headers: SQ_HEADERS,
    body: JSON.stringify({
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
    })
  })
  if (!r.ok) throw new Error(`createBooking ${r.status}: ${await r.text()}`)
  const j = await r.json()
  return j.booking
}

// ==== Ofertas fallback ====
function ceil30 (d) {
  const m = d.minute()
  const add = (m % 30 === 0) ? 0 : (30 - (m % 30))
  return d.add(add, 'minute').second(0).millisecond(0)
}
function genOffers ({ days = 5 }) {
  const res = []
  let cur = ceil30(dayjs().add(Number(BOT_NOW_OFFSET_MIN)||0, 'minute'))
  const end = dayjs().add(days, 'day').endOf('day')
  while (cur.isBefore(end)) {
    const wd = cur.day()
    const hm = cur.format('HH:mm')
    const open = (wd >= 1 && wd <= 5) &&
      ((hm >= '10:00' && hm < '14:00') || (hm >= '16:00' && hm < '20:00')) &&
      !isHoliday(cur)
    if (open) res.push(cur.toISOString())
    cur = cur.add(30, 'minute')
  }
  return res.slice(0, 12)
}
function pickTimeFromText (text, offersISO, preferDay) {
  const t = norm(text)
  const m = t.match(/\b(?:a\s+las?\s+)?(\d{1,2})(?:[:.\s](\d{2}))?\b/)
  if (!m) return null
  const hh = String(m[1]).padStart(2, '0')
  const mm = String(m[2] || '00').padStart(2, '0')
  const wanted = `${hh}:${mm}`
  let list = offersISO
  if (preferDay) {
    list = offersISO.filter(x => dayjs(x).format('YYYY-MM-DD') === preferDay)
      .concat(offersISO.filter(x => dayjs(x).format('YYYY-MM-DD') !== preferDay))
  }
  return list.find(iso => dayjs(iso).format('HH:mm') === wanted) || null
}

// ==== Textos ====
const WELCOME =
  `Gracias por comunicarte con Gapink Nails. Por favor, haznos saber cómo podemos ayudarte.\n\n` +
  `${openingMsg()}\n\n` +
  `Si quieres reservar una cita puedes hacerlo a través de este link:\n\n` +
  `https://gapinknails.square.site/\n\n` +
  `Y si quieres modificarla puedes hacerlo a través del link del sms que llega con su cita.\n\n` +
  `Para cualquier otra consulta, déjenos saber y en el horario establecido le responderemos.\nGracias 😘`

const ASK_LOC = '¿En qué salón te viene mejor, **Málaga – La Luz** o **Torremolinos**?'
const ASK_LASH =
  `¿Qué servicio de **pestañas** necesitas?\n` +
  `* Lifting + tinte\n* Extensiones nuevas: pelo a pelo (clásicas) / 2D / 3D\n* Relleno: pelo a pelo / 2D / 3D\n* Quitar extensiones\n\n` +
  `Escribe por ejemplo: "Extensiones 2D", "Relleno pelo a pelo" o "Lifting + tinte".`

const servicePretty = (k)=>({
  LIFITNG_DE_PESTANAS_Y_TINTE:'Lifting + tinte',
  EXTENSIONES_DE_PESTANAS_NUEVAS_PELO_A_PELO:'Extensiones nuevas pelo a pelo',
  EXTENSIONES_PESTANAS_NUEVAS_2D:'Extensiones nuevas 2D',
  EXTENSIONES_PESTANAS_NUEVAS_3D:'Extensiones nuevas 3D',
  RELLENO_EXTENSIONES_PESTANAS_PELO_A_PELO:'Relleno pelo a pelo',
  RELLENO_PESTANAS_2D:'Relleno 2D',
  RELLENO_PESTANAS_3D:'Relleno 3D',
  QUITAR_EXTENSIONES_PESTANAS:'Quitar extensiones de pestañas'
}[k]||k)

const offersText = (offersISO, label, svc) => {
  if (!offersISO?.length) return 'No tengo huecos ahora mismo.'
  const lines = offersISO.slice(0,3).map(iso=>`* ${dayjs(iso).format('dddd DD/MM HH:mm')}`)
  return `Tengo estos huecos para **${svc}** en **${label}**:\n${lines.join('\n')}\n\n¿Te viene bien el primero? Si prefieres otro día/hora, dímelo.`
}

// ==== WhatsApp ====
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
      const should = (lastDisconnect?.error)?.output?.statusCode !== DisconnectReason.loggedOut
      console.log(should ? '❌ Conexión cerrada. Reintentando…' : '🔴 Sesión cerrada.')
      if (should) startBot()
    } else if (connection === 'open') {
      console.log('✅ WhatsApp listo')
    }
  })

  sock.ev.on('messages.upsert', async ({ messages }) => {
    const msg = messages?.[0]
    if (!msg || msg.key.fromMe) return
    const jid = msg.key.remoteJid
    const text = msg.message?.conversation ||
                 msg.message?.extendedTextMessage?.text ||
                 msg.message?.ephemeralMessage?.message?.conversation || ''
    if (!text) return

    const S = getS(jid)
    S.history.push(text); if (S.history.length>20) S.history=S.history.slice(-20)

    if (S.step === 'INIT') {
      await sock.sendMessage(jid, { text: WELCOME })
      S.step = 'FLOW'
    }

    // Cancelación
    if (/cancel(ar|a|acion|ación)/i.test(text)) {
      await sock.sendMessage(jid, { text: `Para cancelar o mover tu cita, usa el enlace del SMS. Si no lo tienes, dime **nombre completo** y **fecha**.` })
      return
    }

    // Local
    if (!S.locationId) {
      const loc = detectLocation(text)
      if (loc) { S.locationKey=loc.key; S.locationId=loc.id; S.locationLabel=loc.label }
    }

    // Servicio
    if (!S.serviceKey) {
      let sv = detectServiceRules(text)
      if (!sv) sv = await detectServiceIA(S.history)
      if (sv) { S.serviceKey = sv; S.durationMin = SVC_DUR[sv] || 60 }
    }

    // Staff “con X”
    const mStaff = norm(text).match(/\bcon\s+([a-z\u00f1]+(?:\s+[a-z\u00f1]+)?)\b/)
    if (mStaff) {
      const name = mStaff[1]
      S.staffName = name
      S.staffId = STAFF_NAME_TO_ID[name] || null
    }

    if (!S.locationId) { await sock.sendMessage(jid, { text: ASK_LOC }); return }
    if (!S.serviceKey) {
      if (/\bpestan/i.test(text)) await sock.sendMessage(jid, { text: ASK_LASH })
      else await sock.sendMessage(jid, { text: `¿Qué servicio necesitas? (ej.: “Extensiones 2D”, “Lifting + tinte”).` })
      return
    }

    // Ofrecer huecos
    if (!S.offersISO.length) {
      const variation = svcVariationId(S.serviceKey, S.locationKey)
      let offers = []
      if (variation) {
        const start = dayjs().toISOString()
        const end = dayjs().add(Number(BOT_SEARCH_WINDOW_DAYS)||14,'day').toISOString()
        offers = await sqSearchAvailability({
          locationId: S.locationId, serviceVariationId: variation,
          teamMemberId: S.staffId || undefined, startAt: start, endAt: end, max: 10
        })
      }
      if (!offers.length) offers = genOffers({ days: 5 })
      S.offersISO = offers
      if (!offers.length) {
        await sock.sendMessage(jid, { text: `Ahora mismo no veo huecos. Dime un **día y hora** aproximados y lo miro.` })
        return
      }
      await sock.sendMessage(jid, { text: offersText(offers, S.locationLabel, servicePretty(S.serviceKey)) })
      S.step = 'TIME'
      return
    }

    // Elegir hora
    if (S.step === 'TIME') {
      if (/\b(primer[oa]?|el primero|vale|ok|perfecto)\b/i.test(text)) {
        S.selectedISO = S.offersISO[0]
      } else {
        const preferDay = dayjs(S.offersISO[0]).format('YYYY-MM-DD')
        S.selectedISO = pickTimeFromText(text, S.offersISO, preferDay)
      }
      if (!S.selectedISO) {
        await sock.sendMessage(jid, { text: `Dime la hora exacta de las opciones (por ej.: “a las ${dayjs(S.offersISO[0]).format('HH:mm')}”).` })
        return
      }
      S.step = 'NAME'
      await sock.sendMessage(jid, { text: `Para cerrar, dime tu **nombre y apellidos**.` })
      return
    }

    if (S.step === 'NAME') {
      const name = text.trim()
      if (name.split(' ').length < 2) { await sock.sendMessage(jid, { text: `Por favor, **nombre y apellidos**.` }); return }
      S.name = name
      S.step = 'BOOK'
    }

    if (S.step === 'BOOK') {
      try {
        const [given, ...rest] = S.name.split(' ')
        const family = rest.join(' ')
        const variation = svcVariationId(S.serviceKey, S.locationKey)
        const customerId = await sqCreateCustomer({ given, family, phone: jid.split('@')[0] })
        await sqCreateBooking({
          locationId: S.locationId,
          customerId,
          serviceVariationId: variation,
          teamMemberId: S.staffId || undefined,
          startAt: S.selectedISO,
          durationMin: S.durationMin
        })
        await sock.sendMessage(jid, {
          text:
`Reserva confirmada 🎉
Salón: ${S.locationLabel}
Servicio: ${servicePretty(S.serviceKey)}
Profesional: ${S.staffName ? `${S.staffName} (si está disponible)` : 'cualquiera'}
Fecha: ${dayjs(S.selectedISO).format('dddd DD/MM HH:mm')}
Duración: ${S.durationMin} min
Pago en persona.

Si quieres modificarla, usa el link del SMS que te llega.`
        })
        SESSIONS.delete(jid)
      } catch (e) {
        console.error(e)
        await sock.sendMessage(jid, { text: `No he podido confirmar la reserva ahora. ¿Prefieres que te pase **otros huecos** o reservar desde:\nhttps://gapinknails.square.site/` })
      }
    }
  })
}

// ---- Healthcheck web ----
http.createServer((_, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' })
  res.end('Gapink WhatsApp bot OK\n')
}).listen(8080, () => console.log('🌐 Web 8080'))

// ---- Run ----
startBot().catch(e => { console.error(e); process.exit(1) })
