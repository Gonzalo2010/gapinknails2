import express from "express"
import baileys from "@whiskeysockets/baileys"
import pino from "pino"
import qrcode from "qrcode"
import qrcodeTerminal from "qrcode-terminal"
import "dotenv/config"
import OpenAI from "openai"
import fs from "fs"
import { webcrypto } from "crypto"
import Database from "better-sqlite3"
import dayjs from "dayjs"
import { Client, Environment } from "square"

if (!globalThis.crypto) globalThis.crypto = webcrypto

const {
  makeWASocket,
  useMultiFileAuthState,
  DisconnectReason,
  fetchLatestBaileysVersion,
  Browsers
} = baileys

// --------- CONFIG HORARIOS / NEGOCIO ----------
const WORK_DAYS = [1,2,3,4,5,6] // 1=lun ... 6=sáb (dom=7 cerrado)
const OPEN_HOUR = 10
const CLOSE_HOUR = 20
const SLOT_MIN = 30

// Servicios (duración aprox)
const SERVICES = {
  "manicura": 45,
  "pedicura": 60,
  "uñas acrílicas": 90,
  "relleno": 75,
  "semipermanente": 45
}

// Técnicas internas (no mostrar nombres)
const STAFF = [
  { id: "s1", name: "Ana" },
  { id: "s2", name: "Laura" },
  { id: "s3", name: "Marta" },
  { id: "s4", name: "Sara" }
]

// --------- OPENAI ----------
const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY })

// Prompt “humano” y sin revelar técnica
const SYSTEM_PROMPT = `Eres el asistente de WhatsApp de Gapink Nails. Tu objetivo es sonar humano (natural y cercano), no robótico.
Reglas clave:
- Nunca reveles el nombre de la técnica ni ofrezcas elegir profesional.
- Prioriza siempre rellenar los huecos más próximos de la semana. Propón el primer hueco disponible que encaje.
- Sé breve y claro (frases cortas). Nada de párrafos eternos. No repitas información.
- No uses emojis. Tono amable y profesional, con calidez.
- El pago siempre es en persona; no ofrezcas pagos online.
- Si el cliente es nuevo y falta dato, pide SOLO lo imprescindible (nombre y email) sin agobiar.
- Si el mensaje NO trata de reservas, cambios, cancelaciones, disponibilidad u horarios de atención, no respondas.
- Si pide algo muy específico (ej. una técnica concreta), redirígelo con tacto: “Te propongo el primer hueco disponible” sin mencionar nombres.
- En confirmaciones, incluye servicio, fecha/hora y que el pago será en persona.
- Lenguaje: español de España, correcto pero cercano.
- Jamás hables de que eres una IA ni de “modelos” o “prompts”.`

// Clasificador: decide si responder (solo citas/reservas)
async function isBookingIntent(text) {
  try {
    const r = await openai.chat.completions.create({
      model: "gpt-4o-mini",
      messages: [
        { role: "system", content:
`Eres un clasificador. Devuelve SOLO una palabra:
- "BOOKING" si el mensaje trata de reservar, cita, pedir hora, disponibilidad, cambiar o cancelar cita, servicio de uñas o precios/horarios relacionados con reservar.
- "IGNORE" en cualquier otro caso.` },
        { role: "user", content: text || "" }
      ],
      temperature: 0
    })
    const out = (r.choices[0].message.content || "").trim().toUpperCase()
    return out.startsWith("BOOKING")
  } catch (e) {
    console.error("Classifier error:", e)
    return false // silencioso
  }
}

// Generador de respuesta breve y humana (solo para mensajes de cita)
async function aiReply(text) {
  try {
    const r = await openai.chat.completions.create({
      model: "gpt-4o-mini",
      messages: [
        { role: "system", content: SYSTEM_PROMPT },
        { role: "user", content: text }
      ],
      temperature: 0.4
    })
    return r.choices[0].message.content?.trim() || ""
  } catch (e) {
    console.error("AI reply error:", e)
    return "" // silencioso
  }
}

// --------- SQUARE ----------
const square = new Client({
  accessToken: process.env.SQUARE_ACCESS_TOKEN,
  environment: process.env.SQUARE_ENV === "production" ? Environment.Production : Environment.Sandbox
})
const locationId = process.env.SQUARE_LOCATION_ID

async function squareFindCustomerByPhone(phone) {
  try {
    const resp = await square.customersApi.searchCustomers({
      query: { filter: { phoneNumber: { exact: phone } } }
    })
    const list = resp?.result?.customers || []
    return list[0] || null
  } catch (e) {
    console.error("Square search error:", e?.message || e)
    return null
  }
}

async function squareCreateCustomer({ givenName, emailAddress, phoneNumber }) {
  try {
    const resp = await square.customersApi.createCustomer({
      idempotencyKey: `cust_${Date.now()}_${Math.random().toString(36).slice(2,8)}`,
      givenName,
      emailAddress,
      phoneNumber,
      note: "Creado desde bot WhatsApp Gapink Nails"
    })
    return resp?.result?.customer || null
  } catch (e) {
    console.error("Square create error:", e?.message || e)
    return null
  }
}

// --------- DB ----------
const db = new Database("gapink.db")
db.pragma("journal_mode = WAL")

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
  status TEXT,         -- confirmed | cancelled
  created_at TEXT
);

CREATE TABLE IF NOT EXISTS holds (
  token TEXT PRIMARY KEY,
  phone TEXT,
  service TEXT,
  duration_min INTEGER,
  start_iso TEXT,
  end_iso TEXT,
  expires_at TEXT,
  created_at TEXT
);

CREATE TABLE IF NOT EXISTS sessions (
  phone TEXT PRIMARY KEY,
  state TEXT,          -- ask_name | ask_email | idle
  data_json TEXT,
  updated_at TEXT
);
`)

const insertAppt = db.prepare(`INSERT INTO appointments
(id, customer_name, customer_phone, customer_square_id, service, duration_min, start_iso, end_iso, staff_id, status, created_at)
VALUES (@id, @customer_name, @customer_phone, @customer_square_id, @service, @duration_min, @start_iso, @end_iso, @staff_id, @status, @created_at)`)

const listApptsBetween = db.prepare(`SELECT * FROM appointments WHERE status='confirmed' AND start_iso < @to AND end_iso > @from`)
const updateApptStatus = db.prepare(`UPDATE appointments SET status=@status WHERE id=@id`)
const getUpcomingByPhone = db.prepare(`SELECT * FROM appointments WHERE customer_phone=@phone AND status='confirmed' AND start_iso > @now ORDER BY start_iso ASC LIMIT 1`)

const upsertSession = db.prepare(`
INSERT INTO sessions (phone, state, data_json, updated_at) VALUES (@phone, @state, @data_json, @updated_at)
ON CONFLICT(phone) DO UPDATE SET state=excluded.state, data_json=excluded.data_json, updated_at=excluded.updated_at
`)
const getSession = db.prepare(`SELECT * FROM sessions WHERE phone=@phone`)
const clearSession = db.prepare(`DELETE FROM sessions WHERE phone=@phone`)

// --------- UTIL SLOTS ----------
function* slotsGenerator(fromDay, daysAhead = 10) {
  const start = dayjs(fromDay)
  for (let d=0; d<daysAhead; d++) {
    const day = start.add(d, "day")
    const dow = (day.day()+6)%7 + 1
    if (!WORK_DAYS.includes(dow)) continue
    let t = day.hour(OPEN_HOUR).minute(0).second(0).millisecond(0)
    const endOfDay = day.hour(CLOSE_HOUR).minute(0)
    while (t.add(SLOT_MIN, "minute").isBefore(endOfDay) || t.add(SLOT_MIN, "minute").isSame(endOfDay)) {
      yield t
      t = t.add(SLOT_MIN, "minute")
    }
  }
}

function overlaps(aStart, aEnd, bStart, bEnd) {
  return (aStart < bEnd) && (bStart < aEnd)
}

function getBookedIntervals(fromIso, toIso) {
  const appts = listApptsBetween.all({ from: fromIso, to: toIso })
  return appts.map(a => ({ start: dayjs(a.start_iso), end: dayjs(a.end_iso), staff_id: a.staff_id }))
}

function staffHasFree(intervals, start, end) {
  for (const st of STAFF) {
    const busy = intervals
      .filter(i => i.staff_id === st.id)
      .some(i => overlaps(start, end, i.start, i.end))
    if (!busy) return true
  }
  return false
}

// Primer hueco de la semana (prioriza llenar huecos cercanos)
function firstSlotThisWeek(service, durationMin) {
  const now = dayjs().second(0).millisecond(0)
  const endOfWeek = now.day() === 0 ? now.add(6, "day") : now.day(6) // sábado
  const intervals = getBookedIntervals(now.toISOString(), endOfWeek.endOf("day").toISOString())
  for (const s of slotsGenerator(now, 7)) {
    if (s.isAfter(endOfWeek.endOf("day"))) break
    const start = s
    const end = s.add(durationMin, "minute")
    if (start.isBefore(now.add(30, "minute"))) continue
    if (end.hour() > CLOSE_HOUR || (end.hour() === CLOSE_HOUR && end.minute() > 0)) continue
    if (staffHasFree(intervals, start, end)) return { start, end }
  }
  // Si no hay en la semana, el primero en 10 días
  const intervals10 = getBookedIntervals(now.toISOString(), now.add(10, "day").toISOString())
  for (const s of slotsGenerator(now, 10)) {
    const start = s
    const end = s.add(durationMin, "minute")
    if (start.isBefore(now.add(30, "minute"))) continue
    if (end.hour() > CLOSE_HOUR || (end.hour() === CLOSE_HOUR && end.minute() > 0)) continue
    if (staffHasFree(intervals10, start, end)) return { start, end }
  }
  return null
}

function randomId(prefix="apt") {
  return `${prefix}_${Math.random().toString(36).slice(2,8)}${Date.now().toString(36).slice(-4)}`
}

// --------- WEB (estado/QR bonito) ----------
const app = express()
const PORT = process.env.PORT || 8080

let lastQR = null
let conectado = false

app.get("/", (_req, res) => {
  res.send(`
<!DOCTYPE html><html lang="es"><head><meta charset="UTF-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Gapink Nails – Estado</title>
<style>
  :root{--bg1:#fce4ec;--bg2:#f8bbd0;--fg:#4a148c;}
  *{box-sizing:border-box}
  body{font-family:system-ui,-apple-system,"Segoe UI",Roboto,Ubuntu,"Helvetica Neue",Arial;background:linear-gradient(135deg,var(--bg1),var(--bg2));margin:0;min-height:100vh;display:flex;align-items:center;justify-content:center;color:var(--fg)}
  .card{background:#fff;padding:24px 28px;border-radius:16px;box-shadow:0 6px 24px rgba(0,0,0,.08);max-width:480px;width:92%;text-align:center}
  h1{margin:0 0 6px;font-size:28px}
  .status{font-weight:600;margin-bottom:10px}
  img{margin-top:12px;max-width:100%;border-radius:12px}
  footer{margin-top:16px;opacity:.8;font-size:13px}
  footer a{color:var(--fg);text-decoration:none}
  footer a:hover{text-decoration:underline}
  .hint{margin-top:8px;font-size:13px;opacity:.8}
</style></head><body>
<div class="card">
<h1>Gapink Nails</h1>
<div class="status">Estado: ${conectado ? "✅ Conectado" : "❌ Desconectado"}</div>
${!conectado && lastQR ? `<img src="/qr.png" alt="QR para vincular" /> <div class="hint">Escanear en WhatsApp → Dispositivos vinculados</div>` : ""}
<footer>Desarrollado por <a href="https://gonzalog.co" target="_blank" rel="noopener">Gonzalo García Aranda</a></footer>
</div></body></html>
  `)
})

app.get("/estado", (_req, res) => {
  res.json({ conectado, qr: !conectado && lastQR ? "/qr.png" : null })
})

app.get("/qr.png", async (_req, res) => {
  try {
    if (!lastQR) return res.status(404).send("No hay QR activo ahora mismo")
    const png = await qrcode.toBuffer(lastQR, { type: "png", margin: 1, width: 512 })
    res.set("Content-Type", "image/png").send(png)
  } catch (e) {
    console.error("QR route error:", e)
    res.status(500).send("Error")
  }
})

// --------- ARRANQUE WEB + BOT ----------
app.listen(PORT, () => {
  console.log(`🌐 Web escuchando en puerto ${PORT}`)
  startBot().catch((e)=>console.error("Fallo al iniciar bot:", e))
})

// --------- BOT ----------
async function startBot() {
  console.log("🚀 Iniciando bot Gapink Nails...")

  try {
    if (!fs.existsSync("auth_info")) fs.mkdirSync("auth_info", { recursive: true })
    const { state, saveCreds } = await useMultiFileAuthState("auth_info")
    const { version } = await fetchLatestBaileysVersion()
    console.log("ℹ️ Versión WA Web:", version)

    const sock = makeWASocket({
      logger: pino({ level: "silent" }),
      printQRInTerminal: false,
      auth: state,
      version,
      browser: Browsers.macOS("Desktop"),
      syncFullHistory: false,
      connectTimeoutMs: 30000
    })

    sock.ev.on("connection.update", (update) => {
      const { connection, lastDisconnect, qr } = update
      if (qr) {
        lastQR = qr
        conectado = false
        console.log("QR listo (caduca ~20s). También en /qr.png")
        try { qrcodeTerminal.generate(qr, { small: true }) } catch {}
      }
      if (connection === "open") {
        lastQR = null
        conectado = true
        console.log("Bot conectado a WhatsApp.")
      }
      if (connection === "close") {
        conectado = false
        const err = lastDisconnect?.error
        const status = err?.output?.statusCode ?? err?.status ?? "desconocido"
        const msg = err?.message ?? String(err ?? "")
        console.log(`Conexión cerrada. Status: ${status}. Motivo: ${msg}`)
        setTimeout(()=>startBot().catch(console.error), 3000)
      }
    })
    sock.ev.on("creds.update", saveCreds)

    // Helper para enviar sin romper el flujo
    async function safeSend(jid, content) {
      try { await sock.sendMessage(jid, content) } catch (e) { console.error("sendMessage error:", e) }
    }

    // Mensajería
    sock.ev.on("messages.upsert", async ({ messages }) => {
      try {
        const msg = messages?.[0]
        if (!msg?.message || msg.key.fromMe) return

        const from = msg.key.remoteJid
        const phone = from?.split("@")[0] || ""
        const body =
          msg.message.conversation ||
          msg.message.extendedTextMessage?.text ||
          msg.message?.imageMessage?.caption ||
          ""
        const low = (body || "").trim().toLowerCase()

        // STATE MACHINE (si ya estamos pidiendo datos, saltamos clasificador)
        const sess = getSession.get({ phone })
        if (sess?.state === "ask_name") {
          const name = (body || "").trim().replace(/\s+/g, " ").slice(0,80)
          const data = JSON.parse(sess.data_json || "{}")
          data.name = name
          upsertSession.run({ phone, state: "ask_email", data_json: JSON.stringify(data), updated_at: new Date().toISOString() })
          await safeSend(from, { text: "¿Cuál es tu email? (para enviarte la confirmación)" })
          return
        }
        if (sess?.state === "ask_email") {
          const email = (body || "").trim()
          const data = JSON.parse(sess.data_json || "{}")
          data.email = email

          // Crear cliente en Square
          let sq = null
          try {
            sq = await squareCreateCustomer({
              givenName: data.name || "Cliente",
              emailAddress: email || undefined,
              phoneNumber: phone
            })
          } catch (e) { console.error("createCustomer error:", e) }

          if (!sq) { clearSession.run({ phone }); return } // silencio

          // Agendar primer hueco de la semana
          const dur = data.durationMin
          const svc = data.service
          const slot = firstSlotThisWeek(svc, dur)
          if (!slot) { clearSession.run({ phone }); return }

          const aptId = randomId("apt")
          insertAppt.run({
            id: aptId,
            customer_name: data.name,
            customer_phone: phone,
            customer_square_id: sq.id,
            service: svc,
            duration_min: dur,
            start_iso: slot.start.toISOString(),
            end_iso: slot.end.toISOString(),
            staff_id: null,
            status: "confirmed",
            created_at: new Date().toISOString()
          })

          clearSession.run({ phone })
          await safeSend(from, { text:
`Reserva confirmada.
Servicio: ${svc}
Fecha: ${slot.start.format("dddd DD/MM HH:mm")}
Duración: ${dur} min
Pago en persona.` })
          return
        }

        // ---- CLASIFICACIÓN: si no es de cita, NO respondemos
        const seemsBooking = await isBookingIntent(body || "")
        if (!seemsBooking) return

        // Cancelar
        if (/(cancel(ar)? cita)/i.test(low)) {
          const upcoming = getUpcomingByPhone.get({ phone, now: new Date().toISOString() })
          if (!upcoming) return
          updateApptStatus.run({ id: upcoming.id, status: "cancelled" })
          await safeSend(from, { text: "Tu cita ha sido cancelada. Si quieres, pide una nueva con “cita manicura”." })
          return
        }

        // Cambiar / mover
        if (/(cambiar cita|mover cita|reprogramar)/i.test(low)) {
          const upcoming = getUpcomingByPhone.get({ phone, now: new Date().toISOString() })
          if (!upcoming) return
          const slot = firstSlotThisWeek(upcoming.service, upcoming.duration_min)
          if (!slot) return
          db.prepare(`UPDATE appointments SET start_iso=@s, end_iso=@e WHERE id=@id`).run({
            id: upcoming.id, s: slot.start.toISOString(), e: slot.end.toISOString()
          })
          await safeSend(from, { text:
`He movido tu cita a:
${slot.start.format("dddd DD/MM HH:mm")}
Pago en persona.` })
          return
        }

        // “cita / reservar / disponibilidad” con servicio
        const svc = Object.keys(SERVICES).find(s => low.includes(s))
        if (/(reserva|reservar|cita|disponible|disponibilidad|pedir hora|hueco)/i.test(low) && svc) {
          const durationMin = SERVICES[svc]
          let customer = null
          try { customer = await squareFindCustomerByPhone(phone) } catch (e) { console.error("findCustomer error:", e) }

          if (customer) {
            const slot = firstSlotThisWeek(svc, durationMin)
            if (!slot) return
            const aptId = randomId("apt")
            insertAppt.run({
              id: aptId,
              customer_name: customer.givenName || null,
              customer_phone: phone,
              customer_square_id: customer.id,
              service: svc,
              duration_min: durationMin,
              start_iso: slot.start.toISOString(),
              end_iso: slot.end.toISOString(),
              staff_id: null,
              status: "confirmed",
              created_at: new Date().toISOString()
            })
            await safeSend(from, { text:
`Reserva confirmada.
Servicio: ${svc}
Fecha: ${slot.start.format("dddd DD/MM HH:mm")}
Duración: ${durationMin} min
Pago en persona.` })
            return
          } else {
            const data = { service: svc, durationMin }
            upsertSession.run({ phone, state: "ask_name", data_json: JSON.stringify(data), updated_at: new Date().toISOString() })
            await safeSend(from, { text: "Para confirmar la reserva, dime tu nombre y apellidos." })
            return
          }
        }

        // Si habla de disponibilidad pero SIN servicio, pedimos servicio (breve)
        if (/(reserva|reservar|cita|disponible|disponibilidad|pedir hora|hueco)/i.test(low) && !Object.keys(SERVICES).some(s=>low.includes(s))) {
          const txt = Object.keys(SERVICES).map(s=>`• ${s} (${SERVICES[s]} min)`).join("\n")
          await safeSend(from, { text:
`¿Qué servicio necesitas?
${txt}

Ejemplos:
- "cita manicura"
- "reservar pedicura"` })
          return
        }

        // Fallback breve (pero solo si es booking)
        const reply = await aiReply(body)
        if (reply) await safeSend(from, { text: reply })
      } catch (e) {
        console.error("messages.upsert handler error:", e) // nunca se envía al usuario
      }
    })
  } catch (e) {
    console.error("startBot error:", e)
  }
}
