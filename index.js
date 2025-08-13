import express from "express"
import baileys from "@whiskeysockets/baileys"
import pino from "pino"
import qrcode from "qrcode-terminal"
import "dotenv/config"
import OpenAI from "openai"
import fs from "fs"

const { makeWASocket, useMultiFileAuthState, DisconnectReason } = baileys
const client = new OpenAI({ apiKey: process.env.OPENAI_API_KEY })

// ---- Servidor web para que Railway mantenga vivo el proceso
const app = express()
const PORT = process.env.PORT || 8080
app.get("/", (_, res) => res.send("Gapink Nails WhatsApp Bot ✅ OK")))
app.get("/health", (_, res) => res.json({ ok: true }))
app.listen(PORT, () => console.log(`🌐 Servidor web escuchando en el puerto ${PORT}`))

// ---- Bot WhatsApp (Baileys) con reconexión controlada y QR en logs
const AUTH_DIR = "auth_info"
let reconnectAttempts = 0

async function startBot() {
  console.log("🚀 Iniciando bot de Gapink Nails...")

  if (!fs.existsSync(AUTH_DIR)) fs.mkdirSync(AUTH_DIR, { recursive: true })

  const { state, saveCreds } = await useMultiFileAuthState(AUTH_DIR)

  const sock = makeWASocket({
    logger: pino({ level: "silent" }),
    // Mostramos QR en terminal SIEMPRE que WhatsApp nos lo dé
    printQRInTerminal: false,
    auth: state,
    syncFullHistory: false
  })

  sock.ev.on("connection.update", (update) => {
    const { connection, lastDisconnect, qr } = update

    if (qr) {
      console.log("📲 Escanea este QR YA (caduca en ~20s):")
      qrcode.generate(qr, { small: true }) // QR ASCII en logs de Railway
    }

    if (connection === "open") {
      reconnectAttempts = 0
      console.log("✅ Bot conectado a WhatsApp correctamente.")
    }

    if (connection === "close") {
      const error = lastDisconnect?.error
      // Intentamos sacar info útil
      const status = error?.output?.statusCode || error?.status || "desconocido"
      const message = error?.message || error?.toString?.() || "sin detalle"
      console.log(`❌ Conexión cerrada. Status: ${status}. Motivo: ${message}`)

      // Si la sesión fue cerrada desde el móvil, pedimos QR en Railway para re-vincular.
      const shouldRelogin =
        status === DisconnectReason.loggedOut ||
        /logged.?out|invalid|bad session/i.test(String(message))

      // Backoff suave para no entrar en bucle loco
      if (shouldRelogin || reconnectAttempts < 8) {
        const waitMs = Math.min(30000, 1000 * Math.pow(2, reconnectAttempts)) // 1s,2s,4s..max30s
        reconnectAttempts++
        console.log(`🔄 Reintentando en ${waitMs}ms${shouldRelogin ? " (mostraremos QR si hace falta)" : ""}...`)
        setTimeout(() => startBot().catch(console.error), waitMs)
      } else {
        console.log("🛑 Demasiados reintentos. Me quedo a la espera (el server Express sigue vivo).")
      }
    }
  })

  sock.ev.on("creds.update", saveCreds)

  // Mensajes entrantes -> GPT-4o-mini
  sock.ev.on("messages.upsert", async ({ messages }) => {
    const msg = messages?.[0]
    if (!msg?.message || msg.key.fromMe) return

    const from = msg.key.remoteJid
    const text =
      msg.message.conversation ||
      msg.message.extendedTextMessage?.text ||
      msg.message?.imageMessage?.caption ||
      ""

    if (!text) return

    console.log(`📩 ${from}: ${text}`)
    const reply = await responderGPT(text)
    await sock.sendMessage(from, { text: reply })
  })
}

async function responderGPT(userText) {
  try {
    const r = await client.chat.completions.create({
      model: "gpt-4o-mini",
      messages: [
        { role: "system", content: "Eres el asistente de Gapink Nails: amable, breve, directo y profesional." },
        { role: "user", content: userText }
      ],
      temperature: 0.7
    })
    return r.choices[0].message.content.trim()
  } catch (e) {
    console.error("Error OpenAI:", e?.message || e)
    return "Ahora mismo no puedo responder, inténtalo en un momento."
  }
}

// Arrancar el bot
startBot().catch((e) => {
  console.error("Fallo al iniciar el bot:", e)
})
