import express from "express"
import baileys from "@whiskeysockets/baileys"
import pino from "pino"
import qrcode from "qrcode"
import qrcodeTerminal from "qrcode-terminal"
import "dotenv/config"
import OpenAI from "openai"
import fs from "fs"
import { webcrypto } from "crypto"          // 🔧 FIX WebCrypto

// Exponer WebCrypto como global (Baileys lo espera)
if (!globalThis.crypto) globalThis.crypto = webcrypto

const { makeWASocket, useMultiFileAuthState, DisconnectReason } = baileys
const client = new OpenAI({ apiKey: process.env.OPENAI_API_KEY })

// --- Web server para mantener vivo Railway
const app = express()
const PORT = process.env.PORT || 8080

app.get("/", (_req, res) => res.send("Gapink Nails WhatsApp Bot ✅ OK"))

// QR como PNG en vivo
let lastQR = null
app.get("/qr.png", async (_req, res) => {
  try {
    if (!lastQR) return res.status(404).send("No hay QR activo ahora mismo")
    const png = await qrcode.toBuffer(lastQR, { type: "png", margin: 1, width: 512 })
    res.set("Content-Type", "image/png").send(png)
  } catch (e) {
    res.status(500).send("Error generando QR")
  }
})

app.listen(PORT, () => {
  console.log(`🌐 Servidor web escuchando en el puerto ${PORT}`)
  startBot().catch((e) => console.error("Fallo al iniciar el bot:", e))
})

// --- Baileys + reconexión con backoff y QR en logs/PNG
const AUTH_DIR = "auth_info"
let reconnectAttempts = 0

async function startBot() {
  console.log("🚀 Iniciando bot de Gapink Nails...")

  if (!fs.existsSync(AUTH_DIR)) fs.mkdirSync(AUTH_DIR, { recursive: true })
  const { state, saveCreds } = await useMultiFileAuthState(AUTH_DIR)

  const sock = makeWASocket({
    logger: pino({ level: "silent" }),
    printQRInTerminal: false,
    auth: state,
    syncFullHistory: false
  })

  sock.ev.on("connection.update", async (update) => {
    const { connection, lastDisconnect, qr } = update

    if (qr) {
      lastQR = qr
      console.log("📲 Escanéalo YA (caduca en ~20s). También disponible en /qr.png")
      qrcodeTerminal.generate(qr, { small: true })
    }

    if (connection === "open") {
      reconnectAttempts = 0
      lastQR = null
      console.log("✅ Bot conectado a WhatsApp correctamente.")
    }

    if (connection === "close") {
      const err = lastDisconnect?.error
      const status = err?.output?.statusCode ?? err?.status ?? "desconocido"
      const msg = err?.message ?? String(err ?? "")
      console.log(`❌ Conexión cerrada. Status: ${status}. Motivo: ${msg}`)

      const shouldRelogin =
        status === DisconnectReason.loggedOut ||
        /logged.?out|invalid|bad session/i.test(msg)

      if (shouldRelogin || reconnectAttempts < 8) {
        const waitMs = Math.min(30000, 1000 * Math.pow(2, reconnectAttempts)) // 1s→30s
        reconnectAttempts++
        console.log(`🔄 Reintentando en ${waitMs}ms...`)
        setTimeout(() => startBot().catch(console.error), waitMs)
      } else {
        console.log("🛑 Demasiados reintentos. Me quedo vivo gracias a Express; revisa /logs y /qr.png.")
      }
    }
  })

  sock.ev.on("creds.update", saveCreds)

  // Mensajes -> GPT-4o-mini
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
        { role: "system", content: "Eres el asistente de Gapink Nails: amable, breve y profesional." },
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
