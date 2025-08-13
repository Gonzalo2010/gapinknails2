import express from "express"
import baileys from "@whiskeysockets/baileys"
import pino from "pino"
import qrcode from "qrcode"
import qrcodeTerminal from "qrcode-terminal"
import "dotenv/config"
import OpenAI from "openai"
import fs from "fs"
import { webcrypto } from "crypto"

if (!globalThis.crypto) globalThis.crypto = webcrypto

const {
  makeWASocket,
  useMultiFileAuthState,
  DisconnectReason,
  fetchLatestBaileysVersion,
  Browsers
} = baileys

const client = new OpenAI({ apiKey: process.env.OPENAI_API_KEY })

const app = express()
const PORT = process.env.PORT || 8080

let lastQR = null
let conectado = false

app.get("/", (_req, res) => {
  res.send(`
    <!DOCTYPE html>
    <html lang="es">
    <head>
      <meta charset="UTF-8">
      <meta name="viewport" content="width=device-width, initial-scale=1.0">
      <title>Gapink Nails – Estado del Bot</title>
      <style>
        body {
          font-family: 'Poppins', sans-serif;
          background: linear-gradient(135deg, #fce4ec, #f8bbd0);
          margin: 0;
          padding: 0;
          display: flex;
          flex-direction: column;
          align-items: center;
          justify-content: center;
          min-height: 100vh;
          color: #4a148c;
          text-align: center;
        }
        h1 {
          font-size: 2.5rem;
          margin-bottom: 10px;
        }
        .status {
          background: white;
          padding: 20px;
          border-radius: 12px;
          box-shadow: 0 5px 20px rgba(0,0,0,0.1);
          max-width: 400px;
          width: 90%;
        }
        img {
          margin-top: 15px;
          max-width: 100%;
        }
        footer {
          margin-top: 20px;
          font-size: 0.9rem;
          opacity: 0.7;
        }
        footer a {
          color: #4a148c;
          text-decoration: none;
        }
        footer a:hover {
          text-decoration: underline;
        }
      </style>
    </head>
    <body>
      <div class="status">
        <h1>Gapink Nails</h1>
        <p><strong>Estado:</strong> ${conectado ? "✅ Conectado" : "❌ Desconectado"}</p>
        ${!conectado && lastQR ? `<img src="/qr.png" alt="QR para vincular">` : ""}
      </div>
      <footer>Desarrollado por <a href="https://gonzalog.co" target="_blank" rel="noopener">Gonzalo García Aranda</a></footer>
    </body>
    </html>
  `)
})

app.get("/qr.png", async (_req, res) => {
  try {
    if (!lastQR) return res.status(404).send("No hay QR activo ahora mismo")
    const png = await qrcode.toBuffer(lastQR, { type: "png", margin: 1, width: 512 })
    res.set("Content-Type", "image/png").send(png)
  } catch {
    res.status(500).send("Error generando QR")
  }
})

app.listen(PORT, () => {
  console.log(`🌐 Servidor web escuchando en el puerto ${PORT}`)
  startBot().catch((e) => console.error("Fallo al iniciar el bot:", e))
})

const AUTH_DIR = "auth_info"
let reconnectAttempts = 0

async function startBot() {
  console.log("🚀 Iniciando bot de Gapink Nails...")

  if (!fs.existsSync(AUTH_DIR)) fs.mkdirSync(AUTH_DIR, { recursive: true })
  const { state, saveCreds } = await useMultiFileAuthState(AUTH_DIR)

  const { version } = await fetchLatestBaileysVersion()
  console.log("ℹ️ Versión WA Web usada por Baileys:", version)

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
      console.log("📲 Escanéalo YA (caduca en ~20s). También disponible en /qr.png")
      qrcodeTerminal.generate(qr, { small: true })
    }

    if (connection === "open") {
      reconnectAttempts = 0
      lastQR = null
      conectado = true
      console.log("✅ Bot conectado a WhatsApp correctamente.")
    }

    if (connection === "close") {
      conectado = false
      const err = lastDisconnect?.error
      const status = err?.output?.statusCode ?? err?.status ?? "desconocido"
      const msg = err?.message ?? String(err ?? "")
      console.log(`❌ Conexión cerrada. Status: ${status}. Motivo: ${msg}`)

      const shouldRelogin =
        status === DisconnectReason.loggedOut ||
        /logged.?out|invalid|bad session/i.test(msg)

      if (shouldRelogin || reconnectAttempts < 8) {
        const waitMs = Math.min(30000, 1000 * Math.pow(2, reconnectAttempts))
        reconnectAttempts++
        console.log(`🔄 Reintentando en ${waitMs}ms...`)
        setTimeout(() => startBot().catch(console.error), waitMs)
      }
    }
  })

  sock.ev.on("creds.update", saveCreds)

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
