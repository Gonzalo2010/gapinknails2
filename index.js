import baileys from "@whiskeysockets/baileys"
import qrcode from "qrcode-terminal"
import pino from "pino"
import "dotenv/config"
import OpenAI from "openai"
import fs from "fs"

const { makeWASocket, useMultiFileAuthState, DisconnectReason } = baileys
const client = new OpenAI({ apiKey: process.env.OPENAI_API_KEY })

async function startBot() {
    console.log("🚀 Iniciando bot de Gapink Nails...")

    const { state, saveCreds } = await useMultiFileAuthState("auth_info")

    const sock = makeWASocket({
        logger: pino({ level: "silent" }),
        printQRInTerminal: true,
        auth: state
    })

    sock.ev.on("connection.update", (update) => {
        const { connection, lastDisconnect, qr } = update

        if (qr) {
            console.log("📲 Escanea este QR en WhatsApp → Dispositivos vinculados → Vincular dispositivo")
            qrcode.generate(qr, { small: true })
        }

        if (connection === "close") {
            const reason = lastDisconnect?.error?.output?.statusCode
            console.log("❌ Conexión cerrada. Razón:", reason)
            if (reason !== DisconnectReason.loggedOut) {
                startBot()
            }
        } else if (connection === "open") {
            console.log("✅ Bot conectado a WhatsApp correctamente.")
        }
    })

    sock.ev.on("messages.upsert", async (m) => {
        const msg = m.messages[0]
        if (!msg.message || msg.key.fromMe) return

        const from = msg.key.remoteJid
        const text = msg.message.conversation || msg.message.extendedTextMessage?.text || ""

        if (text) {
            console.log(`📩 Mensaje de ${from}: ${text}`)
            const reply = await responderGPT(text)
            await sock.sendMessage(from, { text: reply })
        }
    })

    sock.ev.on("creds.update", saveCreds)
}

async function responderGPT(userText) {
    try {
        const response = await client.chat.completions.create({
            model: "gpt-4o-mini",
            messages: [
                { role: "system", content: "Eres un asistente de Gapink Nails que responde breve, amable y profesional." },
                { role: "user", content: userText }
            ],
            temperature: 0.7
        })
        return response.choices[0].message.content.trim()
    } catch (err) {
        console.error("Error con OpenAI:", err)
        return "Ha ocurrido un error, inténtalo más tarde."
    }
}

startBot()
