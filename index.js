import express from "express"
import baileys from "@whiskeysockets/baileys"
import pino from "pino"
import "dotenv/config"
import OpenAI from "openai"
import fs from "fs"

const { makeWASocket, useMultiFileAuthState } = baileys
const client = new OpenAI({ apiKey: process.env.OPENAI_API_KEY })

// 🚀 Servidor Express para que Railway mantenga vivo el proceso
const app = express()
const PORT = process.env.PORT || 3000

app.get("/", (req, res) => {
    res.send("Gapink Nails WhatsApp Bot está en ejecución ✅")
})

app.listen(PORT, () => {
    console.log(`🌐 Servidor web escuchando en el puerto ${PORT}`)
    startBot()
})

async function startBot() {
    console.log("🚀 Iniciando bot de Gapink Nails...")

    const authFolder = "auth_info"
    if (!fs.existsSync(authFolder) || fs.readdirSync(authFolder).length === 0) {
        console.log("⚠️ No se encontró sesión en auth_info. No se iniciará la conexión.")
        return
    }

    const { state, saveCreds } = await useMultiFileAuthState(authFolder)

    const sock = makeWASocket({
        logger: pino({ level: "silent" }),
        printQRInTerminal: false,
        auth: state
    })

    sock.ev.on("connection.update", (update) => {
        const { connection } = update
        if (connection === "open") {
            console.log("✅ Bot conectado a WhatsApp correctamente usando auth_info.")
        }
        if (connection === "close") {
            console.log("❌ Conexión cerrada. El bot no intentará reconectar en Railway.")
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
