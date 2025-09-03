// index.js — Gapink Nails · v35.3.0
// IA total con flujo de reserva persistente (s.flow="book").
// - Si estamos en reserva, ignoramos intents de ver/editar/cancelar mal detectados.
// - IA con contexto de sesión: entiende “en torremolinos” como sede dentro del booking.
// - Square SOLO consultar availability. No creamos cita en Square.
// - HOLD SQLite 6h por duración de servicio (ENV SQ_DUR_*).
// - DeepSeek-only. Mini web QR. Baileys ESM import dinámico.

import express from "express"
import pino from "pino"
import qrcode from "qrcode"
import qrcodeTerminal from "qrcode-terminal"
import "dotenv/config"
import dayjs from "dayjs"
import utc from "dayjs/plugin/utc.js"
import tz from "dayjs/plugin/timezone.js"
import isoWeek from "dayjs/plugin/isoWeek.js"
import "dayjs/locale/es.js"
import { webcrypto } from "crypto"
import Database from "better-sqlite3"
import { Client, Environment } from "square"

if (!globalThis.crypto) globalThis.crypto = webcrypto
dayjs.extend(utc); dayjs.extend(tz); dayjs.extend(isoWeek); dayjs.locale("es")
const EURO_TZ = "Europe/Madrid"
const nowEU = () => dayjs().tz(EURO_TZ)

// ===== Config
const OPEN = { start: 9, end: 20 }                     // L–V 09:00–20:00
const WORK_DAYS = [1,2,3,4,5]
const SEARCH_WINDOW_DAYS   = Number(process.env.BOT_SEARCH_WINDOW_DAYS || 30)
const EXTENDED_WINDOW_DAYS = Number(process.env.BOT_EXTENDED_WINDOW_DAYS || 60)
const NOW_MIN_OFFSET_MIN   = Number(process.env.BOT_NOW_OFFSET_MIN || 30)
const SHOW_TOP_N           = Number(process.env.SHOW_TOP_N || 5)
const HOLD_HOURS           = Number(process.env.HOLD_HOURS || 6)
const PORT                 = Number(process.env.PORT || 8080)
const BOT_DEBUG            = /^true$/i.test(process.env.BOT_DEBUG || "")

// ===== Square (solo consultar)
const square = new Client({
  accessToken: process.env.SQUARE_ACCESS_TOKEN,
  environment: (process.env.SQUARE_ENV==="production") ? Environment.Production : Environment.Sandbox
})
const LOC_TORRE = (process.env.SQUARE_LOCATION_ID_TORREMOLINOS || "").trim()
const LOC_LUZ   = (process.env.SQUARE_LOCATION_ID_LA_LUZ || "").trim()

// ===== IA DeepSeek (prompts compactos)
const DEEPSEEK_API_KEY = process.env.DEEPSEEK_API_KEY || ""
const DEEPSEEK_MODEL   = process.env.DEEPSEEK_MODEL || "deepseek-chat"
const AI_TIMEOUT_MS    = Number(process.env.AI_TIMEOUT_MS || 12000)
const AI_TOKENS_ROUTER = Number(process.env.AI_TOKENS_ROUTER || 160)
const AI_TOKENS_SHORT  = Number(process.env.AI_TOKENS_SHORT  || 220)
const AI_TOKENS_PICK   = Number(process.env.AI_TOKENS_PICK   || 140)

async function aiChat(system, user, {maxTokens=160, temperature=0.15} = {}){
  if(!DEEPSEEK_API_KEY) return null
  const controller = new AbortController()
  const t = setTimeout(()=>controller.abort(), AI_TIMEOUT_MS)
  try{
    const resp = await fetch("https://api.deepseek.com/chat/completions",{
      method:"POST",
      headers:{ "Content-Type":"application/json", "Authorization":`Bearer ${DEEPSEEK_API_KEY}` },
      body: JSON.stringify({
        model: DEEPSEEK_MODEL, temperature, max_tokens: maxTokens,
        messages:[ { role:"system", content: system }, { role:"user", content: user } ]
      }),
      signal: controller.signal
    })
    clearTimeout(t)
    if(!resp.ok) return null
    const data = await resp.json()
    return data?.choices?.[0]?.message?.content || null
  }catch{ clearTimeout(t); return null }
}
function stripJSON(s){
  if(!s) return null
  s = s.trim().replace(/```json/gi,"```")
  if(s.startsWith("```")) s=s.slice(3)
  if(s.endsWith("```")) s=s.slice(0,-3)
  const i=s.indexOf("{"), j=s.lastIndexOf("}")
  if(i>=0 && j>i) s=s.slice(i,j+1)
  try{ return JSON.parse(s) }catch{ return null }
}

// ===== Utils mínimos
function fmtES(d){ const dias=["domingo","lunes","martes","miércoles","jueves","viernes","sábado"]; const t=dayjs(d).tz(EURO_TZ); return `${dias[t.day()]} ${String(t.date()).padStart(2,"0")}/${String(t.month()+1).padStart(2,"0")} ${String(t.hour()).padStart(2,"0")}:${String(t.minute()).padStart(2,"0")}` }
function insideBusinessHours(d,mins){
  const t=dayjs(d); if(!WORK_DAYS.includes(t.day())) return false
  const end=t.add(mins,"minute"); if(!t.isSame(end,"day")) return false
  const s=t.hour()*60+t.minute(), e=end.hour()*60+end.minute()
  return s >= OPEN.start*60 && e <= OPEN.end*60
}
function nextOpeningFrom(d){
  let t=d.clone()
  const nowMin=t.hour()*60+t.minute(), openMin=OPEN.start*60, closeMin=OPEN.end*60
  if(nowMin<openMin) t=t.hour(OPEN.start).minute(0)
  if(nowMin>=closeMin) t=t.add(1,"day").hour(OPEN.start).minute(0)
  while(!WORK_DAYS.includes(t.day())) t=t.add(1,"day").hour(OPEN.start).minute(0)
  return t
}
function locationToId(key){ return key==="la_luz" ? LOC_LUZ : LOC_TORRE }
function locationNice(key){ return key==="la_luz" ? "Málaga – La Luz" : "Torremolinos" }

// ===== Staff listado para IA (nombre corto)
function parseEmployees(){
  const out=[]
  for(const [k,v] of Object.entries(process.env)){
    if(!k.startsWith("SQ_EMP_")) continue
    const [id, tag] = String(v||"").split("|").map(x=>x?.trim())
    if(!id) continue
    const label = k.replace(/^SQ_EMP_/,"").replace(/_/g," ")
    out.push({ id, label, short: label.split(" ")[0], bookable: (tag||"BOOKABLE").toUpperCase()!=="OFF" })
  }
  return out
}
const EMPLOYEES = parseEmployees()
function staffLabelFromId(id){ return EMPLOYEES.find(e=>e.id===id)?.short || "Equipo" }
const STAFF_SHORT = EMPLOYEES.filter(e=>e.bookable).map(e=>e.short)

// ===== Servicios por sede
function servicesForSede(sedeKey){
  const prefix = (sedeKey==="la_luz") ? "SQ_SVC_luz_" : "SQ_SVC_"
  const out=[]
  const seen = new Set()
  for(const [k,v] of Object.entries(process.env)){
    if(!k.startsWith(prefix)) continue
    const [id,ver] = String(v||"").split("|")
    if(!id) continue
    const label = k.replace(prefix,"").replace(/_/g," ").replace(/\b\w/g, m=>m.toUpperCase()).replace(/\bLuz\b/i,"").trim()
    const key = `${label}::${id}`
    if(seen.has(key)) continue
    seen.add(key)
    out.push({ key:k, id, version: ver?Number(ver):null, label })
  }
  return out
}
function labelToEnvKey(label, sede){
  const hit = servicesForSede(sede).find(s=> s.label.toLowerCase()===String(label||"").toLowerCase())
  return hit?.key || null
}
function labelFromEnvKey(key){
  for(const sede of ["torremolinos","la_luz"]){
    const hit = servicesForSede(sede).find(s=>s.key===key)
    if(hit) return hit.label
  }
  return null
}
async function getServiceIdAndVersion(envKey){
  const raw = process.env[envKey]; if(!raw) return null
  let [id,ver] = String(raw).split("|"); ver=ver?Number(ver):null
  if(!ver){
    try{
      const resp = await square.catalogApi.retrieveCatalogObject(id,true)
      ver = Number(resp?.result?.object?.version || 1)
    }catch{ ver=1 }
  }
  return { id, version: ver }
}
// Duración desde ENV: SQ_SVC_* -> SQ_DUR_* (mismo sufijo)
function durationMinForEnvKey(envKey){
  if(!envKey) return 60
  const durKey = String(envKey).replace(/^SQ_SVC/, "SQ_DUR")
  const v = Number(process.env[durKey] || "")
  return Number.isFinite(v) && v>0 ? v : 60
}

// ===== SQLite: holds (bloqueo 6h)
const db = new Database("gapink_simple.db")
db.pragma("journal_mode = WAL")
db.exec(`
CREATE TABLE IF NOT EXISTS holds (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  phone TEXT,
  sede TEXT,
  env_service_key TEXT,
  start_iso TEXT,
  end_iso TEXT,
  staff_id TEXT,
  created_at TEXT,
  expires_at TEXT,
  status TEXT DEFAULT 'active'
);
CREATE INDEX IF NOT EXISTS idx_holds_lookup ON holds (sede, start_iso, end_iso, status, expires_at);
`)
const insertHold = db.prepare(`INSERT INTO holds (phone,sede,env_service_key,start_iso,end_iso,staff_id,created_at,expires_at,status)
VALUES (@phone,@sede,@env_service_key,@start_iso,@end_iso,@staff_id,@created_at,@expires_at,'active')`)
const findOverlappingHolds = db.prepare(`
SELECT id FROM holds
WHERE status='active'
  AND sede=@sede
  AND expires_at > @now
  AND NOT( end_iso <= @start_iso OR start_iso >= @end_iso )
`)
const expireOldHolds = db.prepare(`UPDATE holds SET status='expired' WHERE status='active' AND expires_at<=@now`)
function cleanupHolds(){ try{ expireOldHolds.run({ now: new Date().toISOString() }) }catch(e){ if(BOT_DEBUG) console.error(e) } }
function hasActiveOverlap({ sede, startISO, endISO }){
  try{
    const row = findOverlappingHolds.get({ sede, start_iso:startISO, end_iso:endISO, now: new Date().toISOString() })
    return !!row
  }catch{ return false }
}
function createHold({ phone, sede, envServiceKey, startISO, endISO, staffId }){
  try{
    const exp = dayjs().add(HOLD_HOURS,"hour").toISOString()
    insertHold.run({
      phone, sede, env_service_key: envServiceKey,
      start_iso:startISO, end_iso:endISO,
      staff_id: staffId || null,
      created_at: new Date().toISOString(),
      expires_at: exp
    })
    return true
  }catch(e){ if(BOT_DEBUG) console.error(e); return false }
}

// ===== IA — Router (con contexto de sesión)
function aiRouterSystem(ctx){
  const now = nowEU().format("YYYY-MM-DD HH:mm")
  return `Devuelve SOLO JSON válido (sin texto extra).
Fecha actual: ${now} Europe/Madrid
Sedes: ["torremolinos","la_luz"]
Staff permitidos (si el cliente pide con alguien): ${JSON.stringify(STAFF_SHORT)}

CONTEXTO:
${JSON.stringify(ctx)}

REGLAS:
- Si el contexto indica flujo de reserva en curso ("flow":"book") o ya se pidió cita, por defecto "intent":"book" salvo que el cliente pida explícitamente ver/editar/cancelar.
- Mensajes cortos como "en torremolinos", "la luz", "con ganna", "viernes tarde" son complementos del booking actual (no cambian a view/edit/cancel/info).
- Si el cliente dice "cita", "reservar", "hora", "agenda", interpreta como "book" salvo contradicción explícita.

Schema:
{
 "intent":"book|view|edit|cancel|info|other",
 "sede":"torremolinos|la_luz|null",
 "staff":"string|null",
 "date":{"type":"day|range|none","day":"YYYY-MM-DD|null","part_of_day":"mañana|tarde|noche|null"}
}

Ejemplos:
"quiero una cita pa cejas" -> {"intent":"book","sede":null,"staff":null,"date":{"type":"none","day":null,"part_of_day":null}}
"en torremolinos" con flow=book -> {"intent":"book","sede":"torremolinos","staff":null,"date":{"type":"none","day":null,"part_of_day":null}}
"la del martes por la mañana" con flow=book -> {"intent":"book","sede":null,"staff":null,"date":{"type":"day","day":"<proximo martes ISO>","part_of_day":"mañana"}}
`
}
async function aiResolve(text, ctx){
  const sys = aiRouterSystem(ctx)
  const out = await aiChat(sys, `Cliente: "${text}"`, {maxTokens: AI_TOKENS_ROUTER})
  return stripJSON(out) || {}
}

// ===== IA — Shortlist (desde la lista COMPLETA)
function aiShortlistSystem(allLabels){
  return `Elige hasta 6 etiquetas EXACTAS de esta lista que encajan con la petición del cliente.
Si el texto es genérico ("cejas", "uñas", etc.), elige las 6 más habituales para ese tema.
Devuelve SOLO JSON: {"labels":["..."]}.
Lista completa: ${allLabels.join(" | ")}`
}
async function aiShortlist(text, sede){
  const labels = servicesForSede(sede).map(s=>s.label)
  const sys = aiShortlistSystem(labels)
  const out = await aiChat(sys, `Texto del cliente: "${text}"`, {maxTokens: AI_TOKENS_SHORT})
  try{
    const obj = stripJSON(out) || {}
    const arr = Array.isArray(obj.labels) ? obj.labels.filter(l=>labels.some(x=>x.toLowerCase()===String(l).toLowerCase())) : []
    return (arr.length?arr:[]).slice(0,6)
  }catch{ return [] }
}

// ===== IA — Elegir UN servicio de la shortlist
async function aiChooseFromShortlist(text, shortlist){
  const sys = `Shortlist (elige UNA etiqueta exacta): ${shortlist.join(" | ")}
Si el cliente dice "cejas" sin más, elige la opción base más común.
Devuelve SOLO JSON: {"pick":"<una etiqueta exacta o null>"}.`
  const out = await aiChat(sys, `Frase del cliente: "${text}"`, {maxTokens: AI_TOKENS_PICK})
  try{
    const obj = stripJSON(out) || {}
    const pick = shortlist.find(l=>l.toLowerCase()===String(obj.pick||"").toLowerCase())
    return pick || null
  }catch{ return null }
}

// ===== IA — Elegir hora desde slots
function aiPickSystem(slots){
  const comp = slots.map(s=>{
    const d=s.date.tz(EURO_TZ)
    return { iso:d.format("YYYY-MM-DDTHH:mm"), dow:["do","lu","ma","mi","ju","vi","sa"][d.day()], ddmm:`${String(d.date()).padStart(2,"0")}/${String(d.month()+1).padStart(2,"0")}`, time:`${String(d.hour()).padStart(2,"0")}:${String(d.minute()).padStart(2,"0")}` }
  })
  return `Elige un "iso" de la lista que encaje con la frase del cliente. Devuelve SOLO JSON {"iso":"YYYY-MM-DDTHH:mm|null"}.
Frases tipo: "la del martes", "la de las 13", "la primera", "la última", "otra tarde".
slots=${JSON.stringify(comp)}`
}
async function aiPick(text, slots){
  if(!slots?.length) return { iso:null }
  const sys = aiPickSystem(slots)
  const out = await aiChat(sys, `Cliente: "${text}"`, {maxTokens: AI_TOKENS_PICK})
  return stripJSON(out) || { iso:null }
}

// ===== Disponibilidad (respeta duración + holds) con fallbacks
async function searchAvail({ sede, envKey, startEU, endEU, part=null }){
  const sv = await getServiceIdAndVersion(envKey); if(!sv) return []
  const durMin = durationMinForEnvKey(envKey)
  const body = {
    query:{ filter:{
      startAtRange:{ startAt: startEU.tz("UTC").toISOString(), endAt: endEU.tz("UTC").toISOString() },
      locationId: locationToId(sede),
      segmentFilters:[{ serviceVariationId: sv.id }]
    } }
  }
  let avail=[]
  try{ const r = await square.bookingsApi.searchAvailability(body); avail=r?.result?.availabilities||[] }catch(e){ if(BOT_DEBUG) console.error(e) }
  cleanupHolds()
  const out=[]
  for(const a of avail){
    const d=dayjs(a.startAt).tz(EURO_TZ)
    const end = d.clone().add(durMin,"minute")
    if(!insideBusinessHours(d,durMin)) continue
    if(part){
      const from = d.clone().hour(part==="mañana"?9:part==="tarde"?15:18).minute(0)
      const to   = d.clone().hour(part==="mañana"?13:part==="tarde"?20:20).minute(0)
      if(!(d.isAfter(from.subtract(1,"minute")) && d.isBefore(to.add(1,"minute")))) continue
    }
    const startISO = d.tz("UTC").toISOString()
    const endISO   = end.tz("UTC").toISOString()
    if (hasActiveOverlap({ sede, startISO, endISO })) continue
    const segs = a.appointmentSegments||a.segments||[]
    out.push({ date:d, end, staffId: segs?.[0]?.teamMemberId || null, durMin })
    if(out.length>=500) break
  }
  return out.sort((x,y)=>x.date.valueOf()-y.date.valueOf())
}

// ===== Mensajes UX
const GREET = `¡Hola! Soy el asistente de Gapink Nails 💅
Dime *salón* (Torremolinos/La Luz) y qué quieres (ej. “cejas con hilo”).`
const BOOKING_SELF = "Para *ver/editar/cancelar* usa el enlace del SMS/email de confirmación ✅"
const bullets = slots => slots.map(s=>`• ${fmtES(s.date)}${s.staffId?` — ${staffLabelFromId(s.staffId)}`:""}`).join("\n")

// ===== Estado en memoria simple
const SESS = new Map()
function getS(phone){
  return SESS.get(phone) || SESS.set(phone,{
    greetedAt:0,
    flow:null,                // "book" | null
    lastPrompt:null,
    sede:null,
    shortlist:[],
    svcKey:null, svcLabel:null,
    prefStaff:null,
    lastSlots:[], prompted:false, lastListAt:0,
    pickedISO:null, pickedDurMin:60,
    snoozeUntil:0
  }).get(phone)
}
function setPrompt(s, p){ s.lastPrompt=p; return s }

// ===== Proponer horas (con fallbacks automáticos si no hay)
async function proposeHours(session, jid, phone, sock, aiDate){
  if(!session.svcKey){
    await sock.sendMessage(jid,{text:`¿Qué *servicio* quieres en ${locationNice(session.sede)}? Dímelo en tus palabras o elige de lo que te proponga.`})
    return
  }
  const base = nextOpeningFrom(nowEU().add(NOW_MIN_OFFSET_MIN,"minute"))
  let start = base.clone()
  let end   = base.clone().add(SEARCH_WINDOW_DAYS, "day")
  let part  = null

  if (aiDate && aiDate.type==="day" && aiDate.day){
    const d = dayjs.tz(aiDate.day + " 09:00", EURO_TZ)
    start = d.clone().hour(OPEN.start).minute(0)
    end   = d.clone().hour(OPEN.end).minute(0)
    part  = aiDate.part_of_day || null
  } else if (aiDate && aiDate.part_of_day){
    part = aiDate.part_of_day
  }

  // 1) Intento principal
  let slots = await searchAvail({ sede:session.sede, envKey:session.svcKey, startEU:start, endEU:end, part })

  // 2) Fallback A: quitar franja
  if(!slots.length && part){
    slots = await searchAvail({ sede:session.sede, envKey:session.svcKey, startEU:start, endEU:end, part:null })
  }

  // 3) Fallback B: ampliar a 60 días, sin franja
  if(!slots.length){
    const start2 = base.clone()
    const end2   = base.clone().add(EXTENDED_WINDOW_DAYS, "day")
    slots = await searchAvail({ sede:session.sede, envKey:session.svcKey, startEU:start2, endEU:end2, part:null })
  }

  if(!slots.length){
    await sock.sendMessage(jid,{text:"Ahora mismo no veo huecos. Dime otra fecha/franja en tus palabras (p. ej. “viernes tarde”) y lo miro de nuevo."})
    return
  }

  const shown = slots.slice(0, SHOW_TOP_N)
  session.lastSlots = shown
  session.prompted  = true
  session.lastListAt = Date.now()

  const header = session.prefStaff ? `Huecos con ${session.prefStaff}:` : "Huecos del equipo:"
  await sock.sendMessage(jid,{text:`${header}\n${bullets(shown)}\n\nDime en texto cuál te viene (“la del martes”, “la de las 13”, “otra tarde”…).`})
}

// ===== Mini web QR
const app = express()
let lastQR = null
let conectado = false

app.get("/", (_req,res)=>{
  res.send(`<!doctype html><meta charset="utf-8"><style>
  :root{color-scheme:light dark}
  body{font-family:system-ui,-apple-system,Segoe UI,Roboto,Inter,Ubuntu;display:grid;place-items:center;min-height:100vh;margin:0;background:Canvas}
  .card{max-width:760px;width:92vw;padding:28px 24px;border-radius:16px;box-shadow:0 10px 30px rgba(0,0,0,.12);background:Canvas;}
  h1{margin:0 0 4px;font-size:24px}
  .row{display:flex;gap:8px;align-items:center;margin:8px 0}
  .pill{padding:6px 10px;border-radius:999px;font-weight:600;font-size:12px}
  .ok{background:#d1fae5;color:#065f46}
  .bad{background:#fee2e2;color:#991b1b}
  .warn{background:#fef3c7;color:#92400e}
  .qr{margin:16px 0;text-align:center}
  footer{opacity:.6;font-size:12px;margin-top:12px}
  </style>
  <div class="card">
    <h1>Gapink Nails · Bot</h1>
    <div class="row">
      <span class="pill ${conectado?'ok':'bad'}">${conectado?'✅ WhatsApp conectado':'❌ WhatsApp desconectado'}</span>
      <span class="pill warn">Modo: consulta • IA: DeepSeek</span>
    </div>
    ${!conectado && lastQR ? `<div class="qr"><img src="/qr.png" width="280" alt="QR WhatsApp" style="border-radius:12px"/></div>` : `<p style="opacity:.8;margin:12px 0">Escanea el QR en terminal o espera reconexión automática.</p>`}
    <footer>Puerto ${PORT} · ${new Date().toLocaleString("es-ES",{ timeZone: "Europe/Madrid" })}</footer>
  </div>`)
})
app.get("/qr.png", async (_req,res)=>{
  if(!lastQR) return res.status(404).send("No QR")
  try{
    const png = await qrcode.toBuffer(lastQR, { type:"png", width:512, margin:1 })
    res.set("Content-Type","image/png").send(png)
  }catch{ res.status(500).send("QR error") }
})

// ===== Baileys (ESM dynamic import)
async function loadBaileys(){
  const mod = await import("@whiskeysockets/baileys")
  const makeWASocket = mod.makeWASocket ?? mod.default?.makeWASocket ?? mod.default
  const useMultiFileAuthState = mod.useMultiFileAuthState ?? mod.default?.useMultiFileAuthState
  const fetchLatestBaileysVersion = mod.fetchLatestBaileysVersion ?? mod.default?.fetchLatestBaileysVersion
  const Browsers = mod.Browsers ?? mod.default?.Browsers ?? { macOS:(n="Desktop")=>["MacOS",n,"121.0.0"] }
  if (!makeWASocket || !useMultiFileAuthState) throw new Error("Baileys ESM no expone funciones esperadas")
  return { makeWASocket, useMultiFileAuthState, fetchLatestBaileysVersion, Browsers }
}

// ===== Bot loop
async function startBot(){
  const { makeWASocket, useMultiFileAuthState, fetchLatestBaileysVersion, Browsers } = await loadBaileys()
  const { state, saveCreds } = await useMultiFileAuthState("auth_info")
  const { version } = await fetchLatestBaileysVersion().catch(()=>({version:[2,3000,0]}))
  const sock = makeWASocket({ logger:pino({level:"silent"}), printQRInTerminal:false, auth:state, version, browser:Browsers.macOS("Desktop"), syncFullHistory:false })

  sock.ev.on("creds.update", saveCreds)
  sock.ev.on("connection.update", ({connection, qr})=>{
    if (qr){ lastQR = qr; conectado = false; try{ qrcodeTerminal.generate(qr,{small:true}) }catch{} }
    if (connection==="open"){ lastQR=null; conectado=true }
    if (connection==="close"){ conectado=false; setTimeout(()=>startBot().catch(console.error), 2500) }
  })

  sock.ev.on("messages.upsert", async ({messages})=>{
    const m = messages?.[0]; if(!m?.message) return
    const jid = m.key.remoteJid
    const isFromMe = !!m.key.fromMe
    const phone = (jid||"").split("@")[0]
    const textRaw = (m.message.conversation || m.message.extendedTextMessage?.text || m.message?.imageMessage?.caption || "").trim()
    if(!textRaw) return

    // Puntitos => silencio 6h (cliente y tú); si lo mandas tú, no responder.
    if(/^[\s.·•⋅]+$/.test(textRaw)){
      const s=getS(phone); s.snoozeUntil=nowEU().add(6,"hour").valueOf(); SESS.set(phone,s)
      if(!isFromMe) return
      return
    }
    if(isFromMe) return

    const s = getS(phone)
    if(s.snoozeUntil && Date.now()<s.snoozeUntil) return

    // Saludo 24h
    if(Date.now()-s.greetedAt > 24*60*60*1000){
      s.greetedAt = Date.now(); SESS.set(phone,s)
      await sock.sendMessage(jid,{text:GREET})
      setPrompt(s,"greet"); SESS.set(phone,s)
    }

    // Si ya mostramos horas y aún no hay elección -> IA decide slot y bloquea
    if(s.lastSlots?.length && !s.pickedISO){
      const picked = await aiPick(textRaw, s.lastSlots)
      const iso = picked?.iso || null
      if(iso){
        const hit = s.lastSlots.find(x=>x.date.format("YYYY-MM-DDTHH:mm")===iso)
        if(hit){
          const durMin = hit.durMin || durationMinForEnvKey(s.svcKey)
          const startISO = hit.date.tz("UTC").toISOString()
          const endISO   = hit.date.clone().add(durMin,"minute").tz("UTC").toISOString()
          cleanupHolds()
          if (hasActiveOverlap({ sede:s.sede, startISO, endISO })){
            await sock.sendMessage(jid,{text:"Ese hueco se acaba de bloquear por otra conversación. Te paso alternativas:"})
            await proposeHours(s, jid, phone, sock, { type:"none", day:null, part_of_day:null })
            return
          }
          const ok = createHold({ phone, sede:s.sede, envServiceKey:s.svcKey, startISO, endISO, staffId: hit.staffId||null })
          if(!ok){
            await sock.sendMessage(jid,{text:"No he podido bloquear ese hueco. Te paso alternativas:"})
            await proposeHours(s, jid, phone, sock, { type:"none", day:null, part_of_day:null })
            return
          }
          s.pickedISO = iso
          s.pickedDurMin = durMin
          SESS.set(phone,s)
        }
      }
    }

    // ===== IA Router con CONTEXTO
    const ctx = {
      flow: s.flow,               // "book" | null
      sede: s.sede,
      haveShortlist: !!s.shortlist?.length,
      haveService: !!s.svcKey,
      awaiting: s.lastPrompt      // greet | ask_sede | ask_service | hours | summary | fallback
    }
    const ai = await aiResolve(textRaw, ctx)

    // 1) Primero, procesa sede/staff/fecha (para no romper el flujo)
    if(ai.sede==="la_luz" || ai.sede==="torremolinos"){
      s.sede = ai.sede
    }
    if(ai?.staff){ s.prefStaff = ai.staff }
    // si ya preguntamos algo de la reserva, marcamos flujo book
    if(!s.flow && (s.lastPrompt==="ask_sede" || s.lastPrompt==="ask_service" || s.lastPrompt==="hours")) s.flow = "book"
    // si IA cree que es booking, activamos el flujo
    if(ai.intent==="book") s.flow = "book"
    SESS.set(phone,s)

    // 2) Solo si NO estamos en flujo de reserva, atendemos view/edit/cancel/info
    if(s.flow!=="book" && (ai.intent==="view" || ai.intent==="edit" || ai.intent==="cancel" || ai.intent==="info")){
      await sock.sendMessage(jid,{text:BOOKING_SELF})
      if(ai.intent==="view"){ s.snoozeUntil = nowEU().add(6,"hour").valueOf(); SESS.set(phone,s) }
      return
    }

    // 3) Si falta sede, pídela una vez
    if(!s.sede){
      if(s.lastPrompt!=="ask_sede"){
        s.flow = "book" // arranca reserva
        await sock.sendMessage(jid,{text:"¿Salón? *Torremolinos* o *La Luz*."})
        setPrompt(s,"ask_sede"); SESS.set(phone,s)
      }
      return
    }

    // 4) Servicio: shortlist IA -> pick IA
    if(!s.svcKey && s.shortlist.length){
      const pick = await aiChooseFromShortlist(textRaw, s.shortlist)
      if(pick){
        const key = labelToEnvKey(pick, s.sede)
        if(key){ s.svcKey=key; s.svcLabel=labelFromEnvKey(key) }
      }
      if(!s.svcKey){
        await sock.sendMessage(jid,{text:`Elige una de las opciones de arriba en tus palabras (vale “la primera” o el nombre).`})
        return
      }
      await proposeHours(s, jid, phone, sock, ai?.date||{type:"none",day:null,part_of_day:null})
      s.flow="book"; setPrompt(s,"hours"); SESS.set(phone,s)
      return
    }

    if(!s.svcKey){
      const labels = servicesForSede(s.sede).map(x=>x.label)
      let list = await aiShortlist(textRaw, s.sede)
      if(!list.length){
        const sys = `De esta lista completa de servicios, propone hasta 6 que encajen con el tema del mensaje. Devuelve SOLO JSON {"labels":["..."]}. Lista: ${labels.join(" | ")}`
        const out2 = await aiChat(sys, `Mensaje: "${textRaw}"`, {maxTokens: AI_TOKENS_SHORT})
        try{
          const obj2 = stripJSON(out2) || {}
          const arr2 = Array.isArray(obj2.labels) ? obj2.labels.filter(l=>labels.some(x=>x.toLowerCase()===String(l).toLowerCase())) : []
          list = (arr2.length?arr2:[]).slice(0,6)
        }catch{}
      }
      s.shortlist = list
      if(list.length){
        const autoPick = await aiChooseFromShortlist(textRaw, list)
        if(autoPick){
          const key = labelToEnvKey(autoPick, s.sede)
          if(key){ s.svcKey=key; s.svcLabel=labelFromEnvKey(key) }
        }
        if(!s.svcKey){
          const enriched = list.map(l=>{
            const env = labelToEnvKey(l, s.sede)
            const dur = durationMinForEnvKey(env)
            return `• ${l} — ${dur} min`
          }).join("\n")
          await sock.sendMessage(jid,{text:`En ${locationNice(s.sede)} te encaja algo de esto (respóndeme en texto, p. ej. “la primera” o el nombre):\n${enriched}`})
          s.flow="book"; setPrompt(s,"ask_service"); SESS.set(phone,s)
          return
        }
      }else{
        s.flow="book"
        await sock.sendMessage(jid,{text:`Dímelo con un poco más de detalle (ej. “cejas con hilo”, “laminación de cejas”, “microblading”).`})
        return
      }
    }

    // 5) Proponer horas si no lo hemos hecho recientemente
    if(s.svcKey && (!s.prompted || (Date.now()-s.lastListAt>2*60*1000))){
      await proposeHours(s, jid, phone, sock, ai?.date||{type:"none",day:null,part_of_day:null})
      s.flow="book"; setPrompt(s,"hours"); SESS.set(phone,s)
      return
    }

    // 6) Resumen si ya hay hold
    if(s.pickedISO){
      const picked = dayjs.tz(s.pickedISO, EURO_TZ)
      const summary = `Resumen:\n• Salón: ${locationNice(s.sede)}\n• Servicio: ${s.svcLabel}\n• Profesional: ${s.prefStaff||"Equipo"}\n• Hora: ${fmtES(picked)}\n• Duración: ${s.pickedDurMin} min\n\nAhora una de las compañeras da el OK ✅`
      await sock.sendMessage(jid,{text:summary})
      // reset suave
      s.lastSlots=[]; s.prompted=false; s.pickedISO=null
      setPrompt(s,"summary"); SESS.set(phone,s)
      return
    }

    // 7) Fallback mínimo (manteniendo flujo)
    if(s.lastPrompt!=="fallback"){
      s.flow="book"
      await sock.sendMessage(jid,{text:GREET})
      setPrompt(s,"fallback"); SESS.set(phone,s)
    }
  })
}

// ===== Arranque
const server = app.listen(PORT, ()=>{ 
  console.log(`🩷 Gapink Nails Bot v35.3.0 — DeepSeek-only · IA total · QR http://localhost:${PORT}`)
  startBot().catch(console.error)
})
process.on("SIGTERM", ()=>{ try{ server.close(()=>process.exit(0)) }catch{ process.exit(0) } })
process.on("SIGINT", ()=>{ try{ server.close(()=>process.exit(0)) }catch{ process.exit(0) } })
