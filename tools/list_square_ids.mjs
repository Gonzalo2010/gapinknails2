import "dotenv/config"
import { Client, Environment } from "square"

const client = new Client({
  accessToken: process.env.SQUARE_ACCESS_TOKEN,
  environment: (process.env.SQUARE_ENV || "sandbox") === "production"
    ? Environment.Production
    : Environment.Sandbox
})

const locationId = process.env.SQUARE_LOCATION_ID

async function listTeamMembers() {
  try {
    // Busca miembros de equipo de la location (si no hay filtro, saca todos)
    const resp = await client.teamApi.searchTeamMembers({
      query: locationId ? { filter: { locationIds: [locationId] } } : undefined
    })
    const members = resp?.result?.teamMembers || []
    console.log("\n=== TEAM MEMBERS (empleados) ===")
    if (!members.length) {
      console.log("No hay miembros de equipo. Crea uno en Square → Empleados.")
    }
    for (const m of members) {
      console.log(`- ${m.givenName || ""} ${m.familyName || ""}  id=${m.id}  status=${m.status}`)
    }
    return members
  } catch (e) {
    console.error("Error listTeamMembers:", e?.message || e)
    return []
  }
}

async function listAppointmentServices() {
  try {
    // Filtramos por product type APPOINTMENTS_SERVICE (servicios de citas)
    const resp = await client.catalogApi.searchCatalogItems({
      productTypes: ["APPOINTMENTS_SERVICE"]
    })
    const items = resp?.result?.items || []
    console.log("\n=== APPOINTMENT SERVICES (servicios) ===")
    if (!items.length) {
      console.log("No hay servicios. Crea uno en Square → Artículos → Servicios.")
    }

    // Cada 'item' trae sus variaciones (normalmente 1). Imprimimos id + version.
    for (const it of items) {
      const name = it.itemData?.name || "(sin nombre)"
      const variations = it.itemData?.variations || []
      console.log(`\nServicio: ${name}  (item_id=${it.id})`)
      if (!variations.length) {
        console.log("  - (sin variaciones)")
        continue
      }
      for (const v of variations) {
        // a veces la versión no viene; por si acaso, la recuperamos
        let version = v.version
        if (!version) {
          try {
            const r = await client.catalogApi.retrieveCatalogObject(v.id, true)
            version = r?.result?.object?.version
          } catch {}
        }
        console.log(`  - Variation: id=${v.id}  version=${version}  (precio: ${v.itemVariationData?.priceMoney?.amount ?? "-"} ${v.itemVariationData?.priceMoney?.currency ?? ""})`)
      }
    }
    return items
  } catch (e) {
    console.error("Error listAppointmentServices:", e?.message || e)
    return []
  }
}

// Si quieres filtrar por nombre “uñas acrílicas” y sacar IDs directos:
async function findUnasAcrilicas() {
  try {
    const term = "uñas acrílicas"
    const resp = await client.catalogApi.searchCatalogItems({
      textFilter: term,
      productTypes: ["APPOINTMENTS_SERVICE"]
    })
    const items = resp?.result?.items || []
    const first = items[0]
    if (first) {
      const name = first.itemData?.name
      const v = first.itemData?.variations?.[0]
      let version = v?.version
      if (v?.id && !version) {
        const r = await client.catalogApi.retrieveCatalogObject(v.id, true)
        version = r?.result?.object?.version
      }
      console.log(`\n>>> MATCH "${term}"`)
      console.log(`   Servicio: ${name}`)
      console.log(`   service_variation_id: ${v?.id}`)
      console.log(`   service_variation_version: ${version}`)
    } else {
      console.log(`\nNo encontré un servicio que contenga: "${term}".`)
    }
  } catch (e) {
    console.error("Error findUnasAcrilicas:", e?.message || e)
  }
}

;(async () => {
  console.log("Square ENV:", process.env.SQUARE_ENV)
  console.log("Location:", locationId || "(no definida)")
  await listTeamMembers()
  await listAppointmentServices()
  await findUnasAcrilicas()
  console.log("\nListo ✅")
})()
