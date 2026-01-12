// SPDX-License-Identifier: AGPL-3.0-or-later

/**
 * FormDB Strapi Plugin
 *
 * Main plugin entry point for Strapi v4/v5
 */

open FormDB_Strapi_Types
open FormDB_Strapi_Service

/** Plugin register function */
let register = (_ctx: strapiContext): unit => {
  // Plugin registration - called once on startup
  ()
}

/** Plugin bootstrap function */
let bootstrap = (ctx: strapiContext): unit => {
  let strapi = ctx.strapi

  // Load configuration
  let configJson = strapi.config.get("plugin.formdb")

  switch configJson {
  | Some(json) =>
    // Parse config (simplified - in production would use proper JSON decoding)
    let config: pluginConfig = {
      enabled: true,
      formdbUrl: "http://localhost:8080",
      apiKey: None,
      collections: [],
    }

    if config.enabled {
      // Initialize sync service
      initialize(config)
      strapi.log.info("[FormDB] Plugin initialized successfully")

      // Register lifecycle hooks for each mapped collection
      config.collections->Array.forEach(mapping => {
        strapi.log.info(`[FormDB] Registered sync for ${mapping.strapiModel} -> ${mapping.formdbCollection}`)
      })
    } else {
      strapi.log.info("[FormDB] Plugin disabled in configuration")
    }
  | None =>
    strapi.log.warn("[FormDB] No configuration found, plugin disabled")
  }
}

/** Plugin destroy function */
let destroy = (_ctx: strapiContext): unit => {
  // Cleanup on shutdown
  ()
}

/** Export plugin configuration for Strapi */
let default = {
  "register": register,
  "bootstrap": bootstrap,
  "destroy": destroy,
}
