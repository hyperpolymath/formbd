// SPDX-License-Identifier: AGPL-3.0-or-later

/**
 * FormDB Payload CMS Plugin
 *
 * Plugin entry point for Payload CMS
 */

open FormDB_Payload_Types
open FormDB_Payload_Hooks

/** Payload plugin type */
type payloadPlugin = payloadConfig => payloadConfig

/** Create FormDB plugin for Payload */
let formdbPlugin = (pluginConfig: pluginConfig): payloadPlugin => {
  // Initialize the sync state
  if pluginConfig.enabled {
    initialize(pluginConfig)
  }

  // Return config modifier
  (incomingConfig: payloadConfig): payloadConfig => {
    if !pluginConfig.enabled {
      incomingConfig
    } else {
      // In a real implementation, this would modify the config
      // to add hooks to each collection specified in pluginConfig.collections
      //
      // Example:
      // incomingConfig.collections = incomingConfig.collections.map(collection => {
      //   if (shouldSyncCollection(collection.slug)) {
      //     return {
      //       ...collection,
      //       hooks: {
      //         ...collection.hooks,
      //         afterChange: [...(collection.hooks?.afterChange || []), afterChangeHook],
      //         afterDelete: [...(collection.hooks?.afterDelete || []), afterDeleteHook],
      //       }
      //     }
      //   }
      //   return collection
      // })

      incomingConfig
    }
  }
}

/** Default export */
let default = formdbPlugin

/** Helper to create config from environment */
let configFromEnv = (): pluginConfig => {
  {
    formdbUrl: %raw(`process.env.FORMDB_URL || "http://localhost:8080"`),
    apiKey: %raw(`process.env.FORMDB_API_KEY || undefined`),
    collections: [],
    enabled: %raw(`process.env.FORMDB_ENABLED !== "false"`),
  }
}
