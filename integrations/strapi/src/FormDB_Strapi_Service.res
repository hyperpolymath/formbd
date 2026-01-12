// SPDX-License-Identifier: AGPL-3.0-or-later

/**
 * FormDB Strapi Service
 *
 * Service layer for FormDB synchronization
 */

open FormDB_Strapi_Types
open FormDB_Strapi_Client

/** Sync service state */
type syncState = {
  mutable client: option<formdbClient>,
  mutable config: option<pluginConfig>,
  mutable mappings: Js.Dict.t<collectionMapping>,
}

/** Global sync state */
let state: syncState = {
  client: None,
  config: None,
  mappings: Js.Dict.empty(),
}

/** Initialize service with config */
let initialize = (config: pluginConfig): unit => {
  state.config = Some(config)
  state.client = Some(fromStrapiConfig(config))

  // Build mappings lookup
  let mappings = Js.Dict.empty()
  config.collections->Array.forEach(mapping => {
    Js.Dict.set(mappings, mapping.strapiModel, mapping)
  })
  state.mappings = mappings
}

/** Get FormDB collection for Strapi model */
let getFormDBCollection = (strapiModel: string): option<string> => {
  switch Js.Dict.get(state.mappings, strapiModel) {
  | Some(mapping) => Some(mapping.formdbCollection)
  | None => None
  }
}

/** Check if model should sync to FormDB */
let shouldSyncToFormDB = (strapiModel: string): bool => {
  switch Js.Dict.get(state.mappings, strapiModel) {
  | Some(mapping) =>
    switch mapping.syncMode {
    | Bidirectional | StrapiToFormDB => true
    | FormDBToStrapi => false
    }
  | None => false
  }
}

/** Check if model should sync from FormDB */
let shouldSyncFromFormDB = (strapiModel: string): bool => {
  switch Js.Dict.get(state.mappings, strapiModel) {
  | Some(mapping) =>
    switch mapping.syncMode {
    | Bidirectional | FormDBToStrapi => true
    | StrapiToFormDB => false
    }
  | None => false
  }
}

/** Sync create event to FormDB */
let syncCreate = async (model: string, data: Js.Json.t): result<queryResult, string> => {
  switch (state.client, getFormDBCollection(model)) {
  | (Some(client), Some(collection)) =>
    if shouldSyncToFormDB(model) {
      try {
        let result = await client.insert(collection, data)
        Ok(result)
      } catch {
      | Js.Exn.Error(e) =>
        let msg = Js.Exn.message(e)->Option.getOr("Unknown error")
        Error(`Failed to sync create to FormDB: ${msg}`)
      }
    } else {
      Error("Sync to FormDB disabled for this model")
    }
  | (None, _) => Error("FormDB client not initialized")
  | (_, None) => Error(`No FormDB collection mapped for ${model}`)
  }
}

/** Sync update event to FormDB */
let syncUpdate = async (model: string, data: Js.Json.t, id: string): result<queryResult, string> => {
  switch (state.client, getFormDBCollection(model)) {
  | (Some(client), Some(collection)) =>
    if shouldSyncToFormDB(model) {
      try {
        let result = await client.update(collection, data, id)
        Ok(result)
      } catch {
      | Js.Exn.Error(e) =>
        let msg = Js.Exn.message(e)->Option.getOr("Unknown error")
        Error(`Failed to sync update to FormDB: ${msg}`)
      }
    } else {
      Error("Sync to FormDB disabled for this model")
    }
  | (None, _) => Error("FormDB client not initialized")
  | (_, None) => Error(`No FormDB collection mapped for ${model}`)
  }
}

/** Sync delete event to FormDB */
let syncDelete = async (model: string, id: string): result<queryResult, string> => {
  switch (state.client, getFormDBCollection(model)) {
  | (Some(client), Some(collection)) =>
    if shouldSyncToFormDB(model) {
      try {
        let result = await client.delete(collection, id)
        Ok(result)
      } catch {
      | Js.Exn.Error(e) =>
        let msg = Js.Exn.message(e)->Option.getOr("Unknown error")
        Error(`Failed to sync delete to FormDB: ${msg}`)
      }
    } else {
      Error("Sync to FormDB disabled for this model")
    }
  | (None, _) => Error("FormDB client not initialized")
  | (_, None) => Error(`No FormDB collection mapped for ${model}`)
  }
}

/** Query FormDB for model data */
let queryFormDB = async (model: string, ~where: option<string>=?, ~limit: option<int>=?): result<array<Js.Json.t>, string> => {
  switch (state.client, getFormDBCollection(model)) {
  | (Some(client), Some(collection)) =>
    try {
      let whereClause = where->Option.map(w => ` WHERE ${w}`)->Option.getOr("")
      let limitClause = limit->Option.map(l => ` LIMIT ${Int.toString(l)}`)->Option.getOr("")
      let fdql = `SELECT * FROM ${collection}${whereClause}${limitClause}`
      let result = await client.query(fdql)
      Ok(result.rows)
    } catch {
    | Js.Exn.Error(e) =>
      let msg = Js.Exn.message(e)->Option.getOr("Unknown error")
      Error(`Failed to query FormDB: ${msg}`)
    }
  | (None, _) => Error("FormDB client not initialized")
  | (_, None) => Error(`No FormDB collection mapped for ${model}`)
  }
}

/** Check FormDB health */
let checkHealth = async (): result<healthResponse, string> => {
  switch state.client {
  | Some(client) =>
    try {
      let health = await client.health()
      Ok(health)
    } catch {
    | Js.Exn.Error(e) =>
      let msg = Js.Exn.message(e)->Option.getOr("Unknown error")
      Error(`FormDB health check failed: ${msg}`)
    }
  | None => Error("FormDB client not initialized")
  }
}
