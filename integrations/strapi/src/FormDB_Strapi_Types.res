// SPDX-License-Identifier: AGPL-3.0-or-later

/**
 * FormDB Strapi Plugin Types
 *
 * Type definitions for Strapi integration
 */

/** Strapi context */
type strapiContext = {
  strapi: strapi,
}
and strapi = {
  plugin: string => pluginApi,
  config: configApi,
  log: logApi,
}
and pluginApi = {
  service: string => serviceApi,
  controller: string => controllerApi,
}
and serviceApi
and controllerApi
and configApi = {
  get: string => option<Js.Json.t>,
}
and logApi = {
  info: string => unit,
  warn: string => unit,
  error: string => unit,
  debug: string => unit,
}

/** Plugin configuration */
type pluginConfig = {
  enabled: bool,
  formdbUrl: string,
  apiKey: option<string>,
  collections: array<collectionMapping>,
}
and collectionMapping = {
  strapiModel: string,
  formdbCollection: string,
  syncMode: syncMode,
}
and syncMode =
  | Bidirectional
  | StrapiToFormDB
  | FormDBToStrapi

/** Sync event */
type syncEvent = {
  model: string,
  action: syncAction,
  data: Js.Json.t,
  timestamp: string,
}
and syncAction =
  | Create
  | Update
  | Delete

/** FormDB client for Strapi */
type formdbClient = {
  query: string => promise<queryResult>,
  insert: (string, Js.Json.t) => promise<queryResult>,
  update: (string, Js.Json.t, string) => promise<queryResult>,
  delete: (string, string) => promise<queryResult>,
  health: unit => promise<healthResponse>,
}
and queryResult = {
  rows: array<Js.Json.t>,
  rowCount: int,
  affectedCount: option<int>,
}
and healthResponse = {
  status: string,
  version: string,
}

/** Parse sync mode from string */
let parseSyncMode = (str: string): syncMode =>
  switch String.toLowerCase(str) {
  | "bidirectional" => Bidirectional
  | "strapi-to-formdb" | "strapitoformdb" => StrapiToFormDB
  | "formdb-to-strapi" | "formdbtostrapi" => FormDBToStrapi
  | _ => Bidirectional
  }

/** Sync mode to string */
let syncModeToString = (mode: syncMode): string =>
  switch mode {
  | Bidirectional => "bidirectional"
  | StrapiToFormDB => "strapi-to-formdb"
  | FormDBToStrapi => "formdb-to-strapi"
  }

/** Sync action to string */
let syncActionToString = (action: syncAction): string =>
  switch action {
  | Create => "create"
  | Update => "update"
  | Delete => "delete"
  }
