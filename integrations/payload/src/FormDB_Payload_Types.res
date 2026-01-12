// SPDX-License-Identifier: AGPL-3.0-or-later

/**
 * FormDB Payload CMS Adapter Types
 *
 * Type definitions for Payload CMS integration
 */

/** Payload hook arguments */
type hookArgs<'doc> = {
  doc: 'doc,
  previousDoc: option<'doc>,
  collection: collectionConfig,
  operation: hookOperation,
  context: hookContext,
}
and collectionConfig = {
  slug: string,
  labels: collectionLabels,
}
and collectionLabels = {
  singular: string,
  plural: string,
}
and hookOperation =
  | Create
  | Update
  | Delete
  | Read
and hookContext = {
  payload: payloadInstance,
}
and payloadInstance = {
  logger: payloadLogger,
  config: payloadConfig,
}
and payloadLogger = {
  info: string => unit,
  warn: string => unit,
  error: string => unit,
}
and payloadConfig = {
  custom: option<customConfig>,
}
and customConfig

/** Plugin configuration */
type pluginConfig = {
  formdbUrl: string,
  apiKey: option<string>,
  collections: array<collectionMapping>,
  enabled: bool,
}
and collectionMapping = {
  payloadSlug: string,
  formdbCollection: string,
  syncMode: syncMode,
  excludeFields: array<string>,
}
and syncMode =
  | Bidirectional
  | PayloadToFormDB
  | FormDBToPayload

/** Hook result */
type hookResult<'doc> = {
  doc: 'doc,
}

/** Sync result */
type syncResult = {
  success: bool,
  collection: string,
  operation: string,
  error: option<string>,
}

/** Parse sync mode */
let parseSyncMode = (str: string): syncMode =>
  switch String.toLowerCase(str) {
  | "bidirectional" => Bidirectional
  | "payload-to-formdb" | "payloadtoformdb" => PayloadToFormDB
  | "formdb-to-payload" | "formdbtopayload" => FormDBToPayload
  | _ => Bidirectional
  }

/** Sync mode to string */
let syncModeToString = (mode: syncMode): string =>
  switch mode {
  | Bidirectional => "bidirectional"
  | PayloadToFormDB => "payload-to-formdb"
  | FormDBToPayload => "formdb-to-payload"
  }

/** Operation to string */
let operationToString = (op: hookOperation): string =>
  switch op {
  | Create => "create"
  | Update => "update"
  | Delete => "delete"
  | Read => "read"
  }
