# FormDB CMS Integrations

Official integrations for popular CMS platforms.

## Available Integrations

| CMS | Package | Type | Status |
|-----|---------|------|--------|
| Strapi | `@formdb/strapi-plugin` | Plugin | Stable |
| Directus | `@formdb/directus-extension` | Hook Extension | Stable |
| Ghost | `@formdb/ghost-integration` | Webhook Server | Stable |
| Payload CMS | `@formdb/payload-adapter` | Plugin | Stable |

## Quick Start

### Strapi

```javascript
// config/plugins.js
module.exports = {
  formdb: {
    enabled: true,
    config: {
      formdbUrl: process.env.FORMDB_URL,
      apiKey: process.env.FORMDB_API_KEY,
      collections: [
        { strapiModel: 'article', formdbCollection: 'articles', syncMode: 'bidirectional' },
      ],
    },
  },
};
```

### Directus

```bash
# Set environment variables
FORMDB_URL=http://localhost:8080
FORMDB_API_KEY=your-api-key
FORMDB_SYNC_COLLECTIONS=articles,products
```

### Ghost

```bash
# Run webhook server
deno run --allow-net --allow-env @formdb/ghost-integration

# Configure webhooks in Ghost Admin
# Point events to: http://your-server:3000/webhook
```

### Payload CMS

```typescript
// payload.config.ts
import formdbPlugin from '@formdb/payload-adapter';

export default buildConfig({
  plugins: [
    formdbPlugin({
      formdbUrl: process.env.FORMDB_URL,
      collections: [
        { payloadSlug: 'posts', formdbCollection: 'posts', syncMode: 'bidirectional' },
      ],
    }),
  ],
});
```

## Sync Modes

All integrations support three sync modes:

| Mode | Description |
|------|-------------|
| `bidirectional` | Sync changes both ways |
| `cms-to-formdb` | Only sync CMS changes to FormDB |
| `formdb-to-cms` | Only sync FormDB changes to CMS |

## Features

### Common Features

- **Real-time Sync** - Automatic sync on content changes
- **Selective Sync** - Choose which content types to sync
- **Audit Trail** - Full provenance tracking in FormDB
- **Error Handling** - Graceful failure with logging

### FormDB Benefits

When you sync to FormDB, you get:

- **Narrative History** - Every change has a reason
- **Reversibility** - Undo any change with full context
- **Audit Grade** - Meet compliance requirements
- **Normalization** - Auto-detect schema improvements
- **Multi-Protocol** - Query via REST, gRPC, or GraphQL

## Architecture

```
integrations/
├── README.md            # This file
├── strapi/              # Strapi v4/v5 plugin
│   ├── src/
│   │   ├── FormDB_Strapi_Types.res
│   │   ├── FormDB_Strapi_Client.res
│   │   ├── FormDB_Strapi_Service.res
│   │   ├── FormDB_Strapi_Plugin.res
│   │   └── FormDB_Strapi_Lifecycles.res
│   └── README.md
├── directus/            # Directus hook extension
│   ├── src/
│   │   ├── FormDB_Directus_Types.res
│   │   └── FormDB_Directus_Hook.res
│   └── README.md
├── ghost/               # Ghost webhook server
│   ├── src/
│   │   ├── FormDB_Ghost_Types.res
│   │   ├── FormDB_Ghost_Webhook.res
│   │   └── FormDB_Ghost_Server.res
│   └── README.md
└── payload/             # Payload CMS plugin
    ├── src/
    │   ├── FormDB_Payload_Types.res
    │   ├── FormDB_Payload_Hooks.res
    │   └── FormDB_Payload_Plugin.res
    └── README.md
```

## Environment Variables

| Variable | Used By | Description |
|----------|---------|-------------|
| `FORMDB_URL` | All | FormDB server URL |
| `FORMDB_API_KEY` | All | API key for authentication |
| `FORMDB_SYNC_COLLECTIONS` | Directus | Collections to sync (comma-separated) |
| `GHOST_WEBHOOK_SECRET` | Ghost | Webhook signature secret |
| `FORMDB_ENABLED` | Payload | Enable/disable plugin |

## Provenance Tracking

All integrations add provenance metadata:

```json
{
  "actor": "strapi-plugin",
  "rationale": "Auto-sync from Strapi create event",
  "source": "strapi",
  "model": "article",
  "action": "create",
  "timestamp": "2026-01-12T10:30:00Z"
}
```

## Development

All integrations are written in ReScript and compile to JavaScript:

```bash
# Build all integrations
cd integrations/strapi && npm run build
cd integrations/directus && npm run build
cd integrations/ghost && deno task build
cd integrations/payload && npm run build
```

## Contributing

See the main FormDB repository for contribution guidelines.

## License

AGPL-3.0-or-later
