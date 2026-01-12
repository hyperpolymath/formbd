# FormDB Payload CMS Adapter

Payload CMS plugin for synchronizing content with FormDB.

## Features

- **Real-time Sync** - Automatic sync on content changes
- **Selective Collections** - Choose which collections to sync
- **Field Exclusion** - Exclude sensitive fields from sync
- **Bidirectional Support** - Sync both ways or one-way

## Installation

```bash
npm install @formdb/payload-adapter
```

## Configuration

Add to your `payload.config.ts`:

```typescript
import { buildConfig } from 'payload/config';
import formdbPlugin from '@formdb/payload-adapter';

export default buildConfig({
  plugins: [
    formdbPlugin({
      formdbUrl: process.env.FORMDB_URL || 'http://localhost:8080',
      apiKey: process.env.FORMDB_API_KEY,
      enabled: true,
      collections: [
        {
          payloadSlug: 'posts',
          formdbCollection: 'posts',
          syncMode: 'bidirectional',
          excludeFields: ['_status', '__v'],
        },
        {
          payloadSlug: 'pages',
          formdbCollection: 'pages',
          syncMode: 'payload-to-formdb',
          excludeFields: [],
        },
      ],
    }),
  ],
  // ... rest of config
});
```

## Sync Modes

| Mode | Description |
|------|-------------|
| `bidirectional` | Sync changes both ways (default) |
| `payload-to-formdb` | Only sync Payload changes to FormDB |
| `formdb-to-payload` | Only sync FormDB changes to Payload |

## Usage

### Automatic Sync

Once configured, content syncs automatically:

```typescript
// Create a post in Payload
await payload.create({
  collection: 'posts',
  data: {
    title: 'My Post',
    content: 'Post content...',
  },
});
// -> Automatically synced to FormDB 'posts' collection
```

### Field Exclusion

Exclude fields from sync:

```typescript
{
  payloadSlug: 'users',
  formdbCollection: 'users',
  syncMode: 'payload-to-formdb',
  excludeFields: ['password', 'resetPasswordToken', '_verified'],
}
```

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `FORMDB_URL` | FormDB server URL | `http://localhost:8080` |
| `FORMDB_API_KEY` | API key for authentication | None |
| `FORMDB_ENABLED` | Enable/disable plugin | `true` |

## Hooks

The plugin adds these hooks to synced collections:

| Hook | Trigger |
|------|---------|
| `afterChange` | After create or update |
| `afterDelete` | After delete |

## Architecture

```
src/
├── FormDB_Payload_Types.res   # Type definitions
├── FormDB_Payload_Hooks.res   # Collection hooks
└── FormDB_Payload_Plugin.res  # Plugin entry point
```

## Provenance

All synced content includes provenance metadata:

```json
{
  "actor": "payload-adapter",
  "rationale": "Auto-sync from Payload afterChange hook",
  "source": "payload",
  "collection": "posts",
  "operation": "create"
}
```

## Local Fields

Payload's `localized` fields are synced as nested objects:

```json
{
  "id": "123",
  "title": {
    "en": "English Title",
    "de": "German Title"
  }
}
```

## Development

```bash
# Build
npm run build

# Test
npm test
```

## TypeScript Support

Type definitions are included:

```typescript
import type { PluginConfig, CollectionMapping, SyncMode } from '@formdb/payload-adapter';

const config: PluginConfig = {
  formdbUrl: 'http://localhost:8080',
  enabled: true,
  collections: [],
};
```

## License

AGPL-3.0-or-later
