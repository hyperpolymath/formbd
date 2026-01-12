# FormDB Strapi Plugin

Strapi plugin for synchronizing content with FormDB.

## Features

- **Bidirectional Sync** - Sync content between Strapi and FormDB
- **Selective Sync** - Choose which content types to sync
- **Lifecycle Hooks** - Automatic sync on create, update, delete
- **Audit Trail** - Full provenance tracking via FormDB

## Installation

```bash
# In your Strapi project
npm install @formdb/strapi-plugin
```

## Configuration

Add to `config/plugins.js`:

```javascript
module.exports = {
  formdb: {
    enabled: true,
    config: {
      formdbUrl: process.env.FORMDB_URL || 'http://localhost:8080',
      apiKey: process.env.FORMDB_API_KEY,
      collections: [
        {
          strapiModel: 'article',
          formdbCollection: 'articles',
          syncMode: 'bidirectional', // or 'strapi-to-formdb', 'formdb-to-strapi'
        },
        {
          strapiModel: 'author',
          formdbCollection: 'authors',
          syncMode: 'strapi-to-formdb',
        },
      ],
    },
  },
};
```

## Sync Modes

| Mode | Description |
|------|-------------|
| `bidirectional` | Sync changes both ways (default) |
| `strapi-to-formdb` | Only sync Strapi changes to FormDB |
| `formdb-to-strapi` | Only sync FormDB changes to Strapi |

## Usage

### Automatic Sync

Once configured, the plugin automatically syncs content:

```javascript
// When you create an article in Strapi
const article = await strapi.entityService.create('api::article.article', {
  data: {
    title: 'My Article',
    content: 'Article content...',
  },
});
// -> Automatically synced to FormDB 'articles' collection
```

### Manual Queries

Access FormDB directly through the plugin service:

```javascript
// In a controller or service
const formdbService = strapi.plugin('formdb').service('sync');

// Query FormDB
const articles = await formdbService.queryFormDB('article', {
  where: 'status = "published"',
  limit: 10,
});

// Check FormDB health
const health = await formdbService.checkHealth();
```

### Lifecycle Hooks

The plugin registers these lifecycle hooks automatically:

- `afterCreate` - Sync new content to FormDB
- `afterUpdate` - Sync updated content to FormDB
- `afterDelete` - Remove content from FormDB

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `FORMDB_URL` | FormDB server URL | `http://localhost:8080` |
| `FORMDB_API_KEY` | API key for authentication | None |

## API Endpoints

The plugin adds these admin API endpoints:

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/formdb/health` | GET | Check FormDB connection |
| `/formdb/sync/:model` | POST | Trigger manual sync for model |
| `/formdb/query/:model` | GET | Query FormDB collection |

## Provenance

All synced content includes provenance metadata:

```json
{
  "actor": "strapi-plugin",
  "rationale": "Auto-sync from Strapi create event",
  "source": "strapi",
  "model": "article",
  "action": "create"
}
```

## Development

```bash
# Build plugin
npm run build

# Run tests
npm test

# Watch mode
npm run dev
```

## Architecture

```
src/
├── FormDB_Strapi_Types.res      # Type definitions
├── FormDB_Strapi_Client.res     # HTTP client for FormDB
├── FormDB_Strapi_Service.res    # Sync service logic
├── FormDB_Strapi_Plugin.res     # Main plugin entry
└── FormDB_Strapi_Lifecycles.res # Strapi lifecycle hooks
```

## License

AGPL-3.0-or-later
