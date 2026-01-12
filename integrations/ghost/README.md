# FormDB Ghost Integration

Webhook service for synchronizing Ghost CMS content with FormDB.

## Features

- **Real-time Sync** - Instant sync via Ghost webhooks
- **Content Types** - Posts, pages, and members
- **Signature Verification** - Secure webhook validation
- **Deno Runtime** - Fast, secure webhook server

## Installation

```bash
# Clone or install
deno install -A -n formdb-ghost jsr:@formdb/ghost-integration

# Or run directly
deno run --allow-net --allow-env jsr:@formdb/ghost-integration
```

## Configuration

### Environment Variables

```bash
# Required
FORMDB_URL=http://localhost:8080

# Optional
FORMDB_API_KEY=your-api-key
GHOST_WEBHOOK_SECRET=your-webhook-secret
PORT=3000
```

### Ghost Admin Setup

1. Go to Ghost Admin → Settings → Integrations
2. Create new Custom Integration
3. Add webhooks for events you want to sync:
   - `post.published` → `http://your-server:3000/webhook`
   - `post.updated` → `http://your-server:3000/webhook`
   - `post.deleted` → `http://your-server:3000/webhook`
   - `page.published` → `http://your-server:3000/webhook`
   - `page.updated` → `http://your-server:3000/webhook`
   - `page.deleted` → `http://your-server:3000/webhook`

## Supported Events

| Event | FormDB Action |
|-------|---------------|
| `post.published` | Insert/Update to `ghost_posts` |
| `post.updated` | Update in `ghost_posts` |
| `post.deleted` | Delete from `ghost_posts` |
| `post.scheduled` | Insert to `ghost_posts` |
| `page.published` | Insert/Update to `ghost_pages` |
| `page.updated` | Update in `ghost_pages` |
| `page.deleted` | Delete from `ghost_pages` |
| `member.created` | Insert to `ghost_members` |
| `member.updated` | Update in `ghost_members` |
| `member.deleted` | Delete from `ghost_members` |

## Usage

### Start Server

```bash
# Development
deno task dev

# Production
deno task start
```

### Health Check

```bash
curl http://localhost:3000/health
# {"status":"ok","service":"formdb-ghost"}
```

### Webhook Endpoint

```bash
# Webhook URL for Ghost
http://your-server:3000/webhook
```

## FormDB Collections

Content is synced to these FormDB collections:

| Ghost Type | FormDB Collection |
|------------|-------------------|
| Posts | `ghost_posts` |
| Pages | `ghost_pages` |
| Members | `ghost_members` |

### Post Schema

```json
{
  "id": "string",
  "uuid": "string",
  "title": "string",
  "slug": "string",
  "html": "string",
  "plaintext": "string",
  "status": "string",
  "visibility": "string",
  "featured": "boolean",
  "featureImage": "string",
  "publishedAt": "string",
  "createdAt": "string",
  "updatedAt": "string"
}
```

## Architecture

```
src/
├── FormDB_Ghost_Types.res    # Type definitions
├── FormDB_Ghost_Webhook.res  # Webhook handler logic
└── FormDB_Ghost_Server.res   # HTTP server
```

## Docker Deployment

```dockerfile
FROM denoland/deno:latest
WORKDIR /app
COPY . .
RUN deno cache src/FormDB_Ghost_Server.res.js
CMD ["run", "--allow-net", "--allow-env", "src/FormDB_Ghost_Server.res.js"]
```

```bash
docker build -t formdb-ghost .
docker run -p 3000:3000 \
  -e FORMDB_URL=http://formdb:8080 \
  -e GHOST_WEBHOOK_SECRET=secret \
  formdb-ghost
```

## Security

### Webhook Signature

Ghost signs webhooks with HMAC-SHA256. Configure `GHOST_WEBHOOK_SECRET` to enable verification.

### Network

- Only expose webhook endpoint to Ghost server
- Use HTTPS in production
- Validate event types before processing

## License

AGPL-3.0-or-later
