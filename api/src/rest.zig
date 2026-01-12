// SPDX-License-Identifier: AGPL-3.0-or-later
// FormDB API Server - REST Handler

const std = @import("std");
const json = std.json;

const config = @import("config.zig");
const auth = @import("auth.zig");
const metrics = @import("metrics.zig");

const log = std.log.scoped(.rest);

pub fn handleRequest(
    allocator: std.mem.Allocator,
    request: *std.http.Server.Request,
    cfg: *const config.Config,
) !void {
    // Authentication check
    if (cfg.require_auth) {
        if (!try auth.validateRequest(request)) {
            try sendUnauthorized(request);
            return;
        }
    }

    const path = request.head.target;
    const method = request.head.method;

    // Strip /v1/ prefix
    const endpoint = if (std.mem.startsWith(u8, path, "/v1/"))
        path[4..]
    else
        path;

    // Route to handler
    if (std.mem.eql(u8, endpoint, "/query") or std.mem.eql(u8, endpoint, "/query/")) {
        try handleQuery(allocator, request, method);
    } else if (std.mem.startsWith(u8, endpoint, "/collections")) {
        try handleCollections(allocator, request, method, endpoint);
    } else if (std.mem.startsWith(u8, endpoint, "/journal")) {
        try handleJournal(allocator, request, method);
    } else if (std.mem.startsWith(u8, endpoint, "/normalize")) {
        try handleNormalize(allocator, request, method, endpoint);
    } else if (std.mem.startsWith(u8, endpoint, "/migrate")) {
        try handleMigrate(allocator, request, method, endpoint);
    } else if (std.mem.eql(u8, endpoint, "/health") or std.mem.eql(u8, endpoint, "/health/")) {
        try handleHealth(request);
    } else if (std.mem.eql(u8, endpoint, "/metrics") or std.mem.eql(u8, endpoint, "/metrics/")) {
        try handleMetrics(allocator, request);
    } else {
        try sendNotFound(request);
    }
}

// =============================================================================
// Query Handler
// =============================================================================

fn handleQuery(
    allocator: std.mem.Allocator,
    request: *std.http.Server.Request,
    method: std.http.Method,
) !void {
    if (method != .POST) {
        try sendMethodNotAllowed(request);
        return;
    }

    // Read request body
    var body_reader = try request.reader();
    const body = try body_reader.readAllAlloc(allocator, 10 * 1024 * 1024);
    defer allocator.free(body);

    // Parse JSON request
    const parsed = json.parseFromSlice(QueryRequest, allocator, body, .{}) catch {
        try sendBadRequest(request, "Invalid JSON in request body");
        return;
    };
    defer parsed.deinit();

    const req = parsed.value;

    log.info("Executing FDQL: {s}", .{req.fdql});

    // TODO: Connect to Form.Bridge for actual execution
    // For now, return mock response

    const response = if (req.explain)
        \\{
        \\  "plan": {
        \\    "steps": [
        \\      {"type": "scan", "collection": "articles"},
        \\      {"type": "filter", "expression": "status = 'published'"},
        \\      {"type": "limit", "count": 10}
        \\    ],
        \\    "estimatedCost": 150.0,
        \\    "rationale": "Full scan with filter (no index on status)"
        \\  },
        \\  "timing": {
        \\    "parseMs": 0.5,
        \\    "planMs": 1.2,
        \\    "executeMs": 0.0,
        \\    "totalMs": 1.7
        \\  }
        \\}
    else
        \\{
        \\  "rows": [],
        \\  "rowCount": 0,
        \\  "journalSeq": 42,
        \\  "timing": {
        \\    "parseMs": 0.5,
        \\    "planMs": 1.2,
        \\    "executeMs": 3.8,
        \\    "totalMs": 5.5
        \\  }
        \\}
    ;

    try sendJson(request, .ok, response);
}

const QueryRequest = struct {
    fdql: []const u8,
    provenance: ?Provenance = null,
    explain: bool = false,
    analyze: bool = false,
    verbose: bool = false,
};

const Provenance = struct {
    actor: []const u8,
    rationale: []const u8,
};

// =============================================================================
// Collections Handler
// =============================================================================

fn handleCollections(
    allocator: std.mem.Allocator,
    request: *std.http.Server.Request,
    method: std.http.Method,
    endpoint: []const u8,
) !void {
    _ = allocator;

    // Check if it's a specific collection
    const collection_name = extractCollectionName(endpoint);

    if (collection_name) |name| {
        switch (method) {
            .GET => try handleGetCollection(request, name),
            .DELETE => try handleDropCollection(request, name),
            else => try sendMethodNotAllowed(request),
        }
    } else {
        switch (method) {
            .GET => try handleListCollections(request),
            .POST => try handleCreateCollection(request),
            else => try sendMethodNotAllowed(request),
        }
    }
}

fn extractCollectionName(endpoint: []const u8) ?[]const u8 {
    // /collections/name -> name
    const prefix = "/collections/";
    if (std.mem.startsWith(u8, endpoint, prefix) and endpoint.len > prefix.len) {
        return endpoint[prefix.len..];
    }
    return null;
}

fn handleListCollections(request: *std.http.Server.Request) !void {
    // TODO: Connect to Form.Bridge
    const response =
        \\{
        \\  "collections": [
        \\    {
        \\      "name": "articles",
        \\      "type": "document",
        \\      "documentCount": 1234,
        \\      "normalForm": "3NF"
        \\    },
        \\    {
        \\      "name": "users",
        \\      "type": "document",
        \\      "documentCount": 567,
        \\      "normalForm": "BCNF"
        \\    }
        \\  ],
        \\  "total": 2
        \\}
    ;
    try sendJson(request, .ok, response);
}

fn handleGetCollection(request: *std.http.Server.Request, name: []const u8) !void {
    _ = name;
    // TODO: Connect to Form.Bridge
    const response =
        \\{
        \\  "name": "articles",
        \\  "type": "document",
        \\  "schema": {
        \\    "fields": [
        \\      {"name": "_id", "type": "string", "nullable": false},
        \\      {"name": "title", "type": "string", "nullable": false},
        \\      {"name": "status", "type": "string", "nullable": true}
        \\    ],
        \\    "constraints": [
        \\      {"type": "primary_key", "fields": ["_id"]}
        \\    ]
        \\  },
        \\  "documentCount": 1234,
        \\  "normalForm": "3NF"
        \\}
    ;
    try sendJson(request, .ok, response);
}

fn handleCreateCollection(request: *std.http.Server.Request) !void {
    // TODO: Connect to Form.Bridge
    const response =
        \\{
        \\  "name": "new_collection",
        \\  "type": "document",
        \\  "documentCount": 0,
        \\  "normalForm": "unknown"
        \\}
    ;
    try sendJson(request, .created, response);
}

fn handleDropCollection(request: *std.http.Server.Request, name: []const u8) !void {
    _ = name;
    // TODO: Connect to Form.Bridge
    request.respond("", .{
        .status = .no_content,
    }) catch {};
}

// =============================================================================
// Journal Handler
// =============================================================================

fn handleJournal(
    allocator: std.mem.Allocator,
    request: *std.http.Server.Request,
    method: std.http.Method,
) !void {
    _ = allocator;

    if (method != .GET) {
        try sendMethodNotAllowed(request);
        return;
    }

    // TODO: Parse query params and connect to Form.Bridge
    const response =
        \\{
        \\  "entries": [
        \\    {
        \\      "seq": 42,
        \\      "timestamp": "2026-01-12T10:30:00Z",
        \\      "operation": "insert",
        \\      "collection": "articles",
        \\      "documentId": "doc-123",
        \\      "after": {"title": "Hello World", "status": "draft"},
        \\      "provenance": {
        \\        "actor": "editor@news.org",
        \\        "rationale": "New article creation"
        \\      },
        \\      "inverse": "DELETE FROM articles WHERE _id = 'doc-123'"
        \\    }
        \\  ],
        \\  "hasMore": false,
        \\  "nextSeq": 43
        \\}
    ;
    try sendJson(request, .ok, response);
}

// =============================================================================
// Normalize Handler
// =============================================================================

fn handleNormalize(
    allocator: std.mem.Allocator,
    request: *std.http.Server.Request,
    method: std.http.Method,
    endpoint: []const u8,
) !void {
    _ = allocator;

    if (method != .POST) {
        try sendMethodNotAllowed(request);
        return;
    }

    if (std.mem.indexOf(u8, endpoint, "/discover")) |_| {
        try handleDiscover(request);
    } else if (std.mem.indexOf(u8, endpoint, "/analyze")) |_| {
        try handleAnalyze(request);
    } else {
        try sendNotFound(request);
    }
}

fn handleDiscover(request: *std.http.Server.Request) !void {
    // TODO: Connect to Form.Normalizer
    const response =
        \\{
        \\  "collection": "orders",
        \\  "functionalDependencies": [
        \\    {
        \\      "determinant": ["order_id"],
        \\      "dependent": "customer_id",
        \\      "confidence": 1.0,
        \\      "tier": "high"
        \\    },
        \\    {
        \\      "determinant": ["customer_id"],
        \\      "dependent": "customer_name",
        \\      "confidence": 0.98,
        \\      "tier": "high"
        \\    }
        \\  ],
        \\  "candidateKeys": [["order_id"]]
        \\}
    ;
    try sendJson(request, .ok, response);
}

fn handleAnalyze(request: *std.http.Server.Request) !void {
    // TODO: Connect to Form.Normalizer
    const response =
        \\{
        \\  "collection": "orders",
        \\  "currentForm": "2NF",
        \\  "violations": [
        \\    {
        \\      "type": "transitive_dependency",
        \\      "description": "customer_name depends on customer_id, not order_id",
        \\      "affectedFields": ["customer_id", "customer_name"]
        \\    }
        \\  ],
        \\  "recommendations": [
        \\    {
        \\      "action": "decompose",
        \\      "description": "Extract customer_name into customers table",
        \\      "targetForm": "3NF",
        \\      "migrationSteps": [
        \\        "CREATE customers (customer_id, customer_name)",
        \\        "INSERT INTO customers SELECT DISTINCT customer_id, customer_name FROM orders",
        \\        "ALTER orders DROP customer_name"
        \\      ]
        \\    }
        \\  ]
        \\}
    ;
    try sendJson(request, .ok, response);
}

// =============================================================================
// Migrate Handler
// =============================================================================

fn handleMigrate(
    allocator: std.mem.Allocator,
    request: *std.http.Server.Request,
    method: std.http.Method,
    endpoint: []const u8,
) !void {
    _ = allocator;

    if (method != .POST) {
        try sendMethodNotAllowed(request);
        return;
    }

    if (std.mem.indexOf(u8, endpoint, "/start")) |_| {
        try handleMigrationStart(request);
    } else if (std.mem.indexOf(u8, endpoint, "/shadow")) |_| {
        try handleMigrationShadow(request);
    } else if (std.mem.indexOf(u8, endpoint, "/commit")) |_| {
        try handleMigrationCommit(request);
    } else if (std.mem.indexOf(u8, endpoint, "/abort")) |_| {
        try handleMigrationAbort(request);
    } else {
        try sendNotFound(request);
    }
}

fn handleMigrationStart(request: *std.http.Server.Request) !void {
    const response =
        \\{
        \\  "id": "mig-001",
        \\  "collection": "orders",
        \\  "phase": "announce",
        \\  "startedAt": "2026-01-12T10:30:00Z",
        \\  "narrative": "Migration announced: Decomposing orders to achieve 3NF by extracting customer_name"
        \\}
    ;
    try sendJson(request, .ok, response);
}

fn handleMigrationShadow(request: *std.http.Server.Request) !void {
    const response =
        \\{
        \\  "id": "mig-001",
        \\  "collection": "orders",
        \\  "phase": "shadow",
        \\  "startedAt": "2026-01-12T10:30:00Z",
        \\  "narrative": "Shadow phase: Dual-writing to old and new schemas"
        \\}
    ;
    try sendJson(request, .ok, response);
}

fn handleMigrationCommit(request: *std.http.Server.Request) !void {
    const response =
        \\{
        \\  "id": "mig-001",
        \\  "collection": "orders",
        \\  "phase": "complete",
        \\  "startedAt": "2026-01-12T10:30:00Z",
        \\  "narrative": "Migration complete: orders is now in 3NF"
        \\}
    ;
    try sendJson(request, .ok, response);
}

fn handleMigrationAbort(request: *std.http.Server.Request) !void {
    const response =
        \\{
        \\  "id": "mig-001",
        \\  "collection": "orders",
        \\  "phase": "aborted",
        \\  "startedAt": "2026-01-12T10:30:00Z",
        \\  "narrative": "Migration aborted: Rolled back to original schema"
        \\}
    ;
    try sendJson(request, .ok, response);
}

// =============================================================================
// Health & Metrics
// =============================================================================

fn handleHealth(request: *std.http.Server.Request) !void {
    const response =
        \\{
        \\  "status": "healthy",
        \\  "version": "0.0.4",
        \\  "uptime": 3600,
        \\  "checks": {
        \\    "database": "pass",
        \\    "journal": "pass"
        \\  }
        \\}
    ;
    try sendJson(request, .ok, response);
}

fn handleMetrics(allocator: std.mem.Allocator, request: *std.http.Server.Request) !void {
    const prometheus_metrics = try metrics.getPrometheus(allocator);
    defer allocator.free(prometheus_metrics);

    request.respond(prometheus_metrics, .{
        .status = .ok,
        .extra_headers = &.{
            .{ .name = "content-type", .value = "text/plain; version=0.0.4" },
        },
    }) catch {};
}

// =============================================================================
// Response Helpers
// =============================================================================

fn sendJson(request: *std.http.Server.Request, status: std.http.Status, body: []const u8) !void {
    request.respond(body, .{
        .status = status,
        .extra_headers = &.{
            .{ .name = "content-type", .value = "application/json" },
        },
    }) catch {};
}

fn sendBadRequest(request: *std.http.Server.Request, message: []const u8) !void {
    _ = message;
    const body =
        \\{"error":"bad_request","message":"Invalid request"}
    ;
    try sendJson(request, .bad_request, body);
}

fn sendUnauthorized(request: *std.http.Server.Request) !void {
    const body =
        \\{"error":"unauthorized","message":"Authentication required"}
    ;
    try sendJson(request, .unauthorized, body);
}

fn sendNotFound(request: *std.http.Server.Request) !void {
    const body =
        \\{"error":"not_found","message":"Resource not found"}
    ;
    try sendJson(request, .not_found, body);
}

fn sendMethodNotAllowed(request: *std.http.Server.Request) !void {
    const body =
        \\{"error":"method_not_allowed","message":"Method not allowed for this endpoint"}
    ;
    request.respond(body, .{
        .status = .method_not_allowed,
        .extra_headers = &.{
            .{ .name = "content-type", .value = "application/json" },
        },
    }) catch {};
}

test "extract collection name" {
    try std.testing.expectEqualStrings("articles", extractCollectionName("/collections/articles").?);
    try std.testing.expectEqual(@as(?[]const u8, null), extractCollectionName("/collections"));
    try std.testing.expectEqual(@as(?[]const u8, null), extractCollectionName("/collections/"));
}
