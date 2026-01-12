// SPDX-License-Identifier: AGPL-3.0-or-later
// FormDB API Server - Authentication

const std = @import("std");
const config = @import("config.zig");

const log = std.log.scoped(.auth);

var allocator: std.mem.Allocator = undefined;
var cfg: *const config.Config = undefined;

// API key storage (in production, use persistent storage)
var api_keys: std.StringHashMap(ApiKey) = undefined;

pub const ApiKey = struct {
    name: []const u8,
    scopes: []const Scope,
    created_at: i64,
    expires_at: ?i64,
};

pub const Scope = enum {
    read,
    write,
    admin,
    migrate,
};

pub fn init(alloc: std.mem.Allocator, config_ptr: *const config.Config) !void {
    allocator = alloc;
    cfg = config_ptr;
    api_keys = std.StringHashMap(ApiKey).init(alloc);

    // Add default development key
    try api_keys.put("dev-key-12345", .{
        .name = "Development Key",
        .scopes = &[_]Scope{ .read, .write, .admin, .migrate },
        .created_at = std.time.timestamp(),
        .expires_at = null,
    });

    log.info("Authentication initialized", .{});
}

pub fn deinit() void {
    api_keys.deinit();
}

pub fn validateRequest(request: *std.http.Server.Request) !bool {
    // Check for API key
    if (getHeader(request, cfg.api_key_header)) |key| {
        return validateApiKey(key);
    }

    // Check for Bearer token
    if (getHeader(request, "authorization")) |auth| {
        if (std.mem.startsWith(u8, auth, "Bearer ")) {
            const token = auth["Bearer ".len..];
            return validateJWT(token);
        }
    }

    return false;
}

pub fn validateApiKey(key: []const u8) bool {
    if (api_keys.get(key)) |_| {
        return true;
    }
    return false;
}

pub fn validateJWT(token: []const u8) bool {
    // Simple JWT validation (in production, use proper JWT library)
    if (cfg.jwt_secret == null) {
        return false;
    }

    // JWT format: header.payload.signature
    var parts = std.mem.splitScalar(u8, token, '.');

    const header = parts.next() orelse return false;
    const payload = parts.next() orelse return false;
    const signature = parts.next() orelse return false;

    // Verify there are exactly 3 parts
    if (parts.next() != null) return false;

    // In production: verify signature using HMAC-SHA256
    _ = header;
    _ = payload;
    _ = signature;

    // Placeholder: accept any well-formed JWT in development
    return true;
}

pub fn getScopes(request: *std.http.Server.Request) []const Scope {
    if (getHeader(request, cfg.api_key_header)) |key| {
        if (api_keys.get(key)) |api_key| {
            return api_key.scopes;
        }
    }
    return &[_]Scope{};
}

pub fn hasScope(request: *std.http.Server.Request, required: Scope) bool {
    const scopes = getScopes(request);
    for (scopes) |s| {
        if (s == required or s == .admin) {
            return true;
        }
    }
    return false;
}

fn getHeader(request: *std.http.Server.Request, name: []const u8) ?[]const u8 {
    var iter = request.iterateHeaders();
    while (iter.next()) |header| {
        if (std.ascii.eqlIgnoreCase(header.name, name)) {
            return header.value;
        }
    }
    return null;
}

// =============================================================================
// JWT Helpers (placeholder implementation)
// =============================================================================

pub const Claims = struct {
    sub: []const u8, // Subject (user ID)
    exp: i64, // Expiration time
    iat: i64, // Issued at
    scopes: []const Scope,
};

pub fn createJWT(claims: Claims) ![]const u8 {
    _ = claims;
    // In production: create proper JWT with HMAC-SHA256 signature
    return "placeholder.jwt.token";
}

pub fn parseJWT(token: []const u8) !Claims {
    _ = token;
    // In production: parse and verify JWT
    return .{
        .sub = "anonymous",
        .exp = std.time.timestamp() + 3600,
        .iat = std.time.timestamp(),
        .scopes = &[_]Scope{.read},
    };
}

// =============================================================================
// mTLS Support (for gRPC)
// =============================================================================

pub const TLSConfig = struct {
    cert_path: []const u8,
    key_path: []const u8,
    ca_path: ?[]const u8 = null, // For mTLS
    require_client_cert: bool = false,
};

pub fn validateClientCert(cert_chain: []const u8) bool {
    // In production: validate client certificate against CA
    _ = cert_chain;
    return true;
}

test "api key validation" {
    try init(std.testing.allocator, undefined);
    defer deinit();

    try std.testing.expect(validateApiKey("dev-key-12345"));
    try std.testing.expect(!validateApiKey("invalid-key"));
}

test "jwt structure" {
    const valid_jwt = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ1c2VyIn0.signature";
    var parts = std.mem.splitScalar(u8, valid_jwt, '.');

    try std.testing.expect(parts.next() != null);
    try std.testing.expect(parts.next() != null);
    try std.testing.expect(parts.next() != null);
    try std.testing.expect(parts.next() == null);
}
