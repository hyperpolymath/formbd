// SPDX-License-Identifier: AGPL-3.0-or-later
// FormDB API Server - gRPC Handler
//
// gRPC over HTTP/2 with Protocol Buffers
// This is a basic implementation; for production use, consider grpc-zig or similar

const std = @import("std");
const config = @import("config.zig");

const log = std.log.scoped(.grpc);

pub fn handleRequest(
    allocator: std.mem.Allocator,
    request: *std.http.Server.Request,
    cfg: *const config.Config,
) !void {
    _ = allocator;
    _ = cfg;

    const path = request.head.target;

    // gRPC uses POST with specific content-type
    if (request.head.method != .POST) {
        try sendGrpcError(request, 12, "Unimplemented: Only POST supported");
        return;
    }

    // Check content-type
    const content_type = getHeader(request, "content-type") orelse "";
    if (!std.mem.startsWith(u8, content_type, "application/grpc")) {
        try sendGrpcError(request, 3, "Invalid content-type for gRPC");
        return;
    }

    // Route to service method
    // Path format: /grpc/formdb.v1.FormDB/MethodName
    if (std.mem.indexOf(u8, path, "/formdb.v1.FormDB/")) |idx| {
        const method = path[idx + "/formdb.v1.FormDB/".len ..];
        try routeGrpcMethod(request, method);
    } else {
        try sendGrpcError(request, 12, "Unknown service");
    }
}

fn routeGrpcMethod(request: *std.http.Server.Request, method: []const u8) !void {
    log.info("gRPC method: {s}", .{method});

    // Route to method handler
    if (std.mem.eql(u8, method, "Query")) {
        try handleQuery(request);
    } else if (std.mem.eql(u8, method, "ListCollections")) {
        try handleListCollections(request);
    } else if (std.mem.eql(u8, method, "GetCollection")) {
        try handleGetCollection(request);
    } else if (std.mem.eql(u8, method, "CreateCollection")) {
        try handleCreateCollection(request);
    } else if (std.mem.eql(u8, method, "GetJournal")) {
        try handleGetJournal(request);
    } else if (std.mem.eql(u8, method, "DiscoverDependencies")) {
        try handleDiscoverDependencies(request);
    } else if (std.mem.eql(u8, method, "AnalyzeNormalForm")) {
        try handleAnalyzeNormalForm(request);
    } else if (std.mem.eql(u8, method, "StartMigration")) {
        try handleStartMigration(request);
    } else if (std.mem.eql(u8, method, "Health")) {
        try handleHealth(request);
    } else {
        try sendGrpcError(request, 12, "Unimplemented method");
    }
}

// =============================================================================
// gRPC Method Handlers (Stub implementations)
// =============================================================================

fn handleQuery(request: *std.http.Server.Request) !void {
    // TODO: Parse protobuf request, execute query, return protobuf response
    // For now, return a simple placeholder
    try sendGrpcResponse(request, &[_]u8{});
}

fn handleListCollections(request: *std.http.Server.Request) !void {
    try sendGrpcResponse(request, &[_]u8{});
}

fn handleGetCollection(request: *std.http.Server.Request) !void {
    try sendGrpcResponse(request, &[_]u8{});
}

fn handleCreateCollection(request: *std.http.Server.Request) !void {
    try sendGrpcResponse(request, &[_]u8{});
}

fn handleGetJournal(request: *std.http.Server.Request) !void {
    try sendGrpcResponse(request, &[_]u8{});
}

fn handleDiscoverDependencies(request: *std.http.Server.Request) !void {
    try sendGrpcResponse(request, &[_]u8{});
}

fn handleAnalyzeNormalForm(request: *std.http.Server.Request) !void {
    try sendGrpcResponse(request, &[_]u8{});
}

fn handleStartMigration(request: *std.http.Server.Request) !void {
    try sendGrpcResponse(request, &[_]u8{});
}

fn handleHealth(request: *std.http.Server.Request) !void {
    try sendGrpcResponse(request, &[_]u8{});
}

// =============================================================================
// gRPC Response Helpers
// =============================================================================

fn sendGrpcResponse(request: *std.http.Server.Request, data: []const u8) !void {
    // gRPC uses length-prefixed messages
    // Format: 1 byte compression flag + 4 bytes length + data
    var frame: [5]u8 = undefined;
    frame[0] = 0; // No compression
    std.mem.writeInt(u32, frame[1..5], @intCast(data.len), .big);

    var response_data: [1024]u8 = undefined;
    @memcpy(response_data[0..5], &frame);
    if (data.len > 0) {
        @memcpy(response_data[5 .. 5 + data.len], data);
    }

    request.respond(response_data[0 .. 5 + data.len], .{
        .status = .ok,
        .extra_headers = &.{
            .{ .name = "content-type", .value = "application/grpc+proto" },
            .{ .name = "grpc-status", .value = "0" },
        },
    }) catch {};
}

fn sendGrpcError(request: *std.http.Server.Request, code: u8, message: []const u8) !void {
    _ = message;
    var code_str: [3]u8 = undefined;
    _ = std.fmt.bufPrint(&code_str, "{d}", .{code}) catch "0";

    request.respond("", .{
        .status = .ok,
        .extra_headers = &.{
            .{ .name = "content-type", .value = "application/grpc+proto" },
            .{ .name = "grpc-status", .value = &code_str },
        },
    }) catch {};
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

test "grpc path parsing" {
    const path = "/grpc/formdb.v1.FormDB/Query";
    if (std.mem.indexOf(u8, path, "/formdb.v1.FormDB/")) |idx| {
        const method = path[idx + "/formdb.v1.FormDB/".len ..];
        try std.testing.expectEqualStrings("Query", method);
    } else {
        return error.TestFailed;
    }
}
