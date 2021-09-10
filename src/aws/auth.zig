
const std = @import("std");
const expectEqualStrings = std.testing.expectEqualStrings;
const hmac = std.crypto.auth.hmac.sha2.HmacSha256;

const dt = @import("../date_time.zig");
const http = @import("http");

const unsigned_payload_lit = "UNSIGNED-PAYLOAD";

///
pub const AuthParams = struct {
    ///
    access_key_id: []const u8,
    ///
    secret_access_key: []const u8,
    ///
    region:        []const u8,
    ///
    service:       []const u8,
    ///
    date_time:     dt.DateTime,

    /// Generates a signing key for AWS requests
    pub fn getKey(self: AuthParams) ![32]u8 {
        var scratch: [128]u8 = undefined;
        if (scratch.len < 4 + self.secret_access_key.len) {
            return error.insufficient_buffer_space;
        }
        std.mem.copy(u8, scratch[0..4], "AWS4");
        std.mem.copy(u8, scratch[4..], self.secret_access_key);
        const initial_key = scratch[0..4+self.secret_access_key.len];

        var date_key: [32]u8 = undefined;
        const str_date = try dt.Iso8601Basic.printDate(self.date_time.date);
        hmac.create(&date_key, &str_date, initial_key[0..]);

        var region_key: [32]u8 = undefined;
        hmac.create(&region_key, self.region, &date_key);

        var service_key: [32]u8 = undefined;
        hmac.create(&service_key, self.service, &region_key);

        var signing_key: [32]u8 = undefined;
        hmac.create(&signing_key, "aws4_request", &service_key);

        return signing_key;
    }

    /// Returns `content` signed with this key, formatted as hex
    pub fn sign(self: AuthParams, content: []const u8) ![64]u8 {
        const key = try self.getKey();

        var signature: [32]u8 = undefined;
        hmac.create(&signature, content, &key);

        var ret: [64]u8 = undefined;
        _ = try std.fmt.bufPrint(&ret, "{x}", .{ std.fmt.fmtSliceHexLower(&signature) });
        return ret;
    }

};

///
const AuthContext = AuthContextImpl(false);

fn AuthContextImpl(comptime debug: bool) type {
    const DebugFields = struct {
        canonical_request: []const u8,
        string_to_sign: []const u8,
    };
    const Mixin = if (debug) DebugFields else .{};

    return struct {
        allocator: *std.mem.Allocator,

        auth_value:            []const u8,
        x_amz_content_value:   ?[]const u8 = null,
        x_amz_date:            []const u8 = null,
        signed_headers_string: []const u8,
        debug: Mixin,

        const Self = @This();

        ///
        pub fn init(allocator: *std.mem.Allocator) Self {
            var ret: Self = undefined;
            ret.allocator = allocator;
            return ret;
        }

        /// Free the memory associated with this AuthContext
        pub fn deinit(self: Self) void {
            allocator.free(self.auth_value);
            if (self.x_amz_content_value) |v| {
                allocator.free(v);
            }
            allocator.free(self.x_amz_date);
            allocator.free(self.signed_headers_string);

            if (debug) {
                allocator.free(self.canonical_request);
                allocator.free(self.string_to_sign);
                allocator.free(self.signature);
            }
        }
    };
}

/// Takes a ready-to-send request and injects all the necessary headers to
///  authenticate it.  Because allocated strings will be strewn throughout
///  the Request structure, the `allocator` parameter should be some sort
///  of arena allocator that can be deinitialized when the request is done.
pub fn authenticate(context: anytype, request: *http.Request, params: AuthParams, signed_payload: bool) !void {

    const ContextType = @TypeOf(context.*);

    // Do we need to sign this payload?
    if (signed_payload) {
        var payload_hash: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(request.body, &payload_hash, .{});
        context.x_amz_content_value = try std.fmt.allocPrint(context.allocator, "{x}", .{std.fmt.fmtSliceHexLower(&payload_hash)});
    }
    try request.headers.append("x-amz-content-sha256", context.x_amz_content_value orelse unsigned_payload_lit);

    // A date header also seems to be expected
    context.x_amz_date = try dt.Iso8601Basic.printAlloc(context.allocator, params.date_time);
    try request.headers.append("x-amz-date", context.x_amz_date);

    // Now we can generate the canonical request
    var cr_builder = std.ArrayList(u8).init(context.allocator);
    defer {
        if (@hasField(ContextType, "debug")) {
            context.debug.canonical_request = cr_builder.toOwnedSlice();
        }
        cr_builder.deinit();
    }
    try writeCanonicalRequest(cr_builder.writer(), request, context);

    // With the canonical request in hand we can create the String To Sign
    var sts_builder = std.ArrayList(u8).init(context.allocator);
    defer {
        if (@hasField(ContextType, "debug")) {
            context.debug.string_to_sign = sts_builder.toOwnedSlice();
        }
        sts_builder.deinit();
    }
    try writeStringToSign(sts_builder.writer(), params, cr_builder.items);

    // We can sign the request now
    const signature = try params.sign(sts_builder.items);

    // Finally, inject the Authorization header
    var auth_value_builder = std.ArrayList(u8).init(context.allocator);
    try auth_value_builder.appendSlice("AWS4-HMAC-SHA256 ");
    try auth_value_builder.appendSlice("Credential=");
    try auth_value_builder.appendSlice(params.access_key_id);
    try auth_value_builder.append('/');
    const date = try dt.Iso8601Basic.printDate(params.date_time.date);
    try auth_value_builder.appendSlice(&date);
    try auth_value_builder.append('/');
    try auth_value_builder.appendSlice(params.region);
    try auth_value_builder.append('/');
    try auth_value_builder.appendSlice(params.service);
    try auth_value_builder.appendSlice("/aws4_request");
    try auth_value_builder.append(',');
    try auth_value_builder.appendSlice("SignedHeaders=");
    try auth_value_builder.appendSlice(context.signed_headers_string);
    try auth_value_builder.append(',');
    try auth_value_builder.appendSlice("Signature=");
    try auth_value_builder.appendSlice(&signature);
    context.auth_value = auth_value_builder.toOwnedSlice();

    try request.headers.append("Authorization", context.auth_value);
}

/// Modifies `request` by injecting some headers, analyzes `request`, and
///  outputs a string representing the Canonical Request as defined by
///  https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html
pub fn writeCanonicalRequest(writer: anytype, request: *const http.Request, context: anytype) !void {
    // method
    try writer.writeAll(request.method.to_bytes());
    try writer.writeByte('\n');

    // canonical path
    if (!std.mem.startsWith(u8, request.uri.path, "/")) {
        try writer.writeByte('/');
    }
    try uriEncode(request.uri.path, writer, false);
    try writer.writeByte('\n');

    // canonical query string
    try writeCanonicalQueryString(writer, context, request.uri);
    try writer.writeByte('\n');


    // canonical headers and signed headers
    try writeCanonicalHeaders(writer, context, request);
    try writer.writeAll(context.signed_headers_string);
    try writer.writeByte('\n');

    // hashed payload (no final line break)
    try writer.writeAll(context.x_amz_content_value orelse unsigned_payload_lit);
}

fn writeCanonicalQueryString(writer: anytype, context: anytype, uri: http.Uri) !void {
    if (uri.query.len == 0) {
        return;
    }

    const params = try http.Uri.mapQuery(context.allocator, uri.query);
    //TODO defer free this

    // We need to encode THEN sort the parameters which means that we
    //  have to allocate for the encoded params
    var encoded_params = try context.allocator.alloc([]const u8, params.count());
    defer {
        for (encoded_params) |el| {
            context.allocator.free(el);
        }
        context.allocator.free(encoded_params);
    }

    var it = params.iterator();
    var encoded_param = std.ArrayList(u8).init(context.allocator);
    defer encoded_param.deinit();
    var i: usize = 0;
    while (it.next()) |entry| {
        try uriEncode(entry.key_ptr.*, encoded_param.writer(), true);
        try encoded_param.append('=');
        try uriEncode(entry.value_ptr.*, encoded_param.writer(), true);
        encoded_params[i] = encoded_param.toOwnedSlice();
        i += 1;
    }

    // Now we can sorted the encoded parameters
    std.sort.sort([]const u8, encoded_params, {}, cmpString);

    // And write them out separated by '&'
    var is_first: bool = true;
    for (encoded_params) |item| {
        if (is_first) {
            is_first = false;
        } else {
            try writer.writeByte('&');
        }
        try writer.writeAll(item);
    }
}

fn writeCanonicalHeaders(writer: anytype, context: anytype, request: *const http.Request) !void {
    var headers = try context.allocator.dupe(http.Header, request.headers.items());
    defer context.allocator.free(headers);

    // Need to lowercase the header names THEN sort them
    for (headers) |*h| {
        var newName = try context.allocator.alloc(u8, h.name.value.len);
        h.name.value = std.ascii.lowerString(newName, h.name.value);
    }
    defer {
        for (headers) |h| {
            context.allocator.free(h.name.value);
        }
    }

    // Sort the headers alphabetically
    std.sort.sort(http.Header, headers, {}, cmpHeader);

    var builder = std.ArrayList(u8).init(context.allocator);
    defer builder.deinit();
    var is_first = true;
    for (headers) |h| {
        try writer.writeAll(h.name.value);
        try writer.writeByte(':');
        try writer.writeAll( std.mem.trim(u8, h.value, &std.ascii.spaces) );
        try writer.writeByte('\n');

        // Also build up the signed_headers_string as we go
        if (is_first) {
            is_first = false;
        } else {
            try builder.append(';');
        }
        try builder.appendSlice(h.name.value);
    }
    try writer.writeByte('\n');

    // Record the signed headers string
    context.signed_headers_string = builder.toOwnedSlice();
}

fn cmpString(context: void, a: []const u8, b: []const u8) bool {
    _ = context;
    return std.mem.lessThan(u8, a, b);
}

fn cmpHeader(context: void, a: http.Header, b: http.Header) bool {
    return cmpString(context, a.name.value, b.name.value);
}

fn writeStringToSign(writer: anytype, params: AuthParams, canonical_request: []const u8) !void {
    try writer.writeAll("AWS4-HMAC-SHA256\n");
    try dt.Iso8601Basic.write(writer, params.date_time);
    try writer.writeByte('\n');

    // scope
    const date = (try dt.Iso8601Basic.printDate(params.date_time.date))[0..];
    try writer.print("{[date]s}/{[region]s}/{[service]s}/aws4_request",
                               .{.date=date, .region=params.region, .service=params.service});
    try writer.writeByte('\n');

    // request hash
    var hash: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(canonical_request, &hash, .{});
    try writer.print("{x}", .{std.fmt.fmtSliceHexLower(&hash)});
}



/// Performs URI Encoding of the supplied bytes. `in` is assumed to be UTF8.
/// Note that AWS recommends against using standard library implementations:
/// https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html
pub fn uriEncode(in: []const u8, out: anytype, encode_slash: bool) !void {
    var it = std.unicode.Utf8Iterator{ .bytes=in, .i=0 };
    while (it.nextCodepoint()) |c| {
        switch (c) {
            'A'...'Z', 'a'...'z', '0'...'9', '_', '-', '~', '.' => try out.writeByte(@intCast(u8, c)),

            '/' => try out.writeAll(if (encode_slash) "%2F" else "/"),

            else => try out.print("%{X}", .{c}),
        }
    }
}

///
pub fn uriEncodeBuf(in: []const u8, out: []u8, encode_slash: bool) ![]u8 {
    var buf = std.io.FixedBufferStream([]u8){ .buffer = out };
    uriEncode(in, buf.writer(), encode_slash);
    return buf.getWritten();
}


fn testExample(request: *http.Request, cr_witness: []const u8, sts_witness: []const u8, auth_witness: []const u8) !void {
    // Example testing parameters
    const params = AuthParams{
        .region = "us-east-1",
        .service = "s3",
        .access_key_id = "AKIAIOSFODNN7EXAMPLE",
        .secret_access_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        .date_time = try dt.Iso8601Basic.parse("20130524T000000Z"),
    };

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    var context = AuthContextImpl(true).init( &arena.allocator );
    try authenticate(&context, request, params, true);

    // Verify that all intermediate steps were performed correctly
    try expectEqualStrings(cr_witness, context.debug.canonical_request);
    try expectEqualStrings(sts_witness, context.debug.string_to_sign);

    // Check that the auth header value is correct
    const auth_header = request.headers.get("Authorization")
                          orelse return error.missing_header;
    try expectEqualStrings(auth_witness, auth_header.value);

    request.deinit();
}

test "Example: GET Object" {
    var request = try http.Request.builder(std.testing.allocator)
              .get("/test.txt")
              .header("Host", "examplebucket.s3.amazonaws.com")
              .header("Range", "bytes=0-9")
              .body("");
    try testExample(&request,
        \\GET
        \\/test.txt
        \\
        \\host:examplebucket.s3.amazonaws.com
        \\range:bytes=0-9
        \\x-amz-content-sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
        \\x-amz-date:20130524T000000Z
        \\
        \\host;range;x-amz-content-sha256;x-amz-date
        \\e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
        ,

        \\AWS4-HMAC-SHA256
        \\20130524T000000Z
        \\20130524/us-east-1/s3/aws4_request
        \\7344ae5b7ee6c3e7e6b0fe0640412a37625d1fbfff95c48bbb2dc43964946972
        ,
        "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,SignedHeaders=host;range;x-amz-content-sha256;x-amz-date,Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"
    );
}

test "Example: PUT Object" {
    var request = try http.Request.builder(std.testing.allocator)
          .put("test$file.text")
          .header("Host", "examplebucket.s3.amazonaws.com")
          .header("Date", "Fri, 24 May 2013 00:00:00 GMT")
          .header("x-amz-storage-class", "REDUCED_REDUNDANCY")
          .body("Welcome to Amazon S3.");

    try testExample(&request,
        \\PUT
        \\/test%24file.text
        \\
        \\date:Fri, 24 May 2013 00:00:00 GMT
        \\host:examplebucket.s3.amazonaws.com
        \\x-amz-content-sha256:44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072
        \\x-amz-date:20130524T000000Z
        \\x-amz-storage-class:REDUCED_REDUNDANCY
        \\
        \\date;host;x-amz-content-sha256;x-amz-date;x-amz-storage-class
        \\44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072
        ,
        \\AWS4-HMAC-SHA256
        \\20130524T000000Z
        \\20130524/us-east-1/s3/aws4_request
        \\9e0e90d9c76de8fa5b200d8c849cd5b8dc7a3be3951ddb7f6a76b4158342019d
        ,
        "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,SignedHeaders=date;host;x-amz-content-sha256;x-amz-date;x-amz-storage-class,Signature=98ad721746da40c64f1a55b78f14c238d841ea1380cd77a1b5971af0ece108bd"
    );
}

test "Example: GET Bucket Lifecycle" {
    var request = try http.Request.builder(std.testing.allocator)
          .get("?lifecycle")
          .header("Host", "examplebucket.s3.amazonaws.com")
          .body("");

    try testExample(&request,
        \\GET
        \\/
        \\lifecycle=
        \\host:examplebucket.s3.amazonaws.com
        \\x-amz-content-sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
        \\x-amz-date:20130524T000000Z
        \\
        \\host;x-amz-content-sha256;x-amz-date
        \\e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
        ,
        \\AWS4-HMAC-SHA256
        \\20130524T000000Z
        \\20130524/us-east-1/s3/aws4_request
        \\9766c798316ff2757b517bc739a67f6213b4ab36dd5da2f94eaebf79c77395ca
        ,
        "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,SignedHeaders=host;x-amz-content-sha256;x-amz-date,Signature=fea454ca298b7da1c68078a5d1bdbfbbe0d65c699e0f91ac7a200a0136783543"
    );
}

test "Example: GET Bucket (List Objects)" {
    var request = try http.Request.builder(std.testing.allocator)
          .get("?max-keys=2&prefix=J")
          .header("Host", "examplebucket.s3.amazonaws.com")
          .body("");

    try testExample(&request,
        \\GET
        \\/
        \\max-keys=2&prefix=J
        \\host:examplebucket.s3.amazonaws.com
        \\x-amz-content-sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
        \\x-amz-date:20130524T000000Z
        \\
        \\host;x-amz-content-sha256;x-amz-date
        \\e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
        ,
        \\AWS4-HMAC-SHA256
        \\20130524T000000Z
        \\20130524/us-east-1/s3/aws4_request
        \\df57d21db20da04d7fa30298dd4488ba3a2b47ca3a489c74750e0f1e7df1b9b7
        ,
        "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,SignedHeaders=host;x-amz-content-sha256;x-amz-date,Signature=34b48302e7b5fa45bde8084f4b7868a86f0a534bc59db6670ed5711ef69dc6f7"
    );
}
