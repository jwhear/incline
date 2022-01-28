const std = @import("std");
const os = std.os;
const expect = std.testing.expect;
const expectEqualStrings = std.testing.expectEqualStrings;
const Allocator = std.mem.Allocator;

//TODO once naive version is implemented, try/benchmark:
// https://github.com/fwessels/simdcsv
// https://github.com/geofflangdale/simdcsv

const AlignedBuffer = []align(4096) u8;

///
const TokenType = enum { field, field_with_escaped_quote, record };

///
const Field = []const u8;
///
const FieldWithEscapedQuotes = struct {
    field: []const u8,
    nEscapedQuotes: u64,
};

///
const Token = union(TokenType) {
    field: Field,
    field_with_escaped_quote: FieldWithEscapedQuotes,
    record: void,
};

/// Tokenizes CSV (or similar separated format) from a buffer.
/// Because this reader provides a simple slicing interface over the underlying
///  buffer, escaped (doubled) quote characters occur in a field, they will _not_
///  be translated to a single quote character
pub const BufferTokenizer = struct {
    fd: i32 = std.math.maxInt(i32),
    buffer: []const u8,
    separator: u8 = ',',
    quote: u8 = '"',
    state: struct {
        o: u64, // current offset in buffer
        isEoR: bool,
    } = .{ .o = 0, .isEoR = false },

    ///
    pub fn fromPath(path: []const u8) !BufferTokenizer {
        var ret: BufferTokenizer = .{
            .buffer = undefined,
        };

        // Get a file descriptor and check the size of the file
        ret.fd = try os.open(path, os.O.RDONLY, 0);
        const st = try os.fstat(ret.fd);
        const fileLen = st.size;

        // Memory map the file into buffer
        ret.buffer = try os.mmap(null,
                                 @intCast(u64, fileLen),
                                 os.PROT.READ,  // prot
                                 os.MAP.SHARED, // flags
                                 ret.fd,
                                 0   // offset
                                 );

        return ret;

    }

    /// Reads the next token from the buffer and returns the slice
    /// Returns null if the buffer is exhausted
    pub fn next(self: *BufferTokenizer) ?Token {
        if (self.state.isEoR) {
            self.state.isEoR = false;
            return Token{ .record = {} };
        }

        if (self.state.o >= self.buffer.len) {
            return null;
        }

        var sliceStart = self.state.o;
        var inQuotes = false;
        var lastUnescapedQuote : u64 = 0;
        var nEscapedQuotes : u64 = 0;

        // Find next unquoted separator/delimiter/eof
        while (self.state.o < self.buffer.len) : (self.state.o += 1) {
            const c = self.buffer[self.state.o];
            if ((c == '\r' or c == '\n') and !inQuotes) {
                self.state.isEoR = true;
                break;
            } else if (c == self.separator and !inQuotes) {
                break;
            } else if (c == self.quote) {
                // Peek ahead: is this a doubled quote char?
                if (inQuotes and
                    self.state.o + 1 < self.buffer.len and
                    self.buffer[self.state.o+1] == self.quote) {
                    // this is escaped, skip over both
                    self.state.o += 1; // +1 here, continuation will add another
                    nEscapedQuotes += 1;
                    continue;
                } else {
                    if (inQuotes) {
                        lastUnescapedQuote = self.state.o;
                    } else {
                        // start the field after the first unescaped quote
                        sliceStart = self.state.o + 1;
                    }
                    inQuotes = !inQuotes;
                    continue;
                }
            }
        }
        const sliceEnd = if (lastUnescapedQuote > 0) lastUnescapedQuote else self.state.o;

        // Consume separator/delimiter
        self.state.o += 1;

        // consume possible trailing '\n' if present (\r\n case)
        if (self.state.o < self.buffer.len and self.buffer[self.state.o] == '\n') {
            self.state.o += 1;
        }

        // If we've exhausted the buffer, set up to emit end-of-record
        if (self.state.o >= self.buffer.len) {
            self.state.isEoR = true;
        }

        const slice = self.buffer[sliceStart .. sliceEnd];
        if (nEscapedQuotes > 0) {
            return Token{ .field_with_escaped_quote=.{ .field=slice, .nEscapedQuotes=nEscapedQuotes } };
        } else {
           return Token{ .field=slice };
        }
    }
};

fn expectField(val: []const u8, tok: ?Token) !void {
    if (tok) |t| {
        if (t == .field) {
            return expectEqualStrings(val, t.field);
        } else if (t == .field_with_escaped_quote) {
            return expectEqualStrings(val, t.field_with_escaped_quote.field);
        } else {
            return error.not_a_field;
        }
    }
    return error.parser_exhausted;
}

fn expectRecord(tok: ?Token) !void {
    if (tok) |t| {
        if (t == .record) {
            return;
        } else {
            return error.not_a_record;
        }
    }
    return error.parser_exhausted;
}

test "basic test of BufferTokenizer" {
    const source =
    \\field 1,field 2,  field 3
    \\1,      2,        3
    \\"a,b",  "c""d""", "foo"
    ;

    var tokens = BufferTokenizer{
        .buffer = source
    };

    try expectField("field 1",   tokens.next());
    try expectField("field 2",   tokens.next());
    try expectField("  field 3", tokens.next());
    try expectRecord(tokens.next());

    try expectField("1", tokens.next());
    try expectField("      2", tokens.next());
    try expectField("        3", tokens.next());
    try expectRecord(tokens.next());

    try expectField("a,b", tokens.next());
    try expectField("c\"\"d\"\"", tokens.next());
    try expectField("foo", tokens.next());
    try expectRecord(tokens.next());

    try expect(tokens.next() == null);
}

/// This layer deals with escaped characters, storing headers,
///  and record-level iteration.  As such it needs to store and use an allocator.
pub const Reader = struct {

    ///
    tokens: *BufferTokenizer,
    ///
    allocator: Allocator,
    ///
    current: []Field,
    ///
    header: []Field,
    ///
    unescapedStrings: std.ArrayList(Field),

    ///
    pub fn init(allocator: Allocator, tokens: *BufferTokenizer, hasHeader: bool) !Reader {
        var ret = Reader{
            .tokens = tokens,
            .allocator = allocator,
            .current = undefined,
            .header = undefined,
            .unescapedStrings = std.ArrayList(Field).init(allocator),
        };

        if (hasHeader) {
            var list = std.ArrayList(Field).init(allocator);
            var tok = tokens.next();
            while (tok) |t| : (tok = tokens.next()) {
                try switch (t) {
                    .field => |v| list.append(v),
                    .field_with_escaped_quote => |v| list.append(try ret.unescape(v, false)),
                    .record => break,
                };
            }
            ret.header = list.items;
        }

        ret.current = try allocator.alloc(Field, ret.header.len);
        return ret;
    }

    ///
    pub fn initWithHeader(allocator: Allocator, tokens: *BufferTokenizer, header: []Field) !Reader {
        var ret = Reader{
            .tokens = tokens,
            .allocator = allocator,
            .current = undefined,
            .header = header,
            .unescapedStrings = std.ArrayList(Field).init(allocator),
        };

        std.debug.print("Allocating current with {} values\n", .{ret.header.len});
        ret.current = try allocator.alloc(Field, ret.header.len);
        return ret;
    }

    ///
    pub fn deinit(self: *Reader) void {
        self.allocator.free(self.current);
        self.allocator.free(self.header);
    }

    ///
    pub fn next(self: *Reader) !?[]Field {
        // eof?
        var tok = self.tokens.next();
        if (tok == null) {
            return null;
        }

        // populate current and return it
        var out : usize = 0;
        while (true) : ({ out += 1; tok = self.tokens.next(); }) {

            if (tok) |t| {
                // if record end
                if (t == .record) {
                    break;
                }

                // more fields than we expect?
                if (out >= self.current.len) {
                    return error.record_too_long;
                }

                self.current[out] = if (t == .field) t.field else try self.unescape(t.field_with_escaped_quote, true);
            } else {
                return error.unexpected_end_of_record;
            }
        }

        // ... premature?
        if (out < self.current.len) {
            return error.unexpected_end_of_record;
        }
        return self.current;
    }

    /// Frees all unescaped strings: you can call this after every record if you
    ///  don't need to retain the content of the records
    pub fn freeUnescapedStrings(self: *Reader) void {
        for (self.unescapedStrings) |str| {
            self.allocator.free(str);
        }
    }

    fn unescape(self: *Reader, content: FieldWithEscapedQuotes, store: bool) !Field {
        // Because we track how many escaped quotes we saw during tokenization,
        //  we can allocate precisely the number of bytes we need
        var ret = try self.allocator.alloc(u8, content.field.len - content.nEscapedQuotes);

        //TODO special cases when escaped quotes are at beginning or end: can simply slice

        // Copy into ret, taking only the second quote char of escaped pairs
        var in: usize = 0;
        var out: usize = 0;
        var isEscaped = false;
        while (in < content.field.len) : (in += 1) {
            const c = content.field[in];

            if (c == self.tokens.quote and !isEscaped) {
                isEscaped = true;
                // don't write anything to ret
            } else {
                if (out >= ret.len) {
                    std.debug.panic("Output is too small: \n" ++
                                    "ret.len = {}, content.nEscapedQuotes = {}, out = {}, content.field = {s}",
                                    .{ret.len, content.nEscapedQuotes, out, content.field});

                }

                ret[out] = c;
                isEscaped = false;
                out += 1;
            }
        }

        // Store the string for later dealloc
        if (store) {
            try self.unescapedStrings.append(ret);
        }
        return ret;
    }
};

fn expectRow(witness: anytype, subj: ?[]Field) !void {
    if (subj) |s| {
        if (witness.len != s.len) {
            return error.unexpected_record_len;
        }

        for (witness) |w, i| {
            return expectEqualStrings(w, s[i]);
        }

    } else {
        return error.unexpected_end_of_file;
    }

}

test "basic test of Reader" {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const source =
    \\field 1,field 2,  field 3
    \\1,      2,        3
    \\"a,b",  "c""d""", "foo"
    ;

    var tokens = BufferTokenizer{
        .buffer = source
    };
    var reader = try Reader.init(allocator, &tokens, true);

    try expect(reader.header.len == 3);
    try expectEqualStrings(reader.header[0], "field 1");
    try expectEqualStrings(reader.header[1], "field 2");
    try expectEqualStrings(reader.header[2], "  field 3");

    try expectRow([_]Field{ "1", "      2", "        3" }, try reader.next());
    try expectRow([_]Field{ "a,b", "c\"d\"", "foo"}, try reader.next());
    const last = try reader.next();
    try expect(last == null);
}

///
const Rename = struct { @"0": []const u8, @"1": []const u8};

///
pub fn StructReader(comptime T: type) type {
    return struct {
        const Self = @This();
        const n_fields = std.meta.fields(T).len;

        reader: *Reader,
        mapping: [n_fields]u32,

        ///
        pub fn init(reader: *Reader) Self {
            const m = comptime createDefaultMapping(T);
            return Self{
                .reader = reader,
                .mapping = m,
            };
        }

        ///
        pub fn initWithMapping(reader: *Reader, map: anytype) !Self {
            const m = try createMapping(T, reader.header, map);
            return Self{
                .reader = reader,
                .mapping = m,
            };
        }

        ///
        pub fn next(self: *Self) !?T {
            const row = try self.reader.next();
            if (row) |r| {
                var ret: T = undefined;
                try self.fill(&ret, r);
                return ret;
            } else {
                return null;
            }
        }

        fn fill(self: Self, v: *T, fields: []Field) !void {
            const ti = @typeInfo(T).Struct;
            inline for (ti.fields) |f, i| {
                const value_index = self.mapping[i];
                if (value_index != BlackHole) {
                    @field(v.*, f.name) = try coerce(f.field_type, fields[i]);
                }
            }
        }
    };
}

// std.meta.eql compares the string pointers, not their contents
fn expectStruct(witness: anytype, t: anytype) !void {
    if (t) |u| {
        inline for (std.meta.fields(@TypeOf(witness))) |field| {
            const a = @field(witness, field.name);
            const b = @field(u, field.name);
            std.debug.print("{s}: {any} == {any}\n", .{field.name, a, b});
            const same = switch (@typeInfo(@TypeOf(a))) {
                .Pointer => |info| blk: {
                    break :blk switch (info.size) {
                        .Slice => std.mem.eql(@TypeOf(a[0]), a, b),
                        else => a == b
                    };
                },
                else => std.meta.eql(a, b),
            };
            if (!same) return error.no_match;
        }

        return;
    } else {
        return error.unexpected_end_of_file;
    }
}
test "StructReader" {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const source =
    \\f1,f2,f3
    \\1,2,foo
    \\2, "c""d""", "bar"
    ;
    const Rec = struct {
        f1: i32,
        f2: []const u8,
        f3: []const u8,
    };

    const witness = [_]Rec{
        .{ .f1=1, .f2="2", .f3="foo" },
        .{ .f1=2, .f2="c\"d\"", .f3="bar" },
    };

    var tokens = BufferTokenizer{
        .buffer = source
    };
    var reader = try Reader.init(allocator, &tokens, true);
    std.debug.print("{s}\n", .{reader.header});
    var structReader = StructReader(Rec).init(&reader);
    try expectStruct(witness[0], try structReader.next());
    try expectStruct(witness[1], try structReader.next());
}

fn coerce(comptime T: type, value: []const u8) !T {
    if (T == []const u8) {
        return value;
    }
    const trimmed = std.mem.trim(u8, value, " \t");
    return switch (@typeInfo(T)) {
        .Bool => {
            if (std.ascii.eqlIgnoreCase(trimmed, "true") or
                std.ascii.eqlIgnoreCase(trimmed, "t")) {
                return true;
            } else if (std.ascii.eqlIgnoreCase(trimmed, "false") or
                std.ascii.eqlIgnoreCase(trimmed, "f")) {
                return false;
            } else {
                return error.not_a_bool_value;
            }
        },

        .Float => try std.fmt.parseFloat(T, trimmed),
        .Int => try std.fmt.parseInt(T, trimmed, 10),
        .Enum => std.meta.stringToEnum(T, trimmed) orelse error.invalid_enum_value,
        else => error.uncoerceable_type,
    };
}
test "coerce" {
    try expect(true == try coerce(bool, "true"));
    try expect(false == try coerce(bool, "F"));
    try expect(123 == try coerce(u32, "123"));
    try expect(123 == try coerce(u32, "  123"));
    try expect(2.0 == try coerce(f32, "  2.0"));
    try expectEqualStrings("  hi  ", try coerce([]const u8, "  hi  "));
}

/// Index value for fields left deliberately unmapped
const BlackHole = std.math.maxInt(u32);

fn createDefaultMapping(comptime T: type) [std.meta.fields(T).len]u32 {
    var ret: [std.meta.fields(T).len]u32 = undefined;
    for (ret) |*v, i| {
        v.* = @intCast(u32, i);
    }
    return ret;
}
test "createDefaultMapping" {
    const TestRec = struct {
        f1: u32,
        f2: []const u8,
    };
    const map = createDefaultMapping(TestRec);
    try expect(map.len == 2);
    try expect(map[0] == 0);
    try expect(map[1] == 1);
}

fn createMapping(comptime T: type, header: []Field, map: anytype) ![std.meta.fields(T).len]u32 {
    const ti = @typeInfo(T).Struct;
    const Map = @TypeOf(map);

    var ret = [_]u32{BlackHole} ** ti.fields.len;
    inline for (ti.fields) |field, i| {
        const name = field.name;

        // Find the mapping entry with this name
        const entry = std.meta.fieldIndex(Map, name);

        // Is the field mapped?
        if (entry) |_| {
            const value : []const u8 = @field(map, name);
            const index = indexOfString(header, value) orelse return error.field_not_found;

            ret[i] = @intCast(u32, index);

        } // else leave it as BlackHole
    }
    return ret;
}
test "createMapping" {
    const TestRec = struct {
        foo: u32,
        bar: []const u8,
        baz: bool,
    };
    var header = [_][]const u8{
        "bim", "bam", "boo", "another"
    };
    const renames = .{
        .foo = "bam",
        .baz = "bim",
    };
    const map = try createMapping(TestRec, header[0..], renames);
    try expect(map.len == 3);
    try expect(map[0] == 1);
    try expect(map[1] == BlackHole);
    try expect(map[2] == 0);
}

fn indexOfString(haystack: [][]const u8, needle: []const u8) ?usize {
    for (haystack) |h, i| {
        if (std.mem.eql(u8, h, needle)) {
            return i;
        }
    }
    return null;
}

///
pub fn readStructsFrom(comptime T: type, allocator: Allocator, path: []const u8) !StructReader(T) {
    var tokens = try BufferTokenizer.fromPath(path);
    var reader = try Reader.init(allocator, &tokens, true);
    return StructReader(T).init(&reader);
}

///
pub fn Writer(comptime Out: type) type {
    return struct {
        const Self = @This();

        out: Out,
        separator: u8 = ',',
        quote: u8 = '"',
        delimiter: []const u8 = "\n",
        currentFieldIndex: u64 = 0,

        ///
        pub fn init(out: Out) Self {
            return Self{
                .out = out,
            };
        }

        ///
        pub fn writeHeader(self: *Self, header: []const u8) !void {
            for (header) |h| {
                self.writeString(h);
            }
            self.finishRecord();
        }

        ///
        pub fn writeHeaderFromStruct(self: *Self, comptime T: type) !void {
            const info = comptime @typeInfo(T);
            for (info.fields) |field| {
                self.writeString(field.name);
            }
            self.finishRecord();
        }

        ///
        pub fn writeField(self: *Self, value: anytype) !void {
            defer self.currentFieldIndex += 1;

            const Type = comptime @TypeOf(value);
            if (comptime std.meta.trait.isZigString(Type)) {
                return try self.writeString(value);
            }

            if (self.currentFieldIndex == 0) {
                _ = try self.out.print("{}", .{value});
            } else {
                _ = try self.out.print("{c}{}", .{self.separator, value});
            }
        }

        fn writeString(self: *Self, value: anytype) !void {
            const Type = comptime @TypeOf(value);
            if (comptime !std.meta.trait.isZigString(Type)) {
                @compileError("Cannot call writeString with a " ++ @typeName(Type));
            }

            if (self.currentFieldIndex > 0) {
                _ = try self.out.writeByte(self.separator);
            }

            // Does the value contain special characters that require us to
            //  wrap this whole string in quotes?
            const needQuoting = [_]u8{ self.separator, self.quote, '\r', '\n'};
            const sepPos = std.mem.indexOfAny(u8, value, needQuoting[0..]);
            if (sepPos) |_| {
                try self.out.writeByte(self.quote);
            }

            // We need to scan value for quote that need escaping
            var window = value[0..];
            var nextQuote = std.mem.indexOfScalar(u8, window, self.quote);
            while (nextQuote != null) : (nextQuote = std.mem.indexOfScalar(u8, window, self.quote)) {
                // Plus one because we want to include the quote in our slice
                const n = nextQuote.? + 1;
                // Write everything up to and including nextQuote
                _ = try self.out.write(window[0..n]);
                // Write a quote (the previous one will be the escaping quote)
                _ = try self.out.writeByte(self.quote);
                // Move window
                window = window[n..];
            }

            // Write the remainder of window
            _ = try self.out.write(window);

            if (sepPos) |_| {
                try self.out.writeByte(self.quote);
            }
        }

        ///
        pub fn finishRecord(self: *Self) !void {
            _ = try self.out.write(self.delimiter);
            self.currentFieldIndex = 0;
        }

        ///
        pub fn writeRecord(self: *Self, record: anytype) !void {
            const Type = comptime @TypeOf(record);
            switch (@typeInfo(Type)) {
                .Array => {
                    for (record) |value| {
                        try self.writeField(value);
                    }
                },

                .Struct => |info| {
                    for (info.fields) |field| {
                        const value = @field(record, field.name);
                        try self.writeField(value);
                    }
                },

                else => return error.unsupported_type,
            }
            try self.finishRecord();
        }
    };
}
