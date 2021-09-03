
const std = @import("std");
const csv = @import("formats/csv.zig");
const args = @import("zig-args/args.zig");
const Random = std.rand.Random;

pub fn main() !void {
    var general_purpose_allocator = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = &general_purpose_allocator.allocator;
    const out = std.io.getStdOut().writer();

    var o = try args.parseForCurrentProcess(struct {
        @"n-fields": usize = 100,
        @"n-records": u64 = 10_000,
        seed: u32 = 1,

        pub const shorthands = .{
            .f = "n-fields",
            .r = "n-records",
            .s = "seed",
        };
    }, allocator, .print);
    defer o.deinit();

    var random = std.rand.DefaultPrng.init(o.options.seed).random;
    var types: []FieldDesc = try allocator.alloc(FieldDesc, o.options.@"n-fields");
    for (types) |*t| {
        t.* = generateFieldDesc(&random);
    }

    var writer = csv.Writer(@TypeOf(out)).init(out);

    try generateHeader(&writer, o.options.@"n-fields");
    var counter = o.options.@"n-records";
    while (counter > 0) : (counter -= 1) {
        try generateRecord(&writer, types, &random);
    }
}

const FieldType = enum { Bool, Int, Float, String };
const FieldDesc = struct {
    data_type: FieldType,
};

fn generateFieldDesc(random: *Random) FieldDesc {
    return FieldDesc{
        .data_type = random.enumValue(FieldType),
    };
}

fn generateHeader(writer: anytype, n_fields: usize) !void {
    var buf: [8]u8 = undefined;
    var i : usize = 0;
    while (i < n_fields) : (i += 1) {
        const str = try std.fmt.bufPrint(buf[0..], "f{}", .{i});
        try writer.writeField(str);
    }
    try writer.finishRecord();
}

fn generateRecord(writer: anytype, fields: []FieldDesc, random: *Random) !void {
    var buf: [100]u8 = undefined;
    for (fields) |f| {
        switch (f.data_type) {
            .Bool  => try writer.writeField(random.boolean()),
            .Int   => try writer.writeField(random.int(u16)),
            .Float => try writer.writeField(random.float(f32)),
            .String=> {
                const l = random.uintLessThan(usize, buf.len);
                for (buf[0..l]) |*char| {
                    char.* = @enumToInt(random.enumValue(StringChars));
                }
                try writer.writeField(buf[0..l]);
            },
        }
    }
    try writer.finishRecord();
}

const StringChars = enum(u8) {
    A = 'A',
    B = 'B',
    C = 'C',
    D = 'D',
    E = 'E',
    F = 'F',
    G = 'G',
    H = 'H',
    I = 'I',
    J = 'J',
    K = 'K',
    L = 'L',
    M = 'M',
    N = 'N',
    O = 'O',
    P = 'P',
    Q = 'Q',
    R = 'R',
    S = 'S',
    T = 'T',
    U = 'U',
    V = 'V',
    W = 'W',
    X = 'X',
    Y = 'Y',
    Z = 'Z',
    a = 'a',
    b = 'b',
    c = 'c',
    d = 'd',
    e = 'e',
    f = 'f',
    g = 'g',
    h = 'h',
    i = 'i',
    j = 'j',
    k = 'k',
    l = 'l',
    m = 'm',
    n = 'n',
    o = 'o',
    p = 'p',
    q = 'q',
    r = 'r',
    s = 's',
    t = 't',
    u = 'u',
    v = 'v',
    w = 'w',
    x = 'x',
    y = 'y',
    z = 'z',
    period = '.',
    comma = ',',
    colon = ':',
    semicolon = ';',
    doublequote = '"',
};
