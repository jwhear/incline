
const std = @import("std");
const csv = @import("../../formats/csv.zig");

pub fn testCSV() !void {

    var general_purpose_allocator = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = &general_purpose_allocator.allocator;

    const stdout = std.io.getStdOut().writer();

    var tokens = try csv.BufferTokenizer.fromPath("data/big.csv");
    //var tokens = try csv.BufferTokenizer.fromPath("data/edge.csv");
    var reader = try csv.Reader.init(allocator, &tokens, true);
    var record = try reader.next();

    while (record) |r| : (record = try reader.next()) {
        for (r) |field| {
            try stdout.print("{s} | ", .{field});
        }
        try stdout.print("\n", .{});
    }
}
