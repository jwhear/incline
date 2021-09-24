pub const kmeans = @import("algorithms/kmeans.zig");

test "" {
    const testing = @import("std").testing;
    testing.refAllDecls(kmeans);
}
