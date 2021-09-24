const std = @import("std");
const incline = @import("./incline.zig");

test "" {
    std.testing.refAllDecls(incline);
}
