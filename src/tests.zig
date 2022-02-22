const incline = @import("./incline.zig");

test "" {
    @import("std").testing.refAllDecls(incline);
}
