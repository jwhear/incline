const std = @import("std");

pub fn build(b: *std.build.Builder) void {
    // Standard release options allow the person running `zig build` to select
    // between Debug, ReleaseSafe, ReleaseFast, and ReleaseSmall.
    const mode = b.standardReleaseOptions();

    // For now we'll rely on a simple XML parser from the vulkan-zig project
    _ = b.exec(&[_][]const u8{
        "curl", "-s",
        "https://raw.githubusercontent.com/Snektron/vulkan-zig/master/generator/xml.zig",
        "-o", "src/formats/xml.zig"
    }) catch {
        std.debug.print("Unable to download XML code\n", .{});
        return;
    };

    const lib = b.addStaticLibrary("incline", "src/incline.zig");
    lib.setBuildMode(mode);
    lib.install();

    //var main_tests = b.addTest("src/tests.zig");
    //main_tests.setBuildMode(mode);

    //const test_step = b.step("test", "Run library tests");
    //test_step.dependOn(&main_tests.step);
}
