const csv_sf_load = @import("tests/csv/sf_loan.zig");
const csv_generic = @import("tests/csv/generic.zig");

pub fn main() !void {
    try csv_sf_load.testCSV();
    try csv_generic.testCSV();
}
