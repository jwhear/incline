
const std = @import("std");
const csv = @import("../../formats/csv.zig");

const Row = struct {
    reference_pool_id: []const u8,
    loan_id: []const u8,
    monthly_reporting_period: []const u8,
    loan_age: []const u8,
    original_ltv: []const u8,
    total_deferral_amount: []const u8,

    pub fn print(self: *Row, out: anytype) !void {
        try out.print("id: {s} | original_ltv: {s}\n", .{self.loan_id, self.original_ltv});
    }
};

pub fn testCSV() !void {
    var general_purpose_allocator = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = &general_purpose_allocator.allocator;

    const stdout = std.io.getStdOut().writer();

    const renames = .{
        .reference_pool_id = "Reference Pool ID",
        .loan_id = "Loan Identifier",
        .monthly_reporting_period = "Monthly Reporting Period",
        .loan_age = "Loan Age",
        .original_ltv = "Original Loan to Value Ratio (LTV)",
        .total_deferral_amount = "Total Deferral Amount",
    };
    var tokens = try csv.BufferTokenizer.fromPath("data/sf-loan-performance-data-sample.csv");
    tokens.separator = '|';
    var reader = try csv.Reader.initWithHeader(allocator, &tokens, header[0..]);
    var records = try csv.StructReader(Row).initWithMapping(&reader, renames);
    var record = try records.next();
    while (record != null) : (record = try records.next()) {
        try record.?.print(stdout);
    }
}

// The data does not have a header row: we'll have to provide it
var header = [_][]const u8{
    "Reference Pool ID",
    "Loan Identifier",
    "Monthly Reporting Period",
    "Channel",
    "Seller Name",
    "Servicer Name",
    "Master Servicer",
    "Original Interest Rate",
    "Current Interest Rate",
    "Original UPB",
    "UPB at Issuance",
    "Current Actual UPB",
    "Original Loan Term",
    "Origination Date",
    "First Payment Date",
    "Loan Age",
    "Remaining Months to Legal Maturity",
    "Remaining Months To Maturity",
    "Maturity Date",
    "Original Loan to Value Ratio (LTV)",
    "Original Combined Loan to Value Ratio (CLTV)",
    "Number of Borrowers",
    "Debt-To-Income (DTI)",
    "Borrower Credit Score at Origination",
    "Co-Borrower Credit Score at Origination",
    "First Time Home Buyer Indicator",
    "Loan Purpose ",
    "Property Type",
    "Number of Units",
    "Occupancy Status",
    "Property State",
    "Metropolitan Statistical Area (MSA)",
    "Zip Code Short",
    "Mortgage Insurance Percentage",
    "Amortization Type",
    "Prepayment Penalty Indicator",
    "Interest Only Loan Indicator",
    "Interest Only First Principal And Interest Payment Date",
    "Months to Amortization",
    "Current Loan Delinquency Status",
    "Loan Payment History",
    "Modification Flag",
    "Mortgage Insurance Cancellation Indicator",
    "Zero Balance Code",
    "Zero Balance Effective Date",
    "UPB at the Time of Removal",
    "Repurchase Date",
    "Scheduled Principal Current",
    "Total Principal Current",
    "Unscheduled Principal Current",
    "Last Paid Installment Date",
    "Foreclosure Date",
    "Disposition Date",
    "Foreclosure Costs",
    "Property Preservation and Repair Costs",
    "Asset Recovery Costs",
    "Miscellaneous Holding Expenses and Credits",
    "Associated Taxes for Holding Property",
    "Net Sales Proceeds",
    "Credit Enhancement Proceeds",
    "Repurchase Make Whole Proceeds",
    "Other Foreclosure Proceeds",
    "Non-Interest Bearing UPB",
    "Principal Forgiveness Amount",
    "Original List Start Date",
    "Original List Price",
    "Current List Start Date",
    "Current List Price",
    "Borrower Credit Score At Issuance",
    "Co-Borrower Credit Score At Issuance",
    "Borrower Credit Score Current ",
    "Co-Borrower Credit Score Current",
    "Mortgage Insurance Type",
    "Servicing Activity Indicator",
    "Current Period Modification Loss Amount",
    "Cumulative Modification Loss Amount",
    "Current Period Credit Event Net Gain or Loss",
    "Cumulative Credit Event Net Gain or Loss",
    "HomeReady® Program Indicator",
    "Foreclosure Principal Write-off Amount",
    "Relocation Mortgage Indicator",
    "Zero Balance Code Change Date",
    "Loan Holdback Indicator",
    "Loan Holdback Effective Date",
    "Delinquent Accrued Interest",
    "Property Valuation Method ",
    "High Balance Loan Indicator ",
    "ARM Initial Fixed-Rate Period  ≤ 5 YR Indicator",
    "ARM Product Type",
    "Initial Fixed-Rate Period ",
    "Interest Rate Adjustment Frequency",
    "Next Interest Rate Adjustment Date",
    "Next Payment Change Date",
    "Index",
    "ARM Cap Structure",
    "Initial Interest Rate Cap Up Percent",
    "Periodic Interest Rate Cap Up Percent",
    "Lifetime Interest Rate Cap Up Percent",
    "Mortgage Margin",
    "ARM Balloon Indicator",
    "ARM Plan Number",
    "Borrower Assistance Plan",
    "High Loan to Value (HLTV) Refinance Option Indicator",
    "Deal Name",
    "Repurchase Make Whole Proceeds Flag",
    "Alternative Delinquency Resolution",
    "Alternative Delinquency  Resolution Count",
    "Total Deferral Amount",
};
