const std = @import("std");
const time_t = std.c.time_t;
const expectEqual = std.testing.expectEqual;
const expectEqualStrings = std.testing.expectEqualStrings;

///
pub const Date = struct {
    year:  i17 = 1970, // -9999 - 9999
    month: u4  = 1,    // 1-12
    day:   u5  = 1,    // 1-31

    ///
    pub fn ensureValid(self: Date) !void {
        if (self.year < 0 or self.year > 9999) {
            return error.invalid_year;
        }
        if (self.month > 12) {
            return error.invalid_month;
        }
        if (self.day > 31) {
            return error.invalid_day;
        }
    }
};

///
pub const Time = struct {
    hour:   u5 = 0, // 0-23
    minute: u6 = 0, // 0-59
    second: u6 = 0, // 0-59
    //TODO include nanoseconds?

    ///
    pub fn ensureValid(self: Time) !void {
        if (self.hour > 23) {
            return error.invalid_hour;
        }
        if (self.minute > 59) {
            return error.invalid_minute;
        }
        if (self.second > 59) {
            return error.invalid_second;
        }
    }
};

///
pub const DateTime = struct {
    date: Date,
    time: Time,

    /// Converts the time_t to a DateTime using C's gmtime function
    pub fn gmtime(t: time_t) !DateTime {
        var a: tm = undefined;
        if (gmtime_r(&t, &a) == null) {
            return error.gmtime_failed;
        }

        return DateTime{
            .date = Date{
                .year  = @intCast(i17, a.tm_year + 1900),
                .month = @intCast(u4, a.tm_mon + 1),
                .day   = @intCast(u5, a.tm_mday),
            },
            .time = Time{
                .hour   = @intCast(u5, a.tm_hour),
                .minute = @intCast(u6, a.tm_min),
                .second = @intCast(u6, a.tm_sec),
            }
        };
    }

    /// Returns the current DateTime (UTC, not local)
    pub fn now() DateTime {
        return gmtime(std.time.timestamp()) catch unreachable;
    }
};

/// Formatter/parser for ISO 8601 (basic) date/time format
pub const Iso8601Basic = struct {

    /// Writes the ISO 8601 representation of `t`
    pub fn write(writer: anytype, dt: DateTime) !void {
        try writeDate(writer, dt.date);
        try writeTime(writer, dt.time);
    }

    ///
    pub fn writeDate(writer: anytype, d: Date) !void {
        try d.ensureValid();

        // cast year to an unsigned integer to prevent print putting a "+" in the front
        const u_year = @intCast(u17, d.year);
        try writer.print("{d:0>4}{d:0>2}{d:0>2}", .{ u_year, d.month, d.day });
    }

    ///
    pub fn writeTime(writer: anytype, t: Time) !void {
        try t.ensureValid();
        try writer.print("T{:0>2}{:0>2}{:0>2}Z", .{ t.hour, t.minute, t.second });
    }

    ///
    pub fn print(dt: DateTime) ![8 + 8]u8 {
        var buf: [8 + 8]u8 = undefined;
        var fbs = std.io.fixedBufferStream(buf[0..]);
        try writeDate(fbs.writer(), dt.date);
        try writeTime(fbs.writer(), dt.time);
        return buf;
    }

    ///
    pub fn printDate(d: Date) ![8]u8 {
        var buf: [8]u8 = undefined;
        var fbs = std.io.fixedBufferStream(buf[0..]);
        try writeDate(fbs.writer(), d);
        return buf;
    }

    ///
    pub fn printTime(t: Time) ![8]u8 {
        var buf: [8]u8 = undefined;
        var fbs = std.io.fixedBufferStream(buf[0..]);
        try writeTime(fbs.writer(), t);
        return buf;
    }

    ///
    pub fn printAlloc(allocator: *std.mem.Allocator, dt: DateTime) ![]u8 {
        var buf = try allocator.alloc(u8, 8 + 8);
        var fbs = std.io.fixedBufferStream(buf);
        try writeDate(fbs.writer(), dt.date);
        try writeTime(fbs.writer(), dt.time);
        return buf;
    }

    ///
    pub fn printDateAlloc(allocator: *std.mem.Allocator, d: Date) ![]u8 {
        var buf = try allocator.alloc(u8, 8);
        var fbs = std.io.fixedBufferStream(buf);
        try writeDate(fbs.writer(), d);
        return buf;
    }

    ///
    pub fn printTimeAlloc(allocator: *std.mem.Allocator, t: Time) ![]u8 {
        var buf = try allocator.alloc(u8, 8);
        var fbs = std.io.fixedBufferStream(buf);
        try writeTime(fbs.writer(), t);
        return buf;
    }

    /// Parses an ISO 8601 date and optionally time into a Timestamp
    pub fn parse(iso: []const u8) !DateTime {
        var ret: DateTime = .{
            .date = Date{},
            .time = Time{},
        };
        ret.date = try parseDate(iso);
        if (std.mem.indexOfScalar(u8, iso, 'T')) |t| {
            ret.time = try parseTime(iso[t..]);
        }
        return ret;
    }

    /// Parses an ISO 8601 date
    pub fn parseDate(iso: []const u8) !Date {
        var ret = Date{};
        var parser = Parser{.source=iso};

        // The standard allows abbreviated forms, e.g. "198" to refer the 1980s
        //  but everyone uses full forms, so we'll expect a complete date
        if (iso.len < 8) {
            return error.invalid_date;
        }
        ret.year  = try parser.expectDigits(@TypeOf(ret.year), 4);
        ret.month = try parser.expectDigits(@TypeOf(ret.month), 2);
        ret.day   = try parser.expectDigits(@TypeOf(ret.day), 2);

        if (ret.month < 1 or ret.month > 12) {
            return error.invalid_month;
        }
        if (ret.day < 1 or ret.day > 31) {
            return error.invalid_day;
        }

        return ret;
    }

    /// Parses an ISO 8601 time
    pub fn parseTime(iso: []const u8) !Time {
        var ret = Time{};
        var parser = Parser{.source=iso};

        try parser.expect('T');
        ret.hour   = try parser.expectDigits(@TypeOf(ret.hour), 2);
        ret.minute = try parser.expectDigits(@TypeOf(ret.minute), 2);
        ret.second = try parser.expectDigits(@TypeOf(ret.second), 2);

        if (ret.hour > 23) {
            return error.invalid_hour;
        }
        if (ret.minute > 59) {
            return error.invalid_minute;
        }
        if (ret.second > 59) {
            return error.invalid_second;
        }

        return ret;
    }

};

// Internal helper for parsing formats
const Parser = struct {
    source: []const u8,
    i: usize = 0,

    pub fn takeOne(self: *Parser) !u8 {
        if (self.i >= self.source.len) {
            return error.unexpected_end_of_input;
        }
        const ret = self.source[self.i];
        self.i += 1;
        return ret;
    }

    pub fn expect(self: *Parser, char: u8) !void {
        if ((try self.takeOne()) != char) {
            return error.unexpected_character;
        }
    }

    pub fn expectDigits(self: *Parser, comptime T: type, n: usize) !T {
        if (self.i + n > self.source.len) {
            return error.unexpected_end_of_input;
        }

        const window = self.source[self.i .. self.i + n];
        for (window) |c| {
            if (c < '0' or c > '9') {
                return error.unexpected_character;
            }
        }
        self.i += n;
        return std.fmt.parseInt(T, window, 10);
    }
};

const tm = extern struct {
    tm_sec:   i32,  // Seconds (0-60) */
    tm_min:   i32,  // Minutes (0-59) */
    tm_hour:  i32,  // Hours (0-23) */
    tm_mday:  i32,  // Day of the month (1-31) */
    tm_mon:   i32,  // Month (0-11) */
    tm_year:  i32,  // Year - 1900 */
    tm_wday:  i32,  // Day of the week (0-6, Sunday = 0) */
    tm_yday:  i32,  // Day in the year (0-365, 1 Jan = 0) */
    tm_isdst: i32,  // Daylight saving time */
};
extern fn time(tloc: ?*std.c.time_t) time_t;
extern fn asctime_s(buf: *u8, bufsz: usize, time_ptr: *const tm) errno_t;
extern fn gmtime_r(timep: *const std.c.time_t, result: ?*tm) ?*tm;
extern fn localtime_r(timep: *const std.c.time_t, result: ?*tm) ?*tm;

test "ISO 8601 (basic)" {
    const str_date = "20210902";
    const str_time = "T115312Z";
    const str = str_date ++ str_time;
    const dt = DateTime{
        .date = Date{
            .year = 2021,
            .month = 9,
            .day = 2,
        },
        .time = Time{
            .hour = 11,
            .minute = 53,
            .second = 12,
        }
    };
    try expectEqual(dt.date, try Iso8601Basic.parseDate(str_date));
    try expectEqual(dt.time, try Iso8601Basic.parseTime(str_time));
    try expectEqual(dt, try Iso8601Basic.parse(str));
    try expectEqualStrings(str_date, (try Iso8601Basic.printDate(dt.date))[0..]);
    try expectEqualStrings(str_time, (try Iso8601Basic.printTime(dt.time))[0..]);
    try expectEqualStrings(str, (try Iso8601Basic.print(dt))[0..]);
}

test "DateTime.gmtime" {
    const timestamp = 1631222375;
    const witness = DateTime{
        .date = Date{
            .year = 2021,
            .month = 9,
            .day = 9
        },
        .time = Time{
            .hour = 21, // 9pm
            .minute = 19,
            .second = 35
        }
    };
    const actual = try DateTime.gmtime(timestamp);
    std.debug.print("{s}", .{ try Iso8601Basic.print(actual) });
    try expectEqual(witness, actual);
}

test "DateTime.now" {
    const now = DateTime.now();
    std.debug.print("{s}", .{ try Iso8601Basic.print(now) });
}
