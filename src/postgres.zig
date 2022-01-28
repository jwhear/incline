const pq = @cImport({
    @cInclude("postgresql/libpq-fe.h");
});

const std = @import("std");

///
pub const Database = struct {
    conn: *pq.PGconn = undefined,
    allocator: std.mem.Allocator,
    show_queries: bool = false,

    ///
    pub fn connect(info: []const u8, allocator: std.mem.Allocator) !Database {
        var conn = pq.PQconnectdb(info.ptr);
        if (pq.PQstatus(conn) != pq.CONNECTION_OK) {
            //std.debug.print("Failed to connect: \n{s}",
                            //.{ PQerrorMessage(conn) });
            return error.postgres_connect_failed;
        }
        var ret = Database{
            .conn = conn orelse return error.postgres_connect_failed,
            .allocator = allocator,
        };

        // Set a secure search path to prevent malicious use
        (
            ret.exec("SELECT pg_catalog.set_config('search_path', '', false)")
             catch return error.postgres_failed_set_search_path
        ).clear();

        return ret;
    }

    ///
    pub fn connectProfile(profile: anytype,
                          allocator: std.mem.Allocator) !Database {
        var buf: [1024]u8 = undefined;
        const infoFmt = "postgresql://{[user]s}:{[password]s}@{[host]s}:{[port]}/{[database]s}";
        const info = try std.fmt.bufPrintZ(buf[0..], infoFmt, profile);

        return Database.connect(info, allocator);
    }

    ///
    pub fn finish(self: *Database) void {
        pq.PQfinish(self.conn);
    }

    ///
    pub fn exec(self: *Database, command: [:0]const u8) !Result {
        if (self.show_queries) {
            std.debug.print("QUERY: {s}\n", .{command});
        }
        return Result.init(pq.PQexec(self.conn, command.ptr));
    }

    pub fn execNoResult(self: *Database, command: [:0]const u8) !void {
        if (self.show_queries) {
            std.debug.print("QUERY: {s}\n", .{command});
        }
        // We'll instantiate a Result because that's where the error handling
        //  takes place
        var res = try Result.init(pq.PQexec(self.conn, command.ptr));
        res.clear();
    }

    ///
    pub fn execFormat(self: *Database, comptime command_fmt: []const u8, args: anytype) !Result {
        const command = try std.fmt.allocPrintZ(self.allocator, command_fmt, args);
        defer self.allocator.free(command);
        return self.exec(command);
    }

    /// Executes command, expects a single field in the result, coerces the result
    ///  to T, returns the value and clear the internal resultset.
    pub fn execGetOne(self: *Database, comptime T: type, command: [:0]const u8) !T {
        if (self.show_queries) {
            std.debug.print("QUERY: {s}\n", .{command});
        }
        var res = try Result.init(pq.PQexec(self.conn, command.ptr));
        defer res.clear();

        // If a string, make sure we copy before clearing the result
        return if (std.meta.trait.isZigString(T))
                    try self.allocator.dupe(u8, try res.oneValue(T))
               else try res.oneValue(T);
    }

    /// Executes a command, sending parameters separately from the query string.
    /// Prefer using this over `execFormat`
    pub fn execParams(self: *Database, command: [:0]const u8, args: anytype) !Result {
        if (self.show_queries) {
            std.debug.print("QUERY: {s}\n", .{command});
        }

        const arg_fields = std.meta.fields(@TypeOf(args));

        // We need to coerce all args to null-terminated strings
        // Any allocations only need to survive to the end of this function, so
        //  we'll use an Arena to make it easy
        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();

        var allocator = arena.allocator();

        var values: [arg_fields.len]?[*:0]const u8 = undefined;
        inline for (arg_fields) |field,i| {
            const value = @field(args, field.name);
            values[i] = try formatValueAsZ(allocator, value);
        }

        return Result.init(
            pq.PQexecParams(self.conn,
                command.ptr,
                values.len,
                null, // types
                &values[0],
                null, // lengths
                null, // formats
                0
            )
        );
    }

    ///
    pub fn execParamsNoResult(self: *Database, command: [:0]const u8, args: anytype) !void {
        // We'll instantiate a Result because that's where the error handling
        //  takes place
        var res = try self.execParams(command, args);
        res.clear();
    }

    //TODO prepared statements?

    ///
    pub fn truncate(self: *Database, table_name: []const u8) !void {
        (try self.execFormat("TRUNCATE TABLE {s};", .{table_name})).clear();
    }

    ///
    pub fn createTable(self: *Database, tableName: []const u8,
                       columns: anytype) !void {
        var builder = std.ArrayList(u8).init(self.allocator);
        defer builder.deinit();
        var writer = builder.writer();

        _ = try writer.write("CREATE TABLE ");
        _ = try writer.write(tableName);
        _ = try writer.write("(\n");
        var cont = false;
        for (columns) |col| {
            if (cont) {
                _ = try writer.write(",");
            } else {
                cont = true;
            }
            _ = try writer.print("\t\"{[name]s}\" {[data_type]s}\n", col);
        }
        _ = try writer.write(")");

        const query = try builder.toOwnedSliceSentinel(0);
        defer self.allocator.free(query);
        (try self.exec(query)).clear();
    }

    ///
    pub fn tableExists(self: *Database, name: []const u8) !bool {
        const dot    = std.mem.indexOfScalar(u8, name, '.');
        const schema = if (dot)|i| name[0..i] else "";
        const table  = if (dot)|i| name[i+1..] else name;

        const fmt =
        \\SELECT 1 FROM information_schema.tables
        \\WHERE table_schema = '{s}' AND
        \\      table_name = '{s}';
        ;
        var res = try self.execFormat(fmt, .{schema, table});
        defer res.clear();
        return res.len() > 0;
    }

    /// Locates all indices on `table_name` and drops them.
    /// This is useful before bulk inserts.  Note that you do not need to
    ///  do this if DROPing the table--Postgres automatically drops indices
    ///  of dropped tables.
    pub fn dropIndicesOn(self: *Database, table_name: []const u8) !void {
        const indices = try self.execFormat(
            \\SELECT cls.relname
            \\FROM pg_index idx
            \\JOIN pg_class cls ON cls.oid=idx.indexrelid
            \\JOIN pg_class tab ON tab.oid=idx.indrelid
            \\JOIN pg_am am ON am.oid=cls.relam
            \\WHERE tab.relname = '{s}';
            , .{ table_name }
        );
        defer indices.clear();

        var i: i32 = indices.len();
        while (i > 0) : (i += 1) {
            try self.execFormat("DROP INDEX {s};", .{ indices.get(i, 1) });
        }
    }
};

///
pub const Result = struct {
    handle: *pq.PGresult,

    ///
    pub fn init(h: ?*pq.PGresult) !Result {
        switch (pq.PQresultStatus(h)) {
            pq.PGRES_EMPTY_QUERY, pq.PGRES_COMMAND_OK, pq.PGRES_TUPLES_OK,
            pq.PGRES_COPY_OUT, pq.PGRES_COPY_IN, pq.PGRES_COPY_BOTH,
            pq.PGRES_NONFATAL_ERROR, pq.PGRES_SINGLE_TUPLE =>
                return Result{ .handle=h orelse return error.postgres_query_error },

            else => {
                std.debug.print("Query error:\n{s}",
                               .{pq.PQresultErrorMessage(h)});
                pq.PQclear(h);
                return error.postgres_query_error;
            }
        }
    }

    ///
    pub fn clear(self: *Result) void {
        pq.PQclear(self.handle);
    }

    ///
    pub fn hasData(self: *const Result) bool {
        return switch (pq.PQresultStatus(self.handle)) {
            pq.PGRES_TUPLES_OK, pq.PGRES_SINGLE_TUPLE => true,
            else => false,
        };
    }

    ///
    pub fn len(self: *const Result) i32 {
        return pq.PQntuples(self.handle);
    }

    ///
    pub fn nFields(self: *const Result) i32 {
        return pq.PQnfields(self.handle);
    }

    /// Returns the number of the field or -1 if not present
    pub fn fieldNumber(self: *const Result, name: [:0]const u8) i32 {
        const ret = pq.PQfnumber(self.handle, name.ptr);
        return ret;
    }

    /// Returns the name of the field or null if the number is out of range
    pub fn fieldName(self: *const Result, n: i32) ?[:0]const u8 {
        if (pq.PQfname(self.handle, n)) |name| {
            return std.mem.span(name);
        }
        return null;
    }

    /// Returns an iterator that gives the field names
    pub fn fieldNames(self: *const Result) FieldNameIterator {
        return FieldNameIterator{ .res=self };
    }

    ///
    pub fn get(self: *const Result, row: i32, col: i32) []const u8 {
        const data_len = @intCast(usize, pq.PQgetlength(self.handle, row, col));
        const data = pq.PQgetvalue(self.handle, row, col);
        return data[0..data_len];
    }

    ///
    pub fn getCopy(self: *const Result, allocator: std.mem.Allocator, row: i32, col: i32) ![] u8 {
        const data_len = @intCast(usize, pq.PQgetlength(self.handle, row, col));
        const data = pq.PQgetvalue(self.handle, row, col);
        return try allocator.dupe(u8, data[0..data_len]);
    }

    ///
    pub fn getAs(self: *const Result, comptime T: type, row: i32, col: i32) !T {
        return self.coerce(T, self.get(row, col), self.isNull(row, col));
    }

    ///
    pub fn getCopyAs(self: *const Result, allocator: std.mem.Allocator, comptime T: type, row: i32, col: i32) !T {
        return self.coerce(T, try self.getCopy(allocator, row, col), self.isNull(row, col));
    }


    ///
    pub fn getField(self: *const Result, row: i32, field: [:0]const u8) ![]const u8 {
        const n = self.fieldNumber(field);
        if (n < 0) return error.invalid_field_name;
        return self.get(row, n);
    }

    ///
    pub fn getFieldAs(self: *const Result, comptime T: type, row: i32, field: [:0]const u8) !T {
        const n = self.fieldNumber(field);
        if (n < 0) return error.invalid_field_name;
        return self.getAs(T, row, n);
    }

    ///
    pub fn getCopyFieldAs(self: *const Result, allocator: std.mem.Allocator, comptime T: type, row: i32, field: [:0]const u8) !T {
        const n = self.fieldNumber(field);
        if (n < 0) return error.invalid_field_name;
        return self.getCopyAs(allocator, T, row, n);
    }

    ///
    pub fn oneValue(self: *const Result, comptime T: type) !T {
        if (self.len() != 1 or self.nFields() != 1) {
            return error.expected_one_value;
        }

        return self.getAs(T, 0, 0);
    }

    ///
    pub fn rowAs(self: *const Result, comptime T: type, row: i32) !T {
        return switch (@typeInfo(T)) {
            .Struct => self.rowAsStruct(T, row),
            .Array => self.rowAsArray(T, row),
            else => @compileError("Cannot coerce a row to a "++@typeName(T)),
        };
    }

    ///
    pub fn rowCopyAs(self: *const Result, allocator: std.mem.Allocator, comptime T: type, row: i32) !T {
        return switch (@typeInfo(T)) {
            .Struct => self.rowCopyAsStruct(allocator, T, row),
            .Array => self.rowCopyAsArray(allocator, T, row),
            else => @compileError("Cannot coerce a row to a "++@typeName(T)),
        };
    }

    ///
    fn rowAsArray(self: *const Result, comptime T: type, row: i32) !T {
        const TI = @typeInfo(T);
        if (TI.Array.len != self.nFields) {
            return error.field_count_not_equal;
        }
        const E = TI.Array.child;

        var ret: T = undefined;
        for (ret) |*v, col| {
            const rawValue = self.get(row, col);
            v.* = try self.coerce(E, rawValue, self.isNull(row, col));
        }
        return ret;
    }

    ///
    fn rowCopyAsArray(self: *const Result, allocator: std.mem.Allocator, comptime T: type, row: i32) !T {
        const TI = @typeInfo(T);
        if (TI.Array.len != self.nFields) {
            return error.field_count_not_equal;
        }
        const E = TI.Array.child;

        var ret: T = undefined;
        for (ret) |*v, col| {
            const rawValue = self.getCopy(allocator, row, col);
            v.* = try self.coerce(E, rawValue, self.isNull(row, col));
        }
        return ret;
    }

    ///
    fn rowAsStruct(self: *const Result, comptime T: type, row: i32) !T {
        const fields = comptime std.meta.fields(T);
        var ret: T = undefined;

        // If a tuple type we need to simply use ordering
        if (comptime std.meta.trait.isTuple(T)) {
            if (fields.len != self.nFields()) {
                return error.field_count_not_equal;
            }

            inline for (fields) |f, i| {
                @field(ret, f.name) = try self.getAs(f.field_type, row, @as(i32, i));
            }
        } else {
            // Otherwise use name to associate
            inline for (fields) |f| {
                // We need field name as a null-terminated string for lookup
                var fz: [f.name.len:0]u8 = std.mem.zeroes([f.name.len:0]u8);
                std.mem.copy(u8, &fz, f.name);
                @field(ret, f.name) = try self.getFieldAs(f.field_type, row, &fz);
            }
        }
        return ret;
    }

    ///
    fn rowCopyAsStruct(self: *const Result, allocator: std.mem.Allocator, comptime T: type, row: i32) !T {
        const fields = comptime std.meta.fields(T);
        var ret: T = undefined;

        // If a tuple type we need to simply use ordering
        if (comptime std.meta.trait.isTuple(T)) {
            if (fields.len != self.nFields()) {
                return error.field_count_not_equal;
            }

            inline for (fields) |f, i| {
                @field(ret, f.name) = try self.getCopyAs(allocator, f.field_type, row, @as(i32, i));
            }
        } else {
            // Otherwise use name to associate
            inline for (fields) |f| {
                // We need field name as a null-terminated string for lookup
                var fz: [f.name.len:0]u8 = std.mem.zeroes([f.name.len:0]u8);
                std.mem.copy(u8, &fz, f.name);
                @field(ret, f.name) = try self.getCopyFieldAs(allocator, f.field_type, row, &fz);
            }
        }
        return ret;
    }

    /// Takes a slice of `E`s and fills it from the specified row.
    /// `values.len` must be equal to the number of fields in the result.
    pub fn fill(self: *const Result, comptime E: type, values: []E, row: i32) !void {
        if (values.len != self.nFields) {
            return error.field_count_not_equal;
        }

        for (values) |*v, i| {
            const column = @intCast(i32, i);
            v.* = try self.coerce(E, self.get(row, column), self.isNull(row, column));
        }
    }

    ///
    pub fn isNull(self: *const Result, row: i32, col: i32) bool {
        return pq.PQgetisnull(self.handle, row, col) == 1;
    }

    pub const CoerceError = error {
        cannot_coerce
    };

    ///
    pub fn coerce(self: *const Result, comptime T: type,
                  value: []const u8, is_null: bool) CoerceError!T {

        // Handle string types simply
        if (comptime std.meta.trait.isZigString(T)) {
            return value;
        }

        const ti = comptime @typeInfo(T);
        return switch (ti) {
            .Bool => return std.mem.eql(u8, value, "true"),
            .Int => |typ|
                if (typ.signedness == .signed)
                    std.fmt.parseInt(T, value, 10) catch error.cannot_coerce
                else
                    std.fmt.parseUnsigned(T, value, 10) catch error.cannot_coerce,
            .Float => try std.fmt.parseFloat(T, value),
            .Optional => |typ|
                if (is_null) null else self.coerce(typ.child, value, false),

            .Enum => std.meta.stringToEnum(T, value) orelse error.cannot_coerce,
            else =>  @compileError("Cannot coerce to type "++@typeName(T))
        } catch {
            return error.cannot_coerce;
        };
    }

    /// Fills `values` with a column. `values.len` must be equal to the number of rows.
    pub fn fillColumnSlice(self: *const Result, comptime E: type, values: []E, column: i32) !void {
        for (values) |*v, i| {
            const row = @intCast(i32, i);
            v.* = try self.coerce(E, self.get(row, column), self.isNull(row, column));
        }
    }

    /// Returns a view of this Result with iteration and where each row is
    ///  coerced to a `Row`
    pub fn typed(self: *const Result, comptime Row: type) TypedResult(Row) {
        return TypedResult(Row){ .result=self };
    }

};

pub const Column = struct {
    name: []const u8,
    data_type: []const u8 = "text",
};

pub const FormatValueAsZError = error { formatValueAsZError };

/// Formats the supplied value as a suitable literal for Postgres
///   null values    => "null"
///   [:0]const u8   => returned untouched
///   [:0]u8         => returned untouched
///   all other types => allocPrintZ()
pub fn formatValueAsZ(allocator: std.mem.Allocator, value: anytype) FormatValueAsZError!?[*:0]const u8 {
    const T = @TypeOf(value);
    const TI = @typeInfo(T);

    if (TI == .Optional) {
        if (value) |v| {
            return formatValueAsZ(allocator, v) catch return error.formatValueAsZError;
        } else {
            return null;
        }
    }

    if (T == [:0]const u8 or T == [:0]u8) {
        return value;
    }

    if (TI == .Enum) {
        return @tagName(value);
    }

    const fmt = comptime if (std.meta.trait.isZigString(T)) "{s}" else "{}";

    return std.fmt.allocPrintZ(allocator, fmt, .{ value }) catch return error.formatValueAsZError;
}

///
pub const FieldNameIterator = struct {
    res: *const Result,
    i: i32 = 0,

    pub fn next(self: *FieldNameIterator) ?[:0]const u8 {
        defer self.i += 1;
        return self.res.fieldName(self.i);
    }
};

///
pub fn TypedResult(comptime Row: type) type {
    return struct {
        const Self = @This();

        result: *const Result,
        row_i: i32 = 0,

        ///
        pub fn next(self: *Self) !?Row {
            if (self.row_i >= self.result.len()) return null;
            defer self.row_i += 1;
            return try self.result.rowAs(Row, self.row_i);
        }
    };
}
