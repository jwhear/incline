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

        return try res.oneValue(T);
    }

    /// Executes a command, sending parameters separately from the query string.
    /// Prefer using this over `execFormat`
    pub fn execParams(self: *Database, command: [:0]const u8, args: anytype) !Result {
        const arg_fields = std.meta.fields(@TypeOf(args));

        // We need to coerce all args to null-terminated strings
        // Any allocations only need to survive to the end of this function, so
        //  we'll use an Arena to make it easy
        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();

        var values: [arg_fields.len]?[*:0]const u8 = undefined;
        inline for (arg_fields) |field,i| {
            const value = @field(args, field.name);
            values[i] = try formatValueAsZ(arena.allocator(), value);

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

    ///
    pub fn get(self: *const Result, row: i32, col: i32) []const u8 {
        const data_len = @intCast(usize, pq.PQgetlength(self.handle, row, col));
        const data = pq.PQgetvalue(self.handle, row, col);
        return data[0..data_len];
    }

    ///
    pub fn getAs(self: *const Result, comptime T: type, row: i32, col: i32) !T {
        return self.coerce(T, self.get(row, col), self.isNull(row, col));
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
    fn rowAsStruct(self: *const Result, comptime T: type, row: i32) !T {
        const fields = comptime std.meta.fields(T);
        if (fields.len != self.nFields) {
            return error.field_count_not_equal;
        }

        var ret: T = undefined;
        const useOrdering = comptime std.meta.trait.isTuple(T);
        inline for (fields) |f, i| {
            const column = if (useOrdering) @intCast(i32, i) else try self.fieldNumber(f.name);
            if (column < 0) {
                std.debug.print("Failed to find a field named '{s}' in the result set\n", .{f.name});
                return error.unmapped_field;
            }
            const rawValue = self.get(row, column);
            @field(ret, f.name) = try self.coerce(f.type, rawValue, self.isNull(row, column));
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

    ///
    pub fn coerce(self: *const Result, comptime T: type,
                  value: []const u8, is_null: bool) !T {

        // Handle string types simply
        if (comptime std.meta.trait.isZigString(T)) {
            return value;
        }

        const ti = comptime @typeInfo(T);
        switch (ti) {
            .Bool => return std.mem.eql(u8, value, "true"),
            .Int => |typ|
                return if (typ.signedness == .signed)
                        try std.fmt.parseInt(T, value, 10)
                       else
                        try std.fmt.parseUnsigned(T, value, 10),
            .Float => return try std.fmt.parseFloat(T, value),
            .Optional => |typ|
                return if (is_null) null else self.coerce(typ.child, value, false),

            .Enum => return std.meta.stringToEnum(T, value),
            else => |typ| {
                std.debug.print("Cannot coerce to type {s}\n", .{typ});
                return error.cannot_coerce;
            }
        }
    }

    /// Fills `values` with a column. `values.len` must be equal to the number of rows.
    pub fn fillColumnSlice(self: *const Result, comptime E: type, values: []E, column: i32) !void {
        for (values) |*v, i| {
            const row = @intCast(i32, i);
            v.* = try self.coerce(E, self.get(row, column), self.isNull(row, column));
        }
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
    if (@typeInfo(@TypeOf(value)) == .Optional) {
        if (value) |v| {
            return formatValueAsZ(allocator, v) catch return error.formatValueAsZError;
        } else {
            return null;
        }
    }

    if (@TypeOf(value) == [:0]const u8 or @TypeOf(value) == [:0]u8) {
        return value;
    }

    const fmt = comptime if (std.meta.trait.isZigString(@TypeOf(value))) "{s}" else "{}";

    return std.fmt.allocPrintZ(allocator, fmt, .{ value }) catch return error.formatValueAsZError;
}

