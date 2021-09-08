usingnamespace @cImport({
    @cInclude("postgresql/libpq-fe.h");
});

const std = @import("std");

///
pub const Database = struct {
    conn: *PGconn = undefined,
    allocator: *std.mem.Allocator,
    show_queries: bool = false,

    ///
    pub fn connect(info: []const u8, allocator: *std.mem.Allocator) !Database {
        var conn = PQconnectdb(info.ptr);
        if (PQstatus(conn) != CONNECTION_OK) {
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
                          allocator: *std.mem.Allocator) !Database {
        var buf: [1024]u8 = undefined;
        const infoFmt = "postgresql://{[user]s}:{[password]s}@{[host]s}:{[port]}/{[database]s}";
        const info = try std.fmt.bufPrintZ(buf[0..], infoFmt, profile);

        return Database.connect(info, allocator);
    }

    ///
    pub fn finish(self: *Database) void {
        PQfinish(self.conn);
    }

    ///
    pub fn exec(self: *Database, command: [:0]const u8) !Result {
        if (self.show_queries) {
            std.debug.print("QUERY: {s}\n", .{command});
        }
        return Result.init(PQexec(self.conn, command.ptr));
    }

    pub fn execNoResult(self: *Database, command: [:0]const u8) !void {
        if (self.show_queries) {
            std.debug.print("QUERY: {s}\n", .{command});
        }
        // We'll instantiate a Result because that's where the error handling
        //  takes place
        var res = try Result.init(PQexec(self.conn, command.ptr));
        res.clear();
    }

    ///
    pub fn execFormat(self: *Database, comptime command_fmt: []const u8, args: anytype) !Result {
        const command = try std.fmt.allocPrintZ(self.allocator, command_fmt, args);
        defer self.allocator.free(command);
        return self.exec(command);
    }

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
};

///
pub const Result = struct {
    handle: *PGresult,

    ///
    pub fn init(h: ?*PGresult) !Result {
        switch (PQresultStatus(h)) {
            PGRES_EMPTY_QUERY, PGRES_COMMAND_OK, PGRES_TUPLES_OK,
            PGRES_COPY_OUT, PGRES_COPY_IN, PGRES_COPY_BOTH,
            PGRES_NONFATAL_ERROR, PGRES_SINGLE_TUPLE =>
                return Result{ .handle=h orelse return error.postgres_query_error },

            else => {
                std.debug.print("Query error:\n{s}",
                               .{PQresultErrorMessage(h)});
                PQclear(h);
                return error.postgres_query_error;
            }
        }
    }

    ///
    pub fn clear(self: *Result) void {
        PQclear(self.handle);
    }

    ///
    pub fn hasData(self: *const Result) bool {
        return switch (PQresultStatus(self.handle)) {
            PGRES_TUPLES_OK, PGRES_SINGLE_TUPLE => true,
            else => false,
        };
    }

    ///
    pub fn len(self: *const Result) i32 {
        return PQntuples(self.handle);
    }

    ///
    pub fn nFields(self: *const Result) i32 {
        return PQnfields(self.handle);
    }

    ///
    pub fn get(self: *const Result, row: i32, col: i32) []const u8 {
        const data_len = @intCast(usize, PQgetlength(self.handle, row, col));
        const data = PQgetvalue(self.handle, row, col);
        return data[0..data_len];
    }

    ///
    pub fn oneValue(self: *const Result, comptime T: type) !T {
        if (self.len() != 1 or self.nFields() != 1) {
            return error.expected_one_value;
        }

        return self.coerce(T, self.get(0, 0), self.isNull(0, 0));
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
        return PQgetisnull(self.handle, row, col) == 1;
    }

    ///
    pub fn coerce(self: *const Result, comptime T: type,
                  value: []const u8, is_null: bool) T {

        // Handle string types simply
        if (std.meta.trait.isZigString(T)) {
            return value;
        }

        const ti = comptime @typeInfo(T);
        switch (ti) {
            .Bool => return std.mem.eql(u8, value, "true"),
            .Int => |typ|
                return if (typ.signedness == .signed)
                        std.fmt.parseInt(T, value, 10)
                       else
                        std.fmt.parseUnsigned(T, value, 10),
            .Float => return std.fmt.parseFloat(T, value),
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
