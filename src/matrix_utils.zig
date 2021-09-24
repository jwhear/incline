const std = @import("std");

/// Produces a function that takes vectors of T and returns the square of the
///  Euclidean distance between a and b.
pub fn EuclideanSquared(comptime T: type) fn([]const T, []const T) T {
    return struct {
        // may be faster than std.math.pow
        fn square(v: T) callconv(.Inline) T {
            return v * v;
        }

        pub fn f(a: []const T, b: []const T) T {
            std.debug.assert(a.len == b.len);

            var accum: T = 0;
            for (a) |av, i| {
                accum += square(av - b[i]);
            }
            return accum;
        }
    }.f;
}

///
pub fn Stride(comptime Slice: type) type {
    return struct{
        const Self = @This();

        data: Slice, // should be evenly divisible by `s`
        index: usize,  // current index
        s: usize,  // stride

        ///
        pub fn next(self: *Self) ?Slice {
            if (self.index >= self.data.len) return null;
            var ret = self.data[self.index .. self.index+self.s];
            self.index += self.s;
            return ret;
        }

        ///
        pub fn len(self: *const Self) usize {
            return self.data.len / self.s;
        }

        /// Reset the iterator to the beginning of `data`
        pub fn reset(self: *Self) void {
            self.index = 0;
        }

        /// Set the current row index to `idx`
        pub fn set(self: *Self, idx: usize) void {
            self.index = idx * self.s;
        }

        /// Randomly access the ith row of `data`.
        pub fn i(self: *const Self, idx: usize) Slice {
            const index = idx * self.s;
            return self.data[index .. index + self.s];
        }
    };
}

/// Induces a iterable sliding window over `data`.
/// `data.len` must be evenly divisible by `by`
pub fn stride(data: anytype, by: usize) Stride(@TypeOf(data)) {
    std.debug.assert(data.len % by == 0);
    return Stride(@TypeOf(data)){
        .data = data,
        .index = 0,
        .s = by,
    };
}
