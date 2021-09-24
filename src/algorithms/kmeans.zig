const std = @import("std");

pub fn main() !void {
    var allocator = std.heap.c_allocator;

    const n_rows = 10_000;
    const n_features = 2;
    const n_centroids = 8;
    var r = &std.rand.DefaultPrng.init(0).random;

    var data = try allocator.alloc(f32, n_rows * n_features);
    for (data) |*v| {
        v.* = r.float(f32) * 100;
    }

    var kmeans = try KMeans(f32, u8, euclideanSquared).init(allocator, data, n_features, n_centroids);
    defer kmeans.deinit(allocator);
    std.debug.print("Converged? {}\n", kmeans.run());

    const writer = std.io.getStdOut().writer();
    try writer.print("x,y,label\n", .{});
    var strider = stride(data, 2);
    var row_i: usize = 0;
    while (strider.next()) |row| : (row_i += 1) {
        try writer.print("{d:.2},{d:.2},{}\n", .{ row[0], row[1], kmeans.labels[row_i] });
    }
}

///TODO use running mean instead (https://nullbuffer.com/articles/welford_algorithm.html)
///  might be able to get rid of the centroid_counts array?
///
/// The random number generator and/or seed can be controlled by setting/changing `.random`
pub fn KMeans(comptime DataT: type, comptime LabelT: type, comptime distance: fn([]DataT, []DataT) DataT) type {
    return struct {
        ///
        data: []DataT,
        ///
        n_rows: usize,
        ///
        n_features: usize,
        ///
        n_centroids: usize,

        ///
        random: *std.rand.Random,

        // Allocated fields
        /// Contains the current centroid label for each row in `data` [n_rows]
        labels: []LabelT,

        /// Contains the centroid points as a row-major matrix [n_centroids x n_features]
        centroid_array: []DataT,

        /// The number of points associated with each centroid [n_centroids]
        centroid_counts: []usize,

        /// Contains the minimum value for each feature [n_features]
        mins: []DataT,

        /// Contains the maximum value for each feature [n_features]
        maxs: []DataT,

        /// Working space for computing new centroid points as a row-major matrix [n_centroids x n_features]
        scratch_centroid_array: []DataT,


        const Self = @This();

        /// Initializes this algorithm without allocating any memory.  The caller
        ///  is responsible for allocating the following with the specified
        ///  types and lengths:
        ///
        ///  .labels: []LabelT (n_rows)
        ///  .centroid_array: []DataT (n_centroids * n_features)
        ///  .centroid_counts: []usize (n_centroids)
        ///  .mins: []DataT (n_features)
        ///  .maxs: []DataT (n_features)
        ///  .scratch_centroid_array: []DataT (n_centroids * n_features)
        ///
        /// If managing this memory yourself, do not call deinit()
        pub fn initNoAlloc(data: []DataT, n_features: usize, n_centroids: usize) Self {
            return Self{
                .data = data,
                .n_rows = data.len / n_features,
                .n_features = n_features,
                .n_centroids = n_centroids,
                .random = &std.rand.DefaultPrng.init(0).random,
                .labels = undefined,
                .centroid_array = undefined,
                .centroid_counts = undefined,
                .mins = undefined,
                .maxs = undefined,
                .scratch_centroid_array = undefined,
            };
        }

        /// Initializes this algorithm using the provided allocator.  Caller should
        ///  use `deinit()` with the same allocator to free when done.
        pub fn init(allocator: *std.mem.Allocator, data: []DataT, n_features: usize, n_centroids: usize) !Self {
            var me = Self.initNoAlloc(data, n_features, n_centroids);
            me.labels = try allocator.alloc(LabelT, me.n_rows);
            me.centroid_array = try allocator.alloc(DataT, me.n_centroids * me.n_features);
            me.centroid_counts = try allocator.alloc(usize, me.n_centroids);
            me.mins = try allocator.alloc(DataT, me.n_features);
            me.maxs = try allocator.alloc(DataT, me.n_features);
            me.scratch_centroid_array = try allocator.alloc(DataT, me.n_centroids * me.n_features);
            return me;
        }

        ///
        pub fn deinit(self: *Self, allocator: *std.mem.Allocator) void {
            allocator.free(self.labels);
            allocator.free(self.centroid_array);
            allocator.free(self.centroid_counts);
            allocator.free(self.mins);
            allocator.free(self.maxs);
            allocator.free(self.scratch_centroid_array);
        }

        /// Populates .mins and .maxs with bounds information for all features
        pub fn computeBounds(self: *Self) void {
            std.debug.assert(self.mins.len == self.n_features);
            std.debug.assert(self.maxs.len == self.n_features);

            var rows = stride(self.data, self.n_features);

            // prime with the first row of data
            if (rows.next()) |row| {
                std.mem.copy(DataT, self.mins, row);
                std.mem.copy(DataT, self.maxs, row);
            }

            while (rows.next()) |row| {
                for (row) |v, feature_i| {
                    self.mins[feature_i] = std.math.min(v, self.mins[feature_i]);
                    self.maxs[feature_i] = std.math.max(v, self.maxs[feature_i]);
                }
            }
        }

        /// Initializes the centroids array with random points within the bounds
        pub fn initializeCentroids(self: *Self) void {
            std.debug.assert(self.centroid_array.len == self.n_features * self.n_centroids);

            var centroids = stride(self.centroid_array, self.n_features);
            while (centroids.next()) |centroid| {
                self.randomCentroid(centroid);
            }
        }

        fn randomCentroid(self: *Self, out: []DataT) void {
            for (out) |*v, feature_i| {
                const scale = self.maxs[feature_i] - self.mins[feature_i];
                v.* = self.random.float(DataT) * scale + self.mins[feature_i];
            }
        }

        /// Performs a single iteration of the kmeans algorithm returning false
        ///  if the algorithm has converged and will no longer be altered by
        ///  additional calls to this function.
        pub fn iterate(self: *Self) bool {
            var converged: bool = true;

            // Clear our centroid working space
            std.mem.set(DataT, self.scratch_centroid_array, 0);
            std.mem.set(usize, self.centroid_counts, 0);
            var new_centroids = stride(self.scratch_centroid_array, self.n_features);

            // Label each row, calculating new centroid points as we go
            var rows = stride(self.data, self.n_features);
            var row_i: usize = 0;
            while (rows.next()) |row| : (row_i += 1) {

                // Compute dist to each centroid, pick closest
                var centroids = stride(self.centroid_array, self.n_features);
                // prime with the first centroid
                var nearest: LabelT = 0;
                var nearest_dist = distance(centroids.i(0), row);
                // now consider the others
                centroids.set(1);
                var i: LabelT = 1;
                while (centroids.next()) |centroid| : (i += 1) {
                    const d = distance(centroid, row);
                    if (d < nearest_dist) {
                        nearest = i;
                        nearest_dist = d;
                    }
                }

                // Does this point need to change label?
                if (self.labels[row_i] != nearest) {
                    converged = false;
                    self.labels[row_i] = nearest;
                }

                // Add this point to the accumulator for this centroid
                // TODO: Welford's algorithm
                for (new_centroids.i(nearest)) |*v, feature_i| {
                    v.* += row[feature_i];
                }
                self.centroid_counts[nearest] += 1;
            }

            // Generate new centroids such that they are in the center of the
            //  points with their label
            for (self.centroid_counts) |count, centroid_i| {
                if (count == 0) {
                    // unused centroid, reinit to a random point
                    self.randomCentroid(new_centroids.i(centroid_i));
                } else {
                    // average the accumulator
                    for (new_centroids.i(centroid_i)) |*v| {
                        v.* /= @intToFloat(DataT, count);
                    }
                }

                // Copy the new centroids into the main buffer
                std.mem.copy(DataT, self.centroid_array, self.scratch_centroid_array);
            }

            return !converged;
        }

        /// Iterates until `max_iterations` or convergence are reached.  Returns
        ///  true if the algorithm converged.
        pub fn run(self: *Self, max_iterations: usize) bool {
            var iteration: u64 = 0;
            while (iteration < max_iterations) : (iteration += 1) {
                if (!self.iterate()) return true;
            }
            return false;  // failed to converge
        }
    };

}

/// Returns the square of the Euclidean distance between a and b.
pub fn euclideanSquared(a: anytype, b: anytype) f32 {
    std.debug.assert(a.len == b.len);

    const E = @TypeOf(a[0]);
    var accum: E = 0;
    for (a) |av, i| {
        accum += std.math.pow(E, av - b[i], 2);
    }
    return accum;
}

fn Stride(comptime Slice: type) type {
    return struct{
        const Self = @This();

        data: Slice, // should be evenly divisible by `s`
        index: usize,  // current index
        s: usize,  // stride

        pub fn next(self: *Self) ?Slice {
            if (self.index >= self.data.len) return null;
            var ret = self.data[self.index .. self.index+self.s];
            self.index += self.s;
            return ret;
        }

        pub fn len(self: *const Self) usize {
            return self.data.len / self.s;
        }

        pub fn reset(self: *Self) void {
            self.index = 0;
        }

        pub fn set(self: *Self, idx: usize) void {
            self.index = idx * self.s;
        }

        pub fn i(self: *const Self, idx: usize) Slice {
            const index = idx * self.s;
            return self.data[index .. index + self.s];
        }
    };
}

fn stride(data: anytype, by: usize) Stride(@TypeOf(data)) {
    return Stride(@TypeOf(data)){
        .data = data,
        .index = 0,
        .s = by,
    };
}
