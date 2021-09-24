const std = @import("std");
const utils = @import("../matrix_utils.zig");

/// Algorithm for computing centroids of a dataset and point membership of
///  those centroids.
/// `DataT` is the type of the elements of the dataset and should be a real number type.
/// `LabelT` is the type of the labels and should be a integer type large enough
///   to hold the number of desired centroids.
/// `distance` is a function taking two vectors of `DataT` and returning the
///   distance (also of type DataT).
///
/// The type returned should be initialized with either `init` or `initNoAlloc`.
/// If using `initNoAlloc`, the caller must allocate certain arrays (see the
///  documentation for (`initNoAlloc`) and should _not_ call `deinit`.
/// If using `init`, the provided allocator will be used and a matching call
///  to `deinit` should be performed when the computation is done and the
///  results are no longer needed.  Note that this type performs no heap allocations
///  outside of `init`.
///
/// This type uses a number of matrices, all of which are row-major:
///  * .data is a [n_rows x n_features] and contains the input data points.
///  * .labels is a vector [n_rows] and contains the current label for each
///      point in `data`.
///  * .centroid_array is a matrix [n_centroids x n_features] and contains
///      the current point for each centroid.
///  * .centroid_counts is a vector [n_centroids] and contains the current
///      number of points labeled with that centroid.
///  * .scratch_centroid_array is a working space buffer.
///
/// General usage:
/// ```
/// var kmeans = try KMeans(f32, u8, comptime EuclideanSquared(f32))
///                    .init(allocator, random, data, n_features, n_centroids);
/// const converged: bool = kmeans.run(1_000);
/// // Use kmeans.labels, kmeans.centroid_array, and kmeans.centroid_counts...
/// kmeans.deinit(allocator);
/// ```
///
/// By reifying this algorithm to a structure and separating the various
///  stages, the user can customize one or more stages.  Here's an example of
///  more customized usage:
/// ```
/// var kmeans = try KMeans(f32, u8, comptime EuclideanSquared(f32))
///                    .initNoAlloc(random, data, n_features, n_centroids);
///
/// // Provide these arrays however you like (e.g. reusing previous allocations)
/// kmeans.labels = try myAllocator.alloc(u8, n_rows);
/// kmeans.centroid_array = try myAllocator.alloc(DataT, n_centroids * n_features);
/// kmeans.centroid_counts = myAllocator.alloc(usize, n_centroids);
/// kmeans.scratch_centroid_array = myAllocator.alloc(DataT, n_centroids * n_features);
///
/// // The built-in method picks random points from `data` for the initial centroids
/// //  but you could provide them yourself using some fancy technique or other
/// if (fancy_centroids) {
///     kmeans.centroid_array = getSomeFancyCentroids();
///     kmeans.centroids_are_initialized = true;
/// } else {
///     kmeans.initializeCentroids();
/// }
///
/// // The `run` method iterates until the max iterations parameter is reached or
/// //  the algorithm converges.  You can take control of this by using the
/// //  `iterate` method to perform a single iteration at a time.  It returns
/// //  true if more iterations are needed for convergence.
/// while (kmeans.iterate()) {
///    // print out intermediate results, tinker with the internal state, etc.
/// }
/// ```
///
/// Numeric stability:
/// This implementation uses the Welford method for computing the new centroids
///  during the single pass (per iteration) over the data.  This should be much
///  more robust against precision loss than the naive method.
pub fn KMeans(comptime DataT: type, comptime LabelT: type, comptime distance: fn([]DataT, []DataT) DataT) type {
    return struct {
        /// The input data array [n_rows x n_features]
        data: []DataT,
        /// The number of rows in `data`
        n_rows: usize,
        /// The number of features (columns) in `data`
        n_features: usize,
        /// The desired number of centroids
        n_centroids: usize,

        /// Only used for picking initial centroids or replacing defunct ones
        random: *std.rand.Random,

        // Allocated fields
        /// Contains the current centroid label for each row in `data` [n_rows]
        labels: []LabelT,

        /// Contains the centroid points as a row-major matrix [n_centroids x n_features]
        centroid_array: []DataT,

        /// The number of points associated with each centroid [n_centroids]
        centroid_counts: []usize,

        /// Working space for computing new centroid points as a row-major matrix [n_centroids x n_features]
        scratch_centroid_array: []DataT,

        /// Whether the centroids have been initialized
        centroids_are_initialized: bool = false,


        const Self = @This();

        /// Initializes this algorithm without allocating any memory.  The caller
        ///  is responsible for allocating the following with the specified
        ///  types and lengths:
        ///
        ///  .labels: []LabelT (n_rows)
        ///  .centroid_array: []DataT (n_centroids * n_features)
        ///  .centroid_counts: []usize (n_centroids)
        ///  .scratch_centroid_array: []DataT (n_centroids * n_features)
        ///
        /// If managing this memory yourself, do not call deinit()
        pub fn initNoAlloc(random: *std.rand.Random,data: []DataT,
                           n_features: usize, n_centroids: usize) Self {
            return Self{
                .data = data,
                .n_rows = data.len / n_features,
                .n_features = n_features,
                .n_centroids = n_centroids,
                .random = random,
                .labels = undefined,
                .centroid_array = undefined,
                .centroid_counts = undefined,
                .scratch_centroid_array = undefined,
            };
        }

        /// Initializes this algorithm using the provided allocator.  Caller should
        ///  use `deinit()` with the same allocator to free when done.
        pub fn init(allocator: *std.mem.Allocator, random: *std.rand.Random,
                    data: []DataT, n_features: usize, n_centroids: usize) !Self {
            var me = Self.initNoAlloc(random, data, n_features, n_centroids);
            me.labels                 = try allocator.alloc(LabelT, me.n_rows);
            me.centroid_array         = try allocator.alloc(DataT, me.n_centroids * me.n_features);
            me.centroid_counts        = try allocator.alloc(usize, me.n_centroids);
            me.scratch_centroid_array = try allocator.alloc(DataT, me.n_centroids * me.n_features);
            return me;
        }

        ///
        pub fn deinit(self: *Self, allocator: *std.mem.Allocator) void {
            allocator.free(self.labels);
            allocator.free(self.centroid_array);
            allocator.free(self.centroid_counts);
            allocator.free(self.scratch_centroid_array);
        }

        /// Initializes the centroids array with random points from data
        pub fn initializeCentroids(self: *Self) void {
            var centroids = utils.stride(self.centroid_array, self.n_features);
            while (centroids.next()) |centroid| {
                self.randomCentroid(centroid);
            }
            self.centroids_are_initialized = true;
        }

        // Picks a random point from `data` and writes it to `out`
        fn randomCentroid(self: *Self, out: []DataT) void {
            std.debug.assert(out.len == self.n_features);
            const i = self.random.uintLessThan(usize, self.n_rows);
            const row = utils.stride(self.data, self.n_features).i(i);
            std.mem.copy(DataT, out, row);
        }

        /// Performs a single iteration of the kmeans algorithm.
        /// Returns false if the algorithm has converged and will no longer be
        ///  altered by additional calls to this function.
        pub fn iterate(self: *Self) bool {

            if (!self.centroids_are_initialized)
                self.initializeCentroids();

            // Assume converged; if a point's label changes, set to false
            var converged: bool = true;

            // Clear the centroid working space
            std.mem.set(DataT, self.scratch_centroid_array, 0);
            std.mem.set(usize, self.centroid_counts, 0);
            var new_centroids = utils.stride(self.scratch_centroid_array, self.n_features);

            // Label each row, calculating new centroid points as we go
            var rows = utils.stride(self.data, self.n_features);
            var row_i: usize = 0;
            while (rows.next()) |row| : (row_i += 1) {

                // Compute dist to each centroid, pick closest
                var centroids = utils.stride(self.centroid_array, self.n_features);
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
                    self.labels[row_i] = nearest;
                    converged = false;
                }

                // Add this point to the accumulator for this centroid
                // We use the Welford method maintain a running average as it
                //  is more numerically stable than the naive method
                self.centroid_counts[nearest] += 1;
                for (new_centroids.i(nearest)) |*v, feature_i| {
                    const count = @intToFloat(DataT, self.centroid_counts[nearest]);
                    v.* += (row[feature_i] - v.*) / count;
                }
            }

            // The scratch_centroid_array now contains the mean of its member
            //  points.  We do want to check for centroids with no members and
            //  simply reinitialize them to a new random point
            for (self.centroid_counts) |count, centroid_i| {
                if (count == 0) {
                    // unused centroid, reinit to a random point
                    self.randomCentroid(new_centroids.i(centroid_i));
                }
            }
            // Copy the new centroids into the main buffer
            std.mem.copy(DataT, self.centroid_array, self.scratch_centroid_array);

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

test "basic usage" {
    var allocator = std.testing.allocator;

    const n_rows = 1_000;
    const n_features = 2;
    const n_centroids = 8;
    var gen = std.rand.DefaultPrng.init(0).random;
    var r = &gen;

    var data = try allocator.alloc(f32, n_rows * n_features);
    defer allocator.free(data);
    for (data) |*v| {
        v.* = r.float(f32) * 100;
    }

    var kmeans = try KMeans(f32, u8, comptime utils.EuclideanSquared(f32))
                      .init(allocator, r, data, n_features, n_centroids);
    defer kmeans.deinit(allocator);
    const converged = kmeans.run(100);
    //std.debug.print("Converged? {}\n", .{ converged });
    try std.testing.expect(converged);

    //const writer = std.io.getStdOut().writer();
    //try writer.print("x,y,label\n", .{});
    //var strider = utils.stride(data, 2);
    //var row_i: usize = 0;
    //while (strider.next()) |row| : (row_i += 1) {
        //try writer.print("{d:.2},{d:.2},{}\n", .{ row[0], row[1], kmeans.labels[row_i] });
    //}
}
