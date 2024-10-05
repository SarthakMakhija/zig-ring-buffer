const std = @import("std");

/// A Fixed size ring-buffer which stores the elements of type T in contiguous locations in memory.
/// The implementation falls back to index 0 if the index reaches the last possible index in the available slice.
/// RingBuffer can be used with multiple threads.
fn RingBuffer(comptime T: type) type {
    return struct {
        index: std.atomic.Value(usize),
        elements: []T,
        allocator: std.mem.Allocator,

        /// Initialize RingBuffer with the given capacity. It uses the allocator of type `std.mem.Allocator`
        /// to allocate the memory for the backing array.
        fn init(allocator: std.mem.Allocator, capacity: usize) !RingBuffer(T) {
            return .{
                .index = std.atomic.Value(usize).init(0),
                .elements = try allocator.alloc(T, capacity),
                .allocator = allocator,
            };
        }

        /// Add the given element to the RingBuffer.
        /// The `add` implementation falls back to index 0 if the index reaches the last possible index in the available slice.
        /// It involves the following:
        /// 1). Loading the existing value of index. This operation can not use `relaxed/unordered`memory ordering. Because,
        /// `relaxed` gives no guarantee on what value of `index` the reading thread will see.
        /// It needs to be stronger than `relaxed/unordered`.
        ///
        /// 2). Using `acquire` forms a happens before relationship with the store operation done using `release`.
        /// `cmpxchgWeak` needs to provide two orderings: success and failure.
        /// In success ordering, we need to pass `acq_rel` as it is required establish a happens before relationship with the
        /// thread that does the store with `release` ordering, because we are using the `existing_index` value.
        /// In Zig, the failure ordering has to be either `monotonic` or `seq_cst`.
        fn add(self: *RingBuffer(T), element: T) void {
            var existing_index = self.index.load(.acquire);
            while (self.index.cmpxchgWeak(existing_index, @mod((existing_index + 1), self.elements.len), .acq_rel, .monotonic)) |existing| {
                existing_index = existing;
            }
            self.elements[existing_index] = element;
        }

        /// Returns the Sorted type which represents the RingBuffer elements in sorted order.
        /// The sorting order of the elements is determined by the `lessThanFn`.
        /// This is used in unit-tests only.
        fn sorted(self: RingBuffer(T), comptime lessThanFn: fn (lhs: T, rhs: T) bool) !Sorted(T) {
            return Sorted(T).init(self.allocator, self, lessThanFn);
        }

        /// Contains all the elements of the RingBuffer in the sorted order.
        /// This is used in unit-tests only.
        fn Sorted(comptime V: type) type {
            return struct {
                elements: []V,
                allocator: std.mem.Allocator,

                /// Initialize the Sorted type.
                /// It uses the allocator of type `std.mem.Allocator` to allocate the memory for the backing array
                /// which stores the sorted elements.
                fn init(allocator: std.mem.Allocator, buffer: RingBuffer(V), comptime lessThanFn: fn (lhs: V, rhs: V) bool) !Sorted(V) {
                    const elements = try allocator.alloc(V, buffer.elements.len);
                    @memcpy(elements, buffer.elements);

                    std.mem.sort(V, elements, {}, struct {
                        fn lessThan(_: void, lhs: V, rhs: V) bool {
                            return lessThanFn(lhs, rhs);
                        }
                    }.lessThan);

                    return .{
                        .elements = elements,
                        .allocator = allocator,
                    };
                }

                /// Release the memory allocated to hold the sorted elements.
                fn deinit(self: Sorted(V)) void {
                    self.allocator.free(self.elements);
                }
            };
        }

        /// Release the memory allocated to hold the ring buffer elements.
        fn deinit(self: RingBuffer(T)) void {
            self.allocator.free(self.elements);
        }
    };
}

test "adds elements to RingBuffer within its capacity and in a single thread" {
    var buffer = try RingBuffer(i32).init(std.testing.allocator, 4);
    defer buffer.deinit();

    buffer.add(10);
    buffer.add(20);
    buffer.add(30);
    buffer.add(40);

    const expected = [_]i32{ 10, 20, 30, 40 };
    try std.testing.expectEqualSlices(i32, &expected, buffer.elements);
}

test "adds elements to RingBuffer beyond its capacity and in a single thread" {
    var buffer = try RingBuffer(i32).init(std.testing.allocator, 4);
    defer buffer.deinit();

    buffer.add(10);
    buffer.add(20);
    buffer.add(30);
    buffer.add(40);
    buffer.add(50);

    const expected = [_]i32{ 50, 20, 30, 40 };
    try std.testing.expectEqualSlices(i32, &expected, buffer.elements);
}

test "adds elements to RingBuffer within its capacity and in multiple threads" {
    var general_purpose_allocator = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = general_purpose_allocator.allocator();
    defer _ = general_purpose_allocator.deinit();

    var pool: std.Thread.Pool = undefined;
    try pool.init(.{
        .allocator = allocator,
    });
    defer pool.deinit();

    var buffer = try RingBuffer(i32).init(std.testing.allocator, 4);
    defer buffer.deinit();

    var waiting_group = std.Thread.WaitGroup{};
    var waiting_group_ptr = &waiting_group;

    for (0..4) |index| {
        pool.spawnWg(waiting_group_ptr, comptime struct {
            fn addToBuffer(ring: *RingBuffer(i32), element: usize) void {
                ring.add(@intCast(element));
            }
        }.addToBuffer, .{ &buffer, index });
    }
    waiting_group_ptr.wait();

    const sorted = try buffer.sorted(struct {
        fn lessThan(one: i32, other: i32) bool {
            return one < other;
        }
    }.lessThan);
    defer sorted.deinit();

    const expected = [_]i32{ 0, 1, 2, 3 };
    try std.testing.expectEqualSlices(i32, &expected, sorted.elements);
}

test "adds elements to RingBuffer beyond its capacity and in multiple (20) threads" {
    var general_purpose_allocator = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = general_purpose_allocator.allocator();
    defer _ = general_purpose_allocator.deinit();

    var pool: std.Thread.Pool = undefined;
    try pool.init(.{
        .allocator = allocator,
    });
    defer pool.deinit();

    var buffer = try RingBuffer(i32).init(std.testing.allocator, 4);
    defer buffer.deinit();

    var waiting_group = std.Thread.WaitGroup{};
    var waiting_group_ptr = &waiting_group;

    for (0..20) |index| {
        pool.spawnWg(waiting_group_ptr, comptime struct {
            fn addToBuffer(ring: *RingBuffer(i32), element: usize) void {
                ring.add(@intCast(element));
            }
        }.addToBuffer, .{ &buffer, index });
    }
    waiting_group_ptr.wait();

    const sorted = try buffer.sorted(struct {
        fn lessThan(one: i32, other: i32) bool {
            return one < other;
        }
    }.lessThan);
    defer sorted.deinit();

    var element_frequency = std.AutoHashMap(i32, usize).init(std.testing.allocator);
    defer element_frequency.deinit();

    for (sorted.elements) |element| {
        if (element_frequency.get(element)) |_| {
            try std.testing.expect(false);
        } else {
            try element_frequency.put(element, 1);
        }
    }
}

test "adds elements to RingBuffer beyond its capacity and in multiple (100) threads" {
    var general_purpose_allocator = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = general_purpose_allocator.allocator();
    defer _ = general_purpose_allocator.deinit();

    var pool: std.Thread.Pool = undefined;
    try pool.init(.{
        .allocator = allocator,
    });
    defer pool.deinit();

    var buffer = try RingBuffer(i32).init(std.testing.allocator, 4);
    defer buffer.deinit();

    var waiting_group = std.Thread.WaitGroup{};
    var waiting_group_ptr = &waiting_group;

    for (0..100) |index| {
        pool.spawnWg(waiting_group_ptr, comptime struct {
            fn addToBuffer(ring: *RingBuffer(i32), element: usize) void {
                ring.add(@intCast(element));
            }
        }.addToBuffer, .{ &buffer, index });
    }
    waiting_group_ptr.wait();

    const sorted = try buffer.sorted(struct {
        fn lessThan(one: i32, other: i32) bool {
            return one < other;
        }
    }.lessThan);
    defer sorted.deinit();

    var element_frequency = std.AutoHashMap(i32, usize).init(std.testing.allocator);
    defer element_frequency.deinit();

    for (sorted.elements) |element| {
        if (element_frequency.get(element)) |_| {
            try std.testing.expect(false);
        } else {
            try element_frequency.put(element, 1);
        }
    }
}
