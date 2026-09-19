const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const test_filters = b.option([]const []const u8, "test-filter", "Only run tests whose name matches a filter") orelse &.{};

    const host = buildKaidb(b, target, optimize);
    b.installArtifact(host.exe);
    b.installArtifact(host.cli_exe);

    const run_step = b.step("run", "Run the app");
    const run_cmd = b.addRunArtifact(host.exe);
    run_step.dependOn(&run_cmd.step);

    run_cmd.step.dependOn(b.getInstallStep());

    if (b.args) |args| {
        run_cmd.addArgs(args);
    }

    const mod_tests = b.addTest(.{
        .root_module = host.mod,
        .filters = test_filters,
    });

    const run_mod_tests = b.addRunArtifact(mod_tests);

    const exe_tests = b.addTest(.{
        .root_module = host.exe.root_module,
        .filters = test_filters,
    });

    const run_exe_tests = b.addRunArtifact(exe_tests);

    const test_step = b.step("test", "Run tests");
    // Tests write scratch databases (test_*.db and test_wal_*/ directories) into
    // the working directory; sweep them once both test binaries have finished so a
    // `zig build test` leaves no stray folders behind. Runs last (depends on the
    // test runs); best-effort so a clean tree does not fail the build.
    const clean_scratch = b.addSystemCommand(&.{ "sh", "-c", "rm -rf test_* 2>/dev/null || true" });
    clean_scratch.step.dependOn(&run_mod_tests.step);
    clean_scratch.step.dependOn(&run_exe_tests.step);
    test_step.dependOn(&clean_scratch.step);

    const cross_step = b.step("cross", "Cross-compile kaidb for all supported OS/arch targets");
    const CrossTarget = struct { triple: []const u8, server: bool };
    const cross_targets = [_]CrossTarget{
        .{ .triple = "aarch64-macos", .server = true },
        .{ .triple = "x86_64-macos", .server = true },
        .{ .triple = "x86_64-linux-gnu", .server = true },
        .{ .triple = "aarch64-linux-gnu", .server = true },
        .{ .triple = "x86_64-windows", .server = true },
        .{ .triple = "aarch64-windows", .server = true },
    };
    for (cross_targets) |ct| {
        const q = std.Build.parseTargetQuery(.{ .arch_os_abi = ct.triple }) catch @panic("bad target triple");
        const rt = b.resolveTargetQuery(q);
        const built = buildKaidb(b, rt, .ReleaseSafe);
        const custom_dir = b.fmt("cross/{s}", .{ct.triple});
        if (ct.server) {
            cross_step.dependOn(&b.addInstallArtifact(built.exe, .{
                .dest_dir = .{ .override = .{ .custom = custom_dir } },
            }).step);
        }
        cross_step.dependOn(&b.addInstallArtifact(built.cli_exe, .{
            .dest_dir = .{ .override = .{ .custom = custom_dir } },
        }).step);
    }
}

const Built = struct {
    exe: *std.Build.Step.Compile,
    cli_exe: *std.Build.Step.Compile,
    mod: *std.Build.Module,
};

fn buildKaidb(b: *std.Build, target: std.Build.ResolvedTarget, optimize: std.builtin.OptimizeMode) Built {
    const utils_mod = b.createModule(.{
        .root_source_file = b.path("src/common/utils.zig"),
        .target = target,
    });

    const yaml_dep = b.dependency("yaml", .{
        .target = target,
        .optimize = optimize,
    });
    const yaml_mod = yaml_dep.module("yaml");

    const tls_dep = b.dependency("tls", .{
        .target = target,
        .optimize = optimize,
    });
    const tls_mod = tls_dep.module("tls");

    // Exposed as a public "btree" module (via addModule, not createModule) so
    // external Zig dependents can `dep.module("btree")`.
    const mod = b.addModule("kaidb", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .imports = &.{
            .{ .name = "utils", .module = utils_mod },
            .{ .name = "yaml", .module = yaml_mod },
            .{ .name = "tls", .module = tls_mod },
        },
    });

    const exe = b.addExecutable(.{
        .name = "kaidb",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
            .link_libc = true,
            .imports = &.{
                .{ .name = "btree", .module = mod },
                .{ .name = "yaml", .module = yaml_mod },
                .{ .name = "utils", .module = utils_mod },
                .{ .name = "tls", .module = tls_mod },
            },
        }),
    });

    const cli_exe = b.addExecutable(.{
        .name = "kaidb-cli",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/cli.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "yaml", .module = yaml_mod },
                .{ .name = "utils", .module = utils_mod },
                .{ .name = "tls", .module = tls_mod },
            },
        }),
    });

    return .{ .exe = exe, .cli_exe = cli_exe, .mod = mod };
}
