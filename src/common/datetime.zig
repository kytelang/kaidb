//! Proleptic-Gregorian UTC date/time arithmetic for the SQL layer.
//!
//! NovaDB stores `DATE`, `TIME`, and `TIMESTAMP` columns as a single signed
//! millisecond count relative to the Unix epoch (1970-01-01T00:00:00Z). That
//! `i64` is what the codec writes into a cell and what comparisons, ordering,
//! and range scans operate on: reducing every temporal value to one integer
//! means the B+Tree never has to know about calendars, and two timestamps sort
//! correctly by plain integer comparison. This module is the only place that
//! converts between that on-the-wire integer and a human-readable broken-down
//! date, in both directions.
//!
//! Everything here is deliberately UTC-only and time-zone-free. There is no
//! `tm_isdst`, no leap seconds, and no locale: a day is exactly 86400 seconds,
//! and the calendar is the proleptic Gregorian one (the 400/100/4 leap rule
//! applied uniformly, including to years before its historical adoption). This
//! keeps the round-trip [`DateTime.toEpochMs`] then [`DateTime.fromEpochMs`]
//! exact and independent of the host's zone.
//!
//! Conversion is done by explicit day-counting loops ([`dateTimeToEpochS`] /
//! [`epochSToDateTime`]) rather than a closed-form algorithm. The loops are
//! simple to audit and correct for the range the SQL layer cares about, at the
//! cost of being O(years-from-1970): a value thousands of years from the epoch
//! iterates thousands of times. Note the forward loop only counts UP from 1970
//! (`while (y < year)`), so composing a `DateTime` whose `year` is before 1970
//! does not add the intervening days; the negative-epoch direction that IS
//! handled is the *decode* side ([`epochSToDateTime`] normalises a negative
//! sub-day remainder by borrowing a day).
//!
//! Parsing ([`parseIso`]) accepts the ISO-8601 subset the SQL grammar emits:
//! `YYYY-MM-DD` optionally followed by `THH:MM:SS` (or a space separator). It
//! validates the calendar (month 1-12, day within [`daysInMonth`]) so an
//! impossible date is rejected rather than silently normalised.

const std = @import("std");

/// A broken-down UTC calendar timestamp: the human-readable form of the `i64`
/// epoch-millisecond value NovaDB actually stores.
///
/// Fields are stored as-is with no normalisation, so it is possible to
/// construct an out-of-range instance (e.g. `month = 13`); the validated entry
/// point is [`parseIso`], and [`fromEpochMs`] always yields an in-range value.
/// `year` is signed to allow dates before the epoch on the decode path, but see
/// the module header for the forward-conversion caveat below 1970. Sub-second
/// precision is not represented: [`toEpochMs`] scales whole seconds up by 1000.
pub const DateTime = struct {
    /// Proleptic-Gregorian year (signed; may be negative on decode).
    year: i32,
    /// Month of year, 1-12 (1 = January). Not range-checked on construction.
    month: u8,
    /// Day of month, 1-31, bounded by [`daysInMonth`] for the given month/year.
    day: u8,
    /// Hour of day, 0-23. Defaults to 0 so a date-only value is midnight UTC.
    hour: u8 = 0,
    /// Minute of hour, 0-59. Defaults to 0.
    minute: u8 = 0,
    /// Second of minute, 0-59 (no leap seconds). Defaults to 0.
    second: u8 = 0,

    /// Converts this calendar timestamp to Unix epoch milliseconds (the stored form).
    ///
    /// Delegates to [`dateTimeToEpochS`] for the second count and scales by 1000;
    /// the fractional millisecond is always zero because [`DateTime`] has no
    /// sub-second field. For `year` values below 1970 the result is wrong by the
    /// pre-epoch day count (see the module header), so callers stay at or above the
    /// epoch, which SQL timestamps do in practice.
    pub fn toEpochMs(self: DateTime) i64 {
        return dateTimeToEpochS(self.year, self.month, self.day, self.hour, self.minute, self.second) * 1000;
    }

    /// Decodes Unix epoch milliseconds back into a broken-down UTC [`DateTime`].
    ///
    /// Floors the millisecond count to whole seconds before handing off to
    /// [`epochSToDateTime`] (which does the calendar walk and the negative-remainder
    /// normalisation), so any sub-second part of a negative `ms` rounds towards
    /// negative infinity rather than towards zero, keeping the field values
    /// non-negative. The inverse of [`toEpochMs`] for epoch-or-later values.
    pub fn fromEpochMs(ms: i64) DateTime {
        return epochSToDateTime(@divFloor(ms, 1000));
    }

    /// Formats this timestamp as a fixed-width ISO-8601 UTC string, e.g. `2026-08-21T14:03:09Z`.
    ///
    /// Always emits the full `YYYY-MM-DDTHH:MM:SSZ` shape with zero-padded,
    /// fixed-width fields and a literal `Z` (the value is UTC by construction).
    /// The returned slice is heap-allocated with `allocator` and owned by the
    /// caller, who must free it. Returns an allocation error if `allocPrint`
    /// fails. Note a `year` outside 0-9999 will exceed four digits and no longer
    /// round-trip through [`parseIso`], which reads exactly four year digits.
    pub fn formatIso(self: DateTime, allocator: std.mem.Allocator) ![]const u8 {
        return std.fmt.allocPrint(allocator, "{d:0>4}-{d:0>2}-{d:0>2}T{d:0>2}:{d:0>2}:{d:0>2}Z", .{
            self.year, self.month, self.day, self.hour, self.minute, self.second,
        });
    }
};

/// Parses an ISO-8601 date (optionally with a time) into Unix epoch milliseconds.
///
/// Accepts the two shapes the SQL layer produces: a bare `YYYY-MM-DD` (10 chars,
/// interpreted as midnight UTC), or `YYYY-MM-DD HH:MM:SS` where the separator at
/// index 10 may be either `T` or a space. Any other length is rejected; there is
/// no support for fractional seconds, offsets, or a trailing `Z`.
///
/// The calendar is validated, not just the syntax: month must be 1-12 and day
/// must be within [`daysInMonth`] for that month and year, so `2026-02-30` errors
/// instead of silently rolling over. Every field is parsed with an explicit fixed
/// slice and the intervening `-`/`:` separators are checked positionally.
///
/// Returns `error.InvalidDateFormat` for a too-short string, a malformed field, a
/// misplaced separator, an out-of-range month/day, or a length that is neither 10
/// nor a full 19-plus date-time. On success the result is `dateTimeToEpochS(...) *
/// 1000`, matching what [`DateTime.toEpochMs`] would produce for the same fields.
pub fn parseIso(s: []const u8) !i64 {
    if (s.len < 10) return error.InvalidDateFormat;

    const year = std.fmt.parseInt(i32, s[0..4], 10) catch return error.InvalidDateFormat;
    if (s[4] != '-') return error.InvalidDateFormat;
    const month = std.fmt.parseInt(u8, s[5..7], 10) catch return error.InvalidDateFormat;
    if (s[7] != '-') return error.InvalidDateFormat;
    const day = std.fmt.parseInt(u8, s[8..10], 10) catch return error.InvalidDateFormat;

    if (month < 1 or month > 12) return error.InvalidDateFormat;
    if (day < 1 or day > daysInMonth(month, year)) return error.InvalidDateFormat;

    var hour: u8 = 0;
    var minute: u8 = 0;
    var second: u8 = 0;

    if (s.len >= 19 and (s[10] == 'T' or s[10] == ' ')) {
        hour = std.fmt.parseInt(u8, s[11..13], 10) catch return error.InvalidDateFormat;
        if (s[13] != ':') return error.InvalidDateFormat;
        minute = std.fmt.parseInt(u8, s[14..16], 10) catch return error.InvalidDateFormat;
        if (s[16] != ':') return error.InvalidDateFormat;
        second = std.fmt.parseInt(u8, s[17..19], 10) catch return error.InvalidDateFormat;
    } else if (s.len != 10) {
        return error.InvalidDateFormat;
    }

    return dateTimeToEpochS(year, month, day, hour, minute, second) * 1000;
}

/// Reports whether `year` is a leap year under the proleptic Gregorian rule.
///
/// A year is a leap year when divisible by 4, except centuries, except centuries
/// divisible by 400 (so 2000 is, 1900 is not). Uses `@mod` so the result is
/// correct for negative years too. Drives both [`daysInMonth`] (February length)
/// and the year-length steps in the epoch conversions.
pub fn isLeapYear(year: i32) bool {
    if (@mod(year, 400) == 0) return true;
    if (@mod(year, 100) == 0) return false;
    return @mod(year, 4) == 0;
}

/// Returns the number of days in `month` (1-12) for the given `year`.
///
/// February returns 29 in a leap year (via [`isLeapYear`]) and 28 otherwise; all
/// other months are constant. The lookup table is 1-indexed with a dummy entry at
/// index 0, so `month` must be in 1-12; index 0 or 13+ is out of bounds. Used to
/// bound the day loop in [`dateTimeToEpochS`]/[`epochSToDateTime`] and to validate
/// the day field in [`parseIso`].
pub fn daysInMonth(month: u8, year: i32) u8 {
    const days = [_]u8{ 0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };
    if (month == 2 and isLeapYear(year)) return 29;
    return days[month];
}

/// Converts broken-down UTC fields to whole Unix epoch seconds by counting days.
///
/// Accumulates elapsed days in three stages: whole years from 1970 up to `year`
/// (each 365 or 366 via [`isLeapYear`]), whole months within `year` (via
/// [`daysInMonth`], so February's length tracks the leap rule), and `day - 1` for
/// the days already elapsed in the current month. The day total is then scaled to
/// seconds and the time-of-day added.
///
/// Subtle limitation: the year loop only runs while `y < year`, counting FORWARD
/// from 1970, so a `year` before 1970 skips the loop entirely and the result is
/// wrong by the pre-epoch span. Callers pass epoch-or-later dates. Inverse of
/// [`epochSToDateTime`].
fn dateTimeToEpochS(year: i32, month: u8, day: u8, hour: u8, minute: u8, second: u8) i64 {
    var total_days: i64 = 0;

    var y: i32 = 1970;
    while (y < year) : (y += 1) {
        total_days += if (isLeapYear(y)) @as(i64, 366) else @as(i64, 365);
    }

    var m: u8 = 1;
    while (m < month) : (m += 1) {
        total_days += @as(i64, daysInMonth(m, year));
    }

    total_days += @as(i64, day) - 1;

    return total_days * 86400 + @as(i64, hour) * 3600 + @as(i64, minute) * 60 + @as(i64, second);
}

/// Converts whole Unix epoch seconds back into a broken-down UTC [`DateTime`].
///
/// First splits the count into whole days and a within-day remainder. When
/// `epoch_s` is negative the remainder from `@mod` is already non-negative, but
/// `@divFloor` has rounded the day count towards negative infinity by one too
/// many for a mid-day negative instant, so the code borrows a day back and adds
/// 86400 to the remainder, and this is what makes pre-epoch instants decode with
/// correct 0-23/0-59 fields. It then walks years forward from 1970 subtracting a
/// year's length at a time, and walks months the same way, leaving `days` as the
/// zero-based day-of-month (hence `days + 1`). Inverse of [`dateTimeToEpochS`].
fn epochSToDateTime(epoch_s: i64) DateTime {
    var days = @divFloor(epoch_s, 86400);
    var remaining = @mod(epoch_s, 86400);
    if (remaining < 0) {
        days -= 1;
        remaining += 86400;
    }

    const hour: u8 = @intCast(@divFloor(remaining, 3600));
    remaining = @mod(remaining, 3600);
    const minute: u8 = @intCast(@divFloor(remaining, 60));
    const second: u8 = @intCast(@mod(remaining, 60));

    var year: i32 = 1970;
    while (true) {
        const diy: i64 = if (isLeapYear(year)) 366 else 365;
        if (days < diy) break;
        days -= diy;
        year += 1;
    }

    var month: u8 = 1;
    while (month <= 12) {
        const dim = @as(i64, daysInMonth(month, year));
        if (days < dim) break;
        days -= dim;
        month += 1;
    }

    return .{
        .year = year,
        .month = month,
        .day = @intCast(days + 1),
        .hour = hour,
        .minute = minute,
        .second = second,
    };
}
