//! UTC time as RFC 3339 text, and back: the `ts` the server stamps on its
//! messages, and the `timestamp` of an event it measures latency from.
//! Proleptic Gregorian dates by Howard Hinnant's day arithmetic.

use std::time::{SystemTime, UNIX_EPOCH};

/// Now, in UTC, with `digits` fractional digits (3: milliseconds, 6:
/// microseconds): `2026-11-19T02:01:29.123Z`.
pub(crate) fn now(digits: u32) -> String {
    let since = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    format(
        i64::try_from(since.as_secs()).unwrap_or(i64::MAX),
        since.subsec_nanos(),
        digits,
    )
}

/// Milliseconds since the Unix epoch, now.
#[cfg(feature = "python")]
pub(crate) fn now_millis() -> i64 {
    let since = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    i64::try_from(since.as_millis()).unwrap_or(i64::MAX)
}

fn format(secs: i64, nanos: u32, digits: u32) -> String {
    let (year, month, day) = civil_from_days(secs.div_euclid(86_400));
    let of_day = secs.rem_euclid(86_400);
    let digits = digits.min(9);
    let fraction = nanos / 10u32.pow(9 - digits);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}.{fraction:0width$}Z",
        of_day / 3600,
        of_day % 3600 / 60,
        of_day % 60,
        width = digits as usize
    )
}

/// Milliseconds since the Unix epoch of `text`: RFC 3339 (`Z` or `±hh:mm`),
/// or the same without an offset, read as UTC. None when it is neither.
#[cfg(any(feature = "python", test))]
pub(crate) fn parse_millis(text: &str) -> Option<i64> {
    let b = text.as_bytes();
    if b.len() < 19
        || b[4] != b'-'
        || b[7] != b'-'
        || !matches!(b[10], b'T' | b't' | b' ')
        || b[13] != b':'
        || b[16] != b':'
    {
        return None;
    }
    let number = |from: usize, to: usize| -> Option<i64> {
        b[from..to].iter().try_fold(0i64, |n, &c| {
            c.is_ascii_digit().then(|| n * 10 + i64::from(c - b'0'))
        })
    };
    let (year, month, day) = (number(0, 4)?, number(5, 7)?, number(8, 10)?);
    let (hour, minute, second) = (number(11, 13)?, number(14, 16)?, number(17, 19)?);
    if !(1..=12).contains(&month)
        || day < 1
        || day > days_in_month(year, month)
        || hour > 23
        || minute > 59
        || second > 60
    {
        return None;
    }
    let mut at = 19;
    let mut millis = 0;
    if b.get(at) == Some(&b'.') {
        let digits = b[at + 1..]
            .iter()
            .take_while(|c| c.is_ascii_digit())
            .count();
        if digits == 0 {
            return None;
        }
        // The first three digits are the milliseconds; the rest are below.
        for i in 0..3 {
            let digit = if i < digits {
                i64::from(b[at + 1 + i] - b'0')
            } else {
                0
            };
            millis = millis * 10 + digit;
        }
        at += 1 + digits;
    }
    let offset = match &b[at..] {
        [] | [b'Z' | b'z'] => 0,
        [sign @ (b'+' | b'-'), _, _, b':', _, _] => {
            let (hours, minutes) = (number(at + 1, at + 3)?, number(at + 4, at + 6)?);
            if hours > 23 || minutes > 59 {
                return None;
            }
            let offset = hours * 3600 + minutes * 60;
            if *sign == b'+' { offset } else { -offset }
        }
        _ => return None,
    };
    let days = days_from_civil(year, month, day);
    Some((days * 86_400 + hour * 3600 + minute * 60 + second - offset) * 1000 + millis)
}

#[cfg(any(feature = "python", test))]
fn days_in_month(year: i64, month: i64) -> i64 {
    match month {
        2 if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

/// Days since the Unix epoch of a date.
#[cfg(any(feature = "python", test))]
fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = year.div_euclid(400);
    let year_of_era = year.rem_euclid(400);
    let shifted = if month > 2 { month - 3 } else { month + 9 };
    let day_of_year = (153 * shifted + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

/// The date of a count of days since the Unix epoch.
fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let day_of_era = z.rem_euclid(146_097);
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let shifted = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * shifted + 2) / 5 + 1;
    let month = if shifted < 10 {
        shifted + 3
    } else {
        shifted - 9
    };
    (year_of_era + era * 400 + i64::from(month <= 2), month, day)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn utc_is_written_to_the_digits_asked() {
        assert_eq!(format(0, 0, 3), "1970-01-01T00:00:00.000Z");
        assert_eq!(
            format(951_827_696, 789_000_000, 3),
            "2000-02-29T12:34:56.789Z"
        );
        assert_eq!(
            format(1_794_621_689, 123_456_789, 6),
            "2026-11-14T02:01:29.123456Z"
        );
        assert_eq!(
            format(4_102_444_799, 999_999_999, 3),
            "2099-12-31T23:59:59.999Z"
        );
        assert_eq!(now(3).len(), 24);
        assert_eq!(now(6).len(), 27);
    }

    #[test]
    fn rfc3339_and_a_time_without_offset_are_read_to_the_millisecond() {
        assert_eq!(parse_millis("1970-01-01T00:00:00Z"), Some(0));
        assert_eq!(
            parse_millis("2000-02-29T12:34:56.789Z"),
            Some(951_827_696_789)
        );
        assert_eq!(
            parse_millis("2026-11-19T02:01:29.123456+03:00"),
            Some(1_795_042_889_123)
        );
        assert_eq!(
            parse_millis("1999-12-31T23:59:59.999-05:30"),
            Some(946_704_599_999)
        );
        assert_eq!(parse_millis("2024-03-01T00:00:00"), Some(1_709_251_200_000));
        assert_eq!(
            parse_millis("2024-03-01 00:00:00.5"),
            Some(1_709_251_200_500)
        );
    }

    #[test]
    fn what_is_not_a_time_is_not_read() {
        for text in [
            "",
            "yesterday",
            "2024-02-30T00:00:00Z",
            "2023-02-29T00:00:00Z",
            "2024-13-01T00:00:00Z",
            "2024-01-01T24:00:00Z",
            "2024-01-01T00:00:00.Z",
            "2024-01-01T00:00:00+0300",
            "2024-01-01T00:00:00Zjunk",
        ] {
            assert_eq!(parse_millis(text), None, "{text}");
        }
    }

    #[test]
    fn dates_go_both_ways_over_eight_centuries() {
        for days in (-146_097..146_097).step_by(7) {
            let (year, month, day) = civil_from_days(days);
            assert_eq!(days_from_civil(year, month, day), days);
        }
        assert_eq!(civil_from_days(0), (1970, 1, 1));
        assert_eq!(civil_from_days(11_016), (2000, 2, 29));
    }
}
