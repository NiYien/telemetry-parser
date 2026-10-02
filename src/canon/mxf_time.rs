// SPDX-License-Identifier: MIT OR Apache-2.0

use chrono::{Datelike, Duration, NaiveDate, NaiveDateTime};

fn bcd(value: u8) -> Option<u32> {
    let tens = value >> 4;
    let units = value & 0x0f;
    (tens < 10 && units < 10).then_some(u32::from(tens) * 10 + u32::from(units))
}

fn offset_minutes(code: u8) -> Option<i32> {
    // SMPTE ST 309:2012, Table 2. Unknown and user-defined offsets cannot be inferred.
    Some(match code {
        0x00..=0x09 => -i32::from(code) * 60,
        0x10..=0x12 => -(10 + i32::from(code - 0x10)) * 60,
        0x0a..=0x0f => -(i32::from(code - 0x0a) * 60 + 30),
        0x1a..=0x1f => -((6 + i32::from(code - 0x1a)) * 60 + 30),
        0x13..=0x19 => (13 - i32::from(code - 0x13)) * 60,
        0x20..=0x25 => (6 - i32::from(code - 0x20)) * 60,
        0x2a..=0x2f => (11 - i32::from(code - 0x2a)) * 60 + 30,
        0x3a..=0x3f => (5 - i32::from(code - 0x3a)) * 60 + 30,
        0x32 => 12 * 60 + 45,
        _ => return None,
    })
}

/// Read the first creation date-time stamp, retaining the existing Preface time and subsecond.
pub(super) fn creation_timezone(pack: &[u8], preface: Option<&str>, fps: f64) -> Option<String> {
    // ST 385 puts the creation stamp after the seven core bytes and the 16-byte UL.
    // ST 331 type 0x82 carries ST 309 date/time/zone; type 0x81 is only a user timecode.
    if pack.len() != 57 || pack[0] & 0x60 != 0x60 || pack[23] != 0x82 {
        return None;
    }
    if pack[32..40].iter().any(|&byte| byte != 0) {
        return None;
    }
    let clock = &pack[24..32];
    let pal = (fps - 25.0).abs() < 0.01 || (fps - 50.0).abs() < 0.01;
    let (bgf2, bgf0) = if pal {
        (clock[2] >> 7, clock[1] >> 7)
    } else {
        (clock[3] >> 7, clock[2] >> 7)
    };
    if bgf2 != 1 || bgf0 != 0 {
        return None;
    }
    let frame_base = if pal { 25 } else if (fps - 24.0).abs() < 0.1 { 24 } else { 30 };
    if bcd(clock[0] & 0x3f)? >= frame_base {
        return None;
    }
    let minutes = offset_minutes(clock[7] & 0x3f)? + if clock[7] & 0x40 != 0 { 60 } else { 0 };
    let reference = NaiveDateTime::parse_from_str(preface?, "%Y:%m:%d %H:%M:%S").ok()?;
    let date = if clock[7] & 0x80 != 0 {
        let mjd = bcd(clock[4])? + bcd(clock[5])? * 100 + bcd(clock[6])? * 10_000;
        NaiveDate::from_ymd_opt(1858, 11, 17)?.checked_add_signed(Duration::days(i64::from(mjd)))?
    } else {
        // The date has a two-digit year. Require the Preface year instead of guessing a century.
        if bcd(clock[6])? != reference.year().rem_euclid(100) as u32 {
            return None;
        }
        NaiveDate::from_ymd_opt(reference.year(), bcd(clock[5])?, bcd(clock[4])?)?
    };
    let stamp = date.and_hms_opt(bcd(clock[3] & 0x3f)?, bcd(clock[2] & 0x7f)?, bcd(clock[1] & 0x7f)?)?;
    // MJD stores UTC. BCD calendar dates store local time, including the DST offset.
    let local = if clock[7] & 0x80 != 0 {
        stamp.checked_add_signed(Duration::minutes(i64::from(minutes)))?
    } else {
        stamp
    };
    if local != reference {
        return None;
    }
    Some(format!("{}{:02}:{:02}", if minutes < 0 { '-' } else { '+' }, minutes.abs() / 60, minutes.abs() % 60))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pack() -> [u8; 57] {
        let mut data = [0; 57];
        data[0] = 0x7c;
        data[23..32].copy_from_slice(&[0x82, 0x40, 0x00, 0x29, 0x97, 0x22, 0x12, 0x24, 0x18]);
        data
    }

    const PREFACE: &str = "2024:12:22 17:29:00";

    #[test]
    fn reads_both_real_r5c_creation_stamps() {
        assert_eq!(creation_timezone(&pack(), Some(PREFACE), 59.94).as_deref(), Some("+08:00"));
        let mut other = pack();
        other[24..32].copy_from_slice(&[0x40, 0x21, 0x27, 0x97, 0x16, 0x02, 0x25, 0x18]);
        assert_eq!(creation_timezone(&other, Some("2025:02:16 17:27:21"), 59.94).as_deref(), Some("+08:00"));
    }

    #[test]
    fn decodes_offsets_including_half_and_quarter_hours() {
        for (code, expected) in [(0x00, "+00:00"), (0x05, "-05:00"), (0x12, "-12:00"),
            (0x0d, "-03:30"), (0x1f, "-11:30"), (0x13, "+13:00"), (0x19, "+07:00"),
            (0x20, "+06:00"), (0x25, "+01:00"), (0x2a, "+11:30"), (0x2f, "+06:30"),
            (0x3a, "+05:30"), (0x3f, "+00:30"), (0x32, "+12:45")] {
            let mut data = pack();
            data[31] = code;
            assert_eq!(creation_timezone(&data, Some(PREFACE), 59.94).as_deref(), Some(expected));
        }
    }

    #[test]
    fn adds_daylight_saving_to_standard_offset() {
        let mut data = pack();
        data[31] |= 0x40;
        assert_eq!(creation_timezone(&data, Some(PREFACE), 59.94).as_deref(), Some("+09:00"));
    }

    #[test]
    fn honors_pal_binary_group_flag_positions() {
        let mut data = pack();
        data[26] |= 0x80;
        data[27] &= 0x7f;
        assert_eq!(creation_timezone(&data, Some(PREFACE), 25.0).as_deref(), Some("+08:00"));
        assert_eq!(creation_timezone(&data, Some(PREFACE), 59.94), None);
    }

    #[test]
    fn reads_mjd_as_utc_then_checks_local_preface() {
        let mut data = pack();
        let day = NaiveDate::from_ymd_opt(2024, 12, 22).unwrap();
        let mjd = day.signed_duration_since(NaiveDate::from_ymd_opt(1858, 11, 17).unwrap()).num_days();
        let encode = |v: i64| ((v / 10) * 16 + v % 10) as u8;
        data[27] = 0x89;
        data[28] = encode(mjd % 100);
        data[29] = encode(mjd / 100 % 100);
        data[30] = encode(mjd / 10_000);
        data[31] |= 0x80;
        assert_eq!(creation_timezone(&data, Some(PREFACE), 59.94).as_deref(), Some("+08:00"));
    }

    #[test]
    fn rejects_unknown_reserved_and_user_defined_offsets() {
        for code in [0x26, 0x27, 0x28, 0x29, 0x30, 0x31, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39] {
            let mut data = pack();
            data[31] = code;
            assert_eq!(creation_timezone(&data, Some(PREFACE), 59.94), None);
        }
    }

    #[test]
    fn rejects_missing_truncated_unmarked_and_invalid_records() {
        let original = pack();
        for len in 0..57 {
            assert_eq!(creation_timezone(&original[..len], Some(PREFACE), 59.94), None);
        }
        for (at, value) in [(0, 0x5c), (23, 0x81), (24, 0x7a), (25, 0x6a),
            (27, 0xb7), (28, 0x32), (29, 0x13), (30, 0x23), (32, 1)] {
            let mut data = original;
            data[at] = value;
            assert_eq!(creation_timezone(&data, Some(PREFACE), 59.94), None, "byte {at}");
        }
        let mut data = original;
        data[27] &= 0x7f;
        assert_eq!(creation_timezone(&data, Some(PREFACE), 59.94), None);
        assert_eq!(creation_timezone(&original, None, 59.94), None);
        assert_eq!(creation_timezone(&original, Some("2024:12:22 17:29:01"), 59.94), None);
    }
}
