// SPDX-License-Identifier: MIT OR Apache-2.0
use crate::{InputOptions, cooke::Cooke, red::RedR3d, tags_impl::*, util::SampleInfo};
use std::{io::Cursor, sync::{Arc, atomic::AtomicBool}};

fn parse_cooke(records: &[String]) -> Vec<SampleInfo> {
    let data = records.join("\n\n");
    Cooke::default().parse(&mut Cursor::new(data.as_bytes()), data.len(), |_| {},
        Arc::new(AtomicBool::new(false)), InputOptions::default()).unwrap()
}
fn record(kind: &str, timestamp: u16, frame: usize) -> String {
    format!("RecordType: rt.temporal.lens.{kind}.raw\nTimestamp: {timestamp}\nTimecode: {{hh: 0, mm: 0, ss: 0, ff: {frame}}}\nDatavals: {{X: 1, Y: 2, Z: 3}}")
}
fn times(samples: &[SampleInfo], group: GroupId) -> Vec<f64> {
    samples.iter().filter_map(|sample| {
        let tag = sample.tag_map.as_ref()?.get(&group)?.get(&TagId::Data)?;
        match &tag.value {
            TagValue::Vec_TimeVector3_f64(value) => Some(value.get().iter().map(|v| v.t).collect::<Vec<_>>()),
            _ => None,
        }
    }).flatten().collect()
}

#[test]
fn cooke_rollovers_preserve_every_tick_for_each_sensor() {
    let mut records = Vec::new();
    for timestamp in [65535, 0, 65535, 0] {
        for kind in ["accelerometer", "gyro", "magnetometer"] {
            records.push(record(kind, timestamp, records.len() + 1));
        }
    }
    let samples = parse_cooke(&records);
    for group in [GroupId::Accelerometer, GroupId::Gyroscope, GroupId::Magnetometer] {
        let actual = times(&samples, group);
        assert_eq!(actual.len(), 4);
        for (time, ticks) in actual.into_iter().zip([65535.0, 65536.0, 131071.0, 131072.0]) {
            assert!((time * 150000.0 - ticks).abs() < 1e-8);
        }
    }
}

#[test]
fn cooke_unknown_and_missing_record_types_preserve_valid_data() {
    let valid = [record("gyro", 65535, 1), record("gyro", 0, 2)];
    let mut records = vec![valid[0].clone()];
    records.extend([
        "RecordType: rt.temporal.lens.gyro.derived\nTimestamp: 42".into(),
        "RecordType: rt.temporal.lens.gyro.derived\nTimestamp: 43".into(),
        "RecordType: rt.future.extension".into(),
        "Timestamp: 44\nDatavals: {X: 4, Y: 5, Z: 6}".into(),
        "Description: no record type or timestamp".into(),
    ]);
    records.push(valid[1].clone());
    let expected = parse_cooke(&valid);
    let actual = parse_cooke(&records);
    assert_eq!(actual.len(), expected.len());
    assert_eq!(times(&actual, GroupId::Gyroscope), times(&expected, GroupId::Gyroscope));
}

#[test]
fn cooke_header_timestamp_does_not_index_temporal_counters() {
    let samples = parse_cooke(&[
        "RecordType: rt.header.lens.info\nTimestamp: 123\nTimecode: {hh: 0, mm: 0, ss: 0, ff: 0}\nModel: test".into(),
        record("gyro", 10, 1),
    ]);
    assert_eq!(samples.len(), 2);
    let tag = &samples[0].tag_map.as_ref().unwrap()[&GroupId::Lens][&TagId::Metadata];
    match &tag.value {
        TagValue::Json(value) => assert_eq!(value.get()["Model"], "test"),
        _ => panic!("Lens metadata must remain JSON"),
    }
    assert_eq!(times(&samples, GroupId::Gyroscope), vec![10.0 / 150000.0]);
}

fn red_entry(kind: u8, id: u8, payload: &[u8]) -> Vec<u8> {
    let mut bytes = ((payload.len() + 4) as u16).to_be_bytes().to_vec();
    bytes.extend([kind, id]);
    bytes.extend(payload);
    bytes
}
fn parse_red(bytes: &[u8]) -> (RedR3d, serde_json::Value) {
    let mut parser = RedR3d::default();
    let mut tags = GroupedTagMap::new();
    parser.parse_meta(bytes, &mut tags, &InputOptions::default()).unwrap();
    let metadata = match &tags[&GroupId::Default][&TagId::Metadata].value {
        TagValue::Json(value) => value.get().clone(),
        _ => panic!("RED metadata must remain JSON"),
    };
    (parser, metadata)
}

#[test]
fn red_strings_stop_at_nul_without_consuming_the_next_entry() {
    for (payload, expected) in [
        (&b"V-RAPTOR XE 8K VV\0"[..], "V-RAPTOR XE 8K VV"),
        (&b"KOMODO\0ignored\xff"[..], "KOMODO"),
        (&b"KOMODO"[..], "KOMODO"),
        (&b"\0"[..], ""),
        (&b""[..], ""),
        (&b"\xff\0"[..], ""),
    ] {
        let mut bytes = red_entry(0x10, 0xA0, payload);
        bytes.extend(red_entry(0x60, 0xBF, &30000u32.to_be_bytes()));
        let (parser, metadata) = parse_red(&bytes);
        assert_eq!(parser.model.as_deref(), Some(expected));
        assert_eq!(metadata["camera_model"], expected);
        assert_eq!(metadata["record_fps"], 30000);
    }
}

#[test]
fn red_legacy_model_fallback_and_explicit_model_priority_remain_available() {
    let mut bytes = red_entry(0x10, 0x71, b"EPIC\0");
    let (parser, _) = parse_red(&bytes);
    assert_eq!(parser.model.as_deref(), Some("EPIC"));
    bytes.extend(red_entry(0x10, 0xA0, b"KOMODO\0"));
    let (parser, metadata) = parse_red(&bytes);
    assert_eq!(parser.model.as_deref(), Some("KOMODO"));
    assert_eq!(metadata["camera_network_name"], "EPIC");
}

#[test]
fn unknown_tag_ids_round_trip_as_hexadecimal() {
    for value in [0, 15, 16, 0xe402, u32::MAX] {
        let id = TagId::Unknown(value);
        assert_eq!(id.to_string().parse::<TagId>().unwrap(), id);
    }
    assert_eq!("0xABcd".parse::<TagId>().unwrap(), TagId::Unknown(0xabcd));
    assert_eq!("Metadata".parse::<TagId>().unwrap(), TagId::Metadata);
    assert_eq!("*".parse::<TagId>().unwrap(), TagId::Any);
    assert_eq!("vendor_field".parse::<TagId>().unwrap(), TagId::Custom("vendor_field".into()));
}

#[test]
fn invalid_hexadecimal_tag_ids_still_return_errors() {
    for value in ["0x", "0xGG", "0x100000000", "0x-1"] {
        assert!(value.parse::<TagId>().is_err(), "{value}");
    }
}
