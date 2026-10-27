// SPDX-License-Identifier: MIT OR Apache-2.0

use super::*;

#[test]
fn upstream_bugfix_sony_cdl_uses_ieee_half_float_including_zero_and_negative_values() {
    let mut bytes = [10u32.to_be_bytes(), 2u32.to_be_bytes()].concat();
    for value in [0.5, 1.5, 1.0, -0.25, 0.0, 0.25, 1.0, 1.0, 1.0, 0.0] {
        bytes.extend(half::f16::from_f32(value).to_bits().to_be_bytes());
    }
    let tag = rtmd_tags::get_tag(0x8117, &bytes);
    let TagValue::Json(value) = tag.value else { panic!("CDL must be JSON") };
    let value = value.get();
    assert_eq!(value["slope"]["r"].as_f64(), Some(0.5));
    assert_eq!(value["slope"]["g"].as_f64(), Some(1.5));
    assert_eq!(value["offset"]["r"].as_f64(), Some(-0.25));
    assert_eq!(value["offset"]["g"].as_f64(), Some(0.0));
    assert_eq!(value["saturation"].as_f64(), Some(0.0));
}

#[test]
fn upstream_bugfix_sony_bad_cooke_response_is_read_without_panicking() {
    let mut map = GroupedTagMap::new();
    let bytes = [&0xe201u16.to_be_bytes()[..], &3u16.to_be_bytes(), b"bad"].concat();
    Sony::parse_metadata(&bytes, &InputOptions::default(), &mut map).unwrap();
    let mut samples = vec![SampleInfo { tag_map: Some(map), ..Default::default() }];
    Sony::default().process_map(&mut samples, &InputOptions::default(), None, None, None);
    let tag = &samples[0].tag_map.as_ref().unwrap().get(&GroupId::Cooke).unwrap().get(&TagId::Data).unwrap().value;
    let TagValue::Json(value) = tag else { panic!("Cooke must be decoded lazily") };
    assert!(value.get().is_null());
}

#[test]
fn upstream_bugfix_sony_bad_frame_metadata_does_not_renumber_later_frames() {
    fn element(value: u8) -> Vec<u8> {
        let mut ancillary = vec![0, 1, 0, 0, 0, 0, 0, 5];
        ancillary.extend(5u32.to_be_bytes());
        ancillary.extend(1u32.to_be_bytes());
        ancillary.extend([0x43, 0x05, 0, 0, value]);
        let mut bytes = vec![0x06, 0x0e, 0x2b, 0x34, 0x01, 0x02, 0x01, 0x01, 0x0d, 0x01, 0x03, 0x01, 0x17, 0x01, 0x02, 0x01];
        bytes.push(ancillary.len() as u8);
        bytes.extend(ancillary);
        bytes
    }
    let bytes = [element(0), element(1)].concat();
    let (samples, _, _) = mxf::parse(&mut Cursor::new(&bytes), bytes.len(), |_| {}, Arc::new(AtomicBool::new(false)), None,
        &InputOptions::default(), |data, _, _| if data == [0] { Err(ErrorKind::InvalidData.into()) } else { Ok(()) }).unwrap();
    assert_eq!(samples.len(), 1);
    assert_eq!(samples[0].sample_index, 1);
    assert_eq!(samples[0].timestamp_ms, 40.0);
}
