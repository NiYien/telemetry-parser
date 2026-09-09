// SPDX-License-Identifier: MIT OR Apache-2.0
use super::rtmd_tags::get_tag;
use crate::tags_impl::{GroupId, TagValue};

#[test]
fn ibis_rotation_scale_is_a_float() {
    let tag = get_tag(0xe402, &1000.0f32.to_be_bytes());
    match tag.value {
        TagValue::f32(value) => assert_eq!(*value.get(), 1000.0),
        _ => panic!("IBIS rotation scale must remain floating point"),
    }
}

#[test]
fn oss_lens_position_retains_signed_components() {
    let bytes: Vec<u8> = [-120i32, 80, 50_000_000].into_iter().flat_map(i32::to_be_bytes).collect();
    match get_tag(0xe410, &bytes).value {
        TagValue::Vector3_i32(value) => {
            let value = value.get();
            assert_eq!((value.x, value.y, value.z), (-120, 80, 50_000_000));
        }
        _ => panic!("OSS lens position must remain a typed vector"),
    }
}

#[test]
fn breathing_samples_decode_and_missing_array_is_empty() {
    let bytes = [0, 0, 0, 2, 0, 0, 0, 16, 0, 128, 1, 1];
    let tag = get_tag(0xe523, &bytes);
    assert_eq!(tag.group, GroupId::LensBreathing);
    match tag.value {
        TagValue::Vec_u16(value) => assert_eq!(value.get(), &[128, 257]),
        _ => panic!("Breathing samples must remain a u16 array"),
    }
    match get_tag(0xe523, &[255, 255, 255, 255, 0, 0, 0, 16]).value {
        TagValue::Vec_u16(value) => assert!(value.get().is_empty()),
        _ => panic!("Missing breathing samples must decode as an empty array"),
    }
}
