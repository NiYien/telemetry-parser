// SPDX-License-Identifier: MIT OR Apache-2.0

use super::*;

#[test]
fn upstream_bugfix_nikon_short_dimensions_and_non_object_summary_keep_valid_lens_data() {
    fn nrmt(id: u32, value: &[u8]) -> Vec<u8> {
        [&((13 + value.len()) as u32).to_be_bytes()[..], b"NRMT", &id.to_be_bytes(), &[0], value].concat()
    }
    let payload = [nrmt(0x0110_0100, &[1]), nrmt(0x0110_0101, &[2, 3]), nrmt(0x0110_920a, &50f32.to_be_bytes())].concat();
    let frame = [&((8 + payload.len()) as u32).to_be_bytes()[..], b"NRAW", &payload].concat();
    let options = InputOptions::default();
    let mut map = GroupedTagMap::new();
    util::insert_tag(&mut map, tag!(parsed GroupId::Default, TagId::Metadata, "Metadata", Json, |v| v.to_string(), serde_json::Value::Null, Vec::new()), &options);
    Nikon::default().parse_nev_frame_metadata(&frame, &mut map, &options).unwrap();
    assert_eq!(map.get(&GroupId::Lens).unwrap().get_t(TagId::FocalLength) as Option<&f32>, Some(&50.0));
    let md = (map.get(&GroupId::Default).unwrap().get_t(TagId::Metadata) as Option<&serde_json::Value>).unwrap();
    assert!(md.get("image_width").is_none());
    assert!(md.get("image_height").is_none());
}

#[test]
fn upstream_bugfix_nikon_nctg_lookup_uses_box_boundaries_instead_of_fixed_offset() {
    fn boxed(name: &[u8], data: &[u8]) -> Vec<u8> {
        [&((8 + data.len()) as u32).to_be_bytes()[..], name, data].concat()
    }
    let tags = b"bounded tags";
    let payload = [boxed(b"NCHD", b"a longer version descriptor"), boxed(b"NCTG", tags), boxed(b"NCVW", b"preview")].concat();
    assert_eq!(util::find_box_data(&payload, b"NCTG"), Some(tags.as_slice()));
    assert!(util::find_box_data(&payload[..payload.len() - 21], b"NCTG").is_none());
}
