// SPDX-License-Identifier: MIT OR Apache-2.0

use super::*;

#[test]
fn upstream_bugfix_braw_bad_box_lengths_and_array_indices_do_not_panic() {
    for (array, data) in [
        (false, [1u32.to_be_bytes().as_slice(), b"bad!", &[0; 8]].concat()),
        (false, [u32::MAX.to_be_bytes().as_slice(), b"bad!", &[0; 8]].concat()),
        (true, [20u32.to_be_bytes().as_slice(), &0u32.to_be_bytes(), &12u32.to_be_bytes(), b"data", &[0; 4]].concat()),
        (true, [20u32.to_be_bytes().as_slice(), &1u32.to_be_bytes(), &99u32.to_be_bytes(), b"data", &[0; 4]].concat()),
    ] {
        let mut called = false;
        BlackmagicBraw::iter_boxes(&data, array, |_, _, _| { called = true; Ok(()) }).unwrap();
        assert!(!called);
    }
}

#[test]
fn upstream_bugfix_braw_zero_box_size_consumes_remainder_once() {
    let data = [&[0; 4][..], b"test", b"payload"].concat();
    let mut found = Vec::new();
    BlackmagicBraw::iter_boxes(&data, false, |name, bytes, _| {
        found.push((name.to_owned(), bytes.to_vec()));
        Ok(())
    }).unwrap();
    assert_eq!(found, [("test".to_owned(), b"payload".to_vec())]);
}

#[test]
fn upstream_bugfix_braw_truncated_global_and_frame_metadata_are_errors() {
    let mut parser = BlackmagicBraw::default();
    let data = [b"xxxx".as_slice(), &1000u32.to_be_bytes(), b"meta", &[0; 4], b"hdlr", &[0; 8]].concat();
    assert!(parser.parse_meta(&mut Cursor::new(&data), data.len()).is_err());
    let frame = [&100u32.to_be_bytes()[..], b"bmdf", &[0; 4]].concat();
    assert!(BlackmagicBraw::parse_per_frame_meta(&frame).is_err());
}
