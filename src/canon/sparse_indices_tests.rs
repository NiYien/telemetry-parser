// SPDX-License-Identifier: MIT OR Apache-2.0

use super::*;

fn boxed(name: &[u8; 4], payload: &[u8]) -> Vec<u8> {
    [&((payload.len() + 8) as u32).to_be_bytes()[..], name, payload].concat()
}

fn full_box(name: &[u8; 4], payload: &[u8]) -> Vec<u8> {
    boxed(name, &[&[0; 4][..], payload].concat())
}

fn metadata_clip(samples: &[Vec<u8>]) -> Vec<u8> {
    let count = samples.len() as u32;
    let timescale = 30000u32;
    let delta = 1001u32;
    let movie = |offset: u32| {
        let mvhd = full_box(b"mvhd", &[&[0; 8][..], &timescale.to_be_bytes(), &(count * delta).to_be_bytes(), &[0; 80]].concat());
        let mdhd = full_box(b"mdhd", &[&[0; 8][..], &timescale.to_be_bytes(), &(count * delta).to_be_bytes(), &[0; 4]].concat());
        let hdlr = full_box(b"hdlr", &[&[0; 4][..], b"meta", &[0; 12], b"MetadataHandler\0"].concat());
        let stts = full_box(b"stts", &[&1u32.to_be_bytes()[..], &count.to_be_bytes(), &delta.to_be_bytes()].concat());
        let stsc = full_box(b"stsc", &[&1u32.to_be_bytes()[..], &1u32.to_be_bytes(), &count.to_be_bytes(), &1u32.to_be_bytes()].concat());
        let sizes = samples.iter().flat_map(|s| (s.len() as u32).to_be_bytes()).collect::<Vec<_>>();
        let stsz = full_box(b"stsz", &[&[0; 4][..], &count.to_be_bytes(), &sizes].concat());
        let stco = full_box(b"stco", &[&1u32.to_be_bytes()[..], &offset.to_be_bytes()].concat());
        let stbl = boxed(b"stbl", &[stts, stsc, stsz, stco].concat());
        let mdia = boxed(b"mdia", &[mdhd, hdlr, boxed(b"minf", &stbl)].concat());
        boxed(b"moov", &[mvhd, boxed(b"trak", &mdia)].concat())
    };
    let ftyp = boxed(b"ftyp", b"isom\0\0\0\0isom");
    let offset = (ftyp.len() + movie(0).len() + 8) as u32;
    [ftyp, movie(offset), boxed(b"mdat", &samples.concat())].concat()
}

fn sparse_clip() -> Vec<u8> {
    let samples = (0..10).map(|i| match i {
        1 | 3 | 8 => Vec::new(),
        4 => vec![0; 4],
        _ => [&16u32.to_le_bytes()[..], &4u32.to_le_bytes(), &[0; 4], &(50u16 + i).to_le_bytes(), &1u16.to_le_bytes()].concat(),
    }).collect::<Vec<_>>();
    metadata_clip(&samples)
}

#[test]
fn upstream_bugfix_canon_sparse_metadata_keeps_original_indices_across_empty_and_short_samples() {
    let data = sparse_clip();
    let mut canon = Canon { is_crm: true, ..Default::default() };
    let samples = canon.parse(&mut Cursor::new(&data), data.len(), |_| {}, Arc::new(AtomicBool::new(false)), InputOptions {
        metadata_sample_stride: Some(3), ..Default::default()
    }).unwrap();
    assert_eq!(samples.iter().map(|s| s.sample_index).collect::<Vec<_>>(), [0, 6, 9]);
    for (sample, index) in samples.iter().zip([0, 6, 9]) {
        assert!((sample.timestamp_ms - index as f64 * 1001.0 / 30.0).abs() < 0.001);
        assert_eq!(sample.tag_map.as_ref().unwrap().get(&GroupId::Lens).unwrap().get_t(TagId::FocalLength) as Option<&f32>, Some(&(50.0 + index as f32)));
    }
}

#[test]
fn upstream_bugfix_canon_indexed_reader_does_not_change_the_existing_sparse_reader_protocol() {
    let data = sparse_clip();
    let mut indexed = Vec::new();
    let (_, count, stride) = util::get_metadata_track_samples_strided_indexed(&mut Cursor::new(&data), data.len(), true, Some(3),
        |info, _, _, _| indexed.push(info.sample_index), Arc::new(AtomicBool::new(false))).unwrap();
    assert_eq!((indexed, count, stride), (vec![0, 6, 9], 10, 3));
    let mut legacy = Vec::new();
    let (_, count, stride) = util::get_metadata_track_samples_strided(&mut Cursor::new(&data), data.len(), true, Some(3),
        |info, _, _, _| legacy.push(info.sample_index), Arc::new(AtomicBool::new(false))).unwrap();
    assert_eq!((legacy, count, stride), (vec![0, 1], 6, 3));
}
