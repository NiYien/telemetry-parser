// SPDX-License-Identifier: MIT OR Apache-2.0

use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

static NEXT: AtomicUsize = AtomicUsize::new(0);

struct ClipFiles { folder: std::path::PathBuf, files: Vec<std::path::PathBuf> }
impl ClipFiles {
    fn new() -> Self {
        let folder = std::env::temp_dir().join(format!("telemetry-red-bugfix-{}-{}", std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed)));
        std::fs::create_dir(&folder).unwrap();
        Self { folder, files: Vec::new() }
    }
    fn write(&mut self, name: &str, bytes: &[u8]) -> String {
        let path = self.folder.join(name);
        std::fs::write(&path, bytes).unwrap();
        self.files.push(path.clone());
        path.to_str().unwrap().to_owned()
    }
}
impl Drop for ClipFiles {
    fn drop(&mut self) {
        for file in &self.files { let _ = std::fs::remove_file(file); }
        let _ = std::fs::remove_dir(&self.folder);
    }
}

fn entry(kind: u8, id: u8, value: &[u8]) -> Vec<u8> {
    [&((value.len() + 4) as u16).to_be_bytes()[..], &[kind, id], value].concat()
}

fn header(metadata: &[u8]) -> Vec<u8> {
    let mut data = vec![0; 4096];
    data[..4].copy_from_slice(&(0x46u32 + metadata.len() as u32).to_be_bytes());
    data[4..8].copy_from_slice(b"RED2");
    data[0x44..0x46].copy_from_slice(&(metadata.len() as u16).to_be_bytes());
    data[0x46..0x46 + metadata.len()].copy_from_slice(metadata);
    data
}

fn frame(number: u32, timestamp: u64) -> Vec<u8> {
    let metadata = entry(0x20, 0x6b, &50f32.to_be_bytes());
    let mut data = vec![0; 4096];
    data[..4].copy_from_slice(&(96u32 + metadata.len() as u32).to_be_bytes());
    data[4..8].copy_from_slice(b"RDI\x01");
    data[64..72].copy_from_slice(&timestamp.to_be_bytes());
    data[76..80].copy_from_slice(&number.to_be_bytes());
    data[94..96].copy_from_slice(&(metadata.len() as u16).to_be_bytes());
    data[96..96 + metadata.len()].copy_from_slice(&metadata);
    data
}

fn imu(timestamps: &[u64]) -> Vec<u8> {
    let size = 4096 + timestamps.len() * 20;
    let mut data = vec![0; size.div_ceil(4096) * 4096];
    data[..4].copy_from_slice(&(size as u32).to_be_bytes());
    data[4..8].copy_from_slice(b"RDX\x02");
    for (record, &timestamp) in data[4096..size].chunks_exact_mut(20).zip(timestamps) {
        record[..8].copy_from_slice(&timestamp.to_be_bytes());
        for i in 0..6 { record[8 + 2 * i..10 + 2 * i].copy_from_slice(&(i as i16 + 1).to_be_bytes()); }
    }
    data
}

fn parse(path: &str, stride: usize) -> Vec<SampleInfo> {
    let mut file = std::fs::File::open(path).unwrap();
    let mut parser = RedR3d::detect(b"\0\0\0\x80RED2\0", path, &InputOptions::default()).unwrap();
    parser.parse(&mut file, 0, |_| {}, Arc::new(AtomicBool::new(false)), InputOptions {
        metadata_sample_stride: Some(stride), ..Default::default()
    }).unwrap()
}

#[test]
fn upstream_bugfix_red_bad_first_timestamps_and_sparse_metadata_keep_frame_positions() {
    let mut clip = header(&entry(0x20, 0x66, &25f32.to_be_bytes()));
    for i in 0..8 {
        let timestamp = 10_000_000 + i * 40_000 - if i < 2 { 1_000_000 } else { 0 };
        clip.extend(frame(i as u32 + 1, timestamp));
    }
    clip.extend([0; 8]);
    let mut files = ClipFiles::new();
    let path = files.write("clock_001.R3D", &clip);
    let samples = parse(&path, 3);
    assert_eq!(samples.len(), 9);
    assert!(samples[0].tag_map.as_ref().unwrap().get(&GroupId::Default).unwrap().get(&TagId::TimestampUs).is_none());
    for (i, sample) in samples[1..].iter().enumerate() {
        assert_eq!(sample.sample_index, i as u64);
        assert!((sample.timestamp_ms - i as f64 * 40.0).abs() < 1e-8);
        let lens = sample.tag_map.as_ref().unwrap().get(&GroupId::Lens);
        assert_eq!(lens.is_some(), i % 3 == 0);
    }
    let map = samples[0].tag_map.as_ref().unwrap().get(&GroupId::Default).unwrap();
    assert_eq!(map.get_t(TagId::RecordFrameRate) as Option<&f64>, Some(&25.0));
}

#[test]
fn upstream_bugfix_red_part_discovery_stops_at_gaps_and_ignores_copies() {
    let mut files = ClipFiles::new();
    let first = files.write("take_001.R3D", &[]);
    let middle = files.write("take_002.R3D", &[]);
    files.write("take_004.R3D", &[]);
    let copy = files.write("take_001 - Copy.R3D", &[]);
    let rmd = files.write("take.RMD", &[]);
    assert_eq!(RedR3d::detect_all_parts(&middle).unwrap(), [rmd, first, middle]);
    assert_eq!(RedR3d::detect_all_parts(&copy).unwrap(), [copy]);
}

#[test]
fn upstream_bugfix_red_short_metadata_keeps_following_valid_entries_and_local_fallbacks() {
    let mut metadata = entry(0x20, 0x6b, &[0]);
    metadata.extend(entry(0x10, 0x71, b"EPIC\0"));
    metadata.extend(entry(0x60, 0xbf, &24000u32.to_be_bytes()));
    metadata.extend(entry(0x10, 0x30, b"20261027"));
    metadata.extend(entry(0x10, 0x31, b"010203"));
    for malformed in [vec![0, 3, 0], vec![0, 20, 0x20, 0x6b]] {
        let mut parser = RedR3d::default();
        let mut map = GroupedTagMap::new();
        parser.parse_meta(&[metadata.clone(), malformed].concat(), &mut map, &InputOptions::default()).unwrap();
        assert_eq!(parser.model.as_deref(), Some("EPIC"));
        assert!((parser.record_framerate.unwrap() - 24000.0 / 1001.0).abs() < 1e-8);
        let defaults = map.get(&GroupId::Default).unwrap();
        assert_eq!((defaults.get_t(TagId::CreationDateUtc) as Option<&String>).map(String::as_str), Some("2026:10:27 01:02:03"));
    }
}

#[test]
fn upstream_bugfix_red_index_rejects_out_of_file_offsets() {
    let mut bytes = vec![0; 4 * 4096];
    let index = &mut bytes[2 * 4096..];
    index[..4].copy_from_slice(&76u32.to_be_bytes());
    index[4..8].copy_from_slice(b"RII\x01");
    index[0x40] = 1;
    index[0x44..0x48].copy_from_slice(&1u32.to_be_bytes());
    index[0x48..0x4c].copy_from_slice(&104u32.to_be_bytes());
    assert!(RedR3d::index_entries(&mut Cursor::new(bytes.clone()), 3 * 4096, 4096, 2, b"RII\x01", 1, None).unwrap().is_ok());
    bytes[2 * 4096 + 0x44..2 * 4096 + 0x48].copy_from_slice(&99u32.to_be_bytes());
    assert!(RedR3d::index_entries(&mut Cursor::new(bytes), 3 * 4096, 4096, 2, b"RII\x01", 1, None).unwrap().is_err());
}

#[test]
fn upstream_bugfix_red_missing_frames_join_video_and_imu_on_the_same_clock() {
    let metadata = entry(0x20, 0x66, &25f32.to_be_bytes());
    let mut first = header(&metadata);
    first.extend(frame(1, 10_000_000));
    first.extend(frame(2, 10_040_000));
    first.extend(imu(&[10_000_000, 10_040_000]));
    first.extend([0; 8]);
    let mut second = header(&metadata);
    second.extend(frame(5, 10_160_000));
    second.extend(frame(6, 10_200_000));
    second.extend(imu(&[10_080_000, 10_120_000, 10_160_000]));
    second.extend([0; 8]);
    let mut files = ClipFiles::new();
    let path = files.write("joined_001.R3D", &first);
    files.write("joined_002.R3D", &second);
    let samples = parse(&path, 1);
    assert_eq!(samples[1..].iter().map(|s| s.sample_index).collect::<Vec<_>>(), [0, 1, 2, 3]);
    assert_eq!(samples[1..].iter().map(|s| s.timestamp_ms).collect::<Vec<_>>(), [0.0, 40.0, 80.0, 120.0]);
    let gyro = (samples[0].tag_map.as_ref().unwrap().get(&GroupId::Gyroscope).unwrap().get_t(TagId::Data) as Option<&Vec<TimeVector3<f64>>>).unwrap();
    assert_eq!(gyro.iter().map(|s| s.t).collect::<Vec<_>>(), [0.0, 0.04, 0.08]);
    assert_eq!((gyro[0].x, gyro[0].y, gyro[0].z), (0.4, 0.5, 0.6));
}

#[test]
fn upstream_bugfix_red_uses_valid_index_instead_of_damaged_frame_size_and_walks_bad_index() {
    let mut bytes = header(&entry(0x20, 0x66, &25f32.to_be_bytes()));
    let mut image = frame(1, 10_000_000);
    image[..4].copy_from_slice(&u32::MAX.to_be_bytes());
    bytes.extend(image);
    let mut index = vec![0; 4096];
    index[..4].copy_from_slice(&76u32.to_be_bytes());
    index[4..8].copy_from_slice(b"RII\x01");
    index[0x40] = 1;
    index[0x44..0x48].copy_from_slice(&1u32.to_be_bytes());
    index[0x48..0x4c].copy_from_slice(&104u32.to_be_bytes());
    bytes.extend(index);
    let mut footer = vec![0; 4096];
    footer[..4].copy_from_slice(&104u32.to_be_bytes());
    footer[4..8].copy_from_slice(b"REOB");
    footer[0x40..0x48].copy_from_slice(&16384u64.to_be_bytes());
    footer[0x4c] = 1;
    footer[0x50..0x54].copy_from_slice(b"rdi\x01");
    footer[0x54..0x58].copy_from_slice(&2u32.to_be_bytes());
    footer[0x58..0x5c].copy_from_slice(&104u32.to_be_bytes());
    footer[0x5c..0x60].copy_from_slice(&1u32.to_be_bytes());
    footer[0x64..0x68].copy_from_slice(&1u32.to_be_bytes());
    bytes.extend(footer);
    let mut files = ClipFiles::new();
    let indexed = files.write("indexed.R3D", &bytes);
    let samples = parse(&indexed, 1);
    assert_eq!(samples.len(), 2);
    assert_eq!(samples[1].sample_index, 0);
    assert_eq!(samples[1].tag_map.as_ref().unwrap().get(&GroupId::Lens).unwrap().get_t(TagId::FocalLength) as Option<&f32>, Some(&50.0));
    bytes[4096..4100].copy_from_slice(&104u32.to_be_bytes());
    bytes[8192 + 0x44..8192 + 0x48].copy_from_slice(&99u32.to_be_bytes());
    let walked = files.write("walked.R3D", &bytes);
    assert_eq!(parse(&walked, 1).len(), 2);
}
