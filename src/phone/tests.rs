// SPDX-License-Identifier: MIT OR Apache-2.0

use super::*;

// These are metadata fixtures, not claims of physical-device validation.
const IPHONE_12_18_MODELS: &[(&str, &[f64])] = &[
    ("iPhone 12", &[26.0, 13.0]),
    ("iPhone 12 mini", &[26.0, 13.0]),
    ("iPhone 12 Pro", &[26.0, 13.0, 52.0]),
    ("iPhone 12 Pro Max", &[26.0, 13.0, 65.0]),
    ("iPhone 13", &[26.0, 13.0]),
    ("iPhone 13 mini", &[26.0, 13.0]),
    ("iPhone 13 Pro", &[26.0, 13.0, 77.0]),
    ("iPhone 13 Pro Max", &[26.0, 13.0, 77.0]),
    ("iPhone 14", &[26.0, 13.0]),
    ("iPhone 14 Plus", &[26.0, 13.0]),
    ("iPhone 14 Pro", &[24.0, 13.0, 77.0]),
    ("iPhone 14 Pro Max", &[24.0, 13.0, 77.0]),
    ("iPhone 15", &[26.0, 13.0]),
    ("iPhone 15 Plus", &[26.0, 13.0]),
    ("iPhone 15 Pro", &[24.0, 13.0, 77.0]),
    ("iPhone 15 Pro Max", &[24.0, 13.0, 120.0]),
    ("iPhone 16", &[26.0, 13.0]),
    ("iPhone 16 Plus", &[26.0, 13.0]),
    ("iPhone 16 Pro", &[24.0, 13.0, 120.0]),
    ("iPhone 16 Pro Max", &[24.0, 13.0, 120.0]),
    ("iPhone 16e", &[26.0]),
    ("iPhone 17", &[26.0, 13.0]),
    ("iPhone 17 Pro", &[24.0, 13.0, 100.0]),
    ("iPhone 17 Pro Max", &[24.0, 13.0, 100.0]),
    ("iPhone 17e", &[26.0]),
    ("iPhone Air", &[26.0]),
    ("iPhone 18 Pro", &[24.0, 13.0, 100.0]),
    ("iPhone 18 Pro Max", &[24.0, 13.0, 100.0]),
];

#[test]
fn iphone_12_through_18_variants_read_legacy_and_track_level_focal_length() {
    for &(model, focals) in IPHONE_12_18_MODELS {
        for &focal in focals {
            let legacy_model = format!("Apple {model} {focal}mm");
            let legacy_lens = format!("{model} {focal}mm");
            let legacy = atom(b"moov", &meta(&[
                (MODEL, 1, legacy_model.as_bytes()),
                (SOFTWARE, 1, b"Blackmagic Cam 2.3.000047"),
                (LENS, 1, legacy_lens.as_bytes()),
            ], false));
            let modern_model = format!("Apple {model}");
            let physical_lens = format!("{model} back camera 4.0mm f/1.8");
            let equivalent = format!("{focal:.2}mm");
            let movie = meta(&[(MODEL, 1, modern_model.as_bytes()), (SOFTWARE, 1, b"Blackmagic Cam 3.3"), (LENS, 1, b"")], false);
            let track = atom(b"trak", &meta(&[
                (APPLE_LENS, 1, physical_lens.as_bytes()),
                (EQUIVALENT_FOCAL, 1, equivalent.as_bytes()),
            ], true));
            let modern = atom(b"moov", &[movie, track].concat());
            for (bytes, expected_lens) in [(legacy, legacy_lens.as_str()), (modern, physical_lens.as_str())] {
                let (phone, map) = parse(&bytes, InputOptions::default());
                assert_eq!(phone.model.as_deref(), Some(model));
                assert_eq!(string(&map, GroupId::Lens, TagId::DisplayName).map(String::as_str), Some(expected_lens));
                assert_eq!(GetWithType::<f32>::get_t(&map[&GroupId::Lens], TagId::FocalLength), Some(&(focal as f32)));
                assert_eq!(phone.frame_readout_time(), None);
                assert!(!phone.has_accurate_timestamps());
            }
        }
    }
}

#[test]
fn equivalent_tag_wins_and_physical_lens_mm_are_never_used_as_equivalent() {
    let bytes = recording(&[(EQUIVALENT_FOCAL, 1, b"13.50 mm")], false);
    let (_, map) = parse(&bytes, InputOptions::default());
    assert_eq!(GetWithType::<f32>::get_t(&map[&GroupId::Lens], TagId::FocalLength), Some(&13.5));
    let bytes = atom(b"moov", &meta(&[
        (MODEL, 1, b"Apple iPhone 18 Pro"),
        (SOFTWARE, 1, b"Blackmagic Cam 3.3"),
        (APPLE_LENS, 1, b"iPhone 18 Pro back camera 6.765mm f/1.78"),
    ], false));
    let (_, map) = parse(&bytes, InputOptions::default());
    assert!(!map[&GroupId::Lens].contains_key(&TagId::FocalLength));
    for value in ["0mm", "-1mm", "NaNmm", "inf", "1e300mm"] {
        assert_eq!(equivalent_focal(value), None);
    }
}

#[test]
#[ignore = "Set PHONE_CAMERA_DB to validate the authoritative Apple table"]
fn authoritative_iphone_12_18_table() {
    let db = camera_db::CameraDatabase::load(&std::env::var("PHONE_CAMERA_DB").unwrap()).unwrap();
    let table = &db.get_brand("APPLE").unwrap().readout;
    let common_rates = [24.0, 25.0, 30.0, 48.0, 50.0, 60.0];
    assert_eq!(table.data.len(), IPHONE_12_18_MODELS.iter().map(|(_, focals)| focals.len()).sum::<usize>());
    for &(model, focals) in IPHONE_12_18_MODELS {
        for &focal in focals {
            let key = format!("{model} {focal}mm");
            let row = table.data.get(&key).unwrap_or_else(|| panic!("Missing {key}"));
            assert_eq!(row.len(), table.columns.len(), "{key}");
            assert!(row.iter().flatten().all(|v| v.is_finite() && *v < 0.0), "{key}");
            for (w, h) in [(3840, 2160), (1920, 1080)] {
                for fps in common_rates {
                    let estimate = lookup_readout(&db, &key, w, h, fps)
                        .unwrap_or_else(|| panic!("Missing fallback for {key} {w}x{h}@{fps}"));
                    assert!(estimate.0 > 0.0 && estimate.0 < 1000.0 / fps && estimate.1);
                    assert_eq!(lookup_readout(&db, &key, w, h, fps / 1.001), Some(estimate));
                }
            }
            if matches!(model, "iPhone 17 Pro" | "iPhone 17 Pro Max" | "iPhone 18 Pro" | "iPhone 18 Pro Max") {
                for h in [2240, 3024] {
                    for fps in common_rates {
                        let estimate = lookup_readout(&db, &key, 4224, h, fps)
                            .unwrap_or_else(|| panic!("Missing RAW fallback for {key} 4224x{h}@{fps}"));
                        assert!(estimate.0 > 0.0 && estimate.0 < 1000.0 / fps && estimate.1);
                        assert_eq!(lookup_readout(&db, &key, 4224, h, fps / 1.001), Some(estimate));
                    }
                }
            }
        }
        let main = format!("{model} {}mm", focals[0]);
        for fps in [100.0, 120.0] {
            let estimate = lookup_readout(&db, &main, 1920, 1080, fps).unwrap();
            assert!(estimate.0 > 0.0 && estimate.0 < 1000.0 / fps && estimate.1);
            assert_eq!(lookup_readout(&db, &main, 1920, 1080, fps / 1.001), Some(estimate));
            let uhd = lookup_readout(&db, &main, 3840, 2160, fps);
            if matches!(model, "iPhone 16 Pro" | "iPhone 16 Pro Max" | "iPhone 17 Pro" | "iPhone 17 Pro Max" | "iPhone 18 Pro" | "iPhone 18 Pro Max") {
                let estimate = uhd.unwrap();
                assert!(estimate.0 > 0.0 && estimate.0 < 1000.0 / fps && estimate.1);
            } else {
                assert_eq!(uhd, None, "{main} UHD@{fps}");
            }
        }
    }
    for model in ["iPhone 15 Pro", "iPhone 15 Pro Max"] {
        assert_eq!(lookup_readout(&db, &format!("{model} 24mm"), 3840, 2160, 25.0), Some((5.3, true)));
        assert_eq!(lookup_readout(&db, &format!("{model} 24mm"), 3840, 2160, 60.0), Some((5.3, true)));
    }
    assert_eq!(lookup_readout(&db, "iPhone 17 Pro 13mm", 3840, 2160, 25.0), Some((6.0, true)));
    assert_eq!(lookup_readout(&db, "iPhone 17 Pro 13mm", 4224, 2240, 25.0), Some((5.6, true)));
    assert_eq!(lookup_readout(&db, "iPhone 17 Pro Max 24mm", 4224, 2240, 60.0), Some((2.3, true)));
    // Cover every common rate within the sources' explicit measurement ranges.
    for (lens, w, h, rates, expected) in [
        ("iPhone 16 Pro Max 24mm", 3840, 2160, &[24.0, 25.0, 30.0, 48.0, 50.0, 60.0, 100.0, 120.0][..], 2.4),
        ("iPhone 16 Pro Max 13mm", 3840, 2160, &[24.0, 25.0, 30.0, 48.0, 50.0, 60.0][..], 5.8),
        ("iPhone 16 Pro Max 120mm", 3840, 2160, &[24.0, 25.0, 30.0, 48.0, 50.0, 60.0][..], 5.5),
        ("iPhone 17 Pro Max 24mm", 4224, 2240, &[24.0, 25.0, 30.0, 48.0, 50.0, 60.0][..], 2.3),
        ("iPhone 18 Pro Max 24mm", 4224, 2240, &[24.0, 25.0, 30.0, 48.0, 50.0, 60.0][..], 2.4),
    ] {
        for &fps in rates {
            assert_eq!(lookup_readout(&db, lens, w, h, fps), Some((expected, true)), "{lens} {w}x{h}@{fps}");
            assert_eq!(lookup_readout(&db, lens, w, h, fps / 1.001), Some((expected, true)));
        }
    }
    for (lens, w, h, fps, expected) in [
        ("iPhone 12 26mm", 3840, 2160, 25.0, 6.6),
        ("iPhone 12 Pro 26mm", 3840, 2160, 25.0, 6.8),
        ("iPhone 12 Pro Max 26mm", 3840, 2160, 24.0, 5.0),
        ("iPhone 13 Pro 26mm", 3840, 2160, 25.0, 6.8),
        ("iPhone 13 Pro 77mm", 3840, 2160, 30.0, 5.0),
        ("iPhone 13 Pro Max 26mm", 3840, 2160, 25.0, 6.8),
        ("iPhone 14 Pro 24mm", 3840, 2160, 25.0, 9.0),
        ("iPhone 14 26mm", 1920, 1080, 120.0, 5.5),
        ("iPhone 14 Plus 26mm", 1920, 1080, 120.0, 5.5),
        ("iPhone 14 Pro Max 24mm", 3840, 2160, 25.0, 9.0),
        ("iPhone 15 26mm", 3840, 2160, 25.0, 7.0),
        ("iPhone 15 Pro Max 24mm", 1920, 1080, 30.0, 7.3),
        ("iPhone 15 Pro Max 24mm", 1920, 1080, 120.0, 4.5),
        ("iPhone 16 Pro 24mm", 3840, 2160, 25.0, 2.4),
        ("iPhone 16e 26mm", 3840, 2160, 25.0, 7.0),
        ("iPhone 17e 26mm", 3840, 2160, 25.0, 7.0),
        ("iPhone Air 26mm", 3840, 2160, 25.0, 7.0),
        ("iPhone 17 Pro 24mm", 3840, 2160, 25.0, 3.0),
        ("iPhone 17 Pro 13mm", 4224, 3024, 24.0, 7.4),
        ("iPhone 17 Pro Max 24mm", 4224, 3024, 30.0, 3.1),
        ("iPhone 18 Pro Max 24mm", 3840, 2160, 25.0, 2.4),
        ("iPhone 18 Pro Max 24mm", 4224, 2240, 60.0, 2.4),
        ("iPhone 18 Pro Max 24mm", 4224, 3024, 25.0, 3.0),
        ("iPhone 18 Pro Max 13mm", 4224, 2240, 25.0, 6.0),
        ("iPhone 18 Pro 24mm", 3840, 2160, 25.0, 2.4),
        ("iPhone 18 Pro Max 13mm", 4224, 3024, 60.0, 8.1),
    ] {
        assert_eq!(lookup_readout(&db, lens, w, h, fps), Some((expected, true)), "{lens} {w}x{h}@{fps}");
        assert_eq!(lookup_readout(&db, lens, w, h, fps / 1.001), Some((expected, true)));
    }
    for (lens, w, h, fps) in [
        ("iPhone 12 Pro 26mm", 4224, 3024, 25.0),
        ("iPhone 12 Pro Max 26mm", 3840, 2160, 120.0),
        ("iPhone 13 Pro 26mm", 3840, 2160, 120.0),
        ("iPhone 14 Pro 24mm", 3840, 2160, 120.0),
        ("iPhone 15 Pro Max 24mm", 3840, 2160, 120.0),
        ("iPhone 16 Pro Max 13mm", 3840, 2160, 120.0),
        ("iPhone 16 26mm", 4224, 2240, 25.0),
        ("iPhone 17 26mm", 4224, 2240, 25.0),
        ("iPhone 17 Pro 24mm", 4096, 2160, 25.0),
        ("iPhone 17 Pro 24mm", 3840, 2160, 59.963),
        ("iPhone 18 Pro Max 48mm", 3840, 2160, 25.0),
        ("iPhone 18 Pro Max 13mm", 1920, 1080, 120.0),
        ("iPhone 18 Pro Max 24mm", 4224, 2240, 120.0),
    ] {
        assert_eq!(lookup_readout(&db, lens, w, h, fps), None, "{lens} {w}x{h}@{fps}");
    }
}
fn atom(name: &[u8; 4], value: &[u8]) -> Vec<u8> {
    [((value.len() + 8) as u32).to_be_bytes().as_slice(), name, value].concat()
}

fn meta(entries: &[(&str, u32, &[u8])], full_box: bool) -> Vec<u8> {
    let mut keys = [0u32.to_be_bytes(), (entries.len() as u32).to_be_bytes()].concat();
    let mut items = Vec::new();
    for (index, (key, typ, value)) in entries.iter().enumerate() {
        keys.extend(atom(b"mdta", key.as_bytes()));
        items.extend(atom(&((index + 1) as u32).to_be_bytes(), &atom(b"data", &[typ.to_be_bytes().as_slice(), &[0; 4], value].concat())));
    }
    let header = atom(b"hdlr", &[&[0; 8][..], b"mdta", &[0; 12]].concat());
    // Intentionally put ilst before keys: order must not alter key indexing.
    let mut payload = if full_box { vec![0; 4] } else { Vec::new() };
    payload.extend([header, atom(b"ilst", &items), atom(b"keys", &keys)].concat());
    atom(b"meta", &payload)
}

fn recording(extra: &[(&str, u32, &[u8])], full_box: bool) -> Vec<u8> {
    let mut entries = vec![
        (MODEL, 1, b"Apple iPhone 16 Pro Max 24mm".as_slice()),
        (SOFTWARE, 1, b"Blackmagic Cam 2.3.000047".as_slice()),
        (LENS, 1, b"iPhone 16 Pro Max 24mm".as_slice()),
        ("com.apple.quicktime.creationdate", 1, b"2025-05-09T20:45:42+0800".as_slice()),
    ];
    entries.extend_from_slice(extra);
    atom(b"moov", &meta(&entries, full_box))
}

fn parse(bytes: &[u8], options: InputOptions) -> (Phone, GroupedTagMap) {
    let mut phone = Phone::detect(bytes, "test.mov", &options).unwrap();
    let samples = phone.parse(&mut Cursor::new(bytes), bytes.len(), |_| {}, Arc::new(AtomicBool::new(false)), options).unwrap();
    (phone, samples.into_iter().next().unwrap().tag_map.unwrap())
}

fn string(map: &GroupedTagMap, group: GroupId, tag: TagId) -> Option<&String> {
    GetWithType::<String>::get_t(map.get(&group)?, tag)
}

#[test]
fn identity_and_zoned_date_do_not_require_database() {
    for full_box in [false, true] {
        for path in [None, Some("missing-phone-database".into())] {
            let bytes = recording(&[], full_box);
            let (phone, map) = parse(&bytes, InputOptions { camera_db_path: path, ..Default::default() });
            assert_eq!(phone.camera_type(), "Apple");
            assert_eq!(phone.model.as_deref(), Some("iPhone 16 Pro Max"));
            assert!(!phone.has_accurate_timestamps());
            assert_eq!(phone.frame_readout_time(), None);
            assert_eq!(string(&map, GroupId::Default, TagId::CreationDateUtc).map(String::as_str), Some("2025:05:09 12:45:42"));
            assert_eq!(string(&map, GroupId::Default, TagId::TimeZoneOffset).map(String::as_str), Some("+08:00"));
            assert_eq!(GetWithType::<f32>::get_t(&map[&GroupId::Lens], TagId::FocalLength), Some(&24.0));
            assert!(!map[&GroupId::Default].contains_key(&TagId::ImageStabilizer));
            assert!(!map[&GroupId::Default].contains_key(&TagId::Custom("SensorWidth".into())));
            assert!(!map.contains_key(&GroupId::Gyroscope));
        }
    }
}

#[test]
fn readout_is_microseconds_and_can_live_on_video_track() {
    let mut payload = recording(&[], false)[8..].to_vec();
    payload.extend(atom(b"trak", &meta(&[(READOUT, 21, &2400i32.to_be_bytes())], true)));
    let (phone, _) = parse(&atom(b"moov", &payload), InputOptions::default());
    assert_eq!(phone.frame_readout_time(), Some(2.4));
    for value in ["0", "-2.4", "NaN", "inf", "1/60"] {
        let (phone, _) = parse(&recording(&[(READOUT, 1, value.as_bytes())], false), InputOptions::default());
        assert_eq!(phone.frame_readout_time(), None, "{value}");
    }
}

#[test]
fn camera_and_unrelated_app_are_not_claimed() {
    let options = InputOptions::default();
    for data in [b"Blackmagic Design com.blackmagic-design.cameraType 24mm".as_slice(), b"Apple iPhone 16 Pro Max 24mm".as_slice()] {
        assert!(Phone::detect(data, "test.mov", &options).is_none());
    }
    let bytes = atom(b"moov", &meta(&[(MODEL, 1, b"Blackmagic Cinema Camera 6K"), (SOFTWARE, 1, b"Blackmagic Cam 2.3")], false));
    let mut phone = Phone::default();
    assert!(phone.parse(&mut Cursor::new(&bytes), bytes.len(), |_| {}, Arc::new(AtomicBool::new(false)), options.clone()).is_err());
    let bytes = atom(b"free", &recording(&[], false));
    let mut phone = Phone::detect(&bytes, "test.mov", &options).unwrap();
    assert!(phone.parse(&mut Cursor::new(&bytes), bytes.len(), |_| {}, Arc::new(AtomicBool::new(false)), options).is_err());
}

#[test]
fn focal_length_must_be_a_positive_final_mm_token() {
    assert_eq!(split_focal_length("iPhone 16 Pro Max 24mm"), ("iPhone 16 Pro Max", Some(24.0)));
    assert_eq!(split_focal_length("iPhone 16 Pro Max 13.5mm"), ("iPhone 16 Pro Max", Some(13.5)));
    for name in ["iPhone 16 Pro Max", "iPhone 16 Pro Max NaNmm", "iPhone 16 Pro Max 1e300mm", "iPhone 16 Pro Max -24mm", "iPhone 16 Pro Max 24mm f1.8"] {
        assert_eq!(split_focal_length(name), (name, None));
    }
}

struct TestDb(std::path::PathBuf);
impl TestDb {
    fn new() -> Self {
        let id = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
        let path = std::env::temp_dir().join(format!("telemetry-phone-{}-{id}", std::process::id()));
        std::fs::create_dir(&path).unwrap();
        std::fs::write(path.join("apple.json"), r#"{"readout":{"columns":["3840x2160@60","1920x1080@60"],"data":{"iPhone 16 Pro Max 24mm":[-2.4,4.0],"iPhone 16 Pro Max 13mm":[-5.8,null]}}}"#).unwrap();
        Self(path)
    }
    fn load(&self) -> camera_db::CameraDatabase { camera_db::CameraDatabase::load(self.0.to_str().unwrap()).unwrap() }
}
impl Drop for TestDb {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(self.0.join("apple.json"));
        let _ = std::fs::remove_dir(&self.0);
    }
}

#[test]
fn readout_requires_exact_lens_dimensions_and_rate() {
    let dir = TestDb::new();
    let db = dir.load();
    assert_eq!(lookup_readout(&db, "iPhone 16 Pro Max 24mm", 3840, 2160, 60.0), Some((2.4, true)));
    assert_eq!(lookup_readout(&db, "iPhone 16 Pro Max 24mm", 3840, 2160, 60000.0 / 1001.0), Some((2.4, true)));
    assert_eq!(lookup_readout(&db, "iPhone 16 Pro Max 24mm", 1920, 1080, 60.0), Some((4.0, false)));
    assert_eq!(lookup_readout(&db, "iPhone 16 Pro Max 13mm", 3840, 2160, 60.0), Some((5.8, true)));
    for (lens, w, h, fps) in [
        ("iPhone 16 Pro Max 13mm", 1920, 1080, 60.0),
        ("iPhone 16 Pro Max 48mm", 3840, 2160, 60.0),
        ("iPhone 16 Pro 24mm", 3840, 2160, 60.0),
        ("iPhone 16 Pro Max 24mm", 3840, 2160, 120.0),
        ("iPhone 16 Pro Max 24mm", 3840, 2160, 59.963),
        ("iPhone 16 Pro Max 24mm", 4096, 2160, 60.0),
        ("iPhone 16 Pro Max 24mm", 2160, 3840, 60.0),
    ] {
        assert_eq!(lookup_readout(&db, lens, w, h, fps), None, "{lens} {w} {h} {fps}");
    }
}

#[test]
fn malformed_and_extended_atoms_respect_parent_boundaries() {
    let bytes = recording(&[], false);
    for end in 0..bytes.len() {
        let mut out = BTreeMap::new();
        let _ = read_metadata(&mut Cursor::new(&bytes[..end]), end as u64, 0, &mut out);
    }
    let extended = [&1u32.to_be_bytes()[..], b"moov", &((bytes.len() + 8) as u64).to_be_bytes(), &bytes[8..]].concat();
    assert_eq!(parse(&extended, InputOptions::default()).0.model.as_deref(), Some("iPhone 16 Pro Max"));
    let mut invalid = bytes;
    invalid[8..12].copy_from_slice(&u32::MAX.to_be_bytes());
    let mut out = BTreeMap::new();
    assert!(read_metadata(&mut Cursor::new(&invalid), invalid.len() as u64, 0, &mut out).is_err());
}

#[test]
fn creation_date_keeps_subseconds_and_crosses_day_boundary() {
    let bytes = recording(&[("com.apple.quicktime.creationdate", 1, b"2025-05-09T00:01:02.125+08:00")], false);
    let (_, map) = parse(&bytes, InputOptions::default());
    assert_eq!(string(&map, GroupId::Default, TagId::CreationDateUtc).map(String::as_str), Some("2025:05:08 16:01:02.125000000"));
}

#[test]
#[ignore = "Set PHONE_SAMPLE_DIR to the two original Blackmagic Camera clips"]
fn real_iphone_clips() {
    let dir = std::path::PathBuf::from(std::env::var_os("PHONE_SAMPLE_DIR").unwrap());
    let camera_db_path = std::env::var("PHONE_CAMERA_DB").ok();
    let expected_readout = camera_db_path.as_ref().map(|_| 2.4);
    for (name, utc) in [("A001_05092045_C020.mov", "2025:05:09 12:45:42"), ("A001_05092046_C022.mov", "2025:05:09 12:46:09")] {
        let path = dir.join(name);
        let mut stream = std::fs::File::open(&path).unwrap();
        let size = stream.metadata().unwrap().len() as usize;
        let input = Input::from_stream_with_options(&mut stream, size, &path, |_| {}, Arc::new(AtomicBool::new(false)), InputOptions {
            camera_db_path: camera_db_path.clone(), ..Default::default()
        }).unwrap();
        assert_eq!(input.camera_type(), "Apple");
        assert_eq!(input.camera_model().map(String::as_str), Some("iPhone 16 Pro Max"));
        assert_eq!(input.frame_readout_time(), expected_readout);
        assert!(!input.has_accurate_timestamps());
        let map = input.samples.as_ref().unwrap()[0].tag_map.as_ref().unwrap();
        if expected_readout.is_some() {
            assert_eq!(GetWithType::<bool>::get_t(&map[&GroupId::Imager], TagId::Custom("readout_estimated".into())), Some(&true));
        }
        assert_eq!(string(map, GroupId::Default, TagId::CreationDateUtc).map(String::as_str), Some(utc));
        assert_eq!(GetWithType::<f32>::get_t(&map[&GroupId::Lens], TagId::PixelFocalLength), Some(&1280.0));
        assert_eq!(GetWithType::<f64>::get_t(&map[&GroupId::Lens], TagId::Custom("unit_pixel_focal_length".into())), Some(&(1920.0 / 36.0)));
        assert!(util::normalized_imu(&input, None).unwrap().is_empty());
        println!("{name}: Apple iPhone 16 Pro Max, equivalent=24mm, fx=1280px (estimate), UTC={utc}, readout={expected_readout:?}ms (estimate when present), IMU=0");

        // Remove the recorded focal suffix in memory; keep the scale for manual focal entry.
        let mut without_focal = std::fs::read(&path).unwrap();
        for offset in 0..without_focal.len().saturating_sub(3) {
            if &without_focal[offset..offset + 4] == b"24mm" {
                without_focal[offset..offset + 4].copy_from_slice(b"    ");
            }
        }
        let modified = Input::from_stream(&mut Cursor::new(&without_focal), without_focal.len(), &path, |_| {}, Arc::new(AtomicBool::new(false))).unwrap();
        let map = modified.samples.as_ref().unwrap()[0].tag_map.as_ref().unwrap();
        assert_eq!(modified.camera_model().map(String::as_str), Some("iPhone 16 Pro Max"));
        assert_eq!(GetWithType::<f64>::get_t(&map[&GroupId::Lens], TagId::Custom("unit_pixel_focal_length".into())), Some(&(1920.0 / 36.0)));
        assert!(!map[&GroupId::Lens].contains_key(&TagId::FocalLength));
        assert!(!map[&GroupId::Lens].contains_key(&TagId::PixelFocalLength));

        // A front camera may report the same equivalent mm; never borrow the rear row.
        let db = TestDb::new();
        let options = InputOptions { camera_db_path: Some(db.0.to_string_lossy().into_owned()), ..Default::default() };
        let original = std::fs::read(&path).unwrap();
        let control = Input::from_stream_with_options(&mut Cursor::new(&original), original.len(), &path, |_| {}, Arc::new(AtomicBool::new(false)), options.clone()).unwrap();
        assert_eq!(control.frame_readout_time(), Some(4.0));
        let mut front = original;
        front.extend(meta(&[
            (APPLE_LENS, 1, b"iPhone 16 Pro Max front camera 2.69mm f/1.9"),
            (EQUIVALENT_FOCAL, 1, b"24.00mm"),
        ], true));
        let parsed = Input::from_stream_with_options(&mut Cursor::new(&front), front.len(), &path, |_| {}, Arc::new(AtomicBool::new(false)), options).unwrap();
        assert_eq!(parsed.frame_readout_time(), None);
    }
}
