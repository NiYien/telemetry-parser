// SPDX-License-Identifier: MIT OR Apache-2.0

use super::*;
use crate::tags_impl::GetWithType;
use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};

struct TestDb(std::path::PathBuf);
impl TestDb {
    fn new(value: serde_json::Value) -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let path = std::env::temp_dir().join(format!(
            "telemetry-kinefinity-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        std::fs::write(
            path.join("kinefinity.json"),
            serde_json::to_vec(&value).unwrap(),
        )
        .unwrap();
        Self(path)
    }
    fn load(&self) -> crate::camera_db::CameraDatabase {
        crate::camera_db::CameraDatabase::load(self.0.to_str().unwrap()).unwrap()
    }
    fn options(&self) -> InputOptions {
        InputOptions {
            camera_db_path: Some(self.0.to_string_lossy().into_owned()),
            ..Default::default()
        }
    }
}
impl Drop for TestDb {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(self.0.join("kinefinity.json"));
        let _ = std::fs::remove_dir(&self.0);
    }
}

fn vista_db() -> TestDb {
    TestDb::new(json!({"models":{"VISTA":{"sw":36,"kinefinity":{
        "sensor_size":[6016,3984],
        "sensor_widths":{"3840":23,"5760":34.5},
        "oversampling":[
            {"format":"FULL","output":[4096,2160],"source":[6016,3172]},
            {"format":"FULL","output":[3840,2160],"source":[5760,3240]}
        ],
        "readout":[{"height":3984,"ms":-18}]
    }}}}))
}

fn atom(kind: &[u8; 4], payload: &[u8]) -> Vec<u8> {
    let mut out = ((payload.len() + 8) as u32).to_be_bytes().to_vec();
    out.extend_from_slice(kind);
    out.extend_from_slice(payload);
    out
}

fn meta(fields: &[(&str, &str)], fullbox: bool) -> Vec<u8> {
    let mut keys = vec![0; 4];
    keys.extend_from_slice(&(fields.len() as u32).to_be_bytes());
    let mut ilst = Vec::new();
    for (i, (key, value)) in fields.iter().enumerate() {
        keys.extend(atom(b"mdta", key.as_bytes()));
        let mut data = vec![0, 0, 0, 1, 0, 0, 0, 0];
        data.extend_from_slice(value.as_bytes());
        ilst.extend(atom(&((i + 1) as u32).to_be_bytes(), &atom(b"data", &data)));
    }
    let mut payload = if fullbox { vec![0; 4] } else { vec![] };
    payload.extend(atom(b"hdlr", &[0; 25]));
    payload.extend(atom(b"keys", &keys));
    payload.extend(atom(b"ilst", &ilst));
    atom(b"meta", &payload)
}

#[test]
fn slate_old_and_new_preserve_optional_fields() {
    let old = "# SLATE.TXT Revision 2.0\r\nCamera Model........: MAVO LF\r\nWidth...............: 6016\r\nHeight..............: 3172\r\nSensor FPS..........: 48\r\nFocal Length........: N/A\r\nShot date...........: 2026.7.14\r\nShot TOD............: 17:05:12\r\n";
    let data = parse_slate_content(old).unwrap();
    assert_eq!(data.camera_model.as_deref(), Some("MAVO LF"));
    assert_eq!(
        (data.width, data.height, data.sensor_fps),
        (Some(6016), Some(3172), Some(48.0))
    );
    assert!(data.focal_length.is_none());
    assert_eq!(data.shot_tod.as_deref(), Some("17:05:12"));
    let new = parse_slate_content("# KINEFINITY CAMERA REPORT\nCamera Model: VISTA\nImage Format: FF\nOversampling: No\nSensor FPS: 60\nProject FPS: 30\nFocal Length: 35mm\nFirmware Rev: 10144\n").unwrap();
    assert_eq!(
        (
            new.oversampling,
            new.sensor_fps,
            new.project_fps,
            new.focal_length
        ),
        (Some(false), Some(60.0), Some(30.0), Some(35.0))
    );
    assert_eq!(new.firmware.as_deref(), Some("10144"));
    for text in ["", "garbage", "Focal Length: -1", "Camera Model: N/A"] {
        assert!(parse_slate_content(text).is_none());
    }
}

#[test]
fn slate_rejects_invalid_numbers_and_dates() {
    for value in ["0", "-1", "NaN", "inf", "N/A"] {
        let data = parse_slate_content(&format!(
            "Camera Model: VISTA\nFocal Length: {value}\nSensor FPS: {value}"
        ))
        .unwrap();
        assert!(data.focal_length.is_none() && data.sensor_fps.is_none());
    }
    assert_eq!(
        normalize_slate_date("2026.7.14").as_deref(),
        Some("2026:07:14")
    );
    assert_eq!(
        normalize_slate_date("2026/07/14").as_deref(),
        Some("2026:07:14")
    );
    for value in ["", "2026.2.30", "2026.13.1", "2026.7", "2026.7.14.1"] {
        assert!(normalize_slate_date(value).is_none());
    }
}

#[test]
fn jsonl_uses_last_committed_clip_and_ignores_truncated_tail() {
    let data = metadata::parse_jsonl(concat!(
        "{\"event\":\"OPEN\",\"clip_uuid\":\"a\",\"model\":\"VISTA\",\"focal_current\":35}\n",
        "{\"event\":\"COMMIT\",\"clip_uuid\":\"a\",\"tc_start\":\"10:00:00:00\"}\n",
        "{\"event\":\"OPEN\",\"clip_uuid\":\"b\",\"model\":\"VISTA\",\"focal_current\":0}\n",
        "{\"event\":\"COMMIT\",\"clip_uuid\":\"wrong\"}\n{bad"
    ))
    .unwrap();
    assert_eq!(data.clip_uuid.as_deref(), Some("a"));
    assert_eq!(data.focal_length, Some(35.0));
    assert_eq!(data.timecode.as_deref(), Some("10:00:00:00"));
}

#[test]
fn second_meta_is_read_and_key_tables_do_not_leak_between_boxes() {
    for fullbox in [false, true] {
        let mut moov = meta(&[], fullbox);
        moov.extend(meta(
            &[
                ("com.kinefinity.camera_model", "VISTA"),
                ("com.kinefinity.sensor_fps", "30"),
            ],
            fullbox,
        ));
        let parsed = metadata::parse_moov(&moov);
        assert_eq!(parsed.camera_model.as_deref(), Some("VISTA"));
        assert_eq!(parsed.sensor_fps, Some(30.0));
        assert!(metadata::detect_metadata(&atom(b"moov", &moov)));
    }
    let sony = atom(
        b"moov",
        &meta(&[("com.apple.quicktime.make", "Sony")], false),
    );
    assert!(!metadata::detect_metadata(&sony));
}

#[test]
fn prores_vendor_detection_is_case_insensitive_and_structural() {
    for vendor in [b"KINE", b"kine"] {
        let mut frame = vec![0; 32];
        frame[..4].copy_from_slice(&32u32.to_be_bytes());
        frame[4..8].copy_from_slice(b"icpf");
        frame[12..16].copy_from_slice(vendor);
        assert!(metadata::detect_prores(&atom(b"mdat", &frame)));
        frame[4..8].copy_from_slice(b"xxxx");
        assert!(!metadata::detect_prores(&atom(b"mdat", &frame)));
    }
    assert!(!metadata::detect_prores(&atom(
        b"free",
        b"random KINE bytes"
    )));
}

#[test]
fn truncated_atoms_do_not_panic_or_identify_other_cameras() {
    let valid = atom(
        b"moov",
        &meta(&[("com.kinefinity.camera_model", "VISTA")], false),
    );
    for end in 0..valid.len() {
        assert!(!metadata::detect_metadata(&valid[..end]));
        let _ = metadata::parse_moov(&valid[..end]);
        let _ = metadata::detect_prores(&valid[..end]);
    }
    for length in [0, 1, 2, 7, u32::MAX] {
        let mut invalid = length.to_be_bytes().to_vec();
        invalid.extend_from_slice(b"moov");
        let _ = metadata::detect_metadata(&invalid);
    }
}

#[test]
fn vista_geometry_distinguishes_full_frame_oversampling_and_native_crop() {
    let db = vista_db().load();
    let full =
        resolve_camera_geometry(&db, "VISTA", (3840, 2160), 30.0, Some("FF"), Some(true)).unwrap();
    let crop = resolve_camera_geometry(&db, "VISTA", (3840, 2160), 30.0, Some("S35"), Some(false))
        .unwrap();
    assert_eq!(full.source_size, Some((5760, 3240)));
    assert_eq!(crop.source_size, Some((3840, 2160)));
    assert!((full.unit_pixel_focal_length - 111.304347826).abs() < 1e-6);
    assert!((crop.unit_pixel_focal_length - 166.956521739).abs() < 1e-6);
    assert!(
        (full.readout.unwrap().readout_time_ms / crop.readout.unwrap().readout_time_ms - 1.5).abs()
            < 1e-9
    );
    assert!(resolve_camera_geometry(&db, "VISTA", (3840, 2160), 30.0, None, None).is_none());
    assert!(
        resolve_camera_geometry(&db, "VISTA", (1234, 567), 30.0, Some("FF"), Some(true)).is_none()
    );
}

#[test]
fn height_estimates_keep_the_reference_and_do_not_scale_with_fps() {
    let db = vista_db().load();
    for fps in [0.2, 24.0, 25.0, 29.97, 30.0, 39.0] {
        let g = resolve_camera_geometry(&db, "VISTA", (6016, 3984), fps, Some("FF"), Some(false))
            .unwrap();
        let rt = g.readout.unwrap();
        assert_eq!(rt.readout_time_ms, 18.0);
        assert!(rt.is_estimated);
        assert!((g.unit_pixel_focal_length - 6016.0 / 36.0).abs() < 1e-9);
    }
    let dci =
        resolve_camera_geometry(&db, "VISTA", (6016, 3172), 30.0, Some("FF"), Some(false)).unwrap();
    let over = resolve_camera_geometry(&db, "VISTA", (4096, 2160), 30.0, Some("FF"), None).unwrap();
    assert_eq!(
        dci.readout.unwrap().readout_time_ms,
        over.readout.unwrap().readout_time_ms
    );
    assert!(
        resolve_camera_geometry(&db, "VISTA", (6016, 3984), f64::NAN, Some("FF"), None).is_none()
    );
}

#[test]
fn raw_metadata_survives_missing_database_and_does_not_invent_lens_or_utc() {
    let mut camera = Kinefinity::default();
    let mut map = GroupedTagMap::new();
    let data = parse_slate_content("Camera Model: VISTA\nFocal Length: 35mm\nSensor FPS: 60\nProject FPS: 30\nShot date: 2026.9.27\nShot TOD: 10:07:08").unwrap();
    camera.process_map(&mut map, &InputOptions::default(), None, &data);
    assert_eq!(camera.model.as_deref(), Some("VISTA"));
    assert!(camera.lens.is_none());
    let default = &map[&GroupId::Default];
    assert_eq!(default.get_t(TagId::FrameRate) as Option<&f64>, Some(&30.0));
    assert_eq!(
        default.get_t(TagId::RecordFrameRate) as Option<&f64>,
        Some(&60.0)
    );
    assert!(!default.contains_key(&TagId::CreationDateUtc));
    assert_eq!(
        map[&GroupId::Lens].get_t(TagId::FocalLength) as Option<&f32>,
        Some(&35.0)
    );
    assert!(!map.contains_key(&GroupId::Imager));
}

#[test]
fn automatic_metadata_carries_geometry_estimates_and_honors_manual_focal_length() {
    let db = vista_db();
    let mut options = db.options();
    options.user_focal_length = Some(50.0);
    let data = SlateData {
        camera_model: Some("VISTA".into()),
        image_format: Some("FF".into()),
        oversampling: Some(false),
        width: Some(1),
        height: Some(1),
        sensor_fps: Some(60.0),
        focal_length: Some(35.0),
        creation_utc: Some("2026:09:27 02:07:08".into()),
        ..Default::default()
    };
    let video = VideoMetadata {
        width: 6016,
        height: 2520,
        fps: 30.0,
        ..Default::default()
    };
    let mut camera = Kinefinity::default();
    let mut map = GroupedTagMap::new();
    camera.process_map(&mut map, &options, Some(&video), &data);
    assert_eq!(
        map[&GroupId::Lens].get_t(TagId::FocalLength) as Option<&f32>,
        Some(&50.0)
    );
    let default = &map[&GroupId::Default];
    assert_eq!(
        default.get_t(TagId::CreationDateUtc) as Option<&String>,
        Some(&"2026:09:27 02:07:08".to_owned())
    );
    let additional: &serde_json::Value = default.get_t(TagId::Metadata).unwrap();
    assert_eq!(additional["readout_estimated"], true);
    assert_eq!(additional["sensor_fps"], 60.0);
    assert_eq!(additional["project_fps"], 30.0);
    assert_eq!(additional["kinefinity_source_size"], json!([6016, 2520]));
    assert!((camera.frame_readout_time.unwrap() - 11.3855421687).abs() < 1e-6);
}

#[test]
fn free_run_timecode_does_not_enable_filename_date_guessing() {
    let mut camera = Kinefinity::default();
    let mut map = GroupedTagMap::new();
    let data = SlateData {
        camera_model: Some("VISTA".into()),
        timecode: Some("10:07:09:08".into()),
        shot_date: Some("2026.9.27".into()),
        shot_tod: Some("10:07:08".into()),
        ..Default::default()
    };
    camera.process_map(&mut map, &InputOptions::default(), None, &data);
    let default = &map[&GroupId::Default];
    assert!(!default.contains_key(&TagId::CreationDateUtc));
    assert!(!default.contains_key(&TagId::Custom("timecode".into())));
    let metadata: &serde_json::Value = default.get_t(TagId::Metadata).unwrap();
    assert_eq!(metadata["recorded_timecode"], "10:07:09:08");
    assert_eq!(metadata["recorded_local_time"], "2026:09:27 10:07:08");
}

#[test]
fn unknown_mavo_does_not_inherit_the_original_mavo_sensor() {
    let db = TestDb::new(json!({"models":{"MAVO":{"sw":24},"VISTA":{"sw":36}}}));
    assert_eq!(
        geometry::find_model(&db.load(), "Kinefinity VISTA")
            .unwrap()
            .0,
        "VISTA"
    );
    assert!(geometry::find_model(&db.load(), "MAVO future LF").is_none());
    let mut camera = Kinefinity::default();
    let mut map = GroupedTagMap::new();
    let data = SlateData {
        camera_model: Some("MAVO future LF".into()),
        width: Some(6016),
        height: Some(3984),
        sensor_fps: Some(30.0),
        ..Default::default()
    };
    camera.process_map(&mut map, &db.options(), None, &data);
    assert_eq!(camera.model.as_deref(), Some("MAVO future LF"));
    assert!(!map[&GroupId::Default].contains_key(&TagId::Custom("SensorWidth".into())));
    assert!(!map.contains_key(&GroupId::Lens));
}

#[test]
fn sidecar_uuid_mismatch_does_not_replace_embedded_metadata() {
    let tmp = vista_db();
    let jsonl = tmp.0.join("clip.kmeta.jsonl");
    let slate = tmp.0.join("clip-slate.txt");
    std::fs::write(
        &jsonl,
        "{\"event\":\"OPEN\",\"clip_uuid\":\"wrong\",\"focal_current\":100}\n",
    )
    .unwrap();
    std::fs::write(
        &slate,
        "Camera Model: VISTA\nClip UUID: expected\nFocal Length: 35mm\n",
    )
    .unwrap();
    let mut data = SlateData {
        clip_uuid: Some("expected".into()),
        camera_model: Some("VISTA".into()),
        ..Default::default()
    };
    metadata::merge_sidecars(tmp.0.join("clip.mov").to_str().unwrap(), &mut data);
    assert_eq!(data.focal_length, Some(35.0));
    assert_eq!(data.clip_uuid.as_deref(), Some("expected"));
    std::fs::remove_file(jsonl).unwrap();
    std::fs::remove_file(slate).unwrap();
}

#[test]
fn embedded_brand_detects_mov_without_a_prores_signature() {
    let db = vista_db();
    let bytes = atom(
        b"moov",
        &meta(
            &[
                ("com.apple.quicktime.make", "Kinefinity"),
                ("com.apple.quicktime.model", "VISTA"),
                ("com.kinefinity.width", "6016"),
                ("com.kinefinity.height", "3984"),
                ("com.kinefinity.sensor_fps", "30"),
            ],
            false,
        ),
    );
    let mut options = db.options();
    options.dont_look_for_sidecar_files = true;
    let input = Input::from_stream_with_options(
        &mut std::io::Cursor::new(&bytes),
        bytes.len(),
        "video.mov",
        |_| (),
        Arc::new(AtomicBool::new(false)),
        options,
    )
    .unwrap();
    assert_eq!(input.camera_type(), "Kinefinity");
    assert_eq!(input.camera_model().map(String::as_str), Some("VISTA"));
    assert_eq!(input.frame_readout_time(), Some(18.0));
}

#[test]
#[ignore = "Set KINEFINITY_CAMERA_DB to validate the authoritative data package"]
fn authoritative_database_modes() {
    let path = std::env::var("KINEFINITY_CAMERA_DB").unwrap();
    let db = crate::camera_db::CameraDatabase::load(&path).unwrap();
    let raw: serde_json::Value = serde_json::from_slice(
        &std::fs::read(std::path::Path::new(&path).join("kinefinity.json")).unwrap(),
    )
    .unwrap();
    let mut count = 0;
    for (model, data) in raw["models"].as_object().unwrap() {
        for mode in data["kinefinity"]["oversampling"].as_array().unwrap() {
            let out = (
                mode["output"][0].as_u64().unwrap() as u32,
                mode["output"][1].as_u64().unwrap() as u32,
            );
            let source = (
                mode["source"][0].as_u64().unwrap() as u32,
                mode["source"][1].as_u64().unwrap() as u32,
            );
            let format = mode["format"].as_str().unwrap();
            for over in [Some(true), None] {
                let g = resolve_camera_geometry(&db, model, out, 30.0, Some(format), over)
                    .unwrap_or_else(|| panic!("{model} {out:?} {format}"));
                assert_eq!(g.source_size, Some(source));
                assert!(g.unit_pixel_focal_length.is_finite() && g.unit_pixel_focal_length > 0.0);
                if let Some(readout) = g.readout {
                    assert!(readout.is_estimated && readout.readout_time_ms > 0.0);
                }
            }
            count += 1;
        }
    }
    let native = [
        ("FF", 6016, 3984),
        ("FF", 6016, 3172),
        ("FF", 6016, 2520),
        ("FF", 5760, 3700),
        ("FF", 5760, 3240),
        ("FF", 5760, 2400),
        ("FF", 5120, 3700),
        ("FF", 5120, 2700),
        ("FF", 5120, 2160),
        ("FF", 4608, 3700),
        ("S35", 4096, 3432),
        ("S35", 4096, 3072),
        ("S35", 4096, 2700),
        ("S35", 4096, 2160),
        ("S35", 4096, 1600),
        ("S35", 3840, 2160),
        ("S35", 3840, 1600),
        ("S35", 3712, 2700),
        ("S35", 3328, 2700),
        ("M43", 3072, 1620),
        ("M43", 3072, 1200),
        ("M43", 2944, 1620),
        ("M43", 2944, 1200),
        ("S16", 2048, 1080),
        ("S16", 2048, 860),
        ("S16", 1920, 1080),
        ("S16", 1920, 800),
    ];
    for (format, w, h) in native {
        let g =
            resolve_camera_geometry(&db, "VISTA", (w, h), 30.0, Some(format), Some(false)).unwrap();
        assert_eq!(g.source_size, Some((w, h)));
        assert!((g.readout.unwrap().readout_time_ms - 18.0 * h as f64 / 3984.0).abs() < 1e-9);
    }
    assert_eq!(
        db.find_model("KINEFINITY", "MAVO mark2 LF").unwrap().0,
        "MAVO2 LF"
    );
    assert_eq!(
        db.find_model("KINEFINITY", "MAVO MARK2 S35").unwrap().0,
        "MAVO2 S35"
    );
    assert_eq!(db.find_model("KINEFINITY", "MAVO LF").unwrap().0, "MAVO LF");
    println!("Validated {count} oversampling mappings, 27 native VISTA modes, and model aliases");
}

#[test]
#[ignore = "Set KINEFINITY_SAMPLE and KINEFINITY_CAMERA_DB to inspect a real camera file"]
fn real_vista_sample() {
    let path = std::env::var("KINEFINITY_SAMPLE").unwrap();
    let mut stream = std::fs::File::open(&path).unwrap();
    let size = stream.metadata().unwrap().len() as usize;
    let options = InputOptions {
        camera_db_path: Some(std::env::var("KINEFINITY_CAMERA_DB").unwrap()),
        ..Default::default()
    };
    let input = Input::from_stream_with_options(
        &mut stream,
        size,
        &path,
        |_| (),
        Arc::new(AtomicBool::new(false)),
        options,
    )
    .unwrap();
    assert_eq!(input.camera_model().map(String::as_str), Some("VISTA"));
    assert_eq!(input.frame_readout_time(), Some(18.0));
    let map = input.samples.as_ref().unwrap()[0].tag_map.as_ref().unwrap();
    let focal: Option<&f32> = map
        .get(&GroupId::Lens)
        .and_then(|g| g.get_t(TagId::FocalLength));
    assert!(focal.is_none());
    let additional: &serde_json::Value = map[&GroupId::Default].get_t(TagId::Metadata).unwrap();
    assert_eq!(additional["readout_estimated"], true);
    assert_eq!(
        map[&GroupId::Default].get_t(TagId::CreationDateUtc) as Option<&String>,
        Some(&"2026:09:27 02:07:08".to_owned())
    );
    println!(
        "VISTA verified: model={:?}, readout={:?}, metadata={}",
        input.camera_model(),
        input.frame_readout_time(),
        additional
    );
}
