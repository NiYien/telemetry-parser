// SPDX-License-Identifier: MIT OR Apache-2.0

use super::*;

fn entry(data: &mut [u8], pos: usize, tag: u16, typ: u16, count: u32, value: u32) {
    data[pos..pos + 2].copy_from_slice(&tag.to_le_bytes());
    data[pos + 2..pos + 4].copy_from_slice(&typ.to_le_bytes());
    data[pos + 4..pos + 8].copy_from_slice(&count.to_le_bytes());
    data[pos + 8..pos + 12].copy_from_slice(&value.to_le_bytes());
}

fn atom(name: &[u8; 4], data: &[u8]) -> Vec<u8> {
    let mut result = ((data.len() + 8) as u32).to_be_bytes().to_vec();
    result.extend(name);
    result.extend(data);
    result
}

fn tiff(model: &str, focal_tenths: u32, equivalent: u16) -> Vec<u8> {
    let name = format!("{model}\0");
    let exif_offset = 38 + name.len();
    let rational_offset = exif_offset + 30;
    let mut data = vec![0u8; rational_offset + 8];
    data[..8].copy_from_slice(b"II\x2a\0\x08\0\0\0");
    data[8..10].copy_from_slice(&2u16.to_le_bytes());
    entry(&mut data, 10, 0x0110, 2, name.len() as u32, 38);
    entry(&mut data, 22, 0x8769, 4, 1, exif_offset as u32);
    data[38..exif_offset].copy_from_slice(name.as_bytes());
    data[exif_offset..exif_offset + 2].copy_from_slice(&2u16.to_le_bytes());
    entry(&mut data, exif_offset + 2, 0x920a, 5, 1, rational_offset as u32);
    entry(&mut data, exif_offset + 14, 0xa405, 3, 1, equivalent as u32);
    data[rational_offset..rational_offset + 4].copy_from_slice(&focal_tenths.to_le_bytes());
    data[rational_offset + 4..].copy_from_slice(&10u32.to_le_bytes());
    data
}

#[test]
fn leic_reads_both_focal_lengths_without_a_lens_name() {
    for (focal, equivalent) in [(125, 30), (340, 81)] {
        let bytes = atom(b"moov", &atom(b"udta", &atom(b"LEIC", &tiff("D-LUX (Typ 109)", focal, equivalent))));
        let mut parser = Panasonic::detect(&bytes, "sample.mp4", &InputOptions::default()).unwrap();
        let exif = parser.parse_mov(&mut Cursor::new(&bytes), bytes.len()).unwrap();
        assert_eq!(exif.model.as_deref(), Some("D-LUX (Typ 109)"));
        assert_eq!(exif.focal_length, Some(focal as f64 / 10.0));
        assert_eq!(exif.focal_length_35mm, Some(equivalent as u32));
        assert!(exif.lens_model.is_none());
    }
}

#[test]
fn leic_detection_does_not_claim_other_leica_or_unboxed_strings() {
    let other = atom(b"LEIC", &tiff("LEICA SL2", 500, 50));
    assert!(Panasonic::detect(&other, "sample.mp4", &InputOptions::default()).is_none());
    assert!(Panasonic::detect(b"LEIC D-LUX (Typ 109)", "sample.mp4", &InputOptions::default()).is_none());
    let truncated = atom(b"LEIC", &tiff("D-LUX (Typ 109)", 340, 81));
    assert!(Panasonic::detect(&truncated[..truncated.len() - 1], "sample.mp4", &InputOptions::default()).is_none());
    let pana = atom(b"moov", &atom(b"udta", &atom(b"PANA", &tiff("DMC-LX100", 340, 81))));
    let mut parser = Panasonic::detect(&pana, "sample.mp4", &InputOptions::default()).unwrap();
    assert_eq!(parser.parse_mov(&mut Cursor::new(&pana), pana.len()).unwrap().focal_length, Some(34.0));
}

#[test]
fn leica_makernotes_read_the_recorded_ois_state_only() {
    for header in [b"LEICA\0\0\0".as_slice(), b"Panasonic\0\0\0".as_slice()] {
        for state in [0u16, 2, 3, 4, 5] {
            let notes_length = header.len() + 30;
            let mut data = vec![0u8; 44 + notes_length];
            data[..8].copy_from_slice(b"II\x2a\0\x08\0\0\0");
            data[8..10].copy_from_slice(&1u16.to_le_bytes());
            entry(&mut data, 10, 0x8769, 4, 1, 26);
            data[26..28].copy_from_slice(&1u16.to_le_bytes());
            entry(&mut data, 28, 0x927c, 7, notes_length as u32, 44);
            data[44..44 + header.len()].copy_from_slice(header);
            let ifd = 44 + header.len();
            data[ifd..ifd + 2].copy_from_slice(&2u16.to_le_bytes());
            entry(&mut data, ifd + 2, 0x001a, 3, 1, state as u32);
            entry(&mut data, ifd + 14, 0x0027, 3, 1, 0);
            let exif = parse_tiff_ifd(&data).unwrap();
            assert_eq!(exif.image_stabilization, Some(state));
            assert!(exif.lens_model.is_none());
            if header.starts_with(b"LEICA") {
                assert!(exif.record_frame_rate.is_none());
                data.truncate(ifd + 4);
                assert!(parse_tiff_ifd(&data).unwrap().image_stabilization.is_none());
            }
        }
    }
}

#[test]
fn dlux_focal_projection_and_readout_match_lx100() {
    let mut json: serde_json::Value = serde_json::from_str(include_str!("../../camera_db/lumix.json")).unwrap();
    json["models"]["D-LUX (Typ 109)"] = json["models"]["LX100"].clone();
    json["readout"]["data"]["D-LUX (Typ 109)"] = json["readout"]["data"]["LX100"].clone();
    let dir = std::env::temp_dir().join(format!("telemetry-dlux-lumix-{}", std::process::id()));
    std::fs::create_dir(&dir).unwrap();
    struct Cleanup(std::path::PathBuf);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(self.0.join("lumix.json"));
            let _ = std::fs::remove_dir(&self.0);
        }
    }
    let _cleanup = Cleanup(dir.clone());
    std::fs::write(dir.join("lumix.json"), serde_json::to_vec(&json).unwrap()).unwrap();
    let options = InputOptions { camera_db_path: Some(dir.to_string_lossy().into_owned()), ..Default::default() };
    let video = VideoMetadata { width: 3840, height: 2160, fps: 25.0, duration_s: 12.0, rotation: 0 };
    for (focal, equivalent, pixel_focal) in [(125, 30, 3200.0f32), (340, 81, 8640.0f32)] {
        let mut outputs = Vec::new();
        for model in ["D-LUX (Typ 109)", "LX100"] {
            let mut exif = parse_tiff_ifd(&tiff(model, focal, equivalent)).unwrap();
            let mut parser = Panasonic::default();
            let mut map = GroupedTagMap::new();
            parser.process_map(&mut map, &options, Some(&video), &exif);
            assert_eq!(parser.model.as_deref(), Some(model));
            assert!(parser.lens.is_none());
            let lens = &map[&GroupId::Lens];
            assert!(!lens.contains_key(&TagId::DisplayName));
            assert_eq!(GetWithType::<f32>::get_t(lens, TagId::FocalLength), Some(&(focal as f32 / 10.0)));
            assert_eq!(GetWithType::<f32>::get_t(lens, TagId::PixelFocalLength), Some(&pixel_focal));
            let scale = *GetWithType::<f64>::get_t(&map[&GroupId::Default], TagId::Custom("scale_35mm".into())).unwrap();
            assert!((scale * focal as f64 / 10.0 - equivalent as f64).abs() < 1e-9);
            outputs.push((parser.frame_readout_time(), scale));
            for (state, expected) in [(2, true), (3, false)] {
                exif.image_stabilization = Some(state);
                let mut state_map = GroupedTagMap::new();
                parser.process_map(&mut state_map, &options, Some(&video), &exif);
                assert_eq!(GetWithType::<bool>::get_t(&state_map[&GroupId::Default], TagId::ImageStabilizer), Some(&expected));
            }
        }
        assert_eq!(outputs[0], outputs[1]);
    }
}

#[test]
fn file_metadata_survives_missing_or_unmatched_camera_database() {
    let dir = std::env::temp_dir().join(format!("telemetry-panasonic-file-metadata-{}", std::process::id()));
    std::fs::create_dir(&dir).unwrap();
    struct Cleanup(std::path::PathBuf);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(self.0.join("lumix.json"));
            let _ = std::fs::remove_dir(&self.0);
        }
    }
    let _cleanup = Cleanup(dir.clone());
    std::fs::write(dir.join("lumix.json"), r#"{"models":{"LX100":{"sw":17.3}}}"#).unwrap();
    let video = VideoMetadata { width: 3840, height: 2160, fps: 25.0, duration_s: 13.92, rotation: 0 };
    for database in [None, Some(dir.to_string_lossy().into_owned()), Some(dir.join("missing").to_string_lossy().into_owned())] {
        let options = InputOptions { camera_db_path: database, ..Default::default() };
        for (focal, equivalent, state) in [(109, 26, 3), (125, 30, 2), (340, 81, 2)] {
            let mut exif = parse_tiff_ifd(&tiff("D-LUX (Typ 109)", focal, equivalent)).unwrap();
            exif.image_stabilization = Some(state);
            let mut parser = Panasonic::default();
            let mut map = GroupedTagMap::new();
            parser.process_map(&mut map, &options, Some(&video), &exif);
            assert_eq!(parser.model.as_deref(), Some("D-LUX (Typ 109)"));
            let default = &map[&GroupId::Default];
            assert_eq!(GetWithType::<String>::get_t(default, TagId::Name).map(String::as_str), Some("D-LUX (Typ 109)"));
            assert_eq!(GetWithType::<bool>::get_t(default, TagId::ImageStabilizer), Some(&(state == 2)));
            assert_eq!(GetWithType::<f64>::get_t(default, TagId::FrameRate), Some(&25.0));
            assert_eq!(GetWithType::<f64>::get_t(default, TagId::RecordFrameRate), Some(&25.0));
            assert!(!default.contains_key(&TagId::Custom("SensorWidth".into())));
            let lens = &map[&GroupId::Lens];
            assert_eq!(GetWithType::<f32>::get_t(lens, TagId::FocalLength), Some(&(focal as f32 / 10.0)));
            assert!(!lens.contains_key(&TagId::DisplayName));
            assert!(!lens.contains_key(&TagId::PixelFocalLength));
            assert!(!lens.contains_key(&TagId::Custom("unit_pixel_focal_length".into())));
            assert!(parser.frame_readout_time().is_none());
        }
    }

    let mut parser = Panasonic::default();
    let mut map = GroupedTagMap::new();
    parser.process_map(&mut map, &InputOptions::default(), Some(&video), &PanasonicExifData::default());
    assert!(parser.model.is_none());
    assert!(!map[&GroupId::Default].contains_key(&TagId::Name));
    assert!(!map[&GroupId::Default].contains_key(&TagId::ImageStabilizer));
}

#[test]
fn detection_ignores_panasonic_strings_in_media_payload() {
    let nested_ftyp = atom(b"ftyp", b"pana\0\0\0\0pana");
    for payload in [b"Panasonic".as_slice(), b"PANA", b"pana", nested_ftyp.as_slice()] {
        let mut bytes = atom(b"ftyp", b"qt  \0\0\0\0qt  ");
        bytes.extend(atom(b"mdat", payload));
        assert!(Panasonic::detect(&bytes, "video.mov", &InputOptions::default()).is_none());
    }
}

#[test]
fn detection_requires_a_panasonic_ftyp_brand_field() {
    for payload in [b"pana\0\0\0\0qt  ".as_slice(), b"qt  \0\0\0\0pana"] {
        let bytes = atom(b"ftyp", payload);
        assert!(Panasonic::detect(&bytes, "video.mov", &InputOptions::default()).is_some());
    }
    for payload in [b"qt  panaqt  ".as_slice(), b"qt  \0\0\0\0xpana___", b"pana", b"pana\0\0\0\0x"] {
        let bytes = atom(b"ftyp", payload);
        assert!(Panasonic::detect(&bytes, "video.mov", &InputOptions::default()).is_none());
    }
    let bytes = atom(b"ftyp", b"qt  \0\0\0\0pana");
    for end in 0..bytes.len() {
        assert!(Panasonic::detect(&bytes[..end], "video.mov", &InputOptions::default()).is_none());
    }
}

#[test]
fn detection_accepts_extended_box_headers() {
    for (kind, payload) in [(b"ftyp", b"pana\0\0\0\0".to_vec()),
        (b"PANA", tiff("DC-S5", 350, 35))] {
        let mut bytes = 1u32.to_be_bytes().to_vec();
        bytes.extend(kind);
        bytes.extend(((payload.len() + 16) as u64).to_be_bytes());
        bytes.extend(payload);
        assert!(Panasonic::detect(&bytes, "video.mov", &InputOptions::default()).is_some());
    }
}

#[test]
fn detection_requires_a_complete_pana_exif_box() {
    let bytes = atom(b"PANA", &tiff("DC-S5", 350, 35));
    assert!(Panasonic::detect(&bytes, "video.mov", &InputOptions::default()).is_some());
    assert!(Panasonic::detect(&bytes[..bytes.len() - 1], "video.mov", &InputOptions::default()).is_none());
    assert!(Panasonic::detect(&atom(b"PANA", b"invalid EXIF"), "video.mov", &InputOptions::default()).is_none());
}

#[test]
fn parse_rejects_missing_or_invalid_panasonic_exif() {
    for metadata in [Vec::new(), atom(b"PANA", b"invalid EXIF")] {
        let mut bytes = atom(b"ftyp", b"pana\0\0\0\0pana");
        bytes.extend(atom(b"moov", &atom(b"udta", &metadata)));
        let mut parser = Panasonic::detect(&bytes, "video.mov", &InputOptions::default()).unwrap();
        assert!(parser.parse(&mut Cursor::new(&bytes), bytes.len(), |_| (),
            Arc::new(AtomicBool::new(false)), InputOptions::default()).is_err());
    }
}

#[test]
fn full_dispatch_preserves_panasonic_and_dlux_file_metadata() {
    for (kind, model) in [(b"PANA", "DC-S5"), (b"LEIC", "D-LUX (Typ 109)")] {
        let mut bytes = atom(b"ftyp", b"qt  \0\0\0\0qt  ");
        bytes.extend(atom(b"moov", &atom(b"udta", &atom(kind, &tiff(model, 350, 35)))));
        let input = Input::from_stream_with_options(&mut Cursor::new(&bytes), bytes.len(),
            "video.mov", |_| (), Arc::new(AtomicBool::new(false)), InputOptions {
                dont_look_for_sidecar_files: true, ..Default::default()
            }).unwrap();
        assert_eq!(input.camera_type(), "Panasonic");
        assert_eq!(input.camera_model().map(String::as_str), Some(model));
        let map = input.samples.as_ref().unwrap()[0].tag_map.as_ref().unwrap();
        let lens = &map[&GroupId::Lens];
        assert_eq!(GetWithType::<f32>::get_t(lens, TagId::FocalLength), Some(&35.0));
        assert_eq!(GetWithType::<f64>::get_t(&map[&GroupId::Default],
            TagId::Custom("scale_35mm".into())), Some(&1.0));
    }
}

#[test]
#[ignore = "Set PANASONIC_SAMPLE_DIR to the Panasonic sample directory"]
fn real_panasonic_samples_preserve_metadata() {
    let root = std::path::PathBuf::from(std::env::var("PANASONIC_SAMPLE_DIR").unwrap());
    for name in ["G100.MP4", "GH5M2.MOV", "GX85.mp4", "LX10.MP4", "S5.MOV", "S52X.MOV",
        "crop/APSC.MOV", "crop/FULL.MOV", "crop/PIXEL.MOV", "gh5s.MOV"] {
        let path = root.join(name);
        let mut stream = std::fs::File::open(&path).unwrap();
        let size = stream.metadata().unwrap().len() as usize;
        let input = Input::from_stream_with_options(&mut stream, size, &path, |_| (),
            Arc::new(AtomicBool::new(false)), InputOptions {
                dont_look_for_sidecar_files: true, ..Default::default()
            }).unwrap();
        assert_eq!(input.camera_type(), "Panasonic", "{name}");
        assert!(input.camera_model().is_some_and(|model| !model.is_empty()), "{name}");
        let samples = input.samples.as_ref().unwrap();
        assert_eq!(samples.len(), 1, "{name}");
        let tags = samples[0].tag_map.as_ref().unwrap().iter().map(|(group, tags)| {
            (group, tags.iter().map(|(id, tag)| (id, &tag.value))
                .collect::<std::collections::BTreeMap<_, _>>())
        }).collect::<std::collections::BTreeMap<_, _>>();
        println!("PANASONIC_SAMPLE {name} model={:?} tags={}", input.camera_model(),
            serde_json::to_string(&tags).unwrap());
    }
}
