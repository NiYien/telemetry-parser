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
