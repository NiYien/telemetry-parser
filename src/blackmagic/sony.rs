// SPDX-License-Identifier: MIT OR Apache-2.0

use crate::{tags_impl::*, InputOptions};

pub(super) struct Metadata {
    shutter_is_angle: bool,
    pub geometry: Option<Geometry>,
}

#[derive(Clone, Copy)]
pub(super) struct Geometry {
    pub pixels_per_mm: f64,
    pub scale_35mm: f64,
}

impl Metadata {
    pub fn new(raw: &serde_json::Value) -> Option<Self> {
        if !raw.get("manufacturer")?.as_str()?.trim().eq_ignore_ascii_case("Sony") {
            return None;
        }
        Some(Self {
            shutter_is_angle: raw.get("shutter_type").and_then(|v| v.as_str()) == Some("Angle"),
            geometry: Geometry::from_metadata(raw),
        })
    }

    pub fn emit_global(&self, raw: &serde_json::Value, map: &mut GroupedTagMap, options: &InputOptions) {
        for (key, id, label) in [
            ("viewing_gamma", TagId::CaptureGammaEquation, "Gamma equation"),
            ("viewing_gamut", TagId::ColorPrimaries, "Color primaries"),
        ] {
            if let Some(value) = raw.get(key).and_then(|v| v.as_str()).filter(|v| !v.trim().is_empty()) {
                crate::util::insert_tag(map, crate::tag!(parsed GroupId::Colors, id, label, String, |v| v.clone(), value.to_owned(), vec![]), options);
            }
        }
        if let Some(value) = raw.get("lens_type").and_then(|v| v.as_str()).filter(|v| !v.trim().is_empty()) {
            crate::util::insert_tag(map, crate::tag!(parsed GroupId::Lens, TagId::DisplayName, "Lens model", String, |v| v.clone(), value.to_owned(), vec![]), options);
        }
        if let Some(geometry) = self.geometry {
            let sensor_width = (36.0 / geometry.scale_35mm) as f32;
            crate::util::insert_tag(map, crate::tag!(parsed GroupId::Default, TagId::Custom("SensorWidth".into()), "Sensor width (mm)", f32, |v| format!("{v:.1} mm"), sensor_width, vec![]), options);
            crate::util::insert_tag(map, crate::tag!(parsed GroupId::Lens, TagId::Custom("unit_pixel_focal_length".into()), "Pixel focal length per mm", f64, |v| format!("{v:.4}"), geometry.pixels_per_mm, vec![]), options);
            crate::util::insert_tag(map, crate::tag!(parsed GroupId::Default, TagId::Custom("scale_35mm".into()), "35mm equivalent scale", f64, |v| format!("{v:.4}"), geometry.scale_35mm, vec![]), options);
            crate::util::insert_tag(map, crate::tag!(parsed GroupId::Lens, TagId::Custom("focal_length_35mm_estimated".into()), "35mm equivalent focal length estimated", bool, |v| v.to_string(), true, vec![]), options);
        }
    }

    pub fn emit_frame(&self, raw: &serde_json::Value, map: &mut GroupedTagMap, options: &InputOptions) {
        if let Some(focal) = raw.get("focal_length").and_then(|v| v.as_str()).and_then(|v| positive(v.trim().strip_suffix("mm")?)) {
            crate::util::insert_tag(map, crate::tag!(parsed GroupId::Lens, TagId::FocalLength, "Focal length", f32, |v| format!("{v:.2} mm"), focal, vec![]), options);
            if let Some(geometry) = self.geometry {
                let pixels = (focal as f64 * geometry.pixels_per_mm) as f32;
                let equivalent = (focal as f64 * geometry.scale_35mm) as f32;
                if pixels.is_finite() && equivalent.is_finite() {
                    crate::util::insert_tag(map, crate::tag!(parsed GroupId::Lens, TagId::PixelFocalLength, "Pixel focal length", f32, |v| format!("{v:.2}"), pixels, vec![]), options);
                    crate::util::insert_tag(map, crate::tag!(parsed GroupId::Lens, TagId::LensZoom35mm, "Focal length (35mm equivalent)", f32, |v| format!("≈{v:.2} mm"), equivalent, vec![]), options);
                }
            }
        }
        if let Some(aperture) = raw.get("aperture").and_then(|v| v.as_str()) {
            let aperture = aperture.trim();
            if let Some(value) = aperture.strip_prefix('F').or_else(|| aperture.strip_prefix('f')).and_then(|v| positive(v.trim_start_matches('/'))) {
                crate::util::insert_tag(map, crate::tag!(parsed GroupId::Lens, TagId::IrisFStop, "Iris", f32, |v| format!("f/{v:.1}"), value, vec![]), options);
            } else if let Some(value) = aperture.strip_prefix('T').or_else(|| aperture.strip_prefix('t')).and_then(|v| positive(v.trim_start_matches('/'))) {
                crate::util::insert_tag(map, crate::tag!(parsed GroupId::Lens, TagId::IrisTStop, "Iris", f32, |v| format!("T/{v:.1}"), value, vec![]), options);
            }
        }
        if let Some(distance) = raw.get("distance").and_then(|v| v.as_str()) {
            let distance = distance.trim();
            let meters = distance.strip_suffix("mm").and_then(positive).map(|v| v / 1000.0)
                .or_else(|| distance.strip_suffix('m').and_then(positive));
            if let Some(meters) = meters.filter(|v| v.is_finite() && *v > 0.0) {
                crate::util::insert_tag(map, crate::tag!(parsed GroupId::Lens, TagId::FocusDistance, "Focus distance", f32, |v| format!("{v:.3} m"), meters, vec![]), options);
            }
        }
        if let Some(iso) = positive_integer(raw, "iso") {
            crate::util::insert_tag(map, crate::tag!(parsed GroupId::Exposure, TagId::ISOValue, "ISO", u32, |v| v.to_string(), iso, vec![]), options);
        }
        if let Some(kelvin) = positive_integer(raw, "white_balance_kelvin") {
            crate::util::insert_tag(map, crate::tag!(parsed GroupId::Colors, TagId::WhiteBalance, "White balance", u32, |v| format!("{v}K"), kelvin, vec![]), options);
        }
        if let Some(shutter) = raw.get("shutter_value").and_then(|v| v.as_str()) {
            if self.shutter_is_angle {
                if let Some(angle) = positive(shutter.trim().trim_end_matches('°')).filter(|v| *v <= 360.0) {
                    crate::util::insert_tag(map, crate::tag!(parsed GroupId::Exposure, TagId::ShutterAngle, "Shutter angle", f32, |v| format!("{v:.2}°"), angle, vec![]), options);
                }
            } else {
                let shutter = shutter.trim().trim_end_matches('s').trim();
                let seconds = if let Some((num, den)) = shutter.split_once('/') {
                    positive(num).zip(positive(den)).map(|(num, den)| num / den)
                } else {
                    positive(shutter)
                };
                if let Some(seconds) = seconds.filter(|v| v.is_finite() && *v > 0.0) {
                    crate::util::insert_tag(map, crate::tag!(parsed GroupId::Exposure, TagId::ShutterSpeed, "Shutter speed", f32, |v| if *v < 1.0 { format!("1/{:.0} s", 1.0 / v) } else { format!("{v:.3} s") }, seconds, vec![]), options);
                }
            }
        }
    }
}

impl Geometry {
    fn from_metadata(raw: &serde_json::Value) -> Option<Self> {
        let model = raw.get("camera_type")?.as_str()?.trim().strip_prefix("Sony ").unwrap_or(raw.get("camera_type")?.as_str()?.trim());
        if !matches!(model, "FX3" | "ILME-FX3" | "ILCE-FX3") { return None; }
        let captured = pair(raw.get("sensor_area_captured")?)?;
        let crop = pair(raw.get("crop_size")?)?;
        let origin = pair(raw.get("crop_origin")?)?;
        // Only the verified full-width FX3 RAW mode is covered. The padded coded
        // width must not be mistaken for the active sensor width.
        if crop.0 != 4240.0 || crop.1 != 2392.0 || captured != (4288.0, 2408.0)
            || origin.0 < 0.0 || origin.1 < 0.0
            || origin.0 + crop.0 > captured.0 || origin.1 + crop.1 > captured.1 {
            return None;
        }
        // Nominal full-width estimate, not a calibrated lens model:
        // https://helpguide.sony.net/ilc/2035/v1/en/contents/TP1000281415.html
        // https://www.sony.com/electronics/support/camcorders-and-video-cameras-interchangeable-lens-camcorders/ilme-fx3/specifications
        Some(Self { pixels_per_mm: 4240.0 / 35.6, scale_35mm: 36.0 / 35.6 })
    }
}

pub(super) fn lookup_model(model: &str) -> &str {
    match model {
        "FX3" => "ILME-FX3",
        "FX30" => "ILME-FX30",
        _ => model,
    }
}

fn positive(value: &str) -> Option<f32> {
    value.trim().parse::<f32>().ok().filter(|v| v.is_finite() && *v > 0.0)
}

fn positive_integer(raw: &serde_json::Value, key: &str) -> Option<u32> {
    u32::try_from(raw.get(key)?.as_u64()?).ok().filter(|v| *v > 0)
}

fn pair(value: &serde_json::Value) -> Option<(f64, f64)> {
    let array = value.as_array()?;
    let pair = (array.get(0)?.as_f64()?, array.get(1)?.as_f64()?);
    (pair.0.is_finite() && pair.1.is_finite()).then_some(pair)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn clip_metadata() -> serde_json::Value {
        json!({"manufacturer":"Sony", "camera_type":"Sony FX3", "shutter_type":"Speed",
            "sensor_area_captured":[4288,2408], "crop_origin":[8,8], "crop_size":[4240,2392],
            "viewing_gamma":"S-Log3", "viewing_gamut":"S-Gamut3.Cine"})
    }

    fn frame_metadata() -> serde_json::Value {
        json!({"focal_length":"57mm", "aperture":"F4.0", "distance":"1255mm",
            "iso":800, "white_balance_kelvin":4200, "shutter_value":"1/645"})
    }

    #[test]
    fn sony_braw_decodes_lens_exposure_and_estimated_projection() {
        let raw = clip_metadata();
        let metadata = Metadata::new(&raw).unwrap();
        let mut map = GroupedTagMap::new();
        metadata.emit_global(&raw, &mut map, &InputOptions::default());
        metadata.emit_frame(&frame_metadata(), &mut map, &InputOptions::default());
        let lens = &map[&GroupId::Lens];
        assert_eq!(lens.get_t(TagId::FocalLength) as Option<&f32>, Some(&57.0));
        assert_eq!(lens.get_t(TagId::IrisFStop) as Option<&f32>, Some(&4.0));
        assert_eq!(lens.get_t(TagId::FocusDistance) as Option<&f32>, Some(&1.255));
        let pixels = *(lens.get_t(TagId::PixelFocalLength) as Option<&f32>).unwrap();
        assert!((pixels as f64 - 57.0 * 4240.0 / 35.6).abs() < 0.001);
        let equivalent = *(lens.get_t(TagId::LensZoom35mm) as Option<&f32>).unwrap();
        assert!((equivalent as f64 - 57.0 * 36.0 / 35.6).abs() < 0.00001);
        assert!(lens[&TagId::LensZoom35mm].value.to_string().starts_with('≈'));
        assert_eq!(map[&GroupId::Exposure].get_t(TagId::ISOValue) as Option<&u32>, Some(&800));
        assert!((*(map[&GroupId::Exposure].get_t(TagId::ShutterSpeed) as Option<&f32>).unwrap() - 1.0 / 645.0).abs() < 1e-8);
        assert_eq!(map[&GroupId::Colors].get_t(TagId::WhiteBalance) as Option<&u32>, Some(&4200));
        assert!(!lens.contains_key(&TagId::DisplayName));
    }

    #[test]
    fn sony_braw_unknown_geometry_keeps_recorded_values_without_equivalent_fallback() {
        for (key, value) in [
            ("camera_type", json!("Sony FX30")),
            ("camera_type", json!("Sony unknown")),
            ("crop_size", json!([2120,1196])),
            ("crop_size", serde_json::Value::Null),
            ("crop_origin", json!([-1,8])),
            ("crop_origin", json!([100,8])),
            ("sensor_area_captured", json!([4240,2392])),
        ] {
            let mut raw = clip_metadata();
            raw[key] = value;
            let metadata = Metadata::new(&raw).unwrap();
            assert!(metadata.geometry.is_none(), "{raw}");
            let mut map = GroupedTagMap::new();
            metadata.emit_global(&raw, &mut map, &InputOptions::default());
            metadata.emit_frame(&frame_metadata(), &mut map, &InputOptions::default());
            let lens = &map[&GroupId::Lens];
            assert_eq!(lens.get_t(TagId::FocalLength) as Option<&f32>, Some(&57.0));
            assert!(!lens.contains_key(&TagId::PixelFocalLength));
            assert!(!lens.contains_key(&TagId::LensZoom35mm));
            assert!(!lens.contains_key(&TagId::Custom("unit_pixel_focal_length".into())));
        }
        assert_eq!(lookup_model("FX3"), "ILME-FX3");
        assert_eq!(lookup_model("FX30"), "ILME-FX30");
        assert_eq!(lookup_model("ILME-FX3"), "ILME-FX3");
        let mut raw = clip_metadata();
        raw["manufacturer"] = json!("Panasonic");
        assert!(Metadata::new(&raw).is_none());
    }

    #[test]
    fn sony_braw_rejects_invalid_numbers_and_unknown_units() {
        let metadata = Metadata::new(&clip_metadata()).unwrap();
        for invalid in ["", "0", "-5", "NaN", "inf", "1e39"] {
            let frame = json!({"focal_length":format!("{invalid}mm"), "aperture":format!("F{invalid}"),
                "distance":format!("{invalid}mm"), "shutter_value":format!("1/{invalid}"),
                "iso":0, "white_balance_kelvin":0});
            let mut map = GroupedTagMap::new();
            metadata.emit_frame(&frame, &mut map, &InputOptions::default());
            assert!(map.is_empty(), "{frame}: {map:?}");
        }
        let mut map = GroupedTagMap::new();
        metadata.emit_frame(&json!({"focal_length":"57", "distance":"1255feet", "aperture":"unknown"}), &mut map, &InputOptions::default());
        assert!(map.is_empty());
    }

    #[test]
    fn sony_braw_preserves_shutter_angles_and_distance_units() {
        let mut raw = clip_metadata();
        raw["shutter_type"] = json!("Angle");
        let metadata = Metadata::new(&raw).unwrap();
        let mut map = GroupedTagMap::new();
        metadata.emit_frame(&json!({"shutter_value":"180°", "aperture":"T/2.1", "distance":"1.255m"}), &mut map, &InputOptions::default());
        assert_eq!(map[&GroupId::Exposure].get_t(TagId::ShutterAngle) as Option<&f32>, Some(&180.0));
        assert!(!map[&GroupId::Exposure].contains_key(&TagId::ShutterSpeed));
        assert_eq!(map[&GroupId::Lens].get_t(TagId::FocusDistance) as Option<&f32>, Some(&1.255));
        assert_eq!(map[&GroupId::Lens].get_t(TagId::IrisTStop) as Option<&f32>, Some(&2.1));
    }

    #[test]
    #[ignore = "requires SONY_BRAW_SAMPLE pointing to Video Assist_0210.braw"]
    fn sony_braw_real_clip_keeps_all_frames_and_clip_summary() {
        use std::{fs::File, io::BufReader, sync::{Arc, atomic::AtomicBool}};
        let path = std::env::var("SONY_BRAW_SAMPLE").expect("SONY_BRAW_SAMPLE");
        for db in [None, std::env::var("SONY_BRAW_CAMERA_DB").ok()] {
            let file = File::open(&path).unwrap();
            let size = file.metadata().unwrap().len() as usize;
            let mut stream = BufReader::new(file);
            let input = crate::Input::from_stream_with_options(&mut stream, size, &path, |_| {}, Arc::new(AtomicBool::new(false)),
                InputOptions { camera_db_path: db, ..Default::default() }).unwrap();
            assert_eq!(input.camera_type(), "Sony");
            assert_eq!(input.frame_readout_time(), None);
            let samples = input.samples.unwrap();
            assert_eq!(samples.len(), 496);
            let summary = samples[0].tag_map.as_ref().unwrap();
            assert_eq!(summary[&GroupId::Default].get_t(TagId::Custom("SensorWidth".into())) as Option<&f32>, Some(&35.6));
            assert_eq!(summary[&GroupId::Lens].get_t(TagId::FocalLength) as Option<&f32>, Some(&57.0));
            assert_eq!(summary[&GroupId::Exposure].get_t(TagId::ISOValue) as Option<&u32>, Some(&800));
            let mut previous = -1.0;
            let mut focals = std::collections::BTreeSet::new();
            for sample in &samples[1..] {
                assert!(sample.timestamp_ms > previous);
                previous = sample.timestamp_ms;
                let lens = &sample.tag_map.as_ref().unwrap()[&GroupId::Lens];
                let focal = *(lens.get_t(TagId::FocalLength) as Option<&f32>).unwrap();
                let pixels = *(lens.get_t(TagId::PixelFocalLength) as Option<&f32>).unwrap();
                assert!((pixels as f64 - focal as f64 * 4240.0 / 35.6).abs() < 0.001);
                assert!((54.0..=64.0).contains(&focal));
                focals.insert(focal as u32);
                assert!(lens.contains_key(&TagId::FocusDistance));
            }
            assert_eq!(focals.len(), 10);
            assert_eq!(previous, 9880.0);
        }
    }

    #[test]
    #[ignore = "requires SONY_CAMERA_DB, SONY_BRAW_SAMPLE and optionally SONY_BRAW_75MM_SAMPLE"]
    fn sony_braw_real_clips_use_explicit_estimates_and_preserve_recorded_timing() {
        use std::{fs::File, io::{BufReader, Read, Seek, SeekFrom}, sync::{Arc, atomic::AtomicBool}};
        let first = std::env::var("SONY_BRAW_SAMPLE").expect("SONY_BRAW_SAMPLE");
        let database = std::env::var("SONY_CAMERA_DB").expect("SONY_CAMERA_DB");
        let options = InputOptions { camera_db_path: Some(database), ..Default::default() };
        let mut paths = vec![(first.clone(),496,57.0)];
        if let Ok(path) = std::env::var("SONY_BRAW_75MM_SAMPLE") { paths.push((path,237,75.0)); }
        for (path,count,focal) in paths {
            let file = File::open(&path).unwrap();
            let size = file.metadata().unwrap().len() as usize;
            let input = crate::Input::from_stream_with_options(&mut BufReader::new(file), size, &path, |_| {},
                Arc::new(AtomicBool::new(false)), options.clone()).unwrap();
            assert_eq!(input.frame_readout_time(),Some(9.2));
            let samples = input.samples.unwrap();
            assert_eq!(samples.len(),count);
            let map = samples[0].tag_map.as_ref().unwrap();
            assert_eq!(map[&GroupId::Lens].get_t(TagId::FocalLength) as Option<&f32>,Some(&focal));
            assert_eq!(map[&GroupId::Imager].get_t(TagId::Custom("readout_estimated".into())) as Option<&bool>,Some(&true));
            let raw = (map[&GroupId::Default].get_t(TagId::Metadata) as Option<&serde_json::Value>).unwrap();
            assert_eq!(raw["sensor_line_time"],0.0);
        }

        // Change only the known fixture's four-byte line time in the read stream.
        // The source file remains read-only; a valid file value must beat the table.
        struct PatchedFile(File);
        impl Read for PatchedFile {
            fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize> {
                let position = self.0.stream_position()?;
                let length = self.0.read(output)?;
                for (index, value) in 4.0_f32.to_be_bytes().iter().enumerate() {
                    let offset = 955283963_u64 + index as u64;
                    if offset >= position && offset < position + length as u64 {
                        output[(offset-position) as usize] = *value;
                    }
                }
                Ok(length)
            }
        }
        impl Seek for PatchedFile {
            fn seek(&mut self, position: SeekFrom) -> std::io::Result<u64> { self.0.seek(position) }
        }
        let file = File::open(&first).unwrap();
        let size = file.metadata().unwrap().len() as usize;
        assert_eq!(size,955284252);
        let input = crate::Input::from_stream_with_options(&mut PatchedFile(file),size,&first,|_|{},
            Arc::new(AtomicBool::new(false)),options).unwrap();
        assert!((input.frame_readout_time().unwrap() - 9.632).abs() < 1e-9);
        let map = input.samples.as_ref().unwrap()[0].tag_map.as_ref().unwrap();
        assert!(!map.get(&GroupId::Imager).unwrap().contains_key(&TagId::Custom("readout_estimated".into())));
    }
}
