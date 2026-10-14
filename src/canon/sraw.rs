// SPDX-License-Identifier: MIT OR Apache-2.0

use crate::{camera_db::CameraDatabase, tags_impl::*, InputOptions};

pub(super) fn process_readout(db: &CameraDatabase, model: &str, fps: f64, map: &mut GroupedTagMap, options: &InputOptions) -> Option<f64> {
    // Only known recording rates may select a SRAW preset. Never borrow an ordinary 4K value.
    let rate = [24, 25, 30, 50, 60].into_iter().find(|rate| {
        let rate = *rate as f64;
        fps.is_finite() && ((fps - rate).abs() < 0.01
            || (matches!(rate as u32, 24 | 30 | 60) && (fps - rate * 1000.0 / 1001.0).abs() < 0.01))
    })?;
    let prefix = format!("SRAW{rate}:");
    let values = db.get_brand("CANON")?.readout.additional.get(model)?;
    let value = values.iter().find_map(|value| value.strip_prefix(&prefix))?.parse::<f64>().ok()?;
    let readout = value.abs();
    if !readout.is_finite() || readout <= 0.0 || readout >= 1000.0 / fps {
        return None;
    }
    crate::util::insert_tag(map, crate::tag!(parsed GroupId::Imager, TagId::FrameReadoutTime, "Frame readout time", f64, |v| format!("{v:.4} ms"), readout, Vec::new()), options);
    if value < 0.0 {
        crate::util::insert_tag(map, crate::tag!(parsed GroupId::Imager, TagId::Custom("readout_estimated".into()), "Readout time estimated", bool, |v| v.to_string(), true, Vec::new()), options);
    }
    Some(readout)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{canon::{Canon, exif::CanonExifData}, util::VideoMetadata, SampleInfo};
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Database(std::path::PathBuf);
    impl Database {
        fn new(additional: serde_json::Value) -> Self {
            static NEXT: AtomicUsize = AtomicUsize::new(0);
            let path = std::env::temp_dir().join(format!("canon-sraw-{}-{}", std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed)));
            std::fs::create_dir(&path).unwrap();
            let data = serde_json::json!({
                "models": {"R5 Mark II": {"sw": 35.9}, "R5": {"sw": 35.9}},
                "readout": {
                    "columns": ["8K60", "8K30", "8K24", "4K60", "4K30", "4K24", "1K30"],
                    "data": {"R5 Mark II": [17.3,17.3,15.5,9.7,9.7,9.7,-9.7], "R5": [15.5,15.5,15.5,9.7,15.5,15.5,-9.7]},
                    "additional": {"R5 Mark II": additional}
                }
            });
            std::fs::write(path.join("canon.json"), serde_json::to_vec(&data).unwrap()).unwrap();
            Self(path)
        }
        fn options(&self) -> InputOptions {
            InputOptions { camera_db_path: Some(self.0.to_string_lossy().into()), ..Default::default() }
        }
    }
    impl Drop for Database {
        fn drop(&mut self) { let _ = std::fs::remove_dir_all(&self.0); }
    }
    fn presets() -> serde_json::Value {
        serde_json::json!(["SRAW24:-17.3", "SRAW25:-17.3", "SRAW30:-17.3", "SRAW50:-12.8", "SRAW60:-12.8"])
    }
    fn parse(model: &str, is_crm: bool, from_cmt: bool, sraw: bool, width: usize, height: usize, fps: f64, options: &InputOptions, native: Option<f64>) -> (Canon, Vec<SampleInfo>) {
        let mut canon = Canon { model: Some(model.into()), is_crm, ..Default::default() };
        let mut map = GroupedTagMap::new();
        if let Some(value) = native {
            crate::util::insert_tag(&mut map, crate::tag!(parsed GroupId::Imager, TagId::FrameReadoutTime, "Native readout", f64, |v| v.to_string(), value, Vec::new()), options);
        }
        let mut samples = vec![SampleInfo { tag_map: Some(map), ..Default::default() }];
        let exif = CanonExifData { from_cmt, canon_sraw: sraw, ..Default::default() };
        let video = VideoMetadata { width, height, fps, ..Default::default() };
        canon.process_map(&mut samples, options, Some(exif), Some(&video), None, None, None);
        (canon, samples)
    }
    #[test]
    fn crm_sraw_reads_each_supported_rate_from_its_own_preset() {
        let db = Database::new(presets());
        for (fps, expected) in [(24000.0/1001.0,17.3), (23.98,17.3), (24.0,17.3), (25.0,17.3), (30000.0/1001.0,17.3), (29.97,17.3), (30.0,17.3), (50.0,12.8), (60000.0/1001.0,12.8), (59.94,12.8), (60.0,12.8)] {
            let (canon, samples) = parse("R5 Mark II", true, true, true, 4096, 2160, fps, &db.options(), None);
            assert_eq!(canon.frame_readout_time(), Some(expected), "fps={fps}");
            let tags = samples[0].tag_map.as_ref().unwrap();
            assert_eq!(tags[&GroupId::Imager].get_t(TagId::Custom("readout_estimated".into())) as Option<&bool>, Some(&true));
            assert_eq!(tags[&GroupId::Default].get_t(TagId::Custom("canon_sraw".into())) as Option<&bool>, Some(&true));
        }
    }
    #[test]
    fn crm_sraw_does_not_reclassify_mp4_8k_or_other_cameras() {
        let db = Database::new(presets());
        for (model, crm, cmt, sraw, width, height, expected) in [
            ("R5 Mark II",false,false,false,2048,1080,9.7),
            ("R5 Mark II",false,true,true,4096,2160,9.7),
            ("R5 Mark II",true,false,true,4096,2160,9.7),
            ("R5 Mark II",true,true,false,4096,2160,9.7),
            ("R5 Mark II",true,true,true,8192,4320,17.3),
            ("R5 Mark II",true,true,true,4096,2304,9.7),
            ("R5",true,true,true,4096,2160,15.5),
        ] {
            let (canon, _) = parse(model, crm, cmt, sraw, width, height, 29.97, &db.options(), None);
            assert_eq!(canon.frame_readout_time(), Some(expected), "{model} {crm} {cmt} {sraw} {width}x{height}");
        }
    }
    #[test]
    fn crm_sraw_keeps_native_readout_and_does_not_guess_with_old_or_invalid_data() {
        let db = Database::new(presets());
        let (canon, samples) = parse("R5 Mark II",true,true,true,4096,2160,29.97,&db.options(),Some(13.25));
        assert_eq!(canon.frame_readout_time(), Some(13.25));
        assert!(!samples[0].tag_map.as_ref().unwrap()[&GroupId::Imager].contains_key(&TagId::Custom("readout_estimated".into())));
        for fps in [0.0, -1.0, f64::NAN, f64::INFINITY, 24.975, 48.0, 49.95, 100.0, 120.0] {
            let (canon, _) = parse("R5 Mark II",true,true,true,4096,2160,fps,&db.options(),None);
            assert_eq!(canon.frame_readout_time(), None, "fps={fps}");
        }
        for values in [serde_json::json!([]), serde_json::json!(["Fine:17.3"]), serde_json::json!(["SRAW30:NaN"]), serde_json::json!(["SRAW30:0"]), serde_json::json!(["SRAW30:40"])] {
            let db = Database::new(values);
            let (canon, _) = parse("R5 Mark II",true,true,true,4096,2160,29.97,&db.options(),None);
            assert_eq!(canon.frame_readout_time(), None);
        }
        let (canon, samples) = parse("R5 Mark II",true,true,true,4096,2160,29.97,&InputOptions::default(),None);
        assert_eq!(canon.frame_readout_time(), None);
        assert_eq!(samples[0].tag_map.as_ref().unwrap()[&GroupId::Default].get_t(TagId::Custom("canon_sraw".into())) as Option<&bool>, Some(&true));
    }
    #[test]
    fn crm_sraw_uses_data_values_instead_of_hardcoded_timings() {
        let db = Database::new(serde_json::json!(["SRAW30:16.25"]));
        let (canon, samples) = parse("R5 Mark II",true,true,true,4096,2160,29.97,&db.options(),None);
        assert_eq!(canon.frame_readout_time(), Some(16.25));
        assert!(!samples[0].tag_map.as_ref().unwrap()[&GroupId::Imager].contains_key(&TagId::Custom("readout_estimated".into())));
    }
    #[test]
    #[ignore = "Requires CANON_R52_SAMPLE_DIR and CANON_CAMERA_DB"]
    fn crm_sraw_original_recordings_and_proxies() {
        use std::{fs::File, io::BufReader, sync::{Arc, atomic::AtomicBool}};
        let root = std::path::PathBuf::from(std::env::var("CANON_R52_SAMPLE_DIR").unwrap());
        let database = std::env::var("CANON_CAMERA_DB").unwrap();
        for (folder, stem, master_readout, master_samples, proxy_samples) in [
            ("RAW", "A_0045C781X260508_000220EJ_R5MK2",17.3,71,60),
            ("SRAW", "A_0045C784X260508_001027EJ_R5MK2",17.3,73,73),
            ("轻RAW", "A_0045C783X260508_000310E8_R5MK2",17.3,70,70),
        ] {
            for (suffix, readout, count) in [(".CRM",master_readout,master_samples),("_Proxy.MP4",9.7,proxy_samples)] {
                for camera_db_path in [None,Some(database.clone())] {
                    let path = root.join(folder).join(format!("{stem}{suffix}"));
                    let file = File::open(&path).unwrap();
                    let size = file.metadata().unwrap().len() as usize;
                    let with_database = camera_db_path.is_some();
                    let input = crate::Input::from_stream_with_options(&mut BufReader::new(file),size,&path, |_| {},Arc::new(AtomicBool::new(false)), InputOptions {
                        camera_db_path, metadata_sample_stride: Some(1), ..Default::default()
                    }).unwrap();
                    assert_eq!(input.frame_readout_time(), with_database.then_some(readout), "{} db={with_database}",path.display());
                    let samples = input.samples.as_ref().unwrap();
                    assert_eq!(samples.len(),count);
                    let marked = samples[0].tag_map.as_ref().and_then(|m| m.get(&GroupId::Default)).and_then(|m| m.get_t(TagId::Custom("canon_sraw".into())) as Option<&bool>).copied().unwrap_or(false);
                    assert_eq!(marked,folder == "SRAW" && suffix == ".CRM");
                    assert!(!crate::util::normalized_imu_interpolated(&input, None).unwrap().is_empty());
                }
            }
        }
    }
}
