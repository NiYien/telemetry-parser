// SPDX-License-Identifier: MIT OR Apache-2.0

use super::*;

impl Sony {
    pub(super) fn process_fx3_readout_fallback(&mut self, samples: &mut Vec<SampleInfo>, options: &InputOptions, video_md: Option<&VideoMetadata>) {
        if self.frame_readout_time.is_some_and(|v| v.is_finite() && v != 0.0) { return; }
        let raw_name = self.model.as_deref().unwrap_or("").trim_start_matches("Sony ");
        if !matches!(raw_name, "FX3" | "ILME-FX3" | "ILCE-FX3") { return; }
        let Some(video) = video_md else { return; };
        let capture_fps = samples.first().and_then(|s| s.tag_map.as_ref())
            .and_then(|map| map.get(&GroupId::Default))
            .and_then(|map| map.get_t(TagId::FrameRate) as Option<&f64>).copied()
            .filter(|fps| fps.is_finite() && *fps > 0.0).unwrap_or(video.fps);
        if !capture_fps.is_finite() || capture_fps <= 0.0 { return; }
        let max_fps = match (video.width, video.height) {
            (1920, 1080) => 240.0,
            (3840, 2160) => 120.0,
            (1280, 720) | (4096, 2160) | (4240, 2392) | (4264, 2408) | (4288, 2408) => 60.0,
            _ => return,
        };
        if capture_fps > max_fps + 0.1 { return; }
        let Some(path) = &options.camera_db_path else { return; };
        let Ok(db) = crate::camera_db::CameraDatabase::load(path) else { return; };
        let Some((name, model)) = db.find_model("SONY", "ILME-FX3") else { return; };
        let mut tags = std::collections::HashMap::new();
        if capture_fps < 23.9 {
            tags.insert("slow_readout_reference".to_owned(), true.into());
        }
        if samples.is_empty() { samples.push(SampleInfo::default()); }
        let map = samples[0].tag_map.get_or_insert_with(GroupedTagMap::new);
        if let Some(captured_readout) = db.process_readout("SONY", name, video.width as u32, video.height as u32,
            capture_fps, 0.0, model.sw, &tags, map, options) {
            // The table describes the captured rows. Sony RTMD consumers expect
            // full-sensor timing and apply the recorded capture-height ratio later.
            let sensor_scale = map.get(&GroupId::Imager).and_then(|imager| {
                let sensor = imager.get_t(TagId::SensorSizePixels) as Option<&(u32,u32)>;
                let captured = imager.get_t(TagId::CaptureAreaSize) as Option<&(f32,f32)>;
                let (sensor,captured) = (sensor?,captured?);
                (sensor.1 > 0 && captured.1.is_finite() && captured.1 > 0.0)
                    .then_some(sensor.1 as f64 / captured.1 as f64)
            }).unwrap_or(1.0);
            let readout = captured_readout * sensor_scale;
            self.frame_readout_time = Some(readout);
            util::insert_tag(map, tag!(parsed GroupId::Imager, TagId::FrameReadoutTime, "Frame readout time", f64, |v| format!("{v:.4} ms"), readout, vec![]), options);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    struct Database(std::path::PathBuf);
    impl Database {
        fn new() -> Self {
            let unique = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
            let path = std::env::temp_dir().join(format!("sony-readout-{}-{unique}", std::process::id()));
            std::fs::create_dir(&path).unwrap();
            std::fs::write(path.join("sony.json"), json!({
                "aliases":{"ILME-":"ILCE-"}, "models":{"ILCE-FX3":{"sw":35.9}},
                "readout":{"columns":["4K120","4K60","4K30","4K24","1K240","1K120","1K60","1K30","1K24"],
                    "data":{"ILCE-FX3":[8.3,9.2,9.2,9.2,-4.15,8.3,9.2,9.2,9.2]},
                    "additional":{"ILCE-FX3":["RES:1280:720:-9.2","RES:4096:2160:-9.2","RES:4240:2392:-9.2","RES:4264:2408:-9.2","RES:4288:2408:-9.2","SLOW:-9.2"]}},
                "readout_adjust":[{"type":"override_res","pattern": "RES:"},{"type":"override","pattern": "SLOW:","when_tag":"slow_readout_reference"}]
            }).to_string()).unwrap();
            Self(path)
        }
        fn options(&self) -> InputOptions {
            InputOptions { camera_db_path: Some(self.0.to_string_lossy().into_owned()), ..Default::default() }
        }
    }
    impl Drop for Database {
        fn drop(&mut self) { let _ = std::fs::remove_dir_all(&self.0); }
    }

    fn sample(focal: Option<f32>, capture_fps: f64, recorded_readout: Option<f64>) -> Vec<SampleInfo> {
        let mut map = GroupedTagMap::new();
        let options = InputOptions::default();
        for (id, value) in [(TagId::SensorWidth,35.6_f32), (TagId::SensorHeight,20.025)] {
            util::insert_tag(&mut map, tag!(parsed GroupId::Default, id, "Sensor size", f32, |v| v.to_string(), value, vec![]), &options);
        }
        util::insert_tag(&mut map, tag!(parsed GroupId::Default, TagId::FrameRate, "Capture rate", f64, |v| v.to_string(), capture_fps, vec![]), &options);
        if let Some(value) = focal {
            util::insert_tag(&mut map, tag!(parsed GroupId::Lens, TagId::FocalLength, "Focal length", f32, |v| v.to_string(), value, vec![]), &options);
        }
        if let Some(value) = recorded_readout {
            util::insert_tag(&mut map, tag!(parsed GroupId::Imager, TagId::FrameReadoutTime, "Readout", f64, |v| v.to_string(), value, vec![]), &options);
        }
        vec![SampleInfo { tag_map: Some(map), ..Default::default() }]
    }

    #[test]
    fn fx3_readout_covers_resolutions_for_all_focal_paths_and_uses_capture_rate() {
        let db = Database::new();
        for focal in [None, Some(0.0), Some(75.0)] {
            for (width, height, fps, expected, estimated) in [
                (3840,2160,23.976,9.2,false), (3840,2160,50.0,9.2,false),
                (3840,2160,119.88,8.3,false), (1920,1080,59.94,9.2,false),
                (1920,1080,100.0,8.3,false), (1920,1080,200.0,4.15,true),
                (1920,1080,239.76,4.15,true), (4096,2160,48.0,9.2,true),
                (4240,2392,50.0,9.2,true), (4264,2408,59.94,9.2,true),
                (4288,2408,25.0,9.2,true), (1280,720,29.97,9.2,true),
                (3840,2160,1.0,9.2,true), (3840,2160,1000.0/1001.0,9.2,true), (1920,1080,12.0,9.2,true),
            ] {
                let mut camera = Sony { model: Some("ILME-FX3".into()), ..Default::default() };
                let mut samples = sample(focal, fps, None);
                let video = VideoMetadata { width, height, fps:25.0, ..Default::default() };
                camera.process_map(&mut samples, &db.options(), Some(&video), None, None);
                assert_eq!(camera.frame_readout_time, Some(expected), "{width}x{height} {fps} focal={focal:?}");
                let imager = &samples[0].tag_map.as_ref().unwrap()[&GroupId::Imager];
                assert_eq!(imager.get_t(TagId::Custom("readout_estimated".into())).copied().unwrap_or(false), estimated);
            }
        }
    }

    #[test]
    fn fx3_readout_preserves_file_timing_and_other_cameras() {
        let db = Database::new();
        let video = VideoMetadata { width:4240, height:2392, fps:50.0, ..Default::default() };
        let mut samples = sample(Some(75.0), 50.0, Some(7.25));
        let mut camera = Sony { model: Some("ILME-FX3".into()), ..Default::default() };
        camera.process_map(&mut samples, &db.options(), Some(&video), None, None);
        assert_eq!(camera.frame_readout_time, Some(7.25));
        assert!(!samples[0].tag_map.as_ref().unwrap()[&GroupId::Imager].contains_key(&TagId::Custom("readout_estimated".into())));
        let mut other = Sony { model:Some("ILME-FX30".into()), ..Default::default() };
        other.process_fx3_readout_fallback(&mut sample(Some(75.0),50.0,None), &db.options(), Some(&video));
        assert_eq!(other.frame_readout_time, None);
    }

    #[test]
    fn fx3_readout_fallback_uses_the_existing_sony_full_sensor_timing_contract() {
        let db = Database::new();
        let mut samples = sample(Some(75.0),50.0,None);
        let map = samples[0].tag_map.as_mut().unwrap();
        util::insert_tag(map, tag!(parsed GroupId::Imager, TagId::SensorSizePixels, "Sensor size", u32x2, |v| format!("{v:?}"), (4240,2832), vec![]), &db.options());
        util::insert_tag(map, tag!(parsed GroupId::Imager, TagId::CaptureAreaSize, "Capture size", f32x2, |v| format!("{v:?}"), (4240.0,2384.0), vec![]), &db.options());
        let mut camera = Sony { model:Some("ILME-FX3".into()), ..Default::default() };
        let video = VideoMetadata { width:3840,height:2160,fps:50.0,..Default::default() };
        camera.process_map(&mut samples,&db.options(),Some(&video),None,None);
        let full_sensor_readout = camera.frame_readout_time.unwrap();
        assert!((full_sensor_readout * 2384.0 / 2832.0 - 9.2).abs() < 1e-9);
        assert_eq!(samples[0].tag_map.as_ref().unwrap()[&GroupId::Imager].get_t(TagId::FrameReadoutTime) as Option<&f64>,Some(&full_sensor_readout));
    }

    #[test]
    #[ignore = "requires SONY_CAMERA_DB pointing to the updated authoritative camera_db"]
    fn fx3_authoritative_readout_table_covers_all_video_sizes() {
        let db = crate::camera_db::CameraDatabase::load(&std::env::var("SONY_CAMERA_DB").expect("SONY_CAMERA_DB")).unwrap();
        for size in [(3840,2160),(1920,1080),(4096,2160),(4240,2392),(4264,2408),(4288,2408),(1280,720)] {
            for fps in [23.976,24.0,25.0,29.97,30.0,48.0,50.0,59.94,60.0] {
                let result = db.lookup_readout("SONY","ILCE-FX3",size.0,size.1,fps,0.0,35.9,None,&Default::default()).unwrap();
                assert_eq!(result.readout_time_ms,9.2,"{size:?} {fps}");
                assert_eq!(result.is_estimated,!matches!(size,(3840,2160)|(1920,1080)));
            }
        }
        for width in [1920,3840] {
            for fps in [100.0,119.88,120.0] {
                let result = db.lookup_readout("SONY","ILCE-FX3",width,if width==1920{1080}else{2160},fps,0.0,35.9,None,&Default::default()).unwrap();
                assert_eq!(result.readout_time_ms,8.3);
                assert!(!result.is_estimated);
            }
        }
        for fps in [200.0,239.76,240.0] {
            let result = db.lookup_readout("SONY","ILCE-FX3",1920,1080,fps,0.0,35.9,None,&Default::default()).unwrap();
            assert_eq!(result.readout_time_ms,4.15);
            assert!(result.is_estimated);
        }
    }
}
