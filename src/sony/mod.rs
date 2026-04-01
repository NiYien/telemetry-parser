// SPDX-License-Identifier: MIT OR Apache-2.0
// Copyright © 2021-2023 Adrian <adrian.eddy at gmail>

pub mod rtmd_tags;
pub mod mxf;

#[cfg(feature="sony-xml")]
pub mod xml_metadata;

use std::io::*;
use std::sync::{ Arc, atomic::AtomicBool };

use byteorder::{ ReadBytesExt, BigEndian };
use rtmd_tags::*;
use crate::tags_impl::*;
use crate::*;
use memchr::memmem;

#[derive(Default)]
pub struct Sony {
    pub model: Option<String>,
    pub lens: Option<String>,
    frame_readout_time: Option<f64>,
    mvhd_creation_time: Option<String>,
}
impl Sony {
    pub fn camera_type(&self) -> String {
        "Sony".to_owned()
    }
    pub fn has_accurate_timestamps(&self) -> bool {
        true
    }
    pub fn possible_extensions() -> Vec<&'static str> {
        vec!["mp4", "mov", "mxf"]
    }
    pub fn frame_readout_time(&self) -> Option<f64> {
        self.frame_readout_time
    }
    pub fn normalize_imu_orientation(v: String) -> String {
        fn invert_case(x: char) -> char {
            if x.is_ascii_lowercase() { x.to_ascii_uppercase() } else { x.to_ascii_lowercase() }
        }
        assert_eq!(v.len(), 3);
        let mut v = v.chars().collect::<Vec<char>>();

        // Normalize to common orientation - swap X/Y and invert Z
        v.swap(0, 1);
        v[2] = invert_case(v[2]);

        v.iter().collect()
    }

    pub fn detect<P: AsRef<std::path::Path>>(buffer: &[u8], _filepath: P, _options: &crate::InputOptions) -> Option<Self> {
        if let Some(p1) = memmem::find(buffer, b"manufacturer=\"Sony\"") {
            return Some(Self {
                model: util::find_between(&buffer[p1..(p1+1024).min(buffer.len())], b"modelName=\"", b'"'),
                lens: util::find_between(&buffer[p1..(p1+1024).min(buffer.len())], b"Lens modelName=\"", b'"'),
                frame_readout_time: None,
                mvhd_creation_time: util::extract_mvhd_creation_time(buffer),
            });
        }
        if buffer.len() > 4
          && buffer[..4] == [0x06, 0x0E, 0x2B, 0x34] // MXF header
          && memmem::find(buffer, &hex_literal::hex!("060e2b34 01020101 0d010301 17010201")).is_some() // Ancillary, SMPTE ST 436
          && memmem::find(buffer, b"MPC-3628").is_some() { // VENICE 2 model number
            return Some(Self {
                model: Some("VENICE 2".to_string()),
                lens: None,
                frame_readout_time: None,
                mvhd_creation_time: None, // TODO: MXF creation time from Material Package
            });
        }
        None
    }

    pub fn parse<T: Read + Seek, F: Fn(f64)>(&mut self, stream: &mut T, size: usize, progress_cb: F, cancel_flag: Arc<AtomicBool>, options: crate::InputOptions) -> Result<Vec<SampleInfo>> {
        let mut header = [0u8; 4];
        stream.read_exact(&mut header)?;
        stream.seek(SeekFrom::Start(0))?;

        let mut mxf_creation_time: Option<String> = None;
        let mut mxf_creation_subsec: Option<String> = None;
        let mut samples = if header == [0x06, 0x0E, 0x2B, 0x34] { // MXF header
            let (s, ct, cs) = mxf::parse(stream, size, progress_cb, cancel_flag, None, &options, Self::parse_metadata)?;
            mxf_creation_time = ct;
            mxf_creation_subsec = cs;
            s
        } else {
            let mut samples = Vec::new();
            let cancel_flag2 = cancel_flag.clone();
            util::get_metadata_track_samples(stream, size, true, |mut info: SampleInfo, data: &[u8], file_position: u64, _video_md: Option<&VideoMetadata>| {
                if size > 0 {
                    progress_cb(file_position as f64 / size as f64);
                }
                if Self::detect_metadata(data) {
                    let mut map = GroupedTagMap::new();
                    if Self::parse_metadata(&data[0x1C..], &options, &mut map).is_ok() {
                        info.tag_map = Some(map);
                        samples.push(info);
                        if options.probe_only {
                            cancel_flag2.store(true, std::sync::atomic::Ordering::Relaxed);
                        }
                    }
                }
            }, cancel_flag)?;
            samples
        };

        // Get video track metadata (resolution, fps)
        stream.seek(SeekFrom::Start(0))?;
        let video_md = util::get_video_metadata(stream, size).ok();

        self.process_map(&mut samples, &options, video_md.as_ref(), mxf_creation_time.as_deref(), mxf_creation_subsec.as_deref());

        Ok(samples)
    }

    fn process_map(&mut self, samples: &mut Vec<SampleInfo>, options: &crate::InputOptions, video_md: Option<&VideoMetadata>, mxf_creation_time: Option<&str>, mxf_creation_subsec: Option<&str>) {
        // Phase 0: existing RTMD logic (unchanged)
        for sample in samples.iter_mut() {
            if let Some(ref mut map) = sample.tag_map {
                if map.contains_key(&GroupId::Accelerometer) {
                    util::insert_tag(map, tag!(parsed GroupId::Accelerometer, TagId::Unit, "Accelerometer unit", String, |v| v.to_string(), "g".into(), Vec::new()), options);
                }

                if let Some(imager) = map.get_mut(&GroupId::Imager) {
                    if let Some(v) = imager.get_t(TagId::FrameReadoutTime) as Option<&f64> {
                        self.frame_readout_time = Some(*v);
                    }

                    let mut crop_scale = 1.0;
                    if let Some(v) = imager.get(&TagId::Unknown(0xe408)) { if let TagValue::i32(x) = &v.value { crop_scale = *x.get() as f32; } }
                    if crop_scale != 1.0 && crop_scale > 0.0 {
                        if let Some(v) = imager.get_mut(&TagId::CaptureAreaOrigin) {
                            if let TagValue::f32x2(x) = &mut v.value {
                                let _ = x.get(); // make sure it's parsed
                                let vv = x.get_mut();
                                vv.0 /= crop_scale;
                                vv.1 /= crop_scale;
                            }
                        }
                        if let Some(v) = imager.get_mut(&TagId::CaptureAreaSize) {
                            if let TagValue::f32x2(x) = &mut v.value {
                                let _ = x.get(); // make sure it's parsed
                                let vv = x.get_mut();
                                vv.0 /= crop_scale;
                                vv.1 /= crop_scale;
                            }
                        }
                    }
                }
                if let Some(cooke) = map.get_mut(&GroupId::Cooke) {
                    let mut cooke_data: Vec<u8> = Vec::new();
                    if let Some(v) = cooke.get(&TagId::Unknown(0xe208)) { if let TagValue::Unknown(x) = &v.value { cooke_data.extend(&x.raw_data); } }
                    if let Some(v) = cooke.get(&TagId::Unknown(0xe209)) { if let TagValue::Unknown(x) = &v.value { cooke_data.extend(&x.raw_data); } }
                    if !cooke_data.is_empty() {
                        cooke.remove(&TagId::Unknown(0xe208));
                        cooke.remove(&TagId::Unknown(0xe209));
                        cooke.insert(TagId::Data2, tag!(GroupId::Cooke, TagId::Data2, "BinaryMetadata2", Json, "{:?}", |d| {
                            Ok(serde_json::Value::Array(crate::cooke::bin::parse(d.get_ref()).unwrap())) // TODO: unwrap
                        }, cooke_data));
                    }
                }
            }
        }
        if let Some(lens_name) = self.lens.as_ref() {
            if let Some(first_sample) = samples.first_mut() {
                if let Some(ref mut map) = first_sample.tag_map {
                    util::insert_tag(map, tag!(parsed GroupId::Lens, TagId::DisplayName, "Lens name", String, |v| v.to_string(), lens_name.clone(), Vec::new()), options);
                }
            }
        }

        // Extract creation date: CaptureTimestamp → MXF Preface → mvhd fallback
        {
            let capture_ts = samples.first()
                .and_then(|s| s.tag_map.as_ref())
                .and_then(|map| {
                    map.get(&GroupId::Default)
                        .and_then(|m| m.get_t(TagId::CaptureTimestamp) as Option<&u64>)
                        .copied()
                });

            let (date_str, subsec) = if let Some(ts) = capture_ts {
                (chrono::TimeZone::timestamp_opt(&chrono::Utc, ts as i64, 0)
                    .single()
                    .map(|dt| dt.format("%Y:%m:%d %H:%M:%S").to_string()),
                 Some("500".to_string()))
            } else if let Some(mxf_time) = mxf_creation_time {
                (Some(mxf_time.to_string()),
                 mxf_creation_subsec.map(|s| s.to_string()).or_else(|| Some("500".to_string())))
            } else {
                (self.mvhd_creation_time.clone(), Some("500".to_string()))
            };

            if let Some(date) = date_str {
                if let Some(first_sample) = samples.first_mut() {
                    let map = first_sample.tag_map.get_or_insert_with(GroupedTagMap::new);
                    // No timezone: TZ byte format in RTMD 0xE304 is unresolved
                    util::write_creation_date_tags(map, &date, None, subsec.as_deref(), options);
                }
            }
        }

        // Determine focal length status from first sample
        // Case 0: FocalLength > 0 → Gyroflow handles everything, done
        // Case 1: FocalLength tag exists but = 0 → compute unit_px_fl from RTMD sensor geometry
        // Case 2: No FocalLength tag → full JSON fallback
        let focal_length_status = samples.first()
            .and_then(|s| s.tag_map.as_ref())
            .and_then(|map| {
                map.get(&GroupId::Lens)
                    .and_then(|m| m.get_t(TagId::FocalLength) as Option<&f32>)
                    .map(|v| *v)
            });

        match focal_length_status {
            Some(fl) if fl > 0.0 => {
                // Case 0: RTMD complete, Gyroflow calculates everything
                return;
            }
            Some(_) => {
                // Case 1: FocalLength = 0, compute unit_px_fl from RTMD sensor geometry
                self.process_case1_rtmd_no_focal(samples, options, video_md);
            }
            None => {
                // Case 2: No FocalLength tag, full JSON fallback
                self.process_case2_json_fallback(samples, options, video_md);
            }
        }
    }

    /// Case 1: RTMD exists but FocalLength = 0.
    /// Compute unit_pixel_focal_length from RTMD sensor geometry so user can set FL manually.
    fn process_case1_rtmd_no_focal(&mut self, samples: &mut Vec<SampleInfo>, options: &crate::InputOptions, video_md: Option<&VideoMetadata>) {
        let resolution_w = video_md.map(|v| v.width as u32).unwrap_or(0);
        if resolution_w == 0 { return; }

        // Extract PixelPitch and CaptureAreaSize from first sample
        let unit_px_fl = samples.first()
            .and_then(|s| s.tag_map.as_ref())
            .and_then(|map| {
                let imager = map.get(&GroupId::Imager)?;
                let pixel_pitch = imager.get_t(TagId::PixelPitch) as Option<&(u32, u32)>;
                let capture_area = imager.get_t(TagId::CaptureAreaSize) as Option<&(f32, f32)>;
                match (pixel_pitch, capture_area) {
                    (Some(pp), Some(ca)) if pp.0 > 0 && ca.0 > 0.0 => {
                        // sensor_w_mm = pixel_pitch_nm * capture_area_pixels / 1e6
                        let sensor_w_mm = pp.0 as f64 * ca.0 as f64 / 1_000_000.0;
                        if sensor_w_mm > 0.0 {
                            Some(resolution_w as f64 / sensor_w_mm)
                        } else {
                            None
                        }
                    }
                    _ => None,
                }
            });

        if let Some(upfl) = unit_px_fl {
            for sample in samples.iter_mut() {
                if let Some(ref mut map) = sample.tag_map {
                    util::insert_tag(map, tag!(parsed GroupId::Lens, TagId::Custom("unit_pixel_focal_length".into()), "Pixel focal length per mm", f64, |v| format!("{:.4}", v), upfl, Vec::new()), options);
                }
            }
        }

        // Slow-motion detection
        self.detect_slowmo(samples, options, video_md);
    }

    /// Always output RecordFrameRate from RTMD Capture Frame Rate (0x8106).
    fn detect_slowmo(&self, samples: &mut Vec<SampleInfo>, options: &crate::InputOptions, video_md: Option<&VideoMetadata>) {
        let playback_fps = video_md.map(|v| v.fps).unwrap_or(0.0);
        if playback_fps <= 0.0 { return; }

        // Get RTMD Capture Frame Rate from first sample (this is the recording fps)
        let record_fps = samples.first()
            .and_then(|s| s.tag_map.as_ref())
            .and_then(|map| map.get(&GroupId::Default))
            .and_then(|m| m.get_t(TagId::FrameRate) as Option<&f64>)
            .copied()
            .unwrap_or(playback_fps);

        if let Some(ref mut map) = samples.first_mut().and_then(|s| s.tag_map.as_mut()) {
            util::insert_tag(map, tag!(parsed GroupId::Default, TagId::RecordFrameRate, "Record frame rate", f64, |v| format!("{:.3} fps", v), record_fps, Vec::new()), options);
        }
    }

    /// Case 2: No RTMD FocalLength tag. Full JSON database fallback.
    fn process_case2_json_fallback(&mut self, samples: &mut Vec<SampleInfo>, options: &crate::InputOptions, video_md: Option<&VideoMetadata>) {
        let db_path = match &options.camera_db_path {
            Some(p) => p,
            None => return,
        };
        let db = match crate::camera_db::CameraDatabase::load(db_path) {
            Ok(d) => d,
            Err(_) => return,
        };
        let model_name = match &self.model {
            Some(m) => m.strip_prefix("Sony ").unwrap_or(m).to_string(),
            None => return,
        };

        let resolution_w = video_md.map(|v| v.width as u32).unwrap_or(0);
        let resolution_h = video_md.map(|v| v.height as u32).unwrap_or(0);
        let fps = video_md.map(|v| v.fps).unwrap_or(0.0);

        // Write video resolution to first sample's tag_map
        if let Some(vmd) = video_md {
            if samples.is_empty() {
                samples.push(SampleInfo::default());
            }
            if let Some(ref mut map) = samples.first_mut().and_then(|s| s.tag_map.as_mut()) {
                util::insert_tag(map, tag!(parsed GroupId::Default, TagId::Custom("video_width".into()), "Video output width", u32, |v| format!("{} px", v), vmd.width as u32, Vec::new()), options);
                util::insert_tag(map, tag!(parsed GroupId::Default, TagId::Custom("video_height".into()), "Video output height", u32, |v| format!("{} px", v), vmd.height as u32, Vec::new()), options);
            }
        }

        // Extract sony_crop (ElectricalExtenderMagnification, tag 0x810C) from first sample
        let sony_crop = samples.first()
            .and_then(|s| s.tag_map.as_ref())
            .and_then(|map| {
                map.get(&GroupId::Default)
                    .and_then(|m| m.get(&TagId::Custom("ElectricalExtenderMagnification".into())))
                    .and_then(|tag| if let TagValue::u16(v) = &tag.value { Some(*v.get() as f64 / 100.0) } else { None })
            })
            .unwrap_or(1.0);

        // First pass on frame 0
        let db_result: Option<(f32, f64, Option<f64>)> = {
            if samples.is_empty() {
                samples.push(SampleInfo::default());
            }
            if let Some(ref mut map) = samples.first_mut().and_then(|s| s.tag_map.as_mut()) {
                if let Some((matched_name, model_data)) = db.process_model("SONY", &model_name, map, options) {
                    self.model = Some(matched_name.to_string());
                    let sensor_w = model_data.sw;

                    // Infer view_angle for crop rules
                    let is_apsc_body = sensor_w < 30.0; // APS-C sensor
                    let view_angle: Option<&str> = if is_apsc_body {
                        Some("APSC")
                    } else if sony_crop > 1.4 && sony_crop < 1.65 {
                        Some("APSC") // Full-frame in APS-C crop mode
                    } else {
                        Some("FULL")
                    };

                    let tags = std::collections::HashMap::new();
                    let db_crop = db.process_crop("SONY", matched_name, resolution_w, resolution_h, fps, view_angle, &tags, map, options);
                    let effective_crop = db_crop * sony_crop;

                    // Write effective crop_factor (overwrite if db_crop was already written by process_crop)
                    if sony_crop != 1.0 {
                        util::insert_tag(map, tag!(parsed GroupId::Default, TagId::Custom("crop_factor".into()), "Crop factor", f64, |v| format!("{:.4}", v), effective_crop, Vec::new()), options);
                    }

                    // Readout fallback
                    if self.frame_readout_time.is_none() {
                        if let Some(rt) = db.process_readout("SONY", matched_name, resolution_w, resolution_h, fps, 0.0, sensor_w, &tags, map, options) {
                            self.frame_readout_time = Some(rt);
                        }
                    }

                    // unit_pixel_focal_length
                    let unit_px_fl = if resolution_w > 0 && sensor_w > 0.0 {
                        Some(resolution_w as f64 * effective_crop / sensor_w as f64)
                    } else {
                        None
                    };

                    if let Some(upfl) = unit_px_fl {
                        util::insert_tag(map, tag!(parsed GroupId::Lens, TagId::Custom("unit_pixel_focal_length".into()), "Pixel focal length per mm", f64, |v| format!("{:.4}", v), upfl, Vec::new()), options);
                    }

                    Some((sensor_w, effective_crop, unit_px_fl))
                } else {
                    None
                }
            } else {
                None
            }
        };

        // Second pass: write to remaining samples
        if let Some((_sensor_w, effective_crop, unit_px_fl)) = db_result {
            for sample in samples.iter_mut().skip(1) {
                if let Some(ref mut smap) = sample.tag_map {
                    if !smap.get(&GroupId::Default).map_or(false, |m| m.contains_key(&TagId::Custom("crop_factor".into()))) {
                        util::insert_tag(smap, tag!(parsed GroupId::Default, TagId::Custom("crop_factor".into()), "Crop factor", f64, |v| format!("{:.4}", v), effective_crop, Vec::new()), options);
                    }
                    if let Some(upfl) = unit_px_fl {
                        if !smap.get(&GroupId::Lens).map_or(false, |m| m.contains_key(&TagId::Custom("unit_pixel_focal_length".into()))) {
                            util::insert_tag(smap, tag!(parsed GroupId::Lens, TagId::Custom("unit_pixel_focal_length".into()), "Pixel focal length per mm", f64, |v| format!("{:.4}", v), upfl, Vec::new()), options);
                        }
                    }
                }
            }
        }

        // Slow-motion detection
        self.detect_slowmo(samples, options, video_md);
    }

    fn detect_metadata(data: &[u8]) -> bool {
        data.len() > 0x1C && data[0..2] == [0x00, 0x1C]
    }

    pub fn parse_metadata(data: &[u8], options: &crate::InputOptions, map: &mut GroupedTagMap) -> Result<()> {
        let mut slice = Cursor::new(data);
        let datalen = data.len() as usize;

        while slice.position() < datalen as u64 {
            let tag = slice.read_u16::<BigEndian>()?;
            if tag == 0x060e {
                /*let uuid = &data[slice.position() as usize - 2..slice.position() as usize + 14];
                log::debug!("--- {} ---", match &uuid[..16] {
                    &hex_literal::hex!("060E2B34 02530101 0C020101 01010000") => "LensUnitMetadata",
                    &hex_literal::hex!("060E2B34 02530101 0C020101 02010000") => "CameraUnitMetadata",
                    &hex_literal::hex!("060E2B34 02530101 0C020101 7F010000") => "UserDefinedAcquisitionMetadata",
                    _ => "Unknown"
                });*/
                slice.seek(SeekFrom::Current(14))?;
                continue;
            }
            if tag == 0 || tag == 0xffff { break; }
            let len = slice.read_u16::<BigEndian>()? as usize;
            let pos = slice.position() as usize;
            if pos + len > datalen {
                log::warn!("Invalid tag: {:02x}, len: {}, Available: {}", tag, len, datalen - pos);
                // log::warn!("{}", crate::util::to_hex(&data[pos-4..]));
                break;
            }
            let tag_data = &data[pos..(pos + len)];
            slice.seek(SeekFrom::Current(len as i64))?;
            if tag == 0x8300 { // Container
                Self::parse_metadata(tag_data, options, map)?;
                continue;
            }
            let mut tag_info = get_tag(tag, tag_data);
            tag_info.native_id = Some(tag as u32);

            util::insert_tag(map, tag_info, options);
        }
        Ok(())
    }
}
