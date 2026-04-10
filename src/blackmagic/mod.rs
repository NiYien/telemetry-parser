// SPDX-License-Identifier: MIT OR Apache-2.0
// Copyright © 2022 Adrian <adrian.eddy at gmail>

use std::io::*;
use std::sync::{ Arc, atomic::AtomicBool };

use crate::tags_impl::*;
use crate::*;
use crate::tiff_ifd;
use byteorder::{ ReadBytesExt, LittleEndian, BigEndian };
use memchr::memmem;

#[derive(Default)]
pub struct BlackmagicBraw {
    pub model: Option<String>,
    frame_readout_time: Option<f64>,
    is_braw: bool,
    /// For Video Assist BRAW: the original manufacturer (e.g. "Panasonic")
    original_manufacturer: Option<String>,
}

impl BlackmagicBraw {
    pub fn camera_type(&self) -> String {
        if let Some(ref mfr) = self.original_manufacturer {
            mfr.clone()
        } else if self.is_braw && self.model.is_none() {
            "Blackmagic RAW".to_owned()
        } else {
            "Blackmagic".to_owned()
        }
    }
    pub fn has_accurate_timestamps(&self) -> bool {
        match self.model.as_deref() {
            Some("Micro Studio Camera 4K G2") => false,
            _ => true
        }
    }
    pub fn possible_extensions() -> Vec<&'static str> {
        vec!["braw", "mov", "mp4", "dng"]
    }
    pub fn frame_readout_time(&self) -> Option<f64> {
        self.frame_readout_time
    }
    pub fn normalize_imu_orientation(v: String) -> String {
        v
    }

    pub fn detect<P: AsRef<std::path::Path>>(buffer: &[u8], filepath: P, _options: &crate::InputOptions) -> Option<Self> {
        // BRAW detection (original)
        if memmem::find(buffer, b"Blackmagic Design").is_some() && memmem::find(buffer, b"braw_codec_bitrate").is_some() {
            return Some(Self { is_braw: true, ..Self::default() });
        }

        let ext = filesystem::get_extension(filepath.as_ref().to_str().unwrap_or_default()).to_ascii_lowercase();

        // MOV/MP4: check for Blackmagic QuickTime mdta keys
        if ext == "mov" || ext == "mp4" {
            if memmem::find(buffer, b"com.blackmagic-design.cameraType").is_some()
                || memmem::find(buffer, b"com.blackmagic-design.cameraCameraType").is_some()
                || memmem::find(buffer, b"com.blackmagic-design.cinemacamera").is_some()
            {
                return Some(Self { is_braw: false, ..Self::default() });
            }
        }

        // DNG: parse TIFF IFD for UniqueCameraModel (0xC614) or Model (0x0110)
        if ext == "dng" {
            if tiff_ifd::is_tiff_header(buffer) {
                let mut found_blackmagic = false;
                let is_le = tiff_ifd::detect_byte_order(buffer).unwrap_or(true);
                if let Some(ifd0_offset) = tiff_ifd::read_u32(buffer, 4, is_le) {
                    tiff_ifd::parse_ifd_entries(buffer, ifd0_offset as usize, is_le,
                        &mut |tag, _typ, count, value_data, _value_offset| {
                            if tag == 0xC614 || tag == 0x0110 {
                                let s = tiff_ifd::read_string(value_data, count);
                                if s.contains("Blackmagic") {
                                    found_blackmagic = true;
                                }
                            }
                        });
                }
                if found_blackmagic {
                    return Some(Self { is_braw: false, ..Self::default() });
                }
            }
        }

        None
    }

    fn normalize_bmd_model(name: &str) -> String {
        let mut n = name.to_string();
        n = n.replace("Blackmagic Pocket Cinema Camera", "BMPCC");
        n = n.replace("Blackmagic Production Camera", "BMCC");
        n = n.replace("Blackmagic Cinema Camera", "BMCC");
        n
    }

    /// Max native sensor resolution width in pixels per model (ref NiYien Tool bmd.cpp)
    fn max_resolution_w(model: &str) -> u32 {
        match model {
            "BMPCC"                  => 1920,
            "BMCC 4K"               => 3840,
            "BMCC 6K"               => 6048,
            "BMPCC 4K"              => 3840,
            "BMPCC 6K"              => 6144,
            "URSA Mini 4K"          => 3840,
            "URSA Mini 4.6K"        => 4608,
            "URSA Mini Pro 4.6K G2" => 4608,
            "URSA Mini Pro 12K"     => 12288,
            _ => 0
        }
    }

    pub fn parse<T: Read + Seek, F: Fn(f64)>(&mut self, stream: &mut T, size: usize, progress_cb: F, cancel_flag: Arc<AtomicBool>, options: crate::InputOptions) -> Result<Vec<SampleInfo>> {
        if !self.is_braw {
            return self.parse_non_braw(stream, size, progress_cb, cancel_flag, options);
        }
        let mut gyro = Vec::new();
        let mut accl = Vec::new();

        let mut map = GroupedTagMap::new();

        let mut samples = Vec::new();
        let mut frame_rate = None;

        let mut firmware_version = String::new();
        // let mut crop_factor = 1.0;
        if let Ok(meta) = self.parse_meta(stream, size) {
            if let Some(cam) = meta.get("camera_type").and_then(|x| x.as_str()) {
                self.model = Some(cam.trim_start_matches("Blackmagic ").to_string());
            }
            // Video Assist BRAW fix (task 5.1): if manufacturer is not "Blackmagic Design",
            // this is a Video Assist recording from another camera brand.
            // Use the original manufacturer as camera_type, and the original camera_type as model.
            if let Some(mfr) = meta.get("manufacturer").and_then(|x| x.as_str()) {
                if mfr != "Blackmagic Design" {
                    // The camera_type field contains the actual camera model (e.g. "Panasonic S5IIX")
                    if let Some(cam) = meta.get("camera_type").and_then(|x| x.as_str()) {
                        // Strip manufacturer prefix from model if present (e.g. "Panasonic S5IIX" → "S5IIX")
                        let model = cam.strip_prefix(mfr).map(|s| s.trim()).unwrap_or(cam);
                        self.model = Some(model.to_string());
                    }
                    self.original_manufacturer = Some(mfr.to_string());
                }
            }
            if let Some(fw) = meta.get("firmware_version").and_then(|x| x.as_str()) {
                firmware_version = fw.to_string();
            }
            if let Some(v) = meta.get("crop_origin").and_then(|v| v.as_array()).and_then(|x| Some((x.get(0)?.as_f64()? as f32, x.get(1)?.as_f64()? as f32))) {
                util::insert_tag(&mut map, tag!(parsed GroupId::Imager, TagId::CaptureAreaOrigin, "Capture area origin", f32x2, |v| format!("{v:?}"), v, vec![]), &options);
            }
            if let Some(v) = meta.get("sensor_area_captured").and_then(|v| v.as_array()).and_then(|x| Some((x.get(0)?.as_f64()? as f32, x.get(1)?.as_f64()? as f32))) {
                util::insert_tag(&mut map, tag!(parsed GroupId::Imager, TagId::CaptureAreaSize, "Capture area size", f32x2, |v| format!("{v:?}"), v, vec![]), &options);
            }
            match self.model.as_deref() {
                Some("Pocket Cinema Camera 6K Pro") |
                Some("Pocket Cinema Camera 6K G2") |
                Some("Pocket Cinema Camera 6K") => {
                    util::insert_tag(&mut map, tag!(parsed GroupId::Imager, TagId::PixelPitch, "Pixel pitch", u32x2, |v| format!("{v:?}"), (3759, 3759), vec![]), &options);
                    // crop_factor = 1.5;
                },
                Some("Pocket Cinema Camera 4K") => {
                    util::insert_tag(&mut map, tag!(parsed GroupId::Imager, TagId::PixelPitch, "Pixel pitch", u32x2, |v| format!("{v:?}"), (4628, 4628), vec![]), &options);
                    // crop_factor = 2.0;
                },
                Some("Micro Studio Camera 4K G2") => {
                    // TODO: this is not confirmed
                    util::insert_tag(&mut map, tag!(parsed GroupId::Imager, TagId::PixelPitch, "Pixel pitch", u32x2, |v| format!("{v:?}"), (4628, 4628), vec![]), &options);
                    // crop_factor = 2.0;
                },
                _ => { }
            }

            util::insert_tag(&mut map, tag!(parsed GroupId::Default, TagId::Metadata, "Metadata", Json, |v| serde_json::to_string(v).unwrap(), meta, vec![]), &options);
        }

        // camera_db integration for BRAW path (skip for Video Assist non-BMD cameras)
        if self.original_manufacturer.is_none() {
            if let Some(db_path) = &options.camera_db_path {
                if let Ok(db) = crate::camera_db::CameraDatabase::load(db_path) {
                    let raw_name = self.model.as_deref().unwrap_or("");
                    if let Some((model_name, model_data)) = db.process_model("BLACKMAGIC", raw_name, &mut map, &options) {
                        self.model = Some(model_name.to_string());
                        let sensor_w = model_data.sw;

                        // Compute unit_pixel_focal_length from sensor_area_captured (BRAW metadata)
                        if let Some(v) = map.get(&GroupId::Imager).and_then(|m| m.get_t(TagId::CaptureAreaSize) as Option<&(f32, f32)>).copied() {
                            let captured_w = v.0 as f64;
                            if captured_w > 0.0 {
                                let effective_sensor_w = sensor_w as f64 * captured_w / Self::max_resolution_w(model_name).max(1) as f64;
                                if effective_sensor_w > 0.0 {
                                    let unit_px_fl = captured_w / effective_sensor_w;
                                    util::insert_tag(&mut map, tag!(parsed GroupId::Lens, TagId::Custom("unit_pixel_focal_length".into()), "Pixel focal length per mm", f64, |v| format!("{:.4}", v), unit_px_fl, vec![]), &options);
                                }
                            }
                        }

                        let tags = std::collections::HashMap::new();
                        if self.frame_readout_time.is_none() {
                            let _ = db.process_readout("BLACKMAGIC", model_name, 0, 0, 0.0, 0.0, sensor_w, &tags, &mut map, &options);
                        }
                    }
                }
            }
        }

        let _ = util::get_track_samples(stream, size, mp4parse::TrackType::Video, true, Some(8192), |mut info: SampleInfo, data: &[u8], file_position: u64, _video_md: Option<&VideoMetadata>| {
            if size > 0 {
                progress_cb(file_position as f64 / size as f64 / 3.0);
            }
            if let Ok(md) = Self::parse_per_frame_meta(data) {
                let mut map = GroupedTagMap::new();

                if let Some(v) = md.get("sensor_rate").and_then(|v| v.as_array()) {
                    if v.len() == 2 {
                        frame_rate = v[0].as_u64().zip(v[1].as_u64()).map(|(a, b)| a as f64 / b.max(1) as f64);
                    }
                }
                if let Some(v) = md.get("focal_length").and_then(|v| v.as_str()) {
                    let v = v.replace("mm", "");
                    if let Ok(v) = v.parse::<f32>() {
                        util::insert_tag(&mut map, tag!(parsed GroupId::Lens, TagId::FocalLength, "Focal length", f32, |v| format!("{v:.2} mm"), v, vec![]), &options);
                    }
                }

                util::insert_tag(&mut map, tag!(parsed GroupId::Default, TagId::Metadata, "Metadata", Json, |v| serde_json::to_string(v).unwrap(), md, vec![]), &options);
                info.tag_map = Some(map);
                samples.push(info);
                if options.probe_only {
                    cancel_flag.store(true, std::sync::atomic::Ordering::Relaxed);
                }
            }
        }, cancel_flag.clone());

        if let Some(fr) = frame_rate {
            util::insert_tag(&mut map, tag!(parsed GroupId::Default, TagId::FrameRate, "Frame rate", f64, |v| format!("{:?}", v), fr, vec![]), &options);
            if let Some(rs) = self.frame_readout_time {
                if firmware_version == "7.9" && rs > (1000.0 / fr) {
                    self.frame_readout_time = Some(rs / 2.0); // Bug in firmware v7.9.0
                }
            }
        }
        let cancel_flag2 = cancel_flag.clone();
        util::get_metadata_track_samples(stream, size, false, |info: SampleInfo, data: &[u8], file_position: u64, _video_md: Option<&VideoMetadata>| {
            if size > 0 {
                progress_cb(((info.track_index as f64 - 1.0) + (file_position as f64 / size as f64)) / 3.0);
            }

            if data.len() >= 4+4+3*4 {
                let mut d = Cursor::new(data);
                crate::try_block!({
                    d.seek(SeekFrom::Start(8)).ok()?;
                    if &data[4..8] == b"mogy" {
                        gyro.push(TimeVector3 { t: (info.timestamp_ms - self.frame_readout_time.unwrap_or(0.0) / 2.0) / 1000.0,
                            x: d.read_f32::<LittleEndian>().ok()? as f64,
                            y: d.read_f32::<LittleEndian>().ok()? as f64,
                            z: d.read_f32::<LittleEndian>().ok()? as f64
                        });
                    } else if &data[4..8] == b"moac" {
                        accl.push(TimeVector3 { t: (info.timestamp_ms - self.frame_readout_time.unwrap_or(0.0) / 2.0) / 1000.0,
                            x: -d.read_f32::<LittleEndian>().ok()? as f64,
                            y: -d.read_f32::<LittleEndian>().ok()? as f64,
                            z: -d.read_f32::<LittleEndian>().ok()? as f64
                        });
                    }
                });
            }
            if options.probe_only {
                cancel_flag2.store(true, std::sync::atomic::Ordering::Relaxed);
            }
        }, cancel_flag)?;


        util::insert_tag(&mut map, tag!(parsed GroupId::Accelerometer, TagId::Data, "Accelerometer data", Vec_TimeVector3_f64, |v| format!("{:?}", v), accl, vec![]), &options);
        util::insert_tag(&mut map, tag!(parsed GroupId::Gyroscope,     TagId::Data, "Gyroscope data",     Vec_TimeVector3_f64, |v| format!("{:?}", v), gyro, vec![]), &options);

        util::insert_tag(&mut map, tag!(parsed GroupId::Accelerometer, TagId::Unit, "Accelerometer unit", String, |v| v.to_string(), "m/s²".into(),  Vec::new()), &options);
        util::insert_tag(&mut map, tag!(parsed GroupId::Gyroscope,     TagId::Unit, "Gyroscope unit",     String, |v| v.to_string(), "rad/s".into(), Vec::new()), &options);

        let imu_orientation = match self.model.as_deref() {
            Some("Micro Studio Camera 4K G2") if firmware_version == "8.4" => "yXZ",
            _ => "yxz"
        };
        util::insert_tag(&mut map, tag!(parsed GroupId::Accelerometer, TagId::Orientation, "IMU orientation", String, |v| v.to_string(), imu_orientation.into(), Vec::new()), &options);
        util::insert_tag(&mut map, tag!(parsed GroupId::Gyroscope,     TagId::Orientation, "IMU orientation", String, |v| v.to_string(), imu_orientation.into(), Vec::new()), &options);

        samples.insert(0, SampleInfo { tag_map: Some(map), ..Default::default() });

        Ok(samples)
    }

    /// Parse non-BRAW files (MOV/MP4/DNG) from Blackmagic cameras
    fn parse_non_braw<T: Read + Seek, F: Fn(f64)>(&mut self, stream: &mut T, size: usize, progress_cb: F, cancel_flag: Arc<AtomicBool>, options: crate::InputOptions) -> Result<Vec<SampleInfo>> {
        let mut map = GroupedTagMap::new();
        let samples = Vec::new();

        // Read file buffer for metadata extraction
        stream.seek(SeekFrom::Start(0))?;
        let all = read_beginning_and_end(stream, size, 4*1024*1024)?;

        // Try to parse QuickTime mdta keys/ilst metadata
        let mut md = serde_json::Map::<String, serde_json::Value>::new();
        let mut raw_keys = Vec::new();
        let mut offs = 0;
        let mut meta_data: Option<&[u8]> = None;
        while let Some(pos) = memchr::memmem::find(&all[offs..], b"meta") {
            if all.len() > offs+pos+12 && &all[offs+pos+8..offs+pos+12] == b"hdlr" {
                if let Ok(sz) = (&all[offs+pos-4..]).read_u32::<BigEndian>().map(|s| s as usize) {
                    if offs+pos-4+sz <= all.len() {
                        meta_data = Some(&all[offs+pos-4..offs+pos-4+sz][8..]);
                    }
                }
                break;
            }
            offs += pos + 4;
        }

        if let Some(meta_buf) = meta_data {
            Self::iter_boxes(meta_buf, false, |name, d, _| {
                if name == "keys" {
                    Self::iter_boxes(&d[8..], false, |_, d, _| {
                        if let Ok(key) = std::str::from_utf8(d) {
                            raw_keys.push(key.to_string());
                        }
                        Ok(())
                    })?;
                }
                if name == "ilst" {
                    Self::iter_boxes(d, true, |_, d, i| {
                        let typ = (&d[..4]).read_u32::<BigEndian>()?;
                        if let Some(key) = raw_keys.get(i).cloned() {
                            let mut d = &d[8..];
                            let v = match typ {
                                1  => serde_json::to_value(std::str::from_utf8(d).unwrap_or("")),
                                23 => serde_json::to_value(d.read_f32::<BigEndian>()? as f64),
                                24 => serde_json::to_value(d.read_f64::<BigEndian>()?),
                                65 => serde_json::to_value(d.read_i8()?),
                                66 => serde_json::to_value(d.read_i16::<BigEndian>()?),
                                67 => serde_json::to_value(d.read_i32::<BigEndian>()?),
                                70 |
                                71 => serde_json::to_value([d.read_f32::<BigEndian>()? as f64, d.read_f32::<BigEndian>()? as f64]),
                                74 => serde_json::to_value(d.read_i64::<BigEndian>()?),
                                75 => serde_json::to_value(d.read_u8()?),
                                76 => serde_json::to_value(d.read_u16::<BigEndian>()?),
                                77 => serde_json::to_value(d.read_u32::<BigEndian>()?),
                                78 => serde_json::to_value(d.read_u64::<BigEndian>()?),
                                _ => Err(serde_json::Error::io(ErrorKind::InvalidData.into()))
                            };
                            if let Ok(v) = v {
                                md.insert(key, v);
                            }
                        }
                        Ok(())
                    })?;
                }
                Ok(())
            })?;
        }

        // ---- Model extraction (multi-level fallback, ref NiYien Tool priority chain) ----

        // Level 1: com.blackmagic-design.cameraType (primary)
        let mut model_name = None;
        if let Some(v) = md.get("com.blackmagic-design.cameraType").and_then(|v| v.as_str()) {
            model_name = Some(v.to_string());
        }
        // Level 2: com.blackmagic-design.cameraCameraType (secondary)
        if model_name.is_none() {
            if let Some(v) = md.get("com.blackmagic-design.cameraCameraType").and_then(|v| v.as_str()) {
                model_name = Some(v.to_string());
            }
        }
        // Level 3: infer from com.apple.proapps.customgamma
        if model_name.is_none() {
            if let Some(gamma) = md.get("com.apple.proapps.customgamma").and_then(|v| v.as_str()) {
                let g = gamma.to_lowercase();
                if g.contains("productioncamera4k") {
                    model_name = Some("Blackmagic Production Camera 4K".to_string());
                } else if g.contains("pocketcinemacamera6k") {
                    model_name = Some("Blackmagic Pocket Cinema Camera 6K".to_string());
                } else if g.contains("pocketcinemacamera4k") {
                    model_name = Some("Blackmagic Pocket Cinema Camera 4K".to_string());
                } else if g.contains("pocketcinemacamera") {
                    model_name = Some("Blackmagic Pocket Cinema Camera".to_string());
                } else if g.contains("ursaminipro") {
                    model_name = Some("URSA Mini Pro 4.6K G2".to_string());
                } else if g.contains("ursamini") {
                    model_name = Some("URSA Mini 4.6K".to_string());
                }
                // "cinemacamera" alone is too generic — leave model_name as None
            }
        }
        // Level 4: DNG TIFF IFD (only for DNG/TIFF files, not MOV/MP4)
        if model_name.is_none() && tiff_ifd::is_tiff_header(&all) {
            let is_le = tiff_ifd::detect_byte_order(&all).unwrap_or(true);
            if let Some(ifd0_offset) = tiff_ifd::read_u32(&all, 4, is_le) {
                let mut unique_model = None;
                let mut model_tag = None;
                tiff_ifd::parse_ifd_entries(&all, ifd0_offset as usize, is_le,
                    &mut |tag, _typ, count, value_data, _value_offset| {
                        match tag {
                            0xC614 => { // UniqueCameraModel (preferred)
                                let s = tiff_ifd::read_string(value_data, count);
                                if s.contains("Blackmagic") { unique_model = Some(s); }
                            }
                            0x0110 => { // Model (fallback)
                                let s = tiff_ifd::read_string(value_data, count);
                                if s.contains("Blackmagic") { model_tag = Some(s); }
                            }
                            _ => {}
                        }
                    });
                model_name = unique_model.or(model_tag);
            }
        }

        // Apply model normalization
        if let Some(ref name) = model_name {
            self.model = Some(Self::normalize_bmd_model(name));
        }

        // Extract focal length from mdta keys (e.g. "31mm" -> 31.0)
        let mut focal_length: Option<f32> = None;
        if let Some(v) = md.get("com.blackmagic-design.cameraLensFocalLength").and_then(|v| v.as_str()) {
            let v = v.replace("mm", "");
            if let Ok(fl) = v.trim().parse::<f32>() {
                focal_length = Some(fl);
                util::insert_tag(&mut map, tag!(parsed GroupId::Lens, TagId::FocalLength, "Focal length", f32, |v| format!("{v:.2} mm"), fl, vec![]), &options);
            }
        }

        // Extract sensor_area_captured from mdta keys (8 bytes: first 4 = f32 big-endian = sensor_width)
        let mut sensor_area_captured_w: Option<f32> = None;
        if let Some(v) = md.get("com.blackmagic-design.cameraSensorAreaCaptured") {
            if let Some(arr) = v.as_array() {
                if arr.len() == 2 {
                    let w = arr[0].as_f64().unwrap_or(0.0) as f32;
                    let h = arr[1].as_f64().unwrap_or(0.0) as f32;
                    if w > 0.0 { sensor_area_captured_w = Some(w); }
                    util::insert_tag(&mut map, tag!(parsed GroupId::Imager, TagId::CaptureAreaSize, "Capture area size", f32x2, |v| format!("{v:?}"), (w, h), vec![]), &options);
                }
            }
        }

        // Extract additional BMD-specific metadata tags (ref NiYien Tool)
        if let Some(v) = md.get("com.blackmagic-design.iso").and_then(|v| v.as_i64()) {
            util::insert_tag(&mut map, tag!(parsed GroupId::Default, TagId::Custom("iso".into()), "ISO", i64, |v| format!("{v}"), v, vec![]), &options);
        }
        if let Some(v) = md.get("com.blackmagic-design.shutterAngle").and_then(|v| v.as_i64()) {
            let angle = v as f64 / 100.0;
            util::insert_tag(&mut map, tag!(parsed GroupId::Default, TagId::ExposureTime, "Shutter angle", f32, |v| format!("{v:.1}°"), angle as f32, vec![]), &options);
        }
        if let Some(v) = md.get("com.blackmagic-design.whiteBalanceKelvin").and_then(|v| v.as_i64()) {
            util::insert_tag(&mut map, tag!(parsed GroupId::Default, TagId::Custom("white_balance_kelvin".into()), "White balance", i64, |v| format!("{v} K"), v, vec![]), &options);
        }
        if let Some(v) = md.get("com.blackmagic-design.firmware").and_then(|v| v.as_str()) {
            util::insert_tag(&mut map, tag!(parsed GroupId::Default, TagId::Custom("firmware".into()), "Firmware", String, |v| v.clone(), v.into(), vec![]), &options);
        }

        // Get video metadata (resolution, fps) from QuickTime track
        stream.seek(SeekFrom::Start(0))?;
        let video_md = util::get_video_metadata(stream, size).ok();
        let resolution_w = video_md.as_ref().map(|v| v.width as u32).unwrap_or(0);
        let resolution_h = video_md.as_ref().map(|v| v.height as u32).unwrap_or(0);
        let fps = video_md.as_ref().map(|v| v.fps).unwrap_or(0.0);
        if fps > 0.0 {
            util::insert_tag(&mut map, tag!(parsed GroupId::Default, TagId::FrameRate, "Frame rate", f64, |v| format!("{:?}", v), fps, vec![]), &options);
        } else if let Some(v) = md.get("com.blackmagic-design.projectFPS").and_then(|v| v.as_i64()) {
            if v > 0 {
                util::insert_tag(&mut map, tag!(parsed GroupId::Default, TagId::FrameRate, "Frame rate", f64, |v| format!("{:?}", v), v as f64, vec![]), &options);
            }
        }

        // camera_db integration
        if let Some(db_path) = &options.camera_db_path {
            if let Ok(db) = crate::camera_db::CameraDatabase::load(db_path) {
                let raw_name = self.model.as_deref().unwrap_or("");
                if let Some((model_name, model_data)) = db.process_model("BLACKMAGIC", raw_name, &mut map, &options) {
                    self.model = Some(model_name.to_string());
                    let sensor_w = model_data.sw;

                    // Use SensorAreaCaptured width if available (handles crop implicitly),
                    // otherwise compute effective sensor width from max_resolution_w (ref NiYien Tool bmd.cpp)
                    let effective_sensor_w = if let Some(captured_w) = sensor_area_captured_w {
                        captured_w as f64
                    } else if resolution_w > 0 {
                        let max_res = Self::max_resolution_w(model_name);
                        if max_res > 0 && resolution_w < max_res {
                            sensor_w as f64 * resolution_w as f64 / max_res as f64
                        } else {
                            sensor_w as f64
                        }
                    } else {
                        sensor_w as f64
                    };

                    if resolution_w > 0 && effective_sensor_w > 0.0 {
                        let unit_px_fl = resolution_w as f64 / effective_sensor_w;
                        util::insert_tag(&mut map, tag!(parsed GroupId::Lens, TagId::Custom("unit_pixel_focal_length".into()), "Pixel focal length per mm", f64, |v| format!("{:.4}", v), unit_px_fl, vec![]), &options);

                        if let Some(fl) = focal_length {
                            let px_fl = fl as f64 / effective_sensor_w * resolution_w as f64;
                            util::insert_tag(&mut map, tag!(parsed GroupId::Lens, TagId::PixelFocalLength, "Pixel focal length", f32, |v| format!("{:.2}", v), px_fl as f32, vec![]), &options);
                        }
                    }

                    let tags = std::collections::HashMap::new();
                    let _crop = db.process_crop("BLACKMAGIC", model_name, resolution_w, resolution_h, fps, None, &tags, &mut map, &options);

                    if self.frame_readout_time.is_none() {
                        if let Some(rt) = db.process_readout("BLACKMAGIC", model_name, resolution_w, resolution_h, fps, 0.0, sensor_w, &tags, &mut map, &options) {
                            self.frame_readout_time = Some(rt);
                        }
                    }
                }
            }
        }

        // Store all metadata
        if !md.is_empty() {
            util::insert_tag(&mut map, tag!(parsed GroupId::Default, TagId::Metadata, "Metadata", Json, |v| serde_json::to_string(v).unwrap(), serde_json::Value::Object(md), vec![]), &options);
        }

        // Also parse gyro/accel data from metadata tracks (same as BRAW path)
        let mut gyro = Vec::new();
        let mut accl = Vec::new();
        let cancel_flag2 = cancel_flag.clone();
        let _ = progress_cb; // Progress is not easily tracked for metadata-only parse
        util::get_metadata_track_samples(stream, size, false, |info: SampleInfo, data: &[u8], _file_position: u64, _video_md: Option<&VideoMetadata>| {
            if data.len() >= 4+4+3*4 {
                let mut d = Cursor::new(data);
                crate::try_block!({
                    d.seek(SeekFrom::Start(8)).ok()?;
                    if &data[4..8] == b"mogy" {
                        gyro.push(TimeVector3 { t: (info.timestamp_ms - self.frame_readout_time.unwrap_or(0.0) / 2.0) / 1000.0,
                            x: d.read_f32::<LittleEndian>().ok()? as f64,
                            y: d.read_f32::<LittleEndian>().ok()? as f64,
                            z: d.read_f32::<LittleEndian>().ok()? as f64
                        });
                    } else if &data[4..8] == b"moac" {
                        accl.push(TimeVector3 { t: (info.timestamp_ms - self.frame_readout_time.unwrap_or(0.0) / 2.0) / 1000.0,
                            x: -d.read_f32::<LittleEndian>().ok()? as f64,
                            y: -d.read_f32::<LittleEndian>().ok()? as f64,
                            z: -d.read_f32::<LittleEndian>().ok()? as f64
                        });
                    }
                });
            }
            if options.probe_only {
                cancel_flag2.store(true, std::sync::atomic::Ordering::Relaxed);
            }
        }, cancel_flag)?;

        util::insert_tag(&mut map, tag!(parsed GroupId::Accelerometer, TagId::Data, "Accelerometer data", Vec_TimeVector3_f64, |v| format!("{:?}", v), accl, vec![]), &options);
        util::insert_tag(&mut map, tag!(parsed GroupId::Gyroscope,     TagId::Data, "Gyroscope data",     Vec_TimeVector3_f64, |v| format!("{:?}", v), gyro, vec![]), &options);

        util::insert_tag(&mut map, tag!(parsed GroupId::Accelerometer, TagId::Unit, "Accelerometer unit", String, |v| v.to_string(), "m/s²".into(),  Vec::new()), &options);
        util::insert_tag(&mut map, tag!(parsed GroupId::Gyroscope,     TagId::Unit, "Gyroscope unit",     String, |v| v.to_string(), "rad/s".into(), Vec::new()), &options);

        util::insert_tag(&mut map, tag!(parsed GroupId::Accelerometer, TagId::Orientation, "IMU orientation", String, |v| v.to_string(), "yxz".into(), Vec::new()), &options);
        util::insert_tag(&mut map, tag!(parsed GroupId::Gyroscope,     TagId::Orientation, "IMU orientation", String, |v| v.to_string(), "yxz".into(), Vec::new()), &options);

        let mut result = samples;
        result.insert(0, SampleInfo { tag_map: Some(map), ..Default::default() });
        Ok(result)
    }

    pub fn parse_meta<T: Read + Seek>(&mut self, stream: &mut T, size: usize) -> Result<serde_json::Value> {
        let all = read_beginning_and_end(stream, size, 4*1024*1024)?;
        let mut offs = 0;
        let mut meta = None;
        while let Some(pos) = memchr::memmem::find(&all[offs..], b"meta") {
            if all.len() > offs+pos+12 && &all[offs+pos+8..offs+pos+12] == b"hdlr" {
                let size = (&all[offs+pos-4..]).read_u32::<BigEndian>()? as usize;
                meta = Some(&all[offs+pos-4..offs+pos-4+size][8..]);
                break;
            }
            offs += pos + 4;
        }

        if let Some(meta) = meta {
            let mut keys = Vec::new();
            let mut md = serde_json::Map::<String, serde_json::Value>::new();
            Self::iter_boxes(meta, false, |name, d, _| {
                if name == "keys" {
                    Self::iter_boxes(&d[8..], false, |_, d, _| {
                        if let Ok(key) = std::str::from_utf8(&d) {
                            keys.push(key.to_string());
                        }
                        Ok(())
                    })?;
                }
                if name == "ilst" {
                    Self::iter_boxes(&d, true, |_, d, i| {
                        let typ = (&d[..4]).read_u32::<BigEndian>()?;
                        if let Some(key) = keys.get(i).cloned() {
                            // https://developer.apple.com/library/archive/documentation/QuickTime/QTFF/Metadata/Metadata.html#//apple_ref/doc/uid/TP40000939-CH1-SW35
                            let mut d = &d[8..];
                            let v = match typ {
                                1  => serde_json::to_value(std::str::from_utf8(d).unwrap_or(&"")),
                                23 => serde_json::to_value(d.read_f32::<BigEndian>()? as f64),
                                24 => serde_json::to_value(d.read_f64::<BigEndian>()?),
                                65 => serde_json::to_value(d.read_i8()?),
                                66 => serde_json::to_value(d.read_i16::<BigEndian>()?),
                                67 => serde_json::to_value(d.read_i32::<BigEndian>()?),
                                70 |
                                71 => serde_json::to_value([d.read_f32::<BigEndian>()? as f64, d.read_f32::<BigEndian>()? as f64]),
                                74 => serde_json::to_value(d.read_i64::<BigEndian>()?),
                                75 => serde_json::to_value(d.read_u8()?),
                                76 => serde_json::to_value(d.read_u16::<BigEndian>()?),
                                77 => serde_json::to_value(d.read_u32::<BigEndian>()?),
                                78 => serde_json::to_value(d.read_u64::<BigEndian>()?),
                                _ => {
                                    log::debug!("{}({}): {}", key, typ, pretty_hex::pretty_hex(&d[..128.min(d.len() - 1)].to_vec()));
                                    Err(serde_json::Error::io(ErrorKind::InvalidData.into()))
                                }
                            };
                            if let Ok(v) = v {
                                md.insert(key, v);
                            }
                        }
                        Ok(())
                    })?;
                }
                Ok(())
            })?;

            if let Some(sensor_area_height) = md.get("sensor_area_captured").and_then(|v| v.as_array()).and_then(|v| v.get(1)).and_then(|v| v.as_f64()) {
                if let Some(sensor_line_time) = md.get("sensor_line_time").and_then(|v| v.as_f64()) {
                    self.frame_readout_time = Some((sensor_area_height * sensor_line_time) / 1000.0);
                }
            }

            return Ok(serde_json::Value::Object(md));
        }
        Err(ErrorKind::InvalidData.into())
    }

    fn parse_per_frame_meta(data: &[u8]) -> Result<serde_json::Value> {
        if data.len() > 8 && &data[4..8] == b"bmdf" {
            let size = (&data[..8]).read_u32::<BigEndian>()? as usize;
            let meta = &data[8..size];
            let mut md = serde_json::Map::<String, serde_json::Value>::new();
            Self::iter_boxes(meta, false, |name, mut d, _| {
                fn get_str<'a>(d: &'a [u8]) -> serde_json::Result<&'a str> {
                    Ok(std::str::from_utf8(d).map_err(|_| serde_json::Error::io(ErrorKind::InvalidData.into()))?.trim_end_matches('\0'))
                }
                let v = match name {
                    "srte" => (Some("sensor_rate"),          serde_json::to_value([d.read_u32::<BigEndian>()?, d.read_u32::<BigEndian>()?])),
                    "innd" => (Some("internal_nd"),          serde_json::to_value(d.read_f32::<BigEndian>()? as f64)),
                    "agpf" => (Some("analog_gain"),          serde_json::to_value(d.read_f32::<BigEndian>()? as f64)),
                    "expo" => (Some("exposure"),             serde_json::to_value(d.read_f32::<BigEndian>()? as f64)),
                    "isoe" => (Some("iso"),                  serde_json::to_value(d.read_u32::<BigEndian>()?)),
                    "wkel" => (Some("white_balance_kelvin"), serde_json::to_value(d.read_u32::<BigEndian>()?)),
                    "wtin" => (Some("white_balance_tint"),   serde_json::to_value(d.read_u16::<BigEndian>()?)),
                    "asct" => (Some("as_shot_kelvin"),       serde_json::to_value(d.read_u32::<BigEndian>()?)),
                    "asti" => (Some("as_shot_tint"),         serde_json::to_value(d.read_u16::<BigEndian>()?)),
                    "shtv" => (Some("shutter_value"),        serde_json::to_value(get_str(d)?)),
                    "aptr" => (Some("aperture"),             serde_json::to_value(get_str(d)?)),
                    "dsnc" => (Some("distance"),             serde_json::to_value(get_str(d)?)),
                    "fcln" => (Some("focal_length"),         serde_json::to_value(get_str(d)?)),
                    _ => {
                        // log::debug!("{name}: {}", pretty_hex::pretty_hex(&d));
                        (None, Err(serde_json::Error::io(ErrorKind::InvalidData.into())))
                    }
                };
                if let Ok(vv) = v.1 {
                    md.insert(v.0.unwrap_or(name).to_string(), vv);
                }
                Ok(())
            })?;
            return Ok(serde_json::Value::Object(md));
        }
        Err(ErrorKind::InvalidData.into())
    }

    fn iter_boxes<F: FnMut(&str, &[u8], usize) -> Result<()>>(data: &[u8], is_array: bool, mut cb: F) -> Result<()> {
        let mut offs = 0;
        while data.len().saturating_sub(offs) > 8 {
            if let Some(mut size_slice) = data.get(offs..offs+4) {
                let size = size_slice.read_u32::<BigEndian>()? as usize;
                if let Some(d) = &data.get(offs+8..offs+size) {
                    if is_array {
                        let index = (&data[offs+4..offs+8]).read_u32::<BigEndian>()? as usize;
                        let size2 = (&data[offs+8..offs+12]).read_u32::<BigEndian>()? as usize;
                        let d = &data[offs+16..offs+8+size2];
                        if let Ok(name) = std::str::from_utf8(&data[offs+12..offs+16]) {
                            cb(name, d, index - 1)?;
                        }
                    } else {
                        if let Ok(name) = std::str::from_utf8(&data[offs+4..offs+8]) {
                            cb(name, d, 0)?;
                        }
                    }
                }
                offs += size;
            } else {
                break;
            }
        }
        Ok(())
    }
}
