// SPDX-License-Identifier: MIT OR Apache-2.0
// Copyright © 2025 Adrian <adrian.eddy at gmail>

use std::io::*;
use std::sync::{ Arc, atomic::AtomicBool };

use byteorder::{ BigEndian, LittleEndian, ReadBytesExt };
use crate::tags_impl::*;
use crate::*;
use memchr::memmem;
mod cndm_tags;
use cndm_tags::get_tag;
pub mod exif;


#[derive(Default)]
pub struct Canon {
    pub model: Option<String>,
    pub lens: Option<String>,
    is_crm: bool,
    frame_readout_time: Option<f64>,
    mvhd_creation_time: Option<String>,
}
impl Canon {
    pub fn camera_type(&self) -> String {
        "Canon".to_owned()
    }
    pub fn has_accurate_timestamps(&self) -> bool {
        true
    }
    pub fn possible_extensions() -> Vec<&'static str> {
        vec!["mp4", "mov", "mxf", "crm"]
    }
    pub fn frame_readout_time(&self) -> Option<f64> {
        self.frame_readout_time
    }
    pub fn normalize_imu_orientation(v: String) -> String {
        v
    }

    pub fn detect<P: AsRef<std::path::Path>>(buffer: &[u8], _filepath: P, _options: &crate::InputOptions) -> Option<Self> {
        if memmem::find(buffer, b"Canon EOS").is_some() {
            return Some(Self {
                model: None,
                lens: None,
                is_crm: memmem::find(buffer, b"ftypcrx").is_some(),
                frame_readout_time: None,
                mvhd_creation_time: util::extract_mvhd_creation_time(buffer),
            });
        }
        // MXF: Application Supplier Name stores "CANON" as UTF-16LE
        if buffer.len() > 4 && buffer[..4] == [0x06, 0x0E, 0x2B, 0x34]
            && memmem::find(buffer, b"C\x00A\x00N\x00O\x00N").is_some() {
            return Some(Self {
                model: None,
                lens: None,
                is_crm: false,
                frame_readout_time: None,
                mvhd_creation_time: None, // MXF has no mvhd
            });
        }
        None
    }

    pub fn parse<T: Read + Seek, F: Fn(f64)>(&mut self, stream: &mut T, size: usize, progress_cb: F, cancel_flag: Arc<AtomicBool>, options: crate::InputOptions) -> Result<Vec<SampleInfo>> {
        let mut header = [0u8; 4];
        stream.read_exact(&mut header)?;
        stream.seek(SeekFrom::Start(0))?;

        let is_mxf = header == [0x06, 0x0E, 0x2B, 0x34];
        let mut mxf_creation_time: Option<String> = None;
        let mut mxf_creation_subsec: Option<String> = None;
        let mut samples = if is_mxf { // MXF header
            let (s, ct, cs) = crate::sony::mxf::parse(stream, size, progress_cb, cancel_flag, None, &options, parse_tags)?;
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

                if data.len() > 8 {
                    if let Err(e) = || -> Result<()> {
                        let mut slice = Cursor::new(&data);
                        if self.is_crm {
                            while let Ok(length) = slice.read_u32::<LittleEndian>() {
                                let length = (length - 8) as usize;
                                let metadata_id = slice.read_u32::<LittleEndian>()?;
                                if slice.position() as usize + length > data.len() {
                                    log::error!("Invalid CRM data!. Length: {length}, data len: {}, position: {}", data.len(), slice.position());
                                    break;
                                }
                                let data_inner = &data[slice.position() as usize..slice.position() as usize + length];
                                slice.seek_relative(length as _)?;
                                let mut d = Cursor::new(&data_inner);
                                match metadata_id {
                                    0x0000000D => { // AcquisitionMetadataPack
                                        let _version  = d.read_u16::<LittleEndian>()?;
                                        let _reserved = d.read_u16::<LittleEndian>()?;
                                        if let Ok(map) = parse_metadata(&mut d, length as usize, &options) {
                                            info.tag_map = Some(map);
                                            samples.push(info.clone());
                                            if options.probe_only {
                                                cancel_flag2.store(true, std::sync::atomic::Ordering::Relaxed);
                                            }
                                        }
                                    }
                                    _ => {
                                        // println!("Unknown CRM data: {metadata_id}, {}", pretty_hex::pretty_hex(&data_inner));
                                    }
                                }
                            }
                        } else {
                            while let Ok(id) = slice.read_u32::<LittleEndian>() {
                                let length = slice.read_u32::<LittleEndian>()? as usize;
                                if slice.position() as usize + length > data.len() {
                                    log::error!("Invalid cndm data!. Length: {length}, data len: {}, position: {}", data.len(), slice.position());
                                    break;
                                }
                                let data_inner = &data[slice.position() as usize..slice.position() as usize + length];
                                slice.seek_relative(length as _)?;
                                let mut d = Cursor::new(&data_inner);
                                match id {
                                    1 => { // Timecode
                                        let _reserved             = d.read_u8()?;
                                        let _drop_frame           = d.read_u8()?;
                                        let _number_of_frames     = d.read_u16::<LittleEndian>()?;
                                        let _timecode_sample_data = d.read_u32::<LittleEndian>()?;
                                        let _user_bit             = d.read_u32::<LittleEndian>()?;
                                    }
                                    2 => { // Acquisition metadata
                                        if let Ok(map) = parse_metadata(&mut d, length as usize, &options) {
                                            info.tag_map = Some(map);
                                            samples.push(info.clone());
                                            if options.probe_only {
                                                cancel_flag2.store(true, std::sync::atomic::Ordering::Relaxed);
                                            }
                                        }
                                    }
                                    _ => {
                                        log::warn!("Unknown cndm data: {id}, {}", pretty_hex::pretty_hex(&data_inner));
                                    }
                                }
                            }
                        }
                        Ok(())
                    }() {
                        log::warn!("Failed to parse Canon metadata: {e:?}");
                    }
                }
            }, cancel_flag)?;
            samples
        };

        // Canon MXF second pass: extract model name and per-frame metadata from Canvas Container
        if is_mxf {
            stream.seek(SeekFrom::Start(0))?;
            if let Ok((mxf_model, canvas_frames)) = parse_mxf_canon_extra(stream, size) {
                let has_st436_samples = !samples.is_empty();
                let model_clean = mxf_model.as_ref().map(|m| m.strip_prefix("Canon ").unwrap_or(m).to_string());

                if !canvas_frames.is_empty() {
                    // Compute MXF-derived crop from 35mm equiv / actual focal length (first valid frame)
                    let mxf_crop: Option<f64> = canvas_frames.iter().find_map(|f| {
                        match (f.focal_length, f.fl_35mm_equiv) {
                            (Some(fl), Some(fl35)) if fl > 0.0 => Some(fl35 as f64 / fl as f64),
                            _ => None,
                        }
                    });

                    if has_st436_samples {
                        // Merge Canvas data into existing ST 436 samples
                        for (i, cf) in canvas_frames.iter().enumerate() {
                            if let Some(sample) = samples.get_mut(i) {
                                if let Some(ref mut map) = sample.tag_map {
                                    write_canvas_tags(map, cf, mxf_crop, &options);
                                    if i == 0 {
                                        if let Some(ref mc) = model_clean {
                                            util::insert_tag(map, tag!(parsed GroupId::Default, TagId::Name, "Camera model", String, |v| v.to_string(), mc.clone(), Vec::new()), &options);
                                        }
                                    }
                                }
                            }
                        }
                    } else {
                        // No ST 436 samples — create new samples from Canvas frames
                        let frame_rate = samples.first()
                            .and_then(|s| s.tag_map.as_ref())
                            .and_then(|m| m.get(&GroupId::Default))
                            .and_then(|g| g.get(&TagId::FrameRate))
                            .and_then(|t| match &t.value { TagValue::f64(v) => Some(*v.get()), _ => None })
                            .unwrap_or(25.0);
                        samples.clear();
                        for (i, cf) in canvas_frames.iter().enumerate() {
                            let mut map = GroupedTagMap::new();
                            if i == 0 {
                                if let Some(ref mc) = model_clean {
                                    util::insert_tag(&mut map, tag!(parsed GroupId::Default, TagId::Name, "Camera model", String, |v| v.to_string(), mc.clone(), Vec::new()), &options);
                                }
                            }
                            write_canvas_tags(&mut map, cf, mxf_crop, &options);
                            let duration_ms = 1000.0 / frame_rate;
                            samples.push(SampleInfo {
                                sample_index: i as u64,
                                duration_ms,
                                timestamp_ms: i as f64 * duration_ms,
                                tag_map: Some(map),
                                ..Default::default()
                            });
                        }
                    }
                } else if let Some(ref mc) = model_clean {
                    // No Canvas frames, just write model name
                    if has_st436_samples {
                        if let Some(ref mut map) = samples[0].tag_map {
                            util::insert_tag(map, tag!(parsed GroupId::Default, TagId::Name, "Camera model", String, |v| v.to_string(), mc.clone(), Vec::new()), &options);
                        }
                    } else {
                        let mut map = GroupedTagMap::new();
                        util::insert_tag(&mut map, tag!(parsed GroupId::Default, TagId::Name, "Camera model", String, |v| v.to_string(), mc.clone(), Vec::new()), &options);
                        samples.push(SampleInfo { tag_map: Some(map), ..Default::default() });
                    }
                }
            }
        }

        // Get video track metadata (resolution, fps)
        stream.seek(SeekFrom::Start(0))?;
        let video_md = util::get_video_metadata(stream, size).ok();

        // Parse Canon UUID EXIF for canon_fine, canon_crop, and fallback metadata (MP4 only)
        stream.seek(SeekFrom::Start(0))?;
        let exif_data = if is_mxf { Err(Error::new(ErrorKind::NotFound, "MXF has no UUID EXIF")) } else { exif::parse_canon_uuid_exif(stream, size) };

        self.process_map(&mut samples, &options, exif_data.ok(), video_md.as_ref(), mxf_creation_time.as_deref(), mxf_creation_subsec.as_deref());

        Ok(samples)
    }

    fn process_map(&mut self, samples: &mut Vec<SampleInfo>, options: &crate::InputOptions, exif_data: Option<exif::CanonExifData>, video_md: Option<&VideoMetadata>, mxf_creation_time: Option<&str>, mxf_creation_subsec: Option<&str>) {
        let imu_orientation = "yxZ";
        for sample in samples.iter_mut() {
            if let Some(ref mut map) = sample.tag_map {
                if map.contains_key(&GroupId::Accelerometer) {
                    util::insert_tag(map, tag!(parsed GroupId::Accelerometer, TagId::Unit, "Accelerometer unit", String, |v| v.to_string(), "g".into(), Vec::new()), options);
                    util::insert_tag(map, tag!(parsed GroupId::Accelerometer, TagId::Orientation, "IMU orientation", String, |v| v.to_string(), imu_orientation.into(), Vec::new()), &options);
                }
                if map.contains_key(&GroupId::Gyroscope) {
                    util::insert_tag(map, tag!(parsed GroupId::Gyroscope,     TagId::Orientation, "IMU orientation", String, |v| v.to_string(), imu_orientation.into(), Vec::new()), &options);
                }
                if let Some(x) = map.get(&GroupId::Default).and_then(|m| m.get(&TagId::Name)) {
                    let v = x.value.to_string();
                    self.model = Some(v.strip_prefix("Canon ").unwrap_or(&v).to_string());
                }

                if let Some(imager) = map.get_mut(&GroupId::Imager) {
                    if let Some(v) = imager.get_t(TagId::FrameReadoutTime) as Option<&f64> {
                        self.frame_readout_time = Some(*v);
                    }
                }
            }
        }

        // Supplement from UUID EXIF when CNDM data is missing
        if let Some(exif) = &exif_data {
            // If no samples (no CNDM track) or first sample has no tag_map, create one from EXIF
            let has_tag_map = samples.first().and_then(|s| s.tag_map.as_ref()).is_some();
            if !has_tag_map {
                let mut map = GroupedTagMap::new();

                if let Some(model) = &exif.model {
                    let model_clean = model.strip_prefix("Canon ").unwrap_or(model).to_string();
                    util::insert_tag(&mut map, tag!(parsed GroupId::Default, TagId::Name, "Camera model", String, |v| v.to_string(), model_clean.clone(), Vec::new()), options);
                    self.model = Some(model_clean);
                }
                if let Some(fl) = exif.focal_length {
                    util::insert_tag(&mut map, tag!(parsed GroupId::Lens, TagId::FocalLength, "Focal length", f32, |v| format!("{:.2} mm", v), fl as f32, Vec::new()), options);
                }
                if let Some(lens) = &exif.lens_model {
                    util::insert_tag(&mut map, tag!(parsed GroupId::Lens, TagId::DisplayName, "Lens model", String, |v| v.to_string(), lens.clone(), Vec::new()), options);
                    self.lens = Some(lens.clone());
                }

                if samples.is_empty() {
                    samples.push(SampleInfo::default());
                }
                samples[0].tag_map = Some(map);
            }

            // Always write canon_fine and canon_crop to first sample's tag_map
            if let Some(ref mut map) = samples.first_mut().and_then(|s| s.tag_map.as_mut()) {
                util::insert_tag(map, tag!(parsed GroupId::Default, TagId::Custom("canon_fine".into()), "Canon Fine mode", bool, |v| v.to_string(), exif.canon_fine, Vec::new()), options);
                util::insert_tag(map, tag!(parsed GroupId::Default, TagId::Custom("canon_crop".into()), "Canon Crop mode", bool, |v| v.to_string(), exif.canon_crop, Vec::new()), options);
            }
        }

        // Write video track resolution to tag_map
        if let Some(vmd) = video_md {
            if let Some(ref mut map) = samples.first_mut().and_then(|s| s.tag_map.as_mut()) {
                // Always write video output resolution as custom tags (for crop calculation and display)
                util::insert_tag(map, tag!(parsed GroupId::Default, TagId::Custom("video_width".into()), "Video output width", u32, |v| format!("{} px", v), vmd.width as u32, Vec::new()), options);
                util::insert_tag(map, tag!(parsed GroupId::Default, TagId::Custom("video_height".into()), "Video output height", u32, |v| format!("{} px", v), vmd.height as u32, Vec::new()), options);
                // If no CNDM Imager group, also write as PixelWidth/PixelHeight
                if !map.contains_key(&GroupId::Imager) {
                    util::insert_tag(map, tag!(parsed GroupId::Imager, TagId::PixelWidth, "Video width", u32, |v| format!("{} px", v), vmd.width as u32, Vec::new()), options);
                    util::insert_tag(map, tag!(parsed GroupId::Imager, TagId::PixelHeight, "Video height", u32, |v| format!("{} px", v), vmd.height as u32, Vec::new()), options);
                }
            }
        }

        // Extract CNDM creation date from frame 0 (date_str, tz_str, subsec_str)
        let cndm_creation_date: Option<(String, Option<String>, Option<String>)> = samples.first()
            .and_then(|s| s.tag_map.as_ref())
            .and_then(|map| {
                // Get TimestampMs (UTC milliseconds)
                let ts_ms = map.get(&GroupId::Default)
                    .and_then(|m| m.get_t(TagId::TimestampMs) as Option<&f64>)
                    .copied()?;
                // Get timezone string if available
                let tz_str = map.get(&GroupId::Default)
                    .and_then(|m| m.get_t(TagId::Custom("CndmTimezone".into())) as Option<&String>)
                    .cloned();
                // Extract millisecond part as subsec string
                let cndm_subsec = format!("{:03}", (ts_ms as i64 % 1000).unsigned_abs());
                // Convert UTC millis to datetime
                let utc_dt = chrono::DateTime::from_timestamp_millis(ts_ms as i64)?;
                if let Some(ref tz) = tz_str {
                    // Parse timezone offset and compute local time
                    let tz_trimmed = tz.trim();
                    let (sign, rest) = if tz_trimmed.starts_with('-') {
                        (-1i64, &tz_trimmed[1..])
                    } else if tz_trimmed.starts_with('+') {
                        (1i64, &tz_trimmed[1..])
                    } else {
                        (1i64, tz_trimmed)
                    };
                    let parts: Vec<&str> = rest.split(':').collect();
                    if parts.len() == 2 {
                        if let (Ok(hours), Ok(minutes)) = (parts[0].parse::<i64>(), parts[1].parse::<i64>()) {
                            let offset_secs = sign * (hours * 3600 + minutes * 60);
                            let local_dt = utc_dt + chrono::Duration::seconds(offset_secs);
                            let local_str = local_dt.format("%Y:%m:%d %H:%M:%S").to_string();
                            return Some((local_str, Some(tz.clone()), Some(cndm_subsec)));
                        }
                    }
                }
                // No valid timezone: output UTC time
                let utc_str = utc_dt.format("%Y:%m:%d %H:%M:%S").to_string();
                Some((utc_str, None, Some(cndm_subsec)))
            });

        // Get common values for crop/readout
        let resolution_w_video = video_md.map(|v| v.width as u32).unwrap_or(0);
        let resolution_h_video = video_md.map(|v| v.height as u32).unwrap_or(0);
        let fps = video_md.map(|v| v.fps).unwrap_or(0.0);
        let canon_fine = exif_data.as_ref().map_or(false, |e| e.canon_fine);
        let canon_crop_flag = exif_data.as_ref().map_or(false, |e| e.canon_crop);

        // Try JSON database first
        if let Some(db_path) = &options.camera_db_path {
            if let Ok(db) = crate::camera_db::CameraDatabase::load(db_path) {
                if let Some(model_name) = &self.model.clone() {
                    // First pass: extract model info, crop, sensor_width from first sample
                    let db_result: Option<(f32, f64, Option<f64>, String)> = {
                        if let Some(ref mut map) = samples.first_mut().and_then(|s| s.tag_map.as_mut()) {
                            if let Some((matched_name, model_data)) = db.process_model("CANON", model_name, map, options) {
                                self.model = Some(matched_name.to_string());
                                let sensor_w = model_data.sw;

                                let lens_model = map.get(&GroupId::Lens)
                                    .and_then(|m| m.get_t(TagId::DisplayName) as Option<&String>)
                                    .cloned()
                                    .unwrap_or_default();

                                // View angle: determined by JSON crop rules via tag conditions
                                let view_angle: Option<&str> = None;

                                // Build tags for canon_fine / canon_crop
                                let mut tags = std::collections::HashMap::new();
                                if canon_fine {
                                    tags.insert("canon_fine".to_string(), serde_json::Value::Bool(true));
                                }
                                if canon_crop_flag {
                                    tags.insert("canon_crop".to_string(), serde_json::Value::Bool(true));
                                }

                                // Use MXF-derived crop if available, otherwise fall back to DB crop
                                let has_mxf_crop = map.get(&GroupId::Default)
                                    .map_or(false, |m| m.contains_key(&TagId::Custom("crop_factor".into())));
                                let effective_crop = if has_mxf_crop {
                                    // MXF crop already written by Canvas parsing
                                    map.get(&GroupId::Default)
                                        .and_then(|m| m.get_t(TagId::Custom("crop_factor".into())) as Option<&f64>)
                                        .copied()
                                        .unwrap_or(1.0)
                                } else {
                                    let c = db.process_crop("CANON", matched_name, resolution_w_video, resolution_h_video, fps, view_angle, &tags, map, options);
                                    // Canon always writes crop_factor (even default 1.0)
                                    if db.match_crop("CANON", matched_name, resolution_w_video, resolution_h_video, fps, view_angle, &tags).is_none() {
                                        util::insert_tag(map, tag!(parsed GroupId::Default, TagId::Custom("crop_factor".into()), "Crop factor", f64, |v| format!("{:.4}", v), 1.0, Vec::new()), options);
                                    }
                                    c
                                };

                                // Readout: use CNDM sensor pixels for resolution if available
                                if self.frame_readout_time.is_none() {
                                    let (readout_w, readout_h) = {
                                        let cndm_w = map.get(&GroupId::Imager).and_then(|m| m.get_t(TagId::PixelWidth) as Option<&u32>).copied();
                                        let cndm_h = map.get(&GroupId::Imager).and_then(|m| m.get_t(TagId::PixelHeight) as Option<&u32>).copied();
                                        match (cndm_w, cndm_h) {
                                            (Some(w), Some(h)) => (w, h),
                                            _ => (resolution_w_video, resolution_h_video),
                                        }
                                    };
                                    if let Some(rt) = db.process_readout("CANON", matched_name, readout_w, readout_h, fps, 0.0, sensor_w, &tags, map, options) {
                                        self.frame_readout_time = Some(rt);
                                    }
                                }

                                // Compute unit_pixel_focal_length
                                let unit_px_fl = if resolution_w_video > 0 && sensor_w > 0.0 {
                                    Some(resolution_w_video as f64 * effective_crop / sensor_w as f64)
                                } else {
                                    None
                                };

                                // Write unit_pixel_focal_length to first sample (process_crop already wrote crop_factor)
                                if let Some(upfl) = unit_px_fl {
                                    util::insert_tag(map, tag!(parsed GroupId::Lens, TagId::Custom("unit_pixel_focal_length".into()), "Pixel focal length per mm", f64, |v| format!("{:.4}", v), upfl, Vec::new()), options);
                                }

                                // Write PixelFocalLength for first sample based on its own FocalLength or user-provided
                                if let Some(upfl) = unit_px_fl {
                                    let fl_from_file = map.get(&GroupId::Lens)
                                        .and_then(|m| m.get_t(TagId::FocalLength) as Option<&f32>)
                                        .map(|v| *v as f64);
                                    let fl = fl_from_file.or(options.user_focal_length);
                                    if let Some(fl) = fl {
                                        if fl > 0.0 {
                                            let px_fl = (fl * upfl) as f32;
                                            util::insert_tag(map, tag!(parsed GroupId::Lens, TagId::PixelFocalLength, "Pixel focal length", f32, |v| format!("{:.2}", v), px_fl, Vec::new()), options);
                                        }
                                    }
                                }

                                Some((sensor_w, effective_crop, unit_px_fl, lens_model))
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    }; // first-sample borrow released here

                    // Second pass: write crop_factor, unit_pixel_focal_length, and PixelFocalLength to remaining samples
                    if let Some((_sensor_w, effective_crop, unit_px_fl, _lens_model)) = db_result {
                        for sample in samples.iter_mut().skip(1) {
                            if let Some(ref mut smap) = sample.tag_map {
                                // Write crop_factor (same for all frames)
                                if !smap.get(&GroupId::Default).map_or(false, |m| m.contains_key(&TagId::Custom("crop_factor".into()))) {
                                    util::insert_tag(smap, tag!(parsed GroupId::Default, TagId::Custom("crop_factor".into()), "Crop factor", f64, |v| format!("{:.4}", v), effective_crop, Vec::new()), options);
                                }

                                // Write unit_pixel_focal_length (same for all frames)
                                if let Some(upfl) = unit_px_fl {
                                    if !smap.get(&GroupId::Lens).map_or(false, |m| m.contains_key(&TagId::Custom("unit_pixel_focal_length".into()))) {
                                        util::insert_tag(smap, tag!(parsed GroupId::Lens, TagId::Custom("unit_pixel_focal_length".into()), "Pixel focal length per mm", f64, |v| format!("{:.4}", v), upfl, Vec::new()), options);
                                    }
                                }

                                // Write PixelFocalLength based on THIS frame's FocalLength or user-provided
                                if let Some(upfl) = unit_px_fl {
                                    let fl_from_file = smap.get(&GroupId::Lens)
                                        .and_then(|m| m.get_t(TagId::FocalLength) as Option<&f32>)
                                        .map(|v| *v as f64);
                                    let fl = fl_from_file.or(options.user_focal_length);
                                    if let Some(fl) = fl {
                                        if fl > 0.0 {
                                            let px_fl = (fl * upfl) as f32;
                                            util::insert_tag(smap, tag!(parsed GroupId::Lens, TagId::PixelFocalLength, "Pixel focal length", f32, |v| format!("{:.2}", v), px_fl, Vec::new()), options);
                                        }
                                    }
                                }
                            }
                        }

                        // Write creation date to frame 0
                        if let Some(first_sample) = samples.first_mut() {
                            let map = first_sample.tag_map.get_or_insert_with(GroupedTagMap::new);
                            if let Some((ref date_str, ref tz_str, ref cndm_subsec)) = cndm_creation_date {
                                util::write_creation_date_tags(map, date_str, tz_str.as_deref(), cndm_subsec.as_deref(), options);
                            } else if let Some(mxf_time) = mxf_creation_time {
                                util::write_creation_date_tags(map, mxf_time, None, mxf_creation_subsec.or(Some("500")), options);
                            } else if let Some(ref exif) = exif_data {
                                if let Some(ref dt) = exif.datetime_original {
                                    util::write_creation_date_tags(map, dt, exif.offset_time_original.as_deref(), exif.subsec_time_original.as_deref().or(Some("500")), options);
                                } else if let Some(ref mvhd_time) = self.mvhd_creation_time {
                                    util::write_creation_date_tags(map, mvhd_time, None, Some("500"), options);
                                }
                            } else if let Some(ref mvhd_time) = self.mvhd_creation_time {
                                util::write_creation_date_tags(map, mvhd_time, None, Some("500"), options);
                            }

                            // Frame rates
                            if fps > 0.0 {
                                util::insert_tag(map, tag!(parsed GroupId::Default, TagId::FrameRate, "Frame rate", f64, |v| format!("{:.3} fps", v), fps, Vec::new()), options);
                                util::insert_tag(map, tag!(parsed GroupId::Default, TagId::RecordFrameRate, "Record frame rate", f64, |v| format!("{:.3} fps", v), fps, Vec::new()), options);
                            }

                            // Image stabilization (default false; CNDM models output per-frame from 0xe21b)
                            if !map.get(&GroupId::Default).map_or(false, |m| m.contains_key(&TagId::ImageStabilizer)) {
                                util::insert_tag(map, tag!(parsed GroupId::Default, TagId::ImageStabilizer, "Image stabilization", bool, |v| if *v { "On" } else { "Off" }.into(), false, Vec::new()), options);
                            }
                        }

                        return; // JSON path complete
                    }
                }
            }
        }

        // Fallback creation date (no JSON db match)
        if let Some(first_sample) = samples.first_mut() {
            let map = first_sample.tag_map.get_or_insert_with(GroupedTagMap::new);
            if let Some((ref date_str, ref tz_str, ref cndm_subsec)) = cndm_creation_date {
                util::write_creation_date_tags(map, date_str, tz_str.as_deref(), cndm_subsec.as_deref(), options);
            } else if let Some(mxf_time) = mxf_creation_time {
                util::write_creation_date_tags(map, mxf_time, None, mxf_creation_subsec.or(Some("500")), options);
            } else if let Some(ref exif) = exif_data {
                if let Some(ref dt) = exif.datetime_original {
                    // Use explicit OffsetTimeOriginal if available, otherwise infer from mvhd UTC
                    let tz = exif.offset_time_original.clone()
                        .or_else(|| self.mvhd_creation_time.as_ref().and_then(|mvhd| util::infer_timezone(dt, mvhd)));
                    util::write_creation_date_tags(map, dt, tz.as_deref(), exif.subsec_time_original.as_deref().or(Some("500")), options);
                } else if let Some(ref mvhd_time) = self.mvhd_creation_time {
                    util::write_creation_date_tags(map, mvhd_time, None, Some("500"), options);
                }
            } else if let Some(ref mvhd_time) = self.mvhd_creation_time {
                util::write_creation_date_tags(map, mvhd_time, None, Some("500"), options);
            }
        }
    }
}

pub fn parse_metadata<T: Read + Seek>(stream: &mut T, _size: usize, options: &crate::InputOptions) -> Result<GroupedTagMap> {
    let mut map = GroupedTagMap::new();
    let mut id = [0u8; 16];
    while let Ok(_) = stream.read_exact(&mut id) {
        if &id[0..4] != &[0x06, 0x0e, 0x2b, 0x34] {
            log::warn!("Unknown ID {} at 0x{:08x}", util::to_hex(&id), stream.stream_position()? - 16);
            while let Ok(byte) = stream.read_u8() {
                if byte == 0x06 {
                    let mut id2 = [0u8; 3];
                    stream.read_exact(&mut id2)?;
                    if id2 == [0x0e, 0x2b, 0x34] {
                        stream.seek(SeekFrom::Current(-4))?;
                        break;
                    }
                    stream.seek(SeekFrom::Current(-3))?;
                }
            }
            continue;
        }

        let length = read_ber(stream)?;

        // println!("{}: {}", util::to_hex(&id), length);

        if id == hex_literal::hex!("060E2B34 02530101 0C020101 01010000") || // Lens Unit Metadata
           id == hex_literal::hex!("060E2B34 02530101 0C020101 02010000") || // Camera Unit Metadata
           id == hex_literal::hex!("060E2B34 02530101 0C020101 7F010000") || // User Defined Acquisition Metadata
           id == hex_literal::hex!("060E2B34 0401010D 0E150004 01000000") || // Canon Lens Metadata
           id == hex_literal::hex!("060E2B34 0401010D 0E150004 02000000") || // Canon Camera Metadata
           id == hex_literal::hex!("060E2B34 0401010D 0E150004 04000000") {  // Cooke /i Lens Metadata
            let mut data = vec![0; length];
            stream.read_exact(&mut data)?;

            parse_tags(&data, options, &mut map)?;
        } else {
            log::warn!("Unknown id: {}, length: {}", util::to_hex(&id), length);
            stream.seek(SeekFrom::Current(length as i64))?;
        }
    }
    Ok(map)
}

fn read_ber<T: Read + Seek>(stream: &mut T) -> Result<usize> {
    let mut size = stream.read_u8()? as usize;

    if size & 0x80 != 0 {
        let bytes = size & 0x7f;
        assert!(bytes <= 8);
        size = 0;
        for _ in 0..bytes {
            size = size << 8 | (stream.read_u8()? as usize);
        }
    }
    Ok(size)
}


/// Write per-frame Canvas metadata tags to a sample's tag_map.
fn write_canvas_tags(map: &mut GroupedTagMap, cf: &MxfCanvasFrame, mxf_crop: Option<f64>, options: &crate::InputOptions) {
    if let Some(fl) = cf.focal_length {
        util::insert_tag(map, tag!(parsed GroupId::Lens, TagId::FocalLength, "Focal length", f32, |v| format!("{:.2} mm", v), fl, Vec::new()), options);
    }
    if let Some(fl35) = cf.fl_35mm_equiv {
        util::insert_tag(map, tag!(parsed GroupId::Lens, TagId::Custom("FocalLength35mm".into()), "35mm equivalent focal length", f32, |v| format!("{:.2} mm", v), fl35, Vec::new()), options);
    }
    if let Some(iso) = cf.iso {
        util::insert_tag(map, tag!(parsed GroupId::Exposure, TagId::Custom("ISO".into()), "ISO sensitivity", u32, |v| format!("{}", v), iso, Vec::new()), options);
    }
    if let Some((num, den)) = cf.shutter {
        let shutter_s = num as f64 / den as f64;
        util::insert_tag(map, tag!(parsed GroupId::Exposure, TagId::ExposureTime, "Shutter speed", f64, |v| format!("1/{}s", (1.0 / v).round() as u32), shutter_s, Vec::new()), options);
    }
    if let Some(tstop) = cf.aperture {
        util::insert_tag(map, tag!(parsed GroupId::Lens, TagId::Custom("TStop".into()), "T-stop", f32, |v| format!("T{:.1}", v), tstop, Vec::new()), options);
    }
    // Write MXF-derived crop factor (from 35mm equiv / actual focal length)
    if let Some(crop) = mxf_crop {
        util::insert_tag(map, tag!(parsed GroupId::Default, TagId::Custom("crop_factor".into()), "Crop factor", f64, |v| format!("{:.4}", v), crop, Vec::new()), options);
    }
}

/// Per-frame data extracted from Canon MXF Canvas Container.
#[derive(Default, Clone)]
struct MxfCanvasFrame {
    focal_length: Option<f32>,       // keyId 0x10: actual focal length (value/10 mm)
    fl_35mm_equiv: Option<f32>,      // keyId 0x11: 35mm equivalent focal length (value/10 mm)
    iso: Option<u32>,                // keyId 0x09: ISO sensitivity
    shutter: Option<(u16, u16)>,     // keyId 0x03: shutter speed (numerator, denominator)
    aperture: Option<f32>,           // keyId 0x05: T-stop (hi_u16/10)
}

/// Canon MXF second pass: extract model name from Identification Set and per-frame metadata from Canvas Container.
fn parse_mxf_canon_extra<T: Read + Seek>(stream: &mut T, size: usize) -> Result<(Option<String>, Vec<MxfCanvasFrame>)> {
    let mut stream = std::io::BufReader::with_capacity(128 * 1024, stream);
    let mut model_name: Option<String> = None;
    let mut frames: Vec<MxfCanvasFrame> = Vec::new();

    let mut id = [0u8; 16];
    while let Ok(_) = stream.read_exact(&mut id) {
        if &id[0..4] != &[0x06, 0x0e, 0x2b, 0x34] {
            // Resync to next KLV key
            while let Ok(byte) = stream.read_u8() {
                if byte == 0x06 {
                    let mut id2 = [0u8; 3];
                    stream.read_exact(&mut id2)?;
                    if id2 == [0x0e, 0x2b, 0x34] {
                        stream.seek(SeekFrom::Current(-4))?;
                        break;
                    }
                    stream.seek(SeekFrom::Current(-3))?;
                }
            }
            continue;
        }

        let length = read_ber(&mut stream)?;

        if size > 0 {
            let pos = stream.stream_position().unwrap_or(0);
            if pos >= size as u64 { break; }
        }

        // Identification Set: extract Application Name (model)
        if id == hex_literal::hex!("060e2b34 02530101 0d010101 01013000") {
            let mut data = vec![0u8; length];
            stream.read_exact(&mut data)?;
            let mut cursor = Cursor::new(data.as_slice());
            while (cursor.position() as usize) + 4 <= data.len() {
                let tag = cursor.read_u16::<BigEndian>()?;
                let len = cursor.read_u16::<BigEndian>()? as usize;
                let pos = cursor.position() as usize;
                if pos + len > data.len() { break; }
                if tag == 0x3C02 { // Application Name (UTF-16)
                    if let Some(name) = decode_mxf_utf16(&data[pos..pos + len]) {
                        if !name.is_empty() {
                            model_name = Some(name);
                        }
                    }
                }
                cursor.seek(SeekFrom::Current(len as i64))?;
            }
            continue;
        }

        // Canvas Container: extract per-frame metadata
        if id == hex_literal::hex!("060e2b34 02430101 0d010301 04010203") {
            let mut data = vec![0u8; length];
            stream.read_exact(&mut data)?;

            let mut frame = MxfCanvasFrame::default();
            let fl_key = hex_literal::hex!("060e2b34 0101010d 0e150001 00010000");
            if let Some(fl_pos) = memmem::find(&data, &fl_key) {
                if let Some(fl_data) = read_sub_klv_data(&data, fl_pos) {
                    parse_canvas_keys(fl_data, &mut frame);
                }
            }
            frames.push(frame);
            continue;
        }

        // Skip other KLVs
        stream.seek(SeekFrom::Current(length as i64))?;
    }

    Ok((model_name, frames))
}

/// Read the data portion of a sub-KLV at the given key offset within a buffer.
fn read_sub_klv_data(data: &[u8], key_pos: usize) -> Option<&[u8]> {
    let start = key_pos + 16;
    if start >= data.len() { return None; }
    let fb = data[start];
    let (len, ber_size) = if fb & 0x80 != 0 {
        let nb = (fb & 0x7f) as usize;
        if start + 1 + nb > data.len() { return None; }
        let mut l = 0usize;
        for i in 0..nb { l = l << 8 | data[start + 1 + i] as usize; }
        (l, 1 + nb)
    } else {
        (fb as usize, 1)
    };
    let data_start = start + ber_size;
    if data_start + len > data.len() { return None; }
    Some(&data[data_start..data_start + len])
}

/// Parse key-value pairs from Canon MXF Canvas Focal Length Element.
fn parse_canvas_keys(fl_data: &[u8], frame: &mut MxfCanvasFrame) {
    if fl_data.is_empty() { return; }
    let num_keys = fl_data[0] as usize;
    let mut off = 1;
    for _ in 0..num_keys {
        if off + 5 > fl_data.len() { break; }
        let key_id = fl_data[off];
        let value = u32::from_be_bytes([fl_data[off + 1], fl_data[off + 2], fl_data[off + 3], fl_data[off + 4]]);
        off += 5;

        if value == 0xFFFFFFFF { continue; } // invalid marker

        match key_id {
            0x03 => { // Shutter speed: (u16 numerator, u16 denominator)
                let num = (value >> 16) as u16;
                let den = (value & 0xFFFF) as u16;
                if num > 0 && den > 0 {
                    frame.shutter = Some((num, den));
                }
            }
            0x05 => { // Aperture T-stop: hi u16 / 10
                let hi = (value >> 16) as u16;
                if hi > 0 && hi < 0xFFFF {
                    frame.aperture = Some(hi as f32 / 10.0);
                }
            }
            0x09 => { // ISO
                if value > 0 && value < 0xFFFFFF {
                    frame.iso = Some(value);
                }
            }
            0x10 => { // Actual focal length: value / 10.0 mm
                let fl = value as f32 / 10.0;
                if fl >= 5.0 && fl <= 10000.0 {
                    frame.focal_length = Some(fl);
                }
            }
            0x11 => { // 35mm equivalent focal length: value / 10.0 mm
                let fl = value as f32 / 10.0;
                if fl >= 5.0 && fl <= 10000.0 {
                    frame.fl_35mm_equiv = Some(fl);
                }
            }
            _ => {}
        }
    }
}

/// Decode UTF-16 (big-endian or little-endian) string from MXF local tag value, trimming null padding.
fn decode_mxf_utf16(data: &[u8]) -> Option<String> {
    if data.len() < 2 { return None; }
    // MXF typically uses UTF-16BE, but Canon uses UTF-16LE in Identification Set
    // Detect by checking if odd bytes are mostly zero (LE) or even bytes are mostly zero (BE)
    let le_zeros = data.iter().skip(1).step_by(2).filter(|&&b| b == 0).count();
    let be_zeros = data.iter().step_by(2).filter(|&&b| b == 0).count();
    let chars: Vec<u16> = if le_zeros > be_zeros {
        // UTF-16LE
        data.chunks_exact(2).map(|c| u16::from_le_bytes([c[0], c[1]])).collect()
    } else {
        // UTF-16BE
        data.chunks_exact(2).map(|c| u16::from_be_bytes([c[0], c[1]])).collect()
    };
    let s = String::from_utf16_lossy(&chars);
    Some(s.trim_end_matches('\0').to_string())
}

pub fn parse_tags(data: &[u8], options: &crate::InputOptions, map: &mut GroupedTagMap) -> Result<()> {
    let mut slice = Cursor::new(data);
    let datalen = data.len() as usize;

    while slice.position() < datalen as u64 {
        let tag = slice.read_u16::<BigEndian>()?;
        if tag == 0x060e {
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
            parse_tags(tag_data, options, map)?;
            continue;
        }
        let mut tag_info = if tag > 0xe000 { //
            get_tag(tag, tag_data)
        } else {
            sony::rtmd_tags::get_tag(tag, tag_data)
        };
        tag_info.native_id = Some(tag as u32);

        util::insert_tag(map, tag_info, options);

        // For 0xE227 (Timestamp), also extract and store timezone info
        if tag == 0xE227 {
            if let Some(tz_str) = cndm_tags::extract_timezone(tag_data) {
                let tz_tag = tag!(parsed GroupId::Default, TagId::Custom("CndmTimezone".into()), "CNDM Timezone offset", String, |v| v.to_string(), tz_str, Vec::new());
                util::insert_tag(map, tz_tag, options);
            }
        }
    }
    Ok(())
}
