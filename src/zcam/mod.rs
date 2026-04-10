// SPDX-License-Identifier: MIT OR Apache-2.0

use std::io::*;
use std::sync::{ Arc, atomic::AtomicBool };

use crate::*;
use crate::tags_impl::*;
use memchr::memmem;


/// Maximum sensor resolution per model, used to compute crop factor as max_res / actual_res.
fn max_resolution(model: &str) -> Option<u32> {
    match model {
        "E2-F8"     => Some(8192),
        "E2-F6 Pro" => Some(6144),
        "E2-F6"     => Some(6144),
        "E2-S6G"    => Some(6144),
        "E2-S6"     => Some(6244),
        "E2-M4"     => Some(4096),
        "E2"        => Some(4096),
        "E2C"       => Some(4096),
        _           => None,
    }
}

#[derive(Default)]
pub struct Zcam {
    pub model: Option<String>,
    pub lens: Option<String>,
    lens_model: Option<String>,
    frame_readout_time: Option<f64>,
    mvhd_creation_time: Option<String>,
}

impl Zcam {
    pub fn camera_type(&self) -> String {
        "ZCAM".to_owned()
    }
    pub fn has_accurate_timestamps(&self) -> bool {
        false
    }
    pub fn possible_extensions() -> Vec<&'static str> {
        vec!["mp4", "mov"]
    }
    pub fn frame_readout_time(&self) -> Option<f64> {
        self.frame_readout_time
    }
    pub fn normalize_imu_orientation(v: String) -> String {
        v
    }

    pub fn detect<P: AsRef<std::path::Path>>(buffer: &[u8], _filepath: P, _options: &crate::InputOptions) -> Option<Self> {
        if memmem::find(buffer, b"Z CAM").is_some() || memmem::find(buffer, b"zCAM").is_some() {
            return Some(Self {
                model: None,
                lens: None,
                lens_model: None,
                frame_readout_time: None,
                mvhd_creation_time: util::extract_mvhd_creation_time(buffer),
            });
        }
        None
    }

    pub fn parse<T: Read + Seek, F: Fn(f64)>(&mut self, stream: &mut T, size: usize, _progress_cb: F, _cancel_flag: Arc<AtomicBool>, options: crate::InputOptions) -> Result<Vec<SampleInfo>> {
        let mut samples = Vec::new();
        let mut first_map = GroupedTagMap::new();

        // Scan MP4 boxes for model string
        stream.seek(SeekFrom::Start(0))?;
        self.scan_for_model(stream)?;

        // If model not found from boxes, try scanning beginning + end of file
        if self.model.is_none() {
            stream.seek(SeekFrom::Start(0))?;
            if let Ok(buf) = util::read_beginning_and_end(stream, size, 4 * 1024 * 1024) {
                self.find_model_in_buffer(&buf);
            }
        }

        // Scan for focal length from QuickTime mdta/keys/ilst metadata
        stream.seek(SeekFrom::Start(0))?;
        if let Ok(buf) = util::read_beginning_and_end(stream, size, 4 * 1024 * 1024) {
            self.extract_focal_length_from_mdta(&buf);
        }

        // Get video track metadata (resolution, fps)
        stream.seek(SeekFrom::Start(0))?;
        let video_md = util::get_video_metadata(stream, size).ok();

        self.process_map(&mut first_map, &options, video_md.as_ref());

        samples.push(SampleInfo {
            tag_map: Some(first_map),
            ..Default::default()
        });

        Ok(samples)
    }

    /// Scan MP4 boxes for model string in moov/udta area
    fn scan_for_model<T: Read + Seek>(&mut self, stream: &mut T) -> Result<()> {
        while let Ok((typ, _offs, size, header_size)) = util::read_box(stream) {
            if size == 0 || typ == 0 { break; }
            let org_pos = stream.stream_position()?;

            if typ == util::fourcc("moov") || typ == util::fourcc("udta") || typ == util::fourcc("meta") {
                continue; // descend into these boxes
            }

            // Check for text-bearing boxes that might contain the model string
            if typ == util::fourcc("\u{00a9}mod") || typ == util::fourcc("\u{00a9}nam") || typ == util::fourcc("\u{00a9}cmt") {
                let data_size = (size - header_size as u64) as usize;
                if data_size > 0 && data_size < 1024 {
                    let mut buf = vec![0u8; data_size];
                    if stream.read_exact(&mut buf).is_ok() {
                        if let Ok(text) = String::from_utf8(buf) {
                            if text.contains("Z CAM") || text.contains("zCAM") || text.contains("E2") {
                                self.model = Some(text.trim().to_string());
                            }
                        }
                    }
                }
            }

            stream.seek(SeekFrom::Start(org_pos + size - header_size as u64))?;
        }
        Ok(())
    }

    /// Scan raw buffer for known model strings
    fn find_model_in_buffer(&mut self, buf: &[u8]) {
        // Search for specific model strings in order of specificity (longest first)
        let patterns: &[(&[u8], &str)] = &[
            (b"E2-F6 Pro", "E2-F6 Pro"),
            (b"E2-F8",     "E2-F8"),
            (b"E2-F6",     "E2-F6"),
            (b"E2-S6G",    "E2-S6G"),
            (b"E2-S6",     "E2-S6"),
            (b"E2-M4",     "E2-M4"),
            (b"E2C",       "E2C"),
            (b"E2",        "E2"),
        ];

        for (pattern, model_name) in patterns {
            if memmem::find(buf, pattern).is_some() {
                self.model = Some(model_name.to_string());
                return;
            }
        }
    }

    /// Extract focal length from QuickTime mdta/keys/ilst structure.
    /// ZCAM stores metadata like "com.zcam.camera.lens.focal_length" in keys atom.
    fn extract_focal_length_from_mdta(&mut self, buf: &[u8]) {
        // Find the meta box containing hdlr with mdta handler
        let mut offs = 0;
        while let Some(pos) = memmem::find(&buf[offs..], b"meta") {
            let abs_pos = offs + pos;
            // meta box: [size:4][meta:4][version+flags:4][hdlr box...]
            // Check that hdlr follows (at offset +8 or +12)
            if buf.len() > abs_pos + 12 && &buf[abs_pos + 8..abs_pos + 12] == b"hdlr" {
                // Found meta box with hdlr, check if handler is mdta
                // hdlr box: [size:4][hdlr:4][version:4][pre_defined:4][handler_type:4]...
                if buf.len() > abs_pos + 24 && &buf[abs_pos + 20..abs_pos + 24] == b"mdta" {
                    // Parse meta box size
                    if abs_pos >= 4 {
                        let meta_size = u32::from_be_bytes([
                            buf[abs_pos - 4], buf[abs_pos - 3],
                            buf[abs_pos - 2], buf[abs_pos - 1],
                        ]) as usize;
                        if meta_size > 8 && abs_pos + meta_size - 4 <= buf.len() {
                            let meta_data = &buf[abs_pos - 4..abs_pos - 4 + meta_size];
                            self.parse_mdta_meta(meta_data);
                            return;
                        }
                    }
                }
            }
            offs = abs_pos + 4;
            if offs >= buf.len() { break; }
        }
    }

    /// Parse a meta box with mdta handler to find keys and ilst values
    fn parse_mdta_meta(&mut self, meta: &[u8]) {
        // meta box can be full box (12-byte header: size+type+version/flags)
        // or plain container (8-byte header: size+type, as used by ZCAM)
        // Detect by checking if a valid child box starts at offset 8 vs 12
        let mut keys: Vec<String> = Vec::new();

        let children_start = if meta.len() > 12 {
            let sz_at_8 = u32::from_be_bytes([meta[8], meta[9], meta[10], meta[11]]) as usize;
            if sz_at_8 >= 8 && sz_at_8 < meta.len() { 8 } else { 12 }
        } else { 8 };
        let mut pos = children_start;
        while pos + 8 <= meta.len() {
            let box_size = u32::from_be_bytes([meta[pos], meta[pos + 1], meta[pos + 2], meta[pos + 3]]) as usize;
            if box_size < 8 || pos + box_size > meta.len() { break; }
            let box_type = &meta[pos + 4..pos + 8];

            if box_type == b"keys" {
                // keys box: [size:4][keys:4][version+flags:4][entry_count:4][entries...]
                // Each entry: [key_size:4][key_namespace:4][key_value:variable]
                if box_size > 16 {
                    let entry_count = u32::from_be_bytes([
                        meta[pos + 12], meta[pos + 13], meta[pos + 14], meta[pos + 15],
                    ]) as usize;
                    let mut key_offs = pos + 16;
                    for _ in 0..entry_count {
                        if key_offs + 8 > pos + box_size { break; }
                        let key_size = u32::from_be_bytes([
                            meta[key_offs], meta[key_offs + 1], meta[key_offs + 2], meta[key_offs + 3],
                        ]) as usize;
                        if key_size < 8 || key_offs + key_size > pos + box_size { break; }
                        // key_namespace is at key_offs+4..key_offs+8, key_value at key_offs+8..key_offs+key_size
                        if let Ok(key_str) = std::str::from_utf8(&meta[key_offs + 8..key_offs + key_size]) {
                            keys.push(key_str.to_string());
                        } else {
                            keys.push(String::new());
                        }
                        key_offs += key_size;
                    }
                }
            } else if box_type == b"ilst" {
                // ilst box: [size:4][ilst:4][indexed items...]
                // Each item: [size:4][index:4(1-based)][data box inside...]
                // Data box: [size:4][data:4][type_indicator:4][locale:4][value...]
                let mut item_offs = pos + 8;
                while item_offs + 8 <= pos + box_size {
                    let item_size = u32::from_be_bytes([
                        meta[item_offs], meta[item_offs + 1], meta[item_offs + 2], meta[item_offs + 3],
                    ]) as usize;
                    if item_size < 8 || item_offs + item_size > pos + box_size { break; }

                    // Index is 1-based
                    let index = u32::from_be_bytes([
                        meta[item_offs + 4], meta[item_offs + 5], meta[item_offs + 6], meta[item_offs + 7],
                    ]) as usize;

                    // Look for the "data" sub-box
                    if item_size > 24 && item_offs + 24 <= pos + box_size {
                        let data_box_size = u32::from_be_bytes([
                            meta[item_offs + 8], meta[item_offs + 9], meta[item_offs + 10], meta[item_offs + 11],
                        ]) as usize;
                        let data_box_type = &meta[item_offs + 12..item_offs + 16];
                        if data_box_type == b"data" && data_box_size >= 16 {
                            let type_indicator = u32::from_be_bytes([
                                meta[item_offs + 16], meta[item_offs + 17], meta[item_offs + 18], meta[item_offs + 19],
                            ]);
                            // locale at item_offs+20..item_offs+24
                            let value_start = item_offs + 24;
                            let value_end = item_offs + 8 + data_box_size;
                            if value_end <= pos + box_size && value_start < value_end {
                                let value_data = &meta[value_start..value_end];
                                if index >= 1 {
                                    if let Some(key) = keys.get(index - 1) {
                                        self.handle_mdta_value(key, type_indicator, value_data);
                                    }
                                }
                            }
                        }
                    }

                    item_offs += item_size;
                }
            }

            pos += box_size;
        }
    }

    /// Handle a single mdta key-value pair
    fn handle_mdta_value(&mut self, key: &str, type_indicator: u32, value_data: &[u8]) {
        // ZCAM lens type/model (e.g. "OLYMPUS M.14-42mm F3.5-5.6 EZ")
        if (key.contains("lensType") || key.contains("lens_type") || key.contains("LensModel")) && type_indicator == 1 {
            if let Ok(s) = std::str::from_utf8(value_data) {
                let s = s.trim();
                if !s.is_empty() {
                    self.lens_model = Some(s.to_string());
                }
            }
        }

        // ZCAM focal length key variants
        let is_focal_length_key =
            key.contains("focal_length") ||
            key.contains("FocalLength") ||
            key.contains("focal.length");

        if is_focal_length_key {
            match type_indicator {
                // type 1 = UTF-8 string
                1 => {
                    if let Ok(s) = std::str::from_utf8(value_data) {
                        // Value may contain "mm" suffix (e.g. "14mm"), strip it before parsing
                        let num_str = s.trim().trim_end_matches("mm").trim_end_matches("MM").trim();
                        if let Ok(fl) = num_str.parse::<f64>() {
                            if fl > 0.0 {
                                self.lens = Some(format!("{:.0}mm", fl));
                            }
                        }
                    }
                }
                // type 23 = float32 BE
                23 => {
                    if value_data.len() >= 4 {
                        let fl = f32::from_be_bytes([value_data[0], value_data[1], value_data[2], value_data[3]]) as f64;
                        if fl > 0.0 {
                            self.lens = Some(format!("{:.0}mm", fl));
                        }
                    }
                }
                // type 24 = float64 BE
                24 => {
                    if value_data.len() >= 8 {
                        let fl = f64::from_be_bytes([
                            value_data[0], value_data[1], value_data[2], value_data[3],
                            value_data[4], value_data[5], value_data[6], value_data[7],
                        ]);
                        if fl > 0.0 {
                            self.lens = Some(format!("{:.0}mm", fl));
                        }
                    }
                }
                _ => {}
            }
        }
    }

    /// Parse focal length value from the lens field (stored as "XXmm" string)
    fn focal_length_mm(&self) -> Option<f64> {
        self.lens.as_ref().and_then(|s| {
            s.trim_end_matches("mm").parse::<f64>().ok().filter(|&v| v > 0.0)
        })
    }

    fn process_map(&mut self, map: &mut GroupedTagMap, options: &crate::InputOptions, video_md: Option<&VideoMetadata>) {
        let resolution_w = video_md.map(|v| v.width as u32).unwrap_or(0);
        let resolution_h = video_md.map(|v| v.height as u32).unwrap_or(0);
        let fps = video_md.map(|v| v.fps).unwrap_or(0.0);

        // Write video track resolution
        if let Some(vmd) = video_md {
            util::insert_tag(map, tag!(parsed GroupId::Default, TagId::Custom("video_width".into()), "Video output width", u32, |v| format!("{} px", v), vmd.width as u32, Vec::new()), options);
            util::insert_tag(map, tag!(parsed GroupId::Default, TagId::Custom("video_height".into()), "Video output height", u32, |v| format!("{} px", v), vmd.height as u32, Vec::new()), options);
        }

        // Try JSON database first
        if let Some(db_path) = &options.camera_db_path {
            if let Ok(db) = crate::camera_db::CameraDatabase::load(db_path) {
                let raw_name = self.model.as_deref().unwrap_or("");
                if let Some((model_name, model_data)) = db.process_model("ZCAM", raw_name, map, options) {
                    self.model = Some(model_name.to_string());
                    let sensor_w = model_data.sw;

                    // Crop factor: ZCAM uses max_resolution / actual_resolution
                    let crop_factor = if resolution_w > 0 {
                        max_resolution(model_name).map(|max_res| {
                            let cf = max_res as f64 / resolution_w as f64;
                            // Only apply crop if resolution is actually lower than max (crop >= 1.0)
                            if cf >= 1.0 { cf } else { 1.0 }
                        })
                    } else {
                        None
                    };

                    if let Some(cf) = crop_factor {
                        util::insert_tag(map, tag!(parsed GroupId::Default, TagId::Custom("crop_factor".into()), "Crop factor", f64, |v| format!("{:.4}", v), cf, Vec::new()), options);
                    }
                    let effective_crop = crop_factor.unwrap_or(1.0);

                    // Focal length and pixel focal length
                    let fl = self.focal_length_mm().or(options.user_focal_length);
                    if let Some(fl_val) = fl {
                        util::insert_tag(map, tag!(parsed GroupId::Lens, TagId::FocalLength, "Focal length", f32, |v| format!("{:.1} mm", v), fl_val as f32, Vec::new()), options);

                        if resolution_w > 0 && sensor_w > 0.0 {
                            let unit_px_fl = resolution_w as f64 * effective_crop / sensor_w as f64;
                            util::insert_tag(map, tag!(parsed GroupId::Lens, TagId::Custom("unit_pixel_focal_length".into()), "Pixel focal length per mm", f64, |v| format!("{:.4}", v), unit_px_fl, Vec::new()), options);
                            let px_fl = fl_val * unit_px_fl;
                            util::insert_tag(map, tag!(parsed GroupId::Lens, TagId::PixelFocalLength, "Pixel focal length", f32, |v| format!("{:.2}", v), px_fl as f32, Vec::new()), options);
                        }
                    } else if resolution_w > 0 && sensor_w > 0.0 {
                        // No focal length available, but still write unit_pixel_focal_length
                        let unit_px_fl = resolution_w as f64 * effective_crop / sensor_w as f64;
                        util::insert_tag(map, tag!(parsed GroupId::Lens, TagId::Custom("unit_pixel_focal_length".into()), "Pixel focal length per mm", f64, |v| format!("{:.4}", v), unit_px_fl, Vec::new()), options);
                    }

                    // Lens model → DisplayName (used by CameraIdentifier as lens_model)
                    if let Some(ref lm) = self.lens_model {
                        util::insert_tag(map, tag!(parsed GroupId::Lens, TagId::DisplayName, "Lens model", String, |v| v.clone(), lm.clone(), Vec::new()), options);
                    } else if let Some(ref lens_str) = self.lens {
                        // Fallback: use focal length string as display name if no lens model
                        util::insert_tag(map, tag!(parsed GroupId::Lens, TagId::DisplayName, "Lens name", String, |v| v.clone(), lens_str.clone(), Vec::new()), options);
                    }

                    // Readout time
                    if self.frame_readout_time.is_none() {
                        let tags = std::collections::HashMap::new();
                        if let Some(rt) = db.process_readout("ZCAM", model_name, resolution_w, resolution_h, fps, 0.0, sensor_w, &tags, map, options) {
                            self.frame_readout_time = Some(rt);
                        }
                    }

                    // Creation date from mvhd
                    if let Some(ref mvhd_time) = self.mvhd_creation_time {
                        util::write_creation_date_tags(map, mvhd_time, None, Some("500"), options);
                    }

                    // Frame rates
                    if fps > 0.0 {
                        util::insert_tag(map, tag!(parsed GroupId::Default, TagId::FrameRate, "Frame rate", f64, |v| format!("{:.3} fps", v), fps, Vec::new()), options);
                        util::insert_tag(map, tag!(parsed GroupId::Default, TagId::RecordFrameRate, "Record frame rate", f64, |v| format!("{:.3} fps", v), fps, Vec::new()), options);
                    }

                    // Image stabilization (no metadata source, default false)
                    util::insert_tag(map, tag!(parsed GroupId::Default, TagId::ImageStabilizer, "Image stabilization", bool, |v| if *v { "On" } else { "Off" }.into(), false, Vec::new()), options);

                    return; // JSON path complete
                }
            }
        }

        // Fallback creation date (no JSON db match)
        if let Some(ref mvhd_time) = self.mvhd_creation_time {
            util::write_creation_date_tags(map, mvhd_time, None, Some("500"), options);
        }
    }
}
