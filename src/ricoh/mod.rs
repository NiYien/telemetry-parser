// SPDX-License-Identifier: MIT OR Apache-2.0

use std::io::*;
use std::sync::{ Arc, atomic::AtomicBool };

use crate::*;
use crate::tags_impl::*;
use memchr::memmem;


#[derive(Default)]
pub struct Ricoh {
    pub model: Option<String>,
    pub lens: Option<String>,
    frame_readout_time: Option<f64>,
    mvhd_creation_time: Option<String>,
}

impl Ricoh {
    pub fn camera_type(&self) -> String {
        "Ricoh".to_owned()
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
        if memmem::find(buffer, b"RICOH").is_some() {
            return Some(Self {
                model: None,
                lens: None,
                frame_readout_time: None,
                mvhd_creation_time: util::extract_mvhd_creation_time(buffer),
            });
        }
        None
    }

    pub fn parse<T: Read + Seek, F: Fn(f64)>(&mut self, stream: &mut T, size: usize, _progress_cb: F, _cancel_flag: Arc<AtomicBool>, options: crate::InputOptions) -> Result<Vec<SampleInfo>> {
        let mut samples = Vec::new();
        let mut first_map = GroupedTagMap::new();

        // Try to find model string by scanning moov/udta boxes
        stream.seek(SeekFrom::Start(0))?;
        self.scan_for_model(stream)?;

        // If model not found from boxes, try scanning beginning + end of file
        if self.model.is_none() {
            stream.seek(SeekFrom::Start(0))?;
            if let Ok(buf) = util::read_beginning_and_end(stream, size, 4 * 1024 * 1024) {
                self.find_model_in_buffer(&buf);
            }
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
            if typ == util::fourcc("©mod") || typ == util::fourcc("©nam") || typ == util::fourcc("©cmt") {
                let data_size = (size - header_size as u64) as usize;
                if data_size > 0 && data_size < 1024 {
                    let mut buf = vec![0u8; data_size];
                    if stream.read_exact(&mut buf).is_ok() {
                        if let Ok(text) = String::from_utf8(buf.clone()) {
                            if text.contains("RICOH") || text.contains("GR") {
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
            (b"GR IV",   "GR IV"),
            (b"GR IIIx", "GR IIIx"),
            (b"GR III",  "GR III"),
            (b"GR II",   "GR II"),
        ];

        for (pattern, model_name) in patterns {
            if memmem::find(buf, pattern).is_some() {
                self.model = Some(model_name.to_string());
                return;
            }
        }

        // Fallback: search for "RICOH" followed by text that might contain "GR"
        if let Some(pos) = memmem::find(buf, b"RICOH") {
            let end = std::cmp::min(pos + 100, buf.len());
            if let Ok(text) = std::str::from_utf8(&buf[pos..end]) {
                if text.contains("GR") {
                    // Store the raw text; JSON camera_db will handle model matching
                    let model_end = text.find('\0').unwrap_or(text.len());
                    self.model = Some(text[..model_end].trim().to_string());
                    return;
                }
            }
        }

        // Last resort: just "GR" alone (but only if "RICOH" was found in buffer, which detect() already confirmed)
        if memmem::find(buf, b"GR").is_some() && self.model.is_none() {
            // Don't set model to "GR" generically - too ambiguous
        }
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
                if let Some((model_name, model_data)) = db.process_model("RICOH", raw_name, map, options) {
                    self.model = Some(model_name.to_string());

                    // Fixed focal length from JSON model data, or user-provided
                    let fl = model_data.fixed_fl.or(options.user_focal_length);
                    if let Some(fl_val) = fl {
                        if model_data.fixed_fl.is_some() {
                            let lens_name = format!("GR LENS {:.0}mm f/2.8", fl_val);
                            util::insert_tag(map, tag!(parsed GroupId::Lens, TagId::DisplayName, "Lens name", String, |v| v.clone(), lens_name, Vec::new()), options);
                        }
                        util::insert_tag(map, tag!(parsed GroupId::Lens, TagId::FocalLength, "Focal length", f32, |v| format!("{:.1} mm", v), fl_val as f32, Vec::new()), options);

                        if resolution_w > 0 {
                            let sensor_w = model_data.sw as f64;
                            let unit_px_fl = resolution_w as f64 / sensor_w;
                            util::insert_tag(map, tag!(parsed GroupId::Lens, TagId::Custom("unit_pixel_focal_length".into()), "Pixel focal length per mm", f64, |v| format!("{:.4}", v), unit_px_fl, Vec::new()), options);
                            let fx = fl_val / sensor_w * resolution_w as f64;
                            util::insert_tag(map, tag!(parsed GroupId::Lens, TagId::PixelFocalLength, "Pixel focal length", f32, |v| format!("{:.2}", v), fx as f32, Vec::new()), options);
                        }
                    }

                    // Crop (Ricoh has no crop rules, but try anyway)
                    let tags = std::collections::HashMap::new();
                    let _effective_crop = db.process_crop("RICOH", model_name, resolution_w, resolution_h, fps, None, &tags, map, options);

                    // Readout
                    if self.frame_readout_time.is_none() {
                        if let Some(rt) = db.process_readout("RICOH", model_name, resolution_w, resolution_h, fps, 0.0, model_data.sw, &tags, map, options) {
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
