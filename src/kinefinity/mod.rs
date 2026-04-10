// SPDX-License-Identifier: MIT OR Apache-2.0

//! Kinefinity camera format parser.
//! Kinefinity MOV files have empty QuickTime keys/ilst metadata.
//! Detection is via "KINE" marker in mdat header area.
//! Core metadata comes from a sidecar file: `{video_name}-slate.txt`
//! (SLATE.TXT Revision 2.0 format — key-value pairs separated by dots and colon).

use std::io::*;
use std::sync::{ Arc, atomic::AtomicBool };

use crate::*;
use crate::tags_impl::*;
use memchr::memmem;


/// Data extracted from the `-slate.txt` sidecar file.
#[derive(Debug, Default)]
pub struct SlateData {
    pub camera_model: Option<String>,
    pub image_format: Option<String>,  // "FF", "S35", "M43", "S16", "16mm"
    pub width: Option<u32>,
    pub height: Option<u32>,
    pub sensor_fps: Option<f64>,
    pub focal_length: Option<f64>,     // None if "N/A"
    pub shot_date: Option<String>,
    pub shot_tod: Option<String>,
}

/// View-angle mapping entry: (view_angle_name, sensor_width_mm, max_resolution_w).
struct ViewAngleEntry {
    view_angle: &'static str,
    sensor_w_mm: f64,
    frame_w: u32,
}

/// Get view-angle mapping for a given model.
/// Returns list of (view_angle_name, sensor_width_mm, max_resolution_w) entries.
fn get_view_angle_map(model: &str) -> &'static [ViewAngleEntry] {
    match model {
        "MAVO Edge 8K" => &[
            ViewAngleEntry { view_angle: "FULL", sensor_w_mm: 36.0, frame_w: 8192 },
            ViewAngleEntry { view_angle: "S35",  sensor_w_mm: 27.0, frame_w: 6144 },
        ],
        "MAVO Edge 6K" => &[
            ViewAngleEntry { view_angle: "FULL", sensor_w_mm: 36.0, frame_w: 6016 },
            ViewAngleEntry { view_angle: "S35",  sensor_w_mm: 24.5, frame_w: 4096 },
        ],
        "MAVO" => &[
            ViewAngleEntry { view_angle: "S35",  sensor_w_mm: 24.0, frame_w: 6016 },
            ViewAngleEntry { view_angle: "M43",  sensor_w_mm: 16.0, frame_w: 4096 },
            ViewAngleEntry { view_angle: "S16",  sensor_w_mm: 12.0, frame_w: 3072 },
            ViewAngleEntry { view_angle: "16mm", sensor_w_mm: 8.0,  frame_w: 2048 },
        ],
        "MAVO2 S35" => &[
            ViewAngleEntry { view_angle: "S35",  sensor_w_mm: 24.0, frame_w: 6144 },
            ViewAngleEntry { view_angle: "M43",  sensor_w_mm: 16.0, frame_w: 4096 },
            ViewAngleEntry { view_angle: "S16",  sensor_w_mm: 12.0, frame_w: 3072 },
            ViewAngleEntry { view_angle: "16mm", sensor_w_mm: 8.0,  frame_w: 2048 },
        ],
        "MAVO LF" | "MAVO2 LF" => &[
            ViewAngleEntry { view_angle: "FULL", sensor_w_mm: 36.0, frame_w: 6016 },
            ViewAngleEntry { view_angle: "S35",  sensor_w_mm: 24.5, frame_w: 4096 },
        ],
        "TERRA 4K" => &[
            ViewAngleEntry { view_angle: "S35",  sensor_w_mm: 19.5, frame_w: 4096 },
            ViewAngleEntry { view_angle: "M43",  sensor_w_mm: 14.62, frame_w: 3072 },
            ViewAngleEntry { view_angle: "S16",  sensor_w_mm: 9.7,  frame_w: 2048 },
        ],
        _ => &[],
    }
}

/// Map the slate "Image Format" field to a canonical view-angle name.
fn normalize_image_format(fmt: &str) -> &str {
    match fmt.trim() {
        "FF"   => "FULL",
        "S35"  => "S35",
        "M43"  => "M43",
        "S16"  => "S16",
        "16mm" => "16mm",
        other  => other,
    }
}


#[derive(Default)]
pub struct Kinefinity {
    pub model: Option<String>,
    pub lens: Option<String>,
    frame_readout_time: Option<f64>,
    video_path: String,
}

impl Kinefinity {
    pub fn camera_type(&self) -> String {
        "Kinefinity".to_owned()
    }
    pub fn has_accurate_timestamps(&self) -> bool {
        false
    }
    pub fn possible_extensions() -> Vec<&'static str> {
        vec!["mov"]
    }
    pub fn frame_readout_time(&self) -> Option<f64> {
        self.frame_readout_time
    }
    pub fn normalize_imu_orientation(v: String) -> String {
        v
    }

    pub fn detect<P: AsRef<std::path::Path>>(buffer: &[u8], filepath: P, _options: &crate::InputOptions) -> Option<Self> {
        // Kinefinity MOV files have "KINE" marker in the first ~64 bytes of the mdat area,
        // typically after "icpf". Scan the first portion of the buffer for it.
        let search_len = buffer.len().min(4096);
        if memmem::find(&buffer[..search_len], b"KINE").is_some() {
            let path = filepath.as_ref().to_str().unwrap_or_default().to_owned();
            return Some(Self {
                model: None,
                lens: None,
                frame_readout_time: None,
                video_path: path,
            });
        }
        None
    }

    pub fn parse<T: Read + Seek, F: Fn(f64)>(&mut self, stream: &mut T, size: usize, _progress_cb: F, _cancel_flag: Arc<AtomicBool>, options: crate::InputOptions) -> Result<Vec<SampleInfo>> {
        let mut samples = Vec::new();
        let mut first_map = GroupedTagMap::new();

        // Get video track metadata (resolution, fps)
        stream.seek(SeekFrom::Start(0))?;
        let video_md = util::get_video_metadata(stream, size).ok();

        // Look for sidecar file: {stem}-slate.txt
        let slate = if !options.dont_look_for_sidecar_files {
            self.find_and_parse_slate()
        } else {
            None
        };

        self.process_map(&mut first_map, &options, video_md.as_ref(), slate.as_ref());

        samples.push(SampleInfo {
            tag_map: Some(first_map),
            ..Default::default()
        });

        Ok(samples)
    }

    /// Attempt to find and parse the `-slate.txt` sidecar file.
    fn find_and_parse_slate(&self) -> Option<SlateData> {
        if self.video_path.is_empty() { return None; }
        let slate_path = find_slate_path(&self.video_path)?;
        parse_slate_file(std::path::Path::new(&slate_path))
    }

    fn process_map(&mut self, map: &mut GroupedTagMap, options: &crate::InputOptions, video_md: Option<&VideoMetadata>, slate: Option<&SlateData>) {
        let resolution_w = slate.and_then(|s| s.width)
            .or_else(|| video_md.map(|v| v.width as u32))
            .unwrap_or(0);
        let resolution_h = slate.and_then(|s| s.height)
            .or_else(|| video_md.map(|v| v.height as u32))
            .unwrap_or(0);
        let fps = slate.and_then(|s| s.sensor_fps)
            .or_else(|| video_md.map(|v| v.fps))
            .unwrap_or(0.0);

        // Write video track resolution
        if resolution_w > 0 {
            util::insert_tag(map, tag!(parsed GroupId::Default, TagId::Custom("video_width".into()), "Video output width", u32, |v| format!("{} px", v), resolution_w, Vec::new()), options);
            util::insert_tag(map, tag!(parsed GroupId::Default, TagId::Custom("video_height".into()), "Video output height", u32, |v| format!("{} px", v), resolution_h, Vec::new()), options);
        }

        // Determine camera model from slate or fallback
        let raw_model_name = slate.and_then(|s| s.camera_model.as_deref()).unwrap_or("");
        let image_format = slate.and_then(|s| s.image_format.as_deref()).unwrap_or("");
        let view_angle = if !image_format.is_empty() {
            Some(normalize_image_format(image_format))
        } else {
            None
        };

        // Try JSON database
        if let Some(db_path) = &options.camera_db_path {
            if let Ok(db) = crate::camera_db::CameraDatabase::load(db_path) {
                if let Some((model_name, model_data)) = db.process_model("KINEFINITY", raw_model_name, map, options) {
                    self.model = Some(model_name.to_string());
                    let sensor_w = model_data.sw;

                    // Determine crop factor from view-angle mapping
                    let view_angle_map = get_view_angle_map(model_name);
                    let va_str = view_angle.unwrap_or("");
                    let va_entry = view_angle_map.iter().find(|e| e.view_angle == va_str);

                    let crop_factor = if let Some(entry) = va_entry {
                        // Compute crop from view-angle frame_w vs actual resolution
                        let max_res_w = entry.frame_w;
                        let effective_sensor_w = entry.sensor_w_mm;

                        // Oversample detection: if actual resolution < max_resolution_w,
                        // crop_factor = sensor_w / effective_sensor_w
                        // Otherwise (actual >= max): crop_factor = sensor_w / effective_sensor_w * (max / actual)
                        // But for Kinefinity, the crop is simply: sensor_w / effective_sensor_w
                        // adjusted by the ratio of max_resolution_w to actual resolution when downsampled.
                        let base_crop = sensor_w as f64 / effective_sensor_w;

                        // If the actual resolution is less than the view-angle native resolution,
                        // it's a downsample (no extra crop). If greater, it shouldn't happen.
                        // The crop factor stays the same regardless of output resolution scaling.
                        if base_crop > 1.001 {
                            util::insert_tag(map, tag!(parsed GroupId::Default, TagId::Custom("crop_factor".into()), "Crop factor", f64, |v| format!("{:.4}", v), base_crop, Vec::new()), options);
                        }

                        // Unit pixel focal length: based on the view-angle's sensor width and max resolution
                        if resolution_w > 0 && max_res_w > 0 {
                            // If actual resolution equals or is a clean scale of max_res_w,
                            // the unit_pixel_focal_length should be based on actual resolution
                            let unit_px_fl = resolution_w as f64 / effective_sensor_w;
                            util::insert_tag(map, tag!(parsed GroupId::Lens, TagId::Custom("unit_pixel_focal_length".into()), "Pixel focal length per mm", f64, |v| format!("{:.4}", v), unit_px_fl, Vec::new()), options);
                        }

                        base_crop
                    } else {
                        // No view-angle match, try JSON crop rules as fallback
                        let tags = std::collections::HashMap::new();
                        db.process_crop("KINEFINITY", model_name, resolution_w, resolution_h, fps, view_angle, &tags, map, options)
                    };

                    // Focal length from slate
                    let fl = slate.and_then(|s| s.focal_length).or(options.user_focal_length);
                    if let Some(fl_val) = fl {
                        util::insert_tag(map, tag!(parsed GroupId::Lens, TagId::FocalLength, "Focal length", f32, |v| format!("{:.1} mm", v), fl_val as f32, Vec::new()), options);

                        // Pixel focal length
                        if resolution_w > 0 {
                            let effective_sensor_w = va_entry.map(|e| e.sensor_w_mm).unwrap_or(sensor_w as f64);
                            let fx = fl_val / effective_sensor_w * resolution_w as f64;
                            if fx > 0.0 {
                                util::insert_tag(map, tag!(parsed GroupId::Lens, TagId::PixelFocalLength, "Pixel focal length", f32, |v| format!("{:.2}", v), fx as f32, Vec::new()), options);
                            }
                        }
                    }

                    // Readout time from database
                    if self.frame_readout_time.is_none() {
                        let tags = std::collections::HashMap::new();
                        let lookup_fps = if fps < 1.0 { 24.0 } else { fps };
                        if let Some(rt) = db.process_readout("KINEFINITY", model_name, resolution_w, resolution_h, lookup_fps, crop_factor, sensor_w, &tags, map, options) {
                            self.frame_readout_time = Some(rt);
                        }
                    }

                    // Creation date from slate
                    if let Some(slate) = slate {
                        if let (Some(date), Some(tod)) = (&slate.shot_date, &slate.shot_tod) {
                            // Combine date and time: "YYYY/MM/DD" + "HH:MM:SS" -> "YYYY:MM:DD HH:MM:SS"
                            let date_str = date.replace('/', ":");
                            let combined = format!("{} {}", date_str, tod);
                            util::write_creation_date_tags(map, &combined, None, Some("500"), options);
                        }
                    }

                    // Frame rates
                    if fps > 0.0 {
                        util::insert_tag(map, tag!(parsed GroupId::Default, TagId::FrameRate, "Frame rate", f64, |v| format!("{:.3} fps", v), fps, Vec::new()), options);
                        util::insert_tag(map, tag!(parsed GroupId::Default, TagId::RecordFrameRate, "Record frame rate", f64, |v| format!("{:.3} fps", v), fps, Vec::new()), options);
                    }

                    // Image stabilization (Kinefinity has no in-body stabilization)
                    util::insert_tag(map, tag!(parsed GroupId::Default, TagId::ImageStabilizer, "Image stabilization", bool, |v| if *v { "On" } else { "Off" }.into(), false, Vec::new()), options);

                    return; // JSON path complete
                }
            }
        }

        // Fallback: write basic metadata without database
        if let Some(slate) = slate {
            if let Some(ref model) = slate.camera_model {
                self.model = Some(model.clone());
            }
            if let (Some(date), Some(tod)) = (&slate.shot_date, &slate.shot_tod) {
                let date_str = date.replace('/', ":");
                let combined = format!("{} {}", date_str, tod);
                util::write_creation_date_tags(map, &combined, None, Some("500"), options);
            }
        }

        if fps > 0.0 {
            util::insert_tag(map, tag!(parsed GroupId::Default, TagId::FrameRate, "Frame rate", f64, |v| format!("{:.3} fps", v), fps, Vec::new()), options);
        }
    }
}


// ---------------------------------------------------------------------------
// Sidecar slate parsing (used externally from gyroflow-core)
// ---------------------------------------------------------------------------

/// Parse a Kinefinity `-slate.txt` sidecar file.
///
/// The file format is "SLATE.TXT Revision 2.0" — each line is:
/// ```text
/// Key Name.........: Value
/// ```
/// The dots are visual padding; the actual delimiter is `": "` (colon-space)
/// after stripping trailing dots from the key portion.
pub fn parse_slate_file(path: &std::path::Path) -> Option<SlateData> {
    let content = std::fs::read_to_string(path).ok()?;
    let mut data = SlateData::default();

    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() { continue; }

        // Find the ": " delimiter (dots before colon are part of key padding)
        let Some(colon_pos) = line.find(": ") else { continue };
        let key_raw = &line[..colon_pos];
        let value = line[colon_pos + 2..].trim();

        // Strip trailing dots and whitespace from key
        let key = key_raw.trim_end_matches('.').trim();

        match key {
            "Camera Model" => {
                data.camera_model = Some(value.to_string());
            }
            "Image Format" => {
                data.image_format = Some(value.to_string());
            }
            "Width" => {
                data.width = value.parse::<u32>().ok();
            }
            "Height" => {
                data.height = value.parse::<u32>().ok();
            }
            "Sensor FPS" => {
                data.sensor_fps = value.parse::<f64>().ok();
            }
            "Focal Length" => {
                if value != "N/A" {
                    // May contain unit, e.g. "35mm" or "35"
                    let num_str = value.trim_end_matches("mm").trim();
                    data.focal_length = num_str.parse::<f64>().ok();
                }
            }
            "Shot date" => {
                data.shot_date = Some(value.to_string());
            }
            "Shot TOD" => {
                data.shot_tod = Some(value.to_string());
            }
            _ => {}
        }
    }

    // Only return if we got at least the camera model
    if data.camera_model.is_some() || data.width.is_some() {
        Some(data)
    } else {
        None
    }
}

/// Find the slate sidecar for a given video path.
/// Given `/path/to/VIDEO_NAME.mov`, looks for `/path/to/VIDEO_NAME-slate.txt`.
pub fn find_slate_path(video_path: &str) -> Option<String> {
    let filename = crate::filesystem::get_filename(video_path);
    let folder = crate::filesystem::get_folder(video_path);

    // Strip extension from filename
    let stem = if let Some(pos) = filename.rfind('.') {
        &filename[..pos]
    } else {
        &filename
    };

    let slate_name = format!("{}-slate.txt", stem);

    // Check if file exists via filesystem abstraction
    let files = crate::filesystem::list_folder(&folder);
    for (name, path) in &files {
        if name.eq_ignore_ascii_case(&slate_name) {
            return Some(path.clone());
        }
    }

    None
}
