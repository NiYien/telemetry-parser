// SPDX-License-Identifier: MIT OR Apache-2.0

//! Leica camera format parser.
//! Extracts metadata from MOV/MP4 files via LEIC atom in moov/udta.
//! The LEIC atom contains null-terminated strings (Make, Model) and may
//! embed TIFF/EXIF data with focal length information.

use std::io::*;
use std::sync::{ Arc, atomic::AtomicBool };

use crate::*;
use crate::tags_impl::*;
use crate::tiff_ifd::{is_tiff_header, read_u16, read_u32, read_string, parse_ifd_entries, extract_tiff_from_jpeg};
use memchr::memmem;


#[derive(Default)]
pub struct Leica {
    pub model: Option<String>,
    pub lens: Option<String>,
    frame_readout_time: Option<f64>,
    mvhd_creation_time: Option<String>,
}

/// Data extracted from Leica LEIC atom / EXIF metadata.
#[derive(Debug, Default)]
struct LeicaExifData {
    model: Option<String>,
    focal_length: Option<f64>,
    lens_model: Option<String>,
    datetime_original: Option<String>,
    offset_time_original: Option<String>,
    subsec_time_original: Option<String>,
}

impl Leica {
    pub fn camera_type(&self) -> String {
        "Leica".to_owned()
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
        if memmem::find(buffer, b"LEIC").is_some() && memmem::find(buffer, b"LEICA").is_some() {
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

        // Parse LEIC atom from moov/udta
        stream.seek(SeekFrom::Start(0))?;
        let exif_data = self.parse_mov(stream, size).unwrap_or_default();

        // Get video track metadata (resolution, fps)
        stream.seek(SeekFrom::Start(0))?;
        let video_md = util::get_video_metadata(stream, size).ok();

        self.process_map(&mut first_map, &options, video_md.as_ref(), &exif_data);

        samples.push(SampleInfo {
            tag_map: Some(first_map),
            ..Default::default()
        });

        Ok(samples)
    }

    /// Parse MOV/MP4: find moov/udta/LEIC atom.
    fn parse_mov<T: Read + Seek>(&mut self, stream: &mut T, _size: usize) -> Result<LeicaExifData> {
        stream.seek(SeekFrom::Start(0))?;

        // First find moov atom
        let moov_data = self.find_moov_data(stream)?;
        let mut cursor = Cursor::new(moov_data.as_slice());
        let moov_len = moov_data.len() as u64;

        // Search inside moov for udta, then LEIC
        while cursor.position() < moov_len {
            let (typ, _offs, size, header_size) = util::read_box(&mut cursor)?;
            let content_size = size as i64 - header_size;
            if content_size <= 0 { break; }

            if typ == util::fourcc("udta") {
                let start = cursor.position() as usize;
                let end = start + content_size as usize;
                if end <= moov_data.len() {
                    if let Some(result) = self.find_leic_in_data(&moov_data[start..end]) {
                        return Ok(result);
                    }
                }
            }
            cursor.seek(SeekFrom::Current(content_size))?;
        }

        // Fallback: search entire moov data for LEIC
        if let Some(result) = self.find_leic_in_data(&moov_data) {
            return Ok(result);
        }

        Err(Error::new(ErrorKind::NotFound, "LEIC atom not found in MOV"))
    }

    fn find_moov_data<T: Read + Seek>(&self, stream: &mut T) -> Result<Vec<u8>> {
        loop {
            let (typ, _offs, size, header_size) = util::read_box(stream)?;
            let content_size = size as i64 - header_size;
            if content_size <= 0 { break; }

            if typ == util::fourcc("moov") {
                let mut data = vec![0u8; content_size as usize];
                stream.read_exact(&mut data)?;
                return Ok(data);
            }
            stream.seek(SeekFrom::Current(content_size))?;
        }
        Err(Error::new(ErrorKind::NotFound, "moov atom not found"))
    }

    fn find_leic_in_data(&self, data: &[u8]) -> Option<LeicaExifData> {
        let mut cursor = Cursor::new(data);
        let len = data.len() as u64;
        while cursor.position() < len {
            let (typ, _offs, size, header_size) = util::read_box(&mut cursor).ok()?;
            let content_size = size as i64 - header_size;
            if content_size < 0 { break; }
            if content_size == 0 { continue; }

            if typ == util::fourcc("LEIC") {
                let start = cursor.position() as usize;
                let end = start + content_size as usize;
                if end <= data.len() {
                    let leic_data = &data[start..end];
                    return Some(extract_from_leic(leic_data));
                }
            }
            cursor.seek(SeekFrom::Current(content_size)).ok()?;
        }
        None
    }

    fn process_map(&mut self, map: &mut GroupedTagMap, options: &crate::InputOptions, video_md: Option<&VideoMetadata>, exif: &LeicaExifData) {
        let raw_model = exif.model.as_deref().unwrap_or("");
        let resolution_w = video_md.map(|v| v.width as u32).unwrap_or(0);
        let resolution_h = video_md.map(|v| v.height as u32).unwrap_or(0);
        let fps = video_md.map(|v| v.fps).unwrap_or(0.0);

        // Write video resolution
        if resolution_w > 0 {
            util::insert_tag(map, tag!(parsed GroupId::Default, TagId::Custom("video_width".into()), "Video output width", u32, |v| format!("{} px", v), resolution_w, Vec::new()), options);
            util::insert_tag(map, tag!(parsed GroupId::Default, TagId::Custom("video_height".into()), "Video output height", u32, |v| format!("{} px", v), resolution_h, Vec::new()), options);
        }

        // Lens
        let has_lens = exif.lens_model.as_ref().map_or(false, |l| !l.is_empty());
        if let Some(ref lens_name) = exif.lens_model {
            if !lens_name.is_empty() {
                self.lens = Some(lens_name.clone());
                util::insert_tag(map, tag!(parsed GroupId::Lens, TagId::DisplayName, "Lens name", String, |v| v.clone(), lens_name.clone(), Vec::new()), options);
            }
        }

        // Try JSON database first
        if let Some(db_path) = &options.camera_db_path {
            if let Ok(db) = crate::camera_db::CameraDatabase::load(db_path) {
                if let Some((model_name, model_data)) = db.process_model("LEICA", raw_model, map, options) {
                    self.model = Some(model_name.to_string());
                    let should_clear_fl = model_data.clear_fl;
                    let sensor_w = model_data.sw;

                    // Unit pixel focal length
                    if resolution_w > 0 {
                        let crop = 1.0_f64;
                        let unit_px_fl = resolution_w as f64 * crop / sensor_w as f64;
                        util::insert_tag(map, tag!(parsed GroupId::Lens, TagId::Custom("unit_pixel_focal_length".into()), "Pixel focal length per mm", f64, |v| format!("{:.4}", v), unit_px_fl, Vec::new()), options);
                    }

                    {
                        let fl_from_file = if has_lens && !should_clear_fl { exif.focal_length } else { None };
                        let fl = fl_from_file.or(options.user_focal_length);
                        if let Some(fl) = fl {
                            if fl_from_file.is_some() {
                                util::insert_tag(map, tag!(parsed GroupId::Lens, TagId::FocalLength, "Focal length", f32, |v| format!("{:.1} mm", v), fl as f32, Vec::new()), options);
                            }
                            if resolution_w > 0 {
                                let crop = 1.0_f64;
                                let fx = fl / sensor_w as f64 * resolution_w as f64 * crop;
                                if fx > 0.0 {
                                    util::insert_tag(map, tag!(parsed GroupId::Lens, TagId::PixelFocalLength, "Pixel focal length", f32, |v| format!("{:.2}", v), fx as f32, Vec::new()), options);
                                }
                            }
                        }
                    }

                    // Crop (Leica has no crop rules)
                    let tags = std::collections::HashMap::new();
                    let _effective_crop = db.process_crop("LEICA", model_name, resolution_w, resolution_h, fps, None, &tags, map, options);

                    // Readout
                    if self.frame_readout_time.is_none() {
                        if let Some(rt) = db.process_readout("LEICA", model_name, resolution_w, resolution_h, fps, 0.0, sensor_w, &tags, map, options) {
                            self.frame_readout_time = Some(rt);
                        }
                    }

                    // Creation date
                    if let Some(ref dt) = exif.datetime_original {
                        util::write_creation_date_tags(map, dt, exif.offset_time_original.as_deref(), exif.subsec_time_original.as_deref().or(Some("500")), options);
                    } else if let Some(ref mvhd_time) = self.mvhd_creation_time {
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
        if let Some(ref dt) = exif.datetime_original {
            util::write_creation_date_tags(map, dt, exif.offset_time_original.as_deref(), exif.subsec_time_original.as_deref().or(Some("500")), options);
        } else if let Some(ref mvhd_time) = self.mvhd_creation_time {
            util::write_creation_date_tags(map, mvhd_time, None, Some("500"), options);
        }
    }
}

// ---- LEIC atom content extraction ----

/// Extract metadata from the LEIC atom content.
/// The LEIC atom typically contains:
/// - "LEICA CAMERA AG\0" (Make string)
/// - At some offset, the model string like "LEICA SL (Typ 601)\0"
/// - May contain embedded TIFF/EXIF data with focal length
fn extract_from_leic(data: &[u8]) -> LeicaExifData {
    let mut result = LeicaExifData::default();

    // Extract model: search for "LEICA " occurrences in the data
    // The first is typically the Make ("LEICA CAMERA AG"), the second is the Model
    let mut search_start = 0;
    let mut occurrences = Vec::new();
    while let Some(pos) = memmem::find(&data[search_start..], b"LEICA ") {
        let abs_pos = search_start + pos;
        // Read null-terminated string from this position
        let str_data = &data[abs_pos..];
        let end = str_data.iter().position(|&b| b == 0).unwrap_or(str_data.len());
        let s = String::from_utf8_lossy(&str_data[..end]).to_string();
        occurrences.push(s);
        search_start = abs_pos + 6; // skip past "LEICA "
        if search_start >= data.len() { break; }
    }

    // The model string is typically the second occurrence (first is "LEICA CAMERA AG")
    // or the one that looks like a camera model name
    for s in &occurrences {
        if s.contains("CAMERA AG") { continue; }
        // This should be the model string
        result.model = Some(s.clone());
        break;
    }

    // If we only found one occurrence and it's not "CAMERA AG", use it
    if result.model.is_none() && !occurrences.is_empty() {
        result.model = Some(occurrences[0].clone());
    }

    // Search for TIFF magic to find embedded EXIF data
    for i in 0..data.len().saturating_sub(8) {
        if is_tiff_header(&data[i..]) {
            if let Ok(exif) = parse_tiff_for_focal_length(&data[i..]) {
                result.focal_length = exif.focal_length;
                result.lens_model = exif.lens_model;
                result.datetime_original = exif.datetime_original;
                result.offset_time_original = exif.offset_time_original;
                result.subsec_time_original = exif.subsec_time_original;
                break;
            }
        }
    }

    // Also try finding EXIF via JPEG APP1 marker
    if result.focal_length.is_none() {
        if let Some(tiff_slice) = extract_tiff_from_jpeg(data) {
            if let Ok(exif) = parse_tiff_for_focal_length(tiff_slice) {
                if result.focal_length.is_none() { result.focal_length = exif.focal_length; }
                if result.lens_model.is_none() { result.lens_model = exif.lens_model; }
                if result.datetime_original.is_none() { result.datetime_original = exif.datetime_original; }
                if result.offset_time_original.is_none() { result.offset_time_original = exif.offset_time_original; }
                if result.subsec_time_original.is_none() { result.subsec_time_original = exif.subsec_time_original; }
            }
        }
    }

    result
}

// ---- Minimal TIFF IFD Parser (for focal length extraction) ----

#[derive(Debug, Default)]
struct TiffExifResult {
    focal_length: Option<f64>,
    lens_model: Option<String>,
    datetime_original: Option<String>,
    offset_time_original: Option<String>,
    subsec_time_original: Option<String>,
}

fn parse_tiff_for_focal_length(tiff_data: &[u8]) -> Result<TiffExifResult> {
    if tiff_data.len() < 8 {
        return Err(Error::new(ErrorKind::InvalidData, "TIFF data too short"));
    }

    let is_le = match (tiff_data[0], tiff_data[1]) {
        (b'I', b'I') => true,
        (b'M', b'M') => false,
        _ => return Err(Error::new(ErrorKind::InvalidData, "Invalid TIFF byte order")),
    };

    let magic = read_u16(tiff_data, 2, is_le);
    if magic != Some(42) {
        return Err(Error::new(ErrorKind::InvalidData, "Invalid TIFF magic number"));
    }

    let ifd0_offset = match read_u32(tiff_data, 4, is_le) {
        Some(o) => o as usize,
        None => return Err(Error::new(ErrorKind::InvalidData, "Cannot read IFD0 offset")),
    };

    let mut result = TiffExifResult::default();
    let mut exif_ifd_offset = None;

    parse_ifd_entries(tiff_data, ifd0_offset, is_le, &mut |tag, _typ, _count, _value_data, _value_offset| {
        if tag == 0x8769 { // ExifIFD offset
            exif_ifd_offset = read_u32(tiff_data, _value_offset, is_le).map(|v| v as usize);
        }
    });

    // Parse ExifIFD for focal length and lens model
    if let Some(offset) = exif_ifd_offset {
        parse_ifd_entries(tiff_data, offset, is_le, &mut |tag, _typ, count, value_data, _value_offset| {
            match tag {
                0x920A => { // FocalLength (RATIONAL)
                    if value_data.len() >= 8 {
                        if let (Some(num), Some(den)) = (read_u32(value_data, 0, is_le), read_u32(value_data, 4, is_le)) {
                            if den > 0 {
                                result.focal_length = Some(num as f64 / den as f64);
                            }
                        }
                    }
                }
                0x9003 => { // DateTimeOriginal
                    result.datetime_original = Some(read_string(value_data, count));
                }
                0x9011 => { // OffsetTimeOriginal
                    result.offset_time_original = Some(read_string(value_data, count));
                }
                0x9291 => { // SubSecTimeOriginal
                    result.subsec_time_original = Some(read_string(value_data, count));
                }
                0xA434 => { // LensModel
                    result.lens_model = Some(read_string(value_data, count));
                }
                _ => {}
            }
        });
    }

    Ok(result)
}

