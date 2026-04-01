// SPDX-License-Identifier: MIT OR Apache-2.0

//! Sigma camera format parser.
//! Extracts metadata from MOV/MP4 files (via SIGM atom containing TIFF IFD)
//! and from DNG files (which are TIFF files directly).

use std::io::*;
use std::sync::{ Arc, atomic::AtomicBool };

use crate::*;
use crate::tags_impl::*;
use crate::tiff_ifd::{is_tiff_header, read_u16, read_u32, read_string, parse_ifd_entries, extract_tiff_from_jpeg};
use memchr::memmem;


#[derive(Default)]
pub struct Sigma {
    pub model: Option<String>,
    pub lens: Option<String>,
    frame_readout_time: Option<f64>,
    mvhd_creation_time: Option<String>,
}

/// Data extracted from Sigma TIFF/EXIF metadata.
#[derive(Debug, Default)]
struct SigmaExifData {
    model: Option<String>,
    focal_length: Option<f64>,
    lens_model: Option<String>,
    focal_length_35mm: Option<u32>,
    default_crop_size: Option<(u32, u32)>,
    default_scale: Option<f64>,
    record_frame_rate: Option<f64>,
    datetime_original: Option<String>,
    offset_time_original: Option<String>,
    subsec_time_original: Option<String>,
}

impl Sigma {
    pub fn camera_type(&self) -> String {
        "Sigma".to_owned()
    }
    pub fn has_accurate_timestamps(&self) -> bool {
        false
    }
    pub fn possible_extensions() -> Vec<&'static str> {
        vec!["mp4", "mov", "dng"]
    }
    pub fn frame_readout_time(&self) -> Option<f64> {
        self.frame_readout_time
    }
    pub fn normalize_imu_orientation(v: String) -> String {
        v
    }

    pub fn detect<P: AsRef<std::path::Path>>(buffer: &[u8], _filepath: P, _options: &crate::InputOptions) -> Option<Self> {
        if memmem::find(buffer, b"SIGM").is_some() || memmem::find(buffer, b"SIGMA").is_some() {
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

        // Determine if this is a DNG file by checking for TIFF header at start
        stream.seek(SeekFrom::Start(0))?;
        let mut header = [0u8; 4];
        stream.read_exact(&mut header)?;
        stream.seek(SeekFrom::Start(0))?;

        let is_dng = (header[0] == b'I' && header[1] == b'I' && header[2] == 0x2a && header[3] == 0x00)
                  || (header[0] == b'M' && header[1] == b'M' && header[2] == 0x00 && header[3] == 0x2a);

        let exif_data = if is_dng {
            self.parse_dng(stream, size)?
        } else {
            self.parse_mov(stream, size)?
        };

        // Get video track metadata (resolution, fps) — only for MOV/MP4
        let video_md = if !is_dng {
            stream.seek(SeekFrom::Start(0))?;
            util::get_video_metadata(stream, size).ok()
        } else {
            None
        };

        self.process_map(&mut first_map, &options, video_md.as_ref(), &exif_data, is_dng);

        samples.push(SampleInfo {
            tag_map: Some(first_map),
            ..Default::default()
        });

        Ok(samples)
    }

    /// Parse MOV/MP4: find moov/udta/SIGM atom containing TIFF IFD.
    fn parse_mov<T: Read + Seek>(&mut self, stream: &mut T, _size: usize) -> Result<SigmaExifData> {
        stream.seek(SeekFrom::Start(0))?;

        // First find moov atom
        let moov_data = self.find_moov_data(stream)?;
        let mut cursor = Cursor::new(moov_data.as_slice());
        let moov_len = moov_data.len() as u64;

        // Search inside moov for udta, or directly for SIGM
        let mut found_in_udta = None;
        while cursor.position() < moov_len {
            let (typ, _offs, size, header_size) = util::read_box(&mut cursor)?;
            let content_size = size as i64 - header_size;
            if content_size <= 0 { break; }

            if typ == util::fourcc("udta") {
                let start = cursor.position() as usize;
                let end = start + content_size as usize;
                if end <= moov_data.len() {
                    if let Some(result) = self.find_sigm_in_data(&moov_data[start..end]) {
                        found_in_udta = Some(result);
                        break;
                    }
                }
            }
            cursor.seek(SeekFrom::Current(content_size))?;
        }

        if let Some(result) = found_in_udta {
            return Ok(result);
        }

        // Fallback: search entire moov data for SIGM
        if let Some(result) = self.find_sigm_in_data(&moov_data) {
            return Ok(result);
        }

        Err(Error::new(ErrorKind::NotFound, "SIGM atom not found in MOV"))
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

    fn find_sigm_in_data(&self, data: &[u8]) -> Option<SigmaExifData> {
        let mut cursor = Cursor::new(data);
        let len = data.len() as u64;
        while cursor.position() < len {
            let (typ, _offs, size, header_size) = util::read_box(&mut cursor).ok()?;
            let content_size = size as i64 - header_size;
            if content_size < 0 { break; }
            if content_size == 0 { continue; }

            if typ == util::fourcc("SIGM") {
                let start = cursor.position() as usize;
                let end = start + content_size as usize;
                if end <= data.len() {
                    let sigm_data = &data[start..end];
                    if let Some(result) = extract_exif_from_sigm(sigm_data) {
                        return Some(result);
                    }
                }
            }
            cursor.seek(SeekFrom::Current(content_size)).ok()?;
        }
        None
    }

    /// Parse DNG: read TIFF IFD from file start.
    fn parse_dng<T: Read + Seek>(&mut self, stream: &mut T, size: usize) -> Result<SigmaExifData> {
        // Read enough of the file for TIFF IFD parsing (up to 1MB should be plenty)
        let read_size = size.min(1024 * 1024);
        stream.seek(SeekFrom::Start(0))?;
        let mut data = vec![0u8; read_size];
        stream.read_exact(&mut data)?;
        parse_tiff_ifd(&data)
    }

    fn process_map(&mut self, map: &mut GroupedTagMap, options: &crate::InputOptions, video_md: Option<&VideoMetadata>, exif: &SigmaExifData, is_dng: bool) {
        let raw_model = exif.model.as_deref().unwrap_or("");

        // For DNG, use DefaultCropSize as resolution; for MOV use video track
        let (resolution_w, resolution_h) = if is_dng {
            exif.default_crop_size.unwrap_or((0, 0))
        } else {
            (
                video_md.map(|v| v.width as u32).unwrap_or(0),
                video_md.map(|v| v.height as u32).unwrap_or(0),
            )
        };
        let fps = video_md.map(|v| v.fps)
            .or(exif.record_frame_rate) // DNG: use CinemaDNG FrameRate tag
            .unwrap_or(0.0);

        // Write video/image resolution
        if resolution_w > 0 {
            util::insert_tag(map, tag!(parsed GroupId::Default, TagId::Custom("video_width".into()), "Video output width", u32, |v| format!("{} px", v), resolution_w, Vec::new()), options);
            util::insert_tag(map, tag!(parsed GroupId::Default, TagId::Custom("video_height".into()), "Video output height", u32, |v| format!("{} px", v), resolution_h, Vec::new()), options);
        }

        // DNG scale tag
        if is_dng {
            if let Some(scale) = exif.default_scale {
                util::insert_tag(map, tag!(parsed GroupId::Default, TagId::Custom("dng_scale".into()), "DNG default scale", f64, |v| format!("{:.6}", v), scale, Vec::new()), options);
            }
        }

        // Lens and focal length
        let has_lens = exif.lens_model.as_ref().map_or(false, |l| !l.is_empty());
        if let Some(ref lens_name) = exif.lens_model {
            if !lens_name.is_empty() {
                self.lens = Some(lens_name.clone());
                util::insert_tag(map, tag!(parsed GroupId::Lens, TagId::DisplayName, "Lens name", String, |v| v.clone(), lens_name.clone(), Vec::new()), options);
            }
        }

        if has_lens {
            if let Some(fl) = exif.focal_length {
                util::insert_tag(map, tag!(parsed GroupId::Lens, TagId::FocalLength, "Focal length", f32, |v| format!("{:.1} mm", v), fl as f32, Vec::new()), options);
            }
        }

        // scale_35mm = focal_length_35mm / focal_length
        let scale_35mm = match (exif.focal_length_35mm, exif.focal_length) {
            (Some(fl35), Some(fl)) if fl35 > 0 && fl > 0.0 => Some(fl35 as f64 / fl),
            _ => None,
        };

        // Try JSON database first
        if let Some(db_path) = &options.camera_db_path {
            if let Ok(db) = crate::camera_db::CameraDatabase::load(db_path) {
                if let Some((model_name, model_data)) = db.process_model("SIGMA", raw_model, map, options) {
                    self.model = Some(model_name.to_string());
                    let sensor_w = model_data.sw;

                    // Crop factor: use scale_35mm if significantly > 1.0, else JSON crop rules
                    let crop_factor = if let Some(scale) = scale_35mm.filter(|s| (*s - 1.0).abs() > 0.01) {
                        util::insert_tag(map, tag!(parsed GroupId::Default, TagId::Custom("crop_factor".into()), "Crop factor", f64, |v| format!("{:.4}", v), scale, Vec::new()), options);
                        scale
                    } else {
                        let tags = std::collections::HashMap::new();
                        db.process_crop("SIGMA", model_name, resolution_w, resolution_h, fps, None, &tags, map, options)
                    };

                    // Unit pixel focal length
                    if resolution_w > 0 {
                        let unit_px_fl = if let Some(scale) = scale_35mm.filter(|s| (s - 1.0).abs() > 0.01) {
                            scale / 36.0 * resolution_w as f64
                        } else {
                            resolution_w as f64 * crop_factor / sensor_w as f64
                        };
                        if unit_px_fl > 0.0 {
                            util::insert_tag(map, tag!(parsed GroupId::Lens, TagId::Custom("unit_pixel_focal_length".into()), "Pixel focal length per mm", f64, |v| format!("{:.4}", v), unit_px_fl, Vec::new()), options);
                        }
                    }

                    // Pixel focal length
                    {
                        let fl = if has_lens { exif.focal_length } else { None }.or(options.user_focal_length);
                        if let Some(fl) = fl {
                            if resolution_w > 0 {
                                let fx = if let Some(scale) = scale_35mm.filter(|s| (s - 1.0).abs() > 0.01) {
                                    fl * scale / 36.0 * resolution_w as f64
                                } else {
                                    util::calc_pixel_focal_length(fl, sensor_w, resolution_w, crop_factor)
                                };
                                if fx > 0.0 {
                                    util::insert_tag(map, tag!(parsed GroupId::Lens, TagId::PixelFocalLength, "Pixel focal length", f32, |v| format!("{:.2}", v), fx as f32, Vec::new()), options);
                                }
                            }
                        }
                    }

                    // Readout
                    if self.frame_readout_time.is_none() {
                        let lookup_fps = if fps < 1.0 { 24.0 } else { fps };
                        let tags = std::collections::HashMap::new();
                        if let Some(rt) = db.process_readout("SIGMA", model_name, resolution_w, resolution_h, lookup_fps, 0.0, sensor_w, &tags, map, options) {
                            self.frame_readout_time = Some(rt);
                        }
                    }

                    // Creation date
                    // Sigma fp/fpl MOV: EXIF DateTimeOriginal is recording END time, use mvhd (start time) instead
                    let is_fp_mov = !is_dng && exif.model.as_deref().unwrap_or("").to_lowercase().contains("fp");
                    if is_fp_mov {
                        if let Some(ref mvhd_time) = self.mvhd_creation_time {
                            // mvhd is UTC; convert to local using EXIF timezone, then write with TZ
                            if let Some(ref tz) = exif.offset_time_original {
                                if let Some(local_str) = util::calculate_local_from_utc(mvhd_time, tz) {
                                    util::write_creation_date_tags(map, &local_str, Some(tz), exif.subsec_time_original.as_deref().or(Some("500")), options);
                                } else {
                                    util::write_creation_date_tags(map, mvhd_time, None, Some("500"), options);
                                }
                            } else {
                                util::write_creation_date_tags(map, mvhd_time, None, Some("500"), options);
                            }
                        } else if let Some(ref dt) = exif.datetime_original {
                            util::write_creation_date_tags(map, dt, exif.offset_time_original.as_deref(), exif.subsec_time_original.as_deref().or(Some("500")), options);
                        }
                    } else if let Some(ref dt) = exif.datetime_original {
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
        let is_fp_mov = !is_dng && exif.model.as_deref().unwrap_or("").to_lowercase().contains("fp");
        if is_fp_mov {
            if let Some(ref mvhd_time) = self.mvhd_creation_time {
                if let Some(ref tz) = exif.offset_time_original {
                    if let Some(local_str) = util::calculate_local_from_utc(mvhd_time, tz) {
                        util::write_creation_date_tags(map, &local_str, Some(tz), exif.subsec_time_original.as_deref().or(Some("500")), options);
                    } else {
                        util::write_creation_date_tags(map, mvhd_time, None, Some("500"), options);
                    }
                } else {
                    util::write_creation_date_tags(map, mvhd_time, None, Some("500"), options);
                }
            } else if let Some(ref dt) = exif.datetime_original {
                util::write_creation_date_tags(map, dt, exif.offset_time_original.as_deref(), exif.subsec_time_original.as_deref().or(Some("500")), options);
            }
        } else if let Some(ref dt) = exif.datetime_original {
            util::write_creation_date_tags(map, dt, exif.offset_time_original.as_deref(), exif.subsec_time_original.as_deref().or(Some("500")), options);
        } else if let Some(ref mvhd_time) = self.mvhd_creation_time {
            util::write_creation_date_tags(map, mvhd_time, None, Some("500"), options);
        }
    }
}

// ---- SIGM content extraction ----

/// Extract EXIF data from SIGM atom content.
/// The content may be raw TIFF (II*\0 or MM\0*) or contain an embedded JPEG with EXIF APP1.
fn extract_exif_from_sigm(sigm_data: &[u8]) -> Option<SigmaExifData> {
    if sigm_data.len() < 8 { return None; }

    // Case 1: Raw TIFF header at start
    if is_tiff_header(sigm_data) {
        return parse_tiff_ifd(sigm_data).ok();
    }

    // Case 2: Search for JPEG SOI (0xFFD8) followed by EXIF APP1
    if let Some(tiff_slice) = extract_tiff_from_jpeg(sigm_data) {
        return parse_tiff_ifd(tiff_slice).ok();
    }

    // Case 3: Search for TIFF header anywhere in the data
    for i in 0..sigm_data.len().saturating_sub(8) {
        if is_tiff_header(&sigm_data[i..]) {
            return parse_tiff_ifd(&sigm_data[i..]).ok();
        }
    }

    None
}


// ---- TIFF IFD Parser ----

fn parse_tiff_ifd(tiff_data: &[u8]) -> Result<SigmaExifData> {
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

    let mut result = SigmaExifData::default();
    let mut exif_ifd_offset = None;

    parse_ifd_entries(tiff_data, ifd0_offset, is_le, &mut |tag, _typ, count, value_data, value_offset| {
        match tag {
            0x0110 => { // Model
                result.model = Some(read_string(value_data, count));
            }
            0x8769 => { // ExifIFD offset
                exif_ifd_offset = read_u32(tiff_data, value_offset, is_le).map(|v| v as usize);
            }
            0xc620 => { // DefaultCropSize (DNG)
                // Can be LONG (type 4) or RATIONAL (type 5)
                if _typ == 4 && count >= 2 && value_data.len() >= 8 {
                    // Two LONGs
                    if let (Some(w), Some(h)) = (read_u32(value_data, 0, is_le), read_u32(value_data, 4, is_le)) {
                        result.default_crop_size = Some((w, h));
                    }
                } else if _typ == 5 && count >= 2 && value_data.len() >= 16 {
                    // Two RATIONALs
                    if let (Some(wn), Some(wd), Some(hn), Some(hd)) = (
                        read_u32(value_data, 0, is_le),
                        read_u32(value_data, 4, is_le),
                        read_u32(value_data, 8, is_le),
                        read_u32(value_data, 12, is_le),
                    ) {
                        let w = if wd > 0 { wn / wd } else { wn };
                        let h = if hd > 0 { hn / hd } else { hn };
                        result.default_crop_size = Some((w, h));
                    }
                }
            }
            0xc61d => { // DefaultScale (DNG)
                if _typ == 5 && value_data.len() >= 8 {
                    if let (Some(num), Some(den)) = (read_u32(value_data, 0, is_le), read_u32(value_data, 4, is_le)) {
                        if den > 0 {
                            result.default_scale = Some(num as f64 / den as f64);
                        }
                    }
                }
            }
            0xc764 => { // FrameRate (CinemaDNG, rational64s: num/den)
                if value_data.len() >= 8 {
                    if let (Some(num), Some(den)) = (read_u32(value_data, 0, is_le), read_u32(value_data, 4, is_le)) {
                        if den > 0 {
                            result.record_frame_rate = Some(num as f64 / den as f64);
                        }
                    }
                }
            }
            _ => {}
        }
    });

    // Parse ExifIFD
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
                0xA405 => { // FocalLengthIn35mmFormat
                    if _typ == 3 && value_data.len() >= 2 {
                        // SHORT
                        if let Some(v) = read_u16(value_data, 0, is_le) {
                            result.focal_length_35mm = Some(v as u32);
                        }
                    } else if _typ == 4 && value_data.len() >= 4 {
                        // LONG
                        if let Some(v) = read_u32(value_data, 0, is_le) {
                            result.focal_length_35mm = Some(v);
                        }
                    }
                }
                _ => {}
            }
        });
    }

    Ok(result)
}

