// SPDX-License-Identifier: MIT OR Apache-2.0

//! Panasonic/Lumix camera format parser.
//! Extracts metadata from MOV/MP4 files via PANA atom in moov/udta.
//! The PANA atom contains a proprietary header followed by TIFF IFD data
//! with EXIF and Panasonic MakerNotes.

use std::io::*;
use std::sync::{ Arc, atomic::AtomicBool };

use crate::*;
use crate::tags_impl::*;
use crate::tiff_ifd::{is_tiff_header, read_u16, read_u32, read_string, parse_ifd_entries};
use memchr::memmem;


#[derive(Default)]
pub struct Panasonic {
    pub model: Option<String>,
    pub lens: Option<String>,
    frame_readout_time: Option<f64>,
    mvhd_creation_time: Option<String>,
}

/// Data extracted from Panasonic TIFF/EXIF metadata.
#[derive(Debug, Default)]
struct PanasonicExifData {
    model: Option<String>,
    focal_length: Option<f64>,
    lens_model: Option<String>,
    focal_length_35mm: Option<u32>,
    dr_boost: bool,
    datetime_original: Option<String>,
    offset_time_original: Option<String>,
    subsec_time_original: Option<String>,
    image_stabilization: Option<u16>,
    record_frame_rate: Option<u16>,
}

impl Panasonic {
    pub fn camera_type(&self) -> String {
        "Panasonic".to_owned()
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
        // Detect via: "Panasonic" string, "PANA" atom, or "pana" ftyp compatible brand
        if memmem::find(buffer, b"Panasonic").is_some()
            || memmem::find(buffer, b"PANA").is_some()
            || (memmem::find(buffer, b"ftyp").is_some() && memmem::find(buffer, b"pana").is_some())
        {
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

        // Parse PANA atom from moov/udta
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

    /// Parse MOV/MP4: find moov/udta/PANA atom containing TIFF IFD.
    fn parse_mov<T: Read + Seek>(&mut self, stream: &mut T, _size: usize) -> Result<PanasonicExifData> {
        stream.seek(SeekFrom::Start(0))?;

        // First find moov atom
        let moov_data = self.find_moov_data(stream)?;
        let mut cursor = Cursor::new(moov_data.as_slice());
        let moov_len = moov_data.len() as u64;

        // Search inside moov for udta, then PANA
        while cursor.position() < moov_len {
            let (typ, _offs, size, header_size) = util::read_box(&mut cursor)?;
            let content_size = size as i64 - header_size;
            if content_size <= 0 { break; }

            if typ == util::fourcc("udta") {
                let start = cursor.position() as usize;
                let end = start + content_size as usize;
                if end <= moov_data.len() {
                    if let Some(result) = self.find_pana_in_data(&moov_data[start..end]) {
                        return Ok(result);
                    }
                }
            }
            cursor.seek(SeekFrom::Current(content_size))?;
        }

        // Fallback: search entire moov data for PANA
        if let Some(result) = self.find_pana_in_data(&moov_data) {
            return Ok(result);
        }

        Err(Error::new(ErrorKind::NotFound, "PANA atom not found in MOV"))
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

    fn find_pana_in_data(&self, data: &[u8]) -> Option<PanasonicExifData> {
        let mut cursor = Cursor::new(data);
        let len = data.len() as u64;
        while cursor.position() < len {
            let (typ, _offs, size, header_size) = util::read_box(&mut cursor).ok()?;
            let content_size = size as i64 - header_size;
            if content_size < 0 { break; }
            if content_size == 0 { continue; }

            if typ == util::fourcc("PANA") {
                let start = cursor.position() as usize;
                let end = start + content_size as usize;
                if end <= data.len() {
                    let pana_data = &data[start..end];
                    if let Some(result) = extract_exif_from_pana(pana_data) {
                        return Some(result);
                    }
                }
            }
            cursor.seek(SeekFrom::Current(content_size)).ok()?;
        }
        None
    }

    fn process_map(&mut self, map: &mut GroupedTagMap, options: &crate::InputOptions, video_md: Option<&VideoMetadata>, exif: &PanasonicExifData) {
        let raw_model = exif.model.as_deref().unwrap_or("");
        let resolution_w = video_md.map(|v| v.width as u32).unwrap_or(0);
        let resolution_h = video_md.map(|v| v.height as u32).unwrap_or(0);
        let fps = video_md.map(|v| v.fps).unwrap_or(0.0);

        // Write video resolution
        if resolution_w > 0 {
            util::insert_tag(map, tag!(parsed GroupId::Default, TagId::Custom("video_width".into()), "Video output width", u32, |v| format!("{} px", v), resolution_w, Vec::new()), options);
            util::insert_tag(map, tag!(parsed GroupId::Default, TagId::Custom("video_height".into()), "Video output height", u32, |v| format!("{} px", v), resolution_h, Vec::new()), options);
        }

        // Lens and focal length
        if let Some(ref lens_name) = exif.lens_model {
            if !lens_name.is_empty() {
                self.lens = Some(lens_name.clone());
                util::insert_tag(map, tag!(parsed GroupId::Lens, TagId::DisplayName, "Lens name", String, |v| v.clone(), lens_name.clone(), Vec::new()), options);
            }
        }

        if let Some(fl) = exif.focal_length {
            if fl > 0.0 {
                util::insert_tag(map, tag!(parsed GroupId::Lens, TagId::FocalLength, "Focal length", f32, |v| format!("{:.1} mm", v), fl as f32, Vec::new()), options);
            }
        }

        // scale_35mm = focal_length_35mm / focal_length
        let scale_35mm = match (exif.focal_length_35mm, exif.focal_length) {
            (Some(fl35), Some(fl)) if fl35 > 0 && fl > 0.0 => Some(fl35 as f64 / fl),
            _ => None,
        };

        if let Some(scale) = scale_35mm {
            util::insert_tag(map, tag!(parsed GroupId::Default, TagId::Custom("scale_35mm".into()), "35mm equivalent scale", f64, |v| format!("{:.2}", v), scale, Vec::new()), options);
        }

        // Write DynamicRangeBoost
        if exif.dr_boost {
            util::insert_tag(map, tag!(parsed GroupId::Default, TagId::Custom("DynamicRangeBoost".into()), "Dynamic Range Boost", bool, |v| v.to_string(), true, Vec::new()), options);
        }

        // Try JSON database first
        if let Some(db_path) = &options.camera_db_path {
            if let Ok(db) = crate::camera_db::CameraDatabase::load(db_path) {
                if let Some((model_name, model_data)) = db.process_model("LUMIX", raw_model, map, options) {
                    self.model = Some(model_name.to_string());
                    let sensor_w = model_data.sw;

                    // Crop factor: use scale_35mm if significantly > 1.0, else JSON crop rules
                    let crop_factor = if let Some(scale) = scale_35mm.filter(|s| (*s - 1.0).abs() > 0.01) {
                        util::insert_tag(map, tag!(parsed GroupId::Default, TagId::Custom("crop_factor".into()), "Crop factor", f64, |v| format!("{:.4}", v), scale, Vec::new()), options);
                        scale
                    } else {
                        let tags = std::collections::HashMap::new();
                        db.process_crop("LUMIX", model_name, resolution_w, resolution_h, fps, None, &tags, map, options)
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
                    let fl = exif.focal_length.or(options.user_focal_length);
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

                    // Readout: build tags with dr_boost
                    if self.frame_readout_time.is_none() {
                        let mut tags = std::collections::HashMap::new();
                        if exif.dr_boost {
                            tags.insert("dr_boost".to_string(), serde_json::Value::Bool(true));
                        }
                        let s35 = scale_35mm.unwrap_or(0.0);
                        if let Some(rt) = db.process_readout("LUMIX", model_name, resolution_w, resolution_h, fps, s35, sensor_w, &tags, map, options) {
                            self.frame_readout_time = Some(rt);
                        }
                    }

                    // Creation date
                    if let Some(ref dt) = exif.datetime_original {
                        util::write_creation_date_tags(map, dt, exif.offset_time_original.as_deref(), exif.subsec_time_original.as_deref().or(Some("500")), options);
                    } else if let Some(ref mvhd_time) = self.mvhd_creation_time {
                        util::write_creation_date_tags(map, mvhd_time, None, Some("500"), options);
                    }

                    // Image stabilization: 2=On(Mode1), 3=Off, 4=On(Mode2), 5=Panning (default false)
                    let is_on = exif.image_stabilization.map(|v| v != 0 && v != 3).unwrap_or(false);
                    util::insert_tag(map, tag!(parsed GroupId::Default, TagId::ImageStabilizer, "Image stabilization", bool, |v| if *v { "On" } else { "Off" }.into(), is_on, Vec::new()), options);

                    // Frame rates: always output both playback and recording fps
                    let playback_fps = fps;
                    let record_fps = exif.record_frame_rate.map(|v| v as f64).unwrap_or(playback_fps);
                    if playback_fps > 0.0 {
                        util::insert_tag(map, tag!(parsed GroupId::Default, TagId::FrameRate, "Frame rate", f64, |v| format!("{:.3} fps", v), playback_fps, Vec::new()), options);
                        util::insert_tag(map, tag!(parsed GroupId::Default, TagId::RecordFrameRate, "Record frame rate", f64, |v| format!("{:.3} fps", v), record_fps, Vec::new()), options);
                    }

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

// ---- PANA atom content extraction ----

/// Extract EXIF data from PANA atom content.
/// The PANA atom has a proprietary header; we search for TIFF magic within it.
fn extract_exif_from_pana(pana_data: &[u8]) -> Option<PanasonicExifData> {
    if pana_data.len() < 8 { return None; }

    // Search for TIFF header (II*\0) within the PANA data
    for i in 0..pana_data.len().saturating_sub(8) {
        if is_tiff_header(&pana_data[i..]) {
            if let Ok(result) = parse_tiff_ifd(&pana_data[i..]) {
                return Some(result);
            }
        }
    }

    None
}

// ---- TIFF IFD Parser ----

fn parse_tiff_ifd(tiff_data: &[u8]) -> Result<PanasonicExifData> {
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

    let mut result = PanasonicExifData::default();
    let mut exif_ifd_offset = None;

    parse_ifd_entries(tiff_data, ifd0_offset, is_le, &mut |tag, _typ, count, value_data, value_offset| {
        match tag {
            0x010F => { // Make
                // Skip, we already know it's Panasonic
            }
            0x0110 => { // Model
                result.model = Some(read_string(value_data, count));
            }
            0x8769 => { // ExifIFD offset
                exif_ifd_offset = read_u32(tiff_data, value_offset, is_le).map(|v| v as usize);
            }
            _ => {}
        }
    });

    // Parse ExifIFD
    if let Some(offset) = exif_ifd_offset {
        let mut makernotes_offset = None;

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
                0xA434 => { // LensModel
                    result.lens_model = Some(read_string(value_data, count));
                }
                0xA405 => { // FocalLengthIn35mmFormat
                    if _typ == 3 && value_data.len() >= 2 {
                        if let Some(v) = read_u16(value_data, 0, is_le) {
                            result.focal_length_35mm = Some(v as u32);
                        }
                    } else if _typ == 4 && value_data.len() >= 4 {
                        if let Some(v) = read_u32(value_data, 0, is_le) {
                            result.focal_length_35mm = Some(v);
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
                0x927C => { // MakerNotes offset — value_data contains the raw MakerNotes
                    // For Panasonic, MakerNotes start with "Panasonic\0" (10 bytes)
                    // then parse as IFD
                    if value_data.len() > 12 && &value_data[..9] == b"Panasonic" {
                        // MakerNotes data is self-contained; offsets are relative to MakerNotes start
                        makernotes_offset = Some((_value_offset, count));
                    }
                }
                _ => {}
            }
        });

        // Parse Panasonic MakerNotes for DynamicRangeBoost (tag 0x00fb)
        if let Some((mn_data_offset, mn_count)) = makernotes_offset {
            // The MakerNotes data pointer: value_data in the callback was already the data
            // But we need to re-read from tiff_data at the offset
            if mn_data_offset + mn_count <= tiff_data.len() {
                let mn_data = &tiff_data[mn_data_offset..mn_data_offset + mn_count];
                if mn_data.len() > 12 && &mn_data[..9] == b"Panasonic" {
                    // Skip "Panasonic\0" header (10 bytes), then padding bytes
                    // The IFD starts after the header; Panasonic MakerNotes use absolute offsets from TIFF start
                    let header_len = 12; // "Panasonic\0" + 2 padding bytes
                    if mn_data.len() > header_len {
                        // Panasonic MakerNotes IFD entries with offsets relative to TIFF start
                        let ifd_offset = mn_data_offset + header_len;
                        parse_ifd_entries(tiff_data, ifd_offset, is_le, &mut |tag, typ, count, value_data, _value_offset| {
                            match tag {
                                0x0051 => { // LensType
                                    if result.lens_model.is_none() {
                                        let s = read_string(value_data, count);
                                        if !s.is_empty() && s != "NO-LENS" && s != "N/A" {
                                            result.lens_model = Some(s);
                                        }
                                    }
                                }
                                0x001A => { // ImageStabilization: u16
                                    // 2=On(Mode1), 3=Off, 4=On(Mode2), 5=Panning
                                    if typ == 3 && value_data.len() >= 2 {
                                        if let Some(v) = read_u16(value_data, 0, is_le) {
                                            result.image_stabilization = Some(v);
                                        }
                                    }
                                }
                                0x0027 => { // VideoFrameRate: u16 (older models only)
                                    if typ == 3 && value_data.len() >= 2 {
                                        if let Some(v) = read_u16(value_data, 0, is_le) {
                                            result.record_frame_rate = Some(v);
                                        }
                                    }
                                }
                                0x00FB => { // DynamicRangeBoost: u16, 1=ON
                                    if typ == 3 && value_data.len() >= 2 {
                                        if let Some(v) = read_u16(value_data, 0, is_le) {
                                            result.dr_boost = v == 1;
                                        }
                                    }
                                }
                                _ => {}
                            }
                        });
                    }
                }
            }
        }
    }

    Ok(result)
}

