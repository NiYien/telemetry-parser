// SPDX-License-Identifier: MIT OR Apache-2.0

//! Fujifilm camera format parser.
//! Extracts metadata from MOV/MP4 files via MVTG atom in moov/udta.
//! The MVTG atom contains TIFF-like IFD structure with Make and Model tags.
//! MinFocalLength/MaxFocalLength extracted from MakerNotes (0x1404/0x1405) or LensInfo (0xa432).
//! When MinFL == MaxFL (prime lens), the value is used as confirmed focal length.

use std::io::*;
use std::sync::{ Arc, atomic::AtomicBool };

use crate::*;
use crate::tags_impl::*;
use crate::tiff_ifd::{is_tiff_header, read_u16, read_u32, read_string, parse_ifd_entries};
use memchr::memmem;


#[derive(Default)]
pub struct Fujifilm {
    pub model: Option<String>,
    pub lens: Option<String>,
    frame_readout_time: Option<f64>,
    mvhd_creation_time: Option<String>,
}

/// Data extracted from Fujifilm MVTG/TIFF metadata.
#[derive(Debug, Default)]
struct FujiExifData {
    model: Option<String>,
    min_focal_length: Option<f64>,
    max_focal_length: Option<f64>,
    min_aperture: Option<f64>,
    max_aperture: Option<f64>,
    datetime_original: Option<String>,
    offset_time_original: Option<String>,
    pub subsec_time_original: Option<String>,
    record_frame_rate: Option<u16>,
    slow_motion: Option<u32>,
    /// Resolved stabilization switch state (see `resolve_image_stabilization`),
    /// not the raw tag component — consumers must not re-interpret hardware type.
    image_stabilization: Option<bool>,
}

impl Fujifilm {
    pub fn camera_type(&self) -> String {
        "Fujifilm".to_owned()
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

    /// Format lens name from LensInfo (MinFL, MaxFL, MinAperture, MaxAperture).
    /// Examples: "23mm f/1.4" (prime), "18-50mm f/2.8" (zoom, constant aperture),
    /// "45-100mm f/4-5.6" (zoom, variable aperture)
    fn format_lens_name(exif: &FujiExifData) -> Option<String> {
        let min_fl = exif.min_focal_length?;
        if min_fl <= 0.0 { return None; }
        let max_fl = exif.max_focal_length.unwrap_or(min_fl);

        let fl_part = if (min_fl - max_fl).abs() < 0.01 {
            format!("{:.0}mm", min_fl)
        } else {
            format!("{:.0}-{:.0}mm", min_fl, max_fl)
        };

        match (exif.min_aperture, exif.max_aperture) {
            (Some(min_ap), Some(max_ap)) if min_ap > 0.0 && max_ap > 0.0 => {
                if (min_ap - max_ap).abs() < 0.01 {
                    Some(format!("{} f/{}", fl_part, format_aperture(min_ap)))
                } else {
                    Some(format!("{} f/{}-{}", fl_part, format_aperture(min_ap), format_aperture(max_ap)))
                }
            }
            (Some(ap), _) | (_, Some(ap)) if ap > 0.0 => {
                Some(format!("{} f/{}", fl_part, format_aperture(ap)))
            }
            _ => Some(fl_part),
        }
    }

    pub fn detect<P: AsRef<std::path::Path>>(buffer: &[u8], _filepath: P, _options: &crate::InputOptions) -> Option<Self> {
        if memmem::find(buffer, b"FUJIFILM").is_some() && memmem::find(buffer, b"MVTG").is_some() {
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

        // Parse MVTG atom from moov/udta
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

    /// Parse MOV/MP4: find moov/udta/MVTG atom containing TIFF IFD.
    fn parse_mov<T: Read + Seek>(&mut self, stream: &mut T, _size: usize) -> Result<FujiExifData> {
        stream.seek(SeekFrom::Start(0))?;

        // First find moov atom
        let moov_data = self.find_moov_data(stream)?;
        let mut cursor = Cursor::new(moov_data.as_slice());
        let moov_len = moov_data.len() as u64;

        // Search inside moov for udta, then MVTG
        while cursor.position() < moov_len {
            let (typ, _offs, size, header_size) = util::read_box(&mut cursor)?;
            let content_size = size as i64 - header_size;
            if content_size <= 0 { break; }

            if typ == util::fourcc("udta") {
                let start = cursor.position() as usize;
                let end = start + content_size as usize;
                if end <= moov_data.len() {
                    if let Some(result) = self.find_mvtg_in_data(&moov_data[start..end]) {
                        return Ok(result);
                    }
                }
            }
            cursor.seek(SeekFrom::Current(content_size))?;
        }

        // Fallback: search entire moov data for MVTG
        if let Some(result) = self.find_mvtg_in_data(&moov_data) {
            return Ok(result);
        }

        Err(Error::new(ErrorKind::NotFound, "MVTG atom not found in MOV"))
    }

    fn find_moov_data<T: Read + Seek>(&self, stream: &mut T) -> Result<Vec<u8>> {
        loop {
            let (typ, _offs, size, header_size) = util::read_box(stream)?;
            let content_size = size as i64 - header_size;
            if content_size < 0 { break; }
            if content_size == 0 { continue; }

            if typ == util::fourcc("moov") {
                let mut data = vec![0u8; content_size as usize];
                stream.read_exact(&mut data)?;
                return Ok(data);
            }
            stream.seek(SeekFrom::Current(content_size))?;
        }
        Err(Error::new(ErrorKind::NotFound, "moov atom not found"))
    }

    fn find_mvtg_in_data(&self, data: &[u8]) -> Option<FujiExifData> {
        let mut cursor = Cursor::new(data);
        let len = data.len() as u64;
        while cursor.position() < len {
            let (typ, _offs, size, header_size) = util::read_box(&mut cursor).ok()?;
            let content_size = size as i64 - header_size;
            if content_size < 0 { break; }
            if content_size == 0 { continue; }

            if typ == util::fourcc("MVTG") {
                let start = cursor.position() as usize;
                let end = start + content_size as usize;
                if end <= data.len() {
                    let mvtg_data = &data[start..end];
                    if let Some(result) = extract_exif_from_mvtg(mvtg_data) {
                        return Some(result);
                    }
                }
            }
            cursor.seek(SeekFrom::Current(content_size)).ok()?;
        }
        None
    }

    fn process_map(&mut self, map: &mut GroupedTagMap, options: &crate::InputOptions, video_md: Option<&VideoMetadata>, exif: &FujiExifData) {
        let raw_model = exif.model.as_deref().unwrap_or("");
        let resolution_w = video_md.map(|v| v.width as u32).unwrap_or(0);
        let resolution_h = video_md.map(|v| v.height as u32).unwrap_or(0);
        let fps = video_md.map(|v| v.fps).unwrap_or(0.0);

        // Write video resolution
        if resolution_w > 0 {
            util::insert_tag(map, tag!(parsed GroupId::Default, TagId::Custom("video_width".into()), "Video output width", u32, |v| format!("{} px", v), resolution_w, Vec::new()), options);
            util::insert_tag(map, tag!(parsed GroupId::Default, TagId::Custom("video_height".into()), "Video output height", u32, |v| format!("{} px", v), resolution_h, Vec::new()), options);
        }

        // Focal length: when min_fl == max_fl (prime lens), use as confirmed FL
        let focal_length = match (exif.min_focal_length, exif.max_focal_length) {
            (Some(min), Some(max)) if min > 0.0 && (min - max).abs() < 0.01 => Some(min),
            _ => None,
        };
        if let Some(fl) = focal_length {
            util::insert_tag(map, tag!(parsed GroupId::Lens, TagId::FocalLength, "Focal length", f32, |v| format!("{:.1} mm", v), fl as f32, Vec::new()), options);
        }

        // Lens DisplayName from LensInfo
        let lens_name = Self::format_lens_name(exif);
        if let Some(ref name) = lens_name {
            self.lens = Some(name.clone());
            util::insert_tag(map, tag!(parsed GroupId::Lens, TagId::DisplayName, "Lens name", String, |v| v.clone(), name.clone(), Vec::new()), options);
        }

        // Try JSON database first
        if let Some(db_path) = &options.camera_db_path {
            if let Ok(db) = crate::camera_db::CameraDatabase::load(db_path) {
                if let Some((model_name, model_data)) = db.process_model("FUJIFILM", raw_model, map, options) {
                    self.model = Some(model_name.to_string());
                    let sensor_w = model_data.sw;

                    // Crop
                    let tags = std::collections::HashMap::new();
                    let effective_crop = db.process_crop("FUJIFILM", model_name, resolution_w, resolution_h, fps, None, &tags, map, options);

                    // Unit pixel focal length
                    if resolution_w > 0 {
                        let unit_px_fl = resolution_w as f64 * effective_crop / sensor_w as f64;
                        util::insert_tag(map, tag!(parsed GroupId::Lens, TagId::Custom("unit_pixel_focal_length".into()), "Pixel focal length per mm", f64, |v| format!("{:.4}", v), unit_px_fl, Vec::new()), options);
                    }

                    // Pixel focal length
                    let fl = focal_length.or(options.user_focal_length);
                    if let Some(fl) = fl {
                        if resolution_w > 0 {
                            let fx = util::calc_pixel_focal_length(fl, sensor_w, resolution_w, effective_crop);
                            if fx > 0.0 {
                                util::insert_tag(map, tag!(parsed GroupId::Lens, TagId::PixelFocalLength, "Pixel focal length", f32, |v| format!("{:.2}", v), fx as f32, Vec::new()), options);
                            }
                        }
                    }

                    // Readout
                    if self.frame_readout_time.is_none() {
                        if let Some(rt) = db.process_readout("FUJIFILM", model_name, resolution_w, resolution_h, fps, 0.0, sensor_w, &tags, map, options) {
                            self.frame_readout_time = Some(rt);
                        }
                    }

                    // Creation date
                    if let Some(ref dt) = exif.datetime_original {
                        util::write_creation_date_tags(map, dt, exif.offset_time_original.as_deref(), exif.subsec_time_original.as_deref().or(Some("500")), options);
                    } else if let Some(ref mvhd_time) = self.mvhd_creation_time {
                        util::write_creation_date_tags(map, mvhd_time, None, Some("500"), options);
                    }

                    // Frame rates: always output both playback and recording fps
                    let playback_fps = fps;
                    let record_fps = exif.record_frame_rate.map(|v| v as f64).unwrap_or(playback_fps);
                    if playback_fps > 0.0 {
                        util::insert_tag(map, tag!(parsed GroupId::Default, TagId::FrameRate, "Frame rate", f64, |v| format!("{:.3} fps", v), playback_fps, Vec::new()), options);
                        util::insert_tag(map, tag!(parsed GroupId::Default, TagId::RecordFrameRate, "Record frame rate", f64, |v| format!("{:.3} fps", v), record_fps, Vec::new()), options);
                    }

                    // Image stabilization (default false if tag not present)
                    let is_on = exif.image_stabilization.unwrap_or(false);
                    util::insert_tag(map, tag!(parsed GroupId::Default, TagId::ImageStabilizer, "Image stabilization", bool, |v| if *v { "On" } else { "Off" }.into(), is_on, Vec::new()), options);

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

// ---- MVTG content extraction ----

/// Extract EXIF data from MVTG atom content.
/// Fujifilm MVTG atoms have a 16-byte proprietary header followed by
/// a Little-Endian IFD (without standard TIFF II/MM magic).
/// Value offsets in the IFD are relative to the IFD base (byte 16 of MVTG content).
/// Falls back to TIFF parsing or string extraction if the primary method fails.
fn extract_exif_from_mvtg(mvtg_data: &[u8]) -> Option<FujiExifData> {
    if mvtg_data.len() < 20 { return None; }

    // Case 1: Fujifilm proprietary IFD (16-byte header + LE IFD)
    // The IFD starts at offset 16 within the MVTG content.
    // Value offsets are relative to the IFD base (offset 16).
    let ifd_base = 16usize;
    let ifd_data = &mvtg_data[ifd_base..];
    if let Some(result) = parse_fuji_ifd(ifd_data) {
        if result.model.is_some() {
            return Some(result);
        }
    }

    // Case 2: Standard TIFF header at start or within the data
    if is_tiff_header(mvtg_data) {
        if let Ok(result) = parse_tiff_ifd(mvtg_data) {
            if result.model.is_some() {
                return Some(result);
            }
        }
    }
    for i in 1..mvtg_data.len().saturating_sub(8) {
        if is_tiff_header(&mvtg_data[i..]) {
            if let Ok(result) = parse_tiff_ifd(&mvtg_data[i..]) {
                if result.model.is_some() {
                    return Some(result);
                }
            }
        }
    }

    // Case 3: Search for "FUJIFILM" string followed by model text
    if let Some(model) = extract_model_from_strings(mvtg_data) {
        return Some(FujiExifData { model: Some(model), ..Default::default() });
    }

    None
}

/// Parse Fujifilm proprietary IFD (Little-Endian, no TIFF magic header).
/// The data starts at the IFD entry count, and value offsets are relative to data start.
fn parse_fuji_ifd(ifd_data: &[u8]) -> Option<FujiExifData> {
    if ifd_data.len() < 4 { return None; }

    let count = read_u16(ifd_data, 0, true)? as usize;
    if count == 0 || count > 100 { return None; }

    let entries_start = 2;
    let mut result = FujiExifData::default();
    let mut exif_ifd_offset: Option<usize> = None;

    for i in 0..count {
        let entry_offset = entries_start + i * 12;
        if entry_offset + 12 > ifd_data.len() { break; }

        let tag = read_u16(ifd_data, entry_offset, true)?;
        let typ = read_u16(ifd_data, entry_offset + 2, true)?;
        let cnt = read_u32(ifd_data, entry_offset + 4, true)? as usize;

        // Validate type
        let type_size = match typ {
            1 | 2 | 6 | 7 => 1,
            3 | 8 => 2,
            4 | 9 | 11 => 4,
            5 | 10 | 12 => 8,
            _ => return None, // Invalid type means this isn't a valid IFD
        };

        let total_size = cnt.saturating_mul(type_size);
        let value_data = if total_size <= 4 {
            let end = (entry_offset + 8 + total_size.min(4)).min(ifd_data.len());
            &ifd_data[entry_offset + 8..end]
        } else {
            let data_offset = read_u32(ifd_data, entry_offset + 8, true)? as usize;
            if data_offset.saturating_add(total_size) <= ifd_data.len() {
                &ifd_data[data_offset..data_offset + total_size]
            } else {
                continue;
            }
        };

        match tag {
            0x0110 => { // Model
                result.model = Some(read_string(value_data, cnt));
            }
            0x8769 => { // ExifIFD offset
                exif_ifd_offset = read_u32(ifd_data, entry_offset + 8, true).map(|v| v as usize);
            }
            _ => {}
        }
    }

    // Parse ExifIFD for LensInfo and MakerNotes
    if let Some(offset) = exif_ifd_offset {
        parse_fuji_sub_ifd(ifd_data, offset, &mut result);
    }

    Some(result)
}

/// Parse ExifIFD and FujiFilm MakerNotes from the proprietary IFD data.
fn parse_fuji_sub_ifd(ifd_data: &[u8], exif_offset: usize, result: &mut FujiExifData) {
    if exif_offset + 2 > ifd_data.len() { return; }

    let count = match read_u16(ifd_data, exif_offset, true) {
        Some(c) if c > 0 && c <= 200 => c as usize,
        _ => return,
    };

    let entries_start = exif_offset + 2;
    let mut makernotes_offset: Option<(usize, usize)> = None;

    for i in 0..count {
        let entry_offset = entries_start + i * 12;
        if entry_offset + 12 > ifd_data.len() { break; }

        let tag = match read_u16(ifd_data, entry_offset, true) { Some(t) => t, None => continue };
        let typ = match read_u16(ifd_data, entry_offset + 2, true) { Some(t) => t, None => continue };
        let cnt = match read_u32(ifd_data, entry_offset + 4, true) { Some(c) => c as usize, None => continue };

        let type_size = match typ {
            1 | 2 | 6 | 7 => 1,
            3 | 8 => 2,
            4 | 9 | 11 => 4,
            5 | 10 | 12 => 8,
            _ => continue,
        };

        let total_size = cnt.saturating_mul(type_size);
        let value_data = if total_size <= 4 {
            let end = (entry_offset + 8 + total_size.min(4)).min(ifd_data.len());
            &ifd_data[entry_offset + 8..end]
        } else {
            let data_offset = match read_u32(ifd_data, entry_offset + 8, true) { Some(o) => o as usize, None => continue };
            if data_offset.saturating_add(total_size) <= ifd_data.len() {
                &ifd_data[data_offset..data_offset + total_size]
            } else {
                continue;
            }
        };

        match tag {
            0x9003 => { // DateTimeOriginal
                let s = read_string(value_data, cnt);
                if !s.is_empty() { result.datetime_original = Some(s); }
            }
            0x9011 => { // OffsetTimeOriginal
                let s = read_string(value_data, cnt);
                if !s.is_empty() { result.offset_time_original = Some(s); }
            }
            0x9291 => { // SubSecTimeOriginal
                let s = read_string(value_data, cnt);
                if !s.is_empty() { result.subsec_time_original = Some(s); }
            }
            0xa432 => { // LensInfo: 4 RATIONALs (MinFL, MaxFL, MinAperture, MaxAperture)
                parse_lens_info(value_data, true, result);
            }
            0x927C => { // MakerNote
                let data_offset = if total_size <= 4 {
                    entry_offset + 8
                } else {
                    match read_u32(ifd_data, entry_offset + 8, true) { Some(o) => o as usize, None => 0 }
                };
                if data_offset > 0 { makernotes_offset = Some((data_offset, cnt)); }
            }
            _ => {}
        }
    }

    // Parse FujiFilm MakerNotes whenever present: besides the lens tags they carry
    // ImageStabilization (0x1422), FrameRate (0x3820) and FullHDHighSpeedRec (0x3824),
    // which have no standard-EXIF equivalent. Lens writes inside are fill-only, so
    // values already provided by LensInfo (0xa432) are never overwritten.
    if let Some((mn_offset, mn_size)) = makernotes_offset {
        parse_fuji_makernotes(ifd_data, mn_offset, mn_size, result);
    }
}

/// Format aperture value: show as integer if whole number, otherwise one decimal.
fn format_aperture(ap: f64) -> String {
    if (ap - ap.round()).abs() < 0.01 {
        format!("{:.0}", ap)
    } else {
        format!("{:.1}", ap)
    }
}

/// Parse the FujiFilm MakerNotes IFD. Lens tags — MinFocalLength (0x1404),
/// MaxFocalLength (0x1405), MaxApertureAtMinFocal (0x1406), MaxApertureAtMaxFocal
/// (0x1407) — are fill-only: they never overwrite values already supplied by
/// standard EXIF LensInfo (0xa432). Also reads ImageStabilization (0x1422),
/// FrameRate (0x3820) and FullHDHighSpeedRec (0x3824), which exist only here.
/// FujiFilm MakerNotes start with "FUJIFILM" (8 bytes) + 4-byte offset to IFD;
/// value offsets are relative to the MakerNotes start and all reads are
/// little-endian regardless of the container's byte order (exiftool: Fujifilm
/// MakerNotes are always LE).
fn parse_fuji_makernotes(ifd_data: &[u8], mn_offset: usize, mn_size: usize, result: &mut FujiExifData) {
    let mn_end = mn_offset.saturating_add(mn_size).min(ifd_data.len());
    if mn_offset + 12 > mn_end { return; }
    let mn_data = &ifd_data[mn_offset..mn_end];

    // Check for "FUJIFILM" header
    if mn_data.len() < 12 || &mn_data[..8] != b"FUJIFILM" {
        return;
    }

    // Offset to IFD from start of MakerNotes (4 bytes LE at offset 8)
    let ifd_rel_offset = match read_u32(mn_data, 8, true) {
        Some(o) => o as usize,
        None => return,
    };

    if ifd_rel_offset + 2 > mn_data.len() { return; }

    let count = match read_u16(mn_data, ifd_rel_offset, true) {
        Some(c) if c > 0 && c <= 500 => c as usize,
        _ => return,
    };

    let entries_start = ifd_rel_offset + 2;
    for i in 0..count {
        let entry_offset = entries_start + i * 12;
        if entry_offset + 12 > mn_data.len() { break; }

        let tag = match read_u16(mn_data, entry_offset, true) { Some(t) => t, None => continue };
        let typ = match read_u16(mn_data, entry_offset + 2, true) { Some(t) => t, None => continue };
        let cnt = match read_u32(mn_data, entry_offset + 4, true) { Some(c) => c as usize, None => continue };

        // Tags we're interested in
        match tag {
            0x1404 | 0x1405 | 0x1406 | 0x1407 => {} // RATIONAL focal/aperture
            0x1422 | 0x3820 | 0x3824 => {}           // int16u/int32u stabilization/framerate
            _ => continue,
        }

        let type_size = match typ {
            1 | 2 | 6 | 7 => 1,
            3 | 8 => 2,
            4 | 9 | 11 => 4,
            5 | 10 | 12 => 8,
            _ => continue,
        };

        let total_size = cnt.saturating_mul(type_size);
        let value_data = if total_size <= 4 {
            let end = (entry_offset + 8 + total_size.min(4)).min(mn_data.len());
            &mn_data[entry_offset + 8..end]
        } else {
            // Offsets in FujiFilm MakerNotes are relative to MakerNotes start
            let data_offset = match read_u32(mn_data, entry_offset + 8, true) { Some(o) => o as usize, None => continue };
            if data_offset.saturating_add(total_size) <= mn_data.len() {
                &mn_data[data_offset..data_offset + total_size]
            } else {
                continue;
            }
        };

        match tag {
            0x1404 | 0x1405 | 0x1406 | 0x1407 => {
                if value_data.len() >= 8 {
                    if let (Some(num), Some(den)) = (read_u32(value_data, 0, true), read_u32(value_data, 4, true)) {
                        if den > 0 {
                            let fl = num as f64 / den as f64;
                            // Fill-only: standard EXIF LensInfo (0xa432) takes precedence
                            match tag {
                                0x1404 => { if result.min_focal_length.is_none() { result.min_focal_length = Some(fl); } }
                                0x1405 => { if result.max_focal_length.is_none() { result.max_focal_length = Some(fl); } }
                                0x1406 => { if result.min_aperture.is_none() { result.min_aperture = Some(fl); } }
                                0x1407 => { if result.max_aperture.is_none() { result.max_aperture = Some(fl); } }
                                _ => {}
                            }
                        }
                    }
                }
            }
            0x3820 => { // FrameRate (int16u)
                if let Some(v) = read_u16(value_data, 0, true) {
                    result.record_frame_rate = Some(v);
                }
            }
            0x3824 => { // FullHDHighSpeedRec (int32u)
                if let Some(v) = read_u32(value_data, 0, true) {
                    result.slow_motion = Some(v);
                }
            }
            0x1422 => { // ImageStabilization (3 x int16u)
                if let Some(on) = resolve_image_stabilization(value_data, cnt) {
                    result.image_stabilization = Some(on);
                }
            }
            _ => {}
        }
    }
}

/// Resolve tag 0x1422 (ImageStabilization) to an on/off state.
/// Per exiftool FujiFilm.pm the tag is 3 x int16u: [0] = hardware type
/// (0 None / 1 Optical / 2 Sensor-shift / 3 OIS Lens), [1] = switch
/// (0 Off / 1 On mode 1 / 2 On mode 2). Stabilization is active only when
/// hardware exists AND the switch is on. When only component [0] is available
/// (count 1, or truncated value data) fall back to [0] != 0 — the fallback must
/// terminate at [0], never at None, so clips carrying only the hardware
/// component keep reporting On instead of regressing through unwrap_or(false).
fn resolve_image_stabilization(value_data: &[u8], cnt: usize) -> Option<bool> {
    let hardware = read_u16(value_data, 0, true)?;
    if cnt >= 2 {
        if let Some(switch) = read_u16(value_data, 2, true) {
            return Some(hardware != 0 && switch != 0);
        }
    }
    Some(hardware != 0)
}

/// Extract LensInfo (0xa432, 4 RATIONALs: MinFocalLength, MaxFocalLength,
/// MinAperture, MaxAperture) into the result. Shared by the proprietary-IFD
/// path (always LE) and the standard-TIFF path (container byte order), so the
/// two paths cannot drift on which bytes carry which field.
fn parse_lens_info(value_data: &[u8], is_le: bool, result: &mut FujiExifData) {
    if value_data.len() >= 16 {
        if let (Some(n1), Some(d1), Some(n2), Some(d2)) = (
            read_u32(value_data, 0, is_le),
            read_u32(value_data, 4, is_le),
            read_u32(value_data, 8, is_le),
            read_u32(value_data, 12, is_le),
        ) {
            if d1 > 0 { result.min_focal_length = Some(n1 as f64 / d1 as f64); }
            if d2 > 0 { result.max_focal_length = Some(n2 as f64 / d2 as f64); }
        }
    }
    if value_data.len() >= 32 {
        if let (Some(n3), Some(d3), Some(n4), Some(d4)) = (
            read_u32(value_data, 16, is_le),
            read_u32(value_data, 20, is_le),
            read_u32(value_data, 24, is_le),
            read_u32(value_data, 28, is_le),
        ) {
            if d3 > 0 { result.min_aperture = Some(n3 as f64 / d3 as f64); }
            if d4 > 0 { result.max_aperture = Some(n4 as f64 / d4 as f64); }
        }
    }
}

/// Try to extract model by finding "FUJIFILM" in the data and reading nearby strings.
fn extract_model_from_strings(data: &[u8]) -> Option<String> {
    let mut search_start = 0;
    while let Some(pos) = memmem::find(&data[search_start..], b"FUJIFILM") {
        let abs_pos = search_start + pos;

        // Read this null-terminated string
        let str_data = &data[abs_pos..];
        let end = str_data.iter().position(|&b| b == 0).unwrap_or(str_data.len());
        let make_str = String::from_utf8_lossy(&str_data[..end]).to_string();

        // If this is just "FUJIFILM" (the Make), skip null padding and read the next string
        if make_str == "FUJIFILM" {
            let mut next_start = abs_pos + end;
            // Skip null padding bytes
            while next_start < data.len() && data[next_start] == 0 {
                next_start += 1;
            }
            if next_start < data.len() {
                let next_data = &data[next_start..];
                let next_end = next_data.iter().position(|&b| b == 0).unwrap_or(next_data.len());
                if next_end > 0 {
                    let model_str = String::from_utf8_lossy(&next_data[..next_end]).trim().to_string();
                    if !model_str.is_empty() {
                        return Some(model_str);
                    }
                }
            }
        } else {
            return Some(make_str);
        }

        search_start = abs_pos + 8;
        if search_start >= data.len() { break; }
    }
    None
}

// ---- TIFF IFD Parser (fallback for non-standard MVTG layouts) ----

fn parse_tiff_ifd(tiff_data: &[u8]) -> Result<FujiExifData> {
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

    let mut result = FujiExifData::default();
    let mut exif_ifd_offset = None;

    parse_ifd_entries(tiff_data, ifd0_offset, is_le, &mut |tag, _typ, count, value_data, value_offset| {
        match tag {
            0x0110 => {
                result.model = Some(read_string(value_data, count));
            }
            0x8769 => { // ExifIFD offset
                exif_ifd_offset = read_u32(tiff_data, value_offset, is_le).map(|v| v as usize);
            }
            _ => {}
        }
    });

    // Parse ExifIFD for LensInfo and MakerNotes
    if let Some(offset) = exif_ifd_offset {
        let mut makernotes_offset: Option<(usize, usize)> = None;

        parse_ifd_entries(tiff_data, offset, is_le, &mut |tag, _typ, count, value_data, value_offset| {
            match tag {
                0x9003 => { // DateTimeOriginal
                    let s = read_string(value_data, count);
                    if !s.is_empty() { result.datetime_original = Some(s); }
                }
                0x9011 => { // OffsetTimeOriginal
                    let s = read_string(value_data, count);
                    if !s.is_empty() { result.offset_time_original = Some(s); }
                }
                0x9291 => { // SubSecTimeOriginal
                    let s = read_string(value_data, count);
                    if !s.is_empty() { result.subsec_time_original = Some(s); }
                }
                0xa432 => { // LensInfo: 4 RATIONALs (apertures included, same as the proprietary path)
                    parse_lens_info(value_data, is_le, &mut result);
                }
                0x927C => { // MakerNote
                    makernotes_offset = Some((value_offset, count));
                }
                _ => {}
            }
        });

        // Parse FujiFilm MakerNotes whenever present, through the same function as
        // the proprietary-IFD path (lens writes fill-only, note-internal reads
        // always LE) so the two paths cannot drift.
        if let Some((mn_offset, mn_size)) = makernotes_offset {
            parse_fuji_makernotes(tiff_data, mn_offset, mn_size, &mut result);
        }
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rational(num: u32, den: u32) -> Vec<u8> {
        let mut v = num.to_le_bytes().to_vec();
        v.extend_from_slice(&den.to_le_bytes());
        v
    }

    /// Build a FujiFilm MakerNote blob: "FUJIFILM" + u32 LE IFD offset + IFD.
    /// Values > 4 bytes are placed after the entry table with offsets relative
    /// to the note start (the Fujifilm convention).
    fn build_makernote(entries: &[(u16, u16, u32, Vec<u8>)]) -> Vec<u8> {
        let ifd_offset = 12usize;
        let entries_start = ifd_offset + 2;
        let value_area_start = entries_start + entries.len() * 12;
        let mut values: Vec<u8> = Vec::new();
        let mut buf = Vec::new();
        buf.extend_from_slice(b"FUJIFILM");
        buf.extend_from_slice(&(ifd_offset as u32).to_le_bytes());
        buf.extend_from_slice(&(entries.len() as u16).to_le_bytes());
        for (tag, typ, cnt, data) in entries {
            buf.extend_from_slice(&tag.to_le_bytes());
            buf.extend_from_slice(&typ.to_le_bytes());
            buf.extend_from_slice(&cnt.to_le_bytes());
            if data.len() <= 4 {
                let mut inline = [0u8; 4];
                inline[..data.len()].copy_from_slice(data);
                buf.extend_from_slice(&inline);
            } else {
                let off = (value_area_start + values.len()) as u32;
                buf.extend_from_slice(&off.to_le_bytes());
                values.extend_from_slice(data);
            }
        }
        buf.extend_from_slice(&values);
        buf
    }

    fn lens_makernote() -> Vec<u8> {
        build_makernote(&[
            (0x1404, 5, 1, rational(23, 1)),
            (0x1405, 5, 1, rational(55, 1)),
            (0x1406, 5, 1, rational(28, 10)),
            (0x1407, 5, 1, rational(4, 1)),
        ])
    }

    #[test]
    fn makernote_lens_writes_are_fill_only() {
        let note = lens_makernote();
        let mut prefilled = FujiExifData {
            min_focal_length: Some(15.0),
            max_focal_length: Some(45.0),
            min_aperture: Some(3.5),
            max_aperture: Some(5.6),
            ..Default::default()
        };
        parse_fuji_makernotes(&note, 0, note.len(), &mut prefilled);
        assert_eq!(prefilled.min_focal_length, Some(15.0));
        assert_eq!(prefilled.max_focal_length, Some(45.0));
        assert_eq!(prefilled.min_aperture, Some(3.5));
        assert_eq!(prefilled.max_aperture, Some(5.6));
    }

    #[test]
    fn makernote_fills_empty_lens_fields() {
        let note = lens_makernote();
        let mut empty = FujiExifData::default();
        parse_fuji_makernotes(&note, 0, note.len(), &mut empty);
        assert_eq!(empty.min_focal_length, Some(23.0));
        assert_eq!(empty.max_focal_length, Some(55.0));
        assert_eq!(empty.min_aperture, Some(2.8));
        assert_eq!(empty.max_aperture, Some(4.0));
        // No 0x1422 in this note: stabilization must stay None
        assert_eq!(empty.image_stabilization, None);
    }

    #[test]
    fn makernote_completes_partial_lens_info_without_clobbering() {
        // LensInfo supplied focal lengths only; MakerNote carries different focal
        // values plus apertures. Apertures fill in, focal lengths must survive.
        let note = lens_makernote();
        let mut partial = FujiExifData {
            min_focal_length: Some(15.0),
            max_focal_length: Some(45.0),
            ..Default::default()
        };
        parse_fuji_makernotes(&note, 0, note.len(), &mut partial);
        assert_eq!(partial.min_focal_length, Some(15.0));
        assert_eq!(partial.max_focal_length, Some(45.0));
        assert_eq!(partial.min_aperture, Some(2.8));
        assert_eq!(partial.max_aperture, Some(4.0));
    }

    #[test]
    fn image_stabilization_component_semantics() {
        // [3,1,0]: OIS lens, switch on (known corpus DSCF2237.MOV)
        assert_eq!(resolve_image_stabilization(&[3, 0, 1, 0, 0, 0], 3), Some(true));
        // [3,0,0]: OIS lens, switch off — the intended flip (exiftool authority;
        // no real material, this test is its only lock)
        assert_eq!(resolve_image_stabilization(&[3, 0, 0, 0, 0, 0], 3), Some(false));
        // [0,1,0]: no stabilization hardware
        assert_eq!(resolve_image_stabilization(&[0, 0, 1, 0, 0, 0], 3), Some(false));
        // count 1, nonzero: legacy single-component behavior preserved
        assert_eq!(resolve_image_stabilization(&[1, 0], 1), Some(true));
        // declared count >= 2 but value data truncated to 2 bytes: fall back to
        // [0] != 0, never regress to None
        assert_eq!(resolve_image_stabilization(&[2, 0], 2), Some(true));
        // unreadable [0]
        assert_eq!(resolve_image_stabilization(&[], 1), None);
    }

    #[test]
    fn makernote_resolves_stabilization_through_parse() {
        let note = build_makernote(&[
            (0x1422, 3, 3, vec![3, 0, 1, 0, 0, 0]),
        ]);
        let mut result = FujiExifData::default();
        parse_fuji_makernotes(&note, 0, note.len(), &mut result);
        assert_eq!(result.image_stabilization, Some(true));
    }

    /// Build a standard-TIFF MVTG payload (LE): IFD0 (Model + ExifIFD pointer),
    /// ExifIFD (LensInfo + MakerNote), value area with the given makernote.
    fn build_tiff_fixture(note: &[u8]) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(b"II");
        buf.extend_from_slice(&42u16.to_le_bytes());
        buf.extend_from_slice(&8u32.to_le_bytes()); // IFD0 at 8
        // Layout: IFD0 @8 (count + 2 entries -> ends 34), ExifIFD @34 (count + 2
        // entries -> ends 60), value area @60: model (5), lens info (32), note.
        let model_off = 60u32;
        let lens_off = model_off + 5;
        let note_off = lens_off + 32;
        // IFD0
        buf.extend_from_slice(&2u16.to_le_bytes());
        buf.extend_from_slice(&0x0110u16.to_le_bytes()); // Model, ASCII x5, offset form
        buf.extend_from_slice(&2u16.to_le_bytes());
        buf.extend_from_slice(&5u32.to_le_bytes());
        buf.extend_from_slice(&model_off.to_le_bytes());
        buf.extend_from_slice(&0x8769u16.to_le_bytes()); // ExifIFD, LONG x1, inline
        buf.extend_from_slice(&4u16.to_le_bytes());
        buf.extend_from_slice(&1u32.to_le_bytes());
        buf.extend_from_slice(&34u32.to_le_bytes());
        // ExifIFD
        buf.extend_from_slice(&2u16.to_le_bytes());
        buf.extend_from_slice(&0xa432u16.to_le_bytes()); // LensInfo, RATIONAL x4, offset form
        buf.extend_from_slice(&5u16.to_le_bytes());
        buf.extend_from_slice(&4u32.to_le_bytes());
        buf.extend_from_slice(&lens_off.to_le_bytes());
        buf.extend_from_slice(&0x927Cu16.to_le_bytes()); // MakerNote, UNDEFINED, offset form
        buf.extend_from_slice(&7u16.to_le_bytes());
        buf.extend_from_slice(&(note.len() as u32).to_le_bytes());
        buf.extend_from_slice(&note_off.to_le_bytes());
        // Value area
        assert_eq!(buf.len(), 60);
        buf.extend_from_slice(b"X-M5\0");
        buf.extend_from_slice(&rational(15, 1));
        buf.extend_from_slice(&rational(45, 1));
        buf.extend_from_slice(&rational(35, 10));
        buf.extend_from_slice(&rational(56, 10));
        buf.extend_from_slice(note);
        buf
    }

    #[test]
    fn tiff_path_reaches_parity_with_proprietary_path() {
        // MakerNote: conflicting focal values (must not clobber LensInfo),
        // 0x1422 = [3,1,0] (6 bytes, offset form) and 0x3820 as a 2-byte
        // inline-form value — structurally unreadable by the deleted inline copy.
        let note = build_makernote(&[
            (0x1404, 5, 1, rational(23, 1)),
            (0x1405, 5, 1, rational(55, 1)),
            (0x1422, 3, 3, vec![3, 0, 1, 0, 0, 0]),
            (0x3820, 3, 1, vec![30, 0]),
        ]);
        let tiff = build_tiff_fixture(&note);
        let result = parse_tiff_ifd(&tiff).expect("fixture must parse");
        assert_eq!(result.model.as_deref(), Some("X-M5"));
        // LensInfo wins over the MakerNote focal values; apertures now read on
        // the TIFF path too (bytes 16-31 of 0xa432)
        assert_eq!(result.min_focal_length, Some(15.0));
        assert_eq!(result.max_focal_length, Some(45.0));
        assert_eq!(result.min_aperture, Some(3.5));
        assert_eq!(result.max_aperture, Some(5.6));
        // MakerNote-only fields populated despite complete LensInfo
        assert_eq!(result.image_stabilization, Some(true));
        assert_eq!(result.record_frame_rate, Some(30));
    }
}

