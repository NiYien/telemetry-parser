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
    /// Container-level creation time from mvhd ("yyyy:MM:dd HH:mm:ss", UTC), if present
    creation_date: Option<String>,
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

    /// Max native sensor resolution width in pixels per model.
    ///
    /// Correctness criterion: with square photosites, the sensor's physical
    /// aspect ratio must equal the aspect ratio of the resolution listed here.
    /// Where a model also has a hardcoded PixelPitch below, `sensor_width /
    /// PixelPitch` must agree with this value (within one pixel); the two are
    /// cross-checks of each other.
    ///
    /// Do NOT take `NiYien_Tool/camera/bmd.cpp` at face value: its
    /// `BMD_MODEL_LIST` resolution lists and its `bmd_calc_profile` `max_frame`
    /// constants contradict each other. In particular that file lumps BMCC 4K
    /// together with BMPCC 4K under `max_frame = 4096`, which is wrong for
    /// BMCC 4K: 21.12 x 11.88 mm is 1.778, i.e. 3840x2160, and the same file's
    /// model list says "3840x2160". BMCC 4K stays 3840 -- do not "fix" it.
    ///
    /// Every model present in camera_db/blackmagic.json must appear here, keyed
    /// by its canonical (post-alias) name. A miss returns 0 and the `.max(1)`
    /// guard at the call site collapses `unit_pixel_focal_length` to
    /// `1 / sensor_width`, which is how "BMSC 4K G2" used to yield 0.0527.
    fn max_resolution_w(model: &str) -> u32 {
        match model {
            "BMPCC"                  => 1920,  // 12.48 x 7.02  -> 1.778
            "BMCC"                  => 2432,  // 15.81 x 8.88  -> 1.780; original Cinema Camera 2.5K (not in NiYien Tool, calibrated from official specs)
            "BMCC 4K"               => 3840,  // 21.12 x 11.88 -> 1.778
            "BMCC 6K"               => 6048,  // 36.00 x 24.00 -> 1.500
            "BMPCC 4K"              => 4096,  // 18.96 x 10.00 -> 1.896; 18.96mm / 4628nm = 4096.8 px
            "BMSC 4K G2"            => 4096,  // same sensor as BMPCC 4K (Micro Studio Camera 4K G2)
            "BMPCC 6K"              => 6144,  // 23.10 x 12.99 -> 1.778
            "URSA Mini 4K"          => 3840,
            "URSA Mini 4.6K"        => 4608,  // 25.34 x 14.25 -> 1.778
            "URSA Mini Pro 4.6K G2" => 4608,
            "URSA Mini Pro 12K"     => 12288, // 27.03 x 14.25 -> 1.897
            _ => 0
        }
    }

    /// Convert `unit_pixel_focal_length` (pixels per mm) into a square pixel
    /// pitch in nanometres, refusing values that cannot be a real sensor.
    ///
    /// The plausibility window exists because `max_resolution_w` returning 0 for
    /// an unlisted model makes the caller's `.max(1)` collapse the pitch by
    /// roughly `sensor_width` orders of magnitude. Refusing to emit leaves the
    /// consumer on its previous fallback, which is wrong but bounded; emitting
    /// the collapsed value would skew every rendered frame instead. Digital
    /// cinema and photo sensors sit between 0.5 um and 20 um per photosite,
    /// i.e. 50..2000 pixels per mm; every Blackmagic model currently in
    /// camera_db lands between 153.85 and 454.6.
    fn pixel_pitch_nm_from_upfl(upfl: f64) -> Option<u32> {
        const UPFL_MIN_PX_PER_MM: f64 = 50.0;
        const UPFL_MAX_PX_PER_MM: f64 = 2000.0;
        if !upfl.is_finite() || !(UPFL_MIN_PX_PER_MM..=UPFL_MAX_PX_PER_MM).contains(&upfl) {
            return None;
        }
        Some((1_000_000.0 / upfl).round() as u32)
    }

    /// Emit a PixelPitch derived from `upfl`, but never over an existing one.
    ///
    /// `util::insert_tag` is a plain map insert, so an unguarded write would
    /// replace the hardcoded pitches of the Pocket Cinema / Micro Studio models
    /// -- all of which do resolve through camera_db and therefore do reach this
    /// code path. Those measured values stay authoritative.
    fn emit_derived_pixel_pitch(map: &mut GroupedTagMap, upfl: f64, options: &crate::InputOptions) {
        let already_present = map
            .get(&GroupId::Imager)
            .map_or(false, |m| m.contains_key(&TagId::PixelPitch));
        if already_present {
            return;
        }
        if let Some(pitch_nm) = Self::pixel_pitch_nm_from_upfl(upfl) {
            util::insert_tag(map, tag!(parsed GroupId::Imager, TagId::PixelPitch, "Pixel pitch", u32x2, |v| format!("{v:?}"), (pitch_nm, pitch_nm), vec![]), options);
        }
    }

    /// Map a Video Assist source camera manufacturer to its camera_db brand key
    /// (the per-brand JSON filename stem, uppercased). Panasonic cameras live in
    /// lumix.json, so they map to "LUMIX"; every other manufacturer passes through
    /// as its own name uppercased (e.g. "Sony" -> "SONY"). An unknown brand simply
    /// misses in `process_model` and the caller falls back to a no-op.
    fn source_db_brand(manufacturer: &str) -> String {
        match manufacturer.trim().to_ascii_lowercase().as_str() {
            "panasonic" => "LUMIX".to_string(),
            other => other.to_uppercase(),
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
        // First valid per-frame focal length (35mm-equivalent for Video Assist
        // source-camera BRAW), captured for the source-camera lens synthesis below.
        let mut equiv_focal: Option<f32> = None;

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

        // Emit unified creation-date tags from the container mvhd time (UTC).
        // BRAW has no timezone field, so write as UTC (tz=None) -> CreationDate == CreationDateUtc,
        // matching the Canon mvhd-fallback semantics. No subsec: mvhd is whole-second precision.
        if let Some(ref creation) = self.creation_date {
            util::write_creation_date_tags(&mut map, creation, None, None, &options);
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
                                    // BRAW states how many pixels were read out, never how
                                    // wide those pixels are, so the file carries no pitch and
                                    // no millimetre quantity at all. Without PixelPitch the
                                    // consumer's lens_params gate (pixel_pitch +
                                    // capture_area_size + focal_length) cannot be satisfied
                                    // and the per-frame camera matrix falls back to a default.
                                    // The pitch is exactly the reciprocal of the pixels-per-mm
                                    // we just derived, so emit it here and reuse the path RED
                                    // and the Pocket Cinema models already take.
                                    //
                                    // This is the inverse of the `upfl = 1e6 / pitch` fallback
                                    // further below; the two are mutually exclusive because
                                    // each only fires when the other's output is absent.
                                    Self::emit_derived_pixel_pitch(&mut map, unit_px_fl, &options);
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

        // Fallback: if no Custom("unit_pixel_focal_length") was emitted but
        // PixelPitch is present (e.g. Pocket Cinema 4K/6K/6K G2 / Micro Studio
        // 4K G2 with no camera_db entry), derive upfl = 1e6 / px_nm so manual
        // focal length downstream still produces a valid camera_matrix. Mirrors
        // RED's fallback in red/mod.rs.
        let upfl_already_set = map
            .get(&GroupId::Lens)
            .map_or(false, |m| m.contains_key(&TagId::Custom("unit_pixel_focal_length".into())));
        if !upfl_already_set {
            if let Some(pp) = map
                .get(&GroupId::Imager)
                .and_then(|m| m.get_t(TagId::PixelPitch) as Option<&(u32, u32)>)
                .copied()
            {
                if pp.0 > 0 {
                    let unit_px_fl = 1_000_000.0_f64 / pp.0 as f64;
                    util::insert_tag(&mut map, tag!(parsed GroupId::Lens, TagId::Custom("unit_pixel_focal_length".into()), "Pixel focal length per mm", f64, |v| format!("{:.4}", v), unit_px_fl, vec![]), &options);
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
                        if equiv_focal.is_none() && v > 0.0 { equiv_focal = Some(v); }
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

        // Video Assist BRAW recorded from a NON-BMD source camera (e.g. Panasonic
        // S1H over RAW HDMI). The real-BMD camera_db block above is skipped because
        // `original_manufacturer` is Some(...). Synthesize lens calibration from the
        // SOURCE camera's camera_db entry (routed by brand, e.g. Panasonic -> LUMIX)
        // so the downstream auto-lens path can build a camera matrix.
        //
        // Why this is safe geometrically: BRAW is the native sensor readout, so the
        // recorded resolution equals the captured sensor region (no downsampling),
        // hence `scale_35mm = full_w / captured_w` is well-defined. focal is computed
        // full-frame referenced (the BRAW focal_length is the 35mm-equivalent, so the
        // crop cancels: fx = equiv * res_w / 36); only readout depends on the crop.
        if let Some(mfr) = self.original_manufacturer.clone() {
            if let Some(db_path) = &options.camera_db_path {
                if let Ok(db) = crate::camera_db::CameraDatabase::load(db_path) {
                    let brand = Self::source_db_brand(&mfr);
                    let raw_name = self.model.as_deref().unwrap_or("").to_string();
                    if let Some((model_name, model_data)) = db.process_model(&brand, &raw_name, &mut map, &options) {
                        self.model = Some(model_name.to_string());
                        let sensor_w = model_data.sw;

                        // captured width/height = sensor_area_captured (BRAW native readout == output)
                        let cap = map.get(&GroupId::Imager)
                            .and_then(|m| m.get_t(TagId::CaptureAreaSize) as Option<&(f32, f32)>)
                            .copied();
                        let captured_w = cap.map(|v| v.0 as f64).filter(|w| *w > 0.0);
                        let res_w = captured_w.map(|w| w.round() as u32).unwrap_or(0);
                        let res_h = cap.map(|v| v.1.round() as u32).unwrap_or(0);

                        // scale_35mm = full_w / captured_w, full_w = sqrt(pc * 1.5) (3:2 full sensor)
                        let scale_35mm = match (captured_w, model_data.extra.get("pc").and_then(|v| v.as_u64())) {
                            (Some(cw), Some(pc)) if pc > 0 && cw > 0.0 => Some((pc as f64 * 1.5).sqrt() / cw),
                            _ => None,
                        };
                        if let Some(scale) = scale_35mm {
                            util::insert_tag(&mut map, tag!(parsed GroupId::Default, TagId::Custom("crop_factor".into()), "Crop factor", f64, |v| format!("{:.4}", v), scale, vec![]), &options);
                        }

                        // focal: full-frame referenced (do NOT multiply by scale_35mm)
                        let upfl_set = map.get(&GroupId::Lens).map_or(false, |m| m.contains_key(&TagId::Custom("unit_pixel_focal_length".into())));
                        if res_w > 0 && !upfl_set {
                            let unit_px_fl = res_w as f64 / 36.0;
                            util::insert_tag(&mut map, tag!(parsed GroupId::Lens, TagId::Custom("unit_pixel_focal_length".into()), "Pixel focal length per mm", f64, |v| format!("{:.4}", v), unit_px_fl, vec![]), &options);

                            let pfl_set = map.get(&GroupId::Lens).map_or(false, |m| m.contains_key(&TagId::PixelFocalLength));
                            if !pfl_set {
                                if let Some(fl) = equiv_focal.filter(|f| *f > 5.0) {
                                    let px_fl = fl as f64 * res_w as f64 / 36.0;
                                    util::insert_tag(&mut map, tag!(parsed GroupId::Lens, TagId::PixelFocalLength, "Pixel focal length", f32, |v| format!("{:.2}", v), px_fl as f32, vec![]), &options);
                                }
                            }
                        }

                        // readout: crop-aware via the existing scale_sensor_norm step.
                        // Treat Some(0.0) as unset: Video Assist BRAW reports
                        // sensor_line_time=0, so `parse_meta` already set
                        // frame_readout_time to Some(0.0) above — recompute it from the
                        // source camera's readout table instead of leaving it at 0.
                        if self.frame_readout_time.map_or(true, |v| v <= 0.0) {
                            let tags = std::collections::HashMap::new();
                            let s35 = scale_35mm.unwrap_or(0.0);
                            let fps = frame_rate.unwrap_or(0.0);
                            if let Some(rt) = db.process_readout(&brand, model_name, res_w, res_h, fps, s35, sensor_w, &tags, &mut map, &options) {
                                self.frame_readout_time = Some(rt);
                            }
                        }
                    }
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
        // Also picks up ImageWidth/ImageLength (0x0100/0x0101) as a resolution
        // fallback: util::get_video_metadata below only understands MXF/BRAW/MP4
        // containers and always fails on TIFF, which would leave resolution_w=0
        // and skip the camera_db upfl derivation entirely for CinemaDNG.
        let mut tiff_width = 0u32;
        let mut tiff_height = 0u32;
        let is_tiff = tiff_ifd::is_tiff_header(&all);
        if model_name.is_none() && is_tiff {
            // First scalar of a SHORT (3) or LONG (4) entry
            fn tiff_uint(typ: u16, value_data: &[u8], is_le: bool) -> u32 {
                match typ {
                    3 => tiff_ifd::read_u16(value_data, 0, is_le).map(|v| v as u32).unwrap_or(0),
                    4 => tiff_ifd::read_u32(value_data, 0, is_le).unwrap_or(0),
                    _ => 0,
                }
            }
            let is_le = tiff_ifd::detect_byte_order(&all).unwrap_or(true);
            if let Some(ifd0_offset) = tiff_ifd::read_u32(&all, 4, is_le) {
                let mut unique_model = None;
                let mut model_tag = None;
                tiff_ifd::parse_ifd_entries(&all, ifd0_offset as usize, is_le,
                    &mut |tag, typ, count, value_data, _value_offset| {
                        match tag {
                            0xC614 => { // UniqueCameraModel (preferred)
                                let s = tiff_ifd::read_string(value_data, count);
                                if s.contains("Blackmagic") { unique_model = Some(s); }
                            }
                            0x0110 => { // Model (fallback)
                                let s = tiff_ifd::read_string(value_data, count);
                                if s.contains("Blackmagic") { model_tag = Some(s); }
                            }
                            0x0100 => { tiff_width  = tiff_uint(typ, value_data, is_le); } // ImageWidth
                            0x0101 => { tiff_height = tiff_uint(typ, value_data, is_le); } // ImageLength
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
        let mut resolution_w = video_md.as_ref().map(|v| v.width as u32).unwrap_or(0);
        let mut resolution_h = video_md.as_ref().map(|v| v.height as u32).unwrap_or(0);
        // TIFF/DNG fallback: there is no QuickTime track to read the size from.
        // Only takes over when the container gave nothing (resolution_w == 0),
        // so MOV/MP4 output is unchanged by control flow, not by coincidence.
        // upfl does not depend on which IFD this size came from (thumbnail or
        // raw): resolution_w cancels out in upfl = w / (sw * w / max_res).
        // The resolution-segmented crop/readout tables are NOT insensitive to
        // it -- before adding any such rule for a CinemaDNG model, follow the
        // SubIFD (NewSubfileType=0) to get the real raw dimensions first.
        if resolution_w == 0 && tiff_width > 0 {
            resolution_w = tiff_width;
            resolution_h = tiff_height;
        }
        let container_fps = video_md.as_ref().map(|v| v.fps).unwrap_or(0.0);
        if container_fps > 0.0 {
            util::insert_tag(&mut map, tag!(parsed GroupId::Default, TagId::FrameRate, "Frame rate", f64, |v| format!("{:?}", v), container_fps, vec![]), &options);
        } else if let Some(v) = md.get("com.blackmagic-design.projectFPS").and_then(|v| v.as_i64()) {
            if v > 0 {
                util::insert_tag(&mut map, tag!(parsed GroupId::Default, TagId::FrameRate, "Frame rate", f64, |v| format!("{:?}", v), v as f64, vec![]), &options);
            }
        }
        // Table-lookup fps: single-frame TIFF/DNG carries no frame rate, so the
        // caller-supplied value is the only way the fps-segmented camera_db
        // tables (crop / readout) can resolve a column. Lookup only -- the
        // FrameRate tag above stays container-sourced.
        let fps = if container_fps > 0.0 {
            container_fps
        } else {
            options.video_fps.filter(|v| *v > 0.0).unwrap_or(0.0)
        };

        // camera_db integration
        if let Some(db_path) = &options.camera_db_path {
            if let Ok(db) = crate::camera_db::CameraDatabase::load(db_path) {
                let raw_name = self.model.as_deref().unwrap_or("");
                if let Some((model_name, model_data)) = db.process_model("BLACKMAGIC", raw_name, &mut map, &options) {
                    self.model = Some(model_name.to_string());
                    let sensor_w = model_data.sw;

                    // Use SensorAreaCaptured width if available (handles crop implicitly),
                    // otherwise compute effective sensor width from max_resolution_w (ref NiYien Tool bmd.cpp).
                    // Scale whenever max_res is known, not only for resolution_w < max_res:
                    // upfl then reduces to max_res / sensor_w for any recorded width.
                    // CinemaDNG frames read out overscan (BMPCC records 1952 px while its
                    // 12.48 mm covers the 1920 active columns); the old strict-less-than
                    // skipped scaling there and overstated upfl by that overscan ratio.
                    // resolution_w == max_res makes both forms identical, and MOV never
                    // records wider than max_res, so this only changes the overscan case.
                    let effective_sensor_w = if let Some(captured_w) = sensor_area_captured_w {
                        captured_w as f64
                    } else if resolution_w > 0 {
                        let max_res = Self::max_resolution_w(model_name);
                        if max_res > 0 {
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

        // Fallback (mirrors RED): if camera_db didn't supply
        // unit_pixel_focal_length but a hardcoded PixelPitch was injected
        // earlier, derive upfl = 1e6 / px_nm.
        let upfl_already_set = map
            .get(&GroupId::Lens)
            .map_or(false, |m| m.contains_key(&TagId::Custom("unit_pixel_focal_length".into())));
        if !upfl_already_set {
            if let Some(pp) = map
                .get(&GroupId::Imager)
                .and_then(|m| m.get_t(TagId::PixelPitch) as Option<&(u32, u32)>)
                .copied()
            {
                if pp.0 > 0 {
                    let unit_px_fl = 1_000_000.0_f64 / pp.0 as f64;
                    util::insert_tag(&mut map, tag!(parsed GroupId::Lens, TagId::Custom("unit_pixel_focal_length".into()), "Pixel focal length per mm", f64, |v| format!("{:.4}", v), unit_px_fl, vec![]), &options);
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
        // TIFF/DNG has no QuickTime metadata track: get_metadata_track_samples
        // fails at parse_mp4 and `?` would discard the whole tag map built above
        // (the caller swallows the Err into samples=None). This, not the missing
        // resolution alone, is why CinemaDNG used to come back with samples=0.
        if !is_tiff {
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
        }

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
        // Container creation time from mvhd (UTC; BRAW carries no timezone field).
        // Captured from the same buffer here; parse() consumes it to emit CreationDate tags.
        self.creation_date = util::extract_mvhd_creation_time(&all);
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

#[cfg(test)]
mod tests {
    use super::*;

    fn pitch_of(map: &GroupedTagMap) -> Option<(u32, u32)> {
        map.get(&GroupId::Imager)
            .and_then(|m| m.get_t(TagId::PixelPitch) as Option<&(u32, u32)>)
            .copied()
    }

    fn hardcoded_pitch_map(nm: u32) -> GroupedTagMap {
        let mut map = GroupedTagMap::new();
        let options = crate::InputOptions::default();
        util::insert_tag(&mut map, tag!(parsed GroupId::Imager, TagId::PixelPitch, "Pixel pitch", u32x2, |v| format!("{v:?}"), (nm, nm), vec![]), &options);
        map
    }

    // ---- max_resolution_w: sensor aspect ratio must match the listed resolution ----

    #[test]
    fn max_resolution_w_matches_sensor_aspect_ratio() {
        // 18.96 x 10.00 -> 1.896, and 18.96mm / 4628nm = 4096.8 px
        assert_eq!(BlackmagicBraw::max_resolution_w("BMPCC 4K"), 4096);
        // Micro Studio Camera 4K G2 shares the BMPCC 4K sensor
        assert_eq!(BlackmagicBraw::max_resolution_w("BMSC 4K G2"), 4096);
        // 21.12 x 11.88 -> 1.778 = 3840/2160. bmd.cpp lumps this with BMPCC 4K
        // under max_frame = 4096; that is the source file's own error.
        assert_eq!(BlackmagicBraw::max_resolution_w("BMCC 4K"), 3840);
        assert_eq!(BlackmagicBraw::max_resolution_w("BMCC 6K"), 6048);
        assert_eq!(BlackmagicBraw::max_resolution_w("BMPCC 6K"), 6144);
        // Original Cinema Camera 2.5K: 15.81 x 8.88 -> 1.780 = 2432/1366
        assert_eq!(BlackmagicBraw::max_resolution_w("BMCC"), 2432);
    }

    #[test]
    fn max_resolution_w_covers_every_camera_db_model() {
        // A miss returns 0, and the caller's `.max(1)` then collapses
        // unit_pixel_focal_length to 1/sensor_width. Keep this list in sync with
        // camera_db/blackmagic.json `models`.
        for model in [
            "BMPCC", "BMCC", "BMCC 4K", "BMCC 6K", "BMPCC 4K", "BMSC 4K G2", "BMPCC 6K",
            "URSA Mini 4K", "URSA Mini 4.6K", "URSA Mini Pro 4.6K G2", "URSA Mini Pro 12K",
        ] {
            assert!(BlackmagicBraw::max_resolution_w(model) > 0, "{model} missing from max_resolution_w");
        }
    }

    // ---- pixel_pitch_nm_from_upfl ----

    #[test]
    fn pixel_pitch_from_upfl_inverts_the_scale() {
        // BMCC 6K: 6048 px / 36.0 mm -> 5952.38 nm, i.e. 36mm/6048px
        assert_eq!(BlackmagicBraw::pixel_pitch_nm_from_upfl(168.0), Some(5952));
        // BMPCC 6K's measured 3759 nm and BMPCC 4K's 4628 nm round-trip to
        // within a nanometre of the camera_db-derived scale.
        assert_eq!(BlackmagicBraw::pixel_pitch_nm_from_upfl(6144.0 / 23.10), Some(3760));
        assert_eq!(BlackmagicBraw::pixel_pitch_nm_from_upfl(4096.0 / 18.96), Some(4629));
    }

    #[test]
    fn pixel_pitch_from_upfl_rejects_implausible_values() {
        // The collapse that an unlisted max_resolution_w used to produce
        // ("BMSC 4K G2": 1/18.96). Emitting 18.96 mm per pixel would skew
        // every rendered frame.
        assert_eq!(BlackmagicBraw::pixel_pitch_nm_from_upfl(1.0 / 18.96), None);
        assert_eq!(BlackmagicBraw::pixel_pitch_nm_from_upfl(3000.0), None);
        assert_eq!(BlackmagicBraw::pixel_pitch_nm_from_upfl(0.0), None);
        assert_eq!(BlackmagicBraw::pixel_pitch_nm_from_upfl(f64::NAN), None);
    }

    // ---- emit_derived_pixel_pitch ----

    #[test]
    fn derived_pixel_pitch_is_written_when_absent() {
        let mut map = GroupedTagMap::new();
        BlackmagicBraw::emit_derived_pixel_pitch(&mut map, 168.0, &crate::InputOptions::default());
        assert_eq!(pitch_of(&map), Some((5952, 5952)));
    }

    #[test]
    fn derived_pixel_pitch_never_overwrites_a_hardcoded_one() {
        // insert_tag is a plain map insert, and the Pocket Cinema models do
        // resolve through camera_db, so they do reach this code path.
        let mut map = hardcoded_pitch_map(3759);
        BlackmagicBraw::emit_derived_pixel_pitch(&mut map, 6144.0 / 23.10, &crate::InputOptions::default());
        assert_eq!(pitch_of(&map), Some((3759, 3759)));

        let mut map = hardcoded_pitch_map(4628);
        BlackmagicBraw::emit_derived_pixel_pitch(&mut map, 4096.0 / 18.96, &crate::InputOptions::default());
        assert_eq!(pitch_of(&map), Some((4628, 4628)));
    }

    #[test]
    fn derived_pixel_pitch_is_skipped_when_upfl_is_implausible() {
        let mut map = GroupedTagMap::new();
        BlackmagicBraw::emit_derived_pixel_pitch(&mut map, 1.0 / 18.96, &crate::InputOptions::default());
        assert_eq!(pitch_of(&map), None, "an implausible scale must leave the consumer on its previous fallback");
    }

    // ---- CinemaDNG: TIFF resolution fallback + camera_db upfl (feedback 20260807-441084d0) ----

    fn push_ifd_entry(d: &mut Vec<u8>, tag: u16, typ: u16, cnt: u32, val: u32) {
        d.extend_from_slice(&tag.to_le_bytes());
        d.extend_from_slice(&typ.to_le_bytes());
        d.extend_from_slice(&cnt.to_le_bytes());
        d.extend_from_slice(&val.to_le_bytes());
    }

    fn synthetic_dng(model: &str, width: u32, height: u32) -> Vec<u8> {
        // Minimal little-endian TIFF: IFD0 with ImageWidth (LONG), ImageLength
        // (SHORT, to exercise both scalar types) and UniqueCameraModel (ASCII,
        // indirect). header(8) + count(2) + 3*12 + next(4) = 50.
        let mut model_z = model.as_bytes().to_vec();
        model_z.push(0);
        let str_offset = 50u32;
        let mut d = Vec::new();
        d.extend_from_slice(b"II");
        d.extend_from_slice(&42u16.to_le_bytes());
        d.extend_from_slice(&8u32.to_le_bytes());
        d.extend_from_slice(&3u16.to_le_bytes());
        push_ifd_entry(&mut d, 0x0100, 4, 1, width);                         // ImageWidth
        push_ifd_entry(&mut d, 0x0101, 3, 1, height);                        // ImageLength
        push_ifd_entry(&mut d, 0xC614, 2, model_z.len() as u32, str_offset); // UniqueCameraModel
        d.extend_from_slice(&0u32.to_le_bytes());
        assert_eq!(d.len(), str_offset as usize);
        d.extend_from_slice(&model_z);
        d
    }

    fn parse_synthetic_dng(bytes: &[u8], db_json: &str, db_dir_name: &str, video_fps: Option<f64>) -> (BlackmagicBraw, Vec<SampleInfo>) {
        let db_dir = std::env::temp_dir().join(db_dir_name);
        std::fs::create_dir_all(&db_dir).unwrap();
        std::fs::write(db_dir.join("blackmagic.json"), db_json).unwrap();
        let options = crate::InputOptions {
            camera_db_path: Some(db_dir.to_string_lossy().into_owned()),
            video_fps,
            ..Default::default()
        };
        assert!(BlackmagicBraw::detect(bytes, "test.dng", &options).is_some(), "synthetic DNG must pass detect");
        let mut bmd = BlackmagicBraw::default();
        let mut stream = Cursor::new(bytes);
        let samples = bmd.parse(&mut stream, bytes.len(), |_| (), Arc::new(AtomicBool::new(false)), options)
            .expect("TIFF input must not error out of parse_non_braw");
        (bmd, samples)
    }

    fn upfl_of(samples: &[SampleInfo]) -> f64 {
        let map = samples.first().and_then(|s| s.tag_map.as_ref()).expect("tag map must survive");
        *(map.get(&GroupId::Lens)
            .and_then(|m| m.get_t(TagId::Custom("unit_pixel_focal_length".into())) as Option<&f64>)
            .expect("upfl must be emitted"))
    }

    #[test]
    fn cinemadng_parse_derives_upfl_from_tiff_fallback() {
        // Without the TIFF fallback, util::get_video_metadata fails on TIFF and
        // resolution_w stays 0, so the camera_db block never emits upfl; and
        // without the is_tiff guard, get_metadata_track_samples errors out and
        // the whole tag map is discarded (samples=None at the caller).
        let bytes = synthetic_dng("Blackmagic Cinema Camera", 2432, 1366);
        let (bmd, samples) = parse_synthetic_dng(&bytes,
            r#"{ "version": 1, "aliases": {}, "models": { "BMCC": { "sw": 15.81 } } }"#,
            "tp-test-camera-db-bmcc25k", None);

        assert_eq!(bmd.model.as_deref(), Some("BMCC"));
        let upfl = upfl_of(&samples);
        // Recorded width == max_resolution_w, so upfl reduces to max_res / sw.
        let expected = 2432.0 / 15.81f32 as f64;
        assert!((upfl - expected).abs() < 1e-6, "upfl = {upfl}, expected ~{expected}");
    }

    const BMCC_DB_WITH_READOUT: &str = r#"{
        "version": 1, "aliases": {},
        "models": { "BMCC": { "sw": 15.81 } },
        "readout": {
            "columns": ["1K30", "1K24"],
            "data": { "BMCC": [-25, -25] }
        }
    }"#;

    #[test]
    fn cinemadng_readout_lookup_uses_the_caller_supplied_fps() {
        // A single DNG frame carries no frame rate, so without the caller's fps
        // the fps-segmented readout table can never resolve a column. The
        // negative table value is the low-confidence convention (abs() on read).
        let bytes = synthetic_dng("Blackmagic Cinema Camera", 2432, 1366);
        let (bmd, samples) = parse_synthetic_dng(&bytes, BMCC_DB_WITH_READOUT,
            "tp-test-camera-db-bmcc25k-ro", Some(30.0));

        assert_eq!(bmd.frame_readout_time, Some(25.0));
        let map = samples.first().and_then(|s| s.tag_map.as_ref()).unwrap();
        let ro: f64 = *(map.get(&GroupId::Imager)
            .and_then(|m| m.get_t(TagId::FrameReadoutTime) as Option<&f64>)
            .expect("readout tag must be emitted"));
        assert!((ro - 25.0).abs() < 1e-9);
        // The FrameRate tag must stay container-sourced: the fallback fps is
        // for table lookups only, and a DNG frame has no container frame rate.
        assert!(map.get(&GroupId::Default).map_or(true, |m| (m.get_t(TagId::FrameRate) as Option<&f64>).is_none()),
            "caller fps must not fabricate a FrameRate tag");
    }

    #[test]
    fn cinemadng_readout_lookup_stays_empty_without_caller_fps() {
        let bytes = synthetic_dng("Blackmagic Cinema Camera", 2432, 1366);
        let (bmd, samples) = parse_synthetic_dng(&bytes, BMCC_DB_WITH_READOUT,
            "tp-test-camera-db-bmcc25k-nofps", None);

        assert_eq!(bmd.frame_readout_time, None);
        let map = samples.first().and_then(|s| s.tag_map.as_ref()).unwrap();
        assert!(map.get(&GroupId::Imager).map_or(true, |m| (m.get_t(TagId::FrameReadoutTime) as Option<&f64>).is_none()));
        // upfl is fps-independent and must still be there.
        let upfl = upfl_of(&samples);
        assert!((upfl - 2432.0 / 15.81f32 as f64).abs() < 1e-6);
    }

    #[test]
    fn cinemadng_overscan_readout_still_yields_the_sensor_scale() {
        // BMPCC CinemaDNG records 1952 px overscan while the 12.48 mm sensor
        // width covers the 1920 active columns (verified on real footage).
        // upfl must stay max_res / sw regardless of the recorded width; the old
        // strict-less-than scaling condition overstated it by the overscan
        // ratio (156.41 instead of 153.85).
        let bytes = synthetic_dng("Blackmagic Pocket Cinema Camera", 1952, 1112);
        let (bmd, samples) = parse_synthetic_dng(&bytes,
            r#"{ "version": 1, "aliases": {}, "models": { "BMPCC": { "sw": 12.48 } } }"#,
            "tp-test-camera-db-bmpcc-overscan", None);

        assert_eq!(bmd.model.as_deref(), Some("BMPCC"));
        let upfl = upfl_of(&samples);
        let expected = 1920.0 / 12.48f32 as f64;
        assert!((upfl - expected).abs() < 1e-6, "upfl = {upfl}, expected ~{expected}");
    }
}
