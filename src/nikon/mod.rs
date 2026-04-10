// SPDX-License-Identifier: MIT OR Apache-2.0
// Copyright © 2025 Adrian <adrian.eddy at gmail>

use std::io::*;
use std::sync::{ Arc, atomic::AtomicBool };

use byteorder::{ BigEndian, ReadBytesExt };
use crate::*;
use crate::tags_impl::*;
use memchr::memmem;


#[derive(Default)]
pub struct Nikon {
    pub model: Option<String>,
    pub lens: Option<String>,
    record_frame_rate: Option<f64>,
    playback_frame_rate: Option<f64>,
    frame_readout_time: Option<f64>,
    mvhd_creation_time: Option<String>,
    electronic_vr: Option<u32>,
    vibration_reduction: Option<u8>,
    is_r3d: bool,
}
impl Nikon {
    pub fn camera_type(&self) -> String {
        "Nikon".to_owned()
    }
    pub fn has_accurate_timestamps(&self) -> bool {
        true
    }
    pub fn possible_extensions() -> Vec<&'static str> {
        vec!["mp4", "mov", "nev", "r3d"]
    }
    pub fn frame_readout_time(&self) -> Option<f64> {
        self.frame_readout_time
    }
    pub fn normalize_imu_orientation(v: String) -> String {
        v
    }

    pub fn detect<P: AsRef<std::path::Path>>(buffer: &[u8], filepath: P, _options: &crate::InputOptions) -> Option<Self> {
        if memmem::find(buffer, b"Nikon").is_some() && memmem::find(buffer, b"NCTG").is_some() {
            let is_r3d = filepath.as_ref().extension()
                .map(|e| e.to_ascii_lowercase() == "r3d")
                .unwrap_or(false);
            return Some(Self {
                model: None,
                lens: None,
                record_frame_rate: None,
                playback_frame_rate: None,
                frame_readout_time: None,
                mvhd_creation_time: util::extract_mvhd_creation_time(buffer),
                electronic_vr: None,
                vibration_reduction: None,
                is_r3d,
            });
        }
        None
    }

    pub fn parse<T: Read + Seek, F: Fn(f64)>(&mut self, stream: &mut T, size: usize, progress_cb: F, cancel_flag: Arc<AtomicBool>, options: crate::InputOptions) -> Result<Vec<SampleInfo>> {
        let mut samples = Vec::new();
        let mut first_map = GroupedTagMap::new();

        while let Ok((typ, _offs, size, header_size)) = util::read_box(stream) {
            if size == 0 || typ == 0 { break; }
            let org_pos = stream.stream_position()?;

            if cancel_flag.load(std::sync::atomic::Ordering::Relaxed) { break; }

            if typ == fourcc("moov") || typ == fourcc("udta") {
                continue; // go inside these boxes
            } else {
                if typ == fourcc("NCDT") {
                    let mut buf = vec![0u8; size as usize - header_size as usize];
                    stream.read_exact(&mut buf)?;
                    self.parse_nev_clip_metadata(&buf[26..], &mut first_map, &options).unwrap();
                }

                stream.seek(SeekFrom::Start(org_pos + size - header_size as u64))?;
            }
        }
        stream.seek(SeekFrom::Start(0))?;

        // NEV frames are ~3.3MB each but metadata (NRFH) is in the first ~1KB.
        // Limit read size to avoid loading full RAW pixel data for every frame.
        let frame_count = std::cell::Cell::new(0u64);
        util::get_track_samples(stream, size, mp4parse::TrackType::Video, true, Some(4096), |mut info: SampleInfo, data: &[u8], file_position: u64, _video_md: Option<&VideoMetadata>| {
            frame_count.set(frame_count.get() + 1);
            if size > 0 {
                progress_cb(file_position as f64 / size as f64);
            }

            if data.len() > 8 {
                let mut map = if info.sample_index == 0 { first_map.clone() } else { GroupedTagMap::new() };
                self.parse_nev_frame_metadata(&data, &mut map, &options).unwrap();
                info.tag_map = Some(map);
                samples.push(info);
            }
        }, cancel_flag)?;

        if samples.is_empty() && !first_map.is_empty() {
            samples.push(SampleInfo {
                tag_map: Some(first_map),
                ..Default::default()
            });
        }

        // Get video track metadata (resolution, fps)
        stream.seek(SeekFrom::Start(0))?;
        let video_md = util::get_video_metadata(stream, size).ok();

        self.process_map(&mut samples, &options, video_md.as_ref());

        Ok(samples)
    }

    pub fn parse_nev_clip_metadata(&mut self, data: &[u8], map: &mut GroupedTagMap, options: &crate::InputOptions) -> Result<()> {
        let mut md = serde_json::Map::<String, serde_json::Value>::new();
        let mut cursor = Cursor::new(data);
        let len = data.len() as u64;

        while cursor.position() + 8 <= len {
            // Read tag header
            let tag_id = cursor.read_u32::<BigEndian>()?;
            let type_id = cursor.read_u16::<BigEndian>()?;
            let count = cursor.read_u16::<BigEndian>()? as usize;

            // Calculate value size based on type
            let type_size: usize = match type_id {
                1 | 2 | 6 | 7 => 1,  // BYTE, ASCII, SBYTE, UNDEFINED
                3 | 8 => 2,          // SHORT, SSHORT
                4 | 9 | 11 => 4,     // LONG, SLONG, FLOAT
                5 | 10 | 12 => 8,    // RATIONAL, SRATIONAL, DOUBLE
                _ => 1,
            };
            let value_size = count * type_size;

            if cursor.position() + value_size as u64 > len { break; }

            let mut value_bytes = vec![0u8; value_size];
            cursor.read_exact(&mut value_bytes)?;

            // Helper: read string
            let as_string = || -> String {
                let end = value_bytes.iter().position(|&b| b == 0).unwrap_or(value_bytes.len());
                String::from_utf8_lossy(&value_bytes[..end]).to_string()
            };

            let as_rational = || -> Option<f64> {
                if value_bytes.len() >= 8 {
                    let mut rdr = Cursor::new(&value_bytes);
                    let num = rdr.read_u32::<BigEndian>().ok()?;
                    let den = rdr.read_u32::<BigEndian>().ok()?;
                    if den > 0 { Some(num as f64 / den as f64) } else { None }
                } else { None }
            };

            // Helper: read u16/u32
            let as_u32 = || -> Option<u32> {
                let mut rdr = Cursor::new(&value_bytes);
                match type_id {
                    3 | 8 => rdr.read_u16::<BigEndian>().ok().map(|v| v as u32),
                    4 | 9 => rdr.read_u32::<BigEndian>().ok(),
                    _ => None,
                }
            };
            let as_i32 = || -> Option<i32> {
                let mut rdr = Cursor::new(&value_bytes);
                match type_id {
                    8 => rdr.read_i16::<BigEndian>().ok().map(|v| v as i32),  // SSHORT
                    9 => rdr.read_i32::<BigEndian>().ok(),                    // SLONG
                    6 => rdr.read_i8().ok().map(|v| v as i32),                // SBYTE
                    _ => None,
                }
            };

            let as_f64 = || -> Option<f64> {
                let mut rdr = Cursor::new(&value_bytes);
                match type_id {
                    5 => { // RATIONAL (u32/u32)
                        let num = rdr.read_u32::<BigEndian>().ok()?;
                        let den = rdr.read_u32::<BigEndian>().ok()?;
                        (den != 0).then(|| num as f64 / den as f64)
                    }
                    10 => { // SRATIONAL (i32/i32)
                        let num = rdr.read_i32::<BigEndian>().ok()?;
                        let den = rdr.read_i32::<BigEndian>().ok()?;
                        (den != 0).then(|| num as f64 / den as f64)
                    }
                    11 => rdr.read_f32::<BigEndian>().ok().map(|v| v as f64), // FLOAT
                    12 => rdr.read_f64::<BigEndian>().ok(),                   // DOUBLE
                    1 | 7 => value_bytes.get(0).copied().map(|v| v as f64),    // BYTE/UNDEFINED
                    3 => rdr.read_u16::<BigEndian>().ok().map(|v| v as f64),   // SHORT
                    4 => rdr.read_u32::<BigEndian>().ok().map(|v| v as f64),   // LONG
                    8 => rdr.read_i16::<BigEndian>().ok().map(|v| v as f64),   // SSHORT
                    9 => rdr.read_i32::<BigEndian>().ok().map(|v| v as f64),   // SLONG
                    _ => None,
                }
            };

            // (optional) timecode decode helper: try ASCII, else treat as frame count
            let frames_to_timecode = |frames: u32, fps: f64| -> String {
                let fps_i = fps.round().max(1.0) as u32;
                let ff = frames % fps_i;
                let total_sec = frames / fps_i;
                let ss = total_sec % 60;
                let mm = (total_sec / 60) % 60;
                let hh = (total_sec / 3600) % 100;
                format!("{:02}:{:02}:{:02}:{:02}", hh, mm, ss, ff)
            };

            // Match tags and insert
            match tag_id {
                0x0000_0002 => {
                    let model = as_string();
                    self.model = Some(model.clone());
                    md.insert("camera_model".into(), model.into());
                }
                0x0000_0016 => { // Framerate (playback)
                    if let Some(fps) = as_rational() {
                        self.playback_frame_rate = Some(fps);
                        self.record_frame_rate = Some(fps);
                        util::insert_tag(map, tag!(parsed GroupId::Default, TagId::FrameRate, "Frame rate", f64, |v| format!("{:.3}", v), fps, vec![]), options);
                        md.insert("framerate".into(), fps.into());
                    }
                }
                0x0000_0017 => { // Record Framerate (actual sensor capture rate)
                    if let Some(fps) = as_rational() {
                        self.record_frame_rate = Some(fps);
                        if self.playback_frame_rate.is_none() {
                            self.playback_frame_rate = Some(fps);
                            util::insert_tag(map, tag!(parsed GroupId::Default, TagId::FrameRate, "Frame rate", f64, |v| format!("{:.3}", v), fps, vec![]), options);
                        }
                        md.insert("record_frame_rate".into(), fps.into());
                    }
                }
                0x0000_0001 => { md.insert("make".into(), as_string().into()); }
                0x0000_0003 => { md.insert("camera_firmware_version".into(), as_string().into()); }
                0x0000_0011 => { md.insert("local_datetime".into(), as_string().into()); }
                0x0000_0012 => { md.insert("gmt_datetime".into(), as_string().into()); }
                0x0000_0013 => { if let Some(v) = as_u32() { md.insert("record_mode".into(), v.into()); } }
                0x0000_0014 => { if let Some(v) = as_u32() { md.insert("flip_horizontal".into(), v.into()); } }
                0x0000_0015 => { if let Some(v) = as_u32() { md.insert("flip_vertical".into(), v.into()); } }
                0x0000_0019 => { md.insert("timezone".into(), as_string().into()); }
                0x0000_0021 => { if let Some(v) = as_u32() { md.insert("color_space".into(), v.into()); } }
                0x0000_0022 => { if let Some(v) = as_u32() { md.insert("image_width".into(), v.into()); } }
                0x0000_0023 => { if let Some(v) = as_u32() { md.insert("image_height".into(), v.into()); } }
                0x0000_0024 => { if let Some(v) = as_u32() { md.insert("bits_per_component".into(), v.into()); } }
                0x0000_0025 => { if let Some(v) = as_u32() { md.insert("bit_depth".into(), v.into()); } }
                0x0000_0026 => { if let Some(v) = as_u32() { md.insert("audio_channels".into(), v.into()); } }
                0x0000_0027 => { if let Some(v) = as_u32() { md.insert("audio_format".into(), v.into()); } }
                0x0000_0031 => { if let Some(v) = as_u32() { md.insert("channel_mask".into(), v.into()); } }
                0x0000_0032 => { if let Some(v) = as_u32() { md.insert("audio_codec".into(), v.into()); } }
                0x0000_0033 => { if let Some(v) = as_u32() { md.insert("sample_size".into(), v.into()); } }
                0x0000_0034 => { if let Some(v) = as_u32() { md.insert("samplerate".into(), v.into()); } }
                0x0000_1017 => { if let Some(v) = as_u32() { md.insert("white_balance_kelvin".into(), v.into()); } }
                0x0000_101A => { if let Some(v) = as_u32() { md.insert("clip_default_color_version".into(), v.into()); } }

                // Standard EXIF tags (0x01xxxxxx = EXIF IFD prefix)
                0x0100_0112 => { if let Some(v) = as_u32() { md.insert("orientation".into(), v.into()); } }
                0x0110_829A => { // Exposure Time
                    if let Some(val) = as_rational() {
                        util::insert_tag(map, tag!(parsed GroupId::Default, TagId::ExposureTime, "Exposure time", f32, |v| format!("{:.6}", v), val as f32, vec![]), options);
                        md.insert("exposure_time".into(), val.into());
                    }
                }
                0x0110_829D => { // F-Number
                    if let Some(val) = as_rational() {
                        util::insert_tag(map, tag!(parsed GroupId::Lens, TagId::IrisFStop, "Aperture", f32, |v| format!("f/{:.1}", v), val as f32, vec![]), options);
                        md.insert("f_number".into(), val.into());
                    }
                }
                0x0110_8827 | 0x0110_8832 => { // ISO
                    if let Some(val) = as_u32() {
                        util::insert_tag(map, tag!(parsed GroupId::Default, TagId::ISOValue, "ISO", u32, |v| v.to_string(), val, vec![]), options);
                        md.insert("iso".into(), val.into());
                    }
                }
                // EXIF ExposureBiasValue (SRATIONAL) -> your SDK exposure_compensation
                0x0110_9204 => {
                    if let Some(val) = as_f64() {
                        md.insert("exposure_compensation".into(), val.into());
                    }
                }
                0x0110_920A => { // Focal Length
                    if let Some(val) = as_rational() {
                        util::insert_tag(map, tag!(parsed GroupId::Lens, TagId::FocalLength, "Focal length", f32, |v| format!("{:.1} mm", v), val as f32, vec![]), options);
                        md.insert("lens_focal_length".into(), val.into());
                    }
                }
                0x0110_8822 => { if let Some(v) = as_u32() { md.insert("exposure_program".into(), v.into()); } }
                0x0110_9207 => { if let Some(v) = as_u32() { md.insert("metering_mode".into(), v.into()); } }
                0x0110_A431 => { md.insert("camera_pin".into(), as_string().into()); }
                0x0110_A433 => { md.insert("lens_make".into(), as_string().into()); }
                0x0110_A432 => { if let Some(val) = as_rational() { md.insert("lens_info".into(), val.into()); } }
                0x0110_A435 => { md.insert("lens_serial_number".into(), as_string().into()); }
                0x0110_A434 => {
                    let name = as_string();
                    util::insert_tag(map, tag!(parsed GroupId::Lens, TagId::DisplayName, "Lens name", String, |v| v.clone(), name.clone(), vec![]), options);
                    md.insert("lens_name".into(), name.into());
                }

                // Nikon MakerNotes (0x0200xxxx prefix)
                0x0200_0005 => { md.insert("white_balance_setting".into(), as_string().trim().into()); }
                0x0200_0007 => { md.insert("focus_mode".into(), as_string().trim().into()); }
                0x0200_001B => {
                    // CropHiSpeed: 7 × u16 [crop_type, full_w, full_h, crop_w, crop_h, x_off, y_off]
                    if type_id == 3 && count >= 7 && value_bytes.len() >= 14 {
                        let mut rdr = Cursor::new(&value_bytes);
                        let crop_type = rdr.read_u16::<BigEndian>().unwrap_or(0);
                        let full_w    = rdr.read_u16::<BigEndian>().unwrap_or(0);
                        let full_h    = rdr.read_u16::<BigEndian>().unwrap_or(0);
                        let crop_w    = rdr.read_u16::<BigEndian>().unwrap_or(0);
                        let crop_h    = rdr.read_u16::<BigEndian>().unwrap_or(0);
                        let _x_offset = rdr.read_u16::<BigEndian>().unwrap_or(0);
                        let _y_offset = rdr.read_u16::<BigEndian>().unwrap_or(0);
                        md.insert("crop_hi_speed_type".into(), crop_type.into());
                        md.insert("crop_hi_speed_full_w".into(), full_w.into());
                        md.insert("crop_hi_speed_full_h".into(), full_h.into());
                        md.insert("crop_hi_speed_crop_w".into(), crop_w.into());
                        md.insert("crop_hi_speed_crop_h".into(), crop_h.into());
                        md.insert("nikon_0x1b".into(), crop_type.into());
                    } else if let Some(v) = as_u32() {
                        md.insert("nikon_0x1b".into(), v.into());
                    }
                }
                0x0200_001F => { // VRInfo binary block
                    // byte[4] = VibrationReduction: 0=n/a, 1=On, 2=Off
                    if value_bytes.len() >= 5 {
                        self.vibration_reduction = Some(value_bytes[4]);
                        md.insert("vibration_reduction".into(), (value_bytes[4] as u32).into());
                    }
                }
                0x0200_002A => { if let Some(v) = as_u32() { md.insert("nikon_0x2a".into(), v.into()); } }
                0x0200_003C => { if let Some(v) = as_u32() { md.insert("nikon_0x3c".into(), v.into()); } }
                0x0200_003F => { if let Some(val) = as_rational() { md.insert("exposure_fine_tune".into(), val.into()); } } // Possibly exposure fine tuning
                0x0200_0084 => { if let Some(val) = as_rational() { md.insert("nikon_lens_info".into(), val.into()); } }
                0x0200_00A7 => { if let Some(v) = as_u32() { md.insert("shutter_count".into(), v.into()); } }
                0x0200_00AB => { md.insert("variant_program".into(), as_string().into()); }
                0x0200_00B1 => { if let Some(v) = as_u32() { md.insert("nikon_0xb1".into(), v.into()); } }
                0x0000_0018 => { if let Some(v) = as_u32() { md.insert("audio_format".into(), v.into()); } }
                0x0000_1000 => { // StartEdgeCode
                    if type_id == 2 {
                        md.insert("start_edge_timecode".into(), as_string().into());
                    } else if let (Some(fr), Some(fps)) = (as_u32(), self.record_frame_rate) {
                        md.insert("start_edge_timecode".into(), frames_to_timecode(fr, fps).into());
                    } else if let Some(fr) = as_u32() {
                        md.insert("start_edge_timecode_frames".into(), fr.into());
                    }
                }
                0x0000_1001 => { // StartTimecode
                    if type_id == 2 {
                        md.insert("start_absolute_timecode".into(), as_string().into());
                    } else if let (Some(fr), Some(fps)) = (as_u32(), self.record_frame_rate) {
                        md.insert("start_absolute_timecode".into(), frames_to_timecode(fr, fps).into());
                    } else if let Some(fr) = as_u32() {
                        md.insert("start_absolute_timecode_frames".into(), fr.into());
                    }
                }
                0x0000_1071 => { // Model
                    let s = as_string();
                    if !s.is_empty() {
                        self.model = Some(s.clone());
                        md.insert("camera_model".into(), s.into());
                    }
                }
                0x0000_1006 => { let s = as_string(); if !s.is_empty() { md.insert("camera_pin".into(), s.into()); } } // SerialNumber
                0x0000_1023 => { let d = as_string(); if !d.is_empty() { md.insert("local_date".into(), d.into()); } } // DateCreated (YYYYMMDD)
                0x0000_1024 => { let t = as_string(); if !t.is_empty() { md.insert("local_time".into(), t.into()); } } // TimeCreated (HHMMSS)
                0x0000_1025 => { let fw = as_string(); if !fw.is_empty() { md.insert("camera_firmware_version".into(), fw.into()); } }
                0x0000_1036 => { if let Some(v) = as_f64() { md.insert("pixel_aspect_ratio".into(), v.into()); } }
                0x0000_106e => { let s = as_string(); if !s.is_empty() { md.insert("lens_mount".into(), s.into()); } }
                0x0000_1070 => { let s = as_string(); if !s.is_empty() { md.insert("lens_name".into(), s.into()); } }
                0x0000_10a1 => { let s = as_string(); if !s.is_empty() { md.insert("sensor_name".into(), s.into()); } }
                0x0000_200d => { if let Some(v) = as_f64() { md.insert("white_balance_kelvin".into(), v.into()); } }
                0x0000_403b => { if let Some(v) = as_u32() { md.insert("iso".into(), v.into()); } }
                0x0000_406a => { if let Some(v) = as_f64() { md.insert("f_number".into(), v.into()); } }
                0x0000_406b => { if let Some(v) = as_f64() { md.insert("lens_focal_length".into(), v.into()); } }
                0x0000_1013 => { // ElectronicVR: 0=Off, 1=On
                    if let Some(v) = as_u32() {
                        self.electronic_vr = Some(v);
                        md.insert("electronic_vr".into(), v.into());
                    }
                }
                _ => {
                    let key = format!("tag_0x{:08x}", tag_id);
                    match type_id {
                        2 => { md.insert(key, as_string().into()); }
                        5 | 10 | 11 | 12 => { if let Some(v) = as_f64() { md.insert(key, v.into()); } }
                        6 | 8 | 9 => { if let Some(v) = as_i32() { md.insert(key, v.into()); } }
                        1 | 3 | 4 | 7 => { if let Some(v) = as_u32() { md.insert(key, v.into()); } }
                        _ => {}
                    }
                }
            }
        }

        // Insert all metadata as JSON
        if !md.is_empty() {
            util::insert_tag(map, tag!(parsed GroupId::Default, TagId::Metadata, "Metadata", Json, |v| serde_json::to_string(v).unwrap(), serde_json::Value::Object(md), vec![]), options);
        }

        Ok(())
    }

    /// Parse per-frame metadata from NRAW frame data
    pub fn parse_nev_frame_metadata(&self, data: &[u8], map: &mut GroupedTagMap, options: &crate::InputOptions) -> Result<()> {
        let mut md = serde_json::Map::<String, serde_json::Value>::new();
        if let Some(map) = map.get(&GroupId::Default) {
            if let Some(v) = map.get_t(TagId::Metadata) as Option<&serde_json::Value> {
                md = v.as_object().unwrap().clone();
            }
        }
        let mut cursor = Cursor::new(data);
        let len = data.len() as u64;

        // Parse NRAW atom tree. Structure:
        //   NRAW (container) > NRFM, NRFH (container) > NRHM, NRMT×N, NRTH, ...
        // NRMT atoms contain per-frame metadata. After NRFH ends, the rest is RAW pixel data.
        // Use atom sizes to skip efficiently — never scan byte-by-byte.
        while cursor.position() + 8 <= len {
            let atom_start = cursor.position();
            let atom_size = cursor.read_u32::<BigEndian>()? as u64;

            if atom_size < 8 {
                break;
            }

            let mut magic = [0u8; 4];
            cursor.read_exact(&mut magic)?;

            // Container atoms (NRAW, NRFH): enter regardless of declared size
            // (data may be truncated via max_sample_size, but children are at the start)
            if &magic == b"NRAW" || &magic == b"NRFH" {
                continue;
            }

            // Non-container atoms: need full data within buffer
            if atom_start + atom_size > len {
                break;
            }

            if &magic == b"NRMT" && atom_size >= 13 {
                // NRMT structure: [size:4]["NRMT":4][tag_id:4][pad:1][value:N]
                let tag_id = cursor.read_u32::<BigEndian>()?;
                let _padding = cursor.read_u8()?; // Skip padding byte
                let value_size = (atom_size - 13) as usize; // 4+4+4+1 = 13 bytes header

                let mut value_bytes = vec![0u8; value_size];
                cursor.read_exact(&mut value_bytes)?;
                let mut value_cursor = Cursor::new(&value_bytes);

                match tag_id {
                    0x0110_0100 => { // ImageWidth
                        let v = u32::from_be_bytes(value_bytes[0..4].try_into().unwrap());
                        md.insert("image_width".into(), (v as u64).into());
                    }
                    0x0110_0101 => { // ImageHeight
                        let v = u32::from_be_bytes(value_bytes[0..4].try_into().unwrap());
                        md.insert("image_height".into(), (v as u64).into());
                    }
                    0x0110_A302 => { // CFAPattern
                        let arr: Vec<serde_json::Value> = value_bytes.iter().map(|&b| (b as u64).into()).collect();
                        md.insert("cfa_pattern".into(), serde_json::Value::Array(arr));
                    }
                    // EXIF-style tags (group 0x0110)
                    0x0110_829A => { // Exposure Time (float)
                        if let Ok(val) = value_cursor.read_f32::<BigEndian>() {
                            util::insert_tag(map, tag!(parsed GroupId::Default, TagId::ExposureTime, "Exposure time", f32, |v| format!("{:.6}", v), val, vec![]), options);
                            md.insert("exposure_time".into(), (val as f64).into());
                        }
                    }
                    0x0110_829D => { // F-Number (float)
                        if let Ok(val) = value_cursor.read_f32::<BigEndian>() {
                            util::insert_tag(map, tag!(parsed GroupId::Lens, TagId::IrisFStop, "Aperture", f32, |v| format!("f/{:.1}", v), val, vec![]), options);
                            md.insert("f_number".into(), (val as f64).into());
                        }
                    }
                    0x0110_8832 => { // ISO (u32)
                        if let Ok(val) = value_cursor.read_u32::<BigEndian>() {
                            util::insert_tag(map, tag!(parsed GroupId::Default, TagId::ISOValue, "ISO", u32, |v| v.to_string(), val, vec![]), options);
                            md.insert("iso".into(), val.into());
                        }
                    }
                    0x0110_9204 => { // Exposure Compensation (float)
                        if let Ok(val) = value_cursor.read_f32::<BigEndian>() {
                            md.insert("exposure_compensation".into(), (val as f64).into());
                        }
                    }
                    0x0110_920A => { // Focal Length (float)
                        if let Ok(val) = value_cursor.read_f32::<BigEndian>() {
                            util::insert_tag(map, tag!(parsed GroupId::Lens, TagId::FocalLength, "Focal length", f32, |v| format!("{:.1} mm", v), val, vec![]), options);
                            md.insert("lens_focal_length".into(), (val as f64).into());
                        }
                    }
                    0x0110_0112 => { // Orientation
                        if let Ok(val) = value_cursor.read_u16::<BigEndian>() {
                            md.insert("orientation".into(), val.into());
                        }
                    }
                    // Nikon-specific tags (group 0x0190)
                    0x0190_0010 => { // White Balance Kelvin
                        if let Ok(val) = value_cursor.read_u16::<BigEndian>() {
                            md.insert("white_balance_kelvin".into(), val.into());
                        }
                    }
                    0x0190_0012 => { // Color/Orientation Matrix (3x3 floats)
                        if value_bytes.len() >= 36 {
                            let mut matrix = Vec::with_capacity(9);
                            for _ in 0..9 {
                                if let Ok(f) = value_cursor.read_f32::<BigEndian>() {
                                    matrix.push(f as f64);
                                }
                            }
                            if matrix.len() == 9 {
                                md.insert("color_matrix".into(), serde_json::to_value(matrix).unwrap_or_default());
                            }
                        }
                    }
                    _ => {
                        // Store unknown with hex ID
                        let key = format!("tag_0x{:08x}", tag_id);
                        if let Ok(v) = value_cursor.read_f32::<BigEndian>() {
                            if v.is_finite() && v.abs() < 1e10 {
                                md.insert(key, (v as f64).into());
                            }
                        }
                    }
                }
            } else {
                // Non-container atoms (NRFM, NRHM, NRTH, NRTI, etc.) — skip by size
                cursor.set_position(atom_start + atom_size);
            }
        }

        // Insert frame metadata as JSON
        if !md.is_empty() {
            let (sensor_size, pixel_pitch) = match self.model.as_deref() {
                Some("NIKON ZR") => (Some((6060, 4032)), Some((5930, 5930))),
                _ => (None, None)
            };
            if let Some(pp) = pixel_pitch {
                if let Some(ss) = sensor_size {
                    util::insert_tag(map, tag!(parsed GroupId::Imager, TagId::SensorSizePixels, "Sensor Size Pixels", u32x2, |v| format!("{v:?}"), ss, vec![]), &options);

                    if let Some(iw) = md.get("image_width").and_then(|v| v.as_u64()).map(|v| v as u32) {
                        if let Some(ih) = md.get("image_height").and_then(|v| v.as_u64()).map(|v| v as u32) {
                            util::insert_tag(map, tag!(parsed GroupId::Imager, TagId::CaptureAreaSize, "Capture Area Size", f32x2, |v| format!("{v:?}"), (iw as f32, ih as f32), vec![]), &options);
                            // Set origin to center
                            util::insert_tag(map, tag!(parsed GroupId::Imager, TagId::CaptureAreaOrigin, "Capture Area Origin", f32x2, |v| format!("{v:?}"), (((ss.0 as f32 - iw as f32)) / 2.0, ((ss.1 as f32 - ih as f32)) / 2.0), vec![]), &options);
                        }
                    }
                }
                util::insert_tag(map, tag!(parsed GroupId::Imager, TagId::PixelPitch, "Pixel pitch", u32x2, |v| format!("{v:?}"), pp, vec![]), &options);
            }

            util::insert_tag(map, tag!(parsed GroupId::Default, TagId::Metadata, "Metadata", Json, |v| serde_json::to_string(v).unwrap(), serde_json::Value::Object(md), vec![]), options);
        }

        Ok(())
    }

    /// Normalize a datetime string to "yyyy:MM:dd HH:mm:ss" format.
    /// Handles formats like "2024:07:23 21:41:48", "2024-07-23T21:41:48", "2024-07-23 21:41:48", etc.
    fn normalize_datetime(s: &str) -> Option<String> {
        let s = s.trim();
        if s.is_empty() { return None; }
        // Already in "yyyy:MM:dd HH:mm:ss" format
        if s.len() >= 19 && s.chars().nth(4) == Some(':') && s.chars().nth(7) == Some(':') && s.chars().nth(13) == Some(':') && s.chars().nth(16) == Some(':') {
            return Some(s[..19].to_string());
        }
        // Try "yyyy-MM-ddTHH:mm:ss" or "yyyy-MM-dd HH:mm:ss"
        if s.len() >= 19 && s.chars().nth(4) == Some('-') && s.chars().nth(7) == Some('-') {
            let date_part = &s[..10]; // yyyy-MM-dd
            let time_part = &s[11..19]; // HH:mm:ss
            let formatted_date = date_part.replace('-', ":");
            return Some(format!("{} {}", formatted_date, time_part));
        }
        None
    }

    /// Compose datetime from "YYYYMMDD" date and "HHMMSS" time strings.
    fn compose_datetime(date: &str, time: &str) -> Option<String> {
        let date = date.trim();
        let time = time.trim();
        if date.len() < 8 || time.len() < 6 { return None; }
        let year = &date[0..4];
        let month = &date[4..6];
        let day = &date[6..8];
        let hour = &time[0..2];
        let min = &time[2..4];
        let sec = &time[4..6];
        Some(format!("{}:{}:{} {}:{}:{}", year, month, day, hour, min, sec))
    }

    fn process_map(&mut self, samples: &mut Vec<SampleInfo>, options: &crate::InputOptions, video_md: Option<&VideoMetadata>) {
        // Extract metadata JSON fields from first sample
        let md_json: Option<serde_json::Value> = samples.first()
            .and_then(|s| s.tag_map.as_ref())
            .and_then(|m| m.get(&GroupId::Default))
            .and_then(|m| m.get(&TagId::Metadata))
            .and_then(|t| {
                if let TagValue::Json(ref v) = t.value { Some(v.get().clone()) } else { None }
            });

        let md = match md_json.as_ref().and_then(|v| v.as_object()) {
            Some(m) => m,
            None => return,
        };

        let raw_model = md.get("camera_model").and_then(|v| v.as_str()).unwrap_or("");
        let resolution_w = video_md.map(|v| v.width as u32).unwrap_or(0);
        let resolution_h = video_md.map(|v| v.height as u32).unwrap_or(0);
        let fps = video_md.map(|v| v.fps).unwrap_or(0.0);

        // Write video track resolution
        if let Some(vmd) = video_md {
            if let Some(ref mut map) = samples.first_mut().and_then(|s| s.tag_map.as_mut()) {
                util::insert_tag(map, tag!(parsed GroupId::Default, TagId::Custom("video_width".into()), "Video output width", u32, |v| format!("{} px", v), vmd.width as u32, Vec::new()), options);
                util::insert_tag(map, tag!(parsed GroupId::Default, TagId::Custom("video_height".into()), "Video output height", u32, |v| format!("{} px", v), vmd.height as u32, Vec::new()), options);
            }
        }

        // CropHiSpeed scale: full_w / crop_w
        let scale_35mm = md.get("crop_hi_speed_full_w").and_then(|v| v.as_f64())
            .and_then(|full_w| {
                md.get("crop_hi_speed_crop_w").and_then(|v| v.as_f64()).and_then(|crop_w| {
                    if crop_w > 0.0 && full_w > 0.0 { Some(full_w / crop_w) } else { None }
                })
            });

        // Lens and focal length
        let lens_name = md.get("lens_name").and_then(|v| v.as_str()).unwrap_or("");
        let _focal_length = if !lens_name.is_empty() {
            md.get("lens_focal_length").and_then(|v| v.as_f64())
        } else {
            None
        };

        // Extract creation date from NCDT metadata
        let creation_date: Option<String> = md.get("local_datetime")
            .and_then(|v| v.as_str())
            .and_then(|s| Self::normalize_datetime(s))
            .or_else(|| {
                // Fallback: compose from local_date + local_time
                let date = md.get("local_date").and_then(|v| v.as_str())?;
                let time = md.get("local_time").and_then(|v| v.as_str())?;
                Self::compose_datetime(date, time)
            });
        let timezone: Option<String> = md.get("timezone").and_then(|v| v.as_str())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());

        // Try JSON database first
        if let Some(db_path) = &options.camera_db_path {
            if let Ok(db) = crate::camera_db::CameraDatabase::load(db_path) {
                // First pass: extract model info, crop, sensor_width from first sample
                let db_result: Option<(String, f32, Option<f64>, Option<f64>)> = {
                    if let Some(ref mut map) = samples.first_mut().and_then(|s| s.tag_map.as_mut()) {
                        if let Some((model_name, model_data)) = db.process_model("NIKON", raw_model, map, options) {
                            self.model = Some(model_name.to_string());
                            let sensor_w = model_data.sw;

                            // Determine crop factor
                            let crop_factor = if let Some(scale) = scale_35mm {
                                Some(scale)
                            } else {
                                let crop_type = md.get("crop_hi_speed_type").and_then(|v| v.as_u64()).map(|v| v as u16);
                                let view_angle = crop_type.and_then(|ct| db.lookup_crop_type("NIKON", ct))
                                    .unwrap_or(if sensor_w > 30.0 { "FX" } else { "DX" });

                                let tags = std::collections::HashMap::new();
                                db.match_crop("NIKON", model_name, resolution_w, resolution_h, fps, Some(view_angle), &tags)
                            };

                            let effective_crop = crop_factor.unwrap_or(1.0);
                            let unit_px_fl = if resolution_w > 0 && sensor_w > 0.0 {
                                Some(resolution_w as f64 * effective_crop / sensor_w as f64)
                            } else {
                                None
                            };

                            // Readout (only on first sample)
                            if self.frame_readout_time.is_none() {
                                let tags = std::collections::HashMap::new();
                                if let Some(rt) = db.process_readout("NIKON", model_name, resolution_w, resolution_h, fps, scale_35mm.unwrap_or(1.0), sensor_w, &tags, map, options) {
                                    self.frame_readout_time = Some(rt);
                                }
                            }

                            Some((model_name.to_string(), sensor_w, crop_factor, unit_px_fl))
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                }; // first-sample borrow released here

                if let Some((_model_name, _sensor_w, crop_factor, unit_px_fl)) = db_result {
                    // Second pass: write crop_factor, unit_pixel_focal_length, and PixelFocalLength to ALL samples
                    for sample in samples.iter_mut() {
                        if let Some(ref mut smap) = sample.tag_map {
                            // Write crop_factor (same for all frames)
                            if !smap.get(&GroupId::Default).map_or(false, |m| m.contains_key(&TagId::Custom("crop_factor".into()))) {
                                if let Some(cf) = crop_factor {
                                    util::insert_tag(smap, tag!(parsed GroupId::Default, TagId::Custom("crop_factor".into()), "Crop factor", f64, |v| format!("{:.4}", v), cf, Vec::new()), options);
                                }
                            }

                            // Write unit_pixel_focal_length (same for all frames)
                            if let Some(upfl) = unit_px_fl {
                                if !smap.get(&GroupId::Lens).map_or(false, |m| m.contains_key(&TagId::Custom("unit_pixel_focal_length".into()))) {
                                    util::insert_tag(smap, tag!(parsed GroupId::Lens, TagId::Custom("unit_pixel_focal_length".into()), "Pixel focal length per mm", f64, |v| format!("{:.4}", v), upfl, Vec::new()), options);
                                }
                            }

                            // Write PixelFocalLength based on THIS frame's FocalLength or user-provided focal length
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

                    // Write to first sample: creation date, slow-motion, stabilization
                    if let Some(ref mut map) = samples.first_mut().and_then(|s| s.tag_map.as_mut()) {
                        if let Some(ref date_str) = creation_date {
                            util::write_creation_date_tags(map, date_str, timezone.as_deref(), Some("500"), options);
                        } else if let Some(ref mvhd_time) = self.mvhd_creation_time {
                            util::write_creation_date_tags(map, mvhd_time, None, Some("500"), options);
                        }

                        // Frame rates: always output RecordFrameRate (FrameRate already set from NCTG 0x16)
                        if let Some(record_fps) = self.record_frame_rate {
                            util::insert_tag(map, tag!(parsed GroupId::Default, TagId::RecordFrameRate, "Record frame rate", f64, |v| format!("{:.3} fps", v), record_fps, Vec::new()), options);
                        }

                        // Image stabilization: ElectronicVR OR VibrationReduction
                        let evr_on = if self.is_r3d {
                            false // R3D files: skip ElectronicVR
                        } else {
                            self.electronic_vr == Some(1)
                        };
                        let vr_on = self.vibration_reduction == Some(1); // 0=n/a, 1=On, 2=Off
                        let is_stabilized = evr_on || vr_on;
                        util::insert_tag(map, tag!(parsed GroupId::Default, TagId::ImageStabilizer, "Image stabilization", bool, |v| if *v { "On" } else { "Off" }.into(), is_stabilized, Vec::new()), options);
                    }

                    return; // JSON path complete
                }
            }
        }

        // Fallback creation date (no JSON db match)
        if let Some(ref mut map) = samples.first_mut().and_then(|s| s.tag_map.as_mut()) {
            if let Some(ref date_str) = creation_date {
                util::write_creation_date_tags(map, date_str, timezone.as_deref(), Some("500"), options);
            } else if let Some(ref mvhd_time) = self.mvhd_creation_time {
                util::write_creation_date_tags(map, mvhd_time, None, Some("500"), options);
            }
        }
    }
}
