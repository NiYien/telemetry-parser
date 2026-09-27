// SPDX-License-Identifier: MIT OR Apache-2.0

use std::{collections::BTreeMap, io::*, sync::{Arc, atomic::AtomicBool}};
use byteorder::{BigEndian, ReadBytesExt};
use crate::{*, tags_impl::*};

const MODEL: &str = "com.apple.quicktime.model";
const SOFTWARE: &str = "com.apple.quicktime.software";
const LENS: &str = "com.blackmagic-design.camera.lensType";
const READOUT: &str = "com.apple.quicktime.camera.framereadouttimeinmicroseconds";

#[derive(Default)]
pub struct Phone {
    pub model: Option<String>,
    frame_readout_time: Option<f64>,
    creation_date: Option<String>,
}

impl Phone {
    pub fn camera_type(&self) -> String { "Apple".into() }
    pub fn possible_extensions() -> Vec<&'static str> { vec!["mov", "mp4"] }
    pub fn has_accurate_timestamps(&self) -> bool { false }
    pub fn frame_readout_time(&self) -> Option<f64> { self.frame_readout_time }
    pub fn normalize_imu_orientation(v: String) -> String { v }

    pub fn detect<P: AsRef<std::path::Path>>(buffer: &[u8], _path: P, _options: &InputOptions) -> Option<Self> {
        // Software alone also occurs in desktop-camera files. Validate actual mdta values in parse.
        [MODEL.as_bytes(), SOFTWARE.as_bytes(), b"iPhone ", b"Blackmagic Cam "].iter()
            .all(|key| memchr::memmem::find(buffer, key).is_some())
            .then(|| Self { creation_date: util::extract_mvhd_creation_time(buffer), ..Self::default() })
    }

    pub fn parse<T: Read + Seek, F: Fn(f64)>(&mut self, stream: &mut T, size: usize, _progress_cb: F, _cancel_flag: Arc<AtomicBool>, options: InputOptions) -> Result<Vec<SampleInfo>> {
        let mut metadata = BTreeMap::new();
        stream.seek(SeekFrom::Start(0))?;
        read_metadata(stream, size as u64, 0, &mut metadata)?;
        let raw_model = metadata.get(MODEL).map(String::as_str).unwrap_or_default();
        let raw_model = raw_model.strip_prefix("Apple ").unwrap_or(raw_model).trim();
        let software = metadata.get(SOFTWARE).map(String::as_str).unwrap_or_default();
        if !raw_model.starts_with("iPhone ") || !software.starts_with("Blackmagic Cam ") {
            return Err(Error::new(ErrorKind::InvalidData, "Not an iPhone Blackmagic Camera recording"));
        }
        let (model, model_focal) = split_focal_length(raw_model);
        self.model = Some(model.into());
        let lens = metadata.get(LENS).map(String::as_str).unwrap_or(raw_model).trim();
        let (lens_model, lens_focal) = split_focal_length(lens);
        let focal = if lens_model == model { lens_focal.or(model_focal) } else { model_focal };

        stream.seek(SeekFrom::Start(0))?;
        let video = util::get_video_metadata(stream, size).ok();
        let mut map = GroupedTagMap::new();
        util::insert_tag(&mut map, tag!(parsed GroupId::Default, TagId::Name, "Camera model", String, |v| v.clone(), model.to_owned(), vec![]), &options);
        util::insert_tag(&mut map, tag!(parsed GroupId::Lens, TagId::DisplayName, "Lens", String, |v| v.clone(), lens.to_owned(), vec![]), &options);

        // This format names the lens in 35mm-equivalent millimetres. Keep that unit for
        // focal length and upfl together; 36mm is a reference width, not a phone sensor size.
        if let Some(focal) = focal {
            util::insert_tag(&mut map, tag!(parsed GroupId::Lens, TagId::FocalLength, "Focal length (35mm equivalent)", f32, |v| format!("{v:.2} mm (35mm equivalent)"), focal as f32, vec![]), &options);
            util::insert_tag(&mut map, tag!(parsed GroupId::Lens, TagId::Custom("focal_length_estimated".into()), "Focal projection estimated", bool, |v| v.to_string(), true, vec![]), &options);
        }
        if let Some(v) = &video {
            let upfl = v.width as f64 / 36.0;
            if upfl > 0.0 {
                // Retain the scale when focal length is absent so manual equivalent mm still work.
                util::insert_tag(&mut map, tag!(parsed GroupId::Lens, TagId::Custom("unit_pixel_focal_length".into()), "Pixels per equivalent mm", f64, |v| format!("{v:.4}"), upfl, vec![]), &options);
                if let Some(pixel_focal) = focal.map(|f| (f * upfl) as f32).filter(|f| f.is_finite() && *f > 0.0) {
                    util::insert_tag(&mut map, tag!(parsed GroupId::Lens, TagId::PixelFocalLength, "Estimated pixel focal length", f32, |v| format!("{v:.2}"), pixel_focal, vec![]), &options);
                }
            }
            util::insert_tag(&mut map, tag!(parsed GroupId::Default, TagId::Custom("video_width".into()), "Video output width", u32, |v| v.to_string(), v.width as u32, vec![]), &options);
            util::insert_tag(&mut map, tag!(parsed GroupId::Default, TagId::Custom("video_height".into()), "Video output height", u32, |v| v.to_string(), v.height as u32, vec![]), &options);
            util::insert_tag(&mut map, tag!(parsed GroupId::Default, TagId::FrameRate, "Frame rate", f64, |v| v.to_string(), v.fps, vec![]), &options);
        }
        let record_fps = metadata.get("com.blackmagic-design.sensorFPS").and_then(|v| positive_number(v));
        if let Some(fps) = record_fps {
            util::insert_tag(&mut map, tag!(parsed GroupId::Default, TagId::RecordFrameRate, "Recording frame rate", f64, |v| v.to_string(), fps, vec![]), &options);
        }

        let readout = metadata.get(READOUT).and_then(|v| positive_number(v)).map(|v| (v / 1000.0, false))
            .or_else(|| {
                let video = video.as_ref()?;
                let db = camera_db::CameraDatabase::load(options.camera_db_path.as_deref()?).ok()?;
                let focal = focal?;
                let key = format!("{model} {focal}mm");
                lookup_readout(&db, &key, video.width, video.height, record_fps.unwrap_or(video.fps))
            });
        if let Some((time, estimated)) = readout {
            self.frame_readout_time = Some(time);
            util::insert_tag(&mut map, tag!(parsed GroupId::Imager, TagId::FrameReadoutTime, "Frame readout time", f64, |v| format!("{v:.4} ms"), time, vec![]), &options);
            if estimated {
                util::insert_tag(&mut map, tag!(parsed GroupId::Imager, TagId::Custom("readout_estimated".into()), "Readout time estimated", bool, |v| v.to_string(), true, vec![]), &options);
            }
        }

        let date = ["com.apple.quicktime.creationdate", "com.blackmagic-design.camera.dateRecorded"].iter()
            .filter_map(|key| metadata.get(*key))
            .find_map(|value| chrono::DateTime::parse_from_rfc3339(value)
                .or_else(|_| chrono::DateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.f%z")).ok());
        if let Some(date) = date {
            let local = date.format("%Y:%m:%d %H:%M:%S").to_string();
            let timezone = date.format("%:z").to_string();
            let subsec = (date.timestamp_subsec_nanos() != 0).then(|| format!("{:09}", date.timestamp_subsec_nanos()));
            util::write_creation_date_tags(&mut map, &local, Some(&timezone), subsec.as_deref(), &options);
        } else if let Some(date) = &self.creation_date {
            util::write_creation_date_tags(&mut map, date, None, None, &options);
        }
        // Preserve the recorded fields without inventing stabilization, zoom or sensor data.
        let mut raw = serde_json::to_value(metadata).unwrap_or_default();
        if let Some(focal) = focal {
            raw["lens_info"] = format!("{focal} mm (35mm equivalent)").into();
            raw["focal_length_estimated"] = true.into();
        }
        if let Some((_, estimated)) = readout {
            raw["readout_estimated"] = estimated.into();
        }
        util::insert_tag(&mut map, tag!(parsed GroupId::Default, TagId::Metadata, "Metadata", Json, |v| v.to_string(), raw, vec![]), &options);
        Ok(vec![SampleInfo { tag_map: Some(map), video_rotation: video.map(|v| v.rotation), ..Default::default() }])
    }
}

fn positive_number(value: &str) -> Option<f64> {
    value.trim().parse::<f64>().ok().filter(|v| v.is_finite() && *v > 0.0)
}

fn split_focal_length(value: &str) -> (&str, Option<f64>) {
    if let Some((model, suffix)) = value.rsplit_once(' ') {
        if let Some(focal) = suffix.strip_suffix("mm").and_then(positive_number).filter(|v| (*v as f32).is_finite()) {
            return (model.trim(), Some(focal));
        }
    }
    (value, None)
}

fn lookup_readout(db: &camera_db::CameraDatabase, lens: &str, width: usize, height: usize, fps: f64) -> Option<(f64, bool)> {
    let nominal = fps.round();
    // Accept integer and NTSC rates, but never substitute a different recording mode.
    if !fps.is_finite() || fps <= 0.0 || ((fps - nominal).abs() > 0.0001 && (fps - nominal / 1.001).abs() > 0.0001) { return None; }
    let table = &db.get_brand("APPLE")?.readout;
    let column = format!("{width}x{height}@{nominal:.0}");
    let index = table.columns.iter().position(|v| v == &column)?;
    let value = (*table.data.get(lens)?.get(index)?)?;
    (value.is_finite() && value != 0.0).then_some((value.abs(), value < 0.0))
}

// Walk the container boundaries and skip media payloads. Both movie and track metadata
// are allowed; reading only the movie header misses Apple's track-level readout tag.
fn read_metadata<T: Read + Seek>(stream: &mut T, end: u64, depth: u8, out: &mut BTreeMap<String, String>) -> Result<()> {
    while stream.stream_position()? < end {
        let (kind, start, size, header) = util::read_box(stream)?;
        let box_end = if size == 0 { end } else { start.checked_add(size).ok_or(ErrorKind::InvalidData)? };
        if box_end > end || box_end < stream.stream_position()? { return Err(ErrorKind::InvalidData.into()); }
        if depth < 3 && [util::fourcc("moov"), util::fourcc("udta"), util::fourcc("trak")].contains(&kind) {
            read_metadata(stream, box_end, depth + 1, out)?;
        } else if kind == util::fourcc("meta") {
            let mut data = Vec::new();
            stream.take(box_end - start - header as u64).read_to_end(&mut data)?;
            parse_meta(&data, out)?;
        }
        stream.seek(SeekFrom::Start(box_end))?;
    }
    Ok(())
}

fn boxes(data: &[u8]) -> Result<Vec<(u32, &[u8])>> {
    let mut stream = Cursor::new(data);
    let mut result = Vec::new();
    while stream.position() < data.len() as u64 {
        let (kind, start, size, _) = util::read_box(&mut stream)?;
        let end = if size == 0 { data.len() as u64 } else { start.checked_add(size).ok_or(ErrorKind::InvalidData)? };
        if end > data.len() as u64 || end < stream.position() { return Err(ErrorKind::InvalidData.into()); }
        result.push((kind, &data[stream.position() as usize..end as usize]));
        stream.set_position(end);
    }
    Ok(result)
}

fn parse_meta(data: &[u8], out: &mut BTreeMap<String, String>) -> Result<()> {
    // QuickTime permits both a plain meta container and the ISO full-box layout.
    let data = if data.starts_with(&[0, 0, 0, 0]) { &data[4..] } else { data };
    let children = boxes(data)?;
    if !children.iter().any(|(kind, value)| *kind == util::fourcc("hdlr") && value.get(8..12) == Some(b"mdta")) { return Ok(()); }
    let mut keys = Vec::new();
    for (_, value) in children.iter().filter(|(kind, _)| *kind == util::fourcc("keys")) {
        let entries = value.get(8..).ok_or(ErrorKind::InvalidData)?;
        for (namespace, key) in boxes(entries)? {
            keys.push((namespace == util::fourcc("mdta")).then(|| std::str::from_utf8(key).ok()).flatten());
        }
    }
    for (_, value) in children.iter().filter(|(kind, _)| *kind == util::fourcc("ilst")) {
        for (index, item) in boxes(value)? {
            let Some(Some(key)) = index.checked_sub(1).and_then(|i| keys.get(i as usize)) else { continue; };
            for (_, data) in boxes(item)?.into_iter().filter(|(kind, _)| *kind == util::fourcc("data")) {
                let Some(value) = data.get(8..) else { continue; };
                let typ = (&data[..4]).read_u32::<BigEndian>()? & 0x00ff_ffff;
                let decoded = match typ {
                    1 => std::str::from_utf8(value).ok().map(|v| v.trim_end_matches('\0').to_owned()),
                    23 => value.try_into().ok().map(|v| f32::from_be_bytes(v).to_string()),
                    24 => value.try_into().ok().map(|v| f64::from_be_bytes(v).to_string()),
                    21 | 65 | 66 | 67 | 74 if [1, 2, 4, 8].contains(&value.len()) => {
                        let mut bytes = [if value[0] & 0x80 != 0 { 0xff } else { 0 }; 8];
                        bytes[8 - value.len()..].copy_from_slice(value);
                        Some(i64::from_be_bytes(bytes).to_string())
                    }
                    22 | 75 | 76 | 77 | 78 if [1, 2, 4, 8].contains(&value.len()) => {
                        let mut bytes = [0; 8];
                        bytes[8 - value.len()..].copy_from_slice(value);
                        Some(u64::from_be_bytes(bytes).to_string())
                    }
                    _ => None,
                };
                if let Some(value) = decoded { out.insert((*key).to_owned(), value); }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
