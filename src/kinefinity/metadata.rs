// SPDX-License-Identifier: MIT OR Apache-2.0

use serde_json::Value;
use std::io::{Read, Seek, SeekFrom};

const MAX_METADATA_SIZE: usize = 16 * 1024 * 1024;

#[derive(Debug, Default, Clone)]
pub struct ClipData {
    pub camera_model: Option<String>,
    pub clip_name: Option<String>,
    pub clip_uuid: Option<String>,
    pub image_format: Option<String>,
    pub oversampling: Option<bool>,
    pub width: Option<u32>,
    pub height: Option<u32>,
    pub coded_size: Option<(u32, u32)>,
    pub sensor_fps: Option<f64>,
    pub project_fps: Option<f64>,
    pub focal_length: Option<f64>,
    pub lens_name: Option<String>,
    pub firmware: Option<String>,
    pub shot_date: Option<String>,
    pub shot_tod: Option<String>,
    pub timecode: Option<String>,
    pub creation_utc: Option<String>,
    pub is_kinefinity: bool,
}

impl ClipData {
    pub fn merge_missing(&mut self, other: Self) {
        macro_rules! fill { ($($field:ident),*) => { $(if self.$field.is_none() { self.$field = other.$field; })* }; }
        fill!(
            camera_model,
            clip_name,
            clip_uuid,
            image_format,
            oversampling,
            width,
            height,
            sensor_fps,
            project_fps,
            focal_length,
            lens_name,
            firmware,
            shot_date,
            shot_tod,
            timecode,
            creation_utc
        );
        self.is_kinefinity |= other.is_kinefinity;
    }

    pub fn matches(&self, other: &Self) -> bool {
        fn agrees(a: &Option<String>, b: &Option<String>) -> bool {
            match (a, b) {
                (Some(a), Some(b)) => a.eq_ignore_ascii_case(b),
                _ => true,
            }
        }
        agrees(&self.clip_uuid, &other.clip_uuid) && agrees(&self.clip_name, &other.clip_name)
    }
}

pub fn positive(value: f64) -> Option<f64> {
    (value.is_finite() && value > 0.0).then_some(value)
}

fn text(value: &Value) -> Option<String> {
    let value = match value {
        Value::String(s) => s.trim().trim_matches('\0').to_owned(),
        Value::Number(n) => n.to_string(),
        _ => return None,
    };
    (!value.is_empty() && !value.eq_ignore_ascii_case("N/A")).then_some(value)
}

fn number(value: &Value) -> Option<f64> {
    positive(value.as_f64().or_else(|| {
        value
            .as_str()?
            .trim()
            .trim_end_matches("mm")
            .trim()
            .parse()
            .ok()
    })?)
}

fn boolean(value: &Value) -> Option<bool> {
    value
        .as_bool()
        .or_else(|| match value.as_u64() {
            Some(0) => Some(false),
            Some(1) => Some(true),
            _ => None,
        })
        .or_else(
            || match value.as_str()?.trim().to_ascii_lowercase().as_str() {
                "yes" | "true" | "1" | "on" => Some(true),
                "no" | "false" | "0" | "off" => Some(false),
                _ => None,
            },
        )
}

pub fn from_json(value: &Value) -> ClipData {
    let mut data = ClipData::default();
    let Some(values) = value.as_object() else {
        return data;
    };
    for (key, value) in values {
        let key = key
            .strip_prefix("com.kinefinity.")
            .or_else(|| key.strip_prefix("com.apple.quicktime."))
            .unwrap_or(key);
        match key {
            "camera_model" | "model" | "camera.identifier" => {
                if data.camera_model.is_none() {
                    data.camera_model = text(value);
                }
            }
            "make" => {
                data.is_kinefinity =
                    text(value).is_some_and(|v| v.eq_ignore_ascii_case("Kinefinity"))
            }
            "clip_name" | "name" => data.clip_name = text(value),
            "clip_uuid" => data.clip_uuid = text(value),
            "image_format" => data.image_format = text(value),
            "oversampling" => data.oversampling = boolean(value),
            "width" => {
                data.width = number(value)
                    .filter(|v| v.fract() == 0.0 && *v <= u32::MAX as f64)
                    .map(|v| v as u32)
            }
            "height" => {
                data.height = number(value)
                    .filter(|v| v.fract() == 0.0 && *v <= u32::MAX as f64)
                    .map(|v| v as u32)
            }
            "sensor_fps" => data.sensor_fps = number(value),
            "project_fps" => data.project_fps = number(value),
            "focal_current" | "focal_length" => data.focal_length = number(value),
            "lens_name" | "lens_model" => data.lens_name = text(value),
            "firmware" => data.firmware = text(value),
            "tc_start" => data.timecode = text(value),
            _ => {}
        }
    }
    data.is_kinefinity |= values.keys().any(|k| k.starts_with("com.kinefinity."));
    data
}

pub fn parse_jsonl(content: &str) -> Option<ClipData> {
    let mut current: Option<ClipData> = None;
    let mut committed = None;
    for line in content.lines() {
        let Ok(value) = serde_json::from_str::<Value>(line.trim_start_matches('\u{feff}')) else {
            continue;
        };
        match value.get("event").and_then(Value::as_str) {
            Some("OPEN") => current = Some(from_json(&value)),
            Some("COMMIT") => {
                let commit = from_json(&value);
                if let Some(open) = current.as_mut().filter(|open| open.matches(&commit)) {
                    open.merge_missing(commit);
                    committed = Some(open.clone());
                }
            }
            _ => {}
        }
    }
    committed.or(current)
}

pub fn parse_slate(content: &str) -> Option<ClipData> {
    let mut data = ClipData::default();
    for line in content.lines() {
        let line = line.trim().trim_start_matches('\u{feff}');
        if line.starts_with('#') {
            continue;
        }
        let Some((key, raw)) = line.split_once(':') else {
            continue;
        };
        let value = Value::String(raw.trim().to_owned());
        match key.trim().trim_end_matches('.').trim() {
            "Camera Model" => data.camera_model = text(&value),
            "Clip Name" => data.clip_name = text(&value),
            "Clip UUID" => data.clip_uuid = text(&value),
            "Image Format" => data.image_format = text(&value),
            "Oversampling" => data.oversampling = boolean(&value),
            "Width" => data.width = raw.trim().parse::<u32>().ok().filter(|v| *v > 0),
            "Height" => data.height = raw.trim().parse::<u32>().ok().filter(|v| *v > 0),
            "Sensor FPS" => data.sensor_fps = number(&value),
            "Project FPS" => data.project_fps = number(&value),
            "Focal Length" => data.focal_length = number(&value),
            "Lens Name" | "Lens Model" => data.lens_name = text(&value),
            "Firmware Rev" => data.firmware = text(&value),
            "Shot date" => data.shot_date = text(&value),
            "Shot TOD" => data.shot_tod = text(&value),
            "SMPTE first frame" => data.timecode = text(&value),
            _ => {}
        }
    }
    (data.camera_model.is_some() || data.width.is_some()).then_some(data)
}

pub fn read_text(path: &str) -> Option<String> {
    let file = crate::filesystem::open_file(path).ok()?;
    if file.size > MAX_METADATA_SIZE {
        return None;
    }
    let mut bytes = Vec::new();
    file.file
        .take(MAX_METADATA_SIZE as u64 + 1)
        .read_to_end(&mut bytes)
        .ok()?;
    if bytes.len() > MAX_METADATA_SIZE {
        return None;
    }
    Some(String::from_utf8_lossy(&bytes).into_owned())
}

pub fn sidecar_paths(video: &str, suffix: &str) -> Vec<String> {
    use crate::filesystem as fs;
    let filename = fs::get_filename(video);
    let stem = filename
        .rsplit_once('.')
        .map(|(stem, _)| stem)
        .unwrap_or(&filename);
    let name = format!("{stem}{suffix}");
    let entries = fs::list_folder(&fs::get_folder(video));
    let mut result: Vec<_> = entries
        .iter()
        .filter(|(n, _)| n.eq_ignore_ascii_case(&name))
        .map(|(_, p)| p.clone())
        .collect();
    // Firmware 10.1 reports this relative metadata folder in the slate itself.
    if let Some((_, folder)) = entries
        .iter()
        .find(|(n, _)| n.eq_ignore_ascii_case("KineClipData"))
    {
        if let Some((_, folder)) = fs::list_folder(folder)
            .iter()
            .find(|(n, _)| n.eq_ignore_ascii_case("Metadata"))
        {
            result.extend(
                fs::list_folder(folder)
                    .into_iter()
                    .filter(|(n, _)| n.eq_ignore_ascii_case(&name))
                    .map(|(_, p)| p),
            );
        }
    }
    result
}

pub fn merge_sidecars(video: &str, data: &mut ClipData) {
    for suffix in [".kmeta.jsonl", "-slate.txt"] {
        for path in sidecar_paths(video, suffix) {
            let Some(text) = read_text(&path) else {
                continue;
            };
            let parsed = if suffix.ends_with("jsonl") {
                parse_jsonl(&text)
            } else {
                parse_slate(&text)
            };
            if let Some(parsed) = parsed {
                if data.matches(&parsed) {
                    data.merge_missing(parsed);
                    break;
                }
                log::warn!("Kinefinity: ignoring mismatched metadata sidecar {path}");
            }
        }
    }
}

fn u32_at(data: &[u8], at: usize) -> Option<u32> {
    Some(u32::from_be_bytes(
        data.get(at..at.checked_add(4)?)?.try_into().ok()?,
    ))
}

pub(super) fn boxes(mut data: &[u8]) -> impl Iterator<Item = ([u8; 4], &[u8])> {
    std::iter::from_fn(move || {
        let size = u32_at(data, 0)?;
        let name = data.get(4..8)?.try_into().ok()?;
        let (size, header) = match size {
            0 => (data.len(), 8),
            1 => (
                usize::try_from(u64::from_be_bytes(data.get(8..16)?.try_into().ok()?)).ok()?,
                16,
            ),
            n => (n as usize, 8),
        };
        if size < header || size > data.len() {
            data = &[];
            return None;
        }
        let payload = &data[header..size];
        data = &data[size..];
        Some((name, payload))
    })
}

fn parse_meta(data: &[u8]) -> Value {
    let data = if data.get(4..8) == Some(b"hdlr") {
        data
    } else {
        data.get(4..).unwrap_or_default()
    };
    let children: Vec<_> = boxes(data).collect();
    let mut keys = Vec::new();
    for (name, payload) in &children {
        if name == b"keys" {
            let Some(count) = u32_at(payload, 4).filter(|n| *n <= 4096) else {
                continue;
            };
            keys = boxes(payload.get(8..).unwrap_or_default())
                .take(count as usize)
                .map(|(_, key)| std::str::from_utf8(key).ok().map(str::to_owned))
                .collect();
        }
    }
    let mut values = serde_json::Map::new();
    for (name, payload) in children {
        if &name != b"ilst" {
            continue;
        }
        for (index, item) in boxes(payload) {
            let Some(index) = u32::from_be_bytes(index).checked_sub(1) else {
                continue;
            };
            let Some(Some(key)) = keys.get(index as usize) else {
                continue;
            };
            for (kind, value) in boxes(item) {
                if &kind != b"data" {
                    continue;
                }
                let Some(typ) = u32_at(value, 0) else {
                    continue;
                };
                let Some(raw) = value.get(8..) else {
                    continue;
                };
                let value = match typ & 0xffffff {
                    1 => std::str::from_utf8(raw)
                        .ok()
                        .map(|v| Value::String(v.trim_matches('\0').to_owned())),
                    23 if raw.len() == 4 => Some(Value::from(f32::from_be_bytes(
                        raw.try_into().unwrap(),
                    ) as f64)),
                    24 if raw.len() == 8 => {
                        Some(Value::from(f64::from_be_bytes(raw.try_into().unwrap())))
                    }
                    21 | 22 if !raw.is_empty() && raw.len() <= 8 => {
                        let mut bytes = [if typ & 0xffffff == 21 && raw[0] & 0x80 != 0 {
                            0xff
                        } else {
                            0
                        }; 8];
                        bytes[8 - raw.len()..].copy_from_slice(raw);
                        Some(if typ & 0xffffff == 21 {
                            Value::from(i64::from_be_bytes(bytes))
                        } else {
                            Value::from(u64::from_be_bytes(bytes))
                        })
                    }
                    _ => None,
                };
                if let Some(value) = value {
                    values.insert(key.clone(), value);
                }
            }
        }
    }
    Value::Object(values)
}

fn video_media(track: &[u8]) -> Option<&[u8]> {
    let mdia = boxes(track).find(|(kind, _)| kind == b"mdia")?.1;
    let handler = boxes(mdia).find(|(kind, _)| kind == b"hdlr")?.1;
    if handler.get(8..12) != Some(b"vide") { return None; }
    Some(mdia)
}

fn coded_video_size(mdia: &[u8]) -> Option<(u32, u32)> {
    let minf = boxes(mdia).find(|(kind, _)| kind == b"minf")?.1;
    let stbl = boxes(minf).find(|(kind, _)| kind == b"stbl")?.1;
    let stsd = boxes(stbl).find(|(kind, _)| kind == b"stsd")?.1;
    let count = u32_at(stsd, 4).filter(|&n| n > 0 && n <= 1024)?;
    let mut size = None;
    let mut seen = 0;
    for (kind, entry) in boxes(stsd.get(8..)?).take(count as usize) {
        if !matches!(&kind, b"avc1" | b"avc3" | b"hvc1" | b"hev1" |
            b"apch" | b"apcn" | b"apcs" | b"apco" | b"ap4h" | b"ap4x") || entry.len() < 78 {
            return None;
        }
        let width = u16::from_be_bytes(entry[24..26].try_into().ok()?) as u32;
        let height = u16::from_be_bytes(entry[26..28].try_into().ok()?) as u32;
        if width == 0 || height == 0 || size.is_some_and(|old| old != (width, height)) {
            return None;
        }
        size = Some((width, height));
        seen += 1;
    }
    (seen == count).then_some(size).flatten()
}

pub fn parse_moov(data: &[u8]) -> ClipData {
    let mut result = ClipData::default();
    let mut video_track_seen = false;
    for (kind, payload) in boxes(data) {
        match &kind {
            b"meta" => result.merge_missing(from_json(&parse_meta(payload))),
            b"trak" if !video_track_seen => {
                if let Some(mdia) = video_media(payload) {
                    video_track_seen = true;
                    result.coded_size = coded_video_size(mdia);
                }
            }
            b"udta" => {
                for (kind, payload) in boxes(payload) {
                    if &kind == b"meta" {
                        result.merge_missing(from_json(&parse_meta(payload)));
                    }
                }
            }
            b"mvhd" => {
                let mut atom = b"mvhd".to_vec();
                atom.extend_from_slice(payload);
                result.creation_utc = crate::util::extract_mvhd_creation_time(&atom);
            }
            _ => {}
        }
    }
    result
}

pub fn read_container<T: Read + Seek>(stream: &mut T, size: usize) -> std::io::Result<ClipData> {
    let mut position = 0u64;
    while position + 8 <= size as u64 {
        stream.seek(SeekFrom::Start(position))?;
        let mut head = [0u8; 8];
        stream.read_exact(&mut head)?;
        let (length, header) = match u32::from_be_bytes(head[..4].try_into().unwrap()) {
            0 => (size as u64 - position, 8),
            1 => {
                let mut n = [0; 8];
                stream.read_exact(&mut n)?;
                (u64::from_be_bytes(n), 16)
            }
            n => (n as u64, 8),
        };
        if length < header || length > size as u64 - position {
            break;
        }
        if &head[4..] == b"moov" && length - header <= MAX_METADATA_SIZE as u64 {
            let mut bytes = vec![0u8; (length - header) as usize];
            stream.read_exact(&mut bytes)?;
            return Ok(parse_moov(&bytes));
        }
        position += length;
    }
    Ok(ClipData::default())
}

pub fn detect_metadata(buffer: &[u8]) -> bool {
    for pos in memchr::memmem::find_iter(buffer, b"moov") {
        let Some(start) = pos.checked_sub(4) else {
            continue;
        };
        let Some((name, payload)) = boxes(&buffer[start..]).next() else {
            continue;
        };
        if &name == b"moov"
            && payload.len() <= MAX_METADATA_SIZE
            && parse_moov(payload).is_kinefinity
        {
            return true;
        }
    }
    false
}

pub fn detect_prores(buffer: &[u8]) -> bool {
    let mut pos = 0usize;
    while let Some(length) = u32_at(buffer, pos) {
        let Some(kind) = buffer.get(pos + 4..pos + 8) else {
            return false;
        };
        let (length, header) = if length == 1 {
            let Some(bytes) = buffer.get(pos + 8..pos + 16) else {
                return false;
            };
            (u64::from_be_bytes(bytes.try_into().unwrap()), 16)
        } else {
            (length as u64, 8)
        };
        if kind == b"mdat" {
            let Some(frame) = buffer.get(pos + header..) else {
                return false;
            };
            let frame_len = u32_at(frame, 0).unwrap_or(0) as u64;
            return frame.get(4..8) == Some(b"icpf")
                && frame
                    .get(12..16)
                    .is_some_and(|v| v.eq_ignore_ascii_case(b"KINE"))
                && frame_len >= 20
                && (length == 0 || frame_len <= length.saturating_sub(header as u64));
        }
        if length < header as u64 {
            return false;
        }
        let Some(next) = (pos as u64)
            .checked_add(length)
            .and_then(|v| usize::try_from(v).ok())
        else {
            return false;
        };
        if next > buffer.len() {
            return false;
        }
        pos = next;
    }
    false
}
