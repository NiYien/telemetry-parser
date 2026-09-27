// SPDX-License-Identifier: MIT OR Apache-2.0

use crate::camera_db::{CameraDatabase, ModelData, ReadoutResult};
use serde::Deserialize;
use std::collections::{BTreeMap, HashMap};

#[derive(Debug, Clone)]
pub struct CameraGeometry {
    pub source_size: Option<(u32, u32)>,
    pub crop_factor: f64,
    pub unit_pixel_focal_length: f64,
    pub oversampling: bool,
    pub readout: Option<ReadoutResult>,
    pub readout_source: &'static str,
}

#[derive(Deserialize)]
struct GeometryData {
    sensor_size: [u32; 2],
    #[serde(default)]
    native_format: Option<String>,
    #[serde(default)]
    sensor_widths: BTreeMap<u32, f64>,
    #[serde(default)]
    oversampling: Vec<OversamplingMode>,
    #[serde(default)]
    readout: Vec<ReadoutReference>,
}

#[derive(Deserialize)]
struct OversamplingMode {
    format: String,
    output: [u32; 2],
    source: [u32; 2],
}

#[derive(Deserialize)]
struct ReadoutReference {
    #[serde(default)]
    format: Option<String>,
    height: u32,
    ms: f64,
    #[serde(default)]
    fps: Option<[f64; 2]>,
}

pub fn normalize_image_format(value: &str) -> &str {
    let value = value.trim();
    for (names, canonical) in [
        (&["FF", "Full Frame", "FULL"][..], "FULL"),
        (&["M4/3", "MFT", "M43"][..], "M43"),
        (&["Super 35", "S35"][..], "S35"),
        (&["Super 16", "S16"][..], "S16"),
    ] {
        if names.iter().any(|name| value.eq_ignore_ascii_case(name)) {
            return canonical;
        }
    }
    value
}

pub(super) fn find_model<'a>(
    db: &'a CameraDatabase,
    raw: &str,
) -> Option<(&'a str, &'a ModelData)> {
    let mut raw = raw.trim();
    if raw
        .get(..10)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("Kinefinity"))
    {
        raw = raw.get(10..)?.trim();
    }
    let mut expanded = raw.to_owned();
    for (from, to) in &db.get_brand("KINEFINITY")?.aliases {
        expanded = expanded.replace(from.as_str(), to.as_str());
    }
    let compact = |name: &str| {
        name.chars()
            .filter(|c| !c.is_whitespace())
            .flat_map(char::to_lowercase)
            .collect::<String>()
    };
    let (name, data) = db.find_model("KINEFINITY", &expanded)?;
    // A future MAVO model must not inherit the original MAVO's smaller sensor by substring.
    (compact(&expanded) == compact(name)).then_some((name, data))
}

/// Resolve both automatic metadata and manual camera selections using the active database.
/// A missing oversampling flag is only inferred when the recorded image format disambiguates it.
pub fn resolve_camera_geometry(
    db: &CameraDatabase,
    model: &str,
    size: (u32, u32),
    sensor_fps: f64,
    image_format: Option<&str>,
    oversampling: Option<bool>,
) -> Option<CameraGeometry> {
    if size.0 == 0 || size.1 == 0 || !sensor_fps.is_finite() || sensor_fps <= 0.0 {
        return None;
    }
    let (name, model_data) = find_model(db, model)?;
    let sensor_w = model_data.sw as f64;
    if !sensor_w.is_finite() || sensor_w <= 0.0 {
        return None;
    }
    let format = image_format
        .map(normalize_image_format)
        .filter(|s| !s.is_empty());
    let config = model_data.extra.get("kinefinity");
    let Some(config) = config else {
        // Older installed lens packages retain their previous geometry until updated.
        let effective_w = legacy_sensor_width(name, format).unwrap_or(sensor_w);
        let readout = legacy_readout(db, name, size, sensor_fps, sensor_w);
        return Some(CameraGeometry {
            source_size: None,
            crop_factor: sensor_w / effective_w,
            unit_pixel_focal_length: size.0 as f64 / effective_w,
            oversampling: oversampling.unwrap_or(false),
            readout,
            readout_source: "legacy_table",
        });
    };
    let config: GeometryData = serde_json::from_value(config.clone()).ok()?;
    let [native_w, native_h] = config.sensor_size;
    if native_w == 0 || native_h == 0 {
        return None;
    }
    let format = format.or_else(|| {
        config
            .native_format
            .as_deref()
            .filter(|_| size.0 == native_w || size.1 == native_h)
    });

    let candidates: Vec<_> = config
        .oversampling
        .iter()
        .filter(|mode| {
            mode.output == [size.0, size.1]
                && format.is_none_or(|f| normalize_image_format(&mode.format) == f)
        })
        .collect();
    let source = match oversampling {
        Some(false) => [size.0, size.1],
        Some(true) => unique_source(&candidates)?,
        None if !candidates.is_empty() => {
            // Without the format this output may be either a native crop or a downsample.
            format?;
            unique_source(&candidates)?
        }
        None => [size.0, size.1],
    };
    if source[0] == 0
        || source[1] == 0
        || source[0] > native_w
        || source[1] > native_h
        || source[0] < size.0
        || source[1] < size.1
    {
        return None;
    }
    let effective_w = config
        .sensor_widths
        .get(&source[0])
        .copied()
        .unwrap_or(sensor_w * source[0] as f64 / native_w as f64);
    if !effective_w.is_finite() || effective_w <= 0.0 || effective_w > sensor_w * 1.001 {
        return None;
    }

    let reference = config.readout.iter().find(|r| {
        r.format
            .as_deref()
            .is_none_or(|f| format == Some(normalize_image_format(f)))
            && r.fps
                .is_none_or(|[lo, hi]| sensor_fps >= lo && sensor_fps <= hi)
    });
    let (readout, readout_source) = if let Some(reference) = reference {
        let ms = reference.ms.abs() * source[1] as f64 / reference.height as f64;
        let result =
            (reference.height > 0 && ms.is_finite() && ms > 0.0).then_some(ReadoutResult {
                readout_time_ms: ms,
                is_estimated: reference.ms < 0.0 || source[1] != reference.height,
            });
        (result, "scan_height_reference")
    } else if config.readout.is_empty() {
        // Old values have no reference height: do not invent a second height scaling.
        // Oversampling must look up the sensor scan, rather than the smaller encoded frame.
        (
            legacy_readout(db, name, (source[0], source[1]), sensor_fps, sensor_w),
            "legacy_table",
        )
    } else {
        (None, "unknown_scan_mode")
    };

    let crop_factor = sensor_w / effective_w;
    let unit_pixel_focal_length = size.0 as f64 / effective_w;
    if !crop_factor.is_finite() || !unit_pixel_focal_length.is_finite() {
        return None;
    }
    Some(CameraGeometry {
        source_size: Some((source[0], source[1])),
        crop_factor,
        unit_pixel_focal_length,
        oversampling: oversampling.unwrap_or(source != [size.0, size.1]),
        readout,
        readout_source,
    })
}

fn unique_source(modes: &[&OversamplingMode]) -> Option<[u32; 2]> {
    let first = modes.first()?.source;
    modes.iter().all(|m| m.source == first).then_some(first)
}

fn legacy_readout(
    db: &CameraDatabase,
    model: &str,
    size: (u32, u32),
    fps: f64,
    sw: f64,
) -> Option<ReadoutResult> {
    let mut result = db.lookup_readout(
        "KINEFINITY",
        model,
        size.0,
        size.1,
        fps.max(15.0),
        1.0,
        sw as f32,
        None,
        &HashMap::new(),
    )?;
    // The historical resolution buckets do not identify an independently measured scan mode.
    result.is_estimated = true;
    Some(result)
}

fn legacy_sensor_width(model: &str, format: Option<&str>) -> Option<f64> {
    match (model, format?) {
        ("MAVO Edge 8K" | "MAVO Edge 6K" | "MAVO LF" | "MAVO2 LF", "FULL") => Some(36.0),
        ("MAVO Edge 8K", "S35") => Some(27.0),
        ("MAVO Edge 6K" | "MAVO LF" | "MAVO2 LF", "S35") => Some(24.5),
        ("MAVO" | "MAVO2 S35", "S35") => Some(24.0),
        ("MAVO" | "MAVO2 S35", "M43") => Some(16.0),
        ("MAVO" | "MAVO2 S35", "S16") => Some(12.0),
        ("MAVO" | "MAVO2 S35", "16mm") => Some(8.0),
        ("TERRA 4K", "S35") => Some(19.5),
        ("TERRA 4K", "M43") => Some(14.62),
        ("TERRA 4K", "S16") => Some(9.7),
        _ => None,
    }
}
