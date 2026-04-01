// SPDX-License-Identifier: MIT OR Apache-2.0
// Copyright © 2025 Adrian <adrian.eddy at gmail>

//! Runtime JSON camera database loader and query engine.
//! Loads per-brand JSON files from a directory, providing model matching,
//! crop rule evaluation, and readout time lookup with brand-specific adjustments.

use std::collections::HashMap;
use std::path::Path;

use crate::util;
use crate::tags_impl::*;

/// Result of a readout time lookup.
#[derive(Debug, Clone)]
pub struct ReadoutResult {
    pub readout_time_ms: f64,
    pub is_estimated: bool,
}

// ---------------------------------------------------------------------------
// Data structures
// ---------------------------------------------------------------------------

/// Top-level camera database holding data for all loaded brands.
pub struct CameraDatabase {
    brands: HashMap<String, BrandData>,
}

/// All data for a single brand, parsed from one JSON file.
pub struct BrandData {
    /// Ordered alias replacements applied to raw camera names before matching.
    pub aliases: Vec<(String, String)>,
    /// Brand-specific crop type map (e.g. Nikon: EXIF code -> view angle name).
    pub crop_type_map: HashMap<u16, String>,
    /// Model entries ordered for longest-match lookup.
    pub models: Vec<(String, ModelData)>,
    /// Ordered crop rules; first fully matching rule wins.
    pub crop_rules: Vec<CropRule>,
    /// Readout time table.
    pub readout: ReadoutData,
    /// Ordered readout adjustment steps.
    pub readout_adjust: Vec<ReadoutAdjustStep>,
}

/// Per-model sensor and lens data.
#[derive(Debug, Clone)]
pub struct ModelData {
    /// Sensor width in millimeters.
    pub sw: f32,
    /// Fixed focal length (35mm equivalent) for fixed-lens cameras (e.g. Ricoh GR).
    pub fixed_fl: Option<f64>,
    /// If true, clear focal length from output (e.g. Leica Q with fixed lens).
    pub clear_fl: bool,
}

/// A single crop rule with optional conditions.
#[derive(Debug, Clone)]
pub struct CropRule {
    /// Model names this rule applies to.
    pub models: Vec<String>,
    /// Crop factor to return when all conditions match.
    pub crop: f64,
    /// Required view angle string (e.g. "DX", "FX").
    pub view_angle: Option<String>,
    /// If set, resolution width must be in this list.
    pub res_w: Option<Vec<u32>>,
    /// If set, resolution height must be in this list.
    pub res_h: Option<Vec<u32>>,
    /// If set, fps must be in [min, max] inclusive.
    pub fps_range: Option<(f64, f64)>,
    /// If set, all tag key/value pairs must match.
    pub tag_condition: Option<HashMap<String, serde_json::Value>>,
}

/// Readout time data table with column headers.
#[derive(Debug, Clone, Default)]
pub struct ReadoutData {
    /// Column names, e.g. ["8K60","8K30",...,"1K24"].
    pub columns: Vec<String>,
    /// Model name -> readout values aligned with `columns`. Negative = estimated.
    pub data: HashMap<String, Vec<Option<f64>>>,
    /// Model name -> additional override strings (e.g. "DR ON:27.5").
    pub additional: HashMap<String, Vec<String>>,
}

/// A single readout adjustment step, executed in order.
#[derive(Debug, Clone)]
pub enum ReadoutAdjustStep {
    /// Override readout with a value from `additional` when a tag is true.
    Override { pattern: String, when_tag: String },
    /// Override readout by matching resolution against "pattern:W:H:value" in additional.
    OverrideRes { pattern: String },
    /// Scale readout by dividing with an expression, only when res_w > min_w.
    Scale { expr: ScaleExpr, min_w: u32 },
}

/// Expression used in readout scaling adjustments.
#[derive(Debug, Clone)]
pub enum ScaleExpr {
    /// Divide by scale_35mm (Nikon).
    Scale35mm,
    /// Divide by (scale_35mm * sensor_w / 35.0) (Lumix).
    ScaleSensorNorm,
}

// ---------------------------------------------------------------------------
// Loading
// ---------------------------------------------------------------------------

impl CameraDatabase {
    /// Load all .json files from the given directory.
    /// Each file's stem (uppercased) becomes the brand key.
    pub fn load(dir_path: &str) -> std::io::Result<Self> {
        let mut brands = HashMap::new();
        let dir = Path::new(dir_path);

        if !dir.is_dir() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("Camera database directory not found: {}", dir_path),
            ));
        }

        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("json") {
                continue;
            }
            let brand = match path.file_stem().and_then(|s| s.to_str()) {
                Some(s) => s.to_uppercase(),
                None => continue,
            };

            let content = std::fs::read_to_string(&path)?;
            let json: serde_json::Value = serde_json::from_str(&content).map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Failed to parse {}: {}", path.display(), e),
                )
            })?;

            match parse_brand(&json) {
                Ok(data) => { brands.insert(brand, data); }
                Err(e) => {
                    log::warn!("Failed to parse brand data from {}: {}", path.display(), e);
                }
            }
        }

        Ok(Self { brands })
    }

    /// Check if a brand exists in the database.
    pub fn has_brand(&self, brand: &str) -> bool {
        self.brands.contains_key(&brand.to_uppercase())
    }

    /// Get raw brand data (for advanced use).
    pub fn get_brand(&self, brand: &str) -> Option<&BrandData> {
        self.brands.get(&brand.to_uppercase())
    }

    /// Find the best matching model for a raw camera name string.
    /// Returns the canonical model name and its data.
    pub fn find_model<'a>(&'a self, brand: &str, raw_name: &str) -> Option<(&'a str, &'a ModelData)> {
        let brand_data = self.brands.get(&brand.to_uppercase())?;

        // Apply alias replacements
        let mut processed = raw_name.to_string();
        for (from, to) in &brand_data.aliases {
            processed = processed.replace(from.as_str(), to.as_str());
        }

        // Collect model names for matching
        let model_names: Vec<&str> = brand_data.models.iter().map(|(n, _)| n.as_str()).collect();

        let matched = util::find_longest_substring(&processed, &model_names)?;

        // Find the model data for the matched name
        brand_data.models.iter()
            .find(|(n, _)| n.as_str() == matched)
            .map(|(n, d)| (n.as_str(), d))
    }

    /// Look up the crop type name from a numeric code (e.g. Nikon EXIF crop type).
    pub fn lookup_crop_type(&self, brand: &str, code: u16) -> Option<&str> {
        let brand_data = self.brands.get(&brand.to_uppercase())?;
        brand_data.crop_type_map.get(&code).map(|s| s.as_str())
    }

    /// Match the first crop rule that satisfies all conditions.
    pub fn match_crop(
        &self,
        brand: &str,
        model: &str,
        res_w: u32,
        res_h: u32,
        fps: f64,
        view_angle: Option<&str>,
        tags: &HashMap<String, serde_json::Value>,
    ) -> Option<f64> {
        let brand_data = self.brands.get(&brand.to_uppercase())?;

        for rule in &brand_data.crop_rules {
            // Model must be in the rule's model list
            if !rule.models.iter().any(|m| m == model) {
                continue;
            }
            // View angle must match if specified
            if let Some(va) = &rule.view_angle {
                if view_angle != Some(va.as_str()) {
                    continue;
                }
            }
            // Resolution width must be in list if specified
            if let Some(ws) = &rule.res_w {
                if !ws.contains(&res_w) {
                    continue;
                }
            }
            // Resolution height must be in list if specified
            if let Some(hs) = &rule.res_h {
                if !hs.contains(&res_h) {
                    continue;
                }
            }
            // FPS must be in range if specified
            if let Some((min, max)) = rule.fps_range {
                if fps < min || fps > max {
                    continue;
                }
            }
            // All tag conditions must match if specified
            if let Some(conds) = &rule.tag_condition {
                if !conds.iter().all(|(k, v)| tags.get(k) == Some(v)) {
                    continue;
                }
            }
            return Some(rule.crop);
        }

        None
    }

    /// Look up readout time for a model, applying brand-specific adjustments.
    pub fn lookup_readout(
        &self,
        brand: &str,
        model: &str,
        res_w: u32,
        res_h: u32,
        fps: f64,
        scale_35mm: f64,
        sensor_w: f32,
        tags: &HashMap<String, serde_json::Value>,
    ) -> Option<ReadoutResult> {
        let brand_data = self.brands.get(&brand.to_uppercase())?;
        let readout = &brand_data.readout;

        // Get the readout row for this model
        let row = readout.data.get(model)?;
        let additional = readout.additional.get(model).cloned().unwrap_or_default();

        // Execute override steps first (if any match, return immediately)
        for step in &brand_data.readout_adjust {
            match step {
                ReadoutAdjustStep::Override { pattern, when_tag } => {
                    let tag_is_true = tags.get(when_tag)
                        .and_then(|v| v.as_bool())
                        .unwrap_or(false);
                    if tag_is_true {
                        if let Some(val) = find_additional_value(&additional, pattern) {
                            return Some(ReadoutResult {
                                readout_time_ms: val,
                                is_estimated: false,
                            });
                        }
                    }
                }
                ReadoutAdjustStep::OverrideRes { pattern } => {
                    if let Some(val) = find_additional_res_match(&additional, pattern, res_w, res_h) {
                        return Some(ReadoutResult {
                            readout_time_ms: val,
                            is_estimated: false,
                        });
                    }
                }
                ReadoutAdjustStep::Scale { .. } => {
                    // Scale is applied after standard lookup, not here
                }
            }
        }

        // Standard column lookup with fallback
        let col_indices = resolve_columns(&readout.columns, res_w, fps);
        let mut value: Option<f64> = None;
        let mut is_estimated = false;

        for col in col_indices {
            if let Some(Some(v)) = row.get(col) {
                is_estimated = *v < 0.0;
                value = Some(v.abs());
                break;
            }
        }

        let mut readout_time = value?;

        // Apply scale adjustments
        for step in &brand_data.readout_adjust {
            if let ReadoutAdjustStep::Scale { expr, min_w } = step {
                if scale_35mm > 1.0 && res_w > *min_w {
                    let divisor = match expr {
                        ScaleExpr::Scale35mm => scale_35mm,
                        ScaleExpr::ScaleSensorNorm => scale_35mm * sensor_w as f64 / 35.0,
                    };
                    if divisor > 0.0 {
                        readout_time /= divisor;
                    }
                }
            }
        }

        Some(ReadoutResult {
            readout_time_ms: readout_time,
            is_estimated,
        })
    }

    // -----------------------------------------------------------------------
    // Convenience methods for brand modules
    // -----------------------------------------------------------------------

    /// Process model matching and write Name + SensorWidth tags.
    /// Returns `(canonical_name, ModelData)` if found.
    pub fn process_model<'a>(&'a self, brand: &str, raw_name: &str, map: &mut crate::tags_impl::GroupedTagMap, options: &crate::InputOptions) -> Option<(&'a str, &'a ModelData)> {
        let (name, data) = self.find_model(brand, raw_name)?;
        util::insert_tag(map, crate::tag!(parsed crate::tags_impl::GroupId::Default, crate::tags_impl::TagId::Name, "Camera model", String, |v| v.clone(), name.to_string(), Vec::new()), options);
        util::insert_tag(map, crate::tag!(parsed crate::tags_impl::GroupId::Default, crate::tags_impl::TagId::Custom("SensorWidth".into()), "Sensor width (mm)", f32, |v| format!("{:.1} mm", v), data.sw, Vec::new()), options);
        Some((name, data))
    }

    /// Process crop matching and write crop_factor tag. Returns effective crop (1.0 if no rule matched).
    pub fn process_crop(&self, brand: &str, model: &str, res_w: u32, res_h: u32, fps: f64, view_angle: Option<&str>, tags: &HashMap<String, serde_json::Value>, map: &mut crate::tags_impl::GroupedTagMap, options: &crate::InputOptions) -> f64 {
        let crop = self.match_crop(brand, model, res_w, res_h, fps, view_angle, tags);
        if let Some(cf) = crop {
            util::insert_tag(map, crate::tag!(parsed crate::tags_impl::GroupId::Default, crate::tags_impl::TagId::Custom("crop_factor".into()), "Crop factor", f64, |v| format!("{:.4}", v), cf, Vec::new()), options);
        }
        crop.unwrap_or(1.0)
    }

    /// Process readout lookup and write FrameReadoutTime tag. Returns readout_time_ms if found.
    pub fn process_readout(&self, brand: &str, model: &str, res_w: u32, res_h: u32, fps: f64, scale_35mm: f64, sensor_w: f32, tags: &HashMap<String, serde_json::Value>, map: &mut crate::tags_impl::GroupedTagMap, options: &crate::InputOptions) -> Option<f64> {
        let result = self.lookup_readout(brand, model, res_w, res_h, fps, scale_35mm, sensor_w, tags)?;
        util::insert_tag(map, crate::tag!(parsed crate::tags_impl::GroupId::Imager, crate::tags_impl::TagId::FrameReadoutTime, "Frame readout time", f64, |v| format!("{:.4} ms", v), result.readout_time_ms, Vec::new()), options);
        if result.is_estimated {
            util::insert_tag(map, crate::tag!(parsed crate::tags_impl::GroupId::Imager, crate::tags_impl::TagId::Custom("readout_estimated".into()), "Readout time estimated", bool, |v| v.to_string(), true, Vec::new()), options);
        }
        Some(result.readout_time_ms)
    }
}

// ---------------------------------------------------------------------------
// Column resolution helpers
// ---------------------------------------------------------------------------

/// Map resolution width to a column prefix.
fn resolution_prefix(res_w: u32) -> Option<&'static str> {
    if res_w >= 5000 {
        Some("8K")
    } else if res_w >= 3400 {
        Some("4K")
    } else if res_w >= 1920 {
        Some("1K")
    } else {
        None
    }
}

/// Map fps to a column suffix.
fn fps_suffix(fps: f64) -> Option<&'static str> {
    if fps >= 180.0 {
        Some("240")
    } else if fps >= 80.0 {
        Some("120")
    } else if fps >= 45.0 {
        Some("60")
    } else if fps >= 26.0 {
        Some("30")
    } else if fps >= 15.0 {
        Some("24")
    } else {
        None
    }
}

/// Resolution prefixes in fallback order starting from the given prefix.
fn resolution_fallback(prefix: &str) -> &'static [&'static str] {
    match prefix {
        "8K" => &["8K", "4K", "1K"],
        "4K" => &["4K", "1K"],
        "1K" => &["1K"],
        _ => &[],
    }
}

/// FPS suffixes in fallback order starting from the given suffix.
fn fps_fallback(suffix: &str) -> &'static [&'static str] {
    match suffix {
        "240" => &["240", "120", "60", "30", "24"],
        "120" => &["120", "60", "30", "24"],
        "60"  => &["60", "30", "24"],
        "30"  => &["30", "24"],
        "24"  => &["24"],
        _ => &[],
    }
}

/// Resolve (resolution_w, fps) to a list of column indices to try, with fallback.
/// Returns indices into the `columns` array, ordered by preference.
fn resolve_columns(columns: &[String], res_w: u32, fps: f64) -> Vec<usize> {
    let primary_prefix = match resolution_prefix(res_w) {
        Some(p) => p,
        None => return Vec::new(),
    };
    let primary_suffix = match fps_suffix(fps) {
        Some(s) => s,
        None => return Vec::new(),
    };

    let mut result = Vec::new();
    for &res_pfx in resolution_fallback(primary_prefix) {
        for &fps_sfx in fps_fallback(primary_suffix) {
            let key = format!("{}{}", res_pfx, fps_sfx);
            if let Some(idx) = columns.iter().position(|c| c == &key) {
                result.push(idx);
            }
        }
    }
    result
}

// ---------------------------------------------------------------------------
// Additional value helpers
// ---------------------------------------------------------------------------

/// Find "pattern:value" in additional strings and parse the value as f64.
fn find_additional_value(additional: &[String], pattern: &str) -> Option<f64> {
    for entry in additional {
        if let Some(rest) = entry.strip_prefix(pattern) {
            if let Ok(v) = rest.parse::<f64>() {
                return Some(v);
            }
        }
    }
    None
}

/// Find "pattern:W:H:value" in additional strings where W and H match.
fn find_additional_res_match(additional: &[String], pattern: &str, res_w: u32, res_h: u32) -> Option<f64> {
    for entry in additional {
        if let Some(rest) = entry.strip_prefix(pattern) {
            let parts: Vec<&str> = rest.split(':').collect();
            if parts.len() == 3 {
                if let (Ok(w), Ok(h), Ok(v)) = (
                    parts[0].parse::<u32>(),
                    parts[1].parse::<u32>(),
                    parts[2].parse::<f64>(),
                ) {
                    if w == res_w && h == res_h {
                        return Some(v);
                    }
                }
            }
        }
    }
    None
}

// ---------------------------------------------------------------------------
// JSON parsing
// ---------------------------------------------------------------------------

/// Parse a single brand JSON value into BrandData.
fn parse_brand(json: &serde_json::Value) -> Result<BrandData, String> {
    let obj = json.as_object().ok_or("Brand JSON root must be an object")?;

    // aliases: { "from": "to", ... } -> Vec<(String, String)>
    let aliases = match obj.get("aliases") {
        Some(v) => {
            let map = v.as_object().ok_or("aliases must be an object")?;
            let mut result = Vec::new();
            for (k, v) in map {
                if let Some(s) = v.as_str() {
                    // "short_name": "canonical_name" -> replace short_name with canonical_name
                    result.push((k.clone(), s.to_string()));
                } else if let Some(arr) = v.as_array() {
                    // "canonical_name": ["alias1", "alias2"] -> replace each alias with canonical_name
                    for item in arr {
                        if let Some(alias) = item.as_str() {
                            result.push((alias.to_string(), k.clone()));
                        }
                    }
                }
            }
            result
        }
        None => Vec::new(),
    };

    // crop_type_map: { "6": "FX", ... } -> HashMap<u16, String>
    let crop_type_map = match obj.get("crop_type_map") {
        Some(v) => {
            let map = v.as_object().ok_or("crop_type_map must be an object")?;
            let mut result = HashMap::new();
            for (k, v) in map {
                let code: u16 = k.parse().map_err(|_| format!("crop_type_map key '{}' must be a u16", k))?;
                let name = v.as_str().ok_or_else(|| format!("crop_type_map value for '{}' must be a string", k))?;
                result.insert(code, name.to_string());
            }
            result
        }
        None => HashMap::new(),
    };

    // models: { "Z 9": { "sw": 35.9, ... }, ... }
    let models = match obj.get("models") {
        Some(v) => {
            let map = v.as_object().ok_or("models must be an object")?;
            let mut entries: Vec<(String, ModelData)> = Vec::new();
            for (name, data) in map {
                let data_obj = data.as_object().ok_or_else(|| format!("model '{}' must be an object", name))?;
                let sw = data_obj.get("sw")
                    .and_then(|v| v.as_f64())
                    .ok_or_else(|| format!("model '{}' missing 'sw' (sensor width)", name))? as f32;
                let fixed_fl = data_obj.get("fixed_fl").and_then(|v| v.as_f64());
                let clear_fl = data_obj.get("clear_fl").and_then(|v| v.as_bool()).unwrap_or(false);
                entries.push((name.clone(), ModelData { sw, fixed_fl, clear_fl }));
            }
            // Sort by name length descending for longest-match priority
            entries.sort_by(|a, b| b.0.len().cmp(&a.0.len()));
            entries
        }
        None => Vec::new(),
    };

    // crop: [ { "m": [...], "c": 1.5, ... }, ... ]
    let crop_rules = match obj.get("crop") {
        Some(v) => {
            let arr = v.as_array().ok_or("crop must be an array")?;
            let mut rules = Vec::new();
            for item in arr {
                rules.push(parse_crop_rule(item)?);
            }
            rules
        }
        None => Vec::new(),
    };

    // readout
    let readout = match obj.get("readout") {
        Some(v) => parse_readout_data(v)?,
        None => ReadoutData::default(),
    };

    // readout_adjust
    let readout_adjust = match obj.get("readout_adjust") {
        Some(v) => {
            let arr = v.as_array().ok_or("readout_adjust must be an array")?;
            let mut steps = Vec::new();
            for item in arr {
                steps.push(parse_readout_adjust_step(item)?);
            }
            steps
        }
        None => Vec::new(),
    };

    Ok(BrandData {
        aliases,
        crop_type_map,
        models,
        crop_rules,
        readout,
        readout_adjust,
    })
}

/// Parse a single crop rule from JSON.
fn parse_crop_rule(json: &serde_json::Value) -> Result<CropRule, String> {
    let obj = json.as_object().ok_or("crop rule must be an object")?;

    let models = obj.get("m")
        .and_then(|v| v.as_array())
        .ok_or("crop rule missing 'm' (models array)")?
        .iter()
        .filter_map(|v| v.as_str().map(String::from))
        .collect();

    let crop = obj.get("c")
        .and_then(|v| v.as_f64())
        .ok_or("crop rule missing 'c' (crop value)")?;

    let view_angle = obj.get("va").and_then(|v| v.as_str()).map(String::from);

    let res_w = obj.get("w").and_then(|v| {
        v.as_array().map(|arr| {
            arr.iter().filter_map(|v| v.as_u64().map(|n| n as u32)).collect()
        })
    });

    let res_h = obj.get("h").and_then(|v| {
        v.as_array().map(|arr| {
            arr.iter().filter_map(|v| v.as_u64().map(|n| n as u32)).collect()
        })
    });

    let fps_range = obj.get("fps").and_then(|v| {
        let arr = v.as_array()?;
        if arr.len() == 2 {
            let min = arr[0].as_f64()?;
            let max = arr[1].as_f64()?;
            Some((min, max))
        } else {
            None
        }
    });

    let tag_condition = obj.get("tag").and_then(|v| {
        v.as_object().map(|map| {
            map.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
        })
    });

    Ok(CropRule {
        models,
        crop,
        view_angle,
        res_w,
        res_h,
        fps_range,
        tag_condition,
    })
}

/// Parse the readout data section from JSON.
fn parse_readout_data(json: &serde_json::Value) -> Result<ReadoutData, String> {
    let obj = json.as_object().ok_or("readout must be an object")?;

    let columns = obj.get("columns")
        .and_then(|v| v.as_array())
        .map(|arr| arr.iter().filter_map(|v| v.as_str().map(String::from)).collect())
        .unwrap_or_default();

    let data = match obj.get("data") {
        Some(v) => {
            let map = v.as_object().ok_or("readout.data must be an object")?;
            let mut result = HashMap::new();
            for (model, values) in map {
                let arr = values.as_array()
                    .ok_or_else(|| format!("readout.data.'{}' must be an array", model))?;
                let parsed: Vec<Option<f64>> = arr.iter().map(|v| v.as_f64()).collect();
                result.insert(model.clone(), parsed);
            }
            result
        }
        None => HashMap::new(),
    };

    let additional = match obj.get("additional") {
        Some(v) => {
            let map = v.as_object().ok_or("readout.additional must be an object")?;
            let mut result = HashMap::new();
            for (model, values) in map {
                let arr = values.as_array()
                    .ok_or_else(|| format!("readout.additional.'{}' must be an array", model))?;
                let parsed: Vec<String> = arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect();
                result.insert(model.clone(), parsed);
            }
            result
        }
        None => HashMap::new(),
    };

    Ok(ReadoutData { columns, data, additional })
}

/// Parse a readout_adjust step from JSON.
fn parse_readout_adjust_step(json: &serde_json::Value) -> Result<ReadoutAdjustStep, String> {
    let obj = json.as_object().ok_or("readout_adjust step must be an object")?;
    let step_type = obj.get("type")
        .and_then(|v| v.as_str())
        .ok_or("readout_adjust step missing 'type'")?;

    match step_type {
        "override" => {
            let pattern = obj.get("pattern")
                .and_then(|v| v.as_str())
                .ok_or("override step missing 'pattern'")?
                .to_string();
            let when_tag = obj.get("when_tag")
                .and_then(|v| v.as_str())
                .ok_or("override step missing 'when_tag'")?
                .to_string();
            Ok(ReadoutAdjustStep::Override { pattern, when_tag })
        }
        "override_res" => {
            let pattern = obj.get("pattern")
                .and_then(|v| v.as_str())
                .ok_or("override_res step missing 'pattern'")?
                .to_string();
            Ok(ReadoutAdjustStep::OverrideRes { pattern })
        }
        "scale" => {
            let expr_str = obj.get("expr")
                .and_then(|v| v.as_str())
                .ok_or("scale step missing 'expr'")?;
            let expr = match expr_str {
                "scale_35mm" => ScaleExpr::Scale35mm,
                "scale_sensor_norm" => ScaleExpr::ScaleSensorNorm,
                other => return Err(format!("unknown scale expr: '{}'", other)),
            };
            let min_w = obj.get("min_w")
                .and_then(|v| v.as_u64())
                .ok_or("scale step missing 'min_w'")? as u32;
            Ok(ReadoutAdjustStep::Scale { expr, min_w })
        }
        other => Err(format!("unknown readout_adjust type: '{}'", other)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resolve_columns() {
        let columns: Vec<String> = vec![
            "8K60","8K30","8K24",
            "4K240","4K120","4K60","4K30","4K24",
            "1K240","1K120","1K60","1K30","1K24",
        ].into_iter().map(String::from).collect();

        // 4K 60fps -> should find "4K60" first, then fallback within 4K, then 1K
        let result = resolve_columns(&columns, 3840, 60.0);
        assert!(!result.is_empty());
        assert_eq!(columns[result[0]], "4K60");

        // 8K 30fps -> should find "8K30" first
        let result = resolve_columns(&columns, 7680, 30.0);
        assert!(!result.is_empty());
        assert_eq!(columns[result[0]], "8K30");

        // 1080p 120fps -> should find "1K120"
        let result = resolve_columns(&columns, 1920, 100.0);
        assert!(!result.is_empty());
        assert_eq!(columns[result[0]], "1K120");
    }

    #[test]
    fn test_find_additional_value() {
        let additional = vec!["DR ON:27.5".to_string(), "Fine:14.3".to_string()];
        assert_eq!(find_additional_value(&additional, "DR ON:"), Some(27.5));
        assert_eq!(find_additional_value(&additional, "Fine:"), Some(14.3));
        assert_eq!(find_additional_value(&additional, "Missing:"), None);
    }

    #[test]
    fn test_find_additional_res_match() {
        let additional = vec!["OpenGate:5952:3968:29.7".to_string()];
        assert_eq!(find_additional_res_match(&additional, "OpenGate:", 5952, 3968), Some(29.7));
        assert_eq!(find_additional_res_match(&additional, "OpenGate:", 3840, 2160), None);
    }
}
