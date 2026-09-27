// SPDX-License-Identifier: MIT OR Apache-2.0

//! Kinefinity MOV metadata and shared sensor geometry.
//! Modern cameras embed QuickTime metadata; older clips use a matching slate sidecar.

use crate::tags_impl::*;
use crate::*;
use std::io::{Read, Seek};
use std::sync::{Arc, atomic::AtomicBool};

mod geometry;
mod metadata;
pub use geometry::{CameraGeometry, normalize_image_format, resolve_camera_geometry};
pub use metadata::ClipData as SlateData;

#[derive(Default)]
pub struct Kinefinity {
    pub model: Option<String>,
    pub lens: Option<String>,
    frame_readout_time: Option<f64>,
    video_path: String,
}

impl Kinefinity {
    pub fn camera_type(&self) -> String {
        "Kinefinity".to_owned()
    }
    pub fn has_accurate_timestamps(&self) -> bool {
        false
    }
    pub fn possible_extensions() -> Vec<&'static str> {
        vec!["mov"]
    }
    pub fn frame_readout_time(&self) -> Option<f64> {
        self.frame_readout_time
    }
    pub fn normalize_imu_orientation(v: String) -> String {
        v
    }

    pub fn detect<P: AsRef<std::path::Path>>(
        buffer: &[u8],
        filepath: P,
        options: &InputOptions,
    ) -> Option<Self> {
        let path = filepath.as_ref().to_str().unwrap_or_default();
        let recognized = metadata::detect_prores(buffer) || metadata::detect_metadata(buffer);
        let recognized = recognized
            || (!options.dont_look_for_sidecar_files && {
                let mut data = SlateData::default();
                metadata::merge_sidecars(path, &mut data);
                data.camera_model.as_deref().is_some_and(|model| {
                    let model = model.trim().to_ascii_uppercase();
                    let model = model.strip_prefix("KINEFINITY ").unwrap_or(&model);
                    model.starts_with("MAVO") || model == "TERRA 4K" || model == "VISTA"
                })
            });
        recognized.then(|| Self {
            video_path: path.to_owned(),
            ..Default::default()
        })
    }

    pub fn parse<T: Read + Seek, F: Fn(f64)>(
        &mut self,
        stream: &mut T,
        size: usize,
        progress_cb: F,
        _cancel_flag: Arc<AtomicBool>,
        options: InputOptions,
    ) -> std::io::Result<Vec<SampleInfo>> {
        let video = util::get_video_metadata(stream, size).ok();
        let mut data = metadata::read_container(stream, size)?;
        if !options.dont_look_for_sidecar_files {
            metadata::merge_sidecars(&self.video_path, &mut data);
        }
        let mut map = GroupedTagMap::new();
        self.process_map(&mut map, &options, video.as_ref(), &data);
        progress_cb(1.0);
        Ok(vec![SampleInfo {
            tag_map: Some(map),
            ..Default::default()
        }])
    }

    fn process_map(
        &mut self,
        map: &mut GroupedTagMap,
        options: &InputOptions,
        video: Option<&VideoMetadata>,
        data: &SlateData,
    ) {
        let width = video
            .map(|v| v.width as u32)
            .filter(|v| *v > 0)
            .or(data.width)
            .unwrap_or(0);
        let height = video
            .map(|v| v.height as u32)
            .filter(|v| *v > 0)
            .or(data.height)
            .unwrap_or(0);
        let project_fps = video
            .and_then(|v| metadata::positive(v.fps))
            .or(data.project_fps)
            .or(data.sensor_fps);
        let sensor_fps = data.sensor_fps.or(project_fps);
        let database = options
            .camera_db_path
            .as_deref()
            .and_then(|path| crate::camera_db::CameraDatabase::load(path).ok());

        self.model = data.camera_model.clone();
        if let (Some(db), Some(raw_model)) = (&database, &data.camera_model) {
            if let Some((name, _)) = geometry::find_model(db, raw_model) {
                db.process_model("KINEFINITY", name, map, options);
                self.model = Some(name.to_owned());
            }
        }
        if let Some(model) = &self.model {
            util::insert_tag(
                map,
                tag!(parsed GroupId::Default, TagId::Name, "Camera model", String, |v| v.clone(), model.clone(), Vec::new()),
                options,
            );
        }
        if width > 0 && height > 0 {
            util::insert_tag(
                map,
                tag!(parsed GroupId::Default, TagId::Custom("video_width".into()), "Video output width", u32, |v| format!("{v} px"), width, Vec::new()),
                options,
            );
            util::insert_tag(
                map,
                tag!(parsed GroupId::Default, TagId::Custom("video_height".into()), "Video output height", u32, |v| format!("{v} px"), height, Vec::new()),
                options,
            );
        }
        if let Some(fps) = project_fps {
            util::insert_tag(
                map,
                tag!(parsed GroupId::Default, TagId::FrameRate, "Frame rate", f64, |v| format!("{v:.3} fps"), fps, Vec::new()),
                options,
            );
        }
        if let Some(fps) = sensor_fps {
            util::insert_tag(
                map,
                tag!(parsed GroupId::Default, TagId::RecordFrameRate, "Record frame rate", f64, |v| format!("{v:.3} fps"), fps, Vec::new()),
                options,
            );
        }
        let focal = options
            .user_focal_length
            .and_then(metadata::positive)
            .or(data.focal_length)
            .filter(|v| (*v as f32).is_finite());
        if let Some(focal) = focal {
            util::insert_tag(
                map,
                tag!(parsed GroupId::Lens, TagId::FocalLength, "Focal length", f32, |v| format!("{v:.1} mm"), focal as f32, Vec::new()),
                options,
            );
        }
        self.lens = data.lens_name.clone();
        if let Some(lens) = &self.lens {
            util::insert_tag(
                map,
                tag!(parsed GroupId::Lens, TagId::DisplayName, "Lens", String, |v| v.clone(), lens.clone(), Vec::new()),
                options,
            );
        }
        let mut additional = serde_json::Map::new();
        if let Some(format) = &data.image_format {
            additional.insert("image_format".into(), normalize_image_format(format).into());
        }
        if let Some(over) = data.oversampling {
            additional.insert("oversampling".into(), over.into());
        }
        if let Some(fps) = sensor_fps {
            additional.insert("sensor_fps".into(), fps.into());
        }
        if let Some(fps) = project_fps {
            additional.insert("project_fps".into(), fps.into());
        }
        if let Some(firmware) = &data.firmware {
            additional.insert("camera_firmware".into(), firmware.clone().into());
        }
        if let (Some(db), Some(model), Some(fps)) = (&database, &self.model, sensor_fps) {
            if let Some(geometry) = resolve_camera_geometry(
                db,
                model,
                (width, height),
                fps,
                data.image_format.as_deref(),
                data.oversampling,
            ) {
                util::insert_tag(
                    map,
                    tag!(parsed GroupId::Default, TagId::Custom("crop_factor".into()), "Crop factor", f64, |v| format!("{v:.4}"), geometry.crop_factor, Vec::new()),
                    options,
                );
                util::insert_tag(
                    map,
                    tag!(parsed GroupId::Lens, TagId::Custom("unit_pixel_focal_length".into()), "Pixel focal length per mm", f64, |v| format!("{v:.4}"), geometry.unit_pixel_focal_length, Vec::new()),
                    options,
                );
                if let Some(focal) = focal {
                    let pixels = (focal * geometry.unit_pixel_focal_length) as f32;
                    if pixels.is_finite() && pixels > 0.0 {
                        util::insert_tag(
                            map,
                            tag!(parsed GroupId::Lens, TagId::PixelFocalLength, "Pixel focal length", f32, |v| format!("{v:.2}"), pixels, Vec::new()),
                            options,
                        );
                    }
                }
                additional.insert("crop_factor".into(), geometry.crop_factor.into());
                additional.insert("oversampling".into(), geometry.oversampling.into());
                if let Some(source_size) = geometry.source_size {
                    additional.insert(
                        "kinefinity_source_size".into(),
                        serde_json::json!(source_size),
                    );
                }
                additional.insert("readout_source".into(), geometry.readout_source.into());
                if let Some(readout) = geometry.readout {
                    self.frame_readout_time = Some(readout.readout_time_ms);
                    util::insert_tag(
                        map,
                        tag!(parsed GroupId::Imager, TagId::FrameReadoutTime, "Frame readout time", f64, |v| format!("{v:.4} ms"), readout.readout_time_ms, Vec::new()),
                        options,
                    );
                    util::insert_tag(
                        map,
                        tag!(parsed GroupId::Imager, TagId::Custom("readout_estimated".into()), "Readout time estimated", bool, |v| v.to_string(), readout.is_estimated, Vec::new()),
                        options,
                    );
                    additional.insert("readout_estimated".into(), readout.is_estimated.into());
                }
            }
        }
        if let Some(date) = &data.creation_utc {
            util::write_creation_date_tags(map, date, None, None, options);
        } else if let (Some(date), Some(tod)) = (&data.shot_date, &data.shot_tod) {
            if let Some(date) = normalize_slate_date(date) {
                // A local slate timestamp has no UTC offset. Preserve it without claiming UTC.
                if chrono::NaiveTime::parse_from_str(tod, "%H:%M:%S").is_ok() {
                    additional.insert("recorded_local_time".into(), format!("{date} {tod}").into());
                }
            }
        }
        if let Some(timecode) = &data.timecode {
            // The generic timecode tag enables a filename-to-UTC guess in Gyroflow.
            // A free-running camera timecode must remain independent of the creation date.
            additional.insert("recorded_timecode".into(), timecode.clone().into());
        }
        util::insert_tag(
            map,
            tag!(parsed GroupId::Default, TagId::ImageStabilizer, "Image stabilization", bool, |v| if *v { "On" } else { "Off" }.into(), false, Vec::new()),
            options,
        );
        let additional = serde_json::Value::Object(additional);
        util::insert_tag(
            map,
            tag!(parsed GroupId::Default, TagId::Metadata, "Metadata", Json, |v| v.to_string(), additional, Vec::new()),
            options,
        );
    }
}

pub fn parse_slate_file(path: &str) -> Option<SlateData> {
    metadata::parse_slate(&metadata::read_text(path)?)
}
pub fn parse_slate_content(content: &str) -> Option<SlateData> {
    metadata::parse_slate(content)
}

pub fn normalize_slate_date(date: &str) -> Option<String> {
    let parts: Vec<_> = date.trim().split(['/', '.', ':']).collect();
    if parts.len() != 3 {
        return None;
    }
    let date = chrono::NaiveDate::from_ymd_opt(
        parts[0].parse().ok()?,
        parts[1].parse().ok()?,
        parts[2].parse().ok()?,
    )?;
    Some(date.format("%Y:%m:%d").to_string())
}

pub fn find_slate_path(video_path: &str) -> Option<String> {
    metadata::sidecar_paths(video_path, "-slate.txt")
        .into_iter()
        .next()
}

#[cfg(test)]
mod tests;
