// SPDX-License-Identifier: MIT OR Apache-2.0
// Copyright © 2022 Adrian <adrian.eddy at gmail>

pub mod dvtm_wm169;
pub mod dvtm_eagle4_wa530;
pub mod dvtm_oq101;

use std::io::*;
use std::sync::{ Arc, atomic::AtomicBool };

use crate::tags_impl::*;
use crate::*;
use crate::util::insert_tag;
use memchr::memmem;
use prost::Message;

mod csv;
pub(crate) mod timing;
use timing::HeldQuaternions;

#[derive(Clone, PartialEq, ::prost::Message)]
struct HeaderProbe {
    #[prost(message, optional, tag = "1")]
    clip_meta: Option<ClipMetaProbe>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
struct ClipMetaProbe {
    #[prost(message, optional, tag = "1")]
    clip_meta_header: Option<ClipMetaHeaderProbe>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
struct ClipMetaHeaderProbe {
    #[prost(string, tag = "1")]
    proto_file_name: String,
    #[prost(string, tag = "10")]
    product_name: String,
}

pub(crate) fn video_frame_rate(ctx: &mp4parse::MediaContext) -> Option<f64> {
    let track = ctx.tracks.iter().find(|x| x.track_type == mp4parse::TrackType::Video)?;
    let timescale = track.timescale?.0;
    let table: Vec<(u32, u32)> = track.stts.as_ref()?.samples.iter().map(|s| (s.sample_count, s.sample_delta)).collect();
    let duration = timing::frame_duration(&table)?;
    (timescale > 0).then(|| timescale as f64 / duration)
}

#[derive(Default)]
pub struct Dji {
    pub model: Option<String>,
    pub frame_readout_time: Option<f64>
}

#[derive(PartialEq, Debug, Clone, Copy)]
enum DeviceProtobuf {
    Unknown,
    Wm169,
    Wa530,
    Oq101,
}

impl Dji {
    pub fn camera_type(&self) -> String {
        "DJI".to_owned()
    }
    pub fn has_accurate_timestamps(&self) -> bool {
        true
    }
    pub fn possible_extensions() -> Vec<&'static str> {
        vec!["mp4", "mov", "osv", "csv"]
    }
    pub fn frame_readout_time(&self) -> Option<f64> {
        self.frame_readout_time
    }
    pub fn normalize_imu_orientation(v: String) -> String {
        v
    }

    pub fn detect<P: AsRef<std::path::Path>>(buffer: &[u8], _filepath: P, _options: &crate::InputOptions) -> Option<Self> {
        if memmem::find(buffer, b"djmd").is_some() && (memmem::find(buffer, b"DJI meta").is_some() || memmem::find(buffer, b"CAM meta").is_some()) {
            Some(Self {
                model: None,
                frame_readout_time: None
            })
        } else if memmem::find(buffer, b"Clock:Tick").is_some() && memmem::find(buffer, b"IMU_ATTI(0):gyroX").is_some() {
            Some(Self {
                model: Some("CSV flight log".into()),
                frame_readout_time: None
            })
        } else {
            None
        }
    }

    pub fn parse<T: Read + Seek, F: Fn(f64)>(&mut self, stream: &mut T, size: usize, progress_cb: F, cancel_flag: Arc<AtomicBool>, options: crate::InputOptions) -> Result<Vec<SampleInfo>> {
        if self.model.is_some() {
            return csv::parse(stream, size, options);
        }

        let mut samples = Vec::new();
        let mut first_timestamp: Option<(i64, f64)> = None;
        let mut frames = Vec::new();
        let mut held_quaternions = Vec::new();
        let mut stated_sensor_fps = None;

        let mut focal_length = None;
        let mut distortion_coeffs = None;
                let mut fps = 59.94;
                // let mut global_quat_i = 0;

        // let mut first_vsync = 0;
        let mut prev_quat: Option<Quaternion<f64>> = None;
        let mut inv = false;

        let mut which_proto = DeviceProtobuf::Unknown;

        let cancel_flag2 = cancel_flag.clone();
        let ctx = util::get_metadata_track_samples(stream, size, true, |mut info: SampleInfo, data: &[u8], file_position: u64, _video_md: Option<&VideoMetadata>| {
            if size > 0 {
                progress_cb(file_position as f64 / size as f64);
            }

            if which_proto == DeviceProtobuf::Unknown {
                let header = HeaderProbe::decode(data).ok().and_then(|h| h.clip_meta?.clip_meta_header).unwrap_or_default();
                let product = format!("{} {}", header.proto_file_name, header.product_name).to_ascii_lowercase();
                which_proto = if product.contains("oq101") { DeviceProtobuf::Oq101 }
                    else if product.contains("wa530") { DeviceProtobuf::Wa530 }
                    else { DeviceProtobuf::Wm169 };
                log::debug!("Using device protobuf: {which_proto:?}");
            }

            macro_rules! handle_parsed {
                ($parsed:expr, $field:tt $(, $sub:tt)?) => {
                    let mut tag_map = GroupedTagMap::new();

                    if let Some(ref clip) = $parsed.clip_meta {
                        self.model              = clip.clip_meta_header       .as_ref().map(|h| h.product_name.replace("DJI ", ""));
                        self.frame_readout_time = clip.sensor_readout_time    .as_ref().map(|h| h.readout_time as f64 / 1000_000.0);
                        focal_length            = clip.digital_focal_length   .as_ref().map(|h| h.focal_length as f64);
                        distortion_coeffs       = clip.distortion_coefficients.as_ref().map(|h| h.coeffients.clone());

                        if let Some(v) = clip.sensor_fps.as_ref().map(|h| h.sensor_frame_rate as f64) {
                            stated_sensor_fps = Some(v);
                        }

                        let v = serde_json::to_value(&clip).map_err(|_| Error::new(ErrorKind::Other, "Serialize error"));
                        if let Ok(vv) = v {
                            log::debug!("Metadata: {:?}", &vv);
                            insert_tag(&mut tag_map, tag!(parsed GroupId::Default, TagId::Metadata, "Metadata", Json, |v| serde_json::to_string(v).unwrap(), vv, vec![]), &options);
                        }
                        if let Some(ref stream) = $parsed.stream_meta {
                            if let Some(ref meta) = stream.video_stream_meta {
                                fps = meta.framerate as f64;
                            }
                        }
                    }

                    if let Some((frame, header)) = $parsed.frame_meta.as_ref().and_then(|frame| Some((frame, frame.frame_meta_header.as_ref()?))) {
                        let frame_ts = header.frame_timestamp as i64;
                        let (first_frame_ts, _) = *first_timestamp.get_or_insert((frame_ts, info.timestamp_ms));
                        let frame_relative_ts = frame_ts - first_frame_ts;
                        frames.push(timing::Frame { sample: info.sample_index, ms: info.timestamp_ms, us: frame_relative_ts });
                        if let Some(ref imu) = frame.imu_frame_meta {
                            if let Some(attitude) = imu.$field.as_ref()$(.and_then(|m| m.$sub.as_ref()))? {
                                let len = attitude.attitude.len() as f64;
                                let mut readings = Vec::new();
                                for (i, q) in attitude.attitude.iter().enumerate() {
                                    if ![q.quaternion_w, q.quaternion_x, q.quaternion_y, q.quaternion_z].into_iter().all(f32::is_finite) { continue; }
                                    let quat = util::multiply_quats((q.quaternion_w as f64, q.quaternion_x as f64, q.quaternion_y as f64, q.quaternion_z as f64), (0.5, -0.5, -0.5, 0.5));
                                    let quat = util::multiply_quats((0.0, 0.0, 1.0, 0.0), (quat.w, quat.x, quat.y, quat.z));
                                    if quat.norm_squared() == 0.0 { continue; }
                                    if prev_quat.is_some_and(|previous| (previous - quat).norm_squared().sqrt() > 1.5) { inv = !inv; }
                                    prev_quat = Some(quat);
                                    readings.push(((i as f64 - attitude.offset as f64) / len, if inv { -quat } else { quat }));
                                }
                                held_quaternions.push(HeldQuaternions { sample: samples.len(), frame_relative_us: frame_relative_ts, readings });
                            }
                        }
                    }

                    // if info.index == 0 { dbg!(&parsed); }

                    info.tag_map = Some(tag_map);

                    samples.push(info);

                    if options.probe_only {
                        cancel_flag2.store(true, std::sync::atomic::Ordering::Relaxed);
                    }
                };
            }

            match which_proto {
                DeviceProtobuf::Unknown => { },
                DeviceProtobuf::Wm169 => match dvtm_wm169::ProductMeta::decode(data) {
                    Ok(parsed) => { handle_parsed!(parsed, imu_attitude_after_fusion); },
                    Err(e) => { log::warn!("Failed to parse protobuf: {:?}", e); }
                },
                DeviceProtobuf::Wa530 => match dvtm_eagle4_wa530::ProductMeta::decode(data) {
                    Ok(parsed) => { handle_parsed!(parsed, imu_single_attitude_after_fusion); },
                    Err(e) => { log::warn!("Failed to parse protobuf: {:?}", e); }
                },
                DeviceProtobuf::Oq101 => match dvtm_oq101::ProductMeta::decode(data) {
                    Ok(parsed) => { handle_parsed!(parsed, imu_attitude_after_fusion, current_frame); },
                    Err(e) => { log::warn!("Failed to parse protobuf: {:?}", e); }
                },
            }
        }, cancel_flag)?;

        let video_fps = video_frame_rate(&ctx).filter(|fps| fps.is_finite() && *fps > 0.0).unwrap_or(fps);
        let sensor_fps = timing::sensor_rate(&frames, 1000.0 / video_fps, stated_sensor_fps).unwrap_or(video_fps);
        if let Some(readout) = self.frame_readout_time.as_mut() { *readout /= video_fps / sensor_fps; }
        let first_frame_ms = first_timestamp.map_or(0.0, |(_, ms)| ms);
        for held in held_quaternions {
            let sample = held.sample;
            let data = held.timed(first_frame_ms, sensor_fps, video_fps);
            if let Some(map) = samples.get_mut(sample).and_then(|sample| sample.tag_map.as_mut()) {
                insert_tag(map, tag!(parsed GroupId::Quaternion, TagId::Data, "Quaternion data", Vec_TimeQuaternion_f64, |v| format!("{:?}", v), data, vec![]), &options);
            }
        }

        match (samples.first_mut(), focal_length, distortion_coeffs) {
            (Some(sample), Some(focal_length), Some(coeffs)) if coeffs.len() >= 4 => {
                if let Some(tkhd) = ctx.tracks.iter().filter(|x| x.track_type == mp4parse::TrackType::Video).filter_map(|x| x.tkhd.as_ref()).next() {
                    let (w, h) = (tkhd.width >> 16, tkhd.height >> 16);

                    let profile = self.get_lens_profile(w, h, focal_length, &coeffs);
                    if let Some(ref mut tag_map) = sample.tag_map {
                        insert_tag(tag_map, tag!(parsed GroupId::Lens, TagId::Data, "Lens profile", Json, |v| serde_json::to_string(v).unwrap(), profile, vec![]), &options);
                    }
                }
            },
            _ => { }
        }

        Ok(samples)
    }

    fn get_lens_profile(&self, width: u32, height: u32, focal_length: f64, coeffs: &[f32]) -> serde_json::Value {
        let model = self.model.clone().unwrap_or_default();
        let half_width = width as f64 / 2.0;
        let half_height = height as f64 / 2.0;
        let output_size = Self::get_output_size(width, height);
        serde_json::json!({
            "calibrated_by": "DJI",
            "camera_brand": "DJI",
            "camera_model": model,
            "calib_dimension":  { "w": width, "h": height },
            "orig_dimension":   { "w": width, "h": height },
            "output_dimension": { "w": output_size.0, "h": output_size.1 },
            "frame_readout_time": self.frame_readout_time,
            "official": true,
            "fisheye_params": {
              "camera_matrix": [
                [ focal_length, 0.0, half_width ],
                [ 0.0, focal_length, half_height ],
                [ 0.0, 0.0, 1.0 ]
              ],
              "distortion_coeffs": coeffs
            },
            "sync_settings": {
              "initial_offset": 0,
              "initial_offset_inv": false,
              "search_size": 0.5,
              "max_sync_points": 5,
              "every_nth_frame": 1,
              "time_per_syncpoint": 0.6,
              "do_autosync": false
            },
            "calibrator_version": "---"
        })
    }

    fn get_output_size(width: u32, height: u32) -> (u32, u32) {
        let aspect = (width as f64 / height as f64 * 100.0) as u32;
        match aspect {
            133 => (width, (width as f64 / 1.7777777777777).round() as u32), // 4:3 -> 16:9
            _   => (width, height)
        }
    }
}
