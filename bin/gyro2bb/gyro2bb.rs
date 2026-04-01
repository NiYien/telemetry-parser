// SPDX-License-Identifier: MIT OR Apache-2.0
// Copyright © 2021 Adrian <adrian.eddy at gmail>

use std::time::Instant;
use argh::FromArgs;
use std::sync::{ Arc, atomic::AtomicBool };

use telemetry_parser::*;
use telemetry_parser::tags_impl::*;

/** gyro2bb v0.2.8
Author: Adrian <adrian.eddy@gmail.com>

Extract gyro data from Sony, GoPro and Insta360 cameras to betaflight blackbox csv log
*/
#[derive(FromArgs)]
struct Opts {
    /// input file
    #[argh(positional)]
    input: String,

    /// dump all metadata
    #[argh(switch, short = 'd')]
    dump: bool,

    /// print summary of key fields (crop, readout, resolution, focal length)
    #[argh(switch, short = 's')]
    summary: bool,

    /// IMU orientation (XYZ, ZXY etc, lowercase is negative, eg. xZy)
    #[argh(option)]
    imuo: Option<String>,

    /// path to camera_db directory containing per-brand JSON files
    #[argh(option)]
    camera_db: Option<String>,
}

fn main() {
    let opts: Opts = argh::from_env();
    let _time = Instant::now();

    let mut stream = std::fs::File::open(&opts.input).unwrap();
    let filesize = stream.metadata().unwrap().len() as usize;

    let mut options = InputOptions::default();
    options.camera_db_path = opts.camera_db;

    let input = Input::from_stream_with_options(&mut stream, filesize, &opts.input, |_|(), Arc::new(AtomicBool::new(false)), options).unwrap();

    let mut i = 0;
    println!("Detected camera: {} {}", input.camera_type(), input.camera_model().unwrap_or(&"".into()));

    let samples = input.samples.as_ref().unwrap();

    if opts.dump {
        for info in samples {
            if info.tag_map.is_none() { continue; }
            let grouped_tag_map = info.tag_map.as_ref().unwrap();

            for (group, map) in grouped_tag_map {
                for (tagid, taginfo) in map {
                    println!("{: <25} {: <25} {: <50}: {}", format!("{}", group), format!("{}", tagid), taginfo.description, &taginfo.value.to_string());
                }
            }
        }
    }

    if opts.summary {
        // Collect key fields from first sample's tag_map
        if let Some(map) = samples.first().and_then(|s| s.tag_map.as_ref()) {
            let get_str = |g: GroupId, t: TagId| -> String {
                map.get(&g).and_then(|m| m.get(&t)).map(|v| v.value.to_string()).unwrap_or_else(|| "-".to_string())
            };
            let get_custom = |g: GroupId, name: &str| -> String {
                map.get(&g).and_then(|m| m.get(&TagId::Custom(std::borrow::Cow::Owned(name.to_owned())))).map(|v| v.value.to_string()).unwrap_or_else(|| "-".to_string())
            };

            let model = get_str(GroupId::Default, TagId::Name);
            let crop = get_custom(GroupId::Default, "crop_factor");
            let readout = get_str(GroupId::Imager, TagId::FrameReadoutTime);
            let readout_est = get_custom(GroupId::Imager, "readout_estimated");
            // Resolution: prefer video output resolution, fallback to CNDM sensor pixels
            let video_w = get_custom(GroupId::Default, "video_width");
            let video_h = get_custom(GroupId::Default, "video_height");
            let pixel_w = if video_w != "-" { video_w } else { get_str(GroupId::Imager, TagId::PixelWidth) };
            let pixel_h = if video_h != "-" { video_h } else { get_str(GroupId::Imager, TagId::PixelHeight) };
            let sensor_pixel_w = get_str(GroupId::Imager, TagId::PixelWidth);
            let sensor_pixel_h = get_str(GroupId::Imager, TagId::PixelHeight);
            let lens = get_str(GroupId::Lens, TagId::DisplayName);
            let focal = get_str(GroupId::Lens, TagId::FocalLength);
            let pixel_focal = get_str(GroupId::Lens, TagId::PixelFocalLength);
            let canon_fine = get_custom(GroupId::Default, "canon_fine");
            let canon_crop = get_custom(GroupId::Default, "canon_crop");
            let scale_35mm = get_custom(GroupId::Default, "scale_35mm");
            let sensor_w = get_str(GroupId::Default, TagId::Custom(std::borrow::Cow::Owned("SensorWidth".to_owned())));

            let est_mark = if readout_est == "true" { " *" } else { "" };

            println!("Model:            {}", model);
            println!("Resolution:       {} x {}", pixel_w, pixel_h);
            if sensor_pixel_w != pixel_w || sensor_pixel_h != pixel_h {
                println!("Sensor pixels:    {} x {}", sensor_pixel_w, sensor_pixel_h);
            }
            println!("Crop factor:      {}", crop);
            println!("Readout time:     {}{}", readout, est_mark);
            println!("Focal length:     {}", focal);
            let unit_pixel_fl = get_custom(GroupId::Lens, "unit_pixel_focal_length");
            println!("Pixel focal len:  {}", pixel_focal);
            println!("Unit pixel FL:    {}", unit_pixel_fl);
            println!("Lens:             {}", lens);
            println!("Sensor width:     {}", sensor_w);
            println!("Scale 35mm:       {}", scale_35mm);
            println!("Canon fine:       {}", canon_fine);
            println!("Canon crop:       {}", canon_crop);
        }
    }

    let imu_data = util::normalized_imu(&input, opts.imuo).unwrap();

    let mut csv = String::with_capacity(2*1024*1024);
    csv.push_str(r#""Product","Blackbox flight data recorder by Nicholas Sherlock""#);
    csv.push('\n');
    crate::try_block!({
        let map = samples.get(0)?.tag_map.as_ref()?;
        let json = (map.get(&GroupId::Default)?.get_t(TagId::Metadata) as Option<&serde_json::Value>)?;
        for (k, v) in json.as_object()? {
            csv.push('"');
            csv.push_str(&k.to_string());
            csv.push_str("\",");
            csv.push_str(&v.to_string());
            csv.push('\n');
        }
    });

    csv.push_str(r#""loopIteration","time","gyroADC[0]","gyroADC[1]","gyroADC[2]","accSmooth[0]","accSmooth[1]","accSmooth[2]""#);
    csv.push('\n');
    for v in imu_data {
        if v.gyro.is_some() || v.accl.is_some() {
            let gyro = v.gyro.unwrap_or_default();
            let accl = v.accl.unwrap_or_default();
            csv.push_str(&format!("{},{:.0},{},{},{},{},{},{}\n", i, (v.timestamp_ms * 1000.0).round(),
                -gyro[2], gyro[1], gyro[0],
                -accl[2] * 2048.0, accl[1] * 2048.0, accl[0] * 2048.0
            ));
            i += 1;
        }
    }
    std::fs::write(&format!("{}.csv", std::path::Path::new(&opts.input).to_path_buf().to_string_lossy()), csv).unwrap();

    println!("Done in {:.3} ms", _time.elapsed().as_micros() as f64 / 1000.0);
}
