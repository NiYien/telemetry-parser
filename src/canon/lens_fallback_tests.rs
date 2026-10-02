// SPDX-License-Identifier: MIT OR Apache-2.0

use super::*;

fn parse_maps(model: &str, exif: exif::CanonExifData, maps: Vec<GroupedTagMap>, width: usize, fps: f64) -> Vec<SampleInfo> {
    let options = InputOptions {
        camera_db_path: Some(format!("{}/camera_db", env!("CARGO_MANIFEST_DIR"))),
        ..Default::default()
    };
    let mut canon = Canon { model: Some(model.into()), ..Default::default() };
    let mut samples: Vec<_> = maps.into_iter().enumerate().map(|(i, map)| SampleInfo {
        timestamp_ms: i as f64 * 1000.0 / fps,
        tag_map: Some(map),
        ..Default::default()
    }).collect();
    let video = VideoMetadata { width, height: 2160, fps, ..Default::default() };
    canon.process_map(&mut samples, &options, Some(exif), Some(&video), None, None, None);
    samples
}

fn focal_map(focal: f32) -> GroupedTagMap {
    let mut map = GroupedTagMap::new();
    util::insert_tag(&mut map, tag!(parsed GroupId::Lens, TagId::FocalLength, "Focal length", f32, |v| v.to_string(), focal, Vec::new()), &InputOptions::default());
    map
}

fn movie_full_frame() -> exif::CanonExifData {
    exif::CanonExifData { movie_crop: Some(false), lens_model: Some("RF28-70mm F2.8 IS STM".into()), ..Default::default() }
}

fn scalar(samples: &[SampleInfo], index: usize, group: GroupId, id: TagId) -> f64 {
    let tags = &samples[index].tag_map.as_ref().unwrap()[&group];
    if let Some(v) = tags.get_t(id.clone()) as Option<&f64> { *v }
    else { *(tags.get_t(id) as Option<&f32>).unwrap() as f64 }
}

#[test]
fn r63_missing_internal_geometry_uses_database_for_every_focal_sample() {
    let samples = parse_maps("R6 Mark III", movie_full_frame(), vec![focal_map(28.0), focal_map(50.0)], 3840, 120000.0 / 1001.0);
    for (i, focal) in [28.0, 50.0].into_iter().enumerate() {
        assert_eq!(scalar(&samples, i, GroupId::Default, TagId::Custom("crop_factor".into())), 1.07);
        assert!((scalar(&samples, i, GroupId::Lens, TagId::PixelFocalLength) - focal * 3840.0 * 1.07 / 35.9).abs() < 0.001);
    }
}

#[test]
fn full_frame_fallback_does_not_override_existing_modes_or_guess_unknown_crop() {
    for (model, width, fps, exif, expected) in [
        ("R6 Mark III", 4096, 120.0, movie_full_frame(), 1.0),
        ("R6 Mark III", 1920, 180.0, movie_full_frame(), 1.13),
        ("R6 Mark III", 3840, 120.0, exif::CanonExifData { aspect_ratio: Some(13), ..movie_full_frame() }, 1.6),
        ("R6 Mark III", 3840, 120.0, exif::CanonExifData { aspect_ratio: Some(12), ..movie_full_frame() }, 1.3),
        ("R6 Mark III", 3840, 120.0, exif::CanonExifData { movie_crop: Some(true), ..movie_full_frame() }, 1.6),
        ("R6 Mark III", 3840, 120.0, exif::CanonExifData { movie_crop: None, ..movie_full_frame() }, 1.0),
        ("R6 Mark III", 3840, 120.0, exif::CanonExifData { aspect_ratio: Some(99), ..movie_full_frame() }, 1.0),
        ("R6 Mark III", 3840, 120.0, exif::CanonExifData { lens_model: Some("RF-S18-45mm F4.5-6.3 IS STM".into()), ..movie_full_frame() }, 1.6),
        ("R7", 3840, 25.0, movie_full_frame(), 1.0),
        ("R6 Mark II", 3840, 50.0, movie_full_frame(), 1.0),
        ("5D Mark IV", 4096, 25.0, movie_full_frame(), 1.75),
    ] {
        let samples = parse_maps(model, exif, vec![focal_map(28.0)], width, fps);
        assert_eq!(scalar(&samples, 0, GroupId::Default, TagId::Custom("crop_factor".into())), expected, "{model} {width} {fps}");
    }
}

#[test]
fn native_capture_geometry_is_not_multiplied_by_database_crop() {
    let mut map = focal_map(28.0);
    let options = InputOptions::default();
    util::insert_tag(&mut map, tag!(parsed GroupId::Default, TagId::SensorWidth, "Effective width", f32, |v| v.to_string(), 35.913, Vec::new()), &options);
    util::insert_tag(&mut map, tag!(parsed GroupId::Default, TagId::SensorHeight, "Effective height", f32, |v| v.to_string(), 18.947, Vec::new()), &options);
    util::insert_tag(&mut map, tag!(parsed GroupId::Imager, TagId::PixelWidth, "Sensor width", u32, |v| v.to_string(), 6960, Vec::new()), &options);
    util::insert_tag(&mut map, tag!(parsed GroupId::Imager, TagId::PixelHeight, "Sensor height", u32, |v| v.to_string(), 3672, Vec::new()), &options);
    let samples = parse_maps("R6 Mark III", movie_full_frame(), vec![map.clone(), map], 3840, 50.0);
    for i in 0..2 {
        assert!((scalar(&samples, i, GroupId::Lens, TagId::Custom("unit_pixel_focal_length".into())) - 3840.0 / 35.913).abs() < 0.0001);
        assert!(!samples[i].tag_map.as_ref().unwrap()[&GroupId::Lens].contains_key(&TagId::PixelFocalLength));
    }
}

#[test]
fn native_pixel_focal_length_survives_when_sensor_dimensions_are_missing() {
    let mut maps = Vec::new();
    for (focal, pair) in [(28.0, vec![3300.0, 3310.0]), (50.0, vec![5700.0, 5710.0])] {
        let mut map = focal_map(focal);
        util::insert_tag(&mut map, tag!(parsed GroupId::Lens, TagId::PixelFocalLength, "Native pixel focal length", Vec_f32, |v| format!("{v:?}"), pair, Vec::new()), &InputOptions::default());
        maps.push(map);
    }
    let samples = parse_maps("R6 Mark III", movie_full_frame(), maps, 3840, 120.0);
    for (i, expected) in [vec![3300.0, 3310.0], vec![5700.0, 5710.0]].iter().enumerate() {
        let pair = samples[i].tag_map.as_ref().unwrap()[&GroupId::Lens].get_t(TagId::PixelFocalLength) as Option<&Vec<f32>>;
        assert_eq!(pair, Some(expected));
        assert_eq!(scalar(&samples, i, GroupId::Default, TagId::Custom("crop_factor".into())), 1.0);
    }
}
