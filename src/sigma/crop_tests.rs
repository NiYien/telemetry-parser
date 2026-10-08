// SPDX-License-Identifier: MIT OR Apache-2.0

use super::*;

#[derive(Clone)]
struct Fixture {
    little_endian: bool,
    flags: u8,
    maker_version: u16,
    flag_type: u16,
    flag_count: u32,
    model: &'static str,
    software: &'static str,
    dimensions: (u32, u32),
    x_density: (u32, u32),
    y_density: (u32, u32),
    unit: u16,
    focal: (u32, u32),
    equivalent: u16,
}

impl Default for Fixture {
    fn default() -> Self {
        Self {
            little_endian: true,
            flags: 0x40,
            maker_version: 0x0401,
            flag_type: 1,
            flag_count: 1,
            model: "SIGMA fp",
            software: "SIGMA fp Ver.5.02.0.V91",
            dimensions: (1920, 1080),
            x_density: (213805, 100),
            y_density: (213805, 100),
            unit: 2,
            focal: (0, 10),
            equivalent: 0,
        }
    }
}

fn u16_bytes(value: u16, le: bool) -> [u8; 2] {
    if le {
        value.to_le_bytes()
    } else {
        value.to_be_bytes()
    }
}
fn u32_bytes(value: u32, le: bool) -> [u8; 4] {
    if le {
        value.to_le_bytes()
    } else {
        value.to_be_bytes()
    }
}
fn rational(value: (u32, u32), le: bool) -> Vec<u8> {
    [u32_bytes(value.0, le), u32_bytes(value.1, le)].concat()
}

fn entry(
    data: &mut Vec<u8>,
    offset: usize,
    tag: u16,
    typ: u16,
    count: u32,
    value: &[u8],
    le: bool,
) {
    data[offset..offset + 2].copy_from_slice(&u16_bytes(tag, le));
    data[offset + 2..offset + 4].copy_from_slice(&u16_bytes(typ, le));
    data[offset + 4..offset + 8].copy_from_slice(&u32_bytes(count, le));
    if value.len() <= 4 {
        data[offset + 8..offset + 8 + value.len()].copy_from_slice(value);
    } else {
        let location = data.len() as u32;
        data[offset + 8..offset + 12].copy_from_slice(&u32_bytes(location, le));
        data.extend_from_slice(value);
    }
}

fn fixture(f: &Fixture) -> Vec<u8> {
    let le = f.little_endian;
    let mut data = vec![0u8; 50];
    data[..2].copy_from_slice(if le { b"II" } else { b"MM" });
    data[2..4].copy_from_slice(&u16_bytes(42, le));
    data[4..8].copy_from_slice(&u32_bytes(8, le));
    data[8..10].copy_from_slice(&u16_bytes(3, le));
    let model = format!("{}\0", f.model);
    let software = format!("{}\0", f.software);
    entry(
        &mut data,
        10,
        0x0110,
        2,
        model.len() as u32,
        model.as_bytes(),
        le,
    );
    entry(
        &mut data,
        22,
        0x0131,
        2,
        software.len() as u32,
        software.as_bytes(),
        le,
    );
    let exif_offset = data.len();
    entry(
        &mut data,
        34,
        0x8769,
        4,
        1,
        &u32_bytes(exif_offset as u32, le),
        le,
    );
    data.resize(exif_offset + 2 + 9 * 12 + 4, 0);
    data[exif_offset..exif_offset + 2].copy_from_slice(&u16_bytes(9, le));
    let mut maker = vec![0u8; 28];
    maker[..8].copy_from_slice(b"SIGMA\0\0\0");
    maker[8..10].copy_from_slice(&u16_bytes(f.maker_version, le));
    maker[10..12].copy_from_slice(&u16_bytes(1, le));
    entry(
        &mut maker,
        12,
        0x010A,
        f.flag_type,
        f.flag_count,
        &[f.flags],
        le,
    );
    let values = [
        (0x927C, 7, maker.len() as u32, maker),
        (0xA002, 4, 1, u32_bytes(f.dimensions.0, le).to_vec()),
        (0xA003, 4, 1, u32_bytes(f.dimensions.1, le).to_vec()),
        (0xA20E, 5, 1, rational(f.x_density, le)),
        (0xA20F, 5, 1, rational(f.y_density, le)),
        (0xA210, 3, 1, u16_bytes(f.unit, le).to_vec()),
        (0x920A, 5, 1, rational(f.focal, le)),
        (0xA434, 2, 1, vec![0]),
        (0xA405, 3, 1, u16_bytes(f.equivalent, le).to_vec()),
    ];
    for (i, (tag, typ, count, value)) in values.iter().enumerate() {
        entry(
            &mut data,
            exif_offset + 2 + i * 12,
            *tag,
            *typ,
            *count,
            value,
            le,
        );
    }
    data
}

fn video() -> VideoMetadata {
    VideoMetadata {
        width: 1920,
        height: 1080,
        fps: 24.0,
        duration_s: 5.0,
        rotation: 0,
    }
}

fn process(exif: &SigmaExifData, options: &InputOptions, is_dng: bool) -> GroupedTagMap {
    let mut map = GroupedTagMap::new();
    Sigma::default().process_map(&mut map, options, Some(&video()), exif, is_dng);
    map
}

fn unit(map: &GroupedTagMap) -> Option<f64> {
    map.get(&GroupId::Lens)?
        .get_t(TagId::Custom("unit_pixel_focal_length".into()))
        .copied()
}

fn metadata(map: &GroupedTagMap) -> Option<&serde_json::Value> {
    map.get(&GroupId::Default)?.get_t(TagId::Metadata)
}

fn pixel_focal(map: &GroupedTagMap) -> Option<f32> {
    map.get(&GroupId::Lens)?
        .get_t(TagId::PixelFocalLength)
        .copied()
}

fn recorded_focal(map: &GroupedTagMap) -> Option<f32> {
    map.get(&GroupId::Lens)?.get_t(TagId::FocalLength).copied()
}

fn lens_name(map: &GroupedTagMap) -> Option<&String> {
    map.get(&GroupId::Lens)?.get_t(TagId::DisplayName)
}

struct Database(std::path::PathBuf);
impl Database {
    fn new() -> Self {
        static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let path = std::env::temp_dir().join(format!(
            "telemetry-sigma-crop-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        std::fs::write(path.join("sigma.json"), r#"{"version":1,"models":{"fp":{"sw":35.9}},"crop":[{"m":["fp"],"c":1.07}],"readout":{"columns":["1K24"],"data":{"fp":[10.8]}}}"#).unwrap();
        Self(path)
    }
    fn options(&self) -> InputOptions {
        InputOptions {
            camera_db_path: Some(self.0.to_string_lossy().into_owned()),
            ..Default::default()
        }
    }
}
impl Drop for Database {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(self.0.join("sigma.json"));
        let _ = std::fs::remove_dir(&self.0);
    }
}

#[test]
fn dc_crop_reads_only_bit_six_with_either_tiff_byte_order() {
    for le in [true, false] {
        for flags in [0, 0x3F, 0x40, 0x7F, 0x80, 0xC0, 0xFF] {
            let exif = parse_tiff_ifd(&fixture(&Fixture {
                little_endian: le,
                flags,
                ..Default::default()
            }))
            .unwrap();
            assert_eq!(exif.dc_crop, Some(flags & 0x40 != 0));
        }
    }
}

#[test]
fn private_crop_rejects_unverified_layouts_and_wrong_field_shapes() {
    for f in [
        Fixture {
            model: "SIGMA fp L",
            ..Default::default()
        },
        Fixture {
            software: "SIGMA fp Ver.5.00.0",
            ..Default::default()
        },
        Fixture {
            maker_version: 0x0301,
            ..Default::default()
        },
        Fixture {
            flag_type: 7,
            ..Default::default()
        },
        Fixture {
            flag_count: 2,
            ..Default::default()
        },
    ] {
        let exif = parse_tiff_ifd(&fixture(&f)).unwrap();
        assert_eq!(exif.dc_crop, None);
        assert!(exif.focal_plane_geometry.is_none());
    }
    let mut data = fixture(&Fixture::default());
    let marker = memmem::find(&data, b"SIGMA\0\0\0").unwrap();
    data[marker + 10..marker + 12].copy_from_slice(&100u16.to_le_bytes());
    assert_eq!(parse_tiff_ifd(&data).unwrap().dc_crop, None);
    data.truncate(marker + 20);
    assert_eq!(parse_tiff_ifd(&data).unwrap().dc_crop, None);
}

#[test]
fn manual_lens_crop_and_native_geometry_do_not_require_database_or_focal_length() {
    let exif = parse_tiff_ifd(&fixture(&Fixture::default())).unwrap();
    let map = process(&exif, &InputOptions::default(), false);
    assert!((unit(&map).unwrap() - 84.1751968503937).abs() < 1e-10);
    let md = metadata(&map).unwrap();
    assert_eq!(md["dc_crop"], true);
    assert_eq!(md["view_angle"], "APSC");
    assert_eq!(md["resolution_format_name"], "APSC");
    assert!(pixel_focal(&map).is_none());
    assert!(recorded_focal(&map).is_none());
    assert!(lens_name(&map).is_none());
}

#[test]
fn user_24mm_uses_native_projection_without_claiming_recorded_focal_length() {
    let exif = parse_tiff_ifd(&fixture(&Fixture::default())).unwrap();
    let opts = InputOptions {
        user_focal_length: Some(24.0),
        ..Default::default()
    };
    let map = process(&exif, &opts, false);
    assert!((pixel_focal(&map).unwrap() - 2020.2047).abs() < 0.001);
    assert!(recorded_focal(&map).is_none());
}

#[test]
fn native_geometry_overrides_database_crop_and_equivalent_focal_ratio() {
    let db = Database::new();
    let exif = parse_tiff_ifd(&fixture(&Fixture {
        focal: (24, 1),
        equivalent: 48,
        ..Default::default()
    }))
    .unwrap();
    let map = process(&exif, &db.options(), false);
    assert!((unit(&map).unwrap() - 84.1751968503937).abs() < 1e-10);
    assert!((pixel_focal(&map).unwrap() - 2020.2047).abs() < 0.001);
    assert_eq!(recorded_focal(&map), Some(24.0));
    let crop = metadata(&map).unwrap()["crop_factor"].as_f64().unwrap();
    assert!((crop / 35.9 * 1920.0 - unit(&map).unwrap()).abs() < 1e-4);
}

#[test]
fn full_frame_uses_its_own_density_without_borrowing_apsc_scale() {
    let exif = parse_tiff_ifd(&fixture(&Fixture {
        flags: 0,
        x_density: (136835, 100),
        y_density: (136835, 100),
        ..Default::default()
    }))
    .unwrap();
    let map = process(&exif, &Database::new().options(), false);
    assert_eq!(metadata(&map).unwrap()["dc_crop"], false);
    assert_eq!(metadata(&map).unwrap()["resolution_format_name"], "FULL");
    assert!((unit(&map).unwrap() - 1368.35 / 25.4).abs() < 1e-10);
}

#[test]
fn invalid_or_resized_geometry_keeps_crop_state_without_full_frame_projection() {
    let db = Database::new();
    for f in [
        Fixture {
            x_density: (0, 100),
            ..Default::default()
        },
        Fixture {
            y_density: (213805, 0),
            ..Default::default()
        },
        Fixture {
            y_density: (136835, 100),
            ..Default::default()
        },
        Fixture {
            unit: 1,
            ..Default::default()
        },
        Fixture {
            dimensions: (3840, 2160),
            ..Default::default()
        },
    ] {
        let exif = parse_tiff_ifd(&fixture(&f)).unwrap();
        let map = process(&exif, &db.options(), false);
        assert_eq!(metadata(&map).unwrap()["dc_crop"], true);
        assert!(unit(&map).is_none());
        assert!(pixel_focal(&map).is_none());
    }
}

#[test]
fn standard_focal_plane_centimeter_units_are_converted_to_millimeters() {
    let exif = parse_tiff_ifd(&fixture(&Fixture {
        x_density: (84175197, 100000),
        y_density: (84175197, 100000),
        unit: 3,
        ..Default::default()
    }))
    .unwrap();
    let map = process(&exif, &InputOptions::default(), false);
    assert!((unit(&map).unwrap() - 84.175197).abs() < 1e-9);
}

#[test]
fn missing_crop_signal_and_dng_keep_previous_database_geometry() {
    let db = Database::new();
    let mut exif = parse_tiff_ifd(&fixture(&Fixture {
        flag_count: 2,
        ..Default::default()
    }))
    .unwrap();
    let map = process(&exif, &db.options(), false);
    assert!((unit(&map).unwrap() - 1920.0 * 1.07 / 35.9).abs() < 1e-4);
    assert!(metadata(&map).is_none());
    exif = parse_tiff_ifd(&fixture(&Fixture::default())).unwrap();
    exif.default_crop_size = Some((1920, 1080));
    let map = process(&exif, &db.options(), true);
    assert!((unit(&map).unwrap() - 1920.0 * 1.07 / 35.9).abs() < 1e-4);
    assert!(metadata(&map).is_none());
}

#[test]
#[ignore = "Requires the two original SIGMA fp MOV clips in GYROFLOW_SIGMA_MOV_DIR"]
fn original_manual_lens_mov_clips_have_apsc_and_correct_24mm_projection() {
    let dir = std::env::var("GYROFLOW_SIGMA_MOV_DIR").expect("GYROFLOW_SIGMA_MOV_DIR");
    let db = Database::new();
    for name in ["A001_003_20261007.MOV", "A001_004_20261007.MOV"] {
        let path = std::path::Path::new(&dir).join(name);
        for with_db in [false, true] {
            for focal in [None, Some(24.0)] {
                let file = std::fs::File::open(&path).unwrap();
                let size = file.metadata().unwrap().len() as usize;
                let mut stream = BufReader::new(file);
                let mut options = if with_db {
                    db.options()
                } else {
                    InputOptions::default()
                };
                options.user_focal_length = focal;
                let input = Input::from_stream_with_options(
                    &mut stream,
                    size,
                    &path,
                    |_| {},
                    Arc::new(AtomicBool::new(false)),
                    options,
                )
                .unwrap();
                assert_eq!(input.camera_type(), "Sigma");
                assert_eq!(input.camera_model().map(String::as_str), Some("fp"));
                let map = input.samples.as_ref().unwrap()[0].tag_map.as_ref().unwrap();
                assert_eq!(metadata(map).unwrap()["dc_crop"], true);
                assert!((unit(map).unwrap() - 84.1751968503937).abs() < 1e-10);
                assert!(recorded_focal(map).is_none());
                if focal.is_some() {
                    assert!((pixel_focal(map).unwrap() - 2020.2047).abs() < 0.001);
                } else {
                    assert!(pixel_focal(map).is_none());
                }
            }
        }
    }
}
