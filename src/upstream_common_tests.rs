// SPDX-License-Identifier: MIT OR Apache-2.0

use super::*;
use crate::tag;

#[test]
fn invalid_box_sizes_stop_and_truncated_payloads_do_not_preallocate_the_declared_size() {
    for bytes in [
        [4u32.to_be_bytes().as_slice(), b"moov"].concat(),
        [1u32.to_be_bytes().as_slice(), b"moov", &u64::MAX.to_be_bytes()].concat(),
    ] {
        assert_eq!(read_box(&mut Cursor::new(bytes)).unwrap_err().kind(), ErrorKind::InvalidData);
    }
    let mut short = Cursor::new(vec![1, 2, 3]);
    assert_eq!(read_data(&mut short, u64::MAX).unwrap_err().kind(), ErrorKind::UnexpectedEof);
    assert_eq!(find_box_data(&[4u32.to_be_bytes().as_slice(), b"moov"].concat(), b"moov"), None);
}

#[test]
fn a_zero_sized_final_box_ends_at_the_stream_end() {
    let bytes = [0u32.to_be_bytes().as_slice(), b"mdat", &[1, 2, 3]].concat();
    assert_eq!(get_mp4_good_size(&mut Cursor::new(&bytes), bytes.len() as u64).unwrap(), bytes.len());
}

#[test]
fn track_rate_uses_sample_durations_without_bframe_composition_delay() {
    let mut samples = mp4parse::TryVec::new();
    samples.push(mp4parse::Sample { sample_count: 900, sample_delta: 1001 }).unwrap();
    let track = mp4parse::Track {
        timescale: Some(mp4parse::TrackTimeScale(60000, 0)),
        duration: Some(mp4parse::TrackScaledTime(902 * 1001, 0)),
        stts: Some(mp4parse::TimeToSampleBox { samples }),
        ..Default::default()
    };
    assert!((get_fps_from_track(&track).unwrap() - 60000.0 / 1001.0).abs() < 1e-10);
    assert_eq!(track_duration(&track), Some(900 * 1001));
}

fn packet(index: u64, count: usize) -> SampleInfo {
    let mut map = GroupedTagMap::new();
    let options = crate::InputOptions::default();
    insert_tag(&mut map, tag!(parsed GroupId::Gyroscope, TagId::Data, "Gyro", Vec_Vector3_i16, |v| format!("{v:?}"), vec![Vector3 { x: 1, y: 2, z: 3 }; count], vec![]), &options);
    insert_tag(&mut map, tag!(parsed GroupId::Gyroscope, TagId::TimestampUs, "STMP", u64, |v| v.to_string(), index * 1_000_000 + 100, vec![]), &options);
    SampleInfo { sample_index: index, timestamp_ms: index as f64 * 1000.0, duration_ms: 1000.0, tag_map: Some(map), ..Default::default() }
}

#[test]
fn missing_gopro_packets_leave_a_gap_in_both_normalized_timelines() {
    let input = crate::Input {
        inner: crate::SupportedFormats::GoPro(crate::gopro::GoPro::default()),
        samples: Some(vec![packet(0, 200), packet(1, 200), packet(3, 200)]),
    };
    for data in [normalized_imu(&input, None).unwrap(), normalized_imu_interpolated(&input, None).unwrap()] {
        assert_eq!(data.len(), 600);
        assert_eq!(data[399].timestamp_ms, 1995.0);
        assert_eq!(data[400].timestamp_ms, 3000.0);
        assert_eq!(data[599].timestamp_ms, 3995.0);
    }
}

#[test]
fn gopro_empty_packets_do_not_change_the_sensor_reading_rate() {
    let packets = vec![packet(0, 200), packet(1, 200), packet(2, 0), packet(3, 200)];
    assert_eq!(crate::gopro::GoPro::get_avg_sample_duration(&packets, &GroupId::Gyroscope), Some(5.0));
}

#[test]
fn mixed_path_separators_use_the_last_separator() {
    assert_eq!(crate::filesystem::get_filename("C:/clips\\take.mp4"), "take.mp4");
    assert_eq!(crate::filesystem::get_folder("C:/clips\\take.mp4"), "C:/clips");
}

fn metadata_file_with_empty_middle_sample() -> Vec<u8> {
    let boxed = |name: &[u8], body: Vec<u8>| [&(body.len() as u32 + 8).to_be_bytes()[..], name, &body].concat();
    let table = |name: &[u8], values: &[u32]| boxed(name, [vec![0; 4], values.iter().flat_map(|x| x.to_be_bytes()).collect()].concat());
    let mut mvhd = vec![0; 100];
    mvhd[12..16].copy_from_slice(&1000u32.to_be_bytes());
    mvhd[16..20].copy_from_slice(&300u32.to_be_bytes());
    let mut tkhd = vec![0; 84];
    tkhd[12..16].copy_from_slice(&1u32.to_be_bytes());
    tkhd[20..24].copy_from_slice(&300u32.to_be_bytes());
    let mut mdhd = vec![0; 24];
    mdhd[12..16].copy_from_slice(&1000u32.to_be_bytes());
    mdhd[16..20].copy_from_slice(&300u32.to_be_bytes());
    let mut hdlr = vec![0; 25];
    hdlr[8..12].copy_from_slice(b"meta");
    let moov = |offset| boxed(b"moov", [
        boxed(b"mvhd", mvhd.clone()),
        boxed(b"trak", [boxed(b"tkhd", tkhd.clone()), boxed(b"mdia", [
            boxed(b"mdhd", mdhd.clone()), boxed(b"hdlr", hdlr.clone()),
            boxed(b"minf", boxed(b"stbl", [
                table(b"stsd", &[0]), table(b"stts", &[1, 3, 100]),
                table(b"stsc", &[1, 1, 3, 1]), table(b"stsz", &[0, 3, 8, 0, 8]), table(b"stco", &[1, offset]),
            ].concat())),
        ].concat())].concat()),
    ].concat());
    let header = moov(0);
    [moov(header.len() as u32 + 8), boxed(b"mdat", vec![7; 16])].concat()
}

#[test]
fn empty_metadata_samples_keep_the_following_sample_index_and_full_parse_rewinds() {
    let bytes = metadata_file_with_empty_middle_sample();
    let mut stream = Cursor::new(&bytes);
    stream.set_position(9);
    let mut indices = Vec::new();
    get_metadata_track_samples(&mut stream, bytes.len(), true, |info, _, _, _| indices.push(info.sample_index), Arc::new(AtomicBool::new(false))).unwrap();
    assert_eq!(indices, vec![0, 2]);
}

fn mdhd_hdlr(version: u8, timescale: u32, name: &str) -> Vec<u8> {
        let dates = if version == 1 { 16 } else { 8 };
        let mut mdhd = vec![version, 0, 0, 0];
        mdhd.extend(std::iter::repeat_n(0x11, dates));
        mdhd.extend(timescale.to_be_bytes());
        mdhd.extend(std::iter::repeat_n(0x22, dates / 2)); // The duration
        mdhd.extend([0x55, 0xC4, 0, 0]); // The language and pre_defined
        let mut hdlr = vec![0; 8]; // The version, flags and pre_defined
        hdlr.extend(b"meta");
        hdlr.extend([0; 12]);
        hdlr.push(name.len() as u8);
        hdlr.extend(name.as_bytes());
        let boxed = |typ: &[u8], body: Vec<u8>| [&(body.len() as u32 + 8).to_be_bytes()[..], typ, &body].concat();
        [boxed(b"mdhd", mdhd), boxed(b"hdlr", hdlr)].concat()
    }

#[test]
fn a_zero_mdhd_timescale_is_patched_in_either_version() {
        let prefix = &b"bytes that read mdhd but aren't a box"[..];
        for version in [0, 1] {
            let mut data = [prefix, &mdhd_hdlr(version, 0, "GoPro MET")].concat();
            patch_mdhd_timescale(&mut data);
            assert_eq!(data, [prefix, &mdhd_hdlr(version, 1000, "GoPro MET")].concat());
        }
        let mut other = mdhd_hdlr(1, 0, "GoPro TCD");
        patch_mdhd_timescale(&mut other);
        assert_eq!(other, mdhd_hdlr(1, 0, "GoPro TCD"));
    }
