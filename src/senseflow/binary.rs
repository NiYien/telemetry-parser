// SPDX-License-Identifier: MIT OR Apache-2.0

use crate::tags_impl::*;
use crate::*;
use byteorder::{BigEndian, LittleEndian, ReadBytesExt};
use std::io::*;

const HEADER_BYTES: u64 = 512;
const FRAME_BYTES: u64 = 16;
const PHYSICAL_SLOTS_PER_IMU_MS: f64 = 1001.0 / 1000.0;

fn imu_time_ms_to_file_pos(timestamp_ms: f64) -> u64 {
    let slot = (timestamp_ms.max(0.0) * PHYSICAL_SLOTS_PER_IMU_MS + 1e-6).floor() as u64;
    HEADER_BYTES + slot * FRAME_BYTES
}

pub fn parse<T: Read + Seek>(stream: &mut T, size: usize, options: crate::InputOptions) -> Result<Vec<SampleInfo>> {
    let mut stream = std::io::BufReader::new(stream);

    let mut last_timestamp;

    let mut buf = vec![0u8; 512];
    stream.read_exact(&mut buf[0..512])?;
    let mut d = std::io::Cursor::new(&buf);

    let brand = unsafe { std::str::from_utf8_unchecked(&buf[0..12]) };
    let version = unsafe { std::str::from_utf8_unchecked(&buf[12..16]) };
    let _product_id = &buf[16..36];
    let _product_sn = &buf[36..52];

    let imu_orientation = std::str::from_utf8(&buf[60..64]).unwrap_or("XYZ");

    let yy = (buf[64] as i32) + 2000;
    let mm = buf[65] as u32;
    let dd = buf[66] as u32;
    let h = buf[67] as u32;
    let m = buf[68] as u32;
    let s = buf[69] as u32;
    let ms = 0u32;
    let created_at = chrono::NaiveDate::from_ymd_opt(yy, mm, dd)
        .and_then(|x| x.and_hms_milli_opt(h, m, s, ms))
        .unwrap_or_default();

    let first_timestamp = 0f64;

    d.seek(SeekFrom::Start(76))?;
    let init_quat = TimeQuaternion {
        t: (first_timestamp * 1000.0) as f64,
        v: Quaternion {
            w: d.read_f32::<LittleEndian>()? as f64,
            x: d.read_f32::<LittleEndian>()? as f64,
            y: d.read_f32::<LittleEndian>()? as f64,
            z: d.read_f32::<LittleEndian>()? as f64,
        },
    };
    let log_freq = d.read_u32::<LittleEndian>()?;

    d.seek(SeekFrom::Start(144))?;
    let accl_odr = d.read_u16::<LittleEndian>()?;
    let accl_max_bw = d.read_u16::<LittleEndian>()?;
    let accl_timeoffset = d.read_i32::<LittleEndian>()?;
    let accl_range = d.read_u32::<LittleEndian>()? as f64;

    let gyro_odr = d.read_u16::<LittleEndian>()?;
    let gyro_max_bw = d.read_u16::<LittleEndian>()?;
    let gyro_timeoffset = d.read_i32::<LittleEndian>()?;
    let gyro_range = d.read_u32::<LittleEndian>()? as f64;

    let magn_odr = d.read_u16::<LittleEndian>()?;
    let magn_max_bw = d.read_u16::<LittleEndian>()?;
    let magn_timeoffset = d.read_i32::<LittleEndian>()?;
    let magn_range = (d.read_u32::<LittleEndian>()? as f64) / 1000.0;

    let timestamp_step = 1.0f64 / (log_freq as f64);

    // Read timezone offset from header (i16 minutes at buf[70..72])
    let tz_offset_min = i16::from_le_bytes([buf[70], buf[71]]);
    let tz_sign = if tz_offset_min >= 0 { "+" } else { "-" };
    let tz_abs = tz_offset_min.unsigned_abs();
    let tz_str = format!("{}{:02}:{:02}", tz_sign, tz_abs / 60, tz_abs % 60);
    let date_str = created_at.format("%Y:%m:%d %H:%M:%S").to_string();

    // ── header_only mode: return metadata without parsing any IMU frames ──
    if options.header_only {
        let duration_ms = if size > HEADER_BYTES as usize {
            (size - HEADER_BYTES as usize) as f64 / FRAME_BYTES as f64 / PHYSICAL_SLOTS_PER_IMU_MS
        } else {
            0.0
        };
        let metadata = serde_json::json!({
            "brand": brand,
            "version": version,
            "created_at": created_at.to_string(),
            "log_freq": log_freq,
            "accl_odr": accl_odr,
            "accl_max_bandwidth": accl_max_bw,
            "accl_timeoffset": accl_timeoffset,
            "accl_range": accl_range,
            "gyro_odr": gyro_odr,
            "gyro_max_bandwidth": gyro_max_bw,
            "gyro_timeoffset": gyro_timeoffset,
            "gyro_range": gyro_range,
            "magn_odr": magn_odr,
            "magn_max_bandwidth": magn_max_bw,
            "magn_timeoffset": magn_timeoffset,
            "magn_range": magn_range,
            "timestamp_step": timestamp_step,
            "init quart": init_quat,
            "lens_index": serde_json::Value::Null,
            "focus_length": serde_json::Value::Null,
        });

        let mut map = GroupedTagMap::new();
        util::insert_tag(&mut map, tag!(parsed GroupId::Default, TagId::Metadata, "Metadata", Json, |v| serde_json::to_string(v).unwrap(), metadata, vec![]), &options);
        util::write_creation_date_tags(&mut map, &date_str, Some(&tz_str), None, &options);

        return Ok(vec![SampleInfo {
            timestamp_ms: first_timestamp as f64,
            duration_ms,
            tag_map: Some(map),
            ..Default::default()
        }]);
    }

    // ── Determine seek range for time_range_ms mode ──
    let (seek_start, seek_timestamp_ms, imu_start_ms, end_pos) = if let Some((start_ms, end_ms)) = options.time_range_ms {
        // Seek 1 second earlier to scan for lens frames
        let lens_scan_start_ms = (start_ms - 1000.0).max(0.0);
        let seek_pos = imu_time_ms_to_file_pos(lens_scan_start_ms);
        let end = imu_time_ms_to_file_pos(end_ms);
        (Some(seek_pos), Some(lens_scan_start_ms), Some(start_ms), Some(end))
    } else {
        (None, None, None, None)
    };

    if let Some(pos) = seek_start {
        stream.seek(SeekFrom::Start(pos))?;
        last_timestamp = seek_timestamp_ms.unwrap_or_default() / 1000.0 - timestamp_step;
    } else {
        last_timestamp = first_timestamp - timestamp_step;
    }

    let mut gyro = Vec::new();
    let mut accl = Vec::new();
    let mut angl = Vec::new();
    let mut magn = Vec::new();
    let mut quat = Vec::new();

    let mut first_lens: Option<(u8, u16)> = None;

    // acc gyro mag quad angle temp -- --
    let sensor_length = [6, 6, 6, 8, 12, 2, 0, 0];
    let mut sensor_valid = [0u8; 8];

    while let Ok(tag) = stream.read_u16::<BigEndian>() {
        // Check end position for time_range mode
        if let Some(ep) = end_pos {
            let pos = stream.stream_position().unwrap_or(0);
            if pos > ep { break; }
        }

        if tag == 0xaa55 {
            let mut data_valid = stream.read_u8()?;
            let mut data_length = 0;
            for n in 0..8 {
                sensor_valid[n] = data_valid & 0b00000001;
                if sensor_valid[n] == 1 {
                    data_length += sensor_length[n];
                }
                data_valid >>= 1;
            }

            if let Ok(mut d) = checksum(&mut stream, data_length) {
                last_timestamp += timestamp_step;

                // In time_range mode, only collect IMU data from imu_start_ms onwards
                let collect_imu = match imu_start_ms {
                    Some(start) => (last_timestamp * 1000.0) >= start,
                    None => true,
                };

                if collect_imu {
                    if sensor_valid[0] == 1 {
                        accl.push(TimeVector3 {
                            t: (last_timestamp as f64) + (accl_timeoffset as f64) / 1000.0,
                            x: ((d.read_i16::<LittleEndian>()? as f64) / 32768.0) * accl_range,
                            y: ((d.read_i16::<LittleEndian>()? as f64) / 32768.0) * accl_range,
                            z: ((d.read_i16::<LittleEndian>()? as f64) / 32768.0) * accl_range,
                        });
                    }

                    if sensor_valid[1] == 1 {
                        gyro.push(TimeVector3 {
                            t: (last_timestamp as f64) + (gyro_timeoffset as f64) / 1000.0,
                            x: ((d.read_i16::<LittleEndian>()? as f64) / 32768.0) * gyro_range,
                            y: ((d.read_i16::<LittleEndian>()? as f64) / 32768.0) * gyro_range,
                            z: ((d.read_i16::<LittleEndian>()? as f64) / 32768.0) * gyro_range,
                        });
                    }

                    if sensor_valid[2] == 1 {
                        magn.push(TimeVector3 {
                            t: (last_timestamp as f64) + (magn_timeoffset as f64) / 1000.0,
                            x: d.read_i16::<LittleEndian>()? as i64,
                            y: d.read_i16::<LittleEndian>()? as i64,
                            z: d.read_i16::<LittleEndian>()? as i64,
                        });
                    }

                    if sensor_valid[3] == 1 {
                        quat.push(TimeQuaternion {
                            t: (last_timestamp * 1000.0) as f64,
                            v: util::multiply_quats(
                                (
                                    (d.read_i16::<LittleEndian>()? as f64) / 32768.0,
                                    (d.read_i16::<LittleEndian>()? as f64) / 32768.0,
                                    (d.read_i16::<LittleEndian>()? as f64) / 32768.0,
                                    (d.read_i16::<LittleEndian>()? as f64) / 32768.0,
                                ),
                                ((2.0_f64).sqrt() * 0.5, 0.0, 0.0, -(2.0_f64).sqrt() * 0.5),
                            ),
                        });
                    }

                    if sensor_valid[4] == 1 {
                        angl.push(TimeVector3 {
                            t: last_timestamp as f64,
                            x: ((d.read_i16::<LittleEndian>()? as f64) / 32768.0) * 180.0, // Roll
                            y: ((d.read_i16::<LittleEndian>()? as f64) / 32768.0) * 180.0, // Pitch
                            z: ((d.read_i16::<LittleEndian>()? as f64) / 32768.0) * 180.0, // Yaw
                        });
                    }
                } else {
                    // Before imu_start: still advance cursor past sensor data (already consumed by checksum)
                    // but don't store. Also skip reading individual fields since checksum already consumed the data.
                }
            }
        } else if tag == 0x55aa {
            // Lens data frame (reversed magic, 1Hz in mix files)
            // Total frame = 16 bytes: 2 (magic) + 1 (data_valid) + 11 (payload) + 1 (checksum) + 1 (padding)
            // Read remaining 14 bytes of the 16-byte frame
            let mut lens_buf = [0u8; 14];
            if stream.read_exact(&mut lens_buf).is_ok() {
                if lens_buf[0] == 0b01000000 && first_lens.is_none() {
                    let lens_index = lens_buf[10];          // byte[12] of frame
                    let foc_hi = lens_buf[11] as u16;       // byte[13] of frame
                    let foc_lo = lens_buf[12] as u16;       // byte[14] of frame
                    let focus_length = (foc_hi << 8) | foc_lo;
                    first_lens = Some((lens_index, focus_length));
                }
                // Lens frame does not occupy an IMU time slot — do not advance timestamp
            }
        }
    }

    let metadata = serde_json::json!({
       "brand": brand,
       "version": version,
       "created_at": created_at.to_string(),
       "log_freq": log_freq,
       "accl_odr": accl_odr,
       "accl_max_bandwidth": accl_max_bw,
       "accl_timeoffset": accl_timeoffset,
       "accl_range": accl_range,
       "gyro_odr": gyro_odr,
       "gyro_max_bandwidth": gyro_max_bw,
       "gyro_timeoffset": gyro_timeoffset,
       "gyro_range": gyro_range,
       "magn_odr": magn_odr,
       "magn_max_bandwidth": magn_max_bw,
       "magn_timeoffset": magn_timeoffset,
       "magn_range": magn_range,
       "timestamp_step": timestamp_step,
       "init quart": init_quat,
       "lens_index": first_lens.map(|(idx, _)| idx),
       "focus_length": first_lens.map(|(_, fl)| fl),
    });

    let mut map = GroupedTagMap::new();

    util::insert_tag(&mut map, tag!(parsed GroupId::Accelerometer, TagId::Data, "Accelerometer data", Vec_TimeVector3_f64, |v| format!("{:?}",  v), accl, vec![]), &options);
    util::insert_tag(&mut map, tag!(parsed GroupId::Gyroscope,     TagId::Data, "Gyroscope data",     Vec_TimeVector3_f64, |v| format!("{:?}", v), gyro, vec![]), &options);

    util::insert_tag(&mut map, tag!(parsed GroupId::Accelerometer, TagId::Unit, "Accelerometer unit", String, |v| v.to_string(), "g".into(), Vec::new()), &options);
    util::insert_tag(&mut map, tag!(parsed GroupId::Gyroscope,     TagId::Unit, "Gyroscope unit",     String, |v| v.to_string(), "deg/s".into(), Vec::new()), &options);

    util::insert_tag(&mut map, tag!(parsed GroupId::Gyroscope,     TagId::Orientation, "IMU orientation", String, |v| v.to_string(), imu_orientation.into(), Vec::new()), &options);
    util::insert_tag(&mut map, tag!(parsed GroupId::Accelerometer, TagId::Orientation, "IMU orientation", String, |v| v.to_string(), imu_orientation.into(), Vec::new()), &options);

    util::insert_tag(&mut map, tag!(parsed GroupId::Magnetometer,  TagId::Data, "Magnetometer data", Vec_TimeVector3_i64f64, |v| format!("{:?}", v), magn, vec![]), &options);
    util::insert_tag(&mut map, tag!(parsed GroupId::Magnetometer,  TagId::Unit, "Magnetometer unit", String, |v| v.to_string(), "μT".into(), Vec::new()), &options);

    util::insert_tag(&mut map, tag!(parsed GroupId::Custom("Angle".into()),        TagId::Data, "Angle data", Vec_TimeVector3_f64, |v| format!("{:?}", v), angl, vec![]), &options);
    util::insert_tag(&mut map, tag!(parsed GroupId::Custom("Angle".into()),        TagId::Unit, "Angle unit", String, |v| v.to_string(), "deg".into(),  Vec::new()), &options);

    util::insert_tag(&mut map, tag!(parsed GroupId::Quaternion,   TagId::Data, "Quaternion data",   Vec_TimeQuaternion_f64,  |v| format!("{:?}", v), quat, vec![]), &options);
    util::insert_tag(&mut map, tag!(parsed GroupId::Default, TagId::Metadata, "Metadata", Json, |v| serde_json::to_string(v).unwrap(), metadata, vec![]), &options);
    util::write_creation_date_tags(&mut map, &date_str, Some(&tz_str), None, &options);

    Ok(vec![SampleInfo {
        timestamp_ms: first_timestamp as f64,
        duration_ms: (last_timestamp - first_timestamp) as f64,
        tag_map: Some(map),
        ..Default::default()
    }])
}

fn checksum<T: Read + Seek>(stream: &mut T, item_size: u64) -> Result<Cursor<Vec<u8>>> {
    let mut buf = vec![0u8; item_size as usize];
    stream.read_exact(&mut buf)?;
    let sum = stream.read_u8()?;
    let init: u8 = 0;
    let calculated_sum = buf.iter().fold(init, |sum, &x| sum.wrapping_add(x));

    if calculated_sum == sum {
        Ok(Cursor::new(buf))
    } else {
        log::error!("Invalid checksum! {} != {} | {}", calculated_sum, sum, crate::util::to_hex(&buf));
        Err(Error::from(ErrorKind::InvalidData))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tags_impl::{GetWithType, GroupId, TagId, TimeVector3};
    use std::io::Cursor;

    fn put_u16(buf: &mut [u8], offset: usize, value: u16) {
        buf[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
    }

    fn put_i32(buf: &mut [u8], offset: usize, value: i32) {
        buf[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
    }

    fn put_u32(buf: &mut [u8], offset: usize, value: u32) {
        buf[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
    }

    fn imu_frame(slot: usize) -> [u8; 16] {
        let mut frame = [0u8; 16];
        frame[0] = 0xaa;
        frame[1] = 0x55;
        frame[2] = 0b0000_0011;
        frame[9..11].copy_from_slice(&(slot as i16).to_le_bytes());
        frame[15] = frame[3..15].iter().fold(0u8, |sum, value| sum.wrapping_add(*value));
        frame
    }

    fn lens_frame() -> [u8; 16] {
        let mut frame = [0u8; 16];
        frame[0] = 0x55;
        frame[1] = 0xaa;
        frame[2] = 0b0100_0000;
        frame[12] = 2;
        frame[13] = 0x01;
        frame[14] = 0x2c;
        frame
    }

    fn senseflow_mix_file(slot_count: usize) -> Vec<u8> {
        let mut data = vec![0u8; 512];
        data[0..9].copy_from_slice(b"SenseFlow");
        data[12..16].copy_from_slice(b"0001");
        data[60..64].copy_from_slice(b"XYZ ");
        data[64] = 26;
        data[65] = 1;
        data[66] = 1;
        data[76..80].copy_from_slice(&1.0f32.to_le_bytes());
        put_u32(&mut data, 92, 1000);

        put_u16(&mut data, 144, 1000);
        put_u16(&mut data, 146, 0);
        put_i32(&mut data, 148, 0);
        put_u32(&mut data, 152, 32768);
        put_u16(&mut data, 156, 1000);
        put_u16(&mut data, 158, 0);
        put_i32(&mut data, 160, 0);
        put_u32(&mut data, 164, 32768);
        put_u16(&mut data, 168, 1000);
        put_u16(&mut data, 170, 0);
        put_i32(&mut data, 172, 0);
        put_u32(&mut data, 176, 32768);

        for slot in 0..slot_count {
            let frame = if slot >= 64 && (slot - 64) % 1001 == 0 {
                lens_frame()
            } else {
                imu_frame(slot)
            };
            data.extend_from_slice(&frame);
        }
        data
    }

    fn gyro_data(samples: &[SampleInfo]) -> &Vec<TimeVector3<f64>> {
        samples[0]
            .tag_map
            .as_ref()
            .unwrap()
            .get(&GroupId::Gyroscope)
            .unwrap()
            .get_t(TagId::Data)
            .unwrap()
    }

    #[test]
    fn time_range_seek_accounts_for_lens_slots_before_seek_start() {
        let data = senseflow_mix_file(5030);
        let mut stream = Cursor::new(data.clone());
        let samples = parse(
            &mut stream,
            data.len(),
            crate::InputOptions {
                time_range_ms: Some((5000.0, 5003.0)),
                ..Default::default()
            },
        )
        .unwrap();
        let gyro = gyro_data(&samples);

        assert!(!gyro.is_empty());
        let first_requested = gyro
            .iter()
            .find(|sample| (sample.t - 5.0).abs() < 1e-9)
            .expect("requested timestamp was not parsed");
        assert!(
            (first_requested.x - 5005.0).abs() < 1e-9,
            "5000ms should read physical slot 5005, got gyro x from slot {}",
            first_requested.x
        );
    }

    #[test]
    fn header_only_duration_uses_logical_imu_time() {
        let data = senseflow_mix_file(1001);
        let mut stream = Cursor::new(data.clone());
        let samples = parse(
            &mut stream,
            data.len(),
            crate::InputOptions {
                header_only: true,
                ..Default::default()
            },
        )
        .unwrap();

        assert_eq!(samples.len(), 1);
        assert!(
            (samples[0].duration_ms - 1000.0).abs() < 1e-9,
            "header_only duration should exclude lens slots, got {}ms",
            samples[0].duration_ms
        );
    }
}
