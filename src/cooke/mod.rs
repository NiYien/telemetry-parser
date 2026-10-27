// SPDX-License-Identifier: MIT OR Apache-2.0
// Copyright © 2023 Adrian <adrian.eddy at gmail>

use crate::*;
use crate::tags_impl::*;
use memchr::memmem;
use serde::de::{ self, DeserializeSeed };
use std::sync::atomic::Ordering::Relaxed;

pub mod bin;

/// The `Timestamp` of the temporal records is the lens's high resolution clock, an unsigned 16-bit counter running at 150 kHz,
/// so it rolls over every 65536 ticks, about 0.44 s (Cooke /i Technology Part III, "5 Timing data")
const TIMESTAMP_MODULUS: i64 = 1 << 16;

#[derive(Default)]
pub struct Cooke {
    pub model: Option<String>,
}

impl Cooke {
    pub fn camera_type(&self) -> String { "Cooke /i".to_owned() }
    pub fn has_accurate_timestamps(&self) -> bool { false }
    pub fn possible_extensions() -> Vec<&'static str> { vec!["yml", "yaml"] }
    pub fn frame_readout_time(&self) -> Option<f64> { None }
    pub fn normalize_imu_orientation(v: String) -> String { v }
    pub fn detect<P: AsRef<std::path::Path>>(buffer: &[u8], _filepath: P, _options: &crate::InputOptions) -> Option<Self> {
        if memmem::find(buffer, b"RecordType: rt.header.lens.info").is_some() || memmem::find(buffer, b"RecordType: rt.header.recorder.info").is_some() {
            Some(Self {
                model: Some("YAML metadata".into()),
            })
        } else {
            None
        }
    }

    pub fn parse<T: Read + Seek, F: Fn(f64)>(&mut self, stream: &mut T, _size: usize, _progress_cb: F, cancel_flag: Arc<AtomicBool>, options: crate::InputOptions) -> Result<Vec<SampleInfo>> {
        let mut samples = Vec::new();
        let mut all_data = String::new();
        stream.read_to_string(&mut all_data)?;

        // let mut map = GroupedTagMap::new();
        let imu_orientation = "XYZ";

        let mut calibration_accl = None;
        let mut calibration_gyro = None;
        let mut calibration_magn = None;

        let mut map = GroupedTagMap::new();
        let mut last_timecode = None;

        let mut prev_timestamp = [0i64; 4];
        let mut prev_absolute_timestamp = [0i64; 4];
        let mut timestamp = [0i64; 4];
        let mut skipped_records = std::collections::BTreeSet::new();

        let mut records = Vec::new();
        read_records(&all_data, &cancel_flag, |record| records.push(serde_json::Value::Object(record)));
        for data in records {
            if cancel_flag.load(Relaxed) { break; }
                    let rtype = data.get("RecordType").and_then(|x| x.as_str());
                    let timecode = Self::get_timecode(&data);
                    let tsi = match rtype {
                        Some("rt.temporal.lens.accelerometer.raw") => Some(0),
                        Some("rt.temporal.lens.gyro.raw")          => Some(1),
                        Some("rt.temporal.lens.magnetometer.raw")  => Some(2),
                        Some("rt.temporal.lens.general")           => Some(3),
                        _ => None
                    };
                    if let (Some(tsi), Some(ts)) = (tsi, data.get("Timestamp").and_then(|x| x.as_i64())) {
                        // Ticks since the previous record of this type: the counter difference modulo 2^16 unwraps the roll-overs
                        timestamp[tsi] += (ts - prev_timestamp[tsi]).rem_euclid(TIMESTAMP_MODULUS);
                        prev_timestamp[tsi] = ts;
                    }
                    //if tsi == 1 { println!("{:?}, ts: {:.2}", timecode, timestamp[tsi] as f64 / 150000.0); }
                    if tsi.is_some() && last_timecode != timecode && !map.is_empty() {
                        samples.push(SampleInfo { sample_index: samples.len() as u64, tag_map: Some(std::mem::take(&mut map)), ..Default::default() });
                    }
                    match rtype {
                        Some("rt.header.lens.info") => {
                            util::insert_tag(&mut map, tag!(parsed GroupId::Lens, TagId::Metadata, "Lens info", Json, |v| format!("{:?}", v), data, vec![]), &options);
                        },
                        Some("rt.header.lens.shading") => {
                            util::insert_tag(&mut map, tag!(parsed GroupId::Lens, TagId::Shading, "Lens shading", Json, |v| format!("{:?}", v), data, vec![]), &options);
                        },
                        Some("rt.header.lens.distortion") => {
                            util::insert_tag(&mut map, tag!(parsed GroupId::Lens, TagId::Distortion, "Lens distortion", Json, |v| format!("{:?}", v), data, vec![]), &options);
                        },
                        Some("rt.header.lens.cal.accelerometer") => { calibration_accl = Self::get_mtrx::<4>(&data); },
                        Some("rt.header.lens.cal.gyro")          => { calibration_gyro = Self::get_mtrx::<7>(&data); },
                        Some("rt.header.lens.cal.magnetometer")  => { calibration_magn = Self::get_mtrx::<4>(&data); },
                        Some("rt.temporal.lens.accelerometer.raw") => {
                            if let Some(vals) = Self::get_datavals(&data) {
                                let mut accl = Vec::with_capacity(vals.len());
                                let num_vals = vals.len() as f64;
                                let timestamp_frac = (timestamp[0] as f64 - prev_absolute_timestamp[0] as f64) / num_vals;
                                for (i, x) in vals.into_iter().enumerate() {
                                    let data = if let Some(calib) = calibration_accl {
                                        (
                                            x.0 * calib[0][0] + x.1 * calib[0][1] + x.2 * calib[0][2] + 1.0 * calib[0][3],
                                            x.0 * calib[1][0] + x.1 * calib[1][1] + x.2 * calib[1][2] + 1.0 * calib[1][3],
                                            x.0 * calib[2][0] + x.1 * calib[2][1] + x.2 * calib[2][2] + 1.0 * calib[2][3]
                                        )
                                    } else {
                                        x
                                    };
                                    accl.push(TimeVector3 { t: ((timestamp[0] as f64 - ((num_vals - 1.0 - i as f64) * timestamp_frac)) / 150000.0),
                                        x: data.0,
                                        y: data.1,
                                        z: data.2
                                    });
                                }
                                util::insert_tag(&mut map, tag!(parsed GroupId::Accelerometer, TagId::Data,        "Accelerometer data",  Vec_TimeVector3_f64, |v| format!("{:?}", v), accl, vec![]), &options);
                                util::insert_tag(&mut map, tag!(parsed GroupId::Accelerometer, TagId::Unit,        "Accelerometer unit",  String, |v| v.to_string(), "m/s²".into(),  Vec::new()), &options);
                                util::insert_tag(&mut map, tag!(parsed GroupId::Accelerometer, TagId::Orientation, "IMU orientation",     String, |v| v.to_string(), imu_orientation.into(), Vec::new()), &options);
                            }
                        },
                        Some("rt.temporal.lens.gyro.raw") => {
                            if let Some(vals) = Self::get_datavals(&data) {
                                //dbg!(&vals);
                                let mut gyro = Vec::with_capacity(vals.len());
                                let num_vals = vals.len() as f64;
                                let timestamp_frac = (timestamp[1] as f64 - prev_absolute_timestamp[1] as f64) / num_vals;
                                for (i, x) in vals.into_iter().enumerate() {
                                    let data = if let Some(calib) = calibration_gyro {
                                        (
                                            x.0 * calib[0][0] + x.1 * calib[0][1] + x.2 * calib[0][2] + x.0.powi(2) * calib[0][3] + x.1.powi(2) * calib[0][4] + x.2.powi(2) * calib[0][5] + 1.0 * calib[0][6],
                                            x.0 * calib[1][0] + x.1 * calib[1][1] + x.2 * calib[1][2] + x.0.powi(2) * calib[1][3] + x.1.powi(2) * calib[1][4] + x.2.powi(2) * calib[1][5] + 1.0 * calib[1][6],
                                            x.0 * calib[2][0] + x.1 * calib[2][1] + x.2 * calib[2][2] + x.0.powi(2) * calib[2][3] + x.1.powi(2) * calib[2][4] + x.2.powi(2) * calib[2][5] + 1.0 * calib[2][6]
                                        )
                                    } else {
                                        x
                                    };
                                    // println!("{}\t{}\t{}", data.0, data.1, data.2);
                                    gyro.push(TimeVector3 { t: ((timestamp[1] as f64 - ((num_vals - 1.0 - i as f64) * timestamp_frac)) / 150000.0),
                                        x: data.0,
                                        y: data.1,
                                        z: data.2
                                    });
                                }
                                util::insert_tag(&mut map, tag!(parsed GroupId::Gyroscope, TagId::Data,        "Gyroscope data",  Vec_TimeVector3_f64, |v| format!("{:?}", v), gyro, vec![]), &options);
                                util::insert_tag(&mut map, tag!(parsed GroupId::Gyroscope, TagId::Unit,        "Gyroscope unit",  String, |v| v.to_string(), "rad/s".into(), Vec::new()), &options);
                                util::insert_tag(&mut map, tag!(parsed GroupId::Gyroscope, TagId::Orientation, "IMU orientation", String, |v| v.to_string(), imu_orientation.into(), Vec::new()), &options);
                            }
                        },
                        Some("rt.temporal.lens.magnetometer.raw") => {
                            if let Some(vals) = Self::get_datavals(&data) {
                                let mut magn = Vec::with_capacity(vals.len());
                                let num_vals = vals.len() as f64;
                                let timestamp_frac = (timestamp[2] as f64 - prev_absolute_timestamp[2] as f64) / num_vals;
                                for (i, x) in vals.into_iter().enumerate() {
                                    let data = if let Some(calib) = calibration_magn {
                                        (
                                            x.0 * calib[0][0] + x.1 * calib[0][1] + x.2 * calib[0][2] + 1.0 * calib[0][3],
                                            x.0 * calib[1][0] + x.1 * calib[1][1] + x.2 * calib[1][2] + 1.0 * calib[1][3],
                                            x.0 * calib[2][0] + x.1 * calib[2][1] + x.2 * calib[2][2] + 1.0 * calib[2][3]
                                        )
                                    } else {
                                        x
                                    };
                                    magn.push(TimeVector3 { t: ((timestamp[2] as f64 - ((num_vals - 1.0 - i as f64) * timestamp_frac)) / 150000.0),
                                        x: data.0,
                                        y: data.1,
                                        z: data.2
                                    });
                                }
                                util::insert_tag(&mut map, tag!(parsed GroupId::Magnetometer, TagId::Data,        "Magnetometer data", Vec_TimeVector3_f64, |v| format!("{:?}", v), magn, vec![]), &options);
                                util::insert_tag(&mut map, tag!(parsed GroupId::Magnetometer, TagId::Unit,        "Magnetometer unit", String, |v| v.to_string(), "T".into(), Vec::new()), &options);
                                util::insert_tag(&mut map, tag!(parsed GroupId::Magnetometer, TagId::Orientation, "IMU orientation",   String, |v| v.to_string(), imu_orientation.into(), Vec::new()), &options);
                            }
                        },
                        Some("rt.temporal.lens.general") => {
                            util::insert_tag(&mut map, tag!(parsed GroupId::Lens, TagId::Data, "Lens data", Json, |v| format!("{:?}", v), data, vec![]), &options);
                        },
                        Some("rt.header.recorder.info") => {
                            util::insert_tag(&mut map, tag!(parsed GroupId::Default, TagId::Metadata, "Recorder info", Json, |v| format!("{:?}", v), data, vec![]), &options);
                        },
                        _ => {
                            // The record types can be extended (e.g. with `.derived` data) and documents without a `RecordType`
                            // are disregarded (Cooke /i Lens Metadata File specification, "LMF Records")
                            if skipped_records.insert(rtype.map(str::to_owned)) {
                                log::debug!("Skipping Cooke /i records of type {rtype:?}");
                            }
                            continue;
                        }
                    }

                    prev_absolute_timestamp = timestamp;
                    last_timecode = timecode;
        }
        if !map.is_empty() {
            samples.push(SampleInfo { sample_index: samples.len() as u64, tag_map: Some(map), ..Default::default() });
        }

        Ok(samples)
    }

    fn get_timecode(data: &serde_json::Value) -> Option<String> {
        let obj = data.get("Timecode")?.as_object()?;
        Some(format!("{:02}:{:02}:{:02}:{:02}", obj.get("hh")?.as_i64()?, obj.get("mm")?.as_i64()?, obj.get("ss")?.as_i64()?, obj.get("ff")?.as_i64()?))
    }

    fn get_datavals(data: &serde_json::Value) -> Option<Vec<(f64, f64, f64)>> {
        let arr = data.get("Datavals")?;
        let arr = if arr.is_object() { serde_json::to_value(vec![arr]).unwrap() } else { arr.clone() };
        let arr = arr.as_array()?;
        let mut ret: Vec<_> = Vec::with_capacity(arr.len());
        for v in arr {
            let v = v.as_object()?;
            ret.push((
                v.get("X")?.as_f64()?,
                v.get("Y")?.as_f64()?,
                v.get("Z")?.as_f64()?
            ));
        }
        Some(ret)
    }
    fn get_mtrx<const N: usize>(data: &serde_json::Value) -> Option<[[f64; N]; 3]> {
        let mut ret = [[0.0f64; N]; 3];
        for r in 1..=3 {
            let arr = data.get(&format!("Row_{r}"))?.as_array()?;
            if arr.len() != N { return None; }
            for (i, x) in arr.iter().enumerate() {
                ret[r - 1][i] = x.as_f64()?;
            }
        }
        Some(ret)
    }
}

#[cfg(test)]
mod upstream_bugfix_tests {
    use super::*;

    #[test]
    fn standard_documents_and_lockit_repeated_keys_keep_their_final_record() {
        let records = "RecordType: rt.header.lens.info\nSerialNumber: one\n---\nRecordType: rt.header.recorder.info\nModel: two\n";
        let parse = |text: &str| Cooke::default().parse(&mut std::io::Cursor::new(text.as_bytes()), text.len(), |_| {}, Arc::new(AtomicBool::new(false)), crate::InputOptions::default()).unwrap();
        let data = parse(records);
        assert_eq!(data.len(), 1);
        assert!(data[0].tag_map.as_ref().unwrap().contains_key(&GroupId::Lens));
        assert!(data[0].tag_map.as_ref().unwrap().contains_key(&GroupId::Default));
        let data = parse(&records.replace("---\n", "\n"));
        assert_eq!(data.len(), 1);
        assert!(data[0].tag_map.as_ref().unwrap().contains_key(&GroupId::Default));
    }

    #[test]
    fn a_bad_yaml_document_does_not_hide_the_following_document() {
        let text = "---\nRecordType: [broken\n---\nRecordType: rt.header.lens.info\nSerialNumber: valid\n";
        let mut records = Vec::new();
        read_records(text, &AtomicBool::new(false), |record| records.push(record));
        assert_eq!(records.len(), 1);
        assert_eq!(records[0]["SerialNumber"], "valid");
    }

    #[test]
    fn calibration_rows_must_have_the_required_number_of_coefficients() {
        for length in [0, 3, 5] {
            let row = vec![1.0; length];
            let data = serde_json::json!({ "Row_1": row, "Row_2": row, "Row_3": row });
            assert!(Cooke::get_mtrx::<4>(&data).is_none());
        }
        assert_eq!(Cooke::get_mtrx::<4>(&serde_json::json!({ "Row_1": [1,2,3,4], "Row_2": [1,2,3,4], "Row_3": [1,2,3,4] })), Some([[1.0,2.0,3.0,4.0]; 3]));
    }
}

type Record = serde_json::Map<String, serde_json::Value>;

/// Calls `f` with the records of a Cooke /i Lens Metadata File, in file order
///
/// The records are the YAML documents of the file, separated by `---` lines (Cooke /i Lens Metadata File specification, "LMF Records").
/// LockitPlus leaves the document markers out and separates the records with blank lines, so to a YAML parser its file is one mapping
/// in which the keys of every record repeat: [`Records`] starts the next record where a key repeats. The YAML parser holds all the
/// events of a document in memory, about 40 times the size of its text, so such a file is handed to it in pieces, see [`record_pieces`].
///
/// libyaml can't carry on after a syntax error, so a document that isn't valid YAML is reported, and reading resumes at the next one
fn read_records(text: &str, cancel_flag: &AtomicBool, mut f: impl FnMut(Record)) {
    for piece in record_pieces(text) {
        let mut start = piece.start;
        loop {
            if cancel_flag.load(Relaxed) { return; }
            let rest = &text[start..piece.end];
            let Err(e) = read_documents(rest, cancel_flag, &mut f) else { break };

            let line = 1 + memchr::memchr_iter(b'\n', &text.as_bytes()[..start]).count();
            log::error!("Skipping a document of the Cooke /i lens metadata that isn't valid YAML: {e}, counting the lines from line {line}");

            let at = e.location().map_or(rest.len(), |l| l.index().min(rest.len()));
            match next_document(rest, at) {
                Some(next) => start += next,
                None => break
            }
        }
    }
}

fn read_documents(text: &str, cancel_flag: &AtomicBool, f: &mut impl FnMut(Record)) -> std::result::Result<(), serde_yaml::Error> {
    for document in serde_yaml::Deserializer::from_str(text) {
        if cancel_flag.load(Relaxed) { break; }
        Records(f).deserialize(document)?;
    }
    Ok(())
}

/// `text` cut before each record that LockitPlus separates from the previous one with a blank line instead of a document marker: before
/// a `RecordType` key that follows a blank line, in a YAML document that already has one. `RecordType` is the first key of every record
/// that LockitPlus and the specification write, so [`Records`] would start a new record there anyway: the cuts change nothing but how
/// much of the file the YAML parser holds at once
fn record_pieces(text: &str) -> Vec<std::ops::Range<usize>> {
    let mut pieces = Vec::new();
    let mut start = 0;
    let mut document_has_record_type = false;
    let mut after_blank_line = false;
    for (offset, line) in lines(text, 0) {
        if is_document_marker(line) {
            document_has_record_type = false;
        } else if line.strip_prefix("RecordType:").is_some_and(|v| v.is_empty() || v.starts_with([' ', '\t'])) {
            if document_has_record_type && after_blank_line {
                pieces.push(start..offset);
                start = offset;
            }
            document_has_record_type = true;
        }
        after_blank_line = line.trim().is_empty();
    }
    pieces.push(start..text.len());
    pieces
}

/// The offset of the first document marker in `text` from the line that holds byte `at`, past the start of `text`
fn next_document(text: &str, at: usize) -> Option<usize> {
    let line_start = memchr::memrchr(b'\n', &text.as_bytes()[..at]).map_or(0, |i| i + 1);
    lines(text, line_start).find(|(offset, line)| *offset > 0 && is_document_marker(line)).map(|(offset, _)| offset)
}

/// A `---` (directives end) or `...` (document end) marker line, which always separates YAML documents
fn is_document_marker(line: &str) -> bool {
    (line.starts_with("---") || line.starts_with("...")) && line[3..].chars().next().is_none_or(|c| c == ' ' || c == '\t')
}

/// The lines of `text` from byte `from`, with their offsets and without their line breaks
fn lines(text: &str, from: usize) -> impl Iterator<Item = (usize, &str)> {
    let mut pos = from;
    std::iter::from_fn(move || {
        if pos >= text.len() { return None; }
        let start = pos;
        let end = memchr::memchr(b'\n', &text.as_bytes()[pos..]).map_or(text.len(), |i| pos + i);
        pos = end + 1;
        let line = &text[start..end];
        Some((start, line.strip_suffix('\r').unwrap_or(line)))
    })
}

/// Reads one YAML document into records: a mapping is a record, but a key that is already in the record starts the next one
struct Records<'a, F>(&'a mut F);

impl<'de, F: FnMut(Record)> DeserializeSeed<'de> for Records<'_, F> {
    type Value = ();
    fn deserialize<D: de::Deserializer<'de>>(self, deserializer: D) -> std::result::Result<(), D::Error> {
        deserializer.deserialize_any(self)
    }
}

impl<'de, F: FnMut(Record)> de::Visitor<'de> for Records<'_, F> {
    type Value = ();
    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result { formatter.write_str("a mapping of record keys") }

    fn visit_map<A: de::MapAccess<'de>>(self, mut map: A) -> std::result::Result<(), A::Error> {
        let mut record = Record::new();
        while let Some(key) = map.next_key::<String>()? {
            let value = map.next_value()?;
            if record.contains_key(&key) {
                (self.0)(std::mem::take(&mut record));
            }
            record.insert(key, value);
        }
        if !record.is_empty() {
            (self.0)(record);
        }
        Ok(())
    }

    // Documents that aren't mappings have no `RecordType` and are disregarded (Cooke /i Lens Metadata File specification, "LMF Records")
    fn visit_unit<E: de::Error>(self) -> std::result::Result<(), E> { Ok(()) }
    fn visit_bool<E: de::Error>(self, _: bool) -> std::result::Result<(), E> { Ok(()) }
    fn visit_i64<E: de::Error>(self, _: i64) -> std::result::Result<(), E> { Ok(()) }
    fn visit_u64<E: de::Error>(self, _: u64) -> std::result::Result<(), E> { Ok(()) }
    fn visit_f64<E: de::Error>(self, _: f64) -> std::result::Result<(), E> { Ok(()) }
    fn visit_str<E: de::Error>(self, _: &str) -> std::result::Result<(), E> { Ok(()) }
    fn visit_seq<A: de::SeqAccess<'de>>(self, mut seq: A) -> std::result::Result<(), A::Error> {
        while seq.next_element::<de::IgnoredAny>()?.is_some() { }
        Ok(())
    }
}
