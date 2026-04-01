// SPDX-License-Identifier: MIT OR Apache-2.0
// Copyright © 2025 Adrian <adrian.eddy at gmail>

//! Minimal TIFF/EXIF parser for Canon MP4 UUID atom.
//! Extracts Model, FocalLength, LensModel, and Canon MakerNotes (canon_fine, canon_crop).

use std::io::*;
// byteorder not needed here - using manual byte parsing for TIFF IFD
use crate::util;

// Canon UUID: 85c0b687-820f-11e0-8111-f4ce462b6a48
const CANON_UUID: [u8; 16] = [
    0x85, 0xc0, 0xb6, 0x87, 0x82, 0x0f, 0x11, 0xe0,
    0x81, 0x11, 0xf4, 0xce, 0x46, 0x2b, 0x6a, 0x48,
];

/// Data extracted from Canon UUID EXIF.
#[derive(Debug, Default)]
pub struct CanonExifData {
    pub model: Option<String>,
    pub focal_length: Option<f64>,
    pub lens_model: Option<String>,
    pub canon_fine: bool,
    pub canon_crop: bool,
    pub datetime_original: Option<String>,
    pub offset_time_original: Option<String>,
    pub subsec_time_original: Option<String>,
}

/// Find the Canon UUID atom inside an MP4 file and extract EXIF data from CNTH/CNDA.
pub fn parse_canon_uuid_exif<T: Read + Seek>(stream: &mut T, file_size: usize) -> Result<CanonExifData> {
    stream.seek(SeekFrom::Start(0))?;

    // Find moov atom
    let moov_data = find_moov_atom(stream, file_size)?;
    let mut cursor = Cursor::new(moov_data.as_slice());
    let moov_len = moov_data.len() as u64;

    // Find uuid atom with Canon UUID inside moov
    while cursor.position() < moov_len {
        let (typ, _pos, size, header_size) = util::read_box(&mut cursor)?;
        let content_size = size as i64 - header_size;
        if content_size <= 0 {
            break;
        }

        if typ == util::fourcc("uuid") && content_size >= 16 {
            let mut uuid = [0u8; 16];
            cursor.read_exact(&mut uuid)?;
            if uuid == CANON_UUID {
                let uuid_content_size = (content_size - 16) as usize;
                let start = cursor.position() as usize;
                let end = start + uuid_content_size;
                if end <= moov_data.len() {
                    let uuid_data = &moov_data[start..end];
                    return parse_canon_uuid_content(uuid_data);
                }
            }
            cursor.seek(SeekFrom::Current(content_size - 16))?;
        } else {
            cursor.seek(SeekFrom::Current(content_size))?;
        }
    }

    Err(Error::new(ErrorKind::NotFound, "Canon UUID atom not found"))
}

/// Read the moov atom data from the MP4 stream.
fn find_moov_atom<T: Read + Seek>(stream: &mut T, file_size: usize) -> Result<Vec<u8>> {
    stream.seek(SeekFrom::Start(0))?;
    let file_size = file_size as u64;

    while stream.stream_position()? < file_size {
        let (typ, _pos, size, header_size) = util::read_box(stream)?;
        let content_size = size as i64 - header_size;
        if content_size <= 0 {
            break;
        }

        if typ == util::fourcc("moov") {
            let content_size = content_size as usize;
            let mut data = vec![0u8; content_size];
            stream.read_exact(&mut data)?;
            return Ok(data);
        }
        stream.seek(SeekFrom::Current(content_size))?;
    }

    Err(Error::new(ErrorKind::NotFound, "moov atom not found"))
}

/// Parse the content of the Canon UUID atom to find CNTH → CNDA → EXIF.
fn parse_canon_uuid_content(data: &[u8]) -> Result<CanonExifData> {
    let mut cursor = Cursor::new(data);
    let len = data.len() as u64;

    while cursor.position() < len {
        let pos = cursor.position() as usize;
        if pos + 8 > data.len() { break; }

        let (typ, _pos, size, header_size) = util::read_box(&mut cursor)?;
        let content_size = size as i64 - header_size;
        if content_size <= 0 { break; }

        if typ == util::fourcc("CNTH") {
            let start = cursor.position() as usize;
            let end = start + content_size as usize;
            if end <= data.len() {
                return parse_cnth(&data[start..end]);
            }
        }
        cursor.seek(SeekFrom::Current(content_size))?;
    }

    Err(Error::new(ErrorKind::NotFound, "CNTH atom not found"))
}

/// Parse CNTH atom to find CNDA sub-atom containing JPEG+EXIF.
fn parse_cnth(data: &[u8]) -> Result<CanonExifData> {
    let mut cursor = Cursor::new(data);
    let len = data.len() as u64;

    while cursor.position() < len {
        if cursor.position() as usize + 8 > data.len() { break; }

        let (typ, _pos, size, header_size) = util::read_box(&mut cursor)?;
        let content_size = size as i64 - header_size;
        if content_size <= 0 { break; }

        if typ == util::fourcc("CNDA") {
            let start = cursor.position() as usize;
            let end = start + content_size as usize;
            if end <= data.len() {
                return parse_cnda_jpeg_exif(&data[start..end]);
            }
        }
        cursor.seek(SeekFrom::Current(content_size))?;
    }

    Err(Error::new(ErrorKind::NotFound, "CNDA atom not found"))
}

/// Parse CNDA data (JPEG with EXIF APP1) to extract TIFF IFD and Canon MakerNotes.
fn parse_cnda_jpeg_exif(data: &[u8]) -> Result<CanonExifData> {
    // Find JPEG APP1 marker (0xFFE1) followed by "Exif\0\0"
    let exif_header = b"Exif\x00\x00";
    let mut pos = 0;
    let tiff_data = loop {
        if pos + 4 > data.len() {
            return Err(Error::new(ErrorKind::InvalidData, "No EXIF APP1 found in CNDA"));
        }
        if data[pos] == 0xFF && data[pos + 1] == 0xE1 {
            let app1_len = u16::from_be_bytes([data[pos + 2], data[pos + 3]]) as usize;
            let app1_start = pos + 4;
            let app1_end = pos + 2 + app1_len;
            if app1_end <= data.len() && app1_start + 6 <= data.len() && &data[app1_start..app1_start + 6] == exif_header {
                break &data[app1_start + 6..app1_end];
            }
        }
        pos += 1;
    };

    parse_tiff_ifd(tiff_data)
}

/// Parse TIFF IFD structure from EXIF data.
fn parse_tiff_ifd(tiff_data: &[u8]) -> Result<CanonExifData> {
    if tiff_data.len() < 8 {
        return Err(Error::new(ErrorKind::InvalidData, "TIFF data too short"));
    }

    let is_le = match &tiff_data[0..2] {
        b"II" => true,
        b"MM" => false,
        _ => return Err(Error::new(ErrorKind::InvalidData, "Invalid TIFF byte order")),
    };

    let magic = read_u16(tiff_data, 2, is_le);
    if magic != 42 {
        return Err(Error::new(ErrorKind::InvalidData, "Invalid TIFF magic number"));
    }

    let ifd0_offset = read_u32(tiff_data, 4, is_le) as usize;

    let mut result = CanonExifData::default();

    // Parse IFD0
    let mut exif_ifd_offset = None;
    parse_ifd(tiff_data, ifd0_offset, is_le, &mut |tag, _typ, count, value_data, value_offset| {
        match tag {
            0x0110 => { // Model
                result.model = Some(read_string(tiff_data, value_data, value_offset, count));
            }
            0x8769 => { // ExifIFD offset
                exif_ifd_offset = Some(read_u32(tiff_data, value_offset, is_le) as usize);
            }
            _ => {}
        }
    })?;

    // Parse ExifIFD
    let mut makernotes_offset = None;
    let mut makernotes_count = 0usize;
    if let Some(offset) = exif_ifd_offset {
        parse_ifd(tiff_data, offset, is_le, &mut |tag, _typ, count, value_data, value_offset| {
            match tag {
                0x920A => { // FocalLength (RATIONAL)
                    if value_data.len() >= 8 {
                        let num = read_u32(value_data, 0, is_le);
                        let den = read_u32(value_data, 4, is_le);
                        if den > 0 {
                            result.focal_length = Some(num as f64 / den as f64);
                        }
                    }
                }
                0xA434 => { // LensModel
                    result.lens_model = Some(read_string(tiff_data, value_data, value_offset, count));
                }
                0x9003 => { // DateTimeOriginal
                    result.datetime_original = Some(read_string(tiff_data, value_data, value_offset, count));
                }
                0x9011 => { // OffsetTimeOriginal
                    result.offset_time_original = Some(read_string(tiff_data, value_data, value_offset, count));
                }
                0x9291 => { // SubSecTimeOriginal
                    result.subsec_time_original = Some(read_string(tiff_data, value_data, value_offset, count));
                }
                0x927C => { // MakerNotes
                    makernotes_offset = Some(value_offset);
                    makernotes_count = count;
                }
                _ => {}
            }
        })?;
    }

    // Parse Canon MakerNotes
    if let Some(offset) = makernotes_offset {
        parse_canon_makernotes(tiff_data, offset, makernotes_count, is_le, &mut result);
    }

    Ok(result)
}

/// Parse a TIFF IFD at the given offset.
fn parse_ifd(tiff_data: &[u8], offset: usize, is_le: bool, callback: &mut dyn FnMut(u16, u16, usize, &[u8], usize)) -> Result<()> {
    if offset + 2 > tiff_data.len() {
        return Ok(());
    }

    let count = read_u16(tiff_data, offset, is_le) as usize;
    let entries_start = offset + 2;

    for i in 0..count {
        let entry_offset = entries_start + i * 12;
        if entry_offset + 12 > tiff_data.len() {
            break;
        }

        let tag = read_u16(tiff_data, entry_offset, is_le);
        let typ = read_u16(tiff_data, entry_offset + 2, is_le);
        let cnt = read_u32(tiff_data, entry_offset + 4, is_le) as usize;

        let type_size = match typ {
            1 | 2 | 6 | 7 => 1, // BYTE, ASCII, SBYTE, UNDEFINED
            3 | 8 => 2,         // SHORT, SSHORT
            4 | 9 | 11 => 4,    // LONG, SLONG, FLOAT
            5 | 10 | 12 => 8,   // RATIONAL, SRATIONAL, DOUBLE
            _ => 1,
        };

        let total_size = cnt * type_size;
        let (value_data, value_offset) = if total_size <= 4 {
            (&tiff_data[entry_offset + 8..entry_offset + 8 + total_size.min(4)], entry_offset + 8)
        } else {
            let data_offset = read_u32(tiff_data, entry_offset + 8, is_le) as usize;
            if data_offset + total_size <= tiff_data.len() {
                (&tiff_data[data_offset..data_offset + total_size], data_offset)
            } else {
                continue;
            }
        };

        callback(tag, typ, cnt, value_data, value_offset);
    }

    Ok(())
}

/// Parse Canon MakerNotes IFD to extract tag 0x0034 and 0x4049.
fn parse_canon_makernotes(tiff_data: &[u8], offset: usize, _count: usize, is_le: bool, result: &mut CanonExifData) {
    // Canon MakerNotes is a standard IFD structure starting at the offset
    let _ = parse_ifd(tiff_data, offset, is_le, &mut |tag, typ, count, value_data, _value_offset| {
        match tag {
            0x0034 => {
                // int32u array. Check element[3] & 0x100 for Fine mode.
                if typ == 4 && count >= 4 && value_data.len() >= 16 {
                    let element3 = read_u32(value_data, 12, is_le);
                    if element3 & 0x100 != 0 {
                        result.canon_fine = true;
                    }
                }
            }
            0x4049 => {
                // int16u[4]. Check element[1] == 1 for crop mode.
                if count >= 4 && value_data.len() >= 8 {
                    let element1 = read_u16(value_data, 2, is_le);
                    if element1 == 1 {
                        result.canon_crop = true;
                    }
                }
            }
            _ => {}
        }
    });
}

// ---- Helper functions ----

fn read_u16(data: &[u8], offset: usize, is_le: bool) -> u16 {
    if is_le {
        u16::from_le_bytes([data[offset], data[offset + 1]])
    } else {
        u16::from_be_bytes([data[offset], data[offset + 1]])
    }
}

fn read_u32(data: &[u8], offset: usize, is_le: bool) -> u32 {
    if is_le {
        u32::from_le_bytes([data[offset], data[offset + 1], data[offset + 2], data[offset + 3]])
    } else {
        u32::from_be_bytes([data[offset], data[offset + 1], data[offset + 2], data[offset + 3]])
    }
}


fn read_string(tiff_data: &[u8], value_data: &[u8], value_offset: usize, count: usize) -> String {
    let _ = (tiff_data, value_offset); // Might be used for large strings
    let s = if count > 0 && value_data.last() == Some(&0) {
        &value_data[..count - 1]
    } else {
        value_data
    };
    String::from_utf8_lossy(s).trim().to_string()
}
