// SPDX-License-Identifier: MIT OR Apache-2.0

//! Shared TIFF IFD parsing utilities for camera brand modules.

/// Check if data starts with a TIFF header ("II*\0" or "MM\0*")
pub fn is_tiff_header(data: &[u8]) -> bool {
    data.len() >= 4 && (
        (data[0] == b'I' && data[1] == b'I' && data[2] == 0x2a && data[3] == 0x00) ||
        (data[0] == b'M' && data[1] == b'M' && data[2] == 0x00 && data[3] == 0x2a)
    )
}

/// Read u16 with byte order
pub fn read_u16(data: &[u8], offset: usize, is_le: bool) -> Option<u16> {
    if offset + 2 > data.len() { return None; }
    Some(if is_le {
        u16::from_le_bytes([data[offset], data[offset + 1]])
    } else {
        u16::from_be_bytes([data[offset], data[offset + 1]])
    })
}

/// Read u32 with byte order
pub fn read_u32(data: &[u8], offset: usize, is_le: bool) -> Option<u32> {
    if offset + 4 > data.len() { return None; }
    Some(if is_le {
        u32::from_le_bytes([data[offset], data[offset + 1], data[offset + 2], data[offset + 3]])
    } else {
        u32::from_be_bytes([data[offset], data[offset + 1], data[offset + 2], data[offset + 3]])
    })
}

/// Read null-terminated string
pub fn read_string(data: &[u8], count: usize) -> String {
    let len = count.min(data.len());
    let end = data[..len].iter().position(|&b| b == 0).unwrap_or(len);
    String::from_utf8_lossy(&data[..end]).trim().to_string()
}

/// Detect byte order from TIFF header. Returns Some(true) for LE, Some(false) for BE, None if invalid.
pub fn detect_byte_order(data: &[u8]) -> Option<bool> {
    if data.len() < 2 { return None; }
    match (data[0], data[1]) {
        (b'I', b'I') => Some(true),
        (b'M', b'M') => Some(false),
        _ => None,
    }
}

/// Iterate IFD entries at given offset, calling callback for each entry.
/// callback(tag, type_id, count, value_data, value_offset_in_tiff)
pub fn parse_ifd_entries(tiff_data: &[u8], offset: usize, is_le: bool, callback: &mut dyn FnMut(u16, u16, usize, &[u8], usize)) {
    if offset + 2 > tiff_data.len() { return; }

    let count = match read_u16(tiff_data, offset, is_le) {
        Some(c) => c as usize,
        None => return,
    };
    let entries_start = offset + 2;

    for i in 0..count {
        let entry_offset = entries_start + i * 12;
        if entry_offset + 12 > tiff_data.len() { break; }

        let tag = match read_u16(tiff_data, entry_offset, is_le) { Some(v) => v, None => continue };
        let typ = match read_u16(tiff_data, entry_offset + 2, is_le) { Some(v) => v, None => continue };
        let cnt = match read_u32(tiff_data, entry_offset + 4, is_le) { Some(v) => v as usize, None => continue };

        let type_size = match typ {
            1 | 2 | 6 | 7 => 1, // BYTE, ASCII, SBYTE, UNDEFINED
            3 | 8 => 2,         // SHORT, SSHORT
            4 | 9 | 11 => 4,    // LONG, SLONG, FLOAT
            5 | 10 | 12 => 8,   // RATIONAL, SRATIONAL, DOUBLE
            _ => 1,
        };

        let total_size = cnt.saturating_mul(type_size);
        let (value_data, value_offset) = if total_size <= 4 {
            let end = (entry_offset + 8 + total_size.min(4)).min(tiff_data.len());
            (&tiff_data[entry_offset + 8..end], entry_offset + 8)
        } else {
            let data_offset = match read_u32(tiff_data, entry_offset + 8, is_le) {
                Some(v) => v as usize,
                None => continue,
            };
            if data_offset.saturating_add(total_size) <= tiff_data.len() {
                (&tiff_data[data_offset..data_offset + total_size], data_offset)
            } else {
                continue;
            }
        };

        callback(tag, typ, cnt, value_data, value_offset);
    }
}

/// Standard EXIF data extracted from TIFF IFD
#[derive(Default, Debug)]
pub struct BasicExifData {
    pub model: Option<String>,
    pub focal_length: Option<f64>,
    pub lens_model: Option<String>,
    pub focal_length_35mm: Option<u32>,
    pub exif_ifd_offset: Option<usize>,
    pub makernotes_offset: Option<usize>,
    pub makernotes_count: Option<usize>,
}

/// Parse standard EXIF tags from a TIFF IFD (IFD0 + ExifIFD).
/// Returns BasicExifData with common fields extracted.
pub fn parse_standard_exif(tiff_data: &[u8]) -> Option<BasicExifData> {
    if tiff_data.len() < 8 { return None; }

    let is_le = detect_byte_order(tiff_data)?;

    let magic = read_u16(tiff_data, 2, is_le);
    if magic != Some(42) { return None; }

    let ifd0_offset = read_u32(tiff_data, 4, is_le)? as usize;

    let mut result = BasicExifData::default();

    parse_ifd_entries(tiff_data, ifd0_offset, is_le, &mut |tag, _typ, count, value_data, value_offset| {
        match tag {
            0x0110 => { // Model
                result.model = Some(read_string(value_data, count));
            }
            0x8769 => { // ExifIFD offset
                result.exif_ifd_offset = read_u32(tiff_data, value_offset, is_le).map(|v| v as usize);
            }
            _ => {}
        }
    });

    // Parse ExifIFD
    if let Some(offset) = result.exif_ifd_offset {
        parse_ifd_entries(tiff_data, offset, is_le, &mut |tag, _typ, count, value_data, _value_offset| {
            match tag {
                0x920A => { // FocalLength (RATIONAL)
                    if value_data.len() >= 8 {
                        if let (Some(num), Some(den)) = (read_u32(value_data, 0, is_le), read_u32(value_data, 4, is_le)) {
                            if den > 0 {
                                result.focal_length = Some(num as f64 / den as f64);
                            }
                        }
                    }
                }
                0xA434 => { // LensModel
                    result.lens_model = Some(read_string(value_data, count));
                }
                0xA405 => { // FocalLengthIn35mmFormat
                    if _typ == 3 && value_data.len() >= 2 {
                        // SHORT
                        if let Some(v) = read_u16(value_data, 0, is_le) {
                            result.focal_length_35mm = Some(v as u32);
                        }
                    } else if _typ == 4 && value_data.len() >= 4 {
                        // LONG
                        if let Some(v) = read_u32(value_data, 0, is_le) {
                            result.focal_length_35mm = Some(v);
                        }
                    }
                }
                0x927C => { // MakerNotes
                    result.makernotes_offset = Some(_value_offset);
                    result.makernotes_count = Some(count);
                }
                _ => {}
            }
        });
    }

    Some(result)
}

/// Extract TIFF data from a JPEG with EXIF APP1.
/// Scans for JPEG SOI + APP1 marker, then finds "Exif\0" header and returns the TIFF portion.
pub fn extract_tiff_from_jpeg(data: &[u8]) -> Option<&[u8]> {
    use memchr::memmem;

    // Find JPEG SOI
    let soi_pos = memmem::find(data, b"\xff\xd8")?;

    // Search for APP1 marker (0xFFE1) after SOI
    let mut pos = soi_pos + 2;
    while pos + 4 < data.len() {
        if data[pos] == 0xFF && data[pos + 1] == 0xE1 {
            let app1_len = u16::from_be_bytes([data[pos + 2], data[pos + 3]]) as usize;
            let app1_data_start = pos + 4;
            let app1_end = pos + 2 + app1_len;
            if app1_end <= data.len() && app1_data_start + 6 <= data.len() {
                // Check for "Exif\0" (5 bytes) -- the 6th byte varies between implementations
                if &data[app1_data_start..app1_data_start + 5] == b"Exif\0" {
                    // TIFF header starts after "Exif\0\0" (6 bytes) or "Exif\0X" (6 bytes)
                    let tiff_start = app1_data_start + 6;
                    if tiff_start < app1_end {
                        return Some(&data[tiff_start..app1_end]);
                    }
                }
            }
            break;
        }
        // Skip to next marker
        if data[pos] == 0xFF {
            if pos + 3 < data.len() {
                let marker_len = u16::from_be_bytes([data[pos + 2], data[pos + 3]]) as usize;
                pos = pos + 2 + marker_len;
            } else {
                break;
            }
        } else {
            pos += 1;
        }
    }
    None
}
