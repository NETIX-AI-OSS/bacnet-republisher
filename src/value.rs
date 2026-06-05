use crate::model::TelemetryValue;
use bacnet_types::enums::{ObjectType, PropertyIdentifier};
use std::fmt::Write;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodeError {
    Empty,
    UnsupportedTag(u8),
    Truncated,
    InvalidUtf8,
}

impl std::fmt::Display for DecodeError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Empty => formatter.write_str("empty BACnet value"),
            Self::UnsupportedTag(tag) => {
                write!(formatter, "unsupported BACnet application tag {tag}")
            }
            Self::Truncated => formatter.write_str("truncated BACnet value"),
            Self::InvalidUtf8 => formatter.write_str("invalid BACnet character string"),
        }
    }
}

impl std::error::Error for DecodeError {}

pub fn decode_scalar_value(bytes: &[u8]) -> Result<TelemetryValue, DecodeError> {
    let Some(first) = bytes.first().copied() else {
        return Err(DecodeError::Empty);
    };

    let tag = first >> 4;
    let length_code = first & 0x07;

    match tag {
        0 => Ok(TelemetryValue::Text("null".to_string())),
        1 => Ok(TelemetryValue::Text((length_code != 0).to_string())),
        2 => decode_unsigned(bytes).map(|value| TelemetryValue::Number(value as f64)),
        3 => decode_signed(bytes).map(|value| TelemetryValue::Number(value as f64)),
        4 => decode_real(bytes),
        5 => decode_double(bytes),
        7 => decode_character_string(bytes),
        9 => decode_unsigned(bytes).map(|value| TelemetryValue::Text(value.to_string())),
        12 => decode_object_id(bytes).map(|(object_type, instance)| {
            TelemetryValue::Text(format!("{},{}", object_type_name(object_type), instance))
        }),
        other => Err(DecodeError::UnsupportedTag(other)),
    }
}

pub fn decode_object_identifier_value(bytes: &[u8]) -> Result<(ObjectType, u32), DecodeError> {
    decode_object_id(bytes)
}

pub fn decode_unsigned_value(bytes: &[u8]) -> Result<u64, DecodeError> {
    decode_unsigned(bytes)
}

pub fn property_identifier_from_text(value: &str) -> Option<PropertyIdentifier> {
    let normalized = normalize_identifier(value);
    if let Ok(raw) = normalized.parse::<u32>() {
        return Some(PropertyIdentifier::from_raw(raw));
    }
    PropertyIdentifier::ALL_NAMED
        .iter()
        .find(|(name, _)| normalize_identifier(name) == normalized)
        .map(|(_, value)| *value)
}

pub fn object_type_from_text(value: &str) -> Option<ObjectType> {
    let normalized = normalize_identifier(value);
    if let Ok(raw) = normalized.parse::<u32>() {
        return Some(ObjectType::from_raw(raw));
    }
    ObjectType::ALL_NAMED
        .iter()
        .find(|(name, _)| normalize_identifier(name) == normalized)
        .map(|(_, value)| *value)
}

#[cfg(test)]
pub fn property_name(property: PropertyIdentifier) -> String {
    PropertyIdentifier::ALL_NAMED
        .iter()
        .find(|(_, value)| *value == property)
        .map(|(name, _)| name.to_ascii_lowercase())
        .unwrap_or_else(|| property.to_raw().to_string())
}

pub fn object_type_name(object_type: ObjectType) -> String {
    ObjectType::ALL_NAMED
        .iter()
        .find(|(_, value)| *value == object_type)
        .map(|(name, _)| name.to_ascii_lowercase())
        .unwrap_or_else(|| object_type.to_raw().to_string())
}

pub fn hex(bytes: &[u8]) -> String {
    let mut value = String::new();
    for byte in bytes {
        let _ = write!(&mut value, "{byte:02X}");
    }
    value
}

fn decode_unsigned(bytes: &[u8]) -> Result<u64, DecodeError> {
    let payload = application_payload(bytes)?;
    let mut value = 0u64;
    for byte in payload {
        value = (value << 8) | u64::from(*byte);
    }
    Ok(value)
}

fn decode_signed(bytes: &[u8]) -> Result<i64, DecodeError> {
    let payload = application_payload(bytes)?;
    if payload.is_empty() {
        return Ok(0);
    }
    let negative = payload[0] & 0x80 != 0;
    let mut value = if negative { -1i64 } else { 0i64 };
    for byte in payload {
        value = (value << 8) | i64::from(*byte);
    }
    Ok(value)
}

fn decode_real(bytes: &[u8]) -> Result<TelemetryValue, DecodeError> {
    let payload = application_payload(bytes)?;
    if payload.len() != 4 {
        return Err(DecodeError::Truncated);
    }
    let raw = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
    Ok(TelemetryValue::Number(f32::from_bits(raw) as f64))
}

fn decode_double(bytes: &[u8]) -> Result<TelemetryValue, DecodeError> {
    let payload = application_payload(bytes)?;
    if payload.len() != 8 {
        return Err(DecodeError::Truncated);
    }
    let raw = u64::from_be_bytes([
        payload[0], payload[1], payload[2], payload[3], payload[4], payload[5], payload[6],
        payload[7],
    ]);
    Ok(TelemetryValue::Number(f64::from_bits(raw)))
}

fn decode_character_string(bytes: &[u8]) -> Result<TelemetryValue, DecodeError> {
    let payload = application_payload(bytes)?;
    if payload.is_empty() {
        return Ok(TelemetryValue::Text(String::new()));
    }
    let encoding = payload[0];
    let data = &payload[1..];
    match encoding {
        0 | 3 | 4 | 5 => std::str::from_utf8(data)
            .map(|value| TelemetryValue::Text(value.to_string()))
            .map_err(|_| DecodeError::InvalidUtf8),
        _ => Ok(TelemetryValue::Text(hex(data))),
    }
}

fn decode_object_id(bytes: &[u8]) -> Result<(ObjectType, u32), DecodeError> {
    let payload = application_payload(bytes)?;
    if payload.len() != 4 {
        return Err(DecodeError::Truncated);
    }
    let raw = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
    let object_type = ObjectType::from_raw((raw >> 22) & 0x3ff);
    let instance = raw & 0x3f_ffff;
    Ok((object_type, instance))
}

fn application_payload(bytes: &[u8]) -> Result<&[u8], DecodeError> {
    let Some(first) = bytes.first().copied() else {
        return Err(DecodeError::Empty);
    };
    let length_code = first & 0x07;
    let (offset, length) = match length_code {
        0..=4 => (1, length_code as usize),
        5 => {
            let Some(length) = bytes.get(1).copied() else {
                return Err(DecodeError::Truncated);
            };
            (2, length as usize)
        }
        _ => return Err(DecodeError::UnsupportedTag(first >> 4)),
    };
    let end = offset + length;
    if end > bytes.len() {
        return Err(DecodeError::Truncated);
    }
    Ok(&bytes[offset..end])
}

fn normalize_identifier(value: &str) -> String {
    value
        .trim()
        .chars()
        .filter(|character| !matches!(character, '_' | '-' | ' '))
        .flat_map(char::to_lowercase)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_real_value() {
        let value = decode_scalar_value(&[0x44, 0x42, 0x90, 0x00, 0x00]).unwrap();
        assert_eq!(value, TelemetryValue::Number(72.0));
    }

    #[test]
    fn decodes_boolean_as_text() {
        assert_eq!(
            decode_scalar_value(&[0x11]).unwrap(),
            TelemetryValue::Text("true".to_string())
        );
    }

    #[test]
    fn maps_named_identifiers_from_text() {
        assert_eq!(
            property_identifier_from_text("present value"),
            Some(PropertyIdentifier::PRESENT_VALUE)
        );
        assert_eq!(
            object_type_from_text("analog_input"),
            Some(ObjectType::ANALOG_INPUT)
        );
    }
}
