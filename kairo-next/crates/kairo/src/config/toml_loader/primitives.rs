use std::collections::BTreeSet;
use std::time::Duration;

use toml::Value;

use crate::config::ConfigError;

pub(super) fn expect_table<'a>(
    value: &'a Value,
    path: &str,
) -> Result<&'a toml::map::Map<String, Value>, ConfigError> {
    value.as_table().ok_or_else(|| ConfigError::InvalidType {
        path: path.to_string(),
        expected: "a table".to_string(),
    })
}

pub(super) fn reject_unknown(
    table: &toml::map::Map<String, Value>,
    path: &str,
    allowed: &[&str],
) -> Result<(), ConfigError> {
    let allowed: BTreeSet<_> = allowed.iter().copied().collect();
    for key in table.keys() {
        if !allowed.contains(key.as_str()) {
            let path = if path.is_empty() {
                key.clone()
            } else {
                format!("{path}.{key}")
            };
            return Err(ConfigError::UnknownKey { path });
        }
    }
    Ok(())
}

pub(super) fn parse_string(value: &Value, path: &str) -> Result<String, ConfigError> {
    value
        .as_str()
        .map(ToString::to_string)
        .ok_or_else(|| ConfigError::InvalidType {
            path: path.to_string(),
            expected: "a string".to_string(),
        })
}

pub(super) fn optional_non_empty_string(
    table: &toml::map::Map<String, Value>,
    key: &str,
    path: &str,
) -> Result<Option<String>, ConfigError> {
    table
        .get(key)
        .map(|value| {
            let value = parse_string(value, path)?;
            Ok((!value.is_empty()).then_some(value))
        })
        .transpose()
        .map(Option::flatten)
}

pub(super) fn optional_bool(
    table: &toml::map::Map<String, Value>,
    key: &str,
    path: &str,
) -> Result<Option<bool>, ConfigError> {
    table
        .get(key)
        .map(|value| {
            value.as_bool().ok_or_else(|| ConfigError::InvalidType {
                path: path.to_string(),
                expected: "a boolean".to_string(),
            })
        })
        .transpose()
}

pub(super) fn optional_duration(
    table: &toml::map::Map<String, Value>,
    key: &str,
    path: &str,
) -> Result<Option<Duration>, ConfigError> {
    table
        .get(key)
        .map(|value| parse_duration(value, path))
        .transpose()
}

pub(super) fn parse_string_array(value: &Value, path: &str) -> Result<Vec<String>, ConfigError> {
    let array = value.as_array().ok_or_else(|| ConfigError::InvalidType {
        path: path.to_string(),
        expected: "an array of strings".to_string(),
    })?;
    array
        .iter()
        .enumerate()
        .map(|(index, item)| parse_string(item, &format!("{path}[{index}]")))
        .collect()
}

pub(super) fn parse_usize(value: &Value, path: &str) -> Result<usize, ConfigError> {
    let value = parse_u64(value, path)?;
    usize::try_from(value).map_err(|_| ConfigError::InvalidValue {
        path: path.to_string(),
        reason: "must fit in usize".to_string(),
    })
}

pub(super) fn parse_u64(value: &Value, path: &str) -> Result<u64, ConfigError> {
    let value = value.as_integer().ok_or_else(|| ConfigError::InvalidType {
        path: path.to_string(),
        expected: "an integer".to_string(),
    })?;
    u64::try_from(value).map_err(|_| ConfigError::InvalidValue {
        path: path.to_string(),
        reason: "must not be negative".to_string(),
    })
}

pub(super) fn parse_duration(value: &Value, path: &str) -> Result<Duration, ConfigError> {
    match value {
        Value::Integer(_) => Ok(Duration::from_millis(parse_u64(value, path)?)),
        Value::String(input) => parse_duration_string(input, path),
        _ => Err(ConfigError::InvalidType {
            path: path.to_string(),
            expected: "a duration string or integer milliseconds".to_string(),
        }),
    }
}

pub(super) fn reject_zero_duration(duration: Duration, path: &str) -> Result<(), ConfigError> {
    if duration.is_zero() {
        Err(ConfigError::InvalidValue {
            path: path.to_string(),
            reason: "must be greater than zero".to_string(),
        })
    } else {
        Ok(())
    }
}

fn parse_duration_string(input: &str, path: &str) -> Result<Duration, ConfigError> {
    let Some((number, multiplier)) = duration_parts(input.trim()) else {
        return Err(ConfigError::InvalidValue {
            path: path.to_string(),
            reason: "use integer milliseconds or a string with ms, s, m, or h suffix".to_string(),
        });
    };
    let value = number
        .parse::<u64>()
        .map_err(|_| ConfigError::InvalidValue {
            path: path.to_string(),
            reason: "duration amount must be an unsigned integer".to_string(),
        })?;
    value
        .checked_mul(multiplier)
        .map(Duration::from_millis)
        .ok_or_else(|| ConfigError::InvalidValue {
            path: path.to_string(),
            reason: "duration is too large".to_string(),
        })
}

fn duration_parts(input: &str) -> Option<(&str, u64)> {
    for (suffix, multiplier) in [("ms", 1), ("s", 1_000), ("m", 60_000), ("h", 3_600_000)] {
        if let Some(number) = input.strip_suffix(suffix) {
            return Some((number.trim(), multiplier));
        }
    }
    None
}
