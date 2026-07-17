#![deny(missing_docs)]

use std::cmp::Ordering;
use std::fmt::{self, Display, Formatter};
use std::hash::{Hash, Hasher};
use std::str::FromStr;

#[derive(Debug, Clone, PartialEq, Eq)]
/// Validation failure for an application version advertised through cluster membership.
pub struct ApplicationVersionError {
    value: String,
    reason: &'static str,
}

impl ApplicationVersionError {
    fn new(value: &str, reason: &'static str) -> Self {
        Self {
            value: value.to_string(),
            reason,
        }
    }
}

impl Display for ApplicationVersionError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "invalid cluster application version {:?}: {}",
            self.value, self.reason
        )
    }
}

impl std::error::Error for ApplicationVersionError {}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ParsedVersion {
    numbers: [i32; 4],
    qualifier: String,
}

#[derive(Debug, Clone)]
/// Comparable application version carried by cluster membership.
///
/// Ordering follows Pekko's `Version`: one to three numeric components are
/// supported, an optional qualifier sorts before the unqualified release, and
/// sbt-dynver commit counts such as `1.2.3+10-deadbeef` compare numerically.
/// A single non-numeric segment remains a valid opaque qualifier. Equality and
/// hashing use the parsed ordering form, so `1`, `1.0`, and `1.0.0` are equal.
pub struct ApplicationVersion {
    value: String,
    parsed: ParsedVersion,
}

impl ApplicationVersion {
    /// Pekko-compatible zero version used when decoding the historical v1 wire layout.
    pub const ZERO: &'static str = "0.0.0";

    /// Parses a cluster application version.
    pub fn new(value: impl Into<String>) -> Result<Self, ApplicationVersionError> {
        let value = value.into();
        let parsed = parse_version(&value)?;
        Ok(Self { value, parsed })
    }

    /// Returns the original version string used on the wire and in diagnostics.
    pub fn as_str(&self) -> &str {
        &self.value
    }

    /// Returns whether this is semantically the zero compatibility version.
    pub fn is_zero(&self) -> bool {
        self == &Self::default()
    }
}

impl Default for ApplicationVersion {
    fn default() -> Self {
        Self {
            value: Self::ZERO.to_string(),
            parsed: ParsedVersion {
                numbers: [0; 4],
                qualifier: String::new(),
            },
        }
    }
}

impl Display for ApplicationVersion {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(&self.value)
    }
}

impl FromStr for ApplicationVersion {
    type Err = ApplicationVersionError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

impl PartialEq for ApplicationVersion {
    fn eq(&self, other: &Self) -> bool {
        self.parsed == other.parsed
    }
}

impl Eq for ApplicationVersion {}

impl Hash for ApplicationVersion {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.parsed.hash(state);
    }
}

impl PartialOrd for ApplicationVersion {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ApplicationVersion {
    fn cmp(&self, other: &Self) -> Ordering {
        self.parsed
            .numbers
            .cmp(&other.parsed.numbers)
            .then_with(|| {
                match (
                    self.parsed.qualifier.is_empty(),
                    other.parsed.qualifier.is_empty(),
                ) {
                    (true, false) => Ordering::Greater,
                    (false, true) => Ordering::Less,
                    _ => self.parsed.qualifier.cmp(&other.parsed.qualifier),
                }
            })
    }
}

fn parse_version(value: &str) -> Result<ParsedVersion, ApplicationVersionError> {
    if value.is_empty() {
        return Err(ApplicationVersionError::new(value, "must not be empty"));
    }

    let segments: Vec<_> = value.split('.').collect();
    let mut numbers = [0; 4];
    let qualifier;
    match segments.as_slice() {
        [single] => {
            if single.as_bytes()[0].is_ascii_digit() {
                match single.parse::<i32>() {
                    Ok(number) => {
                        numbers[0] = number;
                        qualifier = String::new();
                    }
                    Err(_) => qualifier = (*single).to_string(),
                }
            } else {
                qualifier = (*single).to_string();
            }
        }
        [major, last] => {
            numbers[0] = parse_number(value, major)?;
            let (minor, dynver, rest) = parse_last_parts(value, last)?;
            numbers[1] = minor;
            numbers[2] = dynver;
            qualifier = rest;
        }
        [major, minor, last] => {
            numbers[0] = parse_number(value, major)?;
            numbers[1] = parse_number(value, minor)?;
            let (patch, dynver, rest) = parse_last_parts(value, last)?;
            numbers[2] = patch;
            numbers[3] = dynver;
            qualifier = rest;
        }
        _ => {
            return Err(ApplicationVersionError::new(
                value,
                "supports at most three dot-separated components",
            ));
        }
    }

    Ok(ParsedVersion { numbers, qualifier })
}

fn parse_number(value: &str, segment: &str) -> Result<i32, ApplicationVersionError> {
    segment
        .parse::<i32>()
        .map_err(|_| ApplicationVersionError::new(value, "contains an invalid numeric component"))
}

fn parse_last_parts(
    value: &str,
    segment: &str,
) -> Result<(i32, i32, String), ApplicationVersionError> {
    if segment.is_empty() {
        return Ok((0, 0, String::new()));
    }

    let separator = [segment.find('-'), segment.find('+')]
        .into_iter()
        .flatten()
        .min();
    let (number, rest) = match separator {
        Some(index) => (
            parse_number(value, &segment[..index])?,
            &segment[index + 1..],
        ),
        None => (parse_number(value, segment)?, ""),
    };

    if rest.is_empty() || !rest.as_bytes()[0].is_ascii_digit() {
        return Ok((number, 0, rest.to_string()));
    }

    let Some(separator) = rest.find('-') else {
        return Ok((number, 0, rest.to_string()));
    };
    let Ok(dynver) = rest[..separator].parse::<i32>() else {
        return Ok((number, 0, rest.to_string()));
    };
    Ok((number, dynver, rest[separator + 1..].to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn version(value: &str) -> ApplicationVersion {
        ApplicationVersion::new(value).unwrap()
    }

    #[test]
    fn ordering_matches_numeric_qualifier_and_dynver_semantics() {
        assert_eq!(version("1"), version("1.0.0"));
        assert!(version("1.2-RC1") < version("1.2"));
        assert!(version("1.2.3+3-73475dce26") < version("1.2.3+10-ed316bd024"));
        assert!(version("2.0.0") > version("1.99.99"));
    }

    #[test]
    fn parser_accepts_pekko_shapes_and_rejects_invalid_multi_part_values() {
        assert_eq!(version("build-main").as_str(), "build-main");
        assert_eq!(version("1.2-SNAPSHOT").as_str(), "1.2-SNAPSHOT");
        assert!(ApplicationVersion::new("").is_err());
        assert!(ApplicationVersion::new("1.two").is_err());
        assert!(ApplicationVersion::new("1.2.3.4").is_err());
    }
}
