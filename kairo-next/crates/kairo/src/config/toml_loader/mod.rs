mod primitives;
mod sections;

use std::fs;
use std::path::Path;

use self::primitives::reject_unknown;
use self::sections::{parse_actor, parse_cluster, parse_observability, parse_remote};
use super::error::ConfigError;
use super::settings::KairoSettings;

pub fn load_toml_file(path: impl AsRef<Path>) -> Result<KairoSettings, ConfigError> {
    let path = path.as_ref();
    let contents = fs::read_to_string(path).map_err(|error| ConfigError::ReadFailed {
        path: path.to_path_buf(),
        reason: error.to_string(),
    })?;
    parse_toml_str(&contents)
}

pub fn parse_toml_str(input: &str) -> Result<KairoSettings, ConfigError> {
    let table = input
        .parse::<toml::Table>()
        .map_err(|error| ConfigError::ParseFailed {
            reason: error.to_string(),
        })?;
    reject_unknown(&table, "", &["actor", "remote", "cluster", "observability"])?;

    let mut settings = KairoSettings::default();
    if let Some(actor) = table.get("actor") {
        settings.actor = parse_actor(actor)?;
    }
    if let Some(remote) = table.get("remote") {
        settings.remote = parse_remote(remote)?;
    }
    if let Some(cluster) = table.get("cluster") {
        settings.cluster = parse_cluster(cluster)?;
    }
    if let Some(observability) = table.get("observability") {
        settings.observability = parse_observability(observability)?;
    }
    settings.validate()?;
    Ok(settings)
}
