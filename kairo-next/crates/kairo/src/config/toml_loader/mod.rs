mod primitives;
mod sections;

use std::fs;
use std::path::{Path, PathBuf};

use self::primitives::reject_unknown;
use self::sections::{parse_actor, parse_cluster, parse_observability, parse_remote};
use super::error::ConfigError;
use super::settings::KairoSettings;

pub fn load_toml_file(path: impl AsRef<Path>) -> Result<KairoSettings, ConfigError> {
    let path = path.as_ref();
    let table = read_toml_table(path)?;
    parse_toml_table(table)
}

pub fn load_toml_files<I, P>(paths: I) -> Result<KairoSettings, ConfigError>
where
    I: IntoIterator<Item = P>,
    P: AsRef<Path>,
{
    let mut merged = toml::Table::new();
    for path in paths {
        merge_tables(&mut merged, read_toml_table(path.as_ref())?);
    }
    parse_toml_table(merged)
}

fn read_toml_table(path: &Path) -> Result<toml::Table, ConfigError> {
    let contents = fs::read_to_string(path).map_err(|error| ConfigError::ReadFailed {
        path: PathBuf::from(path),
        reason: error.to_string(),
    })?;
    parse_toml_document(&contents)
}

pub fn parse_toml_str(input: &str) -> Result<KairoSettings, ConfigError> {
    parse_toml_table(parse_toml_document(input)?)
}

fn parse_toml_document(input: &str) -> Result<toml::Table, ConfigError> {
    input
        .parse::<toml::Table>()
        .map_err(|error| ConfigError::ParseFailed {
            reason: error.to_string(),
        })
}

fn parse_toml_table(table: toml::Table) -> Result<KairoSettings, ConfigError> {
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

fn merge_tables(base: &mut toml::Table, overlay: toml::Table) {
    for (key, overlay_value) in overlay {
        match (base.get_mut(&key), overlay_value) {
            (Some(toml::Value::Table(base_table)), toml::Value::Table(overlay_table)) => {
                merge_tables(base_table, overlay_table);
            }
            (_, overlay_value) => {
                base.insert(key, overlay_value);
            }
        }
    }
}
