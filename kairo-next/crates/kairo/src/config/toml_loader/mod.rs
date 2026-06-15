mod primitives;
mod sections;

use std::fs;
use std::path::{Path, PathBuf};

use self::primitives::reject_unknown;
use self::sections::{parse_actor, parse_cluster, parse_observability, parse_remote};
use super::error::ConfigError;
use super::settings::KairoSettings;

/// Standard TOML configuration file names in load order.
///
/// `kairo.toml` is the base application configuration, and
/// `kairo.local.toml` is the optional local override applied afterward.
pub const STANDARD_TOML_FILES: [&str; 2] = ["kairo.toml", "kairo.local.toml"];

/// Loads one TOML configuration file into format-neutral [`KairoSettings`].
///
/// The loader validates the projected settings after parsing, so invalid
/// values are reported before they reach runtime builders.
///
/// ```no_run
/// use kairo::prelude::load_toml_file;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let settings = load_toml_file("kairo.local.toml")?;
/// let system = settings.actor_system_builder("app")?.build()?;
/// system.terminate(std::time::Duration::from_secs(1))?;
/// # Ok(())
/// # }
/// ```
pub fn load_toml_file(path: impl AsRef<Path>) -> Result<KairoSettings, ConfigError> {
    let path = path.as_ref();
    let table = read_toml_table(path)?;
    parse_toml_table(table)
}

/// Loads layered TOML configuration files into format-neutral [`KairoSettings`].
///
/// Files are applied in iterator order. Tables are merged recursively, while
/// later scalar values and arrays replace earlier values. This supports a
/// base `kairo.toml` plus a local override such as `kairo.local.toml` without
/// exposing TOML-specific concepts in the resulting settings model.
///
/// ```no_run
/// use kairo::prelude::load_toml_files;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let settings = load_toml_files(["kairo.toml", "kairo.local.toml"])?;
/// let system = settings.actor_system_builder("app")?.build()?;
/// system.terminate(std::time::Duration::from_secs(1))?;
/// # Ok(())
/// # }
/// ```
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

/// Finds standard TOML configuration files that exist in `dir`.
///
/// The returned paths follow [`STANDARD_TOML_FILES`] order, so passing the
/// result to [`load_toml_files`] preserves the base-plus-local override model.
///
/// ```
/// use kairo::prelude::{find_standard_toml_files, STANDARD_TOML_FILES};
///
/// let paths = find_standard_toml_files(".");
/// assert!(paths.len() <= STANDARD_TOML_FILES.len());
/// ```
pub fn find_standard_toml_files(dir: impl AsRef<Path>) -> Vec<PathBuf> {
    let dir = dir.as_ref();
    STANDARD_TOML_FILES
        .iter()
        .map(|file| dir.join(file))
        .filter(|path| path.is_file())
        .collect()
}

/// Loads standard TOML configuration files from `dir`.
///
/// Existing files are discovered with [`find_standard_toml_files`] and then
/// loaded with [`load_toml_files`]. If neither standard file exists, the
/// result is the default validated [`KairoSettings`].
///
/// ```no_run
/// use kairo::prelude::load_standard_toml_files;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let settings = load_standard_toml_files(".")?;
/// let system = settings.actor_system_builder("app")?.build()?;
/// system.terminate(std::time::Duration::from_secs(1))?;
/// # Ok(())
/// # }
/// ```
pub fn load_standard_toml_files(dir: impl AsRef<Path>) -> Result<KairoSettings, ConfigError> {
    load_toml_files(find_standard_toml_files(dir))
}

fn read_toml_table(path: &Path) -> Result<toml::Table, ConfigError> {
    let contents = fs::read_to_string(path).map_err(|error| ConfigError::ReadFailed {
        path: PathBuf::from(path),
        reason: error.to_string(),
    })?;
    parse_toml_document(&contents)
}

/// Parses TOML configuration text into format-neutral [`KairoSettings`].
///
/// This helper is useful for tests, embedded defaults, and callers that
/// already own file discovery or layering.
///
/// ```
/// use kairo::prelude::parse_toml_str;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let settings = parse_toml_str(
///     r#"
/// [actor.dispatchers.default]
/// throughput = 8
/// "#,
/// )?;
///
/// assert_eq!(settings.actor.default_dispatcher()?.throughput, 8);
/// # Ok(())
/// # }
/// ```
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
