use std::{fs, io};

use anyhow::{Context, Result, anyhow};
use camino::Utf8PathBuf;
use serde_derive::Deserialize;

use crate::backend::cache;

// lol: Serde wants a function to call for defaults.
#[inline]
fn defcachesize() -> u64 {
    cache::DEFAULT_SIZE
}

#[derive(Debug, Deserialize)]
pub struct Configuration {
    #[serde(default = "defcachesize")]
    pub cache_size: u64,
}

impl Default for Configuration {
    fn default() -> Self {
        Self {
            cache_size: cache::DEFAULT_SIZE
        }
    }
}

pub fn load() -> Result<Configuration> {
    let mut confpath: Utf8PathBuf = home::home_dir()
        .ok_or_else(|| anyhow!("Can't find home directory"))?
        .try_into()
        .context("Home directory isn't UTF-8")?;
    confpath.extend([".config", "backpak.toml"]);
    let s = match fs::read_to_string(&confpath) {
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            return Ok(Configuration::default())
        }
        found => found
    }.context("Couldn't open {confpath}")?;
    let conf = toml::from_str(&s).context("Couldn't parse {confpath}")?;
    Ok(conf)
}
