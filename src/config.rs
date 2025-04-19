use std::{fs, io};

use anyhow::{Context, Result, anyhow};
use byte_unit::Byte;
use camino::Utf8PathBuf;
use serde_derive::Deserialize;
use tracing::*;

use crate::backend::cache;

// Big Macro demands this be a function and not a value
#[inline]
fn defcachesize() -> Byte {
    cache::DEFAULT_SIZE
}

#[derive(Debug, Default, Deserialize)]
pub struct RestoreConfiguration {
    pub output: Option<Utf8PathBuf>,
}

#[derive(Debug, Deserialize)]
pub struct Configuration {
    #[serde(default = "defcachesize")]
    pub cache_size: Byte,

    #[serde(default)]
    pub skips: Vec<String>,

    #[serde(default)]
    pub restore: RestoreConfiguration,
}

impl Default for Configuration {
    fn default() -> Self {
        Self {
            cache_size: cache::DEFAULT_SIZE,
            skips: vec![],
            restore: Default::default(),
        }
    }
}

pub fn load(p: Option<Utf8PathBuf>) -> Result<Configuration> {
    let confpath: Result<Utf8PathBuf> = match p {
        Some(p) => {
            if p.as_str().is_empty() {
                debug!("Using default config per --config \"\"");
                return Ok(Configuration::default());
            } else {
                Ok(p)
            }
        }
        None => {
            let mut c: Utf8PathBuf = home::home_dir()
                .ok_or_else(|| anyhow!("Can't find home directory"))?
                .try_into()
                .context("Home directory isn't UTF-8")?;
            c.extend([".config", "backpak.toml"]);
            Ok(c)
        }
    };
    let confpath = confpath?;
    let s = match fs::read_to_string(&confpath) {
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Configuration::default()),
        found => found,
    }
    .with_context(|| format!("Couldn't open {confpath}"))?;
    let mut conf: Configuration =
        toml::from_str(&s).with_context(|| format!("Couldn't parse {confpath}"))?;
    let local_conf = load_cwd()?;
    // We have <> at home.
    if let Some(lc) = local_conf {
        if lc.cache_size != cache::DEFAULT_SIZE {
            // Too noisy? I dunno, it's strange.
            warn!("Ignoring cache_size in .backpak - set it globally in ~/.config/backpak.toml");
        }
        // Don't concatenate ignores - you might want to *not* skip certain things
        // the global config would have you skip.
        conf.skips = lc.skips;
        conf.restore.output = lc.restore.output.or(conf.restore.output);
    };
    Ok(conf)
}

fn load_cwd() -> Result<Option<Configuration>> {
    let s = match fs::read_to_string(".backpak") {
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        found => found,
    }
    .with_context(|| format!("Couldn't open .backpak"))?;
    let conf = toml::from_str(&s).with_context(|| format!("Couldn't parse .bakpak"))?;
    Ok(Some(conf))
}

pub fn merge_skips(config: Vec<String>, args: Vec<String>) -> Vec<String> {
    if config.is_empty() {
        args
    } else {
        let mut s = config;
        s.extend(args);
        s.sort();
        s.dedup();
        // Dumb, but makes it less ambiguous as to what escapes are for the regex
        // and which are for str's Display instance
        debug!("Config merged with args for skip list:");
        for a in &s {
            debug!("skip {a}");
        }
        s
    }
}
