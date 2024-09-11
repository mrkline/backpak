use anyhow::{Context, Result};
use camino::Utf8Path;
use regex::RegexSet;

pub fn skip_matching_paths(skips: &[String]) -> Result<impl Fn(&Utf8Path) -> bool> {
    let skipset = RegexSet::new(skips).context("Skip rules are not valid regex")?;

    let filter = move |path: &Utf8Path| !skipset.is_match(path.as_str());
    Ok(filter)
}
