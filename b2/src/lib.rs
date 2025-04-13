use base64::prelude::*;
use serde_json as json;
use thiserror::Error;

use std::io::{prelude::*, Cursor};

#[derive(Error, Debug)]
pub enum Error {
    #[error("B2 I/O failure: {0}")]
    Io(#[from] std::io::Error),
    #[error("B2 HTTP error: {0}")]
    Http(Box<ureq::Error>),
    #[error("B2 {why}: {response}")]
    UnexpectedResponse { why: String, response: String },
    #[error("B2: Couldn't find {what}")]
    NotFound { what: String },
}

impl From<ureq::Error> for Error {
    fn from(e: ureq::Error) -> Self {
        Self::Http(Box::new(e))
    }
}

fn unexpected(w: &str, r: &json::Value) -> Error {
    Error::UnexpectedResponse {
        why: w.to_string(),
        response: r.to_string(),
    }
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub struct Session {
    token: String,
    url: String,
    upload_url: String,
    upload_token: String,
    bucket_name: String,
    bucket_id: String,
}

// Once we authenticate in Session::new,
// we shouldn't have any redirects as the API gives us URLs to use.
fn noredir() -> ureq::Agent {
    ureq::Agent::new_with_config(ureq::Agent::config_builder().max_redirects(0).build())
}

impl Session {
    pub fn new<S: Into<String>>(key_id: &str, application_key: &str, bucket: S) -> Result<Self> {
        let bucket = bucket.into();

        let creds = String::from(key_id) + ":" + application_key;
        let auth = String::from("Basic") + &BASE64_STANDARD.encode(creds);
        let v: json::Value = ureq::get("https://api.backblazeb2.com/b2api/v3/b2_authorize_account")
            .header("Authorization", &auth)
            .call()?
            .body_mut()
            .read_json()?;

        let bad = |s| unexpected(s, &v);

        let id: String = v["accountId"]
            .as_str()
            .ok_or_else(|| bad("login response missing authorization token"))?
            .to_owned();

        let token: String = v["authorizationToken"]
            .as_str()
            .ok_or_else(|| bad("login response missing authorization token"))?
            .to_owned();

        let url = v["apiInfo"]["storageApi"]["apiUrl"]
            .as_str()
            .ok_or_else(|| bad("login response missing API URL"))?
            .to_owned();

        let capes = v["apiInfo"]["storageApi"]["capabilities"]
            .as_array()
            .ok_or_else(|| bad("login response missing capabilities"))?;
        let capes = capes
            .iter()
            .map(|v| {
                v.as_str()
                    .ok_or_else(|| bad("login response had malformed capabilities"))
            })
            .collect::<Result<Vec<&str>>>()?;

        if !capes.iter().any(|c| *c == "listKeys") {
            return Err(bad("credentials can not list files"));
        }
        if !capes.iter().any(|c| *c == "readFiles") {
            return Err(bad("credentials can not read files"));
        }
        if !capes.iter().any(|c| *c == "writeFiles") {
            return Err(bad("credentials can not write files"));
        }
        if !capes.iter().any(|c| *c == "deleteFiles") {
            return Err(bad("credentials can not delete files"));
        }

        let br: json::Value = ureq::get(&(url.clone() + "/b2api/v2/b2_list_buckets"))
            .header("Authorization", &token)
            .query("accountId", &id)
            .query("bucketName", &bucket)
            .call()?
            .body_mut()
            .read_json()?;

        let bucket_id = match br["buckets"].as_array() {
            Some(bs) => {
                match bs
                    .iter()
                    .find(|b| b["bucketName"].as_str() == Some(&bucket))
                {
                    Some(mah_bukkit) => mah_bukkit["bucketId"]
                        .as_str()
                        .ok_or_else(|| unexpected("bucket was missing ID", &br))?
                        .to_owned(),
                    None => return Err(Error::NotFound { what: bucket }),
                }
            }
            None => return Err(Error::NotFound { what: bucket }),
        };

        let ur: json::Value = ureq::get(&(url.clone() + "/b2api/v2/b2_get_upload_url"))
            .header("Authorization", &token)
            .query("bucketId", &bucket_id)
            .call()?
            .body_mut()
            .read_json()?;

        let upload_url = ur["uploadUrl"]
            .as_str()
            .ok_or_else(|| unexpected("couldn't get bucket upload URL", &ur))?
            .to_owned();

        let upload_token = ur["authorizationToken"]
            .as_str()
            .ok_or_else(|| unexpected("couldn't get bucket upload token", &ur))?
            .to_owned();

        Ok(Session {
            token,
            url,
            upload_url,
            upload_token,
            bucket_name: bucket,
            bucket_id,
        })
    }

    pub fn list(&self, prefix: Option<&str>) -> Result<Vec<(String, u64)>> {
        let mut fs = vec![];
        let mut start_name: Option<String> = None;
        loop {
            let mut req = noredir()
                .get(&(self.url.clone() + "/b2api/v2/b2_list_file_names"))
                .header("Authorization", &self.token)
                .query("bucketId", &self.bucket_id)
                .query("maxFileCount", "10000");
            if let Some(p) = prefix {
                req = req.query("prefix", p);
            }
            if let Some(sn) = &start_name {
                req = req.query("startFileName", sn);
            }

            let lfn = req.call()?.body_mut().read_json()?;

            let bad = |s| unexpected(s, &lfn);

            let files = lfn["files"]
                .as_array()
                .ok_or_else(|| bad("didn't list file names"))?;
            for fj in files
                .iter()
                .filter(|f| f["action"].as_str() == Some("upload"))
                .filter_map(
                    |f| match (f["fileName"].as_str(), f["contentLength"].as_u64()) {
                        (Some(n), Some(l)) => Some((n.to_owned(), l)),
                        _ => None,
                    },
                )
            {
                fs.push(fj);
            }

            start_name = lfn["nextFileName"].as_str().map(|s| s.to_owned());
            if start_name.is_none() {
                break;
            }
        }
        fs.shrink_to_fit(); // We won't be growing this any more.
        Ok(fs)
    }

    pub fn get(&self, name: &str) -> Result<impl Read> {
        let r = noredir()
            .get(&(self.url.clone() + "/file/" + &self.bucket_name + "/" + name))
            .header("Authorization", &self.token)
            .call()?;

        Ok(r.into_body().into_reader())
    }

    pub fn put(&self, name: &str, len: u64, contents: &mut dyn Read) -> Result<()> {
        use data_encoding::HEXLOWER;
        use sha1::{Digest, Sha1};

        // B2 wants the SHA1 hash (as hex), but we can provide it at the end.
        // Very nice.
        enum HashAppendingReader<R> {
            Contents { inner: R, hasher: Option<Sha1> },
            HashSuffix(Cursor<Vec<u8>>),
        }

        impl<R> HashAppendingReader<R> {
            fn new(inner: R) -> Self {
                Self::Contents {
                    inner,
                    hasher: Some(Sha1::new()),
                }
            }
        }

        impl<R: Read> Read for HashAppendingReader<R> {
            fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
                match self {
                    Self::Contents { inner, hasher } => {
                        // Read some bytes from the inner Read trait object.
                        let bytes_read = inner.read(buf)?;
                        if bytes_read > 0 {
                            // If we got some bytes, update the SHA1 hash with those and return.
                            hasher.as_mut().unwrap().update(&buf[..bytes_read]);
                            Ok(bytes_read)
                        } else {
                            // Otherwise we're done reading.
                            // Consume the hasher, get the hash,
                            // and start feeding that to whoever's reading.
                            let sha = hasher.take().unwrap().finalize();
                            let sha_hex = HEXLOWER.encode(&sha).to_string().into_bytes();
                            *self = Self::HashSuffix(Cursor::new(sha_hex));
                            // Recurse (to the HashSuffix match arm).
                            self.read(buf)
                        }
                    }
                    Self::HashSuffix(c) => c.read(buf),
                }
            }
        }

        let mut hr = HashAppendingReader::new(contents);

        noredir()
            .post(&self.upload_url)
            .header("Authorization", &self.upload_token)
            .header("Content-Length", &(len + 40).to_string()) // SHA1 is 40 hex digits long.
            .header("X-Bz-File-Name", name) // No need to URL-encode, our names are boring
            .header("Content-Type", "b2/x-auto") // Go ahead and guess
            .header("X-Bz-Content-Sha1", "hex_digits_at_end")
            .send(ureq::SendBody::from_reader(&mut hr))?;

        Ok(())
    }

    pub fn delete(&self, name: &str) -> Result<()> {
        let req = noredir()
            .get(&(self.url.clone() + "/b2api/v2/b2_list_file_versions"))
            .header("Authorization", &self.token)
            .query("bucketId", &self.bucket_id)
            .query("prefix", name);

        let lfv = req.call()?.body_mut().read_json()?;
        let where_name = || unexpected(&format!("couldn't find {name}"), &lfv);

        let versions = lfv["files"].as_array().ok_or_else(where_name)?;

        if versions.is_empty() {
            return Err(where_name());
        }
        if versions.len() != 1 {
            return Err(unexpected(
                &format!("found multiple versions of {name}"),
                &lfv,
            ));
        }
        let id = versions[0]["fileId"]
            .as_str()
            .ok_or_else(|| unexpected(&format!("couldn't find ID for {name}"), &lfv))?;

        ureq::post(&(self.url.clone() + "/b2api/v2/b2_delete_file_version"))
            .header("Authorization", &self.token)
            .send_json(json::json!({
                "fileName": name,
                "fileId": id
            }))?;

        Ok(())
    }
}
