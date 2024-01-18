use base64::prelude::*;
use serde_json as json;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("B2 HTTP error")]
    Http(#[from] minreq::Error),
    #[error("B2 HTTP {code}: {reason}")]
    BadReply { code: i32, reason: String },
    #[error("B2 {why}: {response}")]
    UnexpectedResponse { why: String, response: String },
    #[error("B2: Couldn't find {what}")]
    NotFound { what: String },
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

fn check_response(r: minreq::Response) -> Result<json::Value> {
    if r.status_code != 200 {
        return Err(Error::BadReply {
            code: r.status_code,
            reason: r.as_str()?.to_owned(),
        });
    }
    Ok(r.json()?)
}

impl Session {
    pub fn new<S: Into<String>>(key_id: &str, application_key: &str, bucket: S) -> Result<Self> {
        let bucket = bucket.into();

        let creds = String::from(key_id) + ":" + application_key;
        let auth = String::from("Basic") + &BASE64_STANDARD.encode(creds);
        let v: json::Value = check_response(
            minreq::get("https://api.backblazeb2.com/b2api/v3/b2_authorize_account")
                .with_header("Authorization", auth)
                .send()?,
        )?;

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

        let br: json::Value = check_response(
            minreq::get(url.clone() + "/b2api/v2/b2_list_buckets")
                .with_header("Authorization", &token)
                .with_param("accountId", id)
                .with_param("bucketName", &bucket)
                .send()?,
        )?;

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

        let ur: json::Value = check_response(
            minreq::get(url.clone() + "/b2api/v2/b2_get_upload_url")
                .with_header("Authorization", &token)
                .with_param("bucketId", &bucket_id)
                .send()?,
        )?;

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

    pub fn list(&self) -> Result<Vec<String>> {
        let mut fs = vec![];
        let mut start_name = None;
        loop {
            let mut req = minreq::get(self.url.clone() + "/b2api/v2/b2_list_file_names")
                .with_header("Authorization", &self.token)
                .with_param("bucketId", &self.bucket_id)
                .with_param("maxFileCount", "10000");
            if let Some(sn) = start_name {
                req = req.with_param("startFileName", sn);
            }

            let lfn = check_response(req.send()?)?;

            let bad = |s| unexpected(s, &lfn);

            let files = lfn["files"]
                .as_array()
                .ok_or_else(|| bad("didn't list file names"))?;
            for fj in files
                .iter()
                .filter(|f| f["action"].as_str() == Some("upload"))
                .filter_map(|f| f["fileName"].as_str())
            {
                fs.push(fj.to_owned());
            }

            start_name = lfn["nextFileName"].as_str().map(|s| s.to_owned());
            if start_name.is_none() {
                break;
            }
        }
        Ok(fs)
    }

    // TODO: minreq needs a streming API that doesn't copy byte-by-byte,
    //       and this needs to return a Read.
    pub fn get(&self, name: &str) -> Result<Vec<u8>> {
        let r = minreq::get(self.url.clone() + "/file/" + &self.bucket_name + "/" + name)
            .with_header("Authorization", &self.token)
            .send()?;

        if r.status_code != 200 {
            return Err(Error::BadReply {
                code: r.status_code,
                reason: r.as_str()?.to_owned(),
            });
        }
        Ok(r.into_bytes())
    }

    // TODO: minreq needs a streaming API that doesn't only take buffers.
    //       This should take a Read and it should write it.
    pub fn put(&self, name: &str, contents: &[u8]) -> Result<()> {
        use data_encoding::HEXLOWER;
        use sha1::{Digest, Sha1};

        let mut hasher = Sha1::new();
        hasher.update(contents);
        let sha = hasher.finalize();

        let r = minreq::post(&self.upload_url)
            .with_header("Authorization", &self.upload_token)
            .with_header("Content-Length", contents.len().to_string())
            .with_header("X-Bz-File-Name", name) // No need to URL-encode, our names are boring.
            .with_header("Content-Type", "b2/x-auto") // Go ahead and guess
            .with_header("X-Bz-Content-Sha1", HEXLOWER.encode(&sha).to_string())
            .with_body(contents)
            .send()?;

        check_response(r)?;
        Ok(())
    }

    pub fn delete(&self, name: &str) -> Result<()> {
        let req = minreq::get(self.url.clone() + "/b2api/v2/b2_list_file_versions")
            .with_header("Authorization", &self.token)
            .with_param("bucketId", &self.bucket_id)
            .with_param("prefix", name);

        let lfv = check_response(req.send()?)?;
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

        let del = minreq::post(self.url.clone() + "/b2api/v2/b2_delete_file_version")
            .with_header("Authorization", &self.token)
            .with_json(&json::json!({
                "fileName": name,
                "fileId": id
            }))?
            .send()?;

        check_response(del)?;
        Ok(())
    }
}
