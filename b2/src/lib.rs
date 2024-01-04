use base64::prelude::*;
use serde_json as json;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("B2 HTTP error")]
    Http(#[from] ureq::Error),
    #[error("The B2 response was not valid JSON")]
    BadJson(#[from] std::io::Error),
    #[error("B2 {why}: {response}")]
    UnexpectedResponse { why: String, response: String },
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
    agent: ureq::Agent,
    id: String,
    url: String,
}

impl Session {
    pub fn new(key_id: &str, application_key: &str) -> Result<Self> {
        let agent = ureq::Agent::new();

        let creds = String::from(key_id) + ":" + application_key;
        let auth = String::from("Basic") + &BASE64_STANDARD.encode(&creds);
        let v: json::Value = agent
            .get("https://api.backblazeb2.com/b2api/v3/b2_authorize_account")
            .set("Authorization", &auth)
            .call()?
            .into_json()?;

        println!("{v}"); // DEBUG

        let bad = |s| unexpected(s, &v);

        let id: String = v["accountId"]
            .as_str()
            .ok_or_else(|| bad("login response missing ID"))?
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

        Ok(Session { agent, id, url })
    }
}
