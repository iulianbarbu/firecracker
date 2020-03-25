// Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

use super::super::VmmAction;
use micro_http::StatusCode;
use request::{Body, Error, ParsedRequest};
use vmm::vmm_config::mmds::MmdsConfig;

pub fn parse_get_mmds() -> Result<ParsedRequest, Error> {
    Ok(ParsedRequest::GetMMDS)
}

pub fn parse_put_mmds(body: &Body, first_token: Option<&&str>) -> Result<ParsedRequest, Error> {
    let mut config_request = false;
    if let Some(config_path) = first_token {
        if *config_path == "config" {
            config_request = true;
        } else {
	    return Err(Error::Generic(
                StatusCode::BadRequest,
                format!("Unrecognized PUT request path `{}`.", *config_path)
            ));
	}
    }

    if config_request {
        let mmds_config = serde_json::from_slice::<MmdsConfig>(body.raw()).map_err(|e| {
            //METRICS.put_api_requests.drive_fails.inc();
            Error::SerdeJson(e)
        })?;

        let mmds_ipv4_addr_pool = mmds_config.ipv4_addr_pool();
        if mmds_ipv4_addr_pool.is_err() {
            // Safe, checked above.
            return Err(Error::MmdsConfig(mmds_ipv4_addr_pool.err().unwrap()));
        }

        // Safe, checked above.
        let mmds_ipv4_addr_pool = mmds_ipv4_addr_pool.unwrap();

        if mmds_ipv4_addr_pool.is_empty() {
            // METRICS.put_api_requests.drive_fails.inc();
            return Err(Error::Generic(
                StatusCode::BadRequest,
                "The IPv4 addresses pool is empty.".to_string(),
            ));
        } else {
            return Ok(ParsedRequest::Sync(VmmAction::SetMmdsConfiguration(
                mmds_config,
            )));
        }
    }

    Ok(ParsedRequest::PutMMDS(
        serde_json::from_slice(body.raw()).map_err(Error::SerdeJson)?,
    ))
}

pub fn parse_patch_mmds(body: &Body) -> Result<ParsedRequest, Error> {
    Ok(ParsedRequest::PatchMMDS(
        serde_json::from_slice(body.raw()).map_err(Error::SerdeJson)?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_get_mmds_request() {
        assert!(parse_get_mmds().is_ok());
    }

    #[test]
    fn test_parse_put_mmds_request() {
        let body = r#"{
                "foo": "bar"
              }"#;
        assert!(parse_put_mmds(&Body::new(body), None).is_ok());

        let body = "invalid_body";
        assert!(parse_put_mmds(&Body::new(body), None).is_err());

        let body = r#"{
                "ipv4_address_pool": ["1.1.1.1"]
              }"#;
        let path = "config";
        assert!(parse_put_mmds(&Body::new(body), Some(&path)).is_ok());

        let body = r#"{
                "ipv4_address_pool": []
              }"#;
        let path = "config";
        assert!(parse_put_mmds(&Body::new(body), Some(&path)).is_err());

        let body = r#"{
                "invalid_config": "invalid_value"
              }"#;
        let path = "config";
        assert!(parse_put_mmds(&Body::new(body), Some(&path)).is_err());

        let path = "invalid_path";
        assert!(parse_put_mmds(&Body::new(body), Some(&path)).is_err());
    }

    #[test]
    fn test_parse_patch_mmds_request() {
        let body = r#"{
                "foo": "bar"
              }"#;
        assert!(parse_patch_mmds(&Body::new(body)).is_ok());

        let body = "invalid_body";
        assert!(parse_patch_mmds(&Body::new(body)).is_err());
    }
}
