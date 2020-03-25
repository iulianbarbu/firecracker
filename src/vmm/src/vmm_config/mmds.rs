// Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

use std::fmt::{Display, Formatter};

use std::net::{AddrParseError, Ipv4Addr};
use std::str::FromStr;

/// This struct represents the configuration realted to the MMDS service.
#[derive(Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct MmdsConfig {
    /// Pool of MMDS endpoints..
    ipv4_address_pool: Vec<String>,
}

impl MmdsConfig {
    /// Parse Vec<String> to Vec<Ipv4>. Error out if there parse error encountered.
    pub fn ipv4_addr_pool(&self) -> Result<Vec<Ipv4Addr>, MmdsConfigError> {
        Ok(self
            .ipv4_address_pool
            .iter()
            .map(|ipv4_str| {
                Ipv4Addr::from_str(ipv4_str)
                    .map_err(MmdsConfigError::IPv4ParseError)
                    .unwrap()
            })
            .collect())
    }
}

#[derive(Debug)]
/// Used to describe errors related to MMDS configuration.
pub enum MmdsConfigError {
    /// `SetMmdsConfiguration` operation is not allowed post boot.
    SetMmdsConfigurationNotAllowedPostBoot,
    /// IPv4 parse error.
    IPv4ParseError(AddrParseError),
}

impl Display for MmdsConfigError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            MmdsConfigError::SetMmdsConfigurationNotAllowedPostBoot => {
                write!(f, "Setting MMDS configuration is not allowed after boot.",)
            }
            MmdsConfigError::IPv4ParseError(err) => write!(f, "{:?}", err),
        }
    }
}
