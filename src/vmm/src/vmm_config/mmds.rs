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
    /// Parse Vec<String> to Vec<Ipv4Addr>.
    /// Error out if there parse error encountered.
    pub fn ipv4_addr_pool(&self) -> Result<Vec<Ipv4Addr>, Error> {
        let mut ipv4_addr_vec = Vec::new();
        for s in self.ipv4_address_pool.iter() {
	    let ipv4_addr = Ipv4Addr::from_str(s).map_err(Error::IPv4ParseError)?;
            ipv4_addr_vec.push(ipv4_addr);
	}

        Ok(ipv4_addr_vec)
    }
}

#[derive(Debug)]
/// Used to describe errors related to MMDS configuration.
pub enum Error {
    /// `SetMmdsConfiguration` operation is not allowed post boot.
    SetMmdsConfigurationNotAllowedPostBoot,
    /// IPv4 parse error.
    IPv4ParseError(AddrParseError),
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            Error::SetMmdsConfigurationNotAllowedPostBoot => {
                write!(f, "Setting MMDS configuration is not allowed after boot.",)
            }
            Error::IPv4ParseError(err) => write!(f, "{:?}", err),
        }
    }
}

#[cfg(test)]
mod tests {
    use vmm_config::mmds::MmdsConfig;
    use vmm_config::mmds::Error;

    #[test]
    fn test_error_messages() {
	assert_eq!(format!("{}", Error::SetMmdsConfigurationNotAllowedPostBoot),
		"Setting MMDS configuration is not allowed after boot.");
    }

    #[test]
    fn test_ipv4_addr_pool() {
        let mmds_config = MmdsConfig {
	    ipv4_address_pool: vec!["1.1.1.1".to_string(), "2.2.2.2".to_string()]
        };
	assert!(mmds_config.ipv4_addr_pool().is_ok());

        let mmds_config = MmdsConfig {
	    ipv4_address_pool: Vec::new()
        };
	assert!(mmds_config.ipv4_addr_pool().is_ok());

        let wrong_mmds_config = MmdsConfig {
	    ipv4_address_pool: vec!["1.1.1.1.1".to_string()]
        };
	assert!(wrong_mmds_config.ipv4_addr_pool().is_err());
    }
}
