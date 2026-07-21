use super::LOCAL_NETWORK_RE;
use crate::config::Config;
use crate::utils;
use std::net::{IpAddr, SocketAddr};

/// Address facts fixed when an inbound session is accepted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SessionOrigin {
    peer_ip: IpAddr,
    is_local: bool,
}

impl SessionOrigin {
    pub(crate) fn from_remote_addr(remote_addr: SocketAddr, config: &Config) -> Self {
        let peer_ip = utils::normalize_ip(remote_addr.ip());
        let host_addr = peer_ip.to_string().to_lowercase();
        let is_local = LOCAL_NETWORK_RE.is_match(&host_addr) || config.client_host == host_addr;

        Self { peer_ip, is_local }
    }

    pub(crate) fn peer_ip(self) -> IpAddr {
        self.peer_ip
    }

    pub(crate) fn is_local(self) -> bool {
        self.is_local
    }

    pub(crate) fn admission_class(self, rpc_authorized: bool) -> AdmissionClass {
        if self.is_local || rpc_authorized {
            AdmissionClass::Exempt
        } else {
            AdmissionClass::Limited
        }
    }
}

/// Whether admission must enforce the normal H@H connection limit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AdmissionClass {
    Exempt,
    Limited,
}

/// Java `Settings.isValidRPCServer()` evaluated against the current settings.
/// This is deliberately request-time policy: a persistent session can observe
/// refreshed RPC-server settings for a later `servercmd` request.
pub(crate) fn is_rpc_authorized(peer_ip: IpAddr, config: &Config) -> bool {
    config.disable_ip_origin_check || config.rpc_servers.contains(&peer_ip)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::FixtureDirs;

    #[test]
    fn peer_policy_separates_locality_from_rpc_authorization() {
        let fixture = FixtureDirs::new();
        let mut config = fixture.config();
        let remote_ip: IpAddr = "203.0.113.9".parse().unwrap();

        let origin =
            SessionOrigin::from_remote_addr("[::ffff:203.0.113.9]:443".parse().unwrap(), &config);
        assert_eq!(origin.peer_ip(), remote_ip);
        assert!(!origin.is_local());
        assert!(!is_rpc_authorized(remote_ip, &config));
        assert_eq!(origin.admission_class(false), AdmissionClass::Limited);

        config.rpc_servers.push(remote_ip);
        assert!(is_rpc_authorized(remote_ip, &config));
        assert_eq!(origin.admission_class(true), AdmissionClass::Exempt);

        config.rpc_servers.clear();
        config.disable_ip_origin_check = true;
        assert!(is_rpc_authorized(remote_ip, &config));
    }
}
