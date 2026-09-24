//! Reloadable authorization authority (KN-2 D7: rotation and revocation).
//!
//! The JWT keyring, the manifest/catalog bundle and the cursor expectation
//! derived from it form one [`Authority`], swapped atomically when its source
//! files change. Every reload bumps a generation that open subscriptions
//! watch: each one is re-authorized against the new authority and closed
//! typed when its signing key, manifest revision, stream permission or
//! requirement entitlement is gone. A reload that fails to parse keeps the
//! current authority (and is counted), it never opens access.

use crate::auth::{AccessError, JwtConfig, Principal};
use crate::bundle::Bundle;
use crate::requirement::{require_requirement, StreamRequirement};
use qdl_contracts::cursor_v3::CursorV3Expectation;
use std::sync::{Arc, RwLock};
use tokio::sync::watch;

pub struct Authority {
    pub jwt: JwtConfig,
    pub bundle: Bundle,
    pub expectation: CursorV3Expectation,
}

pub struct AuthorityHandle {
    current: RwLock<Arc<Authority>>,
    generation: watch::Sender<u64>,
}

impl AuthorityHandle {
    pub fn new(authority: Authority) -> Arc<Self> {
        let (generation, _) = watch::channel(0);
        Arc::new(Self {
            current: RwLock::new(Arc::new(authority)),
            generation,
        })
    }

    pub fn current(&self) -> Arc<Authority> {
        self.current
            .read()
            .map(|guard| guard.clone())
            .unwrap_or_else(|poisoned| poisoned.into_inner().clone())
    }

    pub fn replace(&self, authority: Authority) {
        if let Ok(mut guard) = self.current.write() {
            *guard = Arc::new(authority);
        }
        self.generation.send_modify(|value| *value += 1);
    }

    pub fn generation(&self) -> u64 {
        *self.generation.borrow()
    }

    pub fn watch(&self) -> watch::Receiver<u64> {
        self.generation.subscribe()
    }
}

/// Whether a stream opened by `principal` for `requirement` is still
/// authorized under `authority`: the same checks `authenticate` and
/// Subscribe made, minus the token time rules (a stream outlives its token,
/// as in the Python service).
pub fn reauthorize(
    authority: &Authority,
    principal: &Principal,
    consumer_id: &str,
    purpose: &str,
    requirement: &StreamRequirement,
) -> Result<(), AccessError> {
    if !authority.jwt.keys.contains_key(&principal.key_id)
        || authority.jwt.subjects_by_key_id.get(&principal.key_id) != Some(&principal.subject)
    {
        return Err(AccessError::Unauthenticated(
            "workload token signing key was revoked".into(),
        ));
    }
    let manifest = authority
        .bundle
        .manifest_by_subject(&principal.environment, &principal.subject)
        .ok_or_else(|| AccessError::Unauthenticated("consumer manifest was withdrawn".into()))?;
    if principal.consumer_manifest_revision != Some(manifest.manifest_revision)
        || manifest.consumer_id != consumer_id
    {
        return Err(AccessError::Unauthenticated(
            "workload token is not bound to the active consumer manifest revision".into(),
        ));
    }
    if !manifest.allowed_purposes.iter().any(|item| item == purpose) {
        return Err(AccessError::PermissionDenied(
            "consumer manifest does not allow the requested data purpose".into(),
        ));
    }
    let access = crate::auth::Access {
        principal: principal.clone(),
        manifest: manifest.clone(),
        purpose: purpose.to_owned(),
    };
    access.require_stream_read()?;
    require_requirement(manifest, requirement)
}
