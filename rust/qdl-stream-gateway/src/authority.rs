//! Reloadable authorization authority (KN-2 D7: rotation and revocation).
//!
//! The JWT keyring, the manifest/catalog bundle and the cursor expectation
//! derived from it form one [`Authority`], swapped atomically when its source
//! files change. Every reload bumps a generation stored **with** the
//! authority, so a request binds the generation it was admitted under
//! ([`AuthorityHandle::snapshot`]) and later compares numbers instead of
//! relying on a watcher created after admission (Astra KN-2 R1 F1): a reload
//! during replay, catch-up or a blocked send is never missed. A stream is
//! re-authorized against the new authority and closed typed when its signing
//! key, manifest revision, stream permission or entitlement (requirement for
//! Subscribe, feed scope for Replay) is gone. A reload that fails to parse
//! keeps the current authority (and is counted), it never opens access.

use crate::auth::{Access, AccessError, JwtConfig, Principal};
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
    current: RwLock<(Arc<Authority>, u64)>,
    generation: watch::Sender<u64>,
}

impl AuthorityHandle {
    pub fn new(authority: Authority) -> Arc<Self> {
        let (generation, _) = watch::channel(0);
        Arc::new(Self {
            current: RwLock::new((Arc::new(authority), 0)),
            generation,
        })
    }

    pub fn current(&self) -> Arc<Authority> {
        self.snapshot().0
    }

    /// The authority and the generation it belongs to, read together.
    pub fn snapshot(&self) -> (Arc<Authority>, u64) {
        self.current
            .read()
            .map(|guard| guard.clone())
            .unwrap_or_else(|poisoned| poisoned.into_inner().clone())
    }

    /// Swap the authority; the generation is bumped under the same lock
    /// before watchers are woken.
    pub fn replace(&self, authority: Authority) {
        let generation = match self.current.write() {
            Ok(mut guard) => {
                guard.1 += 1;
                guard.0 = Arc::new(authority);
                guard.1
            }
            Err(poisoned) => {
                let mut guard = poisoned.into_inner();
                guard.1 += 1;
                guard.0 = Arc::new(authority);
                guard.1
            }
        };
        self.generation.send_replace(generation);
    }

    pub fn generation(&self) -> u64 {
        self.snapshot().1
    }

    pub fn watch(&self) -> watch::Receiver<u64> {
        self.generation.subscribe()
    }
}

/// What a stream was admitted for: Subscribe carries a requirement, Replay
/// only a product whose feed scope the consumer must hold.
#[derive(Clone, Debug)]
pub enum Entitlement {
    Requirement(Box<StreamRequirement>),
    FeedScope {
        instrument_uid: String,
        feed: String,
    },
}

/// Whether a stream opened by `principal` is still authorized under
/// `authority`: the same checks `authenticate` and Subscribe/Replay made,
/// minus the token time rules (a stream outlives its token, as in the Python
/// service).
pub fn reauthorize(
    authority: &Authority,
    principal: &Principal,
    consumer_id: &str,
    purpose: &str,
    entitlement: &Entitlement,
) -> Result<(), AccessError> {
    if authority.jwt.key_environment(&principal.key_id) != principal.environment
        || !authority.jwt.keys.contains_key(&principal.key_id)
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
    let access = Access {
        principal: principal.clone(),
        manifest: manifest.clone(),
        purpose: purpose.to_owned(),
    };
    access.require_stream_read()?;
    match entitlement {
        Entitlement::Requirement(requirement) => require_requirement(manifest, requirement),
        Entitlement::FeedScope {
            instrument_uid,
            feed,
        } => access.require_feed_scope(instrument_uid, feed),
    }
}
