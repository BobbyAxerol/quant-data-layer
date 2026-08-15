use std::collections::BTreeMap;

use sha2::{Digest, Sha256};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AssignmentPlan {
    pub epoch: u64,
    pub algorithm: &'static str,
    pub assignments: BTreeMap<String, String>,
}

pub fn rendezvous_plan(
    epoch: u64,
    instruments: &[String],
    owners: &[String],
) -> Result<AssignmentPlan, String> {
    if epoch == 0 || owners.is_empty() || owners.iter().any(|owner| owner.trim().is_empty()) {
        return Err("plan epoch and non-empty owners are required".into());
    }
    let mut assignments = BTreeMap::new();
    for instrument in instruments {
        if instrument.trim().is_empty() {
            return Err("instrument identity must not be empty".into());
        }
        let owner = owners
            .iter()
            .max_by_key(|owner| {
                let mut hasher = Sha256::new();
                hasher.update(b"qdl-rendezvous-v1\0");
                hasher.update(instrument.as_bytes());
                hasher.update(b"\0");
                hasher.update(owner.as_bytes());
                let digest: [u8; 32] = hasher.finalize().into();
                digest
            })
            .expect("owners checked non-empty")
            .clone();
        assignments.insert(instrument.clone(), owner);
    }
    Ok(AssignmentPlan {
        epoch,
        algorithm: "sha256-rendezvous-v1",
        assignments,
    })
}

pub fn changed_assignments(previous: &AssignmentPlan, next: &AssignmentPlan) -> usize {
    previous
        .assignments
        .iter()
        .filter(|(instrument, owner)| next.assignments.get(*instrument) != Some(*owner))
        .count()
}

#[cfg(test)]
mod tests {
    use super::{changed_assignments, rendezvous_plan};

    #[test]
    fn adding_owner_has_bounded_churn_and_is_deterministic() {
        let instruments = (0..10_000)
            .map(|index| format!("i-{index}"))
            .collect::<Vec<_>>();
        let two = vec!["owner-a".into(), "owner-b".into()];
        let three = vec!["owner-a".into(), "owner-b".into(), "owner-c".into()];
        let first = rendezvous_plan(1, &instruments, &two).unwrap();
        assert_eq!(first, rendezvous_plan(1, &instruments, &two).unwrap());
        let next = rendezvous_plan(2, &instruments, &three).unwrap();
        let churn = changed_assignments(&first, &next) as f64 / instruments.len() as f64;
        assert!(churn > 0.25 && churn < 0.40, "unexpected churn {churn}");
    }

    #[test]
    fn adding_instrument_does_not_move_existing_ownership() {
        let owners = vec!["a".into(), "b".into(), "c".into()];
        let initial = vec!["btc".into(), "eth".into()];
        let expanded = vec!["btc".into(), "eth".into(), "sol".into()];
        let first = rendezvous_plan(1, &initial, &owners).unwrap();
        let next = rendezvous_plan(2, &expanded, &owners).unwrap();
        assert_eq!(changed_assignments(&first, &next), 0);
    }
}
