//! Task placement constraints and worker tags.
//!
//! Workers describe themselves with `WorkerCapabilities::tags`; tasks ask for particular workers
//! with `Task::tags`. Both fields have always been on the wire, so nothing here changes the
//! protocol (design record `docs/design/0001-two-node-readiness.md`).
//!
//! * **Worker tags.** A worker adds [`system_tags`] (`os:<os>`, `arch:<arch>`) itself, and its
//!   operator may add user tags ([`validate_user_tag`]). In `ListWorkers` / `ClusterStatus`
//!   answers the master adds `link:<path>`, `id:<worker uuid>` and `state:<state>`. Those five
//!   prefixes are [`RESERVED_TAG_PREFIXES`]: no user tag may start with one.
//! * **Task constraints.** `require:<key>=<value>` entries of `Task::tags`, parsed into a
//!   [`Placement`]. Keys: `os`, `arch`, `worker` (UUID or name) and `tag` (repeatable). Every
//!   constraint must hold; other task tags are ignored.

use std::fmt;

use uuid::Uuid;

use crate::capabilities::WorkerCapabilities;

/// Prefix of a placement constraint in `Task::tags`.
pub const REQUIRE_PREFIX: &str = "require:";
/// Prefix of the operating-system tag a worker adds itself (`os:macos`).
pub const OS_TAG_PREFIX: &str = "os:";
/// Prefix of the CPU-architecture tag a worker adds itself (`arch:aarch64`).
pub const ARCH_TAG_PREFIX: &str = "arch:";
/// Prefix of the connection-path tag the master adds to worker listings (`link:TCP/LAN`).
pub const LINK_TAG_PREFIX: &str = "link:";
/// Prefix of the worker-id tag the master adds to worker listings.
pub const ID_TAG_PREFIX: &str = "id:";
/// Prefix of the connection-state tag the master adds to worker listings (`state:busy`).
pub const STATE_TAG_PREFIX: &str = "state:";
/// Tag prefixes only the worker or the master may set.
pub const RESERVED_TAG_PREFIXES: [&str; 5] = [
    OS_TAG_PREFIX,
    ARCH_TAG_PREFIX,
    LINK_TAG_PREFIX,
    ID_TAG_PREFIX,
    STATE_TAG_PREFIX,
];
/// Longest user tag, in bytes.
pub const MAX_USER_TAG_LEN: usize = 64;

/// The tags every worker advertises about itself: `os:<std::env::consts::OS>` and
/// `arch:<std::env::consts::ARCH>`, for example `os:macos` and `arch:aarch64`.
pub fn system_tags() -> Vec<String> {
    vec![
        format!("{OS_TAG_PREFIX}{}", std::env::consts::OS),
        format!("{ARCH_TAG_PREFIX}{}", std::env::consts::ARCH),
    ]
}

/// Checks a tag an operator gives a worker (`worker --tag`): 1 to [`MAX_USER_TAG_LEN`] bytes,
/// no whitespace, `=` or `,`, and none of the [`RESERVED_TAG_PREFIXES`].
pub fn validate_user_tag(tag: &str) -> Result<(), String> {
    if tag.is_empty() {
        return Err("a worker tag cannot be empty".to_string());
    }
    if tag.len() > MAX_USER_TAG_LEN {
        return Err(format!(
            "worker tag `{tag}` is {} bytes long; the limit is {MAX_USER_TAG_LEN}",
            tag.len()
        ));
    }
    if tag
        .chars()
        .any(|c| c.is_whitespace() || c == '=' || c == ',')
    {
        return Err(format!(
            "worker tag `{tag}` contains whitespace, `=` or `,`"
        ));
    }
    if let Some(prefix) = RESERVED_TAG_PREFIXES.iter().find(|p| tag.starts_with(**p)) {
        return Err(format!(
            "worker tag `{tag}` starts with `{prefix}`, which only the worker or the master may set"
        ));
    }
    Ok(())
}

/// The value of the first tag in `tags` that starts with `prefix`, without the prefix.
pub fn tag_value<'a>(tags: &'a [String], prefix: &str) -> Option<&'a str> {
    tags.iter().find_map(|t| t.strip_prefix(prefix))
}

/// The worker id the master put in a listed worker's tags (`id:<uuid>`), if any.
pub fn listed_worker_id(caps: &WorkerCapabilities) -> Option<Uuid> {
    tag_value(&caps.tags, ID_TAG_PREFIX).and_then(|v| v.parse().ok())
}

/// Canonical operating-system name: lowercase, with `mac`, `darwin`, `osx`, `macosx` folded
/// into `macos` and `win`, `win32`, `win64` into `windows`. Other names are only lowercased.
pub fn normalize_os(value: &str) -> String {
    let lower = value.to_ascii_lowercase();
    match lower.as_str() {
        "mac" | "darwin" | "osx" | "macosx" => "macos".to_string(),
        "win" | "win32" | "win64" => "windows".to_string(),
        _ => lower,
    }
}

/// Canonical CPU architecture: lowercase, with `amd64`, `x64`, `x86-64` folded into `x86_64`,
/// `arm64` into `aarch64` and `i386`, `i686` into `x86`. Other names are only lowercased.
pub fn normalize_arch(value: &str) -> String {
    let lower = value.to_ascii_lowercase();
    match lower.as_str() {
        "amd64" | "x64" | "x86-64" => "x86_64".to_string(),
        "arm64" => "aarch64".to_string(),
        "i386" | "i686" => "x86".to_string(),
        _ => lower,
    }
}

/// The placement constraints of one task. An empty `Placement` lets any worker run the task.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct Placement {
    /// Canonical OS ([`normalize_os`]), matched against the worker's `os:` tag.
    pub os: Option<String>,
    /// Canonical architecture ([`normalize_arch`]), matched against the worker's `arch:` tag.
    pub arch: Option<String>,
    /// Worker UUID, or worker name (compared ignoring ASCII case).
    pub worker: Option<String>,
    /// User tags the worker must all carry, sorted and without duplicates.
    pub tags: Vec<String>,
}

impl Placement {
    /// True when the task has no constraint.
    pub fn is_empty(&self) -> bool {
        self.os.is_none() && self.arch.is_none() && self.worker.is_none() && self.tags.is_empty()
    }

    /// Parses the `require:` entries of a task's tags; tags without that prefix are ignored.
    ///
    /// Rejects an entry that is not `require:<key>=<value>`, an unknown key, an empty value, a
    /// second `os`, `arch` or `worker`, and a `tag` that is not a valid user tag.
    pub fn from_task_tags(task_tags: &[String]) -> Result<Self, String> {
        let mut placement = Self::default();
        for tag in task_tags {
            let Some(constraint) = tag.strip_prefix(REQUIRE_PREFIX) else {
                continue;
            };
            let Some((key, value)) = constraint.split_once('=') else {
                return Err(format!(
                    "placement constraint `{tag}` is not of the form require:<key>=<value>"
                ));
            };
            if value.is_empty() {
                return Err(format!("placement constraint `{tag}` has an empty value"));
            }
            let slot = match key {
                "os" => &mut placement.os,
                "arch" => &mut placement.arch,
                "worker" => &mut placement.worker,
                "tag" => {
                    validate_user_tag(value)
                        .map_err(|e| format!("placement constraint `{tag}`: {e}"))?;
                    placement.tags.push(value.to_string());
                    continue;
                }
                other => {
                    return Err(format!(
                        "unknown placement constraint `{other}` in `{tag}` (known: os, arch, worker, tag)"
                    ))
                }
            };
            if slot.is_some() {
                return Err(format!(
                    "placement constraint `{key}` is given more than once"
                ));
            }
            *slot = Some(match key {
                "os" => normalize_os(value),
                "arch" => normalize_arch(value),
                _ => value.to_string(),
            });
        }
        placement.tags.sort();
        placement.tags.dedup();
        Ok(placement)
    }

    /// The `require:` task tags that express these constraints ([`from_task_tags`] reads them
    /// back unchanged).
    ///
    /// [`from_task_tags`]: Self::from_task_tags
    pub fn to_task_tags(&self) -> Vec<String> {
        let mut tags = Vec::new();
        if let Some(os) = &self.os {
            tags.push(format!("{REQUIRE_PREFIX}os={os}"));
        }
        if let Some(arch) = &self.arch {
            tags.push(format!("{REQUIRE_PREFIX}arch={arch}"));
        }
        if let Some(worker) = &self.worker {
            tags.push(format!("{REQUIRE_PREFIX}worker={worker}"));
        }
        for tag in &self.tags {
            tags.push(format!("{REQUIRE_PREFIX}tag={tag}"));
        }
        tags
    }

    /// Whether a worker with this id, name and tags satisfies every constraint.
    pub fn matches(&self, worker_id: Uuid, worker_name: &str, worker_tags: &[String]) -> bool {
        let has_value = |prefix: &str, wanted: &str, normalize: fn(&str) -> String| {
            worker_tags
                .iter()
                .filter_map(|t| t.strip_prefix(prefix))
                .any(|v| normalize(v) == wanted)
        };
        if let Some(os) = &self.os {
            if !has_value(OS_TAG_PREFIX, os, normalize_os) {
                return false;
            }
        }
        if let Some(arch) = &self.arch {
            if !has_value(ARCH_TAG_PREFIX, arch, normalize_arch) {
                return false;
            }
        }
        if let Some(worker) = &self.worker {
            let by_id = worker.parse::<Uuid>().is_ok_and(|id| id == worker_id);
            if !by_id && !worker_name.eq_ignore_ascii_case(worker) {
                return false;
            }
        }
        self.tags.iter().all(|wanted| worker_tags.contains(wanted))
    }

    /// [`matches`](Self::matches) for a worker as `ListWorkers` reports it: its id comes from the
    /// `id:` tag the master adds (a master without it can only be matched by name).
    pub fn matches_listed(&self, caps: &WorkerCapabilities) -> bool {
        let id = listed_worker_id(caps).unwrap_or(Uuid::nil());
        self.matches(id, &caps.name, &caps.tags)
    }
}

impl fmt::Display for Placement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_empty() {
            return f.write_str("any worker");
        }
        let mut parts = Vec::new();
        if let Some(os) = &self.os {
            parts.push(format!("os={os}"));
        }
        if let Some(arch) = &self.arch {
            parts.push(format!("arch={arch}"));
        }
        if let Some(worker) = &self.worker {
            parts.push(format!("worker={worker}"));
        }
        for tag in &self.tags {
            parts.push(format!("tag={tag}"));
        }
        f.write_str(&parts.join(", "))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tags(items: &[&str]) -> Vec<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn test_system_tags_name_this_host() {
        let sys = system_tags();
        assert_eq!(sys.len(), 2);
        assert_eq!(sys[0], format!("os:{}", std::env::consts::OS));
        assert_eq!(sys[1], format!("arch:{}", std::env::consts::ARCH));
        for tag in &sys {
            // System tags are exactly what user tags may not be.
            assert!(validate_user_tag(tag).is_err(), "{tag}");
        }
    }

    #[test]
    fn test_user_tag_rules() {
        for ok in [
            "gpu-box",
            "laptop",
            "zone:home",
            "a",
            &"x".repeat(MAX_USER_TAG_LEN),
        ] {
            assert_eq!(validate_user_tag(ok), Ok(()), "{ok}");
        }
        let too_long = "x".repeat(MAX_USER_TAG_LEN + 1);
        for bad in [
            "",
            too_long.as_str(),
            "two words",
            "tab\there",
            "k=v",
            "a,b",
            "os:macos",
            "arch:x86_64",
            "link:TCP/LAN",
            "id:123",
            "state:busy",
        ] {
            assert!(validate_user_tag(bad).is_err(), "{bad:?} must be refused");
        }
    }

    #[test]
    fn test_parse_all_keys_and_ignore_other_tags() {
        let p = Placement::from_task_tags(&tags(&[
            "batch-7",
            "require:os=Darwin",
            "require:arch=ARM64",
            "require:worker=Mac-Air",
            "require:tag=laptop",
            "require:tag=fast",
            "require:tag=laptop",
        ]))
        .unwrap();
        assert_eq!(p.os.as_deref(), Some("macos"));
        assert_eq!(p.arch.as_deref(), Some("aarch64"));
        assert_eq!(p.worker.as_deref(), Some("Mac-Air"));
        assert_eq!(
            p.tags,
            tags(&["fast", "laptop"]),
            "sorted, duplicates dropped"
        );
        assert!(!p.is_empty());
        assert_eq!(
            p.to_string(),
            "os=macos, arch=aarch64, worker=Mac-Air, tag=fast, tag=laptop"
        );
    }

    #[test]
    fn test_no_require_tags_is_empty() {
        let p = Placement::from_task_tags(&tags(&["anything", "requires:os=x"])).unwrap();
        assert!(p.is_empty());
        assert_eq!(p.to_string(), "any worker");
        assert!(p.to_task_tags().is_empty());
    }

    #[test]
    fn test_aliases() {
        for (raw, os) in [
            ("mac", "macos"),
            ("macOS", "macos"),
            ("osx", "macos"),
            ("Win", "windows"),
            ("win64", "windows"),
            ("Linux", "linux"),
            ("android", "android"),
        ] {
            assert_eq!(normalize_os(raw), os, "{raw}");
        }
        for (raw, arch) in [
            ("amd64", "x86_64"),
            ("X64", "x86_64"),
            ("x86-64", "x86_64"),
            ("arm64", "aarch64"),
            ("AArch64", "aarch64"),
            ("i686", "x86"),
            ("riscv64", "riscv64"),
        ] {
            assert_eq!(normalize_arch(raw), arch, "{raw}");
        }
    }

    #[test]
    fn test_parse_rejections() {
        for (bad, needle) in [
            ("require:os", "not of the form"),
            ("require:os=", "empty value"),
            (
                "require:gpu_vendor=nvidia",
                "unknown placement constraint `gpu_vendor`",
            ),
            ("require:=x", "unknown placement constraint ``"),
            ("require:tag=os:macos", "only the worker or the master"),
            ("require:tag=a b", "whitespace"),
        ] {
            let err = Placement::from_task_tags(&tags(&[bad])).unwrap_err();
            assert!(err.contains(needle), "{bad}: {err}");
        }
        for key in ["os", "arch", "worker"] {
            let err = Placement::from_task_tags(&tags(&[
                &format!("require:{key}=a"),
                &format!("require:{key}=b"),
            ]))
            .unwrap_err();
            assert!(err.contains("more than once"), "{key}: {err}");
        }
    }

    #[test]
    fn test_task_tags_round_trip() {
        let p = Placement {
            os: Some("windows".into()),
            arch: Some("x86_64".into()),
            worker: Some("DELL-VOSTRO".into()),
            tags: tags(&["cpu", "office"]),
        };
        assert_eq!(Placement::from_task_tags(&p.to_task_tags()).unwrap(), p);
    }

    #[test]
    fn test_matches() {
        let id = Uuid::new_v4();
        let mac = tags(&["os:macos", "arch:aarch64", "laptop"]);
        let any = Placement::default();
        assert!(
            any.matches(id, "m", &[]),
            "no constraint matches everything"
        );

        let os = Placement::from_task_tags(&tags(&["require:os=mac"])).unwrap();
        assert!(os.matches(id, "m", &mac));
        assert!(!os.matches(id, "m", &tags(&["os:windows"])));
        assert!(
            !os.matches(id, "m", &[]),
            "a worker without an os tag never matches"
        );
        assert!(
            os.matches(id, "m", &tags(&["os:MacOS"])),
            "worker values are normalized too"
        );

        let both =
            Placement::from_task_tags(&tags(&["require:os=macos", "require:arch=x86_64"])).unwrap();
        assert!(!both.matches(id, "m", &mac), "every constraint must hold");

        let by_tag =
            Placement::from_task_tags(&tags(&["require:tag=laptop", "require:tag=gpu"])).unwrap();
        assert!(!by_tag.matches(id, "m", &mac));
        assert!(by_tag.matches(id, "m", &tags(&["laptop", "gpu"])));
    }

    #[test]
    fn test_matches_worker_by_id_or_name() {
        let id = Uuid::new_v4();
        let other = Uuid::new_v4();
        let by_id = Placement::from_task_tags(&tags(&[&format!("require:worker={id}")])).unwrap();
        assert!(by_id.matches(id, "anything", &[]));
        assert!(!by_id.matches(other, "anything", &[]));

        let by_name = Placement::from_task_tags(&tags(&["require:worker=Dell-Vostro"])).unwrap();
        assert!(
            by_name.matches(other, "DELL-VOSTRO", &[]),
            "names ignore ASCII case"
        );
        assert!(!by_name.matches(other, "dell-vostro-2", &[]));
    }

    #[test]
    fn test_matches_listed_uses_the_id_tag() {
        let id = Uuid::new_v4();
        let mut caps = WorkerCapabilities::new("MAC", 8, 16_384, true, false, None)
            .with_tags(tags(&["os:macos", "link:TCP/LAN"]));
        let pinned = Placement::from_task_tags(&tags(&[&format!("require:worker={id}")])).unwrap();
        assert_eq!(listed_worker_id(&caps), None);
        assert!(
            !pinned.matches_listed(&caps),
            "no id tag: only a name could match"
        );

        caps.tags.push(format!("id:{id}"));
        assert_eq!(listed_worker_id(&caps), Some(id));
        assert!(pinned.matches_listed(&caps));
        assert_eq!(tag_value(&caps.tags, OS_TAG_PREFIX), Some("macos"));
    }
}
