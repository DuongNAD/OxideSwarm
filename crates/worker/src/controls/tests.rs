use super::*;
use chrono::{NaiveDateTime, NaiveTime, TimeZone};
use std::sync::atomic::AtomicI64;

struct FakeClock {
    epoch_secs: AtomicI64,
}

impl FakeClock {
    fn from_local(year: i32, month: u32, day: u32, hour: u32, min: u32) -> Arc<Self> {
        let ndt = NaiveDateTime::new(
            NaiveDate::from_ymd_opt(year, month, day).unwrap(),
            NaiveTime::from_hms_opt(hour, min, 0).unwrap(),
        );
        let dt = Local.from_local_datetime(&ndt).single().unwrap();
        Arc::new(Self {
            epoch_secs: AtomicI64::new(dt.timestamp()),
        })
    }

    fn set_local(&self, year: i32, month: u32, day: u32, hour: u32, min: u32) {
        let ndt = NaiveDateTime::new(
            NaiveDate::from_ymd_opt(year, month, day).unwrap(),
            NaiveTime::from_hms_opt(hour, min, 0).unwrap(),
        );
        let dt = Local.from_local_datetime(&ndt).single().unwrap();
        self.epoch_secs.store(dt.timestamp(), Ordering::Release);
    }
}

impl Clock for FakeClock {
    fn local_now(&self) -> chrono::DateTime<Local> {
        let secs = self.epoch_secs.load(Ordering::Acquire);
        Local.timestamp_opt(secs, 0).single().unwrap()
    }
}

struct FakePowerProbe(AtomicU8);

impl FakePowerProbe {
    fn new(src: PowerSource) -> Arc<Self> {
        Arc::new(Self(AtomicU8::new(src as u8)))
    }
    fn set(&self, src: PowerSource) {
        self.0.store(src as u8, Ordering::Release);
    }
}

impl PowerProbe for FakePowerProbe {
    fn power_source(&self) -> PowerSource {
        PowerSource::from_u8(self.0.load(Ordering::Acquire))
    }
}

struct FakeNetworkProbe(AtomicU8);

impl FakeNetworkProbe {
    fn new(cost: NetworkCost) -> Arc<Self> {
        Arc::new(Self(AtomicU8::new(cost as u8)))
    }
    fn set(&self, cost: NetworkCost) {
        self.0.store(cost as u8, Ordering::Release);
    }
}

impl NetworkCostProbe for FakeNetworkProbe {
    fn network_cost(&self) -> NetworkCost {
        NetworkCost::from_u8(self.0.load(Ordering::Acquire))
    }
}

fn local_dt(year: i32, month: u32, day: u32, hour: u32, min: u32) -> chrono::DateTime<Local> {
    let ndt = NaiveDateTime::new(
        NaiveDate::from_ymd_opt(year, month, day).unwrap(),
        NaiveTime::from_hms_opt(hour, min, 0).unwrap(),
    );
    Local.from_local_datetime(&ndt).single().unwrap()
}

#[test]
fn test_allow_hours_schedule_parsing_and_overnight_rollover() {
    // 2026-11-19 is Thursday; 2026-11-20 is Friday; 2026-11-21 is Saturday; 2026-11-22 is Sunday.
    let sched = AllowHoursSchedule::parse("Mon-Fri 22:00-07:00, Sat-Sun 00:00-24:00").unwrap();
    assert!(!sched.contains(local_dt(2026, 11, 19, 15, 0))); // Thu afternoon
    assert!(sched.contains(local_dt(2026, 11, 19, 22, 0))); // Thu 22:00 inclusive
    assert!(sched.contains(local_dt(2026, 11, 20, 6, 59))); // Fri morning rollover from Thu
    assert!(!sched.contains(local_dt(2026, 11, 20, 7, 0))); // Fri 07:00 exclusive
    assert!(sched.contains(local_dt(2026, 11, 21, 14, 0))); // Sat all day
    assert!(sched.contains(local_dt(2026, 11, 22, 23, 59))); // Sun 23:59

    // Wrap-around day range + comma-separated list + semicolon clause.
    let wrap = AllowHoursSchedule::parse("Fri-Mon 09:00-17:00; Wed 18:00-20:00").unwrap();
    assert!(wrap.contains(local_dt(2026, 11, 20, 10, 0))); // Fri
    assert!(wrap.contains(local_dt(2026, 11, 21, 10, 0))); // Sat
    assert!(wrap.contains(local_dt(2026, 11, 22, 10, 0))); // Sun
    assert!(wrap.contains(local_dt(2026, 11, 23, 10, 0))); // Mon
    assert!(!wrap.contains(local_dt(2026, 11, 24, 10, 0))); // Tue
    assert!(wrap.contains(local_dt(2026, 11, 18, 19, 0))); // Wed
}

#[test]
fn test_validation_rejects_invalid_controls() {
    for bad_sched in [
        "",
        "   ",
        "09:00-17:00",
        "Mon",
        "Funday 09:00-17:00",
        "Mon 09:00-09:00",
        "Mon 24:00-02:00",
        "Mon 09:60-17:00",
        "Mon 09:5-17:00",
        "Mon 09:00-24:30",
    ] {
        assert!(
            AllowHoursSchedule::parse(bad_sched).is_err(),
            "should reject schedule {bad_sched:?}"
        );
    }

    assert!(validate_max_host_cpu_pct(0.5).is_err());
    assert!(validate_max_host_cpu_pct(101.0).is_err());
    assert!(validate_max_host_cpu_pct(f32::NAN).is_err());
    assert_eq!(validate_max_host_cpu_pct(70.0).unwrap(), 70.0);

    assert!(validate_daily_task_hours(0.0).is_err());
    assert!(validate_daily_task_hours(-2.0).is_err());
    assert!(validate_daily_task_hours(f64::INFINITY).is_err());
    assert_eq!(validate_daily_task_hours(4.5).unwrap(), 4.5);
}

#[test]
fn test_injected_probes_gate_all_controls_and_reset_daily_cap_at_midnight() {
    let clock = FakeClock::from_local(2026, 11, 19, 10, 0);
    let cpu = Arc::new(AtomicHostCpuProbe::new(20.0));
    let power = FakePowerProbe::new(PowerSource::Ac);
    let net = FakeNetworkProbe::new(NetworkCost::Unmetered);

    let cfg = ContributorControlsConfig {
        max_host_cpu_pct: Some(60.0),
        allow_hours: Some(AllowHoursSchedule::parse("Daily 09:00-18:00").unwrap()),
        daily_task_hours: Some(1.0),
        pause_on_battery: true,
        pause_on_metered: true,
        control_file: None,
    };

    let controls = WorkerControls::with_probes(
        cfg,
        Arc::clone(&clock) as Arc<dyn Clock>,
        Arc::clone(&cpu) as Arc<dyn HostCpuProbe>,
        Arc::clone(&power) as Arc<dyn PowerProbe>,
        Arc::clone(&net) as Arc<dyn NetworkCostProbe>,
    );

    assert_eq!(controls.check_admission(), Ok(()));

    // 1. CPU cap
    cpu.set(75.0);
    assert!(matches!(
        controls.check_admission(),
        Err(BlockReason::HostCpu { .. })
    ));
    cpu.set(55.0);
    assert_eq!(controls.check_admission(), Ok(()));

    // 2. Weekly schedule
    clock.set_local(2026, 11, 19, 20, 0);
    assert!(matches!(
        controls.check_admission(),
        Err(BlockReason::OutsideHours { .. })
    ));
    clock.set_local(2026, 11, 19, 11, 0);
    assert_eq!(controls.check_admission(), Ok(()));

    // 3. Battery
    power.set(PowerSource::Battery);
    assert_eq!(controls.check_admission(), Err(BlockReason::OnBattery));
    power.set(PowerSource::Ac);
    assert_eq!(controls.check_admission(), Ok(()));

    // 4. Metered network
    net.set(NetworkCost::Metered);
    assert_eq!(controls.check_admission(), Err(BlockReason::MeteredNetwork));
    net.set(NetworkCost::Unmetered);
    assert_eq!(controls.check_admission(), Ok(()));

    // 5. Daily task hours cap + midnight reset
    controls.record_completed_task(Duration::from_secs(3600));
    assert!(matches!(
        controls.check_admission(),
        Err(BlockReason::DailyHoursLimit { .. })
    ));
    clock.set_local(2026, 11, 20, 10, 0);
    assert_eq!(controls.check_admission(), Ok(()));
    assert_eq!(controls.used_task_hours_today(), 0.0);

    // 6. Pause overrides everything
    controls.pause();
    assert_eq!(controls.check_admission(), Err(BlockReason::Paused));
    assert!(controls
        .check_admission()
        .unwrap_err()
        .refusal_message()
        .starts_with(rusty_grid_core::CONTRIBUTOR_CONTROL_REFUSAL_PREFIX));
    controls.resume();
    assert_eq!(controls.check_admission(), Ok(()));
}

#[test]
fn test_pause_file_roundtrip_permissions_and_symlink_refusal() {
    let dir = tempfile::tempdir().unwrap();
    let pause_path = dir.path().join("state").join("worker_pause.w1");

    assert!(!is_pause_file_present(&pause_path).unwrap());
    write_pause_file(&pause_path).unwrap();
    assert!(is_pause_file_present(&pause_path).unwrap());

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let dir_mode = fs::metadata(pause_path.parent().unwrap())
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        let file_mode = fs::metadata(&pause_path).unwrap().permissions().mode() & 0o777;
        assert_eq!(dir_mode, 0o700);
        assert_eq!(file_mode, 0o600);
    }

    assert!(remove_pause_file(&pause_path).unwrap());
    assert!(!remove_pause_file(&pause_path).unwrap());
    assert!(!is_pause_file_present(&pause_path).unwrap());

    #[cfg(unix)]
    {
        let target = dir.path().join("target.txt");
        fs::write(&target, b"hi").unwrap();
        let sym_path = dir.path().join("symlink_pause");
        std::os::unix::fs::symlink(&target, &sym_path).unwrap();
        assert!(write_pause_file(&sym_path).is_err());
        assert!(is_pause_file_present(&sym_path).is_err());
        assert!(remove_pause_file(&sym_path).is_err());
    }
}

#[test]
fn test_startup_diagnostics_warn_on_missing_battery_and_unsupported_metered() {
    let clock = FakeClock::from_local(2026, 11, 19, 10, 0);
    let cpu = Arc::new(AtomicHostCpuProbe::new(0.0));
    let power = FakePowerProbe::new(PowerSource::Unknown);
    let net = FakeNetworkProbe::new(NetworkCost::Unsupported);
    let controls = WorkerControls::with_probes(
        ContributorControlsConfig {
            pause_on_battery: true,
            pause_on_metered: true,
            ..Default::default()
        },
        clock,
        cpu,
        power,
        net,
    );
    let notices = controls.startup_diagnostics();
    assert_eq!(notices.len(), 2);
    assert!(notices[0].contains("--pause-on-battery"));
    assert!(notices[1].contains("--pause-on-metered"));
}

#[test]
fn test_os_metered_and_power_parsers() {
    let scutil_wifi = "\
Network information

IPv4 network interface information
     en0 : flags      : 0x7 (IPv4,IPv6,DNS)
           address    : 192.168.1.224
           reach      : 0x00000002 (Reachable)

   REACH : flags 0x00000002 (Reachable)
Network interfaces: en0
";
    assert_eq!(
        parse_macos_scutil_nwi(scutil_wifi),
        (false, vec!["en0".to_string()])
    );
    assert!(parse_macos_scutil_nwi("pdp_ip0 : flags : 0x7\nIsWWAN : TRUE\n").0);

    assert!(!parse_macos_ifconfig_v(
        "en0: flags=8863<UP,BROADCAST,SMART,RUNNING,SIMPLEX,MULTICAST> mtu 1500\n\teflags=40<TXSTART>"
    ));
    assert!(parse_macos_ifconfig_v(
        "en0: flags=8863<UP,RUNNING>\n\teflags=2000000<EXPENSIVE>\n\txflags=0<>"
    ));
    assert!(parse_macos_ifconfig_v("en0: xflags=100<CONSTRAINED>"));

    assert_eq!(parse_linux_nmcli_metered("yes\n"), NetworkCost::Metered);
    assert_eq!(
        parse_linux_nmcli_metered("yes (guessed)\n"),
        NetworkCost::Metered
    );
    assert_eq!(parse_linux_nmcli_metered("no\n"), NetworkCost::Unmetered);
    assert_eq!(
        parse_linux_nmcli_metered("no (guessed)\n"),
        NetworkCost::Unmetered
    );
    assert_eq!(
        parse_linux_nmcli_metered("unknown\n"),
        NetworkCost::Unsupported
    );

    assert_eq!(parse_linux_busctl_metered("u 1\n"), NetworkCost::Metered);
    assert_eq!(parse_linux_busctl_metered("u 3\n"), NetworkCost::Metered);
    assert_eq!(parse_linux_busctl_metered("u 2\n"), NetworkCost::Unmetered);
    assert_eq!(parse_linux_busctl_metered("u 4\n"), NetworkCost::Unmetered);
    assert_eq!(
        parse_linux_busctl_metered("u 0\n"),
        NetworkCost::Unsupported
    );

    assert_eq!(
        parse_windows_connection_cost("Metered\r\n"),
        NetworkCost::Metered
    );
    assert_eq!(
        parse_windows_connection_cost("Unmetered\r\n"),
        NetworkCost::Unmetered
    );
    assert_eq!(
        parse_windows_connection_cost("Unsupported\r\n"),
        NetworkCost::Unsupported
    );

    assert_eq!(parse_windows_power_status(1, 0), PowerSource::Ac);
    assert_eq!(parse_windows_power_status(0, 1), PowerSource::Battery);
    assert_eq!(parse_windows_power_status(0, 128), PowerSource::Unknown);
    assert_eq!(parse_windows_power_status(255, 255), PowerSource::Unknown);
    assert_eq!(
        resolve_windows_powershell_path(None),
        r"C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe"
    );
    assert_eq!(
        resolve_windows_powershell_path(Some(r"D:\Win\")),
        r"D:\Win\System32\WindowsPowerShell\v1.0\powershell.exe"
    );
}

#[cfg(unix)]
#[test]
fn test_pause_file_never_chmods_existing_parent_and_refuses_unsafe_parents_and_modes() {
    use std::os::unix::fs::PermissionsExt;

    let root = tempfile::tempdir().unwrap();

    // 1. Pre-existing 0o755 parent (like $HOME) keeps 0o755 — never chmodded to 0o700.
    let existing_0755 = root.path().join("existing_0755");
    fs::create_dir(&existing_0755).unwrap();
    fs::set_permissions(&existing_0755, fs::Permissions::from_mode(0o755)).unwrap();
    let pause_in_existing = existing_0755.join("worker_pause");
    write_pause_file(&pause_in_existing).unwrap();
    let mode_after = fs::metadata(&existing_0755).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode_after, 0o755, "existing parent must not be chmodded");
    assert!(is_pause_file_present(&pause_in_existing).unwrap());
    assert!(remove_pause_file(&pause_in_existing).unwrap());

    // 2. Missing nested subdir under 0o755 parent creates only new dirs at 0o700.
    let nested_pause = existing_0755.join("a").join("b").join("worker_pause");
    write_pause_file(&nested_pause).unwrap();
    assert_eq!(
        fs::metadata(existing_0755.join("a"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o700
    );
    assert_eq!(
        fs::metadata(existing_0755.join("a").join("b"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o700
    );
    assert_eq!(
        fs::metadata(&existing_0755).unwrap().permissions().mode() & 0o777,
        0o755
    );

    // 3. Direct unsafe parents (0o775, 0o777, 0o1777) are refused and their modes untouched.
    for (name, bad_mode) in [
        ("group_w", 0o775),
        ("world_w", 0o777),
        ("sticky_world_w", 0o1777),
    ] {
        let bad_dir = root.path().join(name);
        fs::create_dir(&bad_dir).unwrap();
        fs::set_permissions(&bad_dir, fs::Permissions::from_mode(bad_mode)).unwrap();
        let bad_pause = bad_dir.join("worker_pause");
        assert!(
            write_pause_file(&bad_pause).is_err(),
            "expected write_pause_file to refuse {name} ({bad_mode:o})"
        );
        assert!(
            is_pause_file_present(&bad_pause).is_err(),
            "expected is_pause_file_present to refuse {name} ({bad_mode:o})"
        );
        assert!(
            remove_pause_file(&bad_pause).is_err(),
            "expected remove_pause_file to refuse {name} ({bad_mode:o})"
        );
        assert_eq!(
            fs::symlink_metadata(&bad_dir).unwrap().permissions().mode() & 0o7777,
            bad_mode,
            "{name} permissions must stay unchanged"
        );
    }

    // 4. Symlink parent and missing subdir under un-sticky 0o777 ancestor are refused.
    let sym_parent = root.path().join("sym_parent");
    std::os::unix::fs::symlink(&existing_0755, &sym_parent).unwrap();
    assert!(write_pause_file(&sym_parent.join("worker_pause")).is_err());

    let unsticky = root.path().join("unsticky_777");
    fs::create_dir(&unsticky).unwrap();
    fs::set_permissions(&unsticky, fs::Permissions::from_mode(0o777)).unwrap();
    assert!(write_pause_file(&unsticky.join("sub").join("worker_pause")).is_err());

    // Sticky 1777 ancestor *can* create a fresh 0o700 subdir safely (matching sandbox.rs).
    let sticky = root.path().join("sticky_1777");
    fs::create_dir(&sticky).unwrap();
    fs::set_permissions(&sticky, fs::Permissions::from_mode(0o1777)).unwrap();
    let private_sub_pause = sticky.join("my_private").join("worker_pause");
    write_pause_file(&private_sub_pause).unwrap();
    assert!(is_pause_file_present(&private_sub_pause).unwrap());

    // 5. Group/world-writable pause file itself (0o666 / 0o620) is refused on read/write/remove.
    let private_dir = root.path().join("priv");
    fs::create_dir(&private_dir).unwrap();
    fs::set_permissions(&private_dir, fs::Permissions::from_mode(0o700)).unwrap();
    for bad_file_mode in [0o666, 0o620] {
        let p = private_dir.join(format!("p_{bad_file_mode:o}"));
        fs::write(&p, b"paused\n").unwrap();
        fs::set_permissions(&p, fs::Permissions::from_mode(bad_file_mode)).unwrap();
        assert!(is_pause_file_present(&p).is_err());
        assert!(write_pause_file(&p).is_err());
        assert!(remove_pause_file(&p).is_err());
    }
}

#[test]
fn test_watch_file_lifecycle_live_stale_and_named_path() {
    let dir = tempfile::tempdir().unwrap();
    let named = dir.path().join("worker_pause.gpu1");
    let expected_watch = dir.path().join("worker_pause.gpu1.watch");
    assert_eq!(watch_file_path(&named), expected_watch);

    assert_eq!(
        check_watcher_status(&named).unwrap(),
        WatcherStatus::NotWatching
    );

    write_watch_file(&named).unwrap();
    assert_eq!(
        check_watcher_status(&named).unwrap(),
        WatcherStatus::Watching {
            pid: std::process::id()
        }
    );

    // Removing with a different PID already recorded keeps the other worker's file.
    let other_pid = std::process::id().wrapping_add(500_000).max(2);
    write_private_control_file(&expected_watch, format!("{other_pid}\n").as_bytes()).unwrap();
    assert!(!remove_watch_file_if_owned(&named).unwrap());
    assert!(expected_watch.exists());

    // Spawn a trivial child and reap it so we have a guaranteed dead PID.
    #[cfg(unix)]
    let dead_pid = {
        let mut child = std::process::Command::new("true").spawn().unwrap();
        let pid = child.id();
        let _ = child.wait().unwrap();
        pid
    };
    #[cfg(windows)]
    let dead_pid = {
        let mut child = std::process::Command::new("cmd")
            .args(["/C", "exit", "0"])
            .spawn()
            .unwrap();
        let pid = child.id();
        let _ = child.wait().unwrap();
        pid
    };
    assert!(!is_process_alive(dead_pid));
    write_private_control_file(&expected_watch, format!("{dead_pid}\n").as_bytes()).unwrap();
    assert_eq!(
        check_watcher_status(&named).unwrap(),
        WatcherStatus::Stale { pid: dead_pid }
    );

    // Own PID removes cleanly.
    write_watch_file(&named).unwrap();
    assert!(remove_watch_file_if_owned(&named).unwrap());
    assert_eq!(
        check_watcher_status(&named).unwrap(),
        WatcherStatus::NotWatching
    );
}

#[test]
fn test_cached_probes_keep_last_known_state_on_failure_and_update_on_reading() {
    let net = CachedSystemNetworkCostProbe::new();
    assert_eq!(net.network_cost(), NetworkCost::Unsupported);
    net.apply_probe_reading(Some(NetworkCost::Metered));
    assert_eq!(net.network_cost(), NetworkCost::Metered);
    net.apply_probe_reading(None);
    assert_eq!(
        net.network_cost(),
        NetworkCost::Metered,
        "probe timeout/failure must retain Metered instead of failing open"
    );
    net.apply_probe_reading(Some(NetworkCost::Unmetered));
    assert_eq!(net.network_cost(), NetworkCost::Unmetered);
    net.apply_probe_reading(Some(NetworkCost::Unsupported));
    assert_eq!(net.network_cost(), NetworkCost::Unsupported);

    let power = CachedSystemPowerProbe::new(None, false);
    assert_eq!(power.power_source(), PowerSource::Unknown);
    power.apply_probe_reading(Some(PowerSource::Battery));
    assert_eq!(power.power_source(), PowerSource::Battery);
    power.apply_probe_reading(None);
    assert_eq!(
        power.power_source(),
        PowerSource::Battery,
        "battery probe failure must retain Battery"
    );
    power.apply_probe_reading(Some(PowerSource::Ac));
    assert_eq!(power.power_source(), PowerSource::Ac);
}

#[test]
fn test_allow_hours_next_transition_after_handles_entries_exits_and_overnight_windows() {
    // Overnight Mon-Fri 22:00-07:00
    let overnight = AllowHoursSchedule::parse("Mon-Fri 22:00-07:00").unwrap();
    // Thu 2026-11-19 21:30 -> enters in 30m at 22:00
    assert_eq!(
        overnight.next_transition_after(local_dt(2026, 11, 19, 21, 30)),
        Some(Duration::from_secs(30 * 60))
    );
    // Thu 2026-11-19 23:55 -> continuous across midnight to Fri 07:00 (7h 5m, NOT 5m)
    assert_eq!(
        overnight.next_transition_after(local_dt(2026, 11, 19, 23, 55)),
        Some(Duration::from_secs((7 * 60 + 5) * 60))
    );
    // Sat 2026-11-21 06:50 -> exits Sat morning rollover at 07:00 (10m)
    assert_eq!(
        overnight.next_transition_after(local_dt(2026, 11, 21, 6, 50)),
        Some(Duration::from_secs(10 * 60))
    );
    // Sat 2026-11-21 10:00 -> next entry is Mon 22:00 (60 hours later)
    assert_eq!(
        overnight.next_transition_after(local_dt(2026, 11, 21, 10, 0)),
        Some(Duration::from_secs(60 * 3600))
    );

    // Continuous full-week schedule has no transitions.
    let always = AllowHoursSchedule::parse("Daily 00:00-24:00").unwrap();
    assert_eq!(
        always.next_transition_after(local_dt(2026, 11, 19, 12, 0)),
        None
    );
}
