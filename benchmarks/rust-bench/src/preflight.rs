use std::fs;

/// Check OS-level limits and print warnings if they might bottleneck.
pub fn check_os_limits(max_connections: usize) {
    println!("\n  Preflight checks:");

    // File descriptor limit
    #[cfg(unix)]
    {
        let fd_limit = rlimit_nofile();
        let needed = max_connections * 2 + 1000; // client + server FDs + headroom
        if fd_limit < needed as u64 {
            println!(
                "    [WARN] ulimit -n = {} (need ~{} for {} connections)",
                fd_limit, needed, max_connections
            );
            println!("           Run: ulimit -n {}", needed);
        } else {
            println!("    [OK] ulimit -n = {} (need ~{})", fd_limit, needed);
        }
    }

    // Port range
    if let Some((lo, hi)) = read_port_range() {
        let available = hi - lo;
        let per_ip = available as usize;
        let ips_needed = max_connections.div_ceil(per_ip);
        println!(
            "    [OK] port range {}-{} ({} ports/IP, {} source IPs needed for {})",
            lo, hi, available, ips_needed, max_connections
        );
    }

    // somaxconn
    if let Some(val) = read_sysctl_u64("/proc/sys/net/core/somaxconn") {
        if val < 65535 {
            println!("    [WARN] somaxconn = {} (recommend 65535)", val);
        } else {
            println!("    [OK] somaxconn = {}", val);
        }
    }

    // tcp_tw_reuse
    if let Some(val) = read_sysctl_u64("/proc/sys/net/ipv4/tcp_tw_reuse") {
        if val == 0 {
            println!("    [WARN] tcp_tw_reuse = 0 (recommend 1)");
        } else {
            println!("    [OK] tcp_tw_reuse = {}", val);
        }
    }

    println!();
}

#[cfg(unix)]
fn rlimit_nofile() -> u64 {
    use std::io;
    let mut rlim = libc_rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: getrlimit is a standard POSIX call
    let ret = unsafe { libc_getrlimit(RLIMIT_NOFILE, &mut rlim) };
    if ret == 0 {
        rlim.rlim_cur
    } else {
        let _ = io::stderr();
        0
    }
}

#[cfg(unix)]
const RLIMIT_NOFILE: i32 = {
    #[cfg(target_os = "linux")]
    {
        7
    }
    #[cfg(target_os = "macos")]
    {
        8
    }
};

#[cfg(unix)]
#[repr(C)]
struct libc_rlimit {
    rlim_cur: u64,
    rlim_max: u64,
}

#[cfg(unix)]
extern "C" {
    fn getrlimit(resource: i32, rlim: *mut libc_rlimit) -> i32;
}

#[cfg(unix)]
unsafe fn libc_getrlimit(resource: i32, rlim: *mut libc_rlimit) -> i32 {
    unsafe { getrlimit(resource, rlim) }
}

fn read_port_range() -> Option<(u32, u32)> {
    let content = fs::read_to_string("/proc/sys/net/ipv4/ip_local_port_range").ok()?;
    let parts: Vec<&str> = content.split_whitespace().collect();
    if parts.len() == 2 {
        let lo = parts[0].parse().ok()?;
        let hi = parts[1].parse().ok()?;
        Some((lo, hi))
    } else {
        None
    }
}

fn read_sysctl_u64(path: &str) -> Option<u64> {
    fs::read_to_string(path).ok()?.trim().parse().ok()
}
