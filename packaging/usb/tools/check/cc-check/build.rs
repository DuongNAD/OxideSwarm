use std::env;
use std::path::PathBuf;
use std::process::Command;

fn main() {
    println!("cargo:rerun-if-changed=c/add.c");
    let out_dir = PathBuf::from(env::var_os("OUT_DIR").expect("OUT_DIR set by Cargo"));
    let obj_path = out_dir.join("add.o");
    let lib_path = out_dir.join("libadd.a");

    let cc = env::var_os("CC").unwrap_or_else(|| "gcc".into());
    let status = Command::new(&cc)
        .args(["-O2", "-c", "c/add.c", "-o"])
        .arg(&obj_path)
        .status()
        .unwrap_or_else(|e| panic!("failed to run C compiler {:?}: {e}", cc));
    assert!(status.success(), "C compiler {:?} exited with {status}", cc);

    let ar = env::var_os("AR").unwrap_or_else(|| "ar".into());
    let ar_status = Command::new(&ar)
        .args(["rcs"])
        .arg(&lib_path)
        .arg(&obj_path)
        .status()
        .unwrap_or_else(|e| panic!("failed to run archiver {:?}: {e}", ar));
    assert!(ar_status.success(), "archiver {:?} exited with {ar_status}", ar);

    println!("cargo:rustc-link-search=native={}", out_dir.display());
    println!("cargo:rustc-link-lib=static=add");
}
