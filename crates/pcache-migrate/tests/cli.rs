use std::process::Command;

#[test]
fn help_describes_stdin_import_and_replace_guard() {
    let output = Command::new(env!("CARGO_BIN_EXE_hath-rs-pcache-migrate"))
        .arg("--help")
        .output()
        .unwrap();

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("HPCACHE/1 from standard input"));
    assert!(stdout.contains("--data-dir"));
    assert!(stdout.contains("--replace"));
}
