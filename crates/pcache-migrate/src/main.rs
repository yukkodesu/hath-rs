use clap::Parser;
use std::path::PathBuf;

#[derive(Parser)]
#[command(
    name = "hath-rs-pcache-migrate",
    about = "Import a Java H@H persistent cache as HPCACHE/1 from standard input"
)]
struct Args {
    #[arg(long, value_name = "DIR")]
    data_dir: PathBuf,
    #[arg(long, help = "Back up and replace existing pcache_* files in data-dir")]
    replace: bool,
}

fn main() {
    let args = Args::parse();
    if let Err(error) = hath_rs_pcache_migrate::import_from_reader(
        std::io::stdin().lock(),
        &args.data_dir,
        args.replace,
    ) {
        eprintln!("Migration failed: {error}");
        std::process::exit(1);
    }
}
