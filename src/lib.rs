mod atp;
mod coverage;
mod osm;
mod place;
mod u64_table;

// Re-exported for main.rs.
pub use atp::import_atp;
pub use coverage::build_coverage;
pub use osm::import_osm;

use indicatif::{MultiProgress, ProgressBar, ProgressStyle};

#[cfg(fuzzing)]
pub use import_atp::fuzz::fuzz_process_geojson;

const PROGRESS_BAR_STYLE: &str =
    "{prefix} [{elapsed_precise}] {bar:42.blue} {pos:>7}/{len:7} {msg}";

const DOWNLOAD_BAR_STYLE: &str = "{prefix} [{elapsed_precise}] {bar:42.blue} {bytes_per_sec} {eta}";

const SPINNER_STYLE: &str = "{prefix} [{elapsed_precise}] {spinner:blue} {msg} {bytes_per_sec}";

fn make_progress_bar(
    progress: &MultiProgress,
    phase: &str,
    max_value: u64,
    message: &str,
) -> ProgressBar {
    let bar = progress.add(ProgressBar::new(max_value));
    bar.set_prefix(String::from(phase));
    bar.set_message(String::from(message));
    let style = ProgressStyle::with_template(PROGRESS_BAR_STYLE).expect("bad PROGRESS_BAR_STYLE");
    bar.set_style(style);
    bar
}

fn make_download_bar(
    progress: &MultiProgress,
    phase: &str,
    content_length: Option<u64>,
) -> ProgressBar {
    if let Some(content_length) = content_length {
        let bar = progress.add(ProgressBar::new(content_length));
        bar.set_prefix(String::from(phase));
        let style =
            ProgressStyle::with_template(DOWNLOAD_BAR_STYLE).expect("bad DOWNLOAD_BAR_STYLE");
        bar.set_style(style);
        bar
    } else {
        let bar = progress.add(ProgressBar::new_spinner());
        bar.set_prefix(String::from(phase));
        let style = ProgressStyle::with_template(SPINNER_STYLE).expect("bad SPINNER_STYLE");
        bar.set_style(style);
        bar
    }
}
