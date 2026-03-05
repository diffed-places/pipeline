#![no_main]

use diffed_places_pipeline::fuzz_process_geojson;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    fuzz_process_geojson(data);
});
