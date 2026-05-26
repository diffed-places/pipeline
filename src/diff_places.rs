use crate::places::{Place, PlaceIndex};
use crate::s2_util::MergedCellRanges;
use crate::{MatchMask, make_progress_bar, match_distance};
use anyhow::Result;
use indicatif::MultiProgress;
use rayon::prelude::*;
use s2::{cap::Cap, cell::Cell, cellid::CellID, point::Point, region::RegionCoverer};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

pub fn diff_places(
    _coverage: &Path,
    atp: &Path,
    osm: &Path,
    progress: &MultiProgress,
    workdir: &Path,
) -> Result<PathBuf> {
    assert!(workdir.exists());

    let out_path = workdir.join("diff.parquet");
    if out_path.exists() {
        return Ok(out_path);
    }

    // TODO: Open out_path for writing.

    let coverer = RegionCoverer {
        max_cells: 16,
        min_level: 12,
        max_level: s2::cellid::MAX_LEVEL as u8,
        level_mod: 1,
    };
    let atp_places = PlaceIndex::open(atp, 1)?;
    let num_atp_places = atp_places.total_rows() as u64;
    let progress_bar = make_progress_bar(progress, "diff     ", num_atp_places, "features");
    let osm_places = PlaceIndex::open(osm, 32)?;

    let num_atp_features = AtomicU64::new(0);
    let num_candidates = AtomicU64::new(0);

    for group in atp_places.scan_row_groups() {
        // Each group is processed by the Rayon thread pool in parallel,
        // but the outer loop is sequential — so nearby places (within a
        // group) always go to nearby workers, preserving spatial locality.
        group?.par_iter().try_for_each(|place| {
            progress_bar.inc(1);
            if let Some(matcher) = create_matcher(place) {
                num_atp_features.fetch_add(1, Ordering::Relaxed);
                let s2_cell = Cell::from(CellID(place.s2_cell_id));
                let center = s2_cell.center();
                let radius = match_distance(&place.mask);
                let cap = Cap::from_center_chordangle(&center, &radius);
                let covering = coverer.covering(&cap);
                let mut best_candidate: Option<Place> = None;
                let mut best_score: f64 = 0.0;
                for (lo, hi) in MergedCellRanges::new(covering) {
                    let mut iter = osm_places.query(lo..=hi, place.mask)?;
                    let mut bc: Option<&Place> = None;
                    for candidate in &mut iter {
                        num_candidates.fetch_add(1, Ordering::Relaxed);
                        let candidate = candidate?;
                        let score = matcher.score(candidate);
                        if score > best_score {
                            bc = Some(candidate);
                            best_score = score;
                        }
                    }
                    if let Some(b) = bc {
                        best_candidate = Some(b.deep_clone());
                    }
                }
                if let Some(best_candidate) = best_candidate
                    && false
                {
                    println!(
                        "score={} place={:?} best_candidate={:?}",
                        best_score, place, best_candidate
                    );
                }
            };
            Ok::<(), anyhow::Error>(())
        })?;
    }

    let cache_stats = osm_places.cache_stats();
    progress_bar.finish();
    println!(
        "  features: {} candidates: {}",
        num_atp_features.load(Ordering::SeqCst),
        num_candidates.load(Ordering::SeqCst)
    );
    println!(
        "  cache hits: {} misses: {} hit rate: {:.1}%",
        cache_stats.hits,
        cache_stats.misses,
        cache_stats.hit_rate().unwrap_or(0.0) * 100.0
    );

    Ok(out_path)
}

// TODO: Figure out how to best model matchers for different types, such as trees vs shops.
fn distance(pt: &Point, place: &Place) -> f64 {
    let pt2 = Point(CellID(place.s2_cell_id).raw_point().normalize());
    pt.distance(&pt2).rad() * 6_371_000.0
}

fn distance_score(pt: &Point, place: &Place, max_meters: f64) -> f64 {
    let dist = distance(pt, place);
    if dist <= max_meters {
        (max_meters - dist) / max_meters
    } else {
        0.0
    }
}

/// Trait for objects that can score a `Place`.
pub trait Matcher {
    /// Returns a score between 0.0 and 1.0 indicating how well the place matches.
    fn score(&self, place: &Place) -> f64;
}

pub fn create_matcher(place: &Place) -> Option<Box<dyn Matcher + '_>> {
    if place.mask.intersects(&MatchMask::SHOP) {
        Some(Box::new(ShopMatcher::new(place)))
    } else {
        None
    }
}

pub struct ShopMatcher {
    center: Point,
}

impl ShopMatcher {
    fn new(place: &Place) -> ShopMatcher {
        let center = Cell::from(CellID(place.s2_cell_id)).center();
        ShopMatcher { center }
    }
}

impl Matcher for ShopMatcher {
    fn score(&self, p: &Place) -> f64 {
        distance_score(&self.center, p, 2000.0)
    }
}
