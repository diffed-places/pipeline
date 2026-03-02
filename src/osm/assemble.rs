use super::filter::{Node, Way, filtered_file::FilteredFile};
use super::make_progress_bar;
use anyhow::{Ok, Result};
use ext_sort::{ExternalSorter, ExternalSorterBuilder, buffer::mem::MemoryLimitedBufferBuilder};
use indicatif::{MultiProgress, ProgressBar};
use rayon::prelude::*;
use rmp_serde::Deserializer;
use serde::Deserialize;
use std::fs::rename;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
use std::thread;

use crate::place::{ParquetWriter, Place};

pub fn assemble(
    nodes: &FilteredFile,
    ways: &FilteredFile,
    relations: &FilteredFile,
    progress: &MultiProgress,
    workdir: &Path,
    out_path: &Path,
) -> Result<()> {
    let (tx, rx) = sync_channel::<Place>(50_000);
    let feature_count = nodes.feature_count() as u64
        + ways.feature_count() as u64
        + relations.feature_count() as u64;
    let progress_bar = make_progress_bar(progress, "osm.assemble.r", feature_count, "features");
    thread::scope(|s| {
        let node_handler = {
            let tx = tx.clone();
            s.spawn(|| assemble_nodes(nodes, &progress_bar, tx))
        };
        let way_handler = {
            let tx = tx.clone();
            s.spawn(|| assemble_ways(nodes, ways, &progress_bar, tx))
        };
        let writer = s.spawn(|| write_places(rx, progress, workdir, out_path));
        node_handler
            .join()
            .expect("panic in node_handler")
            .and(way_handler.join().expect("panic in way_handler"))?;
        drop(tx); // Done writing. Close channel, so writer can terminate.
        writer.join().expect("panic in writer")
    })?;
    progress_bar.finish();
    Ok(())
}

fn assemble_nodes(
    nodes: &FilteredFile,
    progress_bar: &ProgressBar,
    out: SyncSender<Place>,
) -> Result<()> {
    (0..nodes.feature_count())
        .into_par_iter()
        .try_for_each(|i| {
            if let Some(data) = nodes.feature_data(i) {
                let mut d = Deserializer::new(Cursor::new(data));
                if let std::result::Result::Ok(node) = Node::deserialize(&mut d) {
                    let point =
                        geo::Point::new(node.lon_e7 as f64 * 1e-7, node.lat_e7 as f64 * 1e-7);
                    let source = String::from("osm/nodes");
                    let tags = node
                        .tags
                        .chunks_exact(2)
                        .map(|c| (c[0].clone(), c[1].clone()))
                        .collect();
                    if let Some(place) = Place::new(&point, source, tags) {
                        out.send(place)?;
                        progress_bar.inc(1);
                    }
                };
            };
            Ok(())
        })
}

fn assemble_ways(
    nodes: &FilteredFile,
    ways: &FilteredFile,
    progress_bar: &ProgressBar,
    _out: SyncSender<Place>,
) -> Result<()> {
    (0..ways.feature_count())
        .into_par_iter()
        .try_for_each(|i| {
            if let Some(data) = ways.feature_data(i) {
                let mut d = Deserializer::new(Cursor::new(data));
                if let std::result::Result::Ok(way) = Way::deserialize(&mut d) {
                    let mut coords = Vec::<geo::Point>::with_capacity(way.nodes.len());
                    for node_id in way.nodes.iter() {
                        if let Some(c) = nodes.get_coords(*node_id) {
                            coords.push(c);
                        }
                    }
                    if coords.len() != way.nodes.len() {
                        println!(
                            "*** Missing nodes in way/{:?}, only got {:?} of {:?}, {:?}",
                            way.id,
                            coords.len(),
                            way.nodes.len(),
                            way.nodes
                        );
                    }
                    progress_bar.inc(1);
                    // TODO: Implement.
                }
            }
            Ok(())
        })?;
    Ok(())
}

fn write_places(
    places: Receiver<Place>,
    progress: &MultiProgress,
    workdir: &Path,
    out: &Path,
) -> Result<()> {
    let mut tmp = PathBuf::from(out);
    tmp.add_extension("tmp");

    let mut writer = ParquetWriter::try_new(/* batch size */ 64 * 1024, &tmp)?;
    let sorter: ExternalSorter<Place, std::io::Error, MemoryLimitedBufferBuilder> =
        ExternalSorterBuilder::new()
            .with_tmp_dir(workdir)
            .with_buffer(MemoryLimitedBufferBuilder::new(150_000_000))
            .build()?;

    let feature_count = AtomicU64::new(0);
    let sorted = sorter.sort(places.iter().map(|x| {
        feature_count.fetch_add(1, Ordering::SeqCst);
        std::io::Result::Ok(x)
    }))?;
    let feature_count = feature_count.load(Ordering::SeqCst);

    let progress_bar = make_progress_bar(progress, "osm.assemble.w", feature_count, "features");
    for place in sorted {
        writer.write(place?)?;
        progress_bar.inc(1);
    }
    writer.close()?;
    rename(&tmp, out)?;

    progress_bar.finish_with_message(format!("{} features", feature_count));
    Ok(())
}
