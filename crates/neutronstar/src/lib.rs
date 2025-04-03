use std::path;

use anyhow::{anyhow, bail};

pub mod engine;

pub static HAIKUS: [[&str; 3]; 10] = [
    [
        "Data’s gravity",
        "Collapses to a bright core—",
        "NeutronStar reveals",
    ],
    [
        "All queries converge",
        "On a single blazing node—",
        "Infinite insight",
    ],
    [
        "A single bright point",
        "Data whirls into orbit—",
        "NeutronStar’s new dawn",
    ],
    [
        "One node to read all",
        "The cosmos of raw data—",
        "Bright with silent force",
    ],
    [
        "Dense with memory",
        "NeutronStar devours queries—",
        "Answers pierce the dark",
    ],
    [
        "Event horizon",
        "Of knowledge spun to a star—",
        "Database so bright",
    ],
    [
        "Fusion of all rows",
        "Condensed into core logic—",
        "NeutronStar stands guard",
    ],
    [
        "In radiant beams",
        "Data flows from star to mind—",
        "One node, endless forms",
    ],
    [
        "Colliding queries",
        "Ignite cosmic truth within—",
        "NeutronStar answers",
    ],
    [
        "Singular beacon",
        "Data drawn by glowing mass—",
        "All knowledge in one",
    ],
];

/// Print a random project-related haiku
pub fn print_haiku(print_all: bool) -> anyhow::Result<()> {
    use rand::seq::SliceRandom as _;

    if print_all {
        for h in HAIKUS {
            println!("{}", h.join(":"))
        }
    } else {
        let mut rng = rand::thread_rng();
        println!(
            "{}",
            HAIKUS
                .choose(&mut rng)
                .ok_or(anyhow!("at least one haiku"))?
                .join("\n")
        )
    }
    Ok(())
}

pub struct CmdOptions {
    /// The number of bytes the command memory pool should be limited to
    pub memory_limit_bytes: usize,
}

pub async fn run_cmd(
    options: &CmdOptions,
    sources: &[String],
    table_name: &str,
    sql: &str,
) -> anyhow::Result<()> {
    use futures::stream::StreamExt as _;

    if sources.is_empty() {
        bail!("No sources provided when running command")
    }

    // TODO(alex): Create UDF to print haiku
    let mut engine = engine::Core::new(options.memory_limit_bytes)?
        .add_direct_csv_table(table_name, sources)
        .await?;
    let mut stream = engine.execute(sql).await?;
    let mut batches = Vec::new();
    while let Some(items) = stream.next().await {
        batches.push(items?);
    }

    //while let Some(batch) = stream.next().await {
    //    let pretty_results = arrow::util::pretty::pretty_format_batches(&[items?])?.to_string();
    //    println!("Results:\n{}", pretty_results);
    //}

    let pretty_results = arrow::util::pretty::pretty_format_batches(&batches[..])?.to_string();
    println!("Results:\n{}", pretty_results);
    Ok(())
}

pub async fn run_fs(
    options: &CmdOptions,
    path: &path::Path,
    table_name: &str,
    sql: &str,
) -> anyhow::Result<()> {
    use futures::stream::StreamExt as _;

    // TODO(alex): Create UDF to print haiku
    let mut engine = engine::Core::new(options.memory_limit_bytes)?
        .add_fs_table(table_name, path)
        .await?;
    let mut stream = engine.execute(sql).await?;
    let mut batches = Vec::new();
    while let Some(items) = stream.next().await {
        batches.push(items?);
    }

    //while let Some(batch) = stream.next().await {
    //    let pretty_results = arrow::util::pretty::pretty_format_batches(&[items?])?.to_string();
    //    println!("Results:\n{}", pretty_results);
    //}

    let pretty_results = arrow::util::pretty::pretty_format_batches(&batches[..])?.to_string();
    println!("Results:\n{}", pretty_results);
    Ok(())
}

pub async fn run_fs_table_func(options: &CmdOptions, sql: &str) -> anyhow::Result<()> {
    use futures::stream::StreamExt as _;

    // TODO(alex): Create UDF to print haiku
    let mut engine = engine::Core::new(options.memory_limit_bytes)?
        .add_fs_table_func()
        .await?;
    let mut stream = engine.execute(sql).await?;
    let mut batches = Vec::new();
    while let Some(items) = stream.next().await {
        batches.push(items?);
    }

    //while let Some(batch) = stream.next().await {
    //    let pretty_results = arrow::util::pretty::pretty_format_batches(&[items?])?.to_string();
    //    println!("Results:\n{}", pretty_results);
    //}

    let pretty_results = arrow::util::pretty::pretty_format_batches(&batches[..])?.to_string();
    println!("Results:\n{}", pretty_results);
    Ok(())
}
