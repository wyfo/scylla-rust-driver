use anyhow::Result;
use scylla::frame::types::Consistency;
use scylla::statement::prepared_statement::PreparedStatement;
use scylla::transport::session::Session;
use std::env;
use std::io::prelude::*;
use std::sync::Arc;

use tokio::sync::Semaphore;

#[tokio::main]
async fn main() -> Result<()> {
    let parallelism: usize = env::var("SCYLLA_PARALLELISM")
        .unwrap_or_else(|_| "8".to_owned())
        .parse()?;
    let total_row_count: usize = env::var("SCYLLA_ROW_COUNT")
        .unwrap_or_else(|_| "100000".to_owned())
        .parse()?;
    let row_count = total_row_count / parallelism;
    let total_rate: u32 = env::var("SCYLLA_HZ")
        .unwrap_or_else(|_| "10000".to_owned())
        .parse()?;
    let rate = total_rate / parallelism as u32;
    let sem = Arc::new(Semaphore::new(parallelism));
    let interval = tokio::time::Duration::from_secs(1) / rate;
    let mut contexts: Vec<(Session, PreparedStatement)> = Vec::with_capacity(parallelism);
    println!("Total row count: {}", total_row_count);
    println!("Rate: {} ({:?} per 1 worker request)", total_rate, interval);
    println!("Parallelism: {}", parallelism);
    // Prepare phase: connection is established for every worker and the statement is prepared
    for p in 0..parallelism {
        let uri = env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());
        println!("Worker {} connecting to {} ...", p, uri);
        let session = Session::connect(uri, None).await?;

        session.query("CREATE KEYSPACE IF NOT EXISTS ks WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 3}", &[]).await?;
        session
            .query(
                "CREATE TABLE IF NOT EXISTS ks.t2 (a bigint, b bigint, c text, primary key (a, b))",
                &[],
            )
            .await?;

        let raw_prepared = env::var("SCYLLA_STATEMENT")
            .unwrap_or_else(|_| "INSERT INTO ks.t2 (a, b, c) VALUES (?, ?, 'abc')".to_owned());
        let mut prepared = session.prepare(&raw_prepared).await?;
        prepared.set_consistency(Consistency::Quorum);
        contexts.push((session, prepared));
    }
    // Work phase: each worker runs its payload, sending requests at given rate
    let start = tokio::time::Instant::now();
    println!("Workers launched");
    for p in 0..parallelism {
        let permit = sem.clone().acquire_owned().await;
        let (session, prepared) = contexts.pop().unwrap();
        tokio::task::spawn(async move {
            let mut deadline = tokio::time::Instant::now();
            for i in 0..row_count {
                // All workers are roughly equal, so progress is estimated on worker0 only
                if p == 0 && i % 1000 == 0 {
                    print!(
                        "\r[{}%]: {}/{} sent",
                        (i as f64 * parallelism as f64 / total_row_count as f64 * 100.) as u32,
                        i * parallelism,
                        total_row_count
                    );
                    std::io::stdout().flush().unwrap();
                }
                tokio::time::sleep_until(deadline).await;
                deadline = tokio::time::Instant::now() + interval;
                let _e = session
                    .execute(&prepared, ((i + row_count * p) as i64, 2 * i as i64))
                    .await;
            }
            let _permit = permit;
        });
    }

    // Wait for all in-flight requests to finish
    for _ in 0..parallelism {
        sem.acquire().await.forget();
    }

    let total = tokio::time::Instant::now() - start;
    println!("\r[100%] {}/{} sent", total_row_count, total_row_count);
    println!("Execution time: {:?}", total);
    println!(
        "Effective execution time per query: {:?}",
        total / total_row_count as u32
    );
    println!(
        "Effective rate: {}ops/s",
        total_row_count / total.as_secs() as usize
    );

    Ok(())
}
