use anyhow::Result;
use scylla::macros::FromRow;
use scylla::transport::session::{IntoTypedRows, Session};
use scylla::SessionBuilder;
use scylla::statement::Consistency;
use tracing_subscriber;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let session: Session = SessionBuilder::new().known_node("127.0.0.1:9042").default_consistency(Consistency::One).build().await?;

    println!("will create");
    session.query("CREATE KEYSPACE IF NOT EXISTS ks WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 3}", &[]).await?;
    println!("created; using");
    session.use_keyspace("ks", false).await?;
    println!("used");

    session
        .query(
            "CREATE TABLE IF NOT EXISTS ks.t (a int, b int, c text, primary key (a, b))",
            &[],
        )
        .await?;

    println!("queried createtable");

    session
        .query("INSERT INTO ks.t (a, b, c) VALUES (?, ?, ?)", (3, 4, "def"))
        .await?;


    println!("inserted");
    session
        .query("INSERT INTO ks.t (a, b, c) VALUES (1, 2, 'abc')", &[])
        .await?;

    println!("inserted");
    let prepared = session
        .prepare("INSERT INTO ks.t (a, b, c) VALUES (?, 7, ?)")
        .await?;
    println!("inserted");
    session
        .execute(&prepared, (42_i32, "I'm prepared!"))
        .await?;
    println!("inserted");
    session
        .execute(&prepared, (43_i32, "I'm prepared 2!"))
        .await?;
    println!("inserted 43");
    session
        .execute(&prepared, (44_i32, "I'm prepared 3!"))
        .await?;

    println!("executed");

    // Rows can be parsed as tuples
    if let Some(rows) = session.query("SELECT a, b, c FROM ks.t", &[]).await?.rows {
        for row in rows.into_typed::<(i32, i32, String)>() {
            let (a, b, c) = row?;
            println!("a, b, c: {}, {}, {}", a, b, c);
        }
    }

    // Or as custom structs that derive FromRow
    #[derive(Debug, FromRow)]
    struct RowData {
        a: i32,
        b: Option<i32>,
        c: String,
    }

    if let Some(rows) = session.query("SELECT a, b, c FROM ks.t", &[]).await?.rows {
        for row_data in rows.into_typed::<RowData>() {
            let row_data = row_data?;
            println!("row_data: {:?}", row_data);
        }
    }

    // Or simply as untyped rows
    if let Some(rows) = session.query("SELECT a, b, c FROM ks.t", &[]).await?.rows {
        for row in rows {
            let a = row.columns[0].as_ref().unwrap().as_int().unwrap();
            let b = row.columns[1].as_ref().unwrap().as_int().unwrap();
            let c = row.columns[2].as_ref().unwrap().as_text().unwrap();
            println!("a, b, c: {}, {}, {}", a, b, c);

            // Alternatively each row can be parsed individually
            // let (a2, b2, c2) = row.into_typed::<(i32, i32, String)>() ?;
        }
    }

    let metrics = session.get_metrics();
    println!("Queries requested: {}", metrics.get_queries_num());
    println!("Iter queries requested: {}", metrics.get_queries_iter_num());
    println!("Errors occured: {}", metrics.get_errors_num());
    println!("Iter errors occured: {}", metrics.get_errors_iter_num());
    println!("Average latency: {}", metrics.get_latency_avg_ms().unwrap());
    println!(
        "99.9 latency percentile: {}",
        metrics.get_latency_percentile_ms(99.9).unwrap()
    );

    println!("Ok.");

    Ok(())
}
