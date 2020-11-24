use anyhow::Result;
use scylla::transport::session::{IntoTypedRows, Session};

#[tokio::main]
async fn main() -> Result<()> {
    let uri = "localhost:19042";

    println!("Connecting to {} ...", uri);

    let session = Session::connect(uri, None).await?;
    session.refresh_topology().await?;

    // Hack: force opening connections to all nodes
    let prepared = session
        .prepare("SELECT rack FROM system.peers where peer = ?")
        .await?;
    for i in 0..32 {
        session.execute(&prepared, &scylla::values!(i)).await?;
    }

    session.alter("read_timeout", "2s").await?;
    session.alter("write_timeout", "500ms").await?;

    if let Some(rows) = session
        .query("SELECT JSON address, port, params FROM system.clients", &[])
        .await?
    {
        for row in rows.into_typed::<(String,)>() {
            println!("{}", row?.0);
        }
    }

    Ok(())
}
