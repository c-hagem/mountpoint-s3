use mountpoint_s3::{create_s3_client, parse_cli_args};

#[tokio::main(flavor = "multi_thread", worker_threads = 10)]
async fn main() -> anyhow::Result<()> {
    let cli_args = parse_cli_args(true);
    mountpoint_s3::run(create_s3_client, cli_args).await;
    tokio::signal::ctrl_c().await?;
    Ok(())
}
