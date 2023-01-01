mod problem0;
mod proxy;

#[tokio::main]
async fn main() -> eyre::Result<()> {
    tracing_subscriber::fmt::init();
    stable_eyre::install()?;

    let problem0 = tokio::spawn(problem0::start(9000));

    let _ = tokio::join!(problem0);

    Ok(())
}
