mod problem0;
mod problem1;
mod problem2;
mod proxy;

#[tokio::main]
async fn main() -> eyre::Result<()> {
    tracing_subscriber::fmt::init();
    stable_eyre::install()?;

    let problem0 = tokio::spawn(problem0::start(9000));
    let problem1 = tokio::spawn(problem1::start(9001));
    let problem2 = tokio::spawn(problem2::start(9002));

    let _ = tokio::join!(problem0, problem1, problem2);

    Ok(())
}
