build:
    cargo zigbuild --release --target x86_64-unknown-linux-gnu

deploy: build
    scp target/x86_64-unknown-linux-gnu/release/protohackers protohackers:~/protohackers

run: deploy
    ssh -t protohackers env TOKIO_CONSOLE_BIND=0.0.0.0:9090 RUST_LOG=protohackers=trace,info ./protohackers 2>&1 | tee /tmp/protohackers.log