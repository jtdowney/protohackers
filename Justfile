build:
    cargo zigbuild --release --target x86_64-unknown-linux-gnu

deploy: build
    scp target/x86_64-unknown-linux-gnu/release/protohackers root@143.110.239.155:/usr/bin/protohackers