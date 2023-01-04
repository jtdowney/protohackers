FROM rust as rust-chef
RUN cargo install cargo-chef

FROM rust-chef as planner

WORKDIR /usr/src
COPY . .
RUN cargo chef prepare  --recipe-path recipe.json

FROM rust-chef as builder

WORKDIR /usr/src
COPY --from=planner /usr/src/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json
COPY . .
RUN cargo build --release

FROM debian:bullseye-slim

COPY --from=builder /usr/src/target/release/protohackers /usr/bin
USER nobody
CMD ["/usr/bin/protohackers"]
