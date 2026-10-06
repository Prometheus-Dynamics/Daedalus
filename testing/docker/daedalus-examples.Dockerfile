FROM rust:1.99.0

WORKDIR /workspace

COPY . .

RUN cargo build -p daedalus-rs --features "engine,plugins" --examples
