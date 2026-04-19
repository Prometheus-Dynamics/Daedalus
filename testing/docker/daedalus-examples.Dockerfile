FROM rust:1.94.0

WORKDIR /workspace

COPY . .

RUN cargo build -p daedalus-rs --features "engine,plugins" --examples
