# Beacon

Watches Elite Dangerous journal files and sends JSON events to an HTTP endpoint.

## Build

```sh
cargo build
```

## Run

Start Beacon:

```sh
.beacon -w test_logs --endpoint localhost:8080
```

For TLS, add `--enable-tls`. Override trusted CA roots with one or more `--tls-ca-cert <PATH>` arguments.
