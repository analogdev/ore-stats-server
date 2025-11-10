# Known issues and improvement ideas

The forked service inherits several sharp edges. They are documented here so
future contributors can plan fixes without rediscovering the same gaps.

## High-priority fixes

1. **Miner pagination underflows on small datasets.**
   `get_miners` in `src/main.rs` subtracts from `miners.len()` without ensuring
   the list contains at least two entries. When the cache is empty—or only one
   miner has been observed—the calculation panics.

2. **RPC/WebSocket schemes are hard-coded.**
   Both RPC clients prepend fixed schemes ("https://" and "ws://") to the
   `RPC_URL` environment variable. Deployments that rely on HTTP-only endpoints,
   secure WebSockets, or custom ports must patch and rebuild.

3. **WebSocket reconnection spins without backoff.**
   If `PubsubClient::new` fails, the loop immediately retries, wasting CPU
   during outages. Adding even a short delay (or exponential backoff) would be
   kinder to the host.

## Additional suggestions

- **Configurable bind address.**
  The HTTP listener binds to `127.0.0.1:8080`. Surfacing the address via an
  environment variable would simplify Docker and remote deployments.

- **Replace blanket sleeps with a rate limiter.**
  Many RPC fetch paths sleep for one second between calls. A targeted rate
  limiter could maintain throughput without stalling healthy paths.

- **Standardize on structured logging.**
  Several async flows still use `println!`. Switching them to `tracing` keeps log
  output consistent with the rest of the project.

The inline comments in `src/main.rs` and `src/rpc.rs` reference these items so
that the current behavior stays documented until the fixes land.
