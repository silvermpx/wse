# wse-protocol

The pieces of the [WSE](https://github.com/silvermpx/wse) protocol that its
server ([`wse`](https://crates.io/crates/wse)) and its Rust client
([`wse-client`](https://crates.io/crates/wse-client)) share: the crypto on
aws-lc-rs, end-to-end sessions and signed messages, one implementation for
both sides.

It follows the version of those crates and has no API of its own to keep
stable: depend on `wse` or `wse-client` instead.

## License

MIT
