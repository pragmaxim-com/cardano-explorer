## Cardano Explorer

Cardano explorer on top of [redbit](https://github.com/pragmaxim-com/redbit) and [chain-syncer](https://github.com/pragmaxim-com/chain-syncer)

It uses tiny `block_height/tx_index/utxo_index/[asset_index]` dictionary pointers to big hashes, ie. not a single hash is duplicated,
which allows for much better space efficiency and for ~ `6 000 - 12 000 Utxos+Assets / second` syncing speed with local node and an old SSD.

Chain tip is "eventually consistent" through fork competition, ie. forks get settled eventually and superseded forks are deleted from DB.

### Installation (Debian/Ubuntu)

```
sudo apt-get install rustup
```

### Usage

```
# Cardano node is expected to run locally at port 1337, set socket_path at config/settings.toml
cargo run
```

Indexing might crash especially on laptops with Node running locally and not being synced yet.
In that case, set `fetching_parallelism = "low"` to not put the Node and Laptop under heavy pressure.

### Rest API

http://localhost:8000/swagger-ui/

Querying currently times out during historical indexing. So use it only at the chain tip sync phase
or when indexing is disabled `indexer.enable = false` and we only run http server to query over existing data.