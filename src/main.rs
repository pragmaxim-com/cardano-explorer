#![feature(test)]
extern crate test;

mod block_persistence;
mod block_provider;
mod cardano_client;
mod config;
mod model;
mod storage;

use crate::block_persistence::CardanoBlockPersistence;
use crate::block_provider::CardanoBlockProvider;
use crate::cardano_client::CBOR;
use crate::config::CardanoConfig;
use crate::model::Block;
use anyhow::Result;
use chain_syncer::api::{BlockPersistence, BlockProvider};
use chain_syncer::scheduler::Scheduler;
use chain_syncer::settings::{AppConfig, HttpSettings, IndexerSettings};
use chain_syncer::{combine, info};
use futures::future::ready;
use redbit::redb::Database;
use redbit::*;
use std::env;
use std::sync::Arc;
use tower_http::cors;

async fn maybe_run_server(http_conf: &HttpSettings, db: Arc<Database>) -> () {
    if http_conf.enable {
        info!("Starting http server at {}", http_conf.bind_address);
        let cors = cors::CorsLayer::new()
            .allow_origin(cors::Any) // or use a specific origin: `AllowOrigin::exact("http://localhost:5173".parse().unwrap())`
            .allow_methods(cors::Any)
            .allow_headers(cors::Any);
        serve(RequestState { db: Arc::clone(&db) }, http_conf.bind_address, None, Some(cors)).await
    } else {
        ready(()).await
    }
}

async fn maybe_run_indexing(index_config: &IndexerSettings, scheduler: Scheduler<CBOR, Block>) -> () {
    if index_config.enable {
        info!("Starting indexing process");
        scheduler.schedule(&index_config).await
    } else {
        ready(()).await
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let app_config = AppConfig::new("config/settings")?;
    let cardano_config = CardanoConfig::new("config/cardano")?;
    let db_path: String = format!("{}/{}/{}", app_config.indexer.db_path, "main", "cardano");
    let db = Arc::new(storage::get_db(env::home_dir().unwrap().join(&db_path))?);

    let block_provider: Arc<dyn BlockProvider<CBOR, Block>> = Arc::new(CardanoBlockProvider::new(&cardano_config).await);
    let block_persistence: Arc<dyn BlockPersistence<Block>> = Arc::new(CardanoBlockPersistence { db: Arc::clone(&db) });
    let scheduler: Scheduler<CBOR, Block> = Scheduler::new(block_provider, block_persistence);

    let indexing_f = maybe_run_indexing(&app_config.indexer, scheduler);
    let server_f = maybe_run_server(&app_config.http, Arc::clone(&db));
    combine::futures(indexing_f, server_f).await;
    Ok(())
}
