use pallas::network::miniprotocols::Point;
use pallas::network::miniprotocols::chainsync::NextResponse;
use std::{pin::Pin, sync::Arc};
use tokio::runtime::Runtime;

use super::cardano_client::{CBOR, CardanoClient};
use crate::config::CardanoConfig;
use crate::info;
use crate::model::*;
use ExplorerError;
use async_trait::async_trait;
use chain_syncer::api::{BlockProvider, ChainSyncError};
use chain_syncer::monitor::BoxWeight;
use futures::{Stream, StreamExt};
use pallas::codec::minicbor::{Encode, Encoder};
use pallas::ledger::traverse::{MultiEraBlock, MultiEraInput, MultiEraOutput};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

pub static GENESIS_START_TIME: u32 = 1506203091;

pub struct CardanoBlockProvider {
    pub client: CardanoClient,
}

impl CardanoBlockProvider {
    pub async fn new(cardano_config: &CardanoConfig) -> Self {
        CardanoBlockProvider { client: CardanoClient::new(cardano_config).await }
    }
    fn process_inputs(&self, ins: &[MultiEraInput<'_>]) -> Vec<TempInputRef> {
        // iter zipped with index
        ins.iter()
            .map(|input| {
                let tx_hash: [u8; 32] = **input.hash();
                let tx_hash = TxHash(tx_hash);
                TempInputRef { tx_hash, index: input.index() as u32 }
            })
            .collect()
    }

    fn process_outputs(&self, outs: &[MultiEraOutput<'_>], tx_pointer: TxPointer) -> (BoxWeight, Vec<Utxo>) {
        let mut result_outs = Vec::with_capacity(outs.len());
        let mut asset_count = 0;
        let mut ctx = ();
        for (out_index, out) in outs.iter().enumerate() {
            let address_opt = out.address().ok().map(|a| a.to_vec());
            let script_hash_opt = out.script_ref().map(|h| {
                let mut buffer = Vec::new();
                let mut encoder = Encoder::new(&mut buffer);
                h.encode(&mut encoder, &mut ctx).unwrap();
                buffer
            });
            let utxo_pointer = UtxoPointer::from_parent(tx_pointer.clone(), out_index as u16);

            let mut result_assets = Vec::with_capacity(out.value().assets().iter().map(|p| p.assets().len()).sum());

            // start your pointer index at 0
            let mut idx: u8 = 0;

            for policy_assets in out.value().assets() {
                // clone the policy‐id bytes once
                let pid_bytes = policy_assets.policy().to_vec();

                for asset in policy_assets.assets() {
                    let any_coin = asset.any_coin();
                    let action = match (asset.is_mint(), any_coin < 0) {
                        (true, _) => AssetType::Mint,
                        (_, true) => AssetType::Burn,
                        _ => AssetType::Transfer,
                    };

                    result_assets.push(Asset {
                        id: AssetPointer::from_parent(utxo_pointer.clone(), idx),
                        amount: any_coin.abs() as u64,
                        name: AssetName(asset.name().to_vec()),
                        policy_id: PolicyId(pid_bytes.clone()),
                        asset_action: AssetAction(action.into()),
                    });

                    idx += 1;
                }
            }

            asset_count += result_assets.len();
            result_outs.push(Utxo {
                id: utxo_pointer,
                amount: out.value().coin().into(),
                address: Address(address_opt.unwrap_or_default()),
                script_hash: ScriptHash(script_hash_opt.unwrap_or_default()),
                assets: result_assets,
            })
        }
        (asset_count + result_outs.len(), result_outs)
    }
}

#[async_trait]
impl BlockProvider<CBOR, Block> for CardanoBlockProvider {
    fn process_block(&self, block: &CBOR) -> Result<Block, ChainSyncError> {
        let b = MultiEraBlock::decode(block).map_err(ExplorerError::from)?;

        let hash: [u8; 32] = *b.header().hash();
        let prev_h = b.header().previous_hash().unwrap_or(pallas::crypto::hash::Hash::new([0u8; 32]));
        let prev_hash: [u8; 32] = *prev_h;
        let header = BlockHeader {
            id: BlockHeight(b.header().number() as u32),
            timestamp: BlockTimestamp(b.header().slot() as u32 + GENESIS_START_TIME),
            hash: BlockHash(hash),
            prev_hash: BlockHash(prev_hash),
        };

        let mut block_weight = 0;
        let txs: Vec<pallas::ledger::traverse::MultiEraTx> = b.txs();
        let mut result_txs = Vec::with_capacity(txs.len());

        for (tx_index, tx) in txs.iter().enumerate() {
            let tx_hash: [u8; 32] = *tx.hash();
            let tx_id = TxPointer::from_parent(header.id.clone(), tx_index as u16);
            let inputs = self.process_inputs(&tx.inputs());
            let (box_weight, outputs) = self.process_outputs(&tx.outputs().to_vec(), tx_id.clone()); //TODO perf check
            block_weight += box_weight;
            block_weight += inputs.len();
            result_txs.push(Transaction { id: tx_id.clone(), hash: TxHash(tx_hash), utxos: outputs, inputs: vec![], transient_inputs: inputs })
        }

        Ok(Block { id: header.id.clone(), header, transactions: result_txs, weight: block_weight as u32 }) // usize
    }

    async fn get_chain_tip(&self) -> Result<BlockHeader, ChainSyncError> {
        let best_block = self.client.get_best_block().await?;
        let best_header = self.process_block(&best_block)?;
        Ok(best_header.header)
    }

    fn get_processed_block(&self, h: BlockHeader) -> Result<Block, ChainSyncError> {
        let point = Point::new((h.timestamp.0 - GENESIS_START_TIME) as u64, h.hash.0.to_vec());
        let rt = Runtime::new().unwrap();
        let cbor = rt.block_on(self.client.get_block_by_point(point))?;
        self.process_block(&cbor)
    }

    async fn stream(&self, _chain_tip_header: BlockHeader, last_header: Option<BlockHeader>) -> Pin<Box<dyn Stream<Item = CBOR> + Send + 'life0>> {
        let last_point = last_header.clone().map_or(Point::Origin, |h| Point::new(h.timestamp.0 as u64, h.hash.0.to_vec()));

        let (tx, rx) = mpsc::channel::<CBOR>(100);
        let node_client = Arc::clone(&self.client.node_client);

        tokio::spawn(async move {
            let (_, to) = node_client.lock().await.chainsync().find_intersect(vec![last_point]).await.unwrap();

            info!("Indexing from {} to {}", last_header.map(|h| h.id.0).unwrap_or(0), to.1);
            loop {
                match node_client.lock().await.chainsync().request_or_await_next().await.unwrap() {
                    NextResponse::RollForward(block_bytes, _) => {
                        if tx.send(block_bytes.0).await.is_err() {
                            break;
                        }
                    }
                    // Since we're just scraping data until we catch up, we don't need to handle rollbacks
                    NextResponse::RollBackward(_, _) => {}
                    // Await is returned once we've caught up, and we should let
                    // the node notify us when there's a new block available
                    NextResponse::Await => break,
                }
            }
        });
        ReceiverStream::new(rx).boxed()
    }
}
