use std::sync::atomic::{AtomicU32, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::collections::{HashMap, VecDeque};
use std::pin::Pin;
use std::vec::Splice;

use tonic::{transport::Server, Request, Response, Status};
use tokio::sync::{mpsc::*, RwLock};
use tokio_stream::wrappers::UnboundedReceiverStream;

use compute::worker_pool_server::{WorkerPool, WorkerPoolServer};
use compute::{Empty, WorkPayload, WorkResponse};

pub mod compute {
    tonic::include_proto!("compute"); 
}

type TaskSender = UnboundedSender<Result<WorkPayload, Status>>;

#[derive(Debug, Default)]
pub struct WorkerPoolManager {
    client_id: AtomicU32,
    clients: Arc<RwLock<HashMap<u32, TaskSender>>>,
    results: Mutex<Vec<u32>>,
}


impl WorkerPoolManager {
      fn assign_work(&self, payload: String) {

        let cloned_clients = Arc::clone(&self.clients);
        let payload_chars: Vec<char> = payload.chars().collect();
        let chunk_size = payload_chars.len().div_ceil(4).max(1);
        let mut payloads: VecDeque<String> = payload_chars
            .chunks(chunk_size)
            .map(|chunk| chunk.iter().collect())
            .collect();
        


        tokio::spawn(async move {
            loop {
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                let Some(payload) = payloads.pop_front() else {
                    break;
                };

                {
                    let mut to_drop = Vec::new();
                    let mut send_failed = false;

                    {
                        let lock = cloned_clients.read().await;
                        for (client, sender) in lock.iter() {
                            if sender.send(Ok(WorkPayload{payload: payload.clone()})).is_err() {
                                to_drop.push(client.clone());
                                send_failed = true;
                                println!("Can't send task to client {}", client);
                            }
                        }
                    }

                    if to_drop.len() > 0 {
                        let mut lock = cloned_clients.write().await;
                        for client in to_drop {
                            lock.remove(&client);
                        }
                    }

                    if send_failed {
                        payloads.push_back(payload);
                    }
                }
            }
        });
    }

    async fn collect_results(&self, result: u32) {
            let mut lock = self.results.lock().unwrap();
            lock.push(result);

            if lock.len() == 3 {
                let sum: u32 = lock.iter().sum();
                println!("Job result is {sum}");

                // reset for next job results
                lock.clear();
            }
        }

}

#[tonic::async_trait]
impl WorkerPool for WorkerPoolManager {
    type RegisterStream = UnboundedReceiverStream<Result<WorkPayload, Status>>;

    async fn register(&self, request: Request<Empty>) -> Result<Response<Self::RegisterStream>, Status> {
            
        let id = self.client_id.fetch_add(1, Relaxed);
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<Result<WorkPayload, Status>>();

        let mut write_guard = self.clients.write().await;
        write_guard.insert(id, tx);

        let output_stream = UnboundedReceiverStream::new(rx);

        println!("Registered client {}", id);

        Ok(Response::new(output_stream)) 
    }

    
    async fn complete_work(&self, request: Request<WorkResponse>) -> Result<Response<Empty>, Status> {

        // TODO: Which client? Which task?
        let result = request.into_inner().result;
        println!("client returned {}", result);

        // store the result
        self.collect_results(result).await;

        // ACK
        Ok(Response::new(Empty{}))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:3000".parse()?;
    let manager = WorkerPoolManager::default();
    manager.assign_work("This is a test!".to_owned());

    Server::builder()
        .add_service(WorkerPoolServer::new(manager))
        .serve(addr)
        .await?;

    Ok(())
}