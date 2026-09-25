use std::sync::atomic::{AtomicU32, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::collections::{HashMap, HashSet, VecDeque};

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
    in_flight: Arc<Mutex<HashSet<u32>>>,
    results: Mutex<Vec<u32>>,   // doesn't need Arc because it's never moved to async task
}


impl WorkerPoolManager {
      fn assign_work(&self, payload: String) {

        let payload_id: AtomicU32 = 0.into();

        let cloned_clients = Arc::clone(&self.clients);
        let cloned_in_flight = Arc::clone(&self.in_flight);

        // split the original job
        let payload_chars: Vec<char> = payload.chars().collect();
        let chunk_size = payload_chars.len().div_ceil(4).max(1);
        let mut payloads: VecDeque<(u32, String)> = payload_chars
            .chunks(chunk_size)
            .map(|chunk| (payload_id.fetch_add(1, Relaxed), chunk.iter().collect()))
            .collect();

        tokio::spawn(async move {
            loop {
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

                {
                    let lock = cloned_clients.read().await;
                    if lock.is_empty() { continue; } // no clients to take a job
                }

                if payloads.is_empty() {
                    let lock = cloned_in_flight.lock().unwrap();
                    if lock.is_empty() {
                        // no queued jobs and no jobs in flight - we're done
                        println!("Work queue exhausted");
                        break;
                    }
                    else {
                        continue;  // stay active in case we have to reassign an in-flight job, i.e., client down or corrupt result
                    }
                };

                
                let mut to_drop = Vec::new();

                    {
                        let lock = cloned_clients.read().await;

                        for (client, sender) in lock.iter() {
                         //   let id = payload_id.fetch_add(1, Relaxed);
                            if payloads.front().is_none() { break; }
                            let payload = payloads.front().unwrap();

                            if sender.send(Ok(WorkPayload{id: payload.0, payload: payload.1.clone()})).is_err() {
                                to_drop.push(client.clone());
                                
                                println!("Can't send task to client {}", client);
                            }
                            else {
                                // store the ID of the in-flight job
                                let mut in_flight_lock = cloned_in_flight.lock().unwrap();
                                in_flight_lock.insert(payload.0);

                                payloads.pop_front();

                            }
                        }
                        
                    }

                    if to_drop.len() > 0 {
                        let mut lock = cloned_clients.write().await;
                        for client in to_drop {
                            lock.remove(&client);
                        }
                    }
                
            }
        });
    }

    fn collect_results(&self, id: u32, result: u32) {
            self.results.lock().unwrap().push(result);    
            self.in_flight.lock().unwrap().remove(&id);
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

        // TODO: We don't really care which client returns the job, but it might be nice to know anyway
        let response = request.into_inner();
        println!("client returned task id {} result {}", response.id, response.result);

        // store the result
        self.collect_results(response.id, response.result);

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