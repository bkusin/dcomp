use std::net::{SocketAddr, SocketAddrV6, ToSocketAddrs};
use std::sync::atomic::{AtomicU32, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::collections::{HashMap, HashSet, VecDeque};
use std::time::{Duration, Instant};

use tonic::{transport::Server, Request, Response, Status};
use tokio::sync::{mpsc::*, RwLock};
use tokio_stream::wrappers::UnboundedReceiverStream;

use compute::worker_pool_server::{WorkerPool, WorkerPoolServer};
use compute::{Empty, WorkerId, WorkPayload, WorkResponse, TaskLease};

pub mod compute {
    tonic::include_proto!("compute"); 
}

type TaskSender = UnboundedSender<Result<WorkPayload, Status>>;

#[derive(Debug, Default)]
pub struct WorkerPoolManager {
    client_id: AtomicU32,
    clients: Arc<RwLock<HashMap<u32, TaskSender>>>,
    in_flight: Arc<Mutex<HashMap<u32, HashMap<u32, Instant>>>>,  // job -> <assigned clients, task lease>
    results: Mutex<HashMap<u32, u32>>,                           // doesn't need Arc because it's never moved to async task
    timeout: Duration,                                           // task lease timeout in seconds
}


impl WorkerPoolManager {
    fn assign_work(&self, payload: String) {

        let payload_id: AtomicU32 = 0.into();

        let cloned_clients = Arc::clone(&self.clients);
        let cloned_in_flight = Arc::clone(&self.in_flight);
        let timeout = self.timeout;

        // split the original job
        let payload_chars: Vec<char> = payload.chars().collect();
        let chunk_size = payload_chars.len().div_ceil(4).max(1);
        let mut payloads: HashMap<u32, String> = payload_chars
            .chunks(chunk_size)
            .map(|chunk| (payload_id.fetch_add(1, Relaxed), chunk.iter().collect()))
            .collect();
        let mut queue: VecDeque<u32> = payloads.keys().copied().collect();

        tokio::spawn(async move {
            loop {
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

                let mut connections_to_drop: Vec<u32> = Vec::new();
                
                // Check in-flight jobs. 
                //If any job's entire client list has expired task leases or is unreachable, put the job back on the queue
                {
                    let clients_lock = cloned_clients.read().await;
                    let mut in_flight_lock = cloned_in_flight.lock().unwrap();
                    let mut in_flight_to_drop: Vec<u32> = Vec::new();
                    

                    for (id, client_map) in in_flight_lock.iter_mut() {
                        let mut num_closed = 0;
                        let mut num_late = 0;
                        let mut assigned_clients_to_drop: Vec<u32> = Vec::new();
                        for (client, timestamp) in client_map.iter() {

                            if Instant::now().duration_since(*timestamp) > timeout {
                                num_late += 1;
                            }

                            if clients_lock.get(client).map_or(true, |sender| sender.is_closed()) {
                                num_closed += 1;
                                assigned_clients_to_drop.push(*client);     // remove client from in-flight job's client list
                                connections_to_drop.push(*client);          // remove the client itself from server's connections

                                println!("Can't reach busy client {} processing job {}", client, id);
                            }
                        }

                        // if all assigned clients are stuck or dead, requeue the task
                        if num_late + num_closed == client_map.len() {
                            queue.push_back(*id);
                            in_flight_to_drop.push(*id);

                            println!("Requeuing task {}", id);
                        }

                        for client in &assigned_clients_to_drop {
                            client_map.remove(client);
                        }
                    }

                    for task_id in in_flight_to_drop {
                        in_flight_lock.remove(&task_id);
                    }

                }

                // Drop broken connections before deciding if there is any work left.
                if !connections_to_drop.is_empty() {
                    let mut lock = cloned_clients.write().await;
                    for client in connections_to_drop.drain(..) {
                        lock.remove(&client);
                    }
                }

                if queue.is_empty() {
                    let lock = cloned_in_flight.lock().unwrap();
                    if lock.is_empty() {
                        println!("Work queue exhausted");
                        break;
                    }
                    continue;
                }

                if cloned_clients.read().await.is_empty() {
                    println!("No clients available");
                    continue;
                }
                
                    // task assignment
                    {
                        let clients_lock = cloned_clients.read().await;

                        for (client, sender) in clients_lock.iter() {
                            if queue.front().is_none() { break; }

                            let payload = queue.front().unwrap();

                            // don't assign a task to a client that is already on it
                            let mut in_flight_lock = cloned_in_flight.lock().unwrap();
                            if let Some(assigned_list) = in_flight_lock.get(payload) {
                                if assigned_list.contains_key(client) {
                                    continue;
                                }
                            }

                            if sender.send(Ok(WorkPayload{id: *payload, payload: payloads.get(payload).unwrap().clone()})).is_err() {
                                connections_to_drop.push(*client);
                                
                                println!("Can't send task to client {}", client);
                            }
                            else {
                                // store the ID of the in-flight job and its assigned client
                                if in_flight_lock.contains_key(payload) {
                                    in_flight_lock.get_mut(payload).unwrap().insert(*client, Instant::now());
                                }
                                else { 
                                    in_flight_lock.insert(*payload, HashMap::from([(*client, Instant::now())]));
                                }

                                println!("Assigned task {} to client {}", payload, client);
                                queue.pop_front();
                            }
                        }
                    }

                    if !connections_to_drop.is_empty() {
                        let mut lock = cloned_clients.write().await;
                        for client in &connections_to_drop {
                            lock.remove(client);
                        }
                    }

            }
        });
    }

    fn collect_results(&self, id: u32, result: u32) {
            self.results.lock().unwrap().insert(id, result);    
            self.in_flight.lock().unwrap().remove(&id);
        }

    fn set_timeout(&mut self, duration: Duration) {
        self.timeout = duration;
    }

}

#[tonic::async_trait]
impl WorkerPool for WorkerPoolManager {
    // the function open_stream returns a stream, thus the (generated) expected name for the type is OpenStreamStream
    type OpenStreamStream = UnboundedReceiverStream<Result<WorkPayload, Status>>;

    // gives the client an ID but doesn't store it.
    async fn register(&self, request: Request<Empty>) -> Result<Response<WorkerId>, Status> {
            
        let id = self.client_id.fetch_add(1, Relaxed);

        println!("Registered client {}", id);

        Ok(Response::new(WorkerId{ id: id}))
    }

    // client requests to open a channel. Store the client ID and channel.
    async fn open_stream(&self, request: Request<WorkerId>) -> Result<Response<Self::OpenStreamStream>, Status> {
            
        let id = request.into_inner().id;
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<Result<WorkPayload, Status>>();

        let mut write_guard = self.clients.write().await;
        write_guard.insert(id, tx);

        let output_stream = UnboundedReceiverStream::new(rx);

        println!("Opened channel with client {}", id);

        Ok(Response::new(output_stream)) 
    }


    // TODO
    async fn renew_lease(&self, request: Request<TaskLease>) -> Result<Response<Empty>, Status> {
        // TODO: Change client set to map of (client, last_update) tuples, or use a separate lease list

        let mut in_flight_lock = self.in_flight.lock().unwrap();
        let lease_msg = request.into_inner();

        // renew lease; ignore attempted leases on tasks that are not in flight
        if let Some(lease) = in_flight_lock.get_mut(&lease_msg.task_id) {
            lease.insert(lease_msg.client_id, Instant::now());

            println!("Received lease renewal for client {}, task {}", lease_msg.client_id, lease_msg.task_id);
        };

        Ok(Response::new(Empty{}))
    }

    async fn complete_work(&self, request: Request<WorkResponse>) -> Result<Response<Empty>, Status> {

        // TODO: We don't really care which client returns the job, but it might be nice to know anyway
        let response = request.into_inner();
        println!("client returned task id {} result {}", response.task_id, response.result);

        // store the result
        // tasks are idempotent so just unconditionally store results.
        // if they weren't, then we would need a policy such as First Write Wins
        self.collect_results(response.task_id, response.result);

        // ACK
        Ok(Response::new(Empty{}))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args();
    args.next();
    let addr_arg = args.next().unwrap_or("0.0.0.0:3000".to_owned());
    let addr: SocketAddr = addr_arg.parse().unwrap();
    let mut manager = WorkerPoolManager::default();
    manager.set_timeout(Duration::from_secs(10));
    manager.assign_work("This is a test!".to_owned());

    Server::builder()
        .add_service(WorkerPoolServer::new(manager))
        .serve(addr)
        .await?;

    Ok(())
}