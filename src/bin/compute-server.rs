use std::time::Duration;
use std::net::SocketAddr;

use tonic::transport::Server;

use dcomp::compute::worker_pool_server::WorkerPoolServer;
use dcomp::server::WorkerPoolManager;

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