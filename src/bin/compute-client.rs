use dcomp::compute::worker_pool_client::WorkerPoolClient;
use dcomp::compute::{Empty, WorkerId, WorkResponse};


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    let mut args = std::env::args();
    args.next();
    let addr_arg = args.next().unwrap_or("http://127.0.0.1:3000".to_owned());

    let mut client = WorkerPoolClient::connect(addr_arg).await?;

    let request = tonic::Request::new(Empty {} );

    let client_id = client.register(request).await?.into_inner().id;

    let mut stream = client.open_stream(WorkerId { id: client_id }).await?.into_inner();

    println!("This is client {}", client_id);

    // process work
    // for now, just loop if we can't get work. Quit if we can't send a response.
    loop {
        while let Some(msg) = stream.message().await? {
            println!("{}", msg.payload);

            let (task_id, result) = dcomp::client::do_work(msg.id, &msg.payload, client.clone(), client_id);
            let _ = client.complete_work(WorkResponse {client_id: client_id, task_id: task_id, result: result } ).await?;
        }
    }

    Ok(())
}