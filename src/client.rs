use compute::worker_pool_client::WorkerPoolClient;
use compute::{Empty, WorkerId, WorkResponse, TaskLease};
use tonic::transport::Channel;

pub mod compute {
    tonic::include_proto!("compute");
}

// NOT async; client only does one task at a time for now
fn do_work(id: u32, payload: &str, mut client: WorkerPoolClient<Channel>, client_id: u32) -> (u32, u32) {
    // simulate some work
    std::thread::sleep(std::time::Duration::from_secs(4));
    println!("some work done");

    // Call renew_lease (simulate a progress checkpoint)
    // Currently on the server side, leases are time-based, not progress-based
    // so the timeout must be long enough for the checkpoints in the usual case
    tokio::spawn(async move {
        let _ = client.renew_lease(TaskLease {client_id: client_id, task_id: id }).await;
    });
    println!("issued lease renewal call");
    // simulate more work
    std::thread::sleep(std::time::Duration::from_secs(4));
    
    ( id, payload.chars().fold( 0, |acc, c| if c.to_ascii_lowercase() == 't' {acc + 1} else {acc}) )
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    let mut args = std::env::args();
    args.next();
    let addr_arg = args.next().unwrap_or("http://127.0.0.1:3000".to_owned());

    let mut client = WorkerPoolClient::connect(addr_arg).await?;

    let request = tonic::Request::new(Empty {} );

    // mut because it could change if we reconnect
    let mut client_id = client.register(request).await?.into_inner().id;

    let mut stream = client.open_stream(WorkerId { id: client_id }).await?.into_inner();

    println!("This is client {}", client_id);

    // process work
    // for now, panic if we can't get work or send a response
    loop {
        while let Some(msg) = stream.message().await? {
            println!("{}", msg.payload);

            let (task_id, result) = do_work(msg.id, &msg.payload, client.clone(), client_id);
            let _ = client.complete_work(WorkResponse {client_id: client_id, task_id: task_id, result: result } ).await?;
        }
    }

    Ok(())
}