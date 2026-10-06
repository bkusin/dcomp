use compute::worker_pool_client::WorkerPoolClient;
use compute::{Empty, WorkerId, WorkResponse};

pub mod compute {
    tonic::include_proto!("compute");
}

// NOT async; client only does one task at a time for now
fn do_work(id: u32, payload: &str) -> WorkResponse {
    // simulate a lot of work
    std::thread::sleep(std::time::Duration::from_secs(8));

    WorkResponse { id: id, result: payload.chars().fold( 0, |acc, c| if c.to_ascii_lowercase() == 't' {acc + 1} else {acc}) }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    let mut args = std::env::args();
    args.next();
    let addr_arg = args.next().unwrap_or("http://127.0.0.1:3000".to_owned());

    let mut client = WorkerPoolClient::connect(addr_arg).await?;

    let request = tonic::Request::new(Empty {} );

    // mut because it could change if we reconnect
    let mut id = client.register(request).await?.into_inner().id;

    let mut stream = client.open_stream(WorkerId { id: id }).await?.into_inner();

    println!("This is client {}", id);

    // process work
    // for now, panic if we can't get work or send a response
    loop {
        while let Some(msg) = stream.message().await? {
            println!("{}", msg.payload);
            let _ = client.complete_work(do_work(msg.id, &msg.payload)).await?;
        }
    }

    Ok(())
}