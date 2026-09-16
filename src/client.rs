use compute::worker_pool_client::WorkerPoolClient;
use compute::{Empty, WorkResponse};

pub mod compute {
    tonic::include_proto!("compute");
}

// NOT async; client only does one task at a time for now
fn do_work(payload: &str) -> WorkResponse {
    WorkResponse { result: payload.chars().fold( 0, |acc, c| if c.to_ascii_lowercase() == 't' {acc + 1} else {acc}) }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = WorkerPoolClient::connect("http://[::1]:3000").await?;

    let request = tonic::Request::new(Empty {} );

    let mut stream = client.register(request).await?.into_inner();

    println!("RESPONSE={:?}", stream);

    // process work
    // for now, panic if we can't get work or send a response
    loop {
        while let Some(msg) = stream.message().await? {
            println!("{}", msg.payload);
            let _ = client.complete_work(do_work(&msg.payload)).await?;
        }
    }

    Ok(())
}