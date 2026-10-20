use compute::worker_pool_client::WorkerPoolClient;
use compute::{Empty, WorkerId, WorkResponse, TaskLease};
use tonic::transport::Channel;
/*/
pub mod compute {
    tonic::include_proto!("compute");
}
*/
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
