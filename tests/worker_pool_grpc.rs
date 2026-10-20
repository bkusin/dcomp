use dcomp::compute::worker_pool_client::WorkerPoolClient;
use dcomp::compute::worker_pool_server::WorkerPoolServer;
use dcomp::compute::{Empty, WorkerId};
use dcomp::server::WorkerPoolManager;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

#[tokio::test]
async fn client_registers_and_opens_stream_over_grpc() {
	let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
	let address = listener.local_addr().unwrap();
	let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();

	let server = tokio::spawn(async move {
		Server::builder()
			.add_service(WorkerPoolServer::new(WorkerPoolManager::default()))
			.serve_with_incoming_shutdown(
				TcpListenerStream::new(listener),
				async {
					let _ = shutdown_rx.await;
				},
			)
			.await
			.unwrap();
	});

	let mut client = WorkerPoolClient::connect(format!("http://{address}"))
		.await
		.unwrap();
	let client_id = client
		.register(Empty {})
		.await
		.unwrap()
		.into_inner()
		.id;

	assert_eq!(client_id, 0);
	client
		.open_stream(WorkerId { id: client_id })
		.await
		.unwrap();

	let _ = shutdown_tx.send(());
	server.await.unwrap();
}
