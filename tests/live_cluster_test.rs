// E2E test against a live Riffle cluster (coordinator:21000, server:21100)
// Run with: cargo test --test live_cluster_test -- --nocapture

use ballista_riffle::client::RiffleClient;
use ballista_riffle::config::RiffleConfig;
use ballista_riffle::serde::{record_batches_to_ipc_bytes, shuffle_read_to_record_batches};

use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

#[tokio::test]
async fn test_e2e_live_cluster() {
    let config = RiffleConfig {
        coordinator_host: "127.0.0.1".to_string(),
        coordinator_port: 21000,
        app_id: format!("e2e-test-{}", ts()),
        ..Default::default()
    };

    // Connect
    let client = match RiffleClient::connect(config).await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("SKIP: Riffle cluster not available: {e}");
            return;
        }
    };
    println!("Connected to coordinator");

    // Register app
    client.register_application().await.unwrap();
    println!("Registered app: {}", client.app_id());

    // Get assignments
    let assignments = client.get_shuffle_assignments(1, 4).await.unwrap();
    assert!(!assignments.is_empty());
    let srv = &assignments[0].server[0];
    let host = &srv.ip;
    let port = srv.port;
    println!("Server: {host}:{port}");

    // Register shuffle
    client.register_shuffle(1, 4, host, port).await.unwrap();

    // Create test data — 2 partitions, each with batches
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let batches_p0 = vec![RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["alice", "bob", "carol"])),
        ],
    )
    .unwrap()];

    let batches_p1 = vec![RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![4, 5])),
            Arc::new(StringArray::from(vec!["dave", "eve"])),
        ],
    )
    .unwrap()];

    // Push partition 0
    let ipc0 = record_batches_to_ipc_bytes(&batches_p0, &schema).unwrap();
    let buf0 = client
        .require_buffer(1, ipc0.len() as i32, vec![0], host, port)
        .await
        .unwrap();
    let blk0 = client
        .send_shuffle_data(1, buf0, 0, ipc0.clone(), 0, host, port)
        .await
        .unwrap();
    println!("Pushed partition 0: {} bytes, block_id={blk0}", ipc0.len());

    // Push partition 1
    let ipc1 = record_batches_to_ipc_bytes(&batches_p1, &schema).unwrap();
    let buf1 = client
        .require_buffer(1, ipc1.len() as i32, vec![1], host, port)
        .await
        .unwrap();
    let blk1 = client
        .send_shuffle_data(1, buf1, 1, ipc1.clone(), 0, host, port)
        .await
        .unwrap();
    println!("Pushed partition 1: {} bytes, block_id={blk1}", ipc1.len());

    // Report results
    client
        .report_shuffle_result(1, vec![(0, vec![blk0]), (1, vec![blk1])], host, port)
        .await
        .unwrap();
    println!("Reported results");

    // Read partition 0
    let r0 = client.get_shuffle_data(1, 0, host, port).await.unwrap();
    let b0 = shuffle_read_to_record_batches(&r0.data, &r0.segments).unwrap();
    let rows0: usize = b0.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows0, 3);
    println!("Read partition 0: {} rows OK", rows0);

    // Read partition 1
    let r1 = client.get_shuffle_data(1, 1, host, port).await.unwrap();
    let b1 = shuffle_read_to_record_batches(&r1.data, &r1.segments).unwrap();
    let rows1: usize = b1.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows1, 2);
    println!("Read partition 1: {} rows OK", rows1);

    // Verify actual values
    let ids = b0[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(ids.values(), &[1, 2, 3]);

    let names = b1[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(names.value(0), "dave");
    assert_eq!(names.value(1), "eve");

    println!("SUCCESS: E2E test passed — 2 partitions, 5 total rows roundtripped through Riffle");
}

fn ts() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}
