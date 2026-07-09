// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Configure the Riffle client and talk to a live coordinator.
//!
//! This example walks through every [`RiffleConfig`] knob, connects a
//! [`RiffleClient`] to the coordinator, registers an application and asks for
//! shuffle-server assignments. It is a useful smoke test that your Riffle
//! deployment is reachable before wiring it into a full Ballista cluster
//! (see `examples/executor.rs`).
//!
//! Bring up a local cluster first (see `docker/`), then run:
//! ```bash
//! cargo run --example riffle_config
//! ```

use std::time::Duration;

use ballista_riffle::client::RiffleClient;
use ballista_riffle::config::RiffleConfig;

#[tokio::main]
async fn main() -> ballista_riffle::error::Result<()> {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .try_init();

    // Every field has a sensible default; override only what you need.
    let config = RiffleConfig {
        // --- Coordinator: where clients discover shuffle servers ---
        coordinator_host: "localhost".to_string(),
        coordinator_port: 19999,

        // --- Application identity ---
        // In a cluster this is set to the Ballista job id per stage. For a
        // standalone client, pick something unique per run.
        app_id: format!("riffle-config-example-{}", std::process::id()),

        // --- Replication ---
        // Number of shuffle servers each block is written to / read from.
        data_replica_write: 1,
        data_replica_read: 1,

        // --- Timeouts ---
        connect_timeout: Duration::from_secs(10),
        request_timeout: Duration::from_secs(30),

        // --- Chunking ---
        // Partitions larger than this are split into multiple Riffle blocks.
        max_block_size: 32 * 1024 * 1024, // 32 MB

        // --- Liveness ---
        // How often the scheduler heartbeats the app to the coordinator.
        heartbeat_interval_secs: 10,
    };

    println!("coordinator endpoint: {}", config.coordinator_endpoint());

    // Connect and register the application with the coordinator.
    let client = RiffleClient::connect(config).await?;
    println!("connected, app_id = {}", client.app_id());

    client.register_application().await?;
    println!("registered application");

    // Ask the coordinator for shuffle-server assignments for a 4-partition
    // shuffle. Each assignment maps a partition range to a shuffle server.
    let shuffle_id = 0;
    let num_partitions = 4;
    let assignments = client
        .get_shuffle_assignments(shuffle_id, num_partitions)
        .await?;

    println!("assignments for {num_partitions} partitions:");
    for a in &assignments {
        if let Some(server) = a.server.first() {
            println!(
                "  partitions {}..={} -> {}:{}",
                a.start_partition, a.end_partition, server.ip, server.port
            );
        }
    }

    Ok(())
}
