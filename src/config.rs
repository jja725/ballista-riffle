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

//! Configuration for the Riffle shuffle client.

use std::time::Duration;

/// Configuration for connecting to a Riffle (Uniffle) shuffle service.
#[derive(Debug, Clone)]
pub struct RiffleConfig {
    /// Riffle coordinator hostname.
    pub coordinator_host: String,
    /// Riffle coordinator gRPC port.
    pub coordinator_port: u16,
    /// Application ID used for registering shuffles.
    pub app_id: String,
    /// Number of write replicas (default: 1).
    pub data_replica_write: i32,
    /// Number of read replicas (default: 1).
    pub data_replica_read: i32,
    /// Connection timeout.
    pub connect_timeout: Duration,
    /// Request timeout.
    pub request_timeout: Duration,
    /// Maximum block size in bytes for chunked writes (default: 32MB).
    /// Partitions larger than this are split into multiple blocks.
    pub max_block_size: usize,
    /// Interval for application heartbeat in seconds (default: 10).
    pub heartbeat_interval_secs: u64,
}

impl Default for RiffleConfig {
    fn default() -> Self {
        Self {
            coordinator_host: "localhost".to_string(),
            coordinator_port: 19999,
            app_id: String::new(),
            data_replica_write: 1,
            data_replica_read: 1,
            connect_timeout: Duration::from_secs(10),
            request_timeout: Duration::from_secs(30),
            max_block_size: 32 * 1024 * 1024, // 32MB
            heartbeat_interval_secs: 10,
        }
    }
}

impl RiffleConfig {
    /// Returns the coordinator endpoint URL.
    pub fn coordinator_endpoint(&self) -> String {
        format!("http://{}:{}", self.coordinator_host, self.coordinator_port)
    }

    /// Returns the endpoint URL for a shuffle server.
    pub fn server_endpoint(&self, host: &str, port: i32) -> String {
        format!("http://{host}:{port}")
    }
}

/// Parsed Riffle shuffle path: `riffle://host:port/app_id/shuffle_id/partition_id`
#[derive(Debug, Clone)]
pub struct RiffleShufflePath {
    pub server_host: String,
    pub server_port: i32,
    pub app_id: String,
    pub shuffle_id: i32,
    pub partition_id: i32,
}

impl RiffleShufflePath {
    /// Parse a Riffle shuffle URI from a PartitionLocation path.
    /// Returns None if the path is not a riffle:// URI.
    pub fn parse(path: &str) -> Option<Self> {
        let stripped = path.strip_prefix("riffle://")?;
        let parts: Vec<&str> = stripped.splitn(2, '/').collect();
        if parts.len() != 2 {
            return None;
        }

        let host_port: Vec<&str> = parts[0].splitn(2, ':').collect();
        if host_port.len() != 2 {
            return None;
        }
        let server_host = host_port[0].to_string();
        let server_port = host_port[1].parse().ok()?;

        let rest: Vec<&str> = parts[1].splitn(3, '/').collect();
        if rest.len() != 3 {
            return None;
        }
        let app_id = rest[0].to_string();
        let shuffle_id = rest[1].parse().ok()?;
        let partition_id = rest[2].parse().ok()?;

        Some(Self {
            server_host,
            server_port,
            app_id,
            shuffle_id,
            partition_id,
        })
    }

    /// Returns true if the path is a riffle:// URI.
    pub fn is_riffle_path(path: &str) -> bool {
        path.starts_with("riffle://")
    }
}
