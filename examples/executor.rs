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

//! Run a Ballista executor backed by the Riffle remote shuffle service.
//!
//! This is the main integration point of the crate: plug
//! [`RiffleExecutionEngine`] into a Ballista executor through the
//! `override_execution_engine` hook. Shuffle-write stages then push their
//! output to Riffle (Uniffle) shuffle servers instead of local disk, while
//! every other plan falls back to the default engine.
//!
//! Prerequisites:
//!   * A running Riffle coordinator + shuffle server (see `docker/`).
//!   * A running Ballista scheduler (upstream `ballista-scheduler` binary).
//!
//! Run with:
//! ```bash
//! cargo run --example executor
//! ```

use std::sync::Arc;

use ballista_executor::executor_process::{ExecutorProcessConfig, start_executor_process};
use ballista_riffle::config::RiffleConfig;
use ballista_riffle::execution_engine::RiffleExecutionEngine;

#[tokio::main]
async fn main() -> ballista_core::error::Result<()> {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .parse_filters("ballista=info,ballista_riffle=debug")
        .try_init();

    // 1. Point the executor at your Riffle coordinator. The coordinator hands
    //    out per-partition shuffle-server assignments at runtime. `app_id` is
    //    filled in per job by the execution engine, so leave it empty here.
    //    See `examples/riffle_config.rs` for the full set of knobs.
    let riffle_config = RiffleConfig {
        coordinator_host: "localhost".to_string(),
        coordinator_port: 19999,
        ..Default::default()
    };

    // 2. Build the execution engine. It intercepts shuffle-write stages and
    //    pushes their output to Riffle, delegating all other plans to the
    //    default (local-disk) engine.
    let engine = Arc::new(RiffleExecutionEngine::new(riffle_config));

    // 3. Start the executor with the engine installed via the override hook.
    let config = ExecutorProcessConfig {
        scheduler_host: "localhost".to_string(),
        scheduler_port: 50050,
        override_execution_engine: Some(engine),
        ..Default::default()
    };

    start_executor_process(Arc::new(config)).await
}
