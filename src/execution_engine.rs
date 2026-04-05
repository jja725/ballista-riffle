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

//! Riffle execution engine for Ballista.
//!
//! Implements `ExecutionEngine` to intercept shuffle writes and push
//! data to Riffle instead of writing to local disk.

use std::fmt::{Debug, Display};
use std::sync::Arc;

use async_trait::async_trait;
use ballista_core::execution_plans::ShuffleWriterExec;
use ballista_core::execution_plans::sort_shuffle::SortShuffleWriterExec;
use ballista_core::serde::protobuf::ShuffleWritePartition;
use ballista_executor::execution_engine::{
    DefaultExecutionEngine, ExecutionEngine, QueryStageExecutor,
};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::repartition::BatchPartitioner;
use datafusion::physical_plan::{ExecutionPlan, Partitioning};
use futures::StreamExt;
use log::debug;

use crate::client::RiffleClient;
use crate::config::RiffleConfig;
use crate::serde::record_batches_to_ipc_bytes;

/// Execution engine that pushes shuffle data to Riffle instead of local disk.
///
/// Falls back to the default engine for non-shuffle plans.
pub struct RiffleExecutionEngine {
    config: RiffleConfig,
    fallback: DefaultExecutionEngine,
}

impl RiffleExecutionEngine {
    /// Create a new Riffle execution engine with the given config.
    pub fn new(config: RiffleConfig) -> Self {
        Self {
            config,
            fallback: DefaultExecutionEngine {},
        }
    }
}

impl ExecutionEngine for RiffleExecutionEngine {
    fn create_query_stage_exec(
        &self,
        job_id: String,
        stage_id: usize,
        plan: Arc<dyn ExecutionPlan>,
        work_dir: &str,
    ) -> Result<Arc<dyn QueryStageExecutor>> {
        // Check if this is a shuffle writer plan we should intercept
        if plan.as_any().downcast_ref::<ShuffleWriterExec>().is_some()
            || plan
                .as_any()
                .downcast_ref::<SortShuffleWriterExec>()
                .is_some()
        {
            let partitioning = if let Some(w) =
                plan.as_any().downcast_ref::<ShuffleWriterExec>()
            {
                w.shuffle_output_partitioning().cloned()
            } else if let Some(w) =
                plan.as_any().downcast_ref::<SortShuffleWriterExec>()
            {
                Some(w.shuffle_output_partitioning().clone())
            } else {
                None
            };

            let child_plan = plan.children()[0].clone();

            let config = RiffleConfig {
                app_id: job_id.clone(),
                ..self.config.clone()
            };

            Ok(Arc::new(RiffleQueryStageExec {
                job_id,
                stage_id,
                plan: child_plan,
                partitioning,
                config,
            }))
        } else {
            // Fall back to default (local disk) for non-shuffle plans
            self.fallback
                .create_query_stage_exec(job_id, stage_id, plan, work_dir)
        }
    }
}

/// Query stage executor that pushes shuffle data to Riffle.
#[derive(Debug)]
struct RiffleQueryStageExec {
    job_id: String,
    stage_id: usize,
    plan: Arc<dyn ExecutionPlan>,
    partitioning: Option<Partitioning>,
    config: RiffleConfig,
}

impl Display for RiffleQueryStageExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "RiffleQueryStageExec: job={} stage={} partitioning={:?}",
            self.job_id, self.stage_id, self.partitioning
        )
    }
}

#[async_trait]
impl QueryStageExecutor for RiffleQueryStageExec {
    async fn execute_query_stage(
        &self,
        input_partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<Vec<ShuffleWritePartition>> {
        let schema = self.plan.schema();
        let mut stream = self.plan.execute(input_partition, context)?;

        // Connect to Riffle
        let client = RiffleClient::connect(self.config.clone())
            .await
            .map_err(|e| DataFusionError::Execution(format!("Riffle connect: {e}")))?;

        // Register app (idempotent)
        client
            .register_application()
            .await
            .map_err(|e| DataFusionError::Execution(format!("Riffle register app: {e}")))?;

        let shuffle_id = self.stage_id as i32;

        match &self.partitioning {
            Some(Partitioning::Hash(exprs, num_output_partitions)) => {
                self.execute_hash_shuffle(
                    &client,
                    shuffle_id,
                    &mut stream,
                    &schema,
                    exprs.clone(),
                    *num_output_partitions,
                )
                .await
            }
            None => {
                // No repartitioning — single output partition
                self.execute_passthrough(
                    &client,
                    shuffle_id,
                    &mut stream,
                    &schema,
                    input_partition,
                )
                .await
            }
            other => Err(DataFusionError::Execution(format!(
                "Unsupported partitioning for Riffle shuffle: {other:?}"
            ))),
        }
    }

    fn collect_plan_metrics(&self) -> Vec<MetricsSet> {
        ballista_core::utils::collect_plan_metrics(self.plan.as_ref())
    }
}

impl RiffleQueryStageExec {
    /// Execute a hash shuffle: partition input, push each partition to Riffle.
    async fn execute_hash_shuffle(
        &self,
        client: &RiffleClient,
        shuffle_id: i32,
        stream: &mut datafusion::physical_plan::SendableRecordBatchStream,
        schema: &SchemaRef,
        exprs: Vec<Arc<dyn datafusion::physical_expr::PhysicalExpr>>,
        num_output_partitions: usize,
    ) -> Result<Vec<ShuffleWritePartition>> {
        use std::collections::HashMap;

        // Get server assignments
        let assignments = client
            .get_shuffle_assignments(shuffle_id, num_output_partitions as i32)
            .await
            .map_err(|e| DataFusionError::Execution(format!("Riffle assignments: {e}")))?;

        // Build partition → server mapping
        let mut partition_to_server: HashMap<i32, (String, i32)> = HashMap::new();
        for assignment in &assignments {
            let server = assignment.server.first().ok_or_else(|| {
                DataFusionError::Execution("Assignment has no server".to_string())
            })?;
            for part_id in assignment.start_partition..=assignment.end_partition {
                partition_to_server.insert(part_id, (server.ip.clone(), server.port));
            }
        }

        // Register shuffle on each unique server
        let mut unique_servers: Vec<(String, i32)> = partition_to_server
            .values()
            .cloned()
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();
        unique_servers.sort();

        for (host, port) in &unique_servers {
            client
                .register_shuffle(shuffle_id, num_output_partitions as i32, host, *port)
                .await
                .map_err(|e| {
                    DataFusionError::Execution(format!(
                        "Riffle register shuffle on {host}:{port}: {e}"
                    ))
                })?;
        }

        // Accumulate batches per partition
        let mut buffers: Vec<Vec<datafusion::arrow::record_batch::RecordBatch>> =
            (0..num_output_partitions).map(|_| Vec::new()).collect();
        let mut row_counts: Vec<u64> = vec![0; num_output_partitions];

        let repart_time =
            datafusion::physical_plan::metrics::Time::new();
        let mut partitioner = BatchPartitioner::new_hash_partitioner(
            exprs,
            num_output_partitions,
            repart_time,
        );

        while let Some(result) = stream.next().await {
            let batch = result?;
            partitioner.partition(batch, |output_partition, output_batch| {
                row_counts[output_partition] += output_batch.num_rows() as u64;
                buffers[output_partition].push(output_batch);
                Ok(())
            })?;
        }

        // Push each partition to its assigned server
        let mut results = Vec::new();
        let mut server_block_ids: HashMap<(String, i32), Vec<(i32, Vec<i64>)>> =
            HashMap::new();

        for partition_id in 0..num_output_partitions {
            let (server_host, server_port) = partition_to_server
                .get(&(partition_id as i32))
                .ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "No server for partition {partition_id}"
                    ))
                })?;

            let num_rows = row_counts[partition_id];
            let num_batches = buffers[partition_id].len() as u64;
            let batches = std::mem::take(&mut buffers[partition_id]);

            let mut block_ids = Vec::new();

            if !batches.is_empty() {
                // Chunk by max_block_size
                let max_block_size = self.config.max_block_size;
                let mut chunk = Vec::new();
                let mut chunk_size: usize = 0;

                for batch in batches {
                    let batch_size = batch.get_array_memory_size();
                    if !chunk.is_empty() && chunk_size + batch_size > max_block_size {
                        let blk = push_chunk(
                            client, shuffle_id, partition_id as i32,
                            &chunk, schema, server_host, *server_port,
                        ).await?;
                        block_ids.push(blk);
                        chunk.clear();
                        chunk_size = 0;
                    }
                    chunk_size += batch_size;
                    chunk.push(batch);
                }
                if !chunk.is_empty() {
                    let blk = push_chunk(
                        client, shuffle_id, partition_id as i32,
                        &chunk, schema, server_host, *server_port,
                    ).await?;
                    block_ids.push(blk);
                }
            }

            server_block_ids
                .entry((server_host.clone(), *server_port))
                .or_default()
                .push((partition_id as i32, block_ids));

            results.push(ShuffleWritePartition {
                partition_id: partition_id as u64,
                path: String::new(),
                num_batches,
                num_rows,
                num_bytes: 0, // approximate
                remote_shuffle_app_id: self.config.app_id.clone(),
                remote_shuffle_id: shuffle_id,
                remote_shuffle_server_host: server_host.clone(),
                remote_shuffle_server_port: *server_port,
            });

            debug!(
                "Pushed partition {} to {}:{} ({} batches, {} rows)",
                partition_id, server_host, server_port, num_batches, num_rows
            );
        }

        // Report and commit per server
        for ((host, port), block_ids) in &server_block_ids {
            client
                .report_shuffle_result(shuffle_id, block_ids.clone(), host, *port)
                .await
                .map_err(|e| {
                    DataFusionError::Execution(format!("Riffle report on {host}:{port}: {e}"))
                })?;

            if let Err(e) = client.commit_shuffle_task(shuffle_id, host, *port).await {
                debug!("Riffle commit on {host}:{port} (non-fatal): {e}");
            }
        }

        Ok(results)
    }

    /// Execute without repartitioning — push single partition to Riffle.
    async fn execute_passthrough(
        &self,
        client: &RiffleClient,
        shuffle_id: i32,
        stream: &mut datafusion::physical_plan::SendableRecordBatchStream,
        schema: &SchemaRef,
        input_partition: usize,
    ) -> Result<Vec<ShuffleWritePartition>> {
        let assignments = client
            .get_shuffle_assignments(shuffle_id, 1)
            .await
            .map_err(|e| DataFusionError::Execution(format!("Riffle assignments: {e}")))?;

        let server = assignments[0].server.first().ok_or_else(|| {
            DataFusionError::Execution("No server assigned".to_string())
        })?;
        let host = &server.ip;
        let port = server.port;

        client
            .register_shuffle(shuffle_id, 1, host, port)
            .await
            .map_err(|e| DataFusionError::Execution(format!("Riffle register: {e}")))?;

        let mut batches = Vec::new();
        let mut num_rows: u64 = 0;
        while let Some(result) = stream.next().await {
            let batch = result?;
            num_rows += batch.num_rows() as u64;
            batches.push(batch);
        }

        let num_batches = batches.len() as u64;
        let mut block_ids = Vec::new();

        if !batches.is_empty() {
            let blk = push_chunk(
                client, shuffle_id, input_partition as i32,
                &batches, schema, host, port,
            ).await?;
            block_ids.push(blk);
        }

        client
            .report_shuffle_result(
                shuffle_id,
                vec![(input_partition as i32, block_ids)],
                host,
                port,
            )
            .await
            .map_err(|e| DataFusionError::Execution(format!("Riffle report: {e}")))?;

        Ok(vec![ShuffleWritePartition {
            partition_id: input_partition as u64,
            path: String::new(),
            num_batches,
            num_rows,
            num_bytes: 0,
            remote_shuffle_app_id: self.config.app_id.clone(),
            remote_shuffle_id: shuffle_id,
            remote_shuffle_server_host: host.clone(),
            remote_shuffle_server_port: port,
        }])
    }
}

/// Helper: serialize batches to IPC and push to Riffle.
async fn push_chunk(
    client: &RiffleClient,
    shuffle_id: i32,
    partition_id: i32,
    batches: &[datafusion::arrow::record_batch::RecordBatch],
    schema: &SchemaRef,
    host: &str,
    port: i32,
) -> Result<i64> {
    let ipc_bytes = record_batches_to_ipc_bytes(batches, schema)
        .map_err(|e| DataFusionError::Execution(format!("IPC serialize: {e}")))?;

    let buf_id = client
        .require_buffer(shuffle_id, ipc_bytes.len() as i32, vec![partition_id], host, port)
        .await
        .map_err(|e| DataFusionError::Execution(format!("Riffle require_buffer: {e}")))?;

    let block_id = client
        .send_shuffle_data(shuffle_id, buf_id, partition_id, ipc_bytes, 0, host, port)
        .await
        .map_err(|e| DataFusionError::Execution(format!("Riffle send_data: {e}")))?;

    Ok(block_id)
}
