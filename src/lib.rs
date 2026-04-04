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

//! Ballista Riffle - Uniffle remote shuffle service extension for Ballista.
//!
//! This crate provides a [Riffle](https://github.com/zuston/riffle) (Uniffle)
//! remote shuffle service integration for Apache Ballista. It can be used
//! as an extension following the pattern from
//! [ballista_extensions](https://github.com/milenkovicm/ballista_extensions).

pub mod client;
pub mod config;
pub mod error;
pub mod lifecycle;
pub mod serde;

/// Generated protobuf types for the Uniffle protocol.
pub mod proto {
    tonic::include_proto!("rss.common");
}
