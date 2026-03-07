//! High-throughput benchmark simulating message production and consumption.
//!
//! Run with: `cargo bench --bench throughput`

use bytes::Bytes;
use criterion::{criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use intelstream_core::message::Message;
use intelstream_core::storage::{CommitLog, RetentionPolicy, StorageConfig};
use tempfile::TempDir;

/// Benchmark: append N messages to a commit log (simulates single-partition throughput).
fn bench_sequential_produce(c: &mut Criterion) {
    let mut group = c.benchmark_group("sequential_produce");

    for message_size in [128, 1024, 4096, 16384] {
        group.throughput(Throughput::Bytes(message_size as u64));

        group.bench_function(format!("{}B_message", message_size), |b| {
            b.iter_batched(
                || {
                    let tmp = TempDir::new().unwrap();
                    let config = StorageConfig {
                        segment_size_bytes: 256 * 1024 * 1024, // 256 MiB
                        max_message_size_bytes: 1_048_576,
                        ..Default::default()
                    };
                    let log = CommitLog::open(
                        tmp.path(),
                        config,
                        RetentionPolicy::default(),
                    )
                    .unwrap();
                    let payload = Bytes::from(vec![0u8; message_size]);
                    (tmp, log, payload)
                },
                |(_tmp, mut log, payload)| {
                    let msg = Message::new(Some(Bytes::from("key")), payload);
                    log.append(&msg).unwrap();
                },
                BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

/// Benchmark: batch append (simulates batched producer).
fn bench_batch_produce(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_produce");
    let batch_size = 100;
    let message_size = 1024;

    group.throughput(Throughput::Elements(batch_size));

    group.bench_function(
        format!("batch_{}_msgs_{}B", batch_size, message_size),
        |b| {
            b.iter_batched(
                || {
                    let tmp = TempDir::new().unwrap();
                    let config = StorageConfig {
                        segment_size_bytes: 256 * 1024 * 1024,
                        max_message_size_bytes: 1_048_576,
                        ..Default::default()
                    };
                    let log = CommitLog::open(
                        tmp.path(),
                        config,
                        RetentionPolicy::default(),
                    )
                    .unwrap();
                    let messages: Vec<Message> = (0..batch_size)
                        .map(|_| {
                            Message::new(
                                Some(Bytes::from("key")),
                                Bytes::from(vec![0u8; message_size as usize]),
                            )
                        })
                        .collect();
                    (tmp, log, messages)
                },
                |(_tmp, mut log, messages)| {
                    for msg in &messages {
                        log.append(msg).unwrap();
                    }
                    log.flush().unwrap();
                },
                BatchSize::SmallInput,
            );
        },
    );

    group.finish();
}

criterion_group!(benches, bench_sequential_produce, bench_batch_produce);
criterion_main!(benches);
