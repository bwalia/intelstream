use bytes::Bytes;
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use intelstream_core::message::Message;
use intelstream_core::storage::{CommitLog, RetentionPolicy, StorageConfig};
use tempfile::TempDir;

fn bench_commit_log_append(c: &mut Criterion) {
    let tmp = TempDir::new().unwrap();
    let config = StorageConfig {
        data_dir: tmp.path().to_string_lossy().to_string(),
        segment_size_bytes: 1_073_741_824,
        max_message_size_bytes: 10_485_760,
        ..Default::default()
    };
    let mut log = CommitLog::open(tmp.path(), config, RetentionPolicy::default()).unwrap();

    let payload = Bytes::from(vec![0u8; 1024]); // 1 KiB message

    let mut group = c.benchmark_group("commit_log");
    group.throughput(Throughput::Bytes(1024));

    group.bench_function("append_1kb", |b| {
        b.iter(|| {
            let msg = Message::new(Some(Bytes::from("key")), payload.clone());
            log.append(&msg).unwrap();
        });
    });

    group.finish();
}

criterion_group!(benches, bench_commit_log_append);
criterion_main!(benches);
