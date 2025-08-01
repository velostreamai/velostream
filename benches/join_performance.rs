/*!
# JOIN Performance Benchmarks

Benchmarks for different JOIN types and scenarios to measure performance characteristics.
*/

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use ferrisstreams::ferris::sql::execution::{FieldValue, StreamExecutionEngine, StreamRecord};
use ferrisstreams::ferris::sql::parser::StreamingSqlParser;
use std::collections::HashMap;
use tokio::runtime::Runtime;
use tokio::sync::mpsc;

fn create_test_record_with_id(id: i64, name: &str, amount: f64) -> StreamRecord {
    let mut fields = HashMap::new();
    fields.insert("id".to_string(), FieldValue::Integer(id));
    fields.insert("name".to_string(), FieldValue::String(name.to_string()));
    fields.insert("amount".to_string(), FieldValue::Float(amount));

    StreamRecord {
        fields,
        headers: HashMap::new(),
        timestamp: 1234567890000,
        offset: id,
        partition: 0,
    }
}

fn create_test_records(count: usize) -> Vec<StreamRecord> {
    (0..count)
        .map(|i| create_test_record_with_id(i as i64, &format!("user_{}", i), i as f64 * 10.0))
        .collect()
}

fn benchmark_join_parsing(c: &mut Criterion) {
    let parser = StreamingSqlParser::new();

    c.bench_function("parse_inner_join", |b| {
        b.iter(|| {
            let query = "SELECT * FROM left_stream INNER JOIN right_stream ON left_stream.id = right_stream.right_id";
            black_box(parser.parse(query))
        })
    });

    c.bench_function("parse_left_join", |b| {
        b.iter(|| {
            let query = "SELECT * FROM left_stream LEFT JOIN right_stream ON left_stream.id = right_stream.right_id";
            black_box(parser.parse(query))
        })
    });

    c.bench_function("parse_windowed_join", |b| {
        b.iter(|| {
            let query = "SELECT * FROM left_stream INNER JOIN right_stream ON left_stream.id = right_stream.right_id WITHIN INTERVAL '5' MINUTES";
            black_box(parser.parse(query))
        })
    });

    c.bench_function("parse_complex_join", |b| {
        b.iter(|| {
            let query = "SELECT l.name, l.amount, r.status FROM left_stream l LEFT JOIN right_stream r ON l.id = r.right_id AND l.amount > 100";
            black_box(parser.parse(query))
        })
    });
}

fn benchmark_join_execution(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    c.bench_function("execute_inner_join_small", |b| {
        b.iter(|| {
            rt.block_on(async {
                let (tx, mut rx) = mpsc::unbounded_channel();
                let mut engine = StreamExecutionEngine::new(tx);
                let parser = StreamingSqlParser::new();

                let query = "SELECT * FROM left_stream INNER JOIN right_stream ON left_stream.id = right_stream.right_id";
                let parsed_query = parser.parse(query).unwrap();
                let record = create_test_record_with_id(1, "test", 100.0);

                let json_record: HashMap<String, serde_json::Value> = record.fields.into_iter().map(|(k, v)| {
                    let json_val = match v {
                        FieldValue::Integer(i) => serde_json::Value::Number(serde_json::Number::from(i)),
                        FieldValue::Float(f) => serde_json::Value::Number(serde_json::Number::from_f64(f).unwrap_or(0.into())),
                        FieldValue::String(s) => serde_json::Value::String(s),
                        _ => serde_json::Value::Null,
                    };
                    (k, json_val)
                }).collect();

                black_box(engine.execute(&parsed_query, json_record).await)
            })
        })
    });
}

fn benchmark_join_types_comparison(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let parser = StreamingSqlParser::new();

    let join_queries = vec![
        (
            "inner_join",
            "SELECT * FROM left_stream INNER JOIN right_stream ON left_stream.id = right_stream.right_id",
        ),
        (
            "left_join",
            "SELECT * FROM left_stream LEFT JOIN right_stream ON left_stream.id = right_stream.right_id",
        ),
        (
            "right_join",
            "SELECT * FROM left_stream RIGHT JOIN right_stream ON left_stream.id = right_stream.right_id",
        ),
        (
            "full_outer_join",
            "SELECT * FROM left_stream FULL OUTER JOIN right_stream ON left_stream.id = right_stream.right_id",
        ),
    ];

    for (name, query) in join_queries {
        c.bench_function(&format!("execute_{}", name), |b| {
            b.iter(|| {
                rt.block_on(async {
                    let (tx, mut rx) = mpsc::unbounded_channel();
                    let mut engine = StreamExecutionEngine::new(tx);

                    let parsed_query = parser.parse(query).unwrap();
                    let record = create_test_record_with_id(1, "test", 100.0);

                    let json_record: HashMap<String, serde_json::Value> = record
                        .fields
                        .into_iter()
                        .map(|(k, v)| {
                            let json_val = match v {
                                FieldValue::Integer(i) => {
                                    serde_json::Value::Number(serde_json::Number::from(i))
                                }
                                FieldValue::Float(f) => serde_json::Value::Number(
                                    serde_json::Number::from_f64(f).unwrap_or(0.into()),
                                ),
                                FieldValue::String(s) => serde_json::Value::String(s),
                                _ => serde_json::Value::Null,
                            };
                            (k, json_val)
                        })
                        .collect();

                    black_box(engine.execute(&parsed_query, json_record).await)
                })
            })
        });
    }
}

criterion_group!(
    join_benchmarks,
    benchmark_join_parsing,
    benchmark_join_execution,
    benchmark_join_types_comparison
);
criterion_main!(join_benchmarks);
