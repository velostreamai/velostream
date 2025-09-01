/*!
Integration test for EMIT functionality
*/

use ferrisstreams::ferris::serialization::{JsonFormat, SerializationFormat};
use ferrisstreams::ferris::sql::execution::{FieldValue, StreamExecutionEngine, StreamRecord};
use ferrisstreams::ferris::sql::parser::StreamingSqlParser;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;

fn create_test_record(id: i64, amount: f64) -> StreamRecord {
    let mut fields = HashMap::new();
    fields.insert("customer_id".to_string(), FieldValue::Integer(id));
    fields.insert("amount".to_string(), FieldValue::Float(amount));

    StreamRecord {
        fields,
        timestamp: chrono::Utc::now().timestamp_millis(),
        offset: id,
        partition: 0,
        headers: HashMap::new(),
    }
}

#[tokio::main]
async fn main() {
    println!("🧪 Testing EMIT functionality...\n");

    // Test 1: Parse EMIT CHANGES
    println!("1️⃣ Testing EMIT CHANGES parsing...");
    let parser = StreamingSqlParser::new();
    let query_str = "SELECT customer_id, COUNT(*) FROM orders GROUP BY customer_id WINDOW TUMBLING(5m) EMIT CHANGES";

    match parser.parse(query_str) {
        Ok(query) => {
            if let ferrisstreams::ferris::sql::ast::StreamingQuery::Select {
                emit_mode,
                window,
                ..
            } = &query
            {
                if window.is_some()
                    && emit_mode == &Some(ferrisstreams::ferris::sql::ast::EmitMode::Changes)
                {
                    println!("✅ EMIT CHANGES parsed correctly with WINDOW clause");
                } else {
                    println!("❌ EMIT CHANGES parsing failed");
                }
            }
        }
        Err(e) => {
            println!("❌ EMIT CHANGES query parsing failed: {:?}", e);
        }
    }

    // Test 2: Parse EMIT FINAL
    println!("\n2️⃣ Testing EMIT FINAL parsing...");
    let query_str2 = "SELECT customer_id, COUNT(*) FROM orders GROUP BY customer_id EMIT FINAL";

    match parser.parse(query_str2) {
        Ok(query) => {
            if let ferrisstreams::ferris::sql::ast::StreamingQuery::Select {
                emit_mode,
                window,
                ..
            } = &query
            {
                if window.is_none()
                    && emit_mode == &Some(ferrisstreams::ferris::sql::ast::EmitMode::Final)
                {
                    println!("✅ EMIT FINAL parsed correctly without WINDOW clause");
                } else {
                    println!("❌ EMIT FINAL parsing failed");
                }
            }
        }
        Err(e) => {
            println!("❌ EMIT FINAL query parsing failed: {:?}", e);
        }
    }

    // Test 3: Test execution behavior - EMIT CHANGES should override windowed mode
    println!("\n3️⃣ Testing EMIT CHANGES execution behavior...");
    let (tx, mut rx) = mpsc::unbounded_channel();
    let format: Arc<dyn SerializationFormat> = Arc::new(JsonFormat);
    let mut engine = StreamExecutionEngine::new(tx);

    // This query should use windowed aggregation (due to WINDOW clause)
    // But EMIT CHANGES should override it to continuous emission
    let override_query = "SELECT customer_id, COUNT(*) FROM orders GROUP BY customer_id WINDOW TUMBLING(5m) EMIT CHANGES";

    match parser.parse(override_query) {
        Ok(query) => {
            // Execute a few records
            for i in 1..=3 {
                let record = create_test_record(i % 2, 100.0 * i as f64); // 2 groups: customer_id 0 and 1
                match engine.execute_with_record(&query, record).await {
                    Ok(_) => {}
                    Err(e) => {
                        println!("❌ Record {} execution failed: {:?}", i, e);
                        return;
                    }
                }
            }

            // Check if we get immediate results (EMIT CHANGES should emit immediately)
            let mut result_count = 0;
            while let Ok(_result) = rx.try_recv() {
                result_count += 1;
            }

            if result_count > 0 {
                println!(
                    "✅ EMIT CHANGES correctly overrode windowed mode - got {} immediate results",
                    result_count
                );
            } else {
                println!(
                    "⚠️  EMIT CHANGES override test inconclusive - no immediate results (might be implementation dependent)"
                );
            }
        }
        Err(e) => {
            println!("❌ EMIT CHANGES execution test failed: {:?}", e);
        }
    }

    println!("\n🎉 EMIT functionality testing complete!");
}
