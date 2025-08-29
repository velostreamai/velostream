#!/bin/bash

# List of files to migrate
FILES=$(find tests/unit/sql -name "*.rs" -exec grep -l "InternalValue\|execute_with_metadata\|\.execute(" {} \;)

for file in $FILES; do
    echo "Processing $file"
    
    # Replace imports
    sed -i '' 's/use ferrisstreams::ferris::serialization::{InternalValue, JsonFormat};/use ferrisstreams::ferris::serialization::JsonFormat;/g' "$file"
    
    # Add StreamRecord and FieldValue to imports if not present
    if ! grep -q "StreamRecord, FieldValue" "$file"; then
        sed -i '' 's/use ferrisstreams::ferris::sql::execution::StreamExecutionEngine;/use ferrisstreams::ferris::sql::execution::{StreamExecutionEngine, StreamRecord, FieldValue};/g' "$file"
    fi
    
    # Replace InternalValue with FieldValue
    sed -i '' 's/InternalValue::Integer/FieldValue::Integer/g' "$file"
    sed -i '' 's/InternalValue::Number/FieldValue::Float/g' "$file"
    sed -i '' 's/InternalValue::String/FieldValue::String/g' "$file"
    sed -i '' 's/InternalValue::Boolean/FieldValue::Boolean/g' "$file"
    sed -i '' 's/InternalValue::Null/FieldValue::Null/g' "$file"
    
    # Replace execute calls
    sed -i '' 's/engine\.execute(&/engine.execute_with_record(\&/g' "$file"
    sed -i '' 's/\.execute(&query, record/\.execute_with_record(\&query, record/g' "$file"
    
    # Replace field access patterns
    sed -i '' 's/output\.get("/output.fields.get("/g' "$file"
    sed -i '' 's/result\.get("/result.fields.get("/g' "$file"
    
    echo "  - Basic replacements done for $file"
done

echo "Migration script completed!"
