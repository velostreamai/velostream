-- SQL Application: parsing_error_test
-- Version: 1.0.0
-- Description: Test SQL with parsing error

CREATE STREAM test_stream AS
SELECT 
    id,
    name || amount  -- This should cause the '|' parsing error
FROM test_source;