# String Functions

Complete reference for string manipulation functions in Velostream. Use these functions to process, format, and analyze text data in your streaming queries.

## String Creation and Concatenation

### CONCAT - Join Multiple Strings

```sql
-- Basic concatenation
SELECT
    customer_id,
    CONCAT('Customer: ', first_name, ' ', last_name) as full_name
FROM customers;

-- Build custom identifiers
SELECT
    order_id,
    CONCAT('ORD_', LPAD(order_id, 6, '0'), '_', customer_id) as tracking_number
FROM orders;

-- Create formatted addresses
SELECT
    CONCAT(street_address, ', ', city, ', ', state, ' ', zip_code) as full_address
FROM addresses
WHERE street_address IS NOT NULL;
```

### || Operator - Concatenation Alternative

```sql
-- Same as CONCAT but with || operator
SELECT
    customer_id,
    first_name || ' ' || last_name as full_name,
    'exported_' || UNIX_TIMESTAMP() as export_id
FROM customers;

-- Chain multiple concatenations
SELECT
    product_id,
    category || ' > ' || subcategory || ' > ' || product_name as product_path
FROM products;
```

## String Length and Measurement

### LENGTH and LEN - String Length

```sql
-- Check string lengths
SELECT
    product_name,
    LENGTH(product_name) as name_length,
    LEN(description) as desc_length  -- LEN is alias for LENGTH
FROM products
WHERE LENGTH(product_name) > 50;

-- Data quality check
SELECT
    COUNT(*) as total_records,
    COUNT(CASE WHEN LENGTH(email) > 0 THEN 1 END) as with_email,
    AVG(LENGTH(description)) as avg_description_length
FROM customers;
```

## Case Conversion

### UPPER - Convert to Uppercase

```sql
-- Standardize text case
SELECT
    customer_id,
    UPPER(first_name) as first_name_upper,
    UPPER(company_name) as company_name_upper
FROM customers;

-- Clean product categories
SELECT DISTINCT
    UPPER(TRIM(category)) as clean_category
FROM products
ORDER BY clean_category;
```

### LOWER - Convert to Lowercase

```sql
-- Normalize email addresses
SELECT
    customer_id,
    LOWER(TRIM(email)) as normalized_email
FROM customers
WHERE email IS NOT NULL;

-- Standardize search terms
SELECT
    search_term,
    LOWER(search_term) as normalized_term,
    COUNT(*) as search_count
FROM search_logs
GROUP BY LOWER(search_term)
ORDER BY search_count DESC;
```

## Whitespace Management

### TRIM - Remove Leading and Trailing Whitespace

```sql
-- Clean data input
SELECT
    customer_id,
    TRIM(first_name) as clean_first_name,
    TRIM(last_name) as clean_last_name,
    LENGTH(TRIM(phone)) as clean_phone_length
FROM customers;

-- Remove extra whitespace from descriptions
SELECT
    product_id,
    TRIM(description) as clean_description
FROM products
WHERE description != TRIM(description);  -- Find records with extra whitespace
```

### LTRIM and RTRIM - Left/Right Trimming

```sql
-- Remove whitespace from specific sides
SELECT
    data_field,
    LTRIM(data_field) as left_trimmed,
    RTRIM(data_field) as right_trimmed,
    TRIM(data_field) as both_trimmed
FROM raw_data
WHERE data_field LIKE ' %' OR data_field LIKE '% ';
```

## String Replacement

### REPLACE - Replace Text Patterns

```sql
-- Clean phone numbers
SELECT
    customer_id,
    phone,
    REPLACE(REPLACE(REPLACE(phone, '-', ''), '(', ''), ')', '') as clean_phone
FROM customers;

-- Standardize data formats
SELECT
    product_code,
    REPLACE(product_code, '_', '-') as standardized_code
FROM products;

-- Remove unwanted characters
SELECT
    user_input,
    REPLACE(REPLACE(user_input, '\n', ' '), '\t', ' ') as cleaned_input
FROM user_submissions;
```

## String Extraction

### SUBSTRING - Extract Portions of Strings

```sql
-- Extract parts of codes
SELECT
    product_code,
    SUBSTRING(product_code, 1, 3) as category_code,
    SUBSTRING(product_code, 4, 2) as subcategory_code,
    SUBSTRING(product_code, 6) as item_code
FROM products;

-- Extract domain from email
SELECT
    email,
    SUBSTRING(email, POSITION('@', email) + 1) as email_domain
FROM customers
WHERE POSITION('@', email) > 0;

-- Get first N characters
SELECT
    long_description,
    SUBSTRING(long_description, 1, 100) as preview
FROM articles
WHERE LENGTH(long_description) > 100;
```

### LEFT and RIGHT - Extract from Ends

```sql
-- Extract prefixes and suffixes
SELECT
    order_id,
    LEFT(order_id, 3) as order_prefix,
    RIGHT(order_id, 4) as order_suffix
FROM orders;

-- Get area code from phone numbers
SELECT
    phone,
    LEFT(phone, 3) as area_code,
    RIGHT(phone, 4) as last_four_digits
FROM customers
WHERE LENGTH(phone) = 10;

-- Extract file extensions
SELECT
    filename,
    RIGHT(filename, 3) as file_extension
FROM uploaded_files
WHERE POSITION('.', filename) > 0;
```

## String Search

### POSITION - Find Substring Location

```sql
-- Find position of @ in email addresses
SELECT
    email,
    POSITION('@', email) as at_position,
    CASE
        WHEN POSITION('@', email) = 0 THEN 'Invalid'
        WHEN POSITION('@', email) = 1 THEN 'Missing local part'
        WHEN POSITION('@', email) = LENGTH(email) THEN 'Missing domain'
        ELSE 'Valid format'
    END as email_status
FROM customers;

-- Find multiple occurrences
SELECT
    content,
    POSITION('error', LOWER(content)) as first_error_pos,
    POSITION('warning', LOWER(content)) as first_warning_pos
FROM log_entries
WHERE LOWER(content) LIKE '%error%' OR LOWER(content) LIKE '%warning%';

-- Advanced parsing with multiple positions
SELECT
    url,
    POSITION('://', url) as protocol_end,
    POSITION('/', url, POSITION('://', url) + 3) as path_start
FROM web_requests
WHERE url LIKE 'http%';
```

### LIKE - Pattern Matching

```sql
-- Basic pattern matching
SELECT * FROM products WHERE product_name LIKE 'Wireless%';
SELECT * FROM customers WHERE email LIKE '%@gmail.com';
SELECT * FROM orders WHERE order_id LIKE 'ORD_____';  -- Exactly 8 chars after ORD_

-- Case-insensitive searches
SELECT * FROM products
WHERE LOWER(description) LIKE '%bluetooth%'
   OR LOWER(description) LIKE '%wireless%';

-- Multiple patterns
SELECT
    product_name,
    CASE
        WHEN product_name LIKE '%Pro%' THEN 'Professional'
        WHEN product_name LIKE '%Basic%' THEN 'Entry Level'
        WHEN product_name LIKE '%Premium%' THEN 'High End'
        ELSE 'Standard'
    END as product_tier
FROM products;
```

## Advanced String Processing

### Complex Email Parsing

```sql
-- Extract detailed email information
SELECT
    email,
    SUBSTRING(email, 1, POSITION('@', email) - 1) as local_part,
    SUBSTRING(email, POSITION('@', email) + 1) as domain_part,
    CASE
        WHEN POSITION('.', SUBSTRING(email, POSITION('@', email) + 1)) > 0
        THEN RIGHT(email, LENGTH(email) - POSITION('.', email, POSITION('@', email)))
        ELSE 'unknown'
    END as top_level_domain,
    LENGTH(email) as email_length
FROM customers
WHERE email LIKE '%@%.%';
```

### URL Parsing

```sql
-- Parse URL components
SELECT
    url,
    CASE
        WHEN url LIKE 'https://%' THEN 'HTTPS'
        WHEN url LIKE 'http://%' THEN 'HTTP'
        ELSE 'OTHER'
    END as protocol,
    SUBSTRING(
        url,
        POSITION('://', url) + 3,
        POSITION('/', url, POSITION('://', url) + 3) - POSITION('://', url) - 3
    ) as domain,
    SUBSTRING(url, POSITION('/', url, POSITION('://', url) + 3)) as path
FROM web_requests
WHERE url LIKE 'http%://%';
```

### Data Cleaning Pipeline

```sql
-- Comprehensive string cleaning
SELECT
    raw_data,
    TRIM(
        REPLACE(
            REPLACE(
                REPLACE(
                    UPPER(raw_data),
                    '\n', ' '
                ),
                '\t', ' '
            ),
            '  ', ' '  -- Replace double spaces with single
        )
    ) as cleaned_data
FROM user_inputs
WHERE raw_data IS NOT NULL;
```

## Text Analysis and Metrics

### String Comparison and Analysis

```sql
-- Analyze text patterns
SELECT
    category,
    COUNT(*) as product_count,
    AVG(LENGTH(product_name)) as avg_name_length,
    MIN(LENGTH(product_name)) as min_name_length,
    MAX(LENGTH(product_name)) as max_name_length,
    COUNT(CASE WHEN product_name LIKE '% %' THEN 1 END) as names_with_spaces,
    COUNT(CASE WHEN UPPER(product_name) = product_name THEN 1 END) as all_uppercase_names
FROM products
GROUP BY category
ORDER BY product_count DESC;
```

### Content Analysis

```sql
-- Analyze customer feedback
SELECT
    feedback_text,
    LENGTH(feedback_text) as text_length,
    LENGTH(feedback_text) - LENGTH(REPLACE(LOWER(feedback_text), ' ', '')) + 1 as word_count,
    CASE
        WHEN LOWER(feedback_text) LIKE '%excellent%' OR LOWER(feedback_text) LIKE '%great%' THEN 'Positive'
        WHEN LOWER(feedback_text) LIKE '%terrible%' OR LOWER(feedback_text) LIKE '%awful%' THEN 'Negative'
        ELSE 'Neutral'
    END as sentiment,
    CASE
        WHEN LENGTH(feedback_text) > 500 THEN 'Detailed'
        WHEN LENGTH(feedback_text) > 100 THEN 'Moderate'
        ELSE 'Brief'
    END as feedback_length_category
FROM customer_feedback
WHERE feedback_text IS NOT NULL;
```

## Real-World Examples

### E-commerce Product Standardization

```sql
-- Clean and standardize product data
SELECT
    product_id,
    TRIM(UPPER(product_name)) as standardized_name,
    TRIM(LOWER(REPLACE(category, '_', ' '))) as clean_category,
    CONCAT(
        TRIM(brand),
        ' - ',
        TRIM(product_name),
        ' (', sku, ')'
    ) as display_name,
    CASE
        WHEN LENGTH(TRIM(description)) = 0 THEN 'No description'
        WHEN LENGTH(TRIM(description)) > 200 THEN CONCAT(SUBSTRING(TRIM(description), 1, 200), '...')
        ELSE TRIM(description)
    END as formatted_description
FROM products
WHERE product_name IS NOT NULL;
```

### Log Analysis

```sql
-- Parse and analyze log entries
SELECT
    log_entry,
    SUBSTRING(log_entry, 1, 19) as timestamp_str,
    CASE
        WHEN log_entry LIKE '%ERROR%' THEN 'ERROR'
        WHEN log_entry LIKE '%WARN%' THEN 'WARNING'
        WHEN log_entry LIKE '%INFO%' THEN 'INFO'
        ELSE 'UNKNOWN'
    END as log_level,
    SUBSTRING(
        log_entry,
        POSITION('] ', log_entry) + 2
    ) as message_content,
    LENGTH(log_entry) as entry_length
FROM application_logs
WHERE log_entry IS NOT NULL
ORDER BY timestamp_str DESC;
```

### Customer Data Normalization

```sql
-- Normalize customer contact information
SELECT
    customer_id,
    TRIM(UPPER(CONCAT(first_name, ' ', last_name))) as full_name,
    LOWER(TRIM(email)) as normalized_email,
    REPLACE(REPLACE(REPLACE(phone, '-', ''), '(', ''), ')', '') as clean_phone,
    TRIM(REPLACE(REPLACE(address, '\n', ' '), '  ', ' ')) as clean_address,
    CASE
        WHEN email LIKE '%@gmail.%' THEN 'Gmail'
        WHEN email LIKE '%@yahoo.%' THEN 'Yahoo'
        WHEN email LIKE '%@outlook.%' OR email LIKE '%@hotmail.%' THEN 'Microsoft'
        ELSE 'Other'
    END as email_provider
FROM customers
WHERE email IS NOT NULL;
```

## Performance Tips

### Efficient String Operations

```sql
-- ✅ Good: Use functions efficiently
SELECT product_name
FROM products
WHERE UPPER(category) = 'ELECTRONICS';  -- Consider creating functional index

-- ✅ Good: Avoid repeated function calls
SELECT
    email,
    LOWER(TRIM(email)) as clean_email  -- Calculate once, use multiple times
FROM customers;

-- ⚠️ Careful: Complex string operations on large datasets
SELECT REPLACE(REPLACE(REPLACE(text_field, 'a', 'b'), 'c', 'd'), 'e', 'f')
FROM large_table;  -- Consider pre-processing or indexing
```

### String Index Usage

```sql
-- Create functional indexes for commonly searched patterns
-- (This is conceptual - actual index creation depends on your database)
-- CREATE INDEX idx_customer_email_domain ON customers (SUBSTRING(email, POSITION('@', email) + 1));
-- CREATE INDEX idx_product_clean_name ON products (UPPER(TRIM(product_name)));
```

## Common Patterns

### Data Validation

```sql
-- Validate data formats
SELECT
    email,
    CASE
        WHEN email IS NULL THEN 'Missing'
        WHEN POSITION('@', email) = 0 THEN 'No @ symbol'
        WHEN POSITION('.', email, POSITION('@', email)) = 0 THEN 'No domain extension'
        WHEN LENGTH(email) < 5 THEN 'Too short'
        ELSE 'Valid format'
    END as email_validation
FROM customers;
```

### Text Parsing

```sql
-- Parse CSV-like data
SELECT
    csv_data,
    SUBSTRING(csv_data, 1, POSITION(',', csv_data) - 1) as field1,
    SUBSTRING(
        csv_data,
        POSITION(',', csv_data) + 1,
        POSITION(',', csv_data, POSITION(',', csv_data) + 1) - POSITION(',', csv_data) - 1
    ) as field2
FROM raw_csv_data
WHERE POSITION(',', csv_data) > 0;
```

## Quick Reference

| Function | Purpose | Example |
|----------|---------|---------|
| `CONCAT(a, b, c)` | Join strings | `CONCAT('Hello', ' ', 'World')` |
| `LENGTH(text)` / `LEN(text)` | String length | `LENGTH('Hello')` → 5 |
| `UPPER(text)` | Convert to uppercase | `UPPER('hello')` → 'HELLO' |
| `LOWER(text)` | Convert to lowercase | `LOWER('HELLO')` → 'hello' |
| `TRIM(text)` | Remove whitespace | `TRIM(' hello ')` → 'hello' |
| `REPLACE(text, old, new)` | Replace text | `REPLACE('hello', 'l', 'x')` → 'hexxo' |
| `SUBSTRING(text, start, len)` | Extract substring | `SUBSTRING('hello', 2, 3)` → 'ell' |
| `LEFT(text, n)` | Left n characters | `LEFT('hello', 2)` → 'he' |
| `RIGHT(text, n)` | Right n characters | `RIGHT('hello', 2)` → 'lo' |
| `POSITION(substr, text)` | Find position | `POSITION('l', 'hello')` → 3 |
| `text LIKE pattern` | Pattern matching | `'hello' LIKE 'h%'` → true |

## Next Steps

- [Essential functions](essential.md) - Most commonly used functions
- [Date/time functions](date-time.md) - Date manipulation
- [Pattern detection](../by-task/detect-patterns.md) - Advanced text pattern matching