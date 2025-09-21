#!/usr/bin/env python3

"""
Simple test to check if subqueries are working in VeloStream
"""

import subprocess
import sys
import os

def test_subquery_parsing():
    """Test if basic subquery parsing works"""

    # Create a simple SQL file with subqueries
    sql_content = """
    SELECT
        id,
        name,
        (SELECT 100) as config_value
    FROM test_stream
    WHERE EXISTS (SELECT 1 FROM config WHERE active = true)
      AND id IN (SELECT valid_id FROM allowed_ids);
    """

    # Write to temporary file
    sql_file = "/tmp/test_subquery.sql"
    with open(sql_file, 'w') as f:
        f.write(sql_content)

    # Use the SQL validator to test parsing
    try:
        result = subprocess.run([
            "./target/release/sql_validator",
            sql_file
        ], cwd="/Users/neilavery/RustroverProjects/velostream",
           capture_output=True, text=True, timeout=30)

        print("=== SUBQUERY PARSING TEST ===")
        print(f"Return code: {result.returncode}")
        print(f"STDOUT:\n{result.stdout}")
        print(f"STDERR:\n{result.stderr}")

        # Check if parsing succeeded (should parse even if execution fails)
        if "parsing error" in result.stderr.lower() or "syntax error" in result.stderr.lower():
            print("❌ SUBQUERY PARSING FAILED")
            return False
        else:
            print("✅ SUBQUERY PARSING SUCCEEDED")
            return True

    except subprocess.TimeoutExpired:
        print("❌ TIMEOUT")
        return False
    except Exception as e:
        print(f"❌ ERROR: {e}")
        return False
    finally:
        # Clean up
        if os.path.exists(sql_file):
            os.remove(sql_file)

if __name__ == "__main__":
    success = test_subquery_parsing()
    sys.exit(0 if success else 1)