#!/usr/bin/env rust-script

//! Debug the big-endian encoding/decoding issue

fn main() {
    let original_value = 12345678i64;
    println!("🧪 Testing Big-Endian Encoding/Decoding");
    println!("======================================");
    println!("Original value: {}", original_value);
    println!("Original hex: 0x{:X}", original_value);
    println!("Original binary: {:064b}", original_value);
    println!();

    // Test the encoding
    let bytes = encode_big_endian_signed(original_value);
    println!("Encoded bytes: {:?}", bytes);
    println!(
        "Encoded hex: {:?}",
        bytes
            .iter()
            .map(|b| format!("{:02X}", b))
            .collect::<Vec<_>>()
    );
    println!();

    // Test the decoding
    let decoded_value = decode_big_endian_signed(&bytes);
    println!("Decoded value: {:?}", decoded_value);

    if let Some(decoded) = decoded_value {
        println!("Decoded: {}", decoded);
        println!("Decoded hex: 0x{:X}", decoded);
        println!("Decoded binary: {:064b}", decoded);
        println!();

        if decoded == original_value {
            println!("✅ SUCCESS: Round-trip works!");
        } else {
            println!("❌ FAILED: Values don't match!");
            println!("   Expected: {}", original_value);
            println!("   Got:      {}", decoded);
            println!("   Diff:     {}", decoded - original_value);
        }
    } else {
        println!("❌ FAILED: Decoding returned None");
    }

    // Test with raw big-endian conversion for comparison
    println!();
    println!("🔍 Raw big-endian test:");
    let raw_bytes = original_value.to_be_bytes();
    println!("Raw big-endian bytes: {:?}", raw_bytes);
    let raw_decoded = i64::from_be_bytes(raw_bytes);
    println!("Raw decoded: {}", raw_decoded);
}

fn encode_big_endian_signed(value: i64) -> Vec<u8> {
    // For Avro decimal logical type, we need to preserve the proper two's complement representation
    // We cannot simply remove leading bytes as it breaks sign interpretation

    // Convert to big-endian bytes
    let bytes = value.to_be_bytes();

    // For Avro decimal, find the minimal representation that preserves two's complement
    let significant_bytes = if value >= 0 {
        // For positive numbers, remove leading zeros but ensure high bit is 0
        let start = bytes
            .iter()
            .position(|&b| b != 0)
            .unwrap_or(bytes.len() - 1);
        let significant = &bytes[start..];

        // If the high bit is set, we need to add a 0x00 byte to preserve positive sign
        if (significant[0] & 0x80) != 0 {
            let mut result = vec![0x00];
            result.extend_from_slice(significant);
            result
        } else {
            significant.to_vec()
        }
    } else {
        // For negative numbers, remove leading 0xFF bytes but ensure sign is preserved
        let start = bytes
            .iter()
            .position(|&b| b != 0xFF)
            .unwrap_or(bytes.len() - 1);
        let significant = if start > 0 && (bytes[start] & 0x80) == 0 {
            // Need to keep one 0xFF byte to preserve the sign
            &bytes[start - 1..]
        } else {
            &bytes[start..]
        };
        significant.to_vec()
    };

    significant_bytes
}

fn decode_big_endian_signed(bytes: &[u8]) -> Option<i64> {
    if bytes.is_empty() || bytes.len() > 8 {
        return None;
    }

    // Convert big-endian bytes to i64
    let mut result = 0i64;
    let is_negative = (bytes[0] & 0x80) != 0;

    // Build the absolute value
    for &byte in bytes {
        result = result.checked_shl(8)?;
        result = result.checked_add(byte as i64)?;
    }

    // Handle two's complement for negative numbers
    if is_negative && bytes.len() < 8 {
        // Sign extend for negative numbers
        let sign_extension = !0i64 << (bytes.len() * 8);
        result |= sign_extension;
    }

    Some(result)
}
