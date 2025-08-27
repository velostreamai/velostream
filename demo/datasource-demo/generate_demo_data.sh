#!/bin/bash
# Generate demo data for FerrisStreams SQL file processing demo

# Create directories
mkdir -p demo_data demo_output

echo "📝 Generating sample financial transaction data..."

# Generate the CSV file that the SQL demo expects
cat > demo_data/financial_transactions.csv << 'EOF'
transaction_id,customer_id,amount,currency,timestamp,merchant_category,description
TXN0001,CUST001,67.23,USD,1704110400,grocery,"Whole Foods Market"
TXN0002,CUST002,45.67,USD,1704111264,gas,"Shell Gas Station"
TXN0003,CUST003,123.45,USD,1704112128,restaurant,"McDonald's"
TXN0004,CUST004,89.12,USD,1704112992,retail,"Best Buy Electronics"
TXN0005,CUST005,234.56,USD,1704113856,grocery,"Safeway"
TXN0006,CUST006,12.34,USD,1704114720,coffee,"Starbucks"
TXN0007,CUST007,456.78,USD,1704115584,retail,"Amazon Purchase"
TXN0008,CUST008,78.90,USD,1704116448,gas,"Chevron"
TXN0009,CUST009,345.67,USD,1704117312,restaurant,"Cheesecake Factory"
TXN0010,CUST010,56.78,USD,1704118176,grocery,"Trader Joe's"
TXN0011,CUST011,987.65,USD,1704119040,retail,"Apple Store"
TXN0012,CUST012,23.45,USD,1704119904,coffee,"Peet's Coffee"
TXN0013,CUST013,765.43,USD,1704120768,restaurant,"The Capital Grille"
TXN0014,CUST014,134.56,USD,1704121632,grocery,"Whole Foods Market"
TXN0015,CUST015,298.76,USD,1704122496,retail,"Target"
TXN0016,CUST016,42.89,USD,1704123360,coffee,"Blue Bottle Coffee"
TXN0017,CUST017,189.34,USD,1704124224,grocery,"Costco"
TXN0018,CUST018,756.22,USD,1704125088,retail,"Microsoft Store"
TXN0019,CUST019,34.78,USD,1704125952,gas,"76 Gas Station"
TXN0020,CUST020,445.66,USD,1704126816,restaurant,"The French Laundry"
EOF

echo "✅ Generated 20 sample transactions in demo_data/financial_transactions.csv"
echo ""
echo "📊 Data Summary:"
echo "   • 20 financial transactions"
echo "   • Mix of retail, grocery, restaurant, gas, coffee purchases"
echo "   • Amount range: $12.34 - $987.65"
echo "   • Total transaction value: $(awk -F, 'NR>1 {sum+=$3} END {printf "%.2f", sum}' demo_data/financial_transactions.csv)"
echo ""
echo "🚀 Ready for SQL demo! Run:"
echo "   cargo run --bin ferris-sql --no-default-features"
echo "   Then copy/paste the SQL from: file_processing_sql_demo.sql"
echo ""
echo "📁 Output files will be created in demo_output/ directory"