#!/bin/bash
# Generate demo data for Velostream SQL file processing demo

# Create directories
mkdir -p demo_data demo_output

echo "📝 Generating sample financial transaction data..."

# Generate CSV header
echo "transaction_id,customer_id,amount,currency,timestamp,merchant_category,description" > demo_data/financial_transactions.csv

# Arrays for generating realistic data
customers=("CUST001" "CUST002" "CUST003" "CUST004" "CUST005" "CUST006" "CUST007" "CUST008" "CUST009" "CUST010" "CUST011" "CUST012" "CUST013" "CUST014" "CUST015" "CUST016" "CUST017" "CUST018" "CUST019" "CUST020" "CUST021" "CUST022" "CUST023" "CUST024" "CUST025")

grocery_merchants=("Whole Foods Market" "Safeway" "Trader Joe's" "Costco" "King Soopers" "Kroger" "Walmart Grocery" "Target Grocery" "Fresh Market" "Sprouts")
gas_merchants=("Shell Gas Station" "Chevron" "76 Gas Station" "Exxon" "BP" "Mobil" "Arco" "Conoco" "Valero" "Circle K")
restaurant_merchants=("McDonald's" "Cheesecake Factory" "The Capital Grille" "Olive Garden" "Chipotle" "In-N-Out" "Panera Bread" "Subway" "Taco Bell" "KFC")
retail_merchants=("Best Buy Electronics" "Amazon Purchase" "Apple Store" "Microsoft Store" "Target" "Walmart" "Home Depot" "Costco" "Best Buy" "GameStop")
coffee_merchants=("Starbucks" "Peet's Coffee" "Blue Bottle Coffee" "Dunkin'" "Tim Hortons" "Local Coffee Shop" "Philz Coffee" "Dutch Bros" "Coffee Bean" "Caribou Coffee")
entertainment_merchants=("Netflix" "Disney+" "Spotify" "AMC Theaters" "Regal Cinema" "iTunes Store" "PlayStation Store" "Steam" "Xbox Live" "Hulu")

# Base timestamp (start of 2024)
base_timestamp=1704110400

echo "Generating 5000 transactions..."

# Generate 5000 transactions
for i in $(seq 1 5000); do
    # Select random elements
    customer_idx=$((RANDOM % ${#customers[@]}))
    customer=${customers[$customer_idx]}
    
    # Select category and merchant
    case $((RANDOM % 6)) in
        0) category="grocery"; merchants_arr=(${grocery_merchants[@]}) ;;
        1) category="gas"; merchants_arr=(${gas_merchants[@]}) ;;
        2) category="restaurant"; merchants_arr=(${restaurant_merchants[@]}) ;;
        3) category="retail"; merchants_arr=(${retail_merchants[@]}) ;;
        4) category="coffee"; merchants_arr=(${coffee_merchants[@]}) ;;
        5) category="entertainment"; merchants_arr=(${entertainment_merchants[@]}) ;;
    esac
    
    merchant_idx=$((RANDOM % ${#merchants_arr[@]}))
    merchant=${merchants_arr[$merchant_idx]}
    
    # Generate realistic amount based on category (using bash arithmetic only)
    case $category in
        "grocery") base_cents=1550; max_add=20000 ;;      # $15.50 + up to $200.00
        "gas") base_cents=2500; max_add=12000 ;;          # $25.00 + up to $120.00
        "restaurant") base_cents=875; max_add=15000 ;;    # $8.75 + up to $150.00
        "retail") base_cents=1299; max_add=80000 ;;       # $12.99 + up to $800.00
        "coffee") base_cents=350; max_add=2500 ;;         # $3.50 + up to $25.00
        "entertainment") base_cents=499; max_add=5000 ;;  # $4.99 + up to $50.00
    esac
    
    # Generate amount in cents, then format as dollars
    random_add_cents=$((RANDOM % max_add))
    total_cents=$((base_cents + random_add_cents))
    dollars=$((total_cents / 100))
    cents=$((total_cents % 100))
    amount=$(printf "%d.%02d" $dollars $cents)
    
    # Generate timestamp (spread over 30 days with realistic daily patterns)
    day_offset=$((i * 2592 / 5000))  # Spread over 30 days (2592000 seconds / 5000 records)
    hour_variation=$((RANDOM % 86400))  # Random time within day
    timestamp=$((base_timestamp + day_offset + hour_variation))
    
    # Format transaction ID with leading zeros
    txn_id=$(printf "TXN%05d" $i)
    
    # Write CSV line
    echo "$txn_id,$customer,$amount,USD,$timestamp,$category,\"$merchant\"" >> demo_data/financial_transactions.csv
    
    # Progress indicator
    if [ $((i % 500)) -eq 0 ]; then
        echo "  Generated $i transactions..."
    fi
done

echo "✅ Generated 5000 sample transactions in demo_data/financial_transactions.csv"
echo ""
echo "📊 Data Summary:"
echo "   • 5000 financial transactions across 25 customers"
echo "   • 6 categories: grocery, gas, restaurant, retail, coffee, entertainment"
echo "   • 60 different merchants for realistic variety"
echo "   • Amount range: $3.50 - $812.99 (category-appropriate ranges)"
echo "   • Time span: 30 days with realistic daily patterns"
total_value=$(awk -F, 'NR>1 {sum+=$3} END {printf "%.2f", sum}' demo_data/financial_transactions.csv)
echo "   • Total transaction value: \$${total_value}"
echo "   • File size: $(du -h demo_data/financial_transactions.csv | cut -f1)"
echo ""
echo "🚀 Ready for high-performance SQL demo! Run:"
echo "   cargo run --bin velo-sql-multi --no-default-features -- server"
echo "   Then copy/paste the SQL from: file_processing_sql_demo.sql"
echo ""
echo "⚡ Performance Note:"
echo "   • 5000 records will showcase ScaledInteger exact precision arithmetic"
echo "   • Multi-level caching will demonstrate hot/warm/cold tier management"
echo "   • Real-time processing with windowed aggregations"
echo ""
echo "📁 Output files will be created in demo_output/ directory"