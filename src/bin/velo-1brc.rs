//! One Billion Row Challenge (1BRC) — Velostream Edition
//!
//! Data generator for the 1BRC challenge. Produces `station;temperature` measurement data
//! and an `expected.csv` with correct MIN/AVG/MAX per station for validation.
//!
//! Processing is handled by Velostream's SQL engine via `velo-test run demo/1brc/1brc.sql`.
//!
//! ## Usage
//!
//! ```bash
//! # Generate 1M rows + expected results (default)
//! velo-1brc generate --rows 1 --expected-output expected.csv
//!
//! # Reproducible generation with seed
//! velo-1brc generate --rows 1 --seed 42
//!
//! # Generate 1B rows
//! velo-1brc generate --rows 1000
//! ```

use clap::{Parser, Subcommand};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use rand_distr::{Distribution, Normal};
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::time::Instant;

#[derive(Parser)]
#[command(name = "velo-1brc")]
#[command(about = "One Billion Row Challenge — Velostream Edition (data generator)")]
#[command(version = "1.0.0")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Generate measurement data
    Generate {
        /// Number of rows in millions (default: 1 = 1M rows)
        #[arg(short, long, default_value = "1")]
        rows: u64,

        /// Output file path
        #[arg(short, long, default_value = "measurements.txt")]
        output: String,

        /// Path for expected results CSV (station,min_temp,avg_temp,max_temp)
        #[arg(short, long, default_value = "expected.csv")]
        expected_output: String,

        /// Random seed for reproducible generation (default: random)
        #[arg(short, long)]
        seed: Option<u64>,
    },
}

// --- Station data from the original 1BRC challenge ---
// (station_name, mean_temperature)
const STATIONS: &[(&str, f64)] = &[
    ("Abha", 18.0),
    ("Abidjan", 26.0),
    ("Abéché", 29.4),
    ("Accra", 26.4),
    ("Addis Ababa", 16.0),
    ("Adelaide", 17.3),
    ("Aden", 29.1),
    ("Ahvaz", 25.4),
    ("Albuquerque", 14.0),
    ("Alexandra", 11.0),
    ("Alexandria", 20.0),
    ("Algiers", 18.2),
    ("Alice Springs", 21.0),
    ("Almaty", 10.0),
    ("Amsterdam", 10.2),
    ("Anadyr", -6.9),
    ("Anchorage", 2.8),
    ("Andorra la Vella", 9.8),
    ("Ankara", 12.0),
    ("Antananarivo", 17.9),
    ("Antsiranana", 25.2),
    ("Arkhangelsk", 1.3),
    ("Ashgabat", 17.1),
    ("Asmara", 15.6),
    ("Assab", 30.5),
    ("Astana", 3.5),
    ("Athens", 19.2),
    ("Atlanta", 17.0),
    ("Auckland", 15.2),
    ("Austin", 20.7),
    ("Baghdad", 22.77),
    ("Baku", 15.1),
    ("Baltimore", 13.1),
    ("Bamako", 27.8),
    ("Bangkok", 28.6),
    ("Bangui", 26.0),
    ("Banjul", 26.0),
    ("Barcelona", 18.2),
    ("Bata", 25.1),
    ("Batumi", 14.0),
    ("Beijing", 12.9),
    ("Beirut", 20.9),
    ("Belgrade", 12.5),
    ("Belize City", 26.7),
    ("Benghazi", 19.9),
    ("Bergen", 7.7),
    ("Berlin", 10.3),
    ("Bilbao", 14.7),
    ("Birao", 26.5),
    ("Bishkek", 11.3),
    ("Bissau", 27.0),
    ("Blantyre", 22.2),
    ("Bloemfontein", 15.6),
    ("Boise", 11.4),
    ("Bordeaux", 14.2),
    ("Bosaso", 30.0),
    ("Boston", 10.9),
    ("Bouaké", 26.0),
    ("Brasilia", 21.4),
    ("Bratislava", 10.5),
    ("Brazzaville", 25.0),
    ("Bridgetown", 27.0),
    ("Brisbane", 21.4),
    ("Brussels", 10.5),
    ("Bucharest", 10.8),
    ("Budapest", 11.3),
    ("Bujumbura", 23.8),
    ("Bulawayo", 18.9),
    ("Bursa", 14.6),
    ("Busan", 15.0),
    ("Cabo San Lucas", 23.9),
    ("Cairns", 25.0),
    ("Cairo", 21.4),
    ("Calgary", 4.4),
    ("Canberra", 13.1),
    ("Cape Town", 16.2),
    ("Casablanca", 17.7),
    ("Cayenne", 27.4),
    ("Charlotte", 16.1),
    ("Chengdu", 17.0),
    ("Chennai", 28.6),
    ("Chicago", 9.8),
    ("Chihuahua", 18.6),
    ("Chittagong", 25.9),
    ("Chișinău", 10.2),
    ("Choibalsan", -1.3),
    ("Christchurch", 12.2),
    ("City of San Marino", 11.8),
    ("Colombo", 27.4),
    ("Columbus", 11.7),
    ("Conakry", 26.4),
    ("Copenhagen", 9.1),
    ("Cotonou", 27.2),
    ("Cracow", 9.3),
    ("Da Lat", 17.9),
    ("Da Nang", 25.8),
    ("Dakar", 24.0),
    ("Dallas", 19.0),
    ("Damascus", 17.0),
    ("Dampier", 26.4),
    ("Dar es Salaam", 25.8),
    ("Darwin", 27.6),
    ("Denpasar", 23.7),
    ("Denver", 10.4),
    ("Detroit", 10.0),
    ("Dhaka", 25.9),
    ("Dikson", -11.1),
    ("Dili", 26.6),
    ("Djibouti", 29.9),
    ("Dodoma", 22.7),
    ("Dolisie", 24.0),
    ("Douala", 26.7),
    ("Dubai", 26.9),
    ("Dublin", 9.8),
    ("Dunedin", 11.1),
    ("Durban", 20.6),
    ("Dushanbe", 14.7),
    ("Edinburgh", 9.3),
    ("Edmonton", 4.2),
    ("El Paso", 18.1),
    ("Entebbe", 21.0),
    ("Erbil", 19.5),
    ("Erzurum", 5.1),
    ("Fairbanks", -2.3),
    ("Fianarantsoa", 17.9),
    ("Flores, Petén", 26.4),
    ("Frankfurt", 10.6),
    ("Freetown", 26.8),
    ("Funchal", 19.3),
    ("Gaborone", 21.0),
    ("Garissa", 29.3),
    ("Garoua", 28.3),
    ("George Town", 27.9),
    ("Ghanzi", 21.4),
    ("Gjoa Haven", -14.4),
    ("Guadalajara", 20.9),
    ("Guangzhou", 22.4),
    ("Guatemala City", 20.4),
    ("Halifax", 7.5),
    ("Hamburg", 9.7),
    ("Hamilton", 13.8),
    ("Hanoi", 23.6),
    ("Harare", 18.4),
    ("Harbin", 5.0),
    ("Hargeisa", 21.7),
    ("Hat Yai", 27.0),
    ("Havana", 25.2),
    ("Helsinki", 5.9),
    ("Heraklion", 18.9),
    ("Hiroshima", 16.3),
    ("Ho Chi Minh City", 27.4),
    ("Hobart", 12.7),
    ("Hong Kong", 23.3),
    ("Honiara", 26.5),
    ("Honolulu", 25.4),
    ("Houston", 20.8),
    ("Ifrane", 11.4),
    ("Indianapolis", 11.8),
    ("Iqaluit", -9.3),
    ("Irkutsk", 1.0),
    ("Istanbul", 13.9),
    ("Jacksonville", 20.3),
    ("Jakarta", 26.7),
    ("Jayapura", 27.0),
    ("Jerusalem", 18.3),
    ("Johannesburg", 15.5),
    ("Jos", 22.8),
    ("Juba", 27.8),
    ("Kabul", 12.1),
    ("Kampala", 20.0),
    ("Kandi", 27.7),
    ("Kankan", 26.5),
    ("Kano", 26.4),
    ("Kansas City", 12.5),
    ("Karachi", 26.0),
    ("Karonga", 24.4),
    ("Kathmandu", 18.3),
    ("Khartoum", 29.9),
    ("Kingston", 27.4),
    ("Kinshasa", 25.3),
    ("Kolkata", 26.7),
    ("Kuala Lumpur", 27.3),
    ("Kumasi", 25.9),
    ("Kunming", 15.7),
    ("Kuopio", 3.4),
    ("Kuwait City", 25.7),
    ("Kyiv", 8.4),
    ("Kyoto", 15.8),
    ("La Ceiba", 26.2),
    ("La Paz", 23.7),
    ("Lagos", 26.8),
    ("Lahore", 24.3),
    ("Lake Havasu City", 23.7),
    ("Lake Tekapo", 8.7),
    ("Las Palmas de Gran Canaria", 21.2),
    ("Las Vegas", 20.3),
    ("Launceston", 13.1),
    ("Lhasa", 7.6),
    ("Libreville", 25.9),
    ("Lisbon", 17.5),
    ("Livingstone", 21.8),
    ("Ljubljana", 10.9),
    ("Lodwar", 29.3),
    ("Lomé", 26.9),
    ("London", 11.3),
    ("Los Angeles", 18.6),
    ("Louisville", 13.9),
    ("Luanda", 25.8),
    ("Lubumbashi", 20.8),
    ("Lusaka", 19.9),
    ("Luxembourg City", 9.3),
    ("Lviv", 7.8),
    ("Lyon", 12.5),
    ("Macau", 22.9),
    ("Madison", 7.3),
    ("Mahajanga", 26.3),
    ("Makassar", 26.7),
    ("Makurdi", 26.0),
    ("Malabo", 26.3),
    ("Malé", 28.0),
    ("Managua", 27.3),
    ("Manama", 26.5),
    ("Mandalay", 28.0),
    ("Mangalore", 27.1),
    ("Manila", 28.4),
    ("Maputo", 22.8),
    ("Marrakesh", 19.6),
    ("Marseille", 15.8),
    ("Maun", 22.4),
    ("Medan", 26.5),
    ("Mek'ele", 22.7),
    ("Melbourne", 15.1),
    ("Memphis", 17.2),
    ("Mexicali", 23.1),
    ("Mexico City", 17.5),
    ("Miami", 24.9),
    ("Milan", 12.9),
    ("Milwaukee", 8.9),
    ("Minneapolis", 7.8),
    ("Minsk", 6.7),
    ("Mogadishu", 27.1),
    ("Mombasa", 26.3),
    ("Monaco", 16.4),
    ("Moncton", 6.1),
    ("Monterrey", 22.3),
    ("Montreal", 6.8),
    ("Moscow", 5.8),
    ("Mumbai", 27.1),
    ("Murmansk", 0.6),
    ("Muscat", 28.0),
    ("Mzuzu", 17.7),
    ("N'Djamena", 28.3),
    ("Naha", 23.1),
    ("Nairobi", 17.8),
    ("Nakhon Ratchasima", 27.3),
    ("Napier", 14.6),
    ("Naples", 15.9),
    ("Nashville", 15.4),
    ("Nassau", 25.0),
    ("Ndola", 20.3),
    ("New Delhi", 25.0),
    ("New Orleans", 20.7),
    ("New York City", 12.9),
    ("Ngaoundéré", 22.0),
    ("Niamey", 29.3),
    ("Nicosia", 19.7),
    ("Niigata", 13.9),
    ("Nouadhibou", 21.3),
    ("Nouakchott", 25.7),
    ("Novosibirsk", 1.7),
    ("Nuuk", -1.4),
    ("Odesa", 10.7),
    ("Omaha", 10.6),
    ("Oranjestad", 28.1),
    ("Oslo", 5.7),
    ("Ottawa", 6.6),
    ("Ouagadougou", 28.3),
    ("Ouarzazate", 18.9),
    ("Oulu", 2.7),
    ("Palembang", 27.3),
    ("Palermo", 18.5),
    ("Palm Springs", 24.5),
    ("Palmerston North", 13.2),
    ("Panama City", 28.0),
    ("Paramaribo", 27.6),
    ("Paris", 12.3),
    ("Patna", 25.6),
    ("Perth", 18.7),
    ("Petropavlovsk-Kamchatsky", 1.9),
    ("Philadelphia", 13.2),
    ("Phnom Penh", 28.3),
    ("Phoenix", 23.9),
    ("Pittsburgh", 10.8),
    ("Podgorica", 15.3),
    ("Pointe-Noire", 26.1),
    ("Pontianak", 27.7),
    ("Port Moresby", 26.9),
    ("Port Sudan", 28.4),
    ("Port Vila", 24.3),
    ("Port-Gentil", 26.0),
    ("Portland (OR)", 12.4),
    ("Porto", 15.7),
    ("Prague", 8.4),
    ("Praia", 24.4),
    ("Pretoria", 18.2),
    ("Pyongyang", 10.8),
    ("Québec", 4.0),
    ("Quito", 15.0),
    ("Rabat", 17.2),
    ("Rangpur", 24.4),
    ("Rashid", 22.4),
    ("Rawalpindi", 22.2),
    ("Recife", 25.8),
    ("Reykjavik", 4.3),
    ("Riga", 6.2),
    ("Riyadh", 26.0),
    ("Rome", 15.2),
    ("Roseau", 26.2),
    ("Rostov-on-Don", 9.9),
    ("Sacramento", 16.3),
    ("Saint Petersburg", 5.8),
    ("Salt Lake City", 11.6),
    ("San Antonio", 20.8),
    ("San Diego", 17.8),
    ("San Francisco", 14.6),
    ("San Jose", 16.4),
    ("San José", 22.6),
    ("San Juan", 27.2),
    ("San Salvador", 23.1),
    ("Sana'a", 20.0),
    ("Santo Domingo", 25.9),
    ("Sapporo", 8.9),
    ("Sarajevo", 10.1),
    ("Saskatoon", 3.3),
    ("Seattle", 11.3),
    ("Seoul", 12.5),
    ("Seville", 19.2),
    ("Shanghai", 16.7),
    ("Singapore", 27.0),
    ("Skopje", 12.4),
    ("Sochi", 14.2),
    ("Sofia", 10.6),
    ("Sokoto", 28.0),
    ("Split", 16.1),
    ("St. John's", 5.0),
    ("St. Louis", 13.9),
    ("Stockholm", 6.6),
    ("Surabaya", 27.1),
    ("Suva", 25.6),
    ("Suwałki", 7.2),
    ("Sydney", 17.7),
    ("Tabora", 23.0),
    ("Tabriz", 12.6),
    ("Taipei", 23.0),
    ("Tallinn", 6.4),
    ("Tamale", 27.9),
    ("Tamanrasset", 21.7),
    ("Tampa", 22.9),
    ("Tashkent", 14.8),
    ("Tauranga", 14.8),
    ("Tbilisi", 12.9),
    ("Tegucigalpa", 21.7),
    ("Tehran", 17.0),
    ("Tel Aviv", 20.0),
    ("Thessaloniki", 16.0),
    ("Thiès", 24.0),
    ("Tijuana", 17.8),
    ("Timbuktu", 28.0),
    ("Tirana", 15.2),
    ("Tokyo", 15.4),
    ("Toluca", 12.4),
    ("Toronto", 9.4),
    ("Tripoli", 20.0),
    ("Tromsø", 2.9),
    ("Tucson", 20.9),
    ("Tunis", 18.4),
    ("Ulaanbaatar", -0.4),
    ("Upington", 20.4),
    ("Ürümqi", 7.4),
    ("Vaduz", 10.1),
    ("Valencia", 18.3),
    ("Valletta", 18.8),
    ("Vancouver", 10.4),
    ("Veracruz", 25.4),
    ("Vienna", 10.4),
    ("Vientiane", 25.9),
    ("Vilnius", 6.0),
    ("Virginia Beach", 15.8),
    ("Vladivostok", 4.9),
    ("Warsaw", 8.5),
    ("Washington, D.C.", 14.6),
    ("Wau", 27.8),
    ("Wellington", 12.9),
    ("Whitehorse", -0.1),
    ("Wichita", 13.9),
    ("Willemstad", 28.0),
    ("Winnipeg", 3.0),
    ("Wrocław", 9.6),
    ("Xi'an", 14.1),
    ("Yakutsk", -8.8),
    ("Yangon", 27.5),
    ("Yaoundé", 23.8),
    ("Yellowknife", -4.3),
    ("Yerevan", 12.4),
    ("Yokohama", 15.5),
    ("Zagreb", 10.7),
    ("Zanzibar City", 26.0),
    ("Zürich", 9.3),
];

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Commands::Generate {
            rows,
            output,
            expected_output,
            seed,
        } => cmd_generate(rows, &output, &expected_output, seed),
    }
}

// =============================================================================
// GENERATE SUBCOMMAND
// =============================================================================

/// Per-station aggregation tracker used during data generation
struct StationStats {
    min: f64,
    max: f64,
    sum: f64,
    count: u64,
}

impl StationStats {
    fn new(temp: f64) -> Self {
        Self {
            min: temp,
            max: temp,
            sum: temp,
            count: 1,
        }
    }

    fn update(&mut self, temp: f64) {
        self.min = self.min.min(temp);
        self.max = self.max.max(temp);
        self.sum += temp;
        self.count += 1;
    }

    fn avg(&self) -> f64 {
        self.sum / self.count as f64
    }
}

fn cmd_generate(rows_millions: u64, output_path: &str, expected_path: &str, seed: Option<u64>) {
    let total_rows = rows_millions * 1_000_000;
    eprintln!(
        "Generating {} rows ({} million) to {}",
        total_rows, rows_millions, output_path
    );

    let start = Instant::now();
    let file = File::create(output_path).expect("Failed to create output file");
    let mut writer = BufWriter::with_capacity(8 * 1024 * 1024, file);

    let mut rng = match seed {
        Some(s) => SmallRng::seed_from_u64(s),
        None => SmallRng::from_entropy(),
    };
    let normal = Normal::new(0.0, 10.0).unwrap();

    // Track per-station stats for expected output
    let mut stats: BTreeMap<String, StationStats> = BTreeMap::new();

    // Write CSV header
    writeln!(writer, "station;temperature").unwrap();

    for _ in 0..total_rows {
        let (station, mean) = STATIONS[rng.gen_range(0..STATIONS.len())];
        let temp = (mean + normal.sample(&mut rng)).clamp(-99.9, 99.9);
        // Round to 1 decimal (matches the written precision)
        let temp = (temp * 10.0).round() / 10.0;

        // Track stats
        stats
            .entry(station.to_string())
            .and_modify(|s| s.update(temp))
            .or_insert_with(|| StationStats::new(temp));

        // Write with exactly one decimal place
        writeln!(writer, "{};{:.1}", station, temp).unwrap();
    }

    writer.flush().unwrap();

    let elapsed = start.elapsed();
    let file_size = std::fs::metadata(output_path).unwrap().len();
    eprintln!(
        "Generated in {:.2}s ({:.1} MB, {:.1} M rows/s)",
        elapsed.as_secs_f64(),
        file_size as f64 / (1024.0 * 1024.0),
        total_rows as f64 / elapsed.as_secs_f64() / 1_000_000.0,
    );

    // Write expected results CSV (sorted by station name via BTreeMap)
    let expected_file = File::create(expected_path).expect("Failed to create expected output file");
    let mut expected_writer = BufWriter::new(expected_file);
    writeln!(expected_writer, "station,min_temp,avg_temp,max_temp").unwrap();
    for (station, s) in &stats {
        // CSV-quote station names containing commas (matches SQL engine's CSV sink quoting)
        let station_csv = if station.contains(',') || station.contains('"') {
            format!("\"{}\"", station.replace('"', "\"\""))
        } else {
            station.clone()
        };
        // Use default f64 formatting (no trailing .0) to match SQL engine's f.to_string()
        writeln!(
            expected_writer,
            "{},{},{},{}",
            station_csv,
            s.min,
            s.avg(),
            s.max
        )
        .unwrap();
    }
    expected_writer.flush().unwrap();
    eprintln!(
        "Expected results written to {} ({} stations)",
        expected_path,
        stats.len()
    );
}
