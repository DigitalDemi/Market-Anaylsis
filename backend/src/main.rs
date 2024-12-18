use axum::{extract::Query, routing::get, Json, Router};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::RowAccessor as _;
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::{
    env,
    path::{Path, PathBuf},
};

// Type definitions for standardized properties
#[derive(Debug, Serialize, Deserialize)]
struct Address {
    display_address: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct Size {
    value: f64,
    unit: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct PriceChange {
    date: String,
    amount: f64,
    direction: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct Price {
    amount: f64,
    currency: String,
    frequency: Option<String>,
    price_changes: Vec<PriceChange>,
}

#[derive(Debug, Serialize, Deserialize)]
struct Photo {
    url: String,
    is_main: bool,
}

#[derive(Debug, Serialize, Deserialize)]
struct Agent {
    name: String,
    phone: String,
    email: String,
    address: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct StandardizedProperty {
    property_id: String,
    source: String,
    source_id: String,
    address: Address,
    property_type: String,
    bedrooms: Option<i32>,
    bathrooms: Option<i32>,
    size: Option<Size>,
    ber_rating: Option<String>,
    price: Price,
    created_date: String,
    updated_date: String,
    listing_type: String,
    status: String,
    photos: Vec<Photo>,
    has_video: bool,
    agent: Option<Agent>,
}

// Source-specific types
#[derive(Debug, Serialize, Deserialize)]
struct MyHomeProperty {
    property_id: i64,
    display_address: String,
    property_type: String,
    number_of_beds: Option<i32>,
    number_of_bathrooms: i32,
    size_string_meters: Option<f64>,
    ber_rating: Option<String>,
    price_as_string: String,
    latest_price_change_string: Option<String>,
    created_on_date: String,
    refreshed_on: String,
    is_active: bool,
    main_photo: String,
    has_videos: bool,
    group_name: String,
    group_phone_number: String,
    group_email: String,
    group_address: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct PropertyIEListing {
    address: String,
    price: String,
}

// Search parameters
#[derive(Debug, Deserialize)]
struct SearchParams {
    source: Option<String>,
    min_price: Option<f64>,
    max_price: Option<f64>,
    bedrooms: Option<i32>,
    property_type: Option<String>,
    ber_rating: Option<String>,
}

impl StandardizedProperty {
    fn from_myhome(raw: MyHomeProperty) -> Self {
        let price_amount = raw
            .price_as_string
            .trim_start_matches('€')
            .split('/')
            .next()
            .and_then(|s| s.replace(",", "").trim().parse::<f64>().ok())
            .unwrap_or(0.0);

        StandardizedProperty {
            property_id: format!("myhome_{}", raw.property_id),
            source: "myhome".to_string(),
            source_id: raw.property_id.to_string(),
            address: Address {
                display_address: raw.display_address,
            },
            property_type: raw.property_type,
            bedrooms: raw.number_of_beds,
            bathrooms: Some(raw.number_of_bathrooms),
            size: raw.size_string_meters.map(|v| Size {
                value: v,
                unit: "square_meters".to_string(),
            }),
            ber_rating: raw.ber_rating,
            price: Price {
                amount: price_amount,
                currency: "EUR".to_string(),
                frequency: Some("month".to_string()),
                price_changes: vec![],
            },
            created_date: raw.created_on_date,
            updated_date: raw.refreshed_on,
            listing_type: "rent".to_string(),
            status: if raw.is_active { "active" } else { "inactive" }.to_string(),
            photos: vec![Photo {
                url: raw.main_photo,
                is_main: true,
            }],
            has_video: raw.has_videos,
            agent: Some(Agent {
                name: raw.group_name,
                phone: raw.group_phone_number,
                email: raw.group_email,
                address: raw.group_address,
            }),
        }
    }

    fn from_property_ie(raw: PropertyIEListing) -> Self {
        let price_amount = raw
            .price
            .trim_start_matches('€')
            .split('/')
            .next()
            .and_then(|s| s.replace(",", "").trim().parse::<f64>().ok())
            .unwrap_or(0.0);

        StandardizedProperty {
            property_id: format!(
                "property_{}",
                raw.address
                    .chars()
                    .filter(|c| c.is_alphanumeric())
                    .collect::<String>()
            ),
            source: "property".to_string(),
            source_id: raw.address.clone(), // Using address as source_id since we don't have a unique identifier
            address: Address {
                display_address: raw.address,
            },
            property_type: String::new(), // Not available in data
            bedrooms: None,               // Not available in data
            bathrooms: None,              // Not available in data
            size: None,                   // Not available in data
            ber_rating: None,             // Not available in data
            price: Price {
                amount: price_amount,
                currency: "EUR".to_string(),
                frequency: Some("month".to_string()),
                price_changes: vec![],
            },
            created_date: chrono::Local::now().to_rfc3339(), // Using current time as we don't have actual dates
            updated_date: chrono::Local::now().to_rfc3339(),
            listing_type: "rent".to_string(),
            status: "active".to_string(),
            photos: vec![],   // Not available in data
            has_video: false, // Not available in data
            agent: None,      // Not available in data
        }
        
    }
}

// Helper function to find latest parquet file
fn find_latest_parquet(source: &str, base_path: &str) -> Option<PathBuf> {
    println!("Looking for parquet files in base path: {}", base_path);
    let source_path = Path::new(base_path).join("processed").join(source);

    println!("Source path: {:?}", source_path);

    // Get all years
    let years: Vec<_> = fs::read_dir(&source_path)
        .ok()?
        .filter_map(|entry| {
            entry.ok().and_then(|e| {
                e.path()
                    .file_name()
                    .and_then(|n| n.to_str())
                    .and_then(|s| s.parse::<i32>().ok())
                    .map(|year| (year, e.path()))
            })
        })
        .collect();

    let latest_year = years.iter().max_by_key(|(year, _)| year)?;

    // Get all months in latest year
    let months: Vec<_> = fs::read_dir(&latest_year.1)
        .ok()?
        .filter_map(|entry| {
            entry.ok().and_then(|e| {
                e.path()
                    .file_name()
                    .and_then(|n| n.to_str())
                    .and_then(|s| s.parse::<i32>().ok())
                    .map(|month| (month, e.path()))
            })
        })
        .collect();

    let latest_month = months.iter().max_by_key(|(month, _)| month)?;

    // Get all days in latest month
    let days: Vec<_> = fs::read_dir(&latest_month.1)
        .ok()?
        .filter_map(|entry| {
            entry.ok().and_then(|e| {
                e.path()
                    .file_name()
                    .and_then(|n| n.to_str())
                    .and_then(|s| s.parse::<i32>().ok())
                    .map(|day| (day, e.path()))
            })
        })
        .collect();

    let latest_day = days.iter().max_by_key(|(day, _)| day)?;

    // Get latest parquet file in the latest day
    fs::read_dir(&latest_day.1)
        .ok()?
        .filter_map(|entry| entry.ok().map(|e| e.path()))
        .filter(|path| path.extension().map_or(false, |ext| ext == "parquet"))
        .max_by_key(|path| path.metadata().ok().map(|m| m.modified().ok()).flatten())
}

// API endpoints
async fn health_check() -> &'static str {
    "OK"
}

async fn debug_paths() -> String {
    let current_dir = env::current_dir().unwrap_or_default();
    let data_path = current_dir.join("housing_data");

    format!(
        "Current directory: {:?}\nData path: {:?}\nExists: {}",
        current_dir,
        data_path,
        data_path.exists()
    )
}

fn debug_row_structure(row: &parquet::record::Row) {
    println!("Debug: Row structure:");
    for i in 0..row.len() {
        match row.get_group(i) {
            Ok(group) => println!("Group at index {}: {} columns", i, group.len()),
            Err(_) => match row.get_string(i) {
                Ok(s) => println!("String at index {}: {}", i, s),
                Err(_) => println!("Other type at index {}", i),
            },
        }
    }
}


async fn search_rentals(Query(params): Query<SearchParams>) -> Json<Vec<StandardizedProperty>> {
    let mut properties = Vec::new();
    let sources = vec!["daft", "myhome", "property"];

    println!("Starting search with params: {:?}", params);

    let data_path = "housing_data";

    for source in sources {
        if let Some(source_filter) = &params.source {
            if source != source_filter {
                continue;
            }
        }

        if let Some(latest_file) = find_latest_parquet(source, data_path) {
            println!("Processing file: {:?}", latest_file);

            if let Ok(file) = File::open(&latest_file) {
                if let Ok(reader) = SerializedFileReader::new(file) {
                    let metadata = reader.metadata().file_metadata().schema();
                    println!("File schema: {:?}", metadata);

                    if let Ok(iter) = reader.get_row_iter(None) {
                        for (index, row_result) in iter.enumerate() {
                            match row_result {
                                Ok(row) => {
                                    let property = match source {
                                        "property" => {
                                            let address = row
                                                .get_string(0)
                                                .map(|s| s.to_string())
                                                .unwrap_or_default();
                                            let price_string = row
                                                .get_string(1)
                                                .map(|s| s.to_string())
                                                .unwrap_or_default();

                                            let raw_property = PropertyIEListing {
                                                address,
                                                price: price_string,
                                            };

                                            StandardizedProperty::from_property_ie(raw_property)
                                        },
                                        "myhome" => {
                                            let property_id = row.get_long(0).unwrap_or_default();
                                            let display_address = row
                                                .get_string(1)
                                                .map(|s| s.to_string())
                                                .unwrap_or_default();
                                            let property_type = row
                                                .get_string(2)
                                                .map(|s| s.to_string())
                                                .unwrap_or_default();
                                            let bedrooms = row.get_long(3).ok().map(|b| b as i32);
                                            let bathrooms =
                                                row.get_long(4).unwrap_or_default() as i32;
                                            let size = row.get_double(5);
                                            let ber_rating =
                                                row.get_string(6).map(|s| s.to_string()).ok();
                                            let price_string = row
                                                .get_string(7)
                                                .map(|s| s.to_string())
                                                .unwrap_or_default();
                                            let created_date = row
                                                .get_string(8)
                                                .map(|s| s.to_string())
                                                .unwrap_or_default();
                                            let updated_date = row
                                                .get_string(9)
                                                .map(|s| s.to_string())
                                                .unwrap_or_default();
                                            let is_active = row.get_bool(10).unwrap_or_default();

                                            StandardizedProperty {
                                                property_id: format!("myhome_{}", property_id),
                                                source: "myhome".to_string(),
                                                source_id: property_id.to_string(),
                                                address: Address { display_address },
                                                property_type,
                                                bedrooms,
                                                bathrooms: Some(bathrooms),
                                                size: size.ok().map(|v| Size {
                                                    value: v,
                                                    unit: "square_meters".to_string(),
                                                }),
                                                ber_rating,
                                                price: Price {
                                                    amount: price_string
                                                        .trim_start_matches('€')
                                                        .split('/')
                                                        .next()
                                                        .and_then(|s| {
                                                            s.replace(",", "")
                                                                .trim()
                                                                .parse::<f64>()
                                                                .ok()
                                                        })
                                                        .unwrap_or(0.0),
                                                    currency: "EUR".to_string(),
                                                    frequency: Some("month".to_string()),
                                                    price_changes: vec![],
                                                },
                                                created_date,
                                                updated_date,
                                                listing_type: "rent".to_string(),
                                                status: if is_active {
                                                    "active"
                                                } else {
                                                    "inactive"
                                                }
                                                .to_string(),
                                                photos: vec![],
                                                has_video: false,
                                                agent: None,
                                            }
                                        },
                                     "daft" => {
                                        debug_row_structure(&row);
                                        match parse_daft_row(&row) {
                                            Some(property) => {
                                                println!("Successfully parsed Daft property: {} - {}", 
                                                        property.property_id, 
                                                        property.address.display_address);
                                                property
                                            },
                                            None => {
                                                println!("Failed to parse Daft property row, skipping");
                                                continue;
                                            }
                                        }
                                    },
                                        _ => continue,
                                    };

                                    // Apply filters
                                    let mut should_include = true;

                                    if let Some(min_price) = params.min_price {
                                        if property.price.amount < min_price {
                                            should_include = false;
                                        }
                                    }

                                    if let Some(max_price) = params.max_price {
                                        if property.price.amount > max_price {
                                            should_include = false;
                                        }
                                    }

                                    if let Some(bedrooms) = params.bedrooms {
                                        if property.bedrooms != Some(bedrooms) {
                                            should_include = false;
                                        }
                                    }

                                    if let Some(ref prop_type) = params.property_type {
                                        if property.property_type.to_lowercase()
                                            != prop_type.to_lowercase()
                                        {
                                            should_include = false;
                                        }
                                    }

                                    if let Some(ref ber) = params.ber_rating {
                                        if property.ber_rating.as_ref().map(|b| b.to_lowercase())
                                            != Some(ber.to_lowercase())
                                        {
                                            should_include = false;
                                        }
                                    }

                                    if should_include {
                                        println!(
                                            "Adding property: {} - {}",
                                            property.property_id, property.address.display_address
                                        );
                                        properties.push(property);
                                    }
                                }
                                Err(e) => println!("Error reading row {}: {}", index, e),
                            }
                        }
                    }
                }
            }
        }
    }

    println!("Found {} properties", properties.len());
    Json(properties)
}


fn get_group_field_string(row: &parquet::record::Row, group_idx: usize, field_idx: usize) -> Option<String> {
    match row.get_group(group_idx) {
        Ok(group) => match group.get_string(field_idx) {
            Ok(value) => Some(value.to_string()),
            Err(_) => None,
        },
        Err(_) => None,
    }
}

fn parse_daft_row(row: &parquet::record::Row) -> Option<StandardizedProperty> {
    // Get the listing group (should be at index 0)
    let listing = match row.get_group(0) {
        Ok(l) => l,
        Err(e) => {
            println!("Error getting listing group: {}", e);
            return None;
        }
    };

    // Column indices for the listing group
    // These indices should match your Parquet schema
    const PRICE_IDX: usize = 0;        // abbreviatedPrice
    const BER_GROUP_IDX: usize = 1;    // ber group
    const CATEGORY_IDX: usize = 2;     // category
    const ID_IDX: usize = 7;          // id
    const MEDIA_GROUP_IDX: usize = 8;  // media group
    const BATHROOMS_IDX: usize = 9;    // numBathrooms
    const BEDROOMS_IDX: usize = 10;    // numBedrooms

    // Get ID first (required field)
    let id = match listing.get_long(ID_IDX) {
        Ok(id) => id.to_string(),
        Err(_) => return None,
    };

    // Get price
    let price = match listing.get_string(PRICE_IDX) {
        Ok(p) => p.to_string(),
        Err(_) => "0".to_string(),
    };

    // Get BER rating from nested structure
    let ber_rating = match listing.get_group(BER_GROUP_IDX) {
        Ok(ber) => match ber.get_string(2) { // rating should be at index 2 in ber group
            Ok(rating) => Some(rating.to_string()),
            Err(_) => None,
        },
        Err(_) => None,
    };

    // Get bedrooms and bathrooms
    let bedrooms = match listing.get_string(BEDROOMS_IDX) {
        Ok(beds) => beds.parse::<i32>().ok(),
        Err(_) => None,
    };

    let bathrooms = match listing.get_string(BATHROOMS_IDX) {
        Ok(baths) => baths.parse::<i32>().ok(),
        Err(_) => None,
    };

    // Get property category
    let property_type = match listing.get_string(CATEGORY_IDX) {
        Ok(cat) => cat.to_string(),
        Err(_) => "Not specified".to_string(),
    };

    // Get media information
    let has_video = match listing.get_group(MEDIA_GROUP_IDX) {
        Ok(media) => match media.get_bool(2) { // hasVideo should be at index 2 in media group
            Ok(has_video) => has_video,
            Err(_) => false,
        },
        Err(_) => false,
    };

    // Parse price to float
    let price_amount = price
        .trim_start_matches('€')
        .split('/')
        .next()
        .and_then(|s| s.replace(",", "").trim().parse::<f64>().ok())
        .unwrap_or(0.0);

    Some(StandardizedProperty {
        property_id: format!("daft_{}", id),
        source: "daft".to_string(),
        source_id: id,
        address: Address {
            display_address: property_type.clone(), // Using category as address since title isn't directly available
        },
        property_type,
        bedrooms,
        bathrooms,
        size: None,
        ber_rating,
        price: Price {
            amount: price_amount,
            currency: "EUR".to_string(),
            frequency: Some("month".to_string()),
            price_changes: vec![],
        },
        created_date: chrono::Local::now().to_rfc3339(),
        updated_date: chrono::Local::now().to_rfc3339(),
        listing_type: "rent".to_string(),
        status: "active".to_string(),
        photos: vec![],
        has_video,
        agent: None,
    })
}

#[cfg(test)]
mod tests{
    // Using AAA for testing
    // assert -> true
    // assert eq -> left = right
    // assert ne -> left != right
    // partionaleq and debug
    use reqwest::{Client, Url};
    const BASE_URL: &str = "http://localhost:3000";
    const BASE_PATH: &str = "housing_data";

    #[tokio::test]
    async fn check_connection() -> Result<(), Box<dyn std::error::Error>>{
        let client = Client::new();
        let url = Url::parse(BASE_URL)?.join("/health")?;

        let response = client
            .get(url)
            .send()
            .await?;
    
        assert_eq!(response.status(), 200);
        let body = response.text().await?;
        assert_eq!(body, "OK");

        Ok(())
        
    }
    #[test]
    fn does_directory_exsits(){
        use std::path::Path;
        let path = Path::new(BASE_PATH);
        assert!(path.exists());
    }

}
   

#[tokio::main]
async fn main() {
    // Initialize tracing for logging
    tracing_subscriber::fmt::init();

    // Build the router
    let app = Router::new()
        .route("/health", get(health_check))
        .route("/api/rentals/search", get(search_rentals))
        .route("/debug/paths", get(debug_paths));

    // Create the server
    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000")
        .await
        .unwrap();
    println!("Server listening on {}", listener.local_addr().unwrap());
    axum::serve(listener, app).await.unwrap();
}
