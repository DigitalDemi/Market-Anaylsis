use axum::{extract::Query, routing::get, Json, Router};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::{RowAccessor, ListAccessor};
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::{env, path::{Path, PathBuf}};
use tracing_subscriber;
use log::{error, warn, debug};

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
    seo_url: Option<String>,
}

// Source-specific types
#[derive(Debug, Serialize, Deserialize)]
struct PropertyIEListing {
    address: String,
    price: String,
    id: String,
}

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
    fn from_property_ie(raw: PropertyIEListing) -> Self {
        let price_amount = raw
            .price
            .trim_start_matches('€')
            .trim_end_matches(" monthly")
            .split(|c: char| !c.is_digit(10) && c != ',' && c != '.')
            .next()
            .and_then(|s| s.replace(",", "").trim().parse::<f64>().ok())
            .unwrap_or(0.0);

        StandardizedProperty {
            property_id: format!("property_{}", raw.id),
            source: "property".to_string(),
            source_id: raw.id.clone(),
            address: Address {
                display_address: raw.address.trim().to_string(),
            },
            property_type: String::new(),
            bedrooms: None,
            bathrooms: None,
            size: None,
            ber_rating: None,
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
            has_video: false,
            agent: None,
            seo_url: None,
        }
    }

    fn from_myhome(raw: MyHomeProperty) -> Self {
        let price_amount = raw
            .price_as_string
            .trim_start_matches('€')
            .trim()
            .split(|c: char| !c.is_digit(10) && c != ',' && c != '.')
            .next()
            .map(|s| s.replace(",", ""))
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0);

        StandardizedProperty {
            property_id: format!("myhome_{}", raw.property_id),
            source: "myhome".to_string(),
            source_id: raw.property_id.to_string(),
            address: Address {
                display_address: raw.display_address.trim().to_string(),
            },
            property_type: raw.property_type,
            bedrooms: raw.number_of_beds,
            bathrooms: Some(raw.number_of_bathrooms),
            size: raw.size_string_meters.map(|v| Size {
                value: v,
                unit: "square_meters".to_string(),
            }),
            ber_rating: raw.ber_rating.filter(|s| !s.is_empty()),
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
            seo_url: None
        }
    }
}

fn find_latest_parquet(source: &str, base_path: &str) -> Option<PathBuf> {
    let source_path = Path::new(base_path).join("processed").join(source);

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

    fs::read_dir(&latest_day.1)
        .ok()?
        .filter_map(|entry| entry.ok().map(|e| e.path()))
        .filter(|path| path.extension().map_or(false, |ext| ext == "parquet"))
        .max_by_key(|path| path.metadata().ok().map(|m| m.modified().ok()).flatten())
}

fn parse_price_string(price_str: &str) -> Option<f64> {
    debug!("Parsing price string: {}", price_str);
    
    // Handle empty strings
    if price_str.trim().is_empty() {
        debug!("Empty price string found");
        return None;
    }

    // Handle "POA" case
    if price_str.trim().to_uppercase() == "POA" {
        debug!("Price on Application (POA) found");
        return None;
    }

    // First split by "/" and take the first part
    let price_part = price_str
        .split('/')
        .next()
        .unwrap_or("")
        .trim();

    // Extract only numeric characters and decimal points
    let numeric_str: String = price_part
        .chars()
        .filter(|c| c.is_digit(10) || *c == '.')
        .collect();

    if numeric_str.is_empty() {
        debug!("No numeric value found after cleaning: {}", price_part);
        return None;
    }

    debug!("Cleaned price string for parsing: {}", numeric_str);

    // Parse to float
    match numeric_str.parse::<f64>() {
        Ok(amount) if amount > 0.0 => {
            debug!("Successfully parsed price: {}", amount);
            Some(amount)
        },
        Ok(_) => {
            debug!("Found zero or negative price");
            None
        },
        Err(e) => {
            warn!("Failed to parse price '{}' from original '{}': {}", numeric_str, price_str, e);
            None
        }
    }
}

fn parse_myhome_row(row: &parquet::record::Row) -> Option<StandardizedProperty> {
    debug!("Parsing MyHome row");

    // Basic property details
    let property_id = row.get_long(0).unwrap_or_default();  // PropertyId
    
    let price_string = row.get_string(37)  // PriceAsString
        .map(|s| s.to_string())
        .unwrap_or_default();
    
    debug!("Raw price string: {}", price_string);
    let price_amount = parse_price_string(&price_string)?;  // Early return if price is invalid

    // Display address is at index 42 (DisplayAddress)
    let display_address = row.get_string(42)
        .map(|s| s.to_string())
        .unwrap_or_default();
    
    let bedrooms = row.get_long(36)  // NumberOfBeds
        .ok()
        .map(|b| b as i32);
    
    let bathrooms = row.get_long(48)  // NumberOfBathrooms
        .ok()
        .map(|b| b as i32);
    
    let property_type = row.get_string(46)  // PropertyType
        .map(|s| s.to_string())
        .unwrap_or_default();
    
    let ber_rating = row.get_string(49)  // BerRating
        .map(|s| s.to_string())
        .ok()
        .filter(|s| !s.is_empty());

    let size_meters = row.get_double(40)  // SizeStringMeters
        .ok();

    let is_active = row.get_bool(28)  // IsActive
        .unwrap_or(false);

    // Dates
    let created_date = row.get_string(11)  // CreatedOnDate
        .map(|s| s.to_string())
        .unwrap_or_default();
    
    let updated_date = row.get_string(3)  // RefreshedOn
        .map(|s| s.to_string())
        .unwrap_or_default();

     let seo_url = row.get_string(55)
        .map(|s| s.to_string())
        .ok();

    // Photos - MainPhoto at index 61, Photos list at index 63
    let mut photos = Vec::new();
    
    if let Ok(main_photo) = row.get_string(61) {
        photos.push(Photo {
            url: main_photo.to_string(),
            is_main: true,
        });
    }

    if let Ok(photo_list) = row.get_list(63) {
        for i in 0..photo_list.len() {
            if let Ok(url) = photo_list.get_string(i) {
                let url_string = url.to_string();
                if !photos.iter().any(|p| p.url == url_string) {
                    photos.push(Photo {
                        url: url_string,
                        is_main: false,
                    });
                }
            }
        }
    }

    // Agent information
    let agent = Some(Agent {
        name: row.get_string(8)  // GroupName
            .map(|s| s.to_string())
            .unwrap_or_default(),
        phone: row.get_string(6)  // GroupPhoneNumber
            .map(|s| s.to_string())
            .unwrap_or_default(),
        email: row.get_string(7)  // GroupEmail
            .map(|s| s.to_string())
            .unwrap_or_default(),
        address: row.get_string(9)  // GroupAddress
            .map(|s| s.to_string())
            .unwrap_or_default(),
    });

    let size = size_meters.map(|value| Size {
        value,
        unit: "square_meters".to_string(),
    });

    Some(StandardizedProperty {
        property_id: format!("myhome_{}", property_id),
        source: "myhome".to_string(),
        source_id: property_id.to_string(),
        address: Address {
            display_address,
        },
        property_type,
        bedrooms,
        bathrooms,
        size,
        ber_rating,
        price: Price {
            amount: price_amount,
            currency: "EUR".to_string(),
            frequency: Some("month".to_string()),
            price_changes: vec![],
        },
        created_date,
        updated_date,
        listing_type: "rent".to_string(),
        status: if is_active { "active" } else { "inactive" }.to_string(),
        photos,
        has_video: row.get_bool(31).unwrap_or(false),  // HasVideos
        agent,
        seo_url,
    })
}

fn parse_daft_row(row: &parquet::record::Row) -> Option<StandardizedProperty> {
    let listing = match row.get_group(0) {
        Ok(l) => l,
        Err(e) => {
            error!("Error getting listing group: {}", e);
            return None;
        }
    };

    const PRICE_IDX: usize = 0;        // abbreviatedPrice
    const BER_GROUP_IDX: usize = 1;    // ber group
    const CATEGORY_IDX: usize = 2;     // category
    const ID_IDX: usize = 7;           // id
    const MEDIA_GROUP_IDX: usize = 8;  // media group
    const BATHROOMS_IDX: usize = 9;    // numBathrooms
    const BEDROOMS_IDX: usize = 10;    // numBedrooms
    const ADDRESS_IDX: usize = 11;     // address

    let id = match listing.get_long(ID_IDX) {
        Ok(id) => id.to_string(),
        Err(e) => {
            error!("Error getting ID: {}", e);
            return None;
        }
    };

    let price_str = listing.get_string(PRICE_IDX).map_or("€0".to_string(), |s| s.to_string());
    let price_amount = price_str
        .trim_start_matches('€')
        .trim_end_matches(" per month")
        .split(|c: char| !c.is_digit(10) && c != ',' && c != '.')
        .next()
        .map(|s| s.replace(",", ""))
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or(0.0);

    let ber_rating = listing
        .get_group(BER_GROUP_IDX)
        .ok()
        .and_then(|ber| ber.get_string(2).ok())
        .map(String::from);

    let bedrooms = listing
        .get_string(BEDROOMS_IDX)
        .ok()
        .and_then(|beds| {
            beds.chars()
                .filter(|c| c.is_digit(10))
                .collect::<String>()
                .parse::<i32>()
                .ok()
        });

    let bathrooms = listing
        .get_string(BATHROOMS_IDX)
        .ok()
        .and_then(|baths| {
            baths.chars()
                .filter(|c| c.is_digit(10))
                .collect::<String>()
                .parse::<i32>()
                .ok()
        });

    let property_type = listing
        .get_string(CATEGORY_IDX)
        .map_or("Not specified".to_string(), |s| s.to_string());

    let has_video = listing
        .get_group(MEDIA_GROUP_IDX)
        .ok()
        .and_then(|media| media.get_bool(2).ok())
        .unwrap_or(false);

    let display_address = listing
        .get_string(ADDRESS_IDX)
        .map_or("Address not specified".to_string(), |s| s.to_string());

    let photos = match listing.get_group(MEDIA_GROUP_IDX) {
        Ok(media) => {
            let mut photos_vec = Vec::new();
            if let Ok(photo_urls) = media.get_list(0) {
                for i in 0..photo_urls.len() {
                    if let Ok(url) = photo_urls.get_string(i) {
                        photos_vec.push(Photo {
                            url: url.to_string(),
                            is_main: i == 0,
                        });
                    }
                }
            }
            photos_vec
        }
        Err(_) => Vec::new(),
    };

    Some(StandardizedProperty {
        property_id: format!("daft_{}", id),
        source: "daft".to_string(),
        source_id: id,
        address: Address {
            display_address,
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
        photos,
        has_video,
        agent: None,
        seo_url: None,
    })
}

fn validate_price(amount: f64) -> bool {
    amount > 0.0 && amount < 100000.0 // Reasonable range for monthly rent
}

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

async fn search_rentals(Query(params): Query<SearchParams>) -> Json<Vec<StandardizedProperty>> {
    let mut properties = Vec::new();
    let sources = match &params.source {
        Some(source) => vec![source.as_str()],
        None => vec!["daft", "myhome", "property"]
    };

    debug!("Starting search with params: {:?}", params);
    debug!("Searching in sources: {:?}", sources);

    let data_path = "housing_data";

    for source in sources {
        // Skip if source doesn't match requested source
        if let Some(ref requested_source) = params.source {
            if requested_source.to_lowercase() != source.to_lowercase() {
                continue;
            }
        }

        if let Some(latest_file) = find_latest_parquet(source, data_path) {
            debug!("Processing file for source {}: {:?}", source, latest_file);

            if let Ok(file) = File::open(&latest_file) {
                if let Ok(reader) = SerializedFileReader::new(file) {
                    if let Ok(iter) = reader.get_row_iter(None) {
                        for row_result in iter {
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
                                            let id = row
                                                .get_string(2)
                                                .map(|s| s.to_string())
                                                .unwrap_or_default();

                                            StandardizedProperty::from_property_ie(PropertyIEListing {
                                                address,
                                                price: price_string,
                                                id,
                                            })
                                        },
                                        "myhome" => match parse_myhome_row(&row) {
                                            Some(property) => property,
                                            None => continue,
                                        },
                                        "daft" => match parse_daft_row(&row) {
                                            Some(property) => property,
                                            None => continue,
                                        },
                                        _ => continue,
                                    };

                                    // Validate the price before including the property
                                    if !validate_price(property.price.amount) {
                                        warn!("Invalid price {} for property {}", property.price.amount, property.property_id);
                                        continue;
                                    }

                                    // Apply filters using a separate function for clarity
                                    if should_include_property(&property, &params) {
                                        debug!(
                                            "Adding property: {} - {} from source {}",
                                            property.property_id, property.address.display_address, source
                                        );
                                        properties.push(property);
                                    }
                                }
                                Err(e) => error!("Error reading row: {}", e),
                            }
                        }
                    }
                }
            }
        } else {
            warn!("No parquet file found for source: {}", source);
        }
    }

    debug!("Found {} total properties", properties.len());
    Json(properties)
}

// Helper function to determine if a property matches all filter criteria
fn should_include_property(property: &StandardizedProperty, params: &SearchParams) -> bool {
    // Price filters
    if let Some(min_price) = params.min_price {
        if property.price.amount < min_price {
            return false;
        }
    }
    if let Some(max_price) = params.max_price {
        if property.price.amount > max_price {
            return false;
        }
    }

    // Bedrooms filter
    if let Some(bedrooms) = params.bedrooms {
        if let Some(prop_beds) = property.bedrooms {
            if prop_beds != bedrooms {
                return false;
            }
        } else {
            return false;
        }
    }

    // Property type filter
    if let Some(ref prop_type) = params.property_type {
        if !property.property_type
            .to_lowercase()
            .contains(&prop_type.to_lowercase()) {
            return false;
        }
    }

    // BER rating filter
    if let Some(ref ber) = params.ber_rating {
        if let Some(ref property_ber) = property.ber_rating {
            if !property_ber.to_lowercase().contains(&ber.to_lowercase()) {
                return false;
            }
        } else {
            return false;
        }
    }

    true
}


#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::{Client, Url};

    const BASE_URL: &str = "http://localhost:3000";
    const BASE_PATH: &str = "housing_data";

    #[tokio::test]
    async fn test_health_check() -> Result<(), Box<dyn std::error::Error>> {
        let client = Client::new();
        let url = Url::parse(BASE_URL)?.join("/health")?;

        let response = client.get(url).send().await?;
        assert_eq!(response.status(), 200);
        
        let body = response.text().await?;
        assert_eq!(body, "OK");
        Ok(())
    }

    #[test]
    fn test_directory_exists() {
        let path = Path::new(BASE_PATH);
        assert!(path.exists());
    }

    #[test]
    fn test_property_ie_parsing() {
        let listing = PropertyIEListing {
            address: "Test Address".to_string(),
            price: "€1,500 monthly".to_string(),
            id: "12345".to_string(),
        };
        let property = StandardizedProperty::from_property_ie(listing);
        assert_eq!(property.price.amount, 1500.0);
        assert_eq!(property.source, "property");
    }
}


#[tokio::main]
async fn main() {
    // Initialize logging
    tracing_subscriber::fmt::init();

    // Setup router with all our endpoints
    let app = Router::new()
        .route("/health", get(health_check))
        .route("/api/rentals/search", get(search_rentals))
        .route("/debug/paths", get(debug_paths));

    // Start the server
    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000")
        .await
        .unwrap();
    println!("Server listening on {}", listener.local_addr().unwrap());
    
    // Start serving
    axum::serve(listener, app)
        .await
        .unwrap();
}
