use axum::{extract::Query, routing::get, Json, Router};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::{RowAccessor, ListAccessor};
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::{env, path::{Path, PathBuf}};
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
            .split(|c: char| !c.is_ascii_digit() && c != ',' && c != '.')
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
        .max_by_key(|path| path.metadata().ok().and_then(|m| m.modified().ok()))
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
        .filter(|c| c.is_ascii_digit() || *c == '.')
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
    debug!("Starting to parse Daft row");

    let listing = match row.get_group(0) {
        Ok(l) => {
            debug!("Successfully got listing group");
            l
        },
        Err(e) => {
            error!("Failed to get listing group: {}", e);
            return None;
        }
    };

    // First, let's debug the structure
    debug!("Listing field count: {}", listing.len());
    debug!("Listing fields: {:?}", listing);

    // Get abbreviatedPrice (index 0)
    let price_string = match listing.get_string(0) {
        Ok(price) => {
            debug!("Found price string: {}", price);
            price.to_string()
        },
        Err(e) => {
            error!("Failed to get price string: {}", e);
            return None;
        }
    };

    let price_amount = match parse_price_string(&price_string) {
        Some(amount) => amount,
        None => return None,
    };

    // Get PropertyId (index 3)
    let property_id = match listing.get_string(3) {
        Ok(id) => {
            debug!("Found property ID: {}", id);
            id.to_string()
        },
        Err(e) => {
            error!("Failed to get property ID: {}", e);
            return None;
        }
    };

    // Get DisplayAddress (using seoTitle or title, likely index 5)
    let display_address = match listing.get_string(5) {
        Ok(addr) => {
            debug!("Found display address: {}", addr);
            addr.to_string()
        },
        Err(e) => {
            debug!("Failed to get display address: {}", e);
            "Address not available".to_string()
        }
    };

    // Get PropertyType (likely index 2)
    let property_type = match listing.get_string(2) {
        Ok(pt) => {
            debug!("Found property type: {}", pt);
            pt.to_string()
        },
        Err(e) => {
            debug!("Failed to get property type: {}", e);
            "Not specified".to_string()
        }
    };

    // Get seoFriendlyPath (index 23 in the listing struct)
    let seo_url = match listing.get_string(23) {
        Ok(path) => {
            debug!("Found SEO path: {}", path);
            Some(path.to_string())
        },
        Err(e) => {
            debug!("Failed to get SEO friendly path: {}", e);
            // Fallback to brochure url from media if available
            match listing.get_group(8)  // media
                .and_then(|media| media.get_group(0))  // brochure
                .and_then(|brochure_list| brochure_list.get_group(0))  // first brochure
                .and_then(|brochure| brochure.get_string(0)) { // url
                Ok(url) => Some(url.to_string()),
                Err(_) => None,
            }
        }
    };

    // Get BER rating from the nested BER struct (index 1)
    let ber_rating = if let Ok(ber_group) = listing.get_group(1) {
        match ber_group.get_string(2) { // rating field
            Ok(rating) => Some(rating.to_string()),
            Err(_) => None,
        }
    } else {
        None
    };


    // For bedrooms and bathrooms, we'll rely on the property_type string parsing for now
    let bedrooms = None;  // We'll implement proper parsing later
    let bathrooms = None; // We'll implement proper parsing later

    // For now, we'll return a simplified property structure
    Some(StandardizedProperty {
        property_id: format!("daft_{}", property_id),
        source: "daft".to_string(),
        source_id: property_id,
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
        created_date: chrono::Utc::now().to_rfc3339(),
        updated_date: chrono::Utc::now().to_rfc3339(),
        listing_type: "rent".to_string(),
        status: "active".to_string(),
        photos: vec![], // We'll implement photo parsing later
        has_video: false,
        agent: None,    // We'll implement agent parsing later
        seo_url
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

        debug!("Processing source: {}", source);
        
        if let Some(latest_file) = find_latest_parquet(source, data_path) {
            debug!("Found latest file for {}: {:?}", source, latest_file);

            match File::open(&latest_file) {
                Ok(file) => {
                    match SerializedFileReader::new(file) {
                        Ok(reader) => {
                            match reader.get_row_iter(None) {
                                Ok(iter) => {
                                    for row_result in iter {
                                        match row_result {
                                            Ok(row) => {
                                                let property = match source {
                                                    "daft" => {
                                                        debug!("Parsing Daft row");
                                                        match parse_daft_row(&row) {
                                                            Some(p) => {
                                                                debug!("Successfully parsed Daft property: {} - {}", 
                                                                    p.property_id, p.price.amount);
                                                                p
                                                            },
                                                            None => {
                                                                debug!("Failed to parse Daft property");
                                                                continue;
                                                            }
                                                        }
                                                    },
                                                    "myhome" => {
                                                        match parse_myhome_row(&row) {
                                                            Some(p) => p,
                                                            None => continue,
                                                        }
                                                    },
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
                                                    _ => continue,
                                                };

                                                // Validate the price before including the property
                                                if !validate_price(property.price.amount) {
                                                    debug!("Invalid price {} for property {}", 
                                                        property.price.amount, property.property_id);
                                                    continue;
                                                }

                                                // Apply filters
                                                if should_include_property(&property, &params) {
                                                    debug!("Adding property {} with price {}", 
                                                        property.property_id, property.price.amount);
                                                    properties.push(property);
                                                } else {
                                                    debug!("Property {} filtered out by criteria", 
                                                        property.property_id);
                                                }
                                            }
                                            Err(e) => error!("Error reading row: {}", e),
                                        }
                                    }
                                }
                                Err(e) => error!("Error getting row iterator: {}", e),
                            }
                        }
                        Err(e) => error!("Error creating reader for {}: {}", source, e),
                    }
                }
                Err(e) => error!("Error opening file for {}: {}", source, e),
            }
        } else {
            warn!("No parquet file found for source: {}", source);
        }
    }

    debug!("Found {} total properties", properties.len());
    Json(properties)
}

fn should_include_property(property: &StandardizedProperty, params: &SearchParams) -> bool {
    debug!("Checking property {} against filters", property.property_id);
    
    // Price filters
    if let Some(min_price) = params.min_price {
        if property.price.amount < min_price {
            debug!("Property {} filtered out by min price: {} < {}", 
                property.property_id, property.price.amount, min_price);
            return false;
        }
    }
    if let Some(max_price) = params.max_price {
        if property.price.amount > max_price {
            debug!("Property {} filtered out by max price: {} > {}", 
                property.property_id, property.price.amount, max_price);
            return false;
        }
    }

    // Bedrooms filter
    if let Some(bedrooms) = params.bedrooms {
        if let Some(prop_beds) = property.bedrooms {
            if prop_beds != bedrooms {
                debug!("Property {} filtered out by bedrooms: {} != {}", 
                    property.property_id, prop_beds, bedrooms);
                return false;
            }
        } else {
            debug!("Property {} filtered out: no bedroom info", property.property_id);
            return false;
        }
    }

    // Property type filter
    if let Some(ref prop_type) = params.property_type {
        if !property.property_type
            .to_lowercase()
            .contains(&prop_type.to_lowercase()) {
            debug!("Property {} filtered out by type: {} doesn't contain {}", 
                property.property_id, property.property_type, prop_type);
            return false;
        }
    }

    // BER rating filter
    if let Some(ref ber) = params.ber_rating {
        if let Some(ref property_ber) = property.ber_rating {
            if !property_ber.to_lowercase().contains(&ber.to_lowercase()) {
                debug!("Property {} filtered out by BER: {} doesn't match {}", 
                    property.property_id, property_ber, ber);
                return false;
            }
        } else {
            debug!("Property {} filtered out: no BER info", property.property_id);
            return false;
        }
    }

    debug!("Property {} passed all filters", property.property_id);
    true
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


