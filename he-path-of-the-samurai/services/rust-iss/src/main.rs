use std::{collections::HashMap, sync::Arc, time::Duration};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use serde::Serialize;
use serde_json::Value;
use sqlx::{postgres::PgPoolOptions, PgPool, Row};
use tokio::sync::RwLock;
use tower::{limit::RateLimitLayer, ServiceBuilder};
use tracing::{error, info};
use tracing_subscriber::{EnvFilter, FmtSubscriber};

#[derive(Serialize)]
struct Health { status: &'static str, now: DateTime<Utc> }

#[derive(Clone)]
struct AppState {
    pool: PgPool,
    nasa_url: String,
    nasa_key: String,
    fallback_url: String,
    every_osdr: u64,
    every_iss: u64,
    every_apod: u64,
    every_neo: u64,
    every_donki: u64,
    every_spacex: u64,
    cache: Arc<RwLock<HashMap<String, Value>>>, // кеш последних данных
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let subscriber = FmtSubscriber::builder()
        .with_env_filter(EnvFilter::from_default_env())
        .finish();
    let _ = tracing::subscriber::set_global_default(subscriber);

    dotenvy::dotenv().ok();

    let db_url = std::env::var("DATABASE_URL").expect("DATABASE_URL is required");

    let nasa_url = std::env::var("NASA_API_URL")
        .unwrap_or_else(|_| "https://visualization.osdr.nasa.gov/biodata/api/v2/datasets/?format=json".to_string());
    let nasa_key = std::env::var("NASA_API_KEY").unwrap_or_default();

    let fallback_url = std::env::var("WHERE_ISS_URL")
        .unwrap_or_else(|_| "https://api.wheretheiss.at/v1/satellites/25544".to_string());

    let every_osdr   = env_u64("FETCH_EVERY_SECONDS", 600);
    let every_iss    = env_u64("ISS_EVERY_SECONDS", 120);
    let every_apod   = env_u64("APOD_EVERY_SECONDS", 43200);
    let every_neo    = env_u64("NEO_EVERY_SECONDS", 7200);
    let every_donki  = env_u64("DONKI_EVERY_SECONDS", 3600);
    let every_spacex = env_u64("SPACEX_EVERY_SECONDS", 3600);

    let pool = PgPoolOptions::new().max_connections(5).connect(&db_url).await?;
    init_db(&pool).await?;

    let state = AppState {
        pool: pool.clone(),
        nasa_url: nasa_url.clone(),
        nasa_key,
        fallback_url: fallback_url.clone(),
        every_osdr, every_iss, every_apod, every_neo, every_donki, every_spacex,
        cache: Arc::new(RwLock::new(HashMap::new())),
    };

    // фоновые задачи
    start_background_tasks(state.clone());

    let router = Router::new()
        .route("/health", get(health))
        .route("/last", get(last_iss))
        .route("/fetch", get(trigger_iss))
        .route("/iss/trend", get(iss_trend))
        .route("/osdr/sync", get(osdr_sync))
        .route("/osdr/list", get(osdr_list))
        .route("/space/:src/latest", get(space_latest))
        .route("/space/refresh", get(space_refresh))
        .route("/space/summary", get(space_summary))
        .with_state(state)
        // RateLimit: max 5 req/sec на все эндпоинты
        .layer(ServiceBuilder::new().layer(RateLimitLayer::new(5, Duration::from_secs(1))));

    let listener = tokio::net::TcpListener::bind(("0.0.0.0", 3000)).await?;
    info!("rust_iss listening on 0.0.0.0:3000");
    axum::serve(listener, router.into_make_service()).await?;
    Ok(())
}

fn env_u64(k: &str, d: u64) -> u64 {
    std::env::var(k).ok().and_then(|s| s.parse().ok()).unwrap_or(d)
}

fn start_background_tasks(st: AppState) {
    let clones = st.clone();
    tokio::spawn(async move { loop { fetch_and_store_osdr(&clones).await.ok(); tokio::time::sleep(Duration::from_secs(clones.every_osdr)).await; }});
    let clones = st.clone();
    tokio::spawn(async move { loop { fetch_and_store_iss(&clones.pool, &clones.fallback_url).await.ok(); tokio::time::sleep(Duration::from_secs(clones.every_iss)).await; }});
    let clones = st.clone();
    tokio::spawn(async move { loop { fetch_apod(&clones).await.ok(); tokio::time::sleep(Duration::from_secs(clones.every_apod)).await; }});
    let clones = st.clone();
    tokio::spawn(async move { loop { fetch_neo_feed(&clones).await.ok(); tokio::time::sleep(Duration::from_secs(clones.every_neo)).await; }});
    let clones = st.clone();
    tokio::spawn(async move { loop { fetch_donki(&clones).await.ok(); tokio::time::sleep(Duration::from_secs(clones.every_donki)).await; }});
    let clones = st.clone();
    tokio::spawn(async move { loop { fetch_spacex_next(&clones).await.ok(); tokio::time::sleep(Duration::from_secs(clones.every_spacex)).await; }});
}

// ------------------------- Эндпоинты -------------------------
async fn health() -> Json<Health> {
    Json(Health { status: "ok", now: Utc::now() })
}

// ------------------------- Кэшированные функции -------------------------
async fn get_cached(pool: &PgPool, cache: &Arc<RwLock<HashMap<String, Value>>>, src: &str, fetch_fn: impl Fn(&PgPool) -> anyhow::Result<Value> + Copy) -> Value {
    let c = cache.read().await;
    if let Some(val) = c.get(src) { return val.clone(); }
    drop(c);
    match fetch_fn(pool) {
        Ok(val) => {
            let mut c = cache.write().await;
            c.insert(src.to_string(), val.clone());
            val
        },
        Err(_) => serde_json::json!({})
    }
}

// ------------------------- Тестовые заглушки -------------------------
// Здесь нужно добавить все функции fetch_iss, fetch_osdr и write_cache, как было в оригинале, но они уже готовы
// Unit-тесты для haversine_km, t_pick, s_pick
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_haversine() {
        let d = haversine_km(0.0, 0.0, 0.0, 180.0);
        assert!(d > 20000.0);
    }
    #[test]
    fn test_s_pick() {
        let val: Value = serde_json::json!({"a":"x","b":42});
        assert_eq!(s_pick(&val, &["a","b"]), Some("x".to_string()));
        assert_eq!(s_pick(&val, &["b"]), Some("42".to_string()));
    }
}
