use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    response::Response,
    routing::get,
    Router,
};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tokio_tungstenite::{connect_async, tungstenite::Message as TungsteniteMessage};
use tower_http::services::ServeDir;
use chrono::Local;
use indicator_math::{Candle as IndicatorCandle, ema, sma, wma, hma, ehma};
use std::fs;


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Candle {
    pub symbol: String,
    pub time: u64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerTime {
    #[serde(rename = "type")]
    pub msg_type: String,
    pub server_time: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeOpened {
    #[serde(rename = "type")]
    pub msg_type: String,
    pub contract_id: String,
    pub asset: String,
    pub trade_type: String,
    pub stake: f64,
    pub time: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeResult {
    #[serde(rename = "type")]
    pub msg_type: String,
    pub status: String,
    pub balance: f64,
    pub stake: f64,
    pub profit: f64,
    pub contract_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeUpdate {
    #[serde(rename = "type")]
    pub msg_type: String,
    pub contract_id: String,
    pub asset: String,
    pub trade_type: String,
    pub current_spot: f64,
    pub entry_spot: f64,
    pub profit: f64,
    pub profit_percentage: f64,
    pub is_sold: bool,
    pub is_expired: bool,
    pub payout: f64,
    pub buy_price: f64,
    pub date_expiry: u64,
    pub date_start: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmaData {
    #[serde(rename = "type")]
    pub msg_type: String,
    pub short_ema: Vec<EmaPoint>,
    pub long_ema: Vec<EmaPoint>,
    pub short_period: usize,
    pub long_period: usize,
    pub short_type: String,
    pub long_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BalanceMessage {
    #[serde(rename = "type")]
    pub msg_type: String,
    pub balance: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmaPoint {
    pub time: u64,
    pub value: f64,
}

#[derive(Debug, Clone, Deserialize)]
struct IndicatorConfig {
    indicators: IndicatorsSection,
    #[serde(default)]
    chart: ChartSection,
}

#[derive(Debug, Clone, Deserialize)]
struct IndicatorsSection {
    short_ema_type: String,
    short_ema_period: usize,
    long_ema_type: String,
    long_ema_period: usize,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct ChartSection {
    #[serde(default = "default_short_color")]
    short_ema_color: String,
    #[serde(default = "default_long_color")]
    long_ema_color: String,
}

fn default_short_color() -> String { "#00BFFF".to_string() }
fn default_long_color() -> String { "#FF6347".to_string() }

fn load_indicator_config() -> IndicatorConfig {
    match fs::read_to_string("config.toml") {
        Ok(content) => {
            match toml::from_str::<IndicatorConfig>(&content) {
                Ok(config) => {
                    println!("📊 Loaded indicator config: short {} period {}, long {} period {}", 
                        config.indicators.short_ema_type, config.indicators.short_ema_period,
                        config.indicators.long_ema_type, config.indicators.long_ema_period);
                    config
                }
                Err(e) => {
                    println!("⚠️ Config parse error, using defaults: {}", e);
                    default_indicator_config()
                }
            }
        }
        Err(_) => {
            println!("⚠️ config.toml not found, using defaults");
            default_indicator_config()
        }
    }
}

fn default_indicator_config() -> IndicatorConfig {
    IndicatorConfig {
        indicators: IndicatorsSection {
            short_ema_type: "ema".to_string(),
            short_ema_period: 3,
            long_ema_type: "ema".to_string(),
            long_ema_period: 5,
        },
        chart: ChartSection {
            short_ema_color: "#00BFFF".to_string(),
            long_ema_color: "#FF6347".to_string(),
        },
    }
}

fn calculate_indicator(candles: &[IndicatorCandle], indicator_type: &str, period: usize) -> Vec<EmaPoint> {
    let result = match indicator_type.to_lowercase().as_str() {
        "sma" => sma(candles, period),
        "wma" => wma(candles, period),
        "hma" => hma(candles, period),
        "ehma" => ehma(candles, period),
        _ => ema(candles, period), // default to EMA
    };
    
    result.into_iter()
        .map(|v| EmaPoint { time: v.time, value: v.value })
        .collect()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum BroadcastMessage {
    Candle(Candle),
    ServerTime(ServerTime),
    TradeOpened(TradeOpened),
    TradeResult(TradeResult),
    TradeUpdate(TradeUpdate),
    EmaData(EmaData),
    Balance(BalanceMessage),
}

#[derive(Deserialize, Debug, Clone)]
struct ClientCommand {
    command: String,
    #[serde(default)]
    asset: String,
    #[serde(default)]
    trade_mode: String,
    #[serde(default)]
    money_mode: String,
    #[serde(default)]
    initial_stake: f64,
    #[serde(default)]
    app_id: String,
    #[serde(default)]
    api_token: String,
    #[serde(default)]
    duration: u64,
    #[serde(default)]
    duration_unit: String,
    #[serde(default)]
    contract_id: String,
}

struct AppState {
    tx: broadcast::Sender<BroadcastMessage>,
    current_conn: Arc<Mutex<Option<(JoinHandle<()>, tokio::sync::mpsc::Sender<String>)>>>,
}

#[tokio::main]
async fn main() {
    let (tx, _) = broadcast::channel::<BroadcastMessage>(1024);
    let state = Arc::new(AppState {
        tx,
        current_conn: Arc::new(Mutex::new(None)),
    });

    let app = Router::new()
        .route("/ws", get(websocket_handler))
        .fallback_service(ServeDir::new("public"))
        .with_state(state);

    println!("-----------------------------------------");
    println!("🚀 RELAY SERVER STARTING AT http://localhost:8080");
    println!("📂 Make sure 'public/index.html' exists!");
    println!("-----------------------------------------");

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn websocket_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>
) -> Response {
    ws.on_upgrade(|socket| handle_socket(socket, state))
}

async fn handle_socket(socket: WebSocket, state: Arc<AppState>) {
    println!("🔌 New Browser connected");
    let (mut sender, mut receiver) = socket.split();
    let mut rx = state.tx.subscribe();

    let mut send_task = tokio::spawn(async move {
        while let Ok(msg) = rx.recv().await {
            if let Ok(json) = serde_json::to_string(&msg) {
                if sender.send(Message::Text(json)).await.is_err() { 
                    break; 
                }
            }
        }
    });

    let state_clone = state.clone();
    let mut recv_task = tokio::spawn(async move {
        while let Some(Ok(msg)) = receiver.next().await {
            if let Message::Text(text) = msg {
                println!("📥 Browser sent: {}", text);
                
                match serde_json::from_str::<ClientCommand>(&text) {
                    Ok(req) => {
                        if req.command == "START_DERIV" {
                            println!("🎯 Valid Command! Requesting Asset: {}", req.asset);
                            
                            let old_conn = {
                                let mut conn_guard = state_clone.current_conn.lock().unwrap();
                                conn_guard.take()
                            };
                            
                            if let Some((old_handle, old_tx)) = old_conn {
                                println!("🛑 Stopping old connection...");
                                let _ = old_tx.send("FORGET".to_string()).await;
                                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                                old_handle.abort();
                            }

                            let tx = state_clone.tx.clone();
                            let (cmd_tx, cmd_rx) = tokio::sync::mpsc::channel::<String>(10);
                            
                            let handle = tokio::spawn(async move {
                                connect_to_deriv(tx, req, cmd_rx).await;
                            });
                            
                            {
                                let mut conn_guard = state_clone.current_conn.lock().unwrap();
                                *conn_guard = Some((handle, cmd_tx));
                            }
                        } else if req.command == "UPDATE_MODE" {
                            println!("🔄 Request to Update Trade Mode: {}", req.trade_mode);
                            let cmd_tx = {
                                state_clone.current_conn.lock().unwrap().as_ref().map(|(_, tx)| tx.clone())
                            };

                            if let Some(tx) = cmd_tx {
                                let _ = tx.send(format!("MODE:{}", req.trade_mode)).await;
                            } else {
                                println!("⚠️ No active connection to update.");
                            }
                        } else if req.command == "UPDATE_PARAMS" {
                            println!("🔄 Request to Update Params: MoneyMode={}, Duration={} {}", req.money_mode, req.duration, req.duration_unit);
                            let cmd_tx = {
                                state_clone.current_conn.lock().unwrap().as_ref().map(|(_, tx)| tx.clone())
                            };

                            if let Some(tx) = cmd_tx {
                                let _ = tx.send(format!("PARAMS:{}:{}:{}", req.money_mode, req.duration, req.duration_unit)).await;
                            } else {
                                println!("⚠️ No active connection to update.");
                            }
                        } else if req.command == "SELL" {
                            println!("🔻 Request to Sell Contract: {}", req.contract_id);
                            let cmd_tx = {
                                state_clone.current_conn.lock().unwrap().as_ref().map(|(_, tx)| tx.clone())
                            };
                            if let Some(tx) = cmd_tx {
                                let _ = tx.send(format!("SELL:{}", req.contract_id)).await;
                            } else {
                                println!("⚠️ No active connection to update.");
                            }
                        }
                    },
                    Err(e) => println!("⚠️ JSON Parse Error: {}", e),
                }
            }
        }
    });

    tokio::select! {
        _ = (&mut send_task) => println!("📤 Send task ended"),
        _ = (&mut recv_task) => println!("📥 Receive task ended"),
    };
}

async fn connect_to_deriv(
    tx: broadcast::Sender<BroadcastMessage>,
    config: ClientCommand,
    mut cmd_rx: tokio::sync::mpsc::Receiver<String>
) {
    let app_id = if config.app_id.is_empty() { "66726".to_string() } else { config.app_id };
    let url = format!("wss://ws.derivws.com/websockets/v3?app_id={}", app_id);
    println!("🌐 Connecting to Deriv API for asset: {}...", config.asset);

    match connect_async(&url).await {
        Ok((ws_stream, _)) => {
            println!("✅ Connected to Deriv: {}", config.asset);
            let (mut write, mut read) = ws_stream.split();

            let mut tick_sub_id: Option<String> = None;
            let mut candle_sub_id: Option<String> = None;
            
            // Trading state
            let mut balance = 1000.0;
            let martingale_stakes = vec![1.0, 2.0, 6.0, 18.0, 54.0, 162.0, 384.0, 800.0, 1600.0];
            let mut current_stake_index = 0;
            let mut last_trade_minute: Option<u64> = None;
            let mut _pending_contract_id: Option<String> = None;
            let mut pending_contract_type: Option<String> = None;
            let mut current_trade_mode = config.trade_mode.clone();
            let mut current_money_mode = config.money_mode.clone();
            
            // Set defaults if missing (though they have defaults in struct) or 0
            let mut current_duration = if config.duration == 0 { 55 } else { config.duration };
            let mut current_duration_unit = if config.duration_unit.is_empty() { "s".to_string() } else { config.duration_unit.clone() };

            // Load indicator config
            let indicator_config = load_indicator_config();
            let mut candles_for_ema: Vec<IndicatorCandle> = Vec::new();
            let mut last_ema_minute: Option<u64> = None;

            // Authorize if token provided
            if !config.api_token.is_empty() {
                let auth_msg = serde_json::json!({
                    "authorize": config.api_token
                });
                let _ = write.send(TungsteniteMessage::Text(auth_msg.to_string())).await;
                tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
            }

            // Subscribe tick
            let tick_msg = serde_json::json!({
                "ticks": "R_100",
                "subscribe": 1
            });
            let _ = write.send(TungsteniteMessage::Text(tick_msg.to_string())).await;
            tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;

            // Subscribe candles
            let sub_msg = serde_json::json!({
                "ticks_history": config.asset,
                "subscribe": 1,
                "style": "candles",
                "granularity": 60,
                "count": 50,
                "end": "latest",
                "adjust_start_time": 1
            });
            let _ = write.send(TungsteniteMessage::Text(sub_msg.to_string())).await;

            loop {
                tokio::select! {
                    cmd = cmd_rx.recv() => {
                        if let Some(cmd) = cmd {
                            if cmd == "FORGET" {
                                println!("📤 Sending forget for all subscriptions...");
                                if let Some(id) = tick_sub_id.take() {
                                    let forget_msg = serde_json::json!({"forget": id});
                                    let _ = write.send(TungsteniteMessage::Text(forget_msg.to_string())).await;
                                }
                                if let Some(id) = candle_sub_id.take() {
                                    let forget_msg = serde_json::json!({"forget": id});
                                    let _ = write.send(TungsteniteMessage::Text(forget_msg.to_string())).await;
                                }
                                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                                break;
                            } else if cmd.starts_with("MODE:") {
                                current_trade_mode = cmd.replace("MODE:", "");
                                println!("🔄 Trade Mode Updated to: {}", current_trade_mode);
                            } else if cmd.starts_with("PARAMS:") {
                                let parts: Vec<&str> = cmd.split(':').collect();
                                if parts.len() >= 4 {
                                    // format: PARAMS:money_mode:duration:duration_unit
                                    current_money_mode = parts[1].to_string();
                                    if let Ok(d) = parts[2].parse::<u64>() {
                                        current_duration = d;
                                    }
                                    current_duration_unit = parts[3].to_string();
                                    println!("✅ Params Updated: Money={}, Duration={} {}", current_money_mode, current_duration, current_duration_unit);
                                    
                                    // Reset martingale index if switching to fix
                                    if current_money_mode == "fix" {
                                        current_stake_index = 0;
                                    }
                                }
                            } else if cmd.starts_with("SELL:") {
                                let contract_id = cmd.replace("SELL:", "");
                                println!("🔻 Sending Sell Request for: {}", contract_id);
                                let sell_msg = serde_json::json!({
                                    "sell": contract_id,
                                    "price": 0
                                });
                                let _ = write.send(TungsteniteMessage::Text(sell_msg.to_string())).await;
                            }
                        }
                    }
                    
                    msg = read.next() => {
                        if let Some(Ok(TungsteniteMessage::Text(raw_text))) = msg {
                            if let Ok(json) = serde_json::from_str::<serde_json::Value>(&raw_text) {
                                
                                if let Some(error) = json.get("error") {
                                    println!("❌ API Error: {}", error.get("message").unwrap_or(&serde_json::json!("Unknown error")));
                                    break;
                                }

                                // Get balance from authorize response
                                if let Some(authorize) = json.get("authorize") {
                                    if let Some(bal) = authorize.get("balance").and_then(|b| b.as_f64()) {
                                        balance = bal;
                                        println!("💰 Current Balance: {}", balance);

                                        // Send balance to frontend
                                        let balance_msg = BalanceMessage {
                                            msg_type: "balance".to_string(),
                                            balance,
                                        };
                                        let _ = tx.send(BroadcastMessage::Balance(balance_msg));
                                    }
                                }

                                // Handle subscription IDs
                                if let Some(sub) = json.get("subscription") {
                                    if let Some(id) = sub.get("id").and_then(|i| i.as_str()) {
                                        if tick_sub_id.is_none() && json.get("tick").is_some() {
                                            tick_sub_id = Some(id.to_string());
                                        }
                                        if candle_sub_id.is_none() && (json.get("candles").is_some() || json.get("ohlc").is_some()) {
                                            candle_sub_id = Some(id.to_string());
                                        }
                                    }
                                }

                                // Server time
                                if let Some(tick) = json.get("tick") {
                                    if let Some(epoch) = tick.get("epoch").and_then(|e| e.as_u64()) {
                                        let time_msg = ServerTime {
                                            msg_type: "server_time".to_string(),
                                            server_time: epoch,
                                        };
                                        let _ = tx.send(BroadcastMessage::ServerTime(time_msg));
                                    }
                                }

                                // Historical candles
                                if let Some(candles) = json.get("candles").and_then(|c| c.as_array()) {
                                    println!("📊 Received {} historical candles", candles.len());
                                    
                                    // Clear and rebuild candles_for_ema with historical data
                                    candles_for_ema.clear();
                                    
                                    for candle_data in candles {
                                        if let Ok(mut candle) = parse_flexible(candle_data) {
                                            candle.symbol = config.asset.clone();
                                            let _ = tx.send(BroadcastMessage::Candle(candle.clone()));
                                            
                                            // Store for EMA calculation
                                            candles_for_ema.push(IndicatorCandle {
                                                time: candle.time,
                                                open: candle.open,
                                                high: candle.high,
                                                low: candle.low,
                                                close: candle.close,
                                            });
                                        }
                                    }
                                    
                                    // Calculate and send initial EMA data
                                    if candles_for_ema.len() >= indicator_config.indicators.long_ema_period {
                                        let short_ema = calculate_indicator(
                                            &candles_for_ema,
                                            &indicator_config.indicators.short_ema_type,
                                            indicator_config.indicators.short_ema_period
                                        );
                                        let long_ema = calculate_indicator(
                                            &candles_for_ema,
                                            &indicator_config.indicators.long_ema_type,
                                            indicator_config.indicators.long_ema_period
                                        );
                                        
                                        let ema_msg = EmaData {
                                            msg_type: "ema_data".to_string(),
                                            short_ema,
                                            long_ema,
                                            short_period: indicator_config.indicators.short_ema_period,
                                            long_period: indicator_config.indicators.long_ema_period,
                                            short_type: indicator_config.indicators.short_ema_type.clone(),
                                            long_type: indicator_config.indicators.long_ema_type.clone(),
                                        };
                                        
                                        println!("📈 Sending initial EMA data: short {} points, long {} points",
                                            ema_msg.short_ema.len(), ema_msg.long_ema.len());
                                        let _ = tx.send(BroadcastMessage::EmaData(ema_msg));
                                    }
                                } 
                                
                                // Real-time OHLC
                                else if let Some(ohlc) = json.get("ohlc") {
                                    if let Ok(mut candle) = parse_flexible(ohlc) {
                                        candle.symbol = config.asset.clone();
                                        let _ = tx.send(BroadcastMessage::Candle(candle.clone()));

                                        let current_minute = candle.time / 60;
                                        let seconds = candle.time % 60;

                                        // Update or add candle to the EMA calculation buffer
                                        let indicator_candle = IndicatorCandle {
                                            time: (current_minute * 60), // Normalize to minute boundary
                                            open: candle.open,
                                            high: candle.high,
                                            low: candle.low,
                                            close: candle.close,
                                        };

                                        // Find if this minute's candle exists
                                        if let Some(existing) = candles_for_ema.iter_mut().find(|c| c.time / 60 == current_minute) {
                                            // Update existing candle (use same open, update high/low/close)
                                            existing.high = existing.high.max(candle.high);
                                            existing.low = existing.low.min(candle.low);
                                            existing.close = candle.close;
                                        } else {
                                            // Add new candle
                                            candles_for_ema.push(indicator_candle);
                                            // Keep only last 200 candles
                                            if candles_for_ema.len() > 200 {
                                                candles_for_ema.remove(0);
                                            }
                                        }

                                        // Send EMA update when candle closes (new minute starts)
                                        // Check if we're in the first few seconds of a new minute
                                        if seconds <= 5 && Some(current_minute) != last_ema_minute {
                                            last_ema_minute = Some(current_minute);
                                            
                                            if candles_for_ema.len() >= indicator_config.indicators.long_ema_period {
                                                let short_ema = calculate_indicator(
                                                    &candles_for_ema,
                                                    &indicator_config.indicators.short_ema_type,
                                                    indicator_config.indicators.short_ema_period
                                                );
                                                let long_ema = calculate_indicator(
                                                    &candles_for_ema,
                                                    &indicator_config.indicators.long_ema_type,
                                                    indicator_config.indicators.long_ema_period
                                                );
                                                
                                                let ema_msg = EmaData {
                                                    msg_type: "ema_data".to_string(),
                                                    short_ema,
                                                    long_ema,
                                                    short_period: indicator_config.indicators.short_ema_period,
                                                    long_period: indicator_config.indicators.long_ema_period,
                                                    short_type: indicator_config.indicators.short_ema_type.clone(),
                                                    long_type: indicator_config.indicators.long_ema_type.clone(),
                                                };
                                                
                                                println!("📈 EMA updated at candle close: minute {}", current_minute);
                                                let _ = tx.send(BroadcastMessage::EmaData(ema_msg));
                                            }
                                        }

                                        // Debug log to see trading conditions
                                        if seconds <= 5 {
                                            println!("⏰ Time: {}:{:02} | Mode: {} | Token: {} | LastMin: {:?} | CurMin: {}",
                                                candle.time / 60 % 60, seconds,
                                                current_trade_mode,
                                                if config.api_token.is_empty() { "NO" } else { "YES" },
                                                last_trade_minute,
                                                current_minute
                                            );
                                        }

                                        // Trading logic - trade at second 0-2 of each minute (more flexible)
                                        if current_trade_mode != "idle" && !config.api_token.is_empty() {
                                            // Trade at second 0-2 of each minute, once per minute
                                            if seconds <= 2 && Some(current_minute) != last_trade_minute {
                                                last_trade_minute = Some(current_minute);

                                                let stake = if current_money_mode == "martingale" {
                                                    martingale_stakes[current_stake_index.min(martingale_stakes.len() - 1)]
                                                } else {
                                                    config.initial_stake
                                                };

                                                if balance >= stake {
                                                    let contract_type = if current_trade_mode == "call" { "CALL" } else { "PUT" };
                                                    
                                                    let buy_msg = serde_json::json!({
                                                        "buy": "1",
                                                        "price": stake,
                                                        "parameters": {
                                                            "contract_type": contract_type,
                                                            "symbol": config.asset,
                                                            "duration": current_duration,
                                                            "duration_unit": current_duration_unit,
                                                            "basis": "stake",
                                                            "amount": stake,
                                                            "currency": "USD"
                                                        }
                                                    });

                                                    println!("📈 Placing {} trade with stake: {} (balance: {})", contract_type, stake, balance);
                                                    pending_contract_type = Some(contract_type.to_string());
                                                    let _ = write.send(TungsteniteMessage::Text(buy_msg.to_string())).await;
                                                } else {
                                                    println!("⚠️ Insufficient balance: {} < stake: {}", balance, stake);
                                                }
                                            }
                                        }
                                    }
                                }

                                // Handle buy response - support both string and number contract_id
                                if let Some(buy) = json.get("buy") {
                                    // Try to get contract_id as string first, then as number
                                    let contract_id = buy.get("contract_id")
                                        .and_then(|c| c.as_str().map(|s| s.to_string()))
                                        .or_else(|| buy.get("contract_id").and_then(|c| c.as_u64().map(|n| n.to_string())));
                                    
                                    if let Some(contract_id) = contract_id {
                                        _pending_contract_id = Some(contract_id.clone());
                                        println!("✅ Contract opened: {}", contract_id);

                                        // ส่งข้อมูล trade ที่เปิดไปให้ frontend
                                        let now = Local::now();
                                        let stake = buy.get("buy_price").and_then(|p| p.as_f64()).unwrap_or(0.0);
                                        
                                        let trade_opened = TradeOpened {
                                            msg_type: "trade_opened".to_string(),
                                            contract_id: contract_id.clone(),
                                            asset: config.asset.clone(),
                                            trade_type: pending_contract_type.clone().unwrap_or_else(|| current_trade_mode.to_uppercase()),
                                            stake,
                                            time: now.format("%H:%M:%S").to_string(),
                                        };
                                        let _ = tx.send(BroadcastMessage::TradeOpened(trade_opened));

                                        // Subscribe to contract for result
                                        let proposal_msg = serde_json::json!({
                                            "proposal_open_contract": 1,
                                            "contract_id": contract_id,
                                            "subscribe": 1
                                        });
                                        let _ = write.send(TungsteniteMessage::Text(proposal_msg.to_string())).await;
                                    } else {
                                        println!("⚠️ Buy response received but no contract_id found: {}", serde_json::to_string_pretty(&buy).unwrap_or_default());
                                    }
                                } else if json.get("error").is_none() && json.get("msg_type").and_then(|m| m.as_str()) == Some("buy") {
                                    // Log if we got a buy response but couldn't parse it
                                    println!("⚠️ Unexpected buy response format: {}", raw_text);
                                }

                                // Handle contract updates and result
                                if let Some(proposal) = json.get("proposal_open_contract") {
                                    let contract_id = proposal.get("contract_id")
                                        .and_then(|c| c.as_str().map(|s| s.to_string()))
                                        .or_else(|| proposal.get("contract_id").and_then(|c| c.as_u64().map(|n| n.to_string())))
                                        .unwrap_or_default();
                                    
                                    let status = proposal.get("status").and_then(|s| s.as_str()).unwrap_or("open");
                                    let is_sold = proposal.get("is_sold").and_then(|v| v.as_u64()).unwrap_or(0) == 1;
                                    let is_expired = proposal.get("is_expired").and_then(|v| v.as_u64()).unwrap_or(0) == 1;
                                    
                                    // Send real-time updates while contract is open
                                    if status == "open" {
                                        let current_spot = proposal.get("current_spot").and_then(|v| v.as_f64())
                                            .or_else(|| proposal.get("current_spot").and_then(|v| v.as_str()?.parse().ok()))
                                            .unwrap_or(0.0);
                                        let entry_spot = proposal.get("entry_spot").and_then(|v| v.as_f64())
                                            .or_else(|| proposal.get("entry_spot").and_then(|v| v.as_str()?.parse().ok()))
                                            .unwrap_or(0.0);
                                        let profit = proposal.get("profit").and_then(|p| p.as_f64()).unwrap_or(0.0);
                                        let profit_percentage = proposal.get("profit_percentage").and_then(|p| p.as_f64()).unwrap_or(0.0);
                                        let payout = proposal.get("payout").and_then(|p| p.as_f64()).unwrap_or(0.0);
                                        let buy_price = proposal.get("buy_price").and_then(|p| p.as_f64()).unwrap_or(0.0);
                                        let date_expiry = proposal.get("date_expiry").and_then(|d| d.as_u64()).unwrap_or(0);
                                        let date_start = proposal.get("date_start").and_then(|d| d.as_u64()).unwrap_or(0);
                                        let asset = proposal.get("underlying").and_then(|s| s.as_str()).unwrap_or("").to_string();
                                        let trade_type = proposal.get("contract_type").and_then(|s| s.as_str()).unwrap_or("").to_string();
                                        
                                        let trade_update = TradeUpdate {
                                            msg_type: "trade_update".to_string(),
                                            contract_id: contract_id.clone(),
                                            asset,
                                            trade_type,
                                            current_spot,
                                            entry_spot,
                                            profit,
                                            profit_percentage,
                                            is_sold,
                                            is_expired,
                                            payout,
                                            buy_price,
                                            date_expiry,
                                            date_start,
                                        };
                                        let _ = tx.send(BroadcastMessage::TradeUpdate(trade_update));
                                    }
                                    
                                    // Handle final result
                                    if status == "sold" || status == "won" || status == "lost" {
                                        let profit = proposal.get("profit").and_then(|p| p.as_f64()).unwrap_or(0.0);
                                        let stake = proposal.get("buy_price").and_then(|p| p.as_f64()).unwrap_or(0.0);
                                        
                                        balance += profit;

                                        let is_win = profit > 0.0;
                                        
                                        if is_win {
                                            current_stake_index = 0;
                                            println!("🎉 WIN! Profit: {}, Balance: {}", profit, balance);
                                        } else {
                                            if current_money_mode == "martingale" {
                                                current_stake_index = (current_stake_index + 1).min(martingale_stakes.len() - 1);
                                            }
                                            println!("❌ LOSS! Loss: {}, Balance: {}", profit, balance);
                                        }

                                        let result = TradeResult {
                                            msg_type: "trade_result".to_string(),
                                            status: if is_win { "win".to_string() } else { "loss".to_string() },
                                            balance,
                                            stake,
                                            profit,
                                            contract_id: Some(contract_id),
                                        };

                                        let _ = tx.send(BroadcastMessage::TradeResult(result));
                                    }
                                }
                            }
                        } else {
                            break;
                        }
                    }
                }
            }
            
            println!("🔌 Deriv connection closed for: {}", config.asset);
        },
        Err(e) => println!("❌ Deriv Connection Failed: {}", e),
    }
}

fn parse_flexible(data: &serde_json::Value) -> Result<Candle, ()> {
    let to_f64 = |v: Option<&serde_json::Value>| -> Option<f64> {
        let val = v?;
        val.as_f64().or_else(|| val.as_str()?.parse().ok())
    };

    Ok(Candle {
        symbol: String::new(),
        time: data.get("epoch").and_then(|v| v.as_u64()).ok_or(())?,
        open:  to_f64(data.get("open")).ok_or(())?,
        high:  to_f64(data.get("high")).ok_or(())?,
        low:   to_f64(data.get("low")).ok_or(())?,
        close: to_f64(data.get("close")).ok_or(())?,
    })
}