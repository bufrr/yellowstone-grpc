use {
    anyhow::{Context, Result},
    backoff::{future::retry, ExponentialBackoff},
    clap::Parser,
    futures::{sink::SinkExt, stream::StreamExt},
    log::{error, info, trace},
    solana_pubkey::Pubkey,
    solana_signature::Signature,
    std::{
        collections::HashMap,
        env,
        fs::OpenOptions,
        io::Write,
        str::FromStr,
        sync::{Arc, Mutex},
        time::{SystemTime, UNIX_EPOCH},
    },
    tonic::transport::channel::ClientTlsConfig,
    yellowstone_grpc_client::{GeyserGrpcClient, Interceptor},
    yellowstone_grpc_proto::prelude::{
        subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequest,
        SubscribeRequestFilterTransactions, SubscribeRequestPing,
    },
};

#[derive(Parser, Debug)]
#[clap(
    author,
    version,
    about = "Monitor DEX Router transactions - simple mode"
)]
struct Args {
    /// gRPC endpoint URL
    #[clap(short, long, default_value = "http://127.0.0.1:10000")]
    endpoint: String,

    /// DEX Router program address to monitor
    #[clap(short, long)]
    router_address: String,

    /// X-Token for authentication (optional)
    #[clap(long)]
    x_token: Option<String>,

    /// Log file path for structured logging (optional)
    #[clap(long)]
    log_file: Option<String>,
}

impl Args {
    async fn connect(&self) -> Result<GeyserGrpcClient<impl Interceptor>> {
        // Normalize endpoint: if scheme is missing, default to http
        let endpoint = match self.endpoint.parse::<http::Uri>() {
            Ok(uri) if uri.scheme_str().is_some() => uri,
            _ => format!("http://{}", self.endpoint)
                .parse::<http::Uri>()
                .context("Invalid endpoint URI")?,
        };

        let builder = GeyserGrpcClient::build_from_shared(endpoint.to_string())?
            .x_token(self.x_token.clone())?;

        // Apply TLS only for HTTPS endpoints
        let builder = if endpoint.scheme_str() == Some("https") {
            builder.tls_config(ClientTlsConfig::new().with_native_roots())?
        } else {
            builder
        };

        builder.connect().await.map_err(Into::into)
    }
}

async fn monitor_dex_router(
    mut client: GeyserGrpcClient<impl Interceptor>,
    router_address: String,
    log_file: Option<Arc<Mutex<std::fs::File>>>,
) -> Result<()> {
    // Validate address
    let _router_pubkey = Pubkey::from_str(&router_address).context("Invalid router address")?;

    // Create subscription for processed transactions
    let mut transactions_filter = HashMap::new();
    transactions_filter.insert(
        "dex_monitor".to_string(),
        SubscribeRequestFilterTransactions {
            vote: Some(false),   // Filter out vote transactions
            failed: Some(false), // Filter out failed transactions
            signature: None,
            account_include: vec![router_address.clone()],
            account_exclude: vec![],
            account_required: vec![],
        },
    );

    let request = SubscribeRequest {
        transactions: transactions_filter,
        commitment: Some(CommitmentLevel::Processed as i32),
        ..Default::default()
    };

    // Subscribe
    let (mut subscribe_tx, mut stream) = client
        .subscribe_with_request(Some(request))
        .await
        .context("Failed to subscribe")?;

    info!("Monitoring DEX router: {}", router_address);
    println!("Timestamp (ms) | Transaction Signature");
    println!("{}", "-".repeat(80));

    // Process stream
    let mut ping_id: i32 = 0;
    while let Some(message) = stream.next().await {
        match message {
            Ok(msg) => {
                match msg.update_oneof {
                    Some(UpdateOneof::Transaction(tx)) => {
                        // Get local current timestamp in milliseconds
                        let local_timestamp_ms = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_millis();

                        // Get signature and log
                        if let Some(tx_info) = tx.transaction {
                            if let Ok(signature) = Signature::try_from(tx_info.signature.as_slice())
                            {
                                println!("{:<14} | {}", local_timestamp_ms, signature);

                                // Write structured log to file if provided
                                if let Some(ref log_file) = log_file {
                                    let log_entry = format!(
                                        "SOL,{},,,,,,,,,,, {},,,,,,,,,,\n",
                                        signature, local_timestamp_ms
                                    );

                                    if let Ok(mut file) = log_file.lock() {
                                        if let Err(e) = file.write_all(log_entry.as_bytes()) {
                                            error!("Failed to write to log file: {}", e);
                                        } else if let Err(e) = file.flush() {
                                            error!("Failed to flush log file: {}", e);
                                        }
                                    }
                                }

                                // Also use trace logging for the structured format
                                trace!(
                                    "SOL,{},,,,,,,,,,, {},,,,,,,,,,",
                                    signature,
                                    local_timestamp_ms
                                );
                            }
                        }
                    }
                    Some(UpdateOneof::Ping(_)) => {
                        // Keep connection alive by replying with a ping (incrementing id)
                        ping_id = ping_id.wrapping_add(1);
                        subscribe_tx
                            .send(SubscribeRequest {
                                ping: Some(SubscribeRequestPing { id: ping_id }),
                                ..Default::default()
                            })
                            .await?;
                    }
                    _ => {} // Ignore other updates
                }
            }
            Err(e) => {
                error!("Stream error: {}", e);
                return Err(e.into());
            }
        }
    }
    // Treat closed stream as transient error to trigger reconnect
    Err(anyhow::anyhow!("stream closed"))
}

#[tokio::main]
async fn main() -> Result<()> {
    // Setup logger
    env::set_var(
        env_logger::DEFAULT_FILTER_ENV,
        env::var_os(env_logger::DEFAULT_FILTER_ENV).unwrap_or_else(|| "info".into()),
    );
    env_logger::init();

    let args = Args::parse();

    // Setup log file if provided
    let log_file = if let Some(ref path) = args.log_file {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .context("Failed to open log file")?;
        Some(Arc::new(Mutex::new(file)))
    } else {
        None
    };

    // Retry with exponential backoff
    retry(ExponentialBackoff::default(), || async {
        info!("Connecting to: {}", args.endpoint);

        let client = args.connect().await.map_err(|e| {
            error!("Connection failed: {}", e);
            backoff::Error::transient(e)
        })?;

        match monitor_dex_router(client, args.router_address.clone(), log_file.clone()).await {
            Ok(()) => Ok(()),
            Err(e) => {
                error!("Monitor error: {}", e);
                Err(backoff::Error::transient(e))
            }
        }
    })
    .await?;

    Ok(())
}
