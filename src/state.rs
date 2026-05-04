use regex::Regex;
use rhai::{AST, Engine, Scope};
use std::{
    collections::HashMap,
    error::Error,
    sync::{Arc, RwLock},
};
use object_store::{aws::AmazonS3Builder, ObjectStore};
use tonic::transport::Channel;

pub(crate) const MAX_CONCURRENT_REQUESTS: usize = 32;

#[derive(Clone)]
pub(crate) struct ExtensionState {
    pub clients: Arc<RwLock<HashMap<String, Arc<dyn ObjectStore>>>>,
    pub rhai_engine: Arc<Engine>,
    pub script_cache: Arc<RwLock<HashMap<String, AST>>>,
    pub grpc_channels: Arc<RwLock<HashMap<String, Channel>>>,
    pub http_client: Arc<reqwest::Client>,
    pub runtime: Arc<tokio::runtime::Runtime>,
}

impl ExtensionState {
    pub fn new() -> Self {
        let mut engine = Engine::new();

        engine.set_max_expr_depths(64, 64);
        engine.set_max_string_size(10 * 1024 * 1024);

        engine.register_fn("regex_replace", |text: &str, pattern: &str, replacement: &str| -> Result<String, Box<rhai::EvalAltResult>> {
            let re = Regex::new(pattern)
                .map_err(|e| format!("Invalid regex '{}': {}", pattern, e))?;
            Ok(re.replace_all(text, replacement).into_owned())
        });

        engine.register_fn("regex_match", |text: &str, pattern: &str| -> Result<bool, Box<rhai::EvalAltResult>> {
            let re = Regex::new(pattern)
                .map_err(|e| format!("Invalid regex '{}': {}", pattern, e))?;
            Ok(re.is_match(text))
        });

        engine.register_fn("lines", |text: &str| -> rhai::Array {
            text.lines().map(|s| rhai::Dynamic::from(s.to_string())).collect()
        });

        engine.register_fn("truncate", |text: &str, max_len: i64| -> String {
            if max_len < 0 {
                return text.to_string();
            }
            let max = max_len as usize;
            if text.len() <= max {
                text.to_string()
            } else {
                text.chars().take(max).collect()
            }
        });

        engine.register_fn("take_lines", |text: &str, n: i64| -> String {
            text.lines().take(n.max(0) as usize).collect::<Vec<_>>().join("\n")
        });

        engine.register_fn("skip_lines", |text: &str, n: i64| -> String {
            text.lines().skip(n.max(0) as usize).collect::<Vec<_>>().join("\n")
        });

        engine.register_fn("squeeze_whitespace", |text: &str| -> String {
            let re = Regex::new(r"\s+").unwrap();
            re.replace_all(text, " ").into_owned()
        });

        let http_client = reqwest::Client::builder()
            .pool_max_idle_per_host(32)
            .build()
            .expect("Failed to create HTTP client");

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .expect("Failed to create Tokio runtime");

        Self {
            clients: Arc::new(RwLock::new(HashMap::new())),
            rhai_engine: Arc::new(engine),
            script_cache: Arc::new(RwLock::new(HashMap::new())),
            grpc_channels: Arc::new(RwLock::new(HashMap::new())),
            http_client: Arc::new(http_client),
            runtime: Arc::new(runtime),
        }
    }

    pub fn get_or_create_client(&self, bucket: &str) -> Result<Arc<dyn ObjectStore>, Box<dyn Error>> {
        {
            let clients = self.clients.read().map_err(|e| format!("Lock poisoned: {}", e))?;
            if let Some(client) = clients.get(bucket) {
                return Ok(client.clone());
            }
        }

        let client: Arc<dyn ObjectStore> = Arc::new(
            AmazonS3Builder::from_env()
                .with_bucket_name(bucket)
                .build()
                .map_err(|e| format!("Failed to create S3 client for bucket '{}': {}", bucket, e))?
        );

        let mut clients = self.clients.write().map_err(|e| format!("Lock poisoned: {}", e))?;
        clients.insert(bucket.to_string(), client.clone());
        Ok(client)
    }

    pub async fn get_or_create_channel(&self, endpoint: &str) -> Result<Channel, Box<dyn Error + Send + Sync>> {
        {
            let channels = self.grpc_channels.read().map_err(|e| format!("Lock poisoned: {}", e))?;
            if let Some(channel) = channels.get(endpoint) {
                return Ok(channel.clone());
            }
        }

        let channel = Channel::from_shared(endpoint.to_string())
            .map_err(|e| format!("Invalid endpoint '{}': {}", endpoint, e))?
            .connect()
            .await
            .map_err(|e| format!("Failed to connect to '{}': {}", endpoint, e))?;

        let mut channels = self.grpc_channels.write().map_err(|e| format!("Lock poisoned: {}", e))?;
        channels.insert(endpoint.to_string(), channel.clone());
        Ok(channel)
    }

    pub fn drop_channel(&self, endpoint: &str) {
        if let Ok(mut channels) = self.grpc_channels.write() {
            channels.remove(endpoint);
        }
    }

    pub fn get_or_compile_script(&self, script: &str) -> Result<AST, Box<dyn Error>> {
        {
            let cache = self.script_cache.read().map_err(|e| format!("Lock poisoned: {}", e))?;
            if let Some(ast) = cache.get(script) {
                return Ok(ast.clone());
            }
        }

        let ast = self.rhai_engine.compile(script)
            .map_err(|e| format!("Script compilation error: {}", e))?;

        let mut cache = self.script_cache.write().map_err(|e| format!("Lock poisoned: {}", e))?;
        cache.insert(script.to_string(), ast.clone());
        Ok(ast)
    }

    pub fn run_script(&self, text: &str, ast: &AST) -> Result<String, Box<dyn Error>> {
        let mut scope = Scope::new();
        scope.push("text", text.to_string());

        let result: String = self.rhai_engine.eval_ast_with_scope(&mut scope, ast)
            .map_err(|e| format!("Script execution error: {}", e))?;

        Ok(result)
    }
}
