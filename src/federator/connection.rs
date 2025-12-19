//! WebSocket connection management for federator client
//!
//! Handles connection establishment, reconnection, and message sending/receiving.

use crate::federator::protocol::{FederatorMessage, NodeMessage};
use anyhow::{anyhow, Result};
use futures_util::{SinkExt, StreamExt};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::timeout;
use tokio_tungstenite::{
    connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream,
};
use tracing::{debug, error, info, warn};

/// WebSocket connection to the federator
pub struct WebSocketConnection {
    url: String,
    write_tx: Option<mpsc::Sender<NodeMessage>>,
    connected: Arc<AtomicBool>,
}

impl WebSocketConnection {
    /// Create a new WebSocket connection manager
    pub fn new(url: &str) -> Self {
        Self {
            url: url.to_string(),
            write_tx: None,
            connected: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Check if connected
    pub fn is_connected(&self) -> bool {
        self.connected.load(Ordering::SeqCst)
    }

    /// Connect to the federator and start message handling
    pub async fn connect(
        &mut self,
        message_handler: mpsc::Sender<FederatorMessage>,
    ) -> Result<()> {
        info!("Connecting to federator at {}", self.url);

        let (ws_stream, _response) = connect_async(&self.url)
            .await
            .map_err(|e| anyhow!("Failed to connect to federator: {}", e))?;

        info!("Connected to federator");
        self.connected.store(true, Ordering::SeqCst);

        let (write_half, read_half) = ws_stream.split();

        // Create channel for outgoing messages
        let (write_tx, write_rx) = mpsc::channel::<NodeMessage>(100);
        self.write_tx = Some(write_tx);

        // Spawn write task
        let connected = self.connected.clone();
        tokio::spawn(async move {
            Self::write_loop(write_half, write_rx, connected).await;
        });

        // Spawn read task
        let connected = self.connected.clone();
        tokio::spawn(async move {
            Self::read_loop(read_half, message_handler, connected).await;
        });

        Ok(())
    }

    /// Send a message to the federator
    pub async fn send(&self, msg: NodeMessage) -> Result<()> {
        if let Some(tx) = &self.write_tx {
            tx.send(msg)
                .await
                .map_err(|e| anyhow!("Failed to queue message: {}", e))?;
            Ok(())
        } else {
            Err(anyhow!("Not connected"))
        }
    }

    /// Disconnect from the federator
    pub fn disconnect(&mut self) {
        self.connected.store(false, Ordering::SeqCst);
        self.write_tx = None;
    }

    /// Write loop - sends messages from the channel to the WebSocket
    async fn write_loop(
        mut write_half: futures_util::stream::SplitSink<
            WebSocketStream<MaybeTlsStream<TcpStream>>,
            Message,
        >,
        mut write_rx: mpsc::Receiver<NodeMessage>,
        connected: Arc<AtomicBool>,
    ) {
        while let Some(msg) = write_rx.recv().await {
            if !connected.load(Ordering::SeqCst) {
                break;
            }

            match serde_json::to_string(&msg) {
                Ok(json) => {
                    debug!("Sending message: {}", json);
                    if let Err(e) = write_half.send(Message::Text(json)).await {
                        error!("Failed to send message: {}", e);
                        connected.store(false, Ordering::SeqCst);
                        break;
                    }
                }
                Err(e) => {
                    error!("Failed to serialize message: {}", e);
                }
            }
        }

        // Try to close gracefully
        let _ = write_half.close().await;
        info!("Write loop ended");
    }

    /// Read loop - receives messages from WebSocket and forwards to handler
    async fn read_loop(
        mut read_half: futures_util::stream::SplitStream<
            WebSocketStream<MaybeTlsStream<TcpStream>>,
        >,
        message_handler: mpsc::Sender<FederatorMessage>,
        connected: Arc<AtomicBool>,
    ) {
        while let Some(result) = read_half.next().await {
            if !connected.load(Ordering::SeqCst) {
                break;
            }

            match result {
                Ok(Message::Text(text)) => {
                    debug!("Received message: {}", text);
                    match serde_json::from_str::<FederatorMessage>(&text) {
                        Ok(msg) => {
                            if let Err(e) = message_handler.send(msg).await {
                                error!("Failed to forward message to handler: {}", e);
                                break;
                            }
                        }
                        Err(e) => {
                            warn!("Failed to parse federator message: {} - {}", e, text);
                        }
                    }
                }
                Ok(Message::Close(_)) => {
                    info!("Federator closed connection");
                    break;
                }
                Ok(Message::Ping(data)) => {
                    debug!("Received ping");
                    // Pong is handled automatically by tungstenite
                }
                Ok(Message::Pong(_)) => {
                    debug!("Received pong");
                }
                Ok(_) => {
                    // Binary or other message types - ignore
                }
                Err(e) => {
                    error!("WebSocket read error: {}", e);
                    break;
                }
            }
        }

        connected.store(false, Ordering::SeqCst);
        info!("Read loop ended");
    }
}

/// Connection manager with automatic reconnection
pub struct ReconnectingConnection {
    url: String,
    reconnect_delay: Duration,
    max_reconnect_delay: Duration,
    connection: Option<WebSocketConnection>,
    connected: Arc<AtomicBool>,
}

impl ReconnectingConnection {
    /// Create a new reconnecting connection manager
    pub fn new(url: &str, reconnect_delay_secs: u64) -> Self {
        Self {
            url: url.to_string(),
            reconnect_delay: Duration::from_secs(reconnect_delay_secs),
            max_reconnect_delay: Duration::from_secs(300), // Max 5 minutes
            connection: None,
            connected: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Check if connected
    pub fn is_connected(&self) -> bool {
        self.connected.load(Ordering::SeqCst)
    }

    /// Get the connection (if connected)
    pub fn connection(&self) -> Option<&WebSocketConnection> {
        self.connection.as_ref()
    }

    /// Connect with automatic reconnection on failure
    pub async fn connect_with_retry(
        &mut self,
        message_handler: mpsc::Sender<FederatorMessage>,
    ) -> Result<()> {
        let mut delay = self.reconnect_delay;

        loop {
            let mut conn = WebSocketConnection::new(&self.url);

            match conn.connect(message_handler.clone()).await {
                Ok(()) => {
                    self.connection = Some(conn);
                    self.connected.store(true, Ordering::SeqCst);
                    return Ok(());
                }
                Err(e) => {
                    warn!(
                        "Failed to connect to federator: {}. Retrying in {:?}",
                        e, delay
                    );
                    tokio::time::sleep(delay).await;

                    // Exponential backoff
                    delay = std::cmp::min(delay * 2, self.max_reconnect_delay);
                }
            }
        }
    }

    /// Send a message (returns error if not connected)
    pub async fn send(&self, msg: NodeMessage) -> Result<()> {
        if let Some(conn) = &self.connection {
            conn.send(msg).await
        } else {
            Err(anyhow!("Not connected to federator"))
        }
    }

    /// Disconnect
    pub fn disconnect(&mut self) {
        if let Some(conn) = &mut self.connection {
            conn.disconnect();
        }
        self.connection = None;
        self.connected.store(false, Ordering::SeqCst);
    }
}
