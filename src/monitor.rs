use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use minecraft_protocol::data::server_status::{OnlinePlayer, OnlinePlayers, ServerVersion};
use minecraft_protocol::decoder::{Decoder, DecoderReadExt};
use minecraft_protocol::version::v1_14_4::handshake::Handshake;
use minecraft_protocol::version::v1_20_3::status::{
    PingRequest, PingResponse, ServerStatus, StatusRequest,
};
use rand::Rng;
use serde::Deserialize;
use serde_json::Value;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::time;

use crate::config::Config;
use crate::proto::client::{Client, ClientState};
use crate::proto::{self, packet, packets};
use crate::proxy;
use crate::server::{Server, State};

/// Monitor ping inverval in seconds.
const MONITOR_POLL_INTERVAL: Duration = Duration::from_secs(2);

/// Status request timeout in seconds.
const STATUS_TIMEOUT: u64 = 20;

/// Ping request timeout in seconds.
const PING_TIMEOUT: u64 = 10;

/// Monitor server.
pub async fn monitor_server(config: Arc<Config>, server: Arc<Server>) {
    // Server address
    let addr = config.server.address;

    let mut poll_interval = time::interval(MONITOR_POLL_INTERVAL);
    let mut ping_only = false;

    loop {
        poll_interval.tick().await;

        // Poll server state and update internal status
        trace!(target: "lazymc::monitor", "Fetching status for {} ... ", addr);
        let status = poll_server(&config, &server, addr).await;
        apply_poll(&config, &server, status, &mut ping_only).await;

        // Sleep server when it's bedtime
        if server.should_sleep(&config).await {
            info!(target: "lazymc::monitor", "Server has been idle, sleeping...");
            server.stop(&config).await;
        }

        // Check whether we should force kill server
        if server.should_kill().await {
            error!(target: "lazymc::monitor", "Force killing server, took too long to start or stop");
            if !server.force_kill().await {
                warn!(target: "lazymc", "Failed to force kill server");
            }
        }
    }
}

/// Apply the result of one poll to the server state.
///
/// `ping_only` tracks whether the last polls only got a ping answer, so the warning is logged
/// once when that starts instead of on every poll.
async fn apply_poll(
    config: &Config,
    server: &Server,
    status: Result<Option<ServerStatus>, ()>,
    ping_only: &mut bool,
) {
    match status {
        // Got status, update
        Ok(Some(status)) => {
            if std::mem::take(ping_only) {
                info!(target: "lazymc::monitor", "Server status can be read again");
            }
            server.update_status(config, Some(status)).await;
        }

        // Error, reset status
        Err(_) => {
            *ping_only = false;
            server.update_status(config, None).await;
        }

        // Didn't get status, but ping fallback worked: leave the state as-is. The player count is
        // unknown and the cached one may be stale, so count the server as active: when unsure,
        // keep it awake.
        Ok(None) => {
            if !std::mem::replace(ping_only, true) {
                warn!(target: "lazymc::monitor", "Failed to poll server status, ping fallback succeeded, keeping the server awake");
            } else {
                debug!(target: "lazymc::monitor", "Failed to poll server status, ping fallback succeeded");
            }
            server.update_last_active().await;
        }
    }
}

/// Poll server state.
///
/// Returns `Ok` if status/ping succeeded, includes server status most of the time.
/// Returns `Err` if no connection could be established or if an error occurred.
pub async fn poll_server(
    config: &Config,
    server: &Server,
    addr: SocketAddr,
) -> Result<Option<ServerStatus>, ()> {
    // Fetch status
    if let Ok(status) = fetch_status(config, addr).await {
        return Ok(Some(status));
    }

    // Try ping fallback if server is currently started
    if server.state() == State::Started {
        debug!(target: "lazymc::monitor", "Failed to get status from started server, trying ping...");
        do_ping(config, addr).await?;

        // The server answers, keep it started
        return Ok(None);
    }

    Err(())
}

/// Attemp to fetch status from server.
async fn fetch_status(config: &Config, addr: SocketAddr) -> Result<ServerStatus, ()> {
    let mut stream = TcpStream::connect(addr).await.map_err(|_| ())?;

    // Add proxy header
    if config.server.send_proxy_v2 {
        trace!(target: "lazymc::monitor", "Sending local proxy header for server connection");
        stream
            .write_all(&proxy::local_proxy_header().map_err(|_| ())?)
            .await
            .map_err(|_| ())?;
    }

    // Dummy client
    let client = Client::dummy();

    send_handshake(&client, &mut stream, config, addr).await?;
    request_status(&client, &mut stream).await?;
    wait_for_status_timeout(&client, &mut stream).await
}

/// Attemp to ping server.
async fn do_ping(config: &Config, addr: SocketAddr) -> Result<(), ()> {
    let mut stream = TcpStream::connect(addr).await.map_err(|_| ())?;

    // Add proxy header
    if config.server.send_proxy_v2 {
        trace!(target: "lazymc::monitor", "Sending local proxy header for server connection");
        stream
            .write_all(&proxy::local_proxy_header().map_err(|_| ())?)
            .await
            .map_err(|_| ())?;
    }

    // Dummy client
    let client = Client::dummy();

    send_handshake(&client, &mut stream, config, addr).await?;
    let token = send_ping(&client, &mut stream).await?;
    wait_for_ping_timeout(&client, &mut stream, token).await
}

/// Send handshake.
async fn send_handshake(
    client: &Client,
    stream: &mut TcpStream,
    config: &Config,
    addr: SocketAddr,
) -> Result<(), ()> {
    packet::write_packet(
        Handshake {
            protocol_version: config.public.protocol as i32,
            server_addr: addr.ip().to_string(),
            server_port: addr.port(),
            next_state: ClientState::Status.to_id(),
        },
        client,
        &mut stream.split().1,
    )
    .await
}

/// Send status request.
async fn request_status(client: &Client, stream: &mut TcpStream) -> Result<(), ()> {
    packet::write_packet(StatusRequest {}, client, &mut stream.split().1).await
}

/// Send status request.
async fn send_ping(client: &Client, stream: &mut TcpStream) -> Result<u64, ()> {
    let token = rand::thread_rng().gen();
    packet::write_packet(PingRequest { time: token }, client, &mut stream.split().1).await?;
    Ok(token)
}

/// Wait for a status response.
async fn wait_for_status(client: &Client, stream: &mut TcpStream) -> Result<ServerStatus, ()> {
    // Get stream reader, set up buffer
    let (mut reader, mut _writer) = stream.split();
    let mut buf = BytesMut::new();

    loop {
        // Read packet from stream
        let (packet, _raw) = match packet::read_packet(client, &mut buf, &mut reader).await {
            Ok(Some(packet)) => packet,
            Ok(None) => break,
            Err(_) => continue,
        };

        // Catch status response
        if packet.id == packets::status::CLIENT_STATUS {
            return decode_status(&packet.data);
        }
    }

    // Some error occurred
    Err(())
}

/// Largest status response JSON accepted, in bytes. Modded servers can send more than the 32 KiB
/// the protocol crate allows for strings.
const STATUS_MAX_LEN: usize = 1024 * 1024;

/// Whether the last decoded status needed the fallback below, so the warning is logged once when
/// that starts instead of on every poll.
static STATUS_FALLBACK: AtomicBool = AtomicBool::new(false);

/// Decode a status response payload into a server status.
///
/// A status response means the server is up, so this is lenient: it only fails if the payload is
/// not JSON at all.
///
/// - Servers before Minecraft 1.20.3, and newer servers with a styled MOTD, send `description` as
///   a JSON text component (an object or an array) rather than a plain string. It is flattened to
///   its plain text. A missing or null `description` becomes empty.
/// - `players.online` and `players.max` are read on their own; `players.sample` is best effort.
/// - If `version` or the player count is missing or malformed (some plugins hide it), a
///   placeholder version is used and one player is reported online, so the server is seen online
///   but never put to sleep.
fn decode_status(data: &[u8]) -> Result<ServerStatus, ()> {
    let status: Value = serde_json::from_str(read_status_json(data)?).map_err(|_| ())?;

    let description = match status.get("description") {
        None | Some(Value::Null) => String::new(),
        Some(Value::String(text)) => text.clone(),
        Some(component) => {
            let mut text = String::new();
            push_component_text(component, &mut text);
            text
        }
    };
    let favicon = status
        .get("favicon")
        .and_then(Value::as_str)
        .map(str::to_owned);
    let version = status
        .get("version")
        .and_then(|v| ServerVersion::deserialize(v).ok());
    let players = status.get("players");
    let count = |key: &str| {
        players
            .and_then(|p| p.get(key))
            .and_then(Value::as_u64)
            .and_then(|n| u32::try_from(n).ok())
    };
    let (online, max) = (count("online"), count("max"));
    // Best effort: MOTD plugins put fake entries here, a bad one must not cost the real count.
    let sample = players
        .and_then(|p| p.get("sample"))
        .and_then(Value::as_array)
        .map(|entries| {
            entries
                .iter()
                .filter_map(|entry| OnlinePlayer::deserialize(entry).ok())
                .collect()
        })
        .unwrap_or_default();

    let (version, players) = match (version, online, max) {
        (Some(version), Some(online), Some(max)) => {
            if STATUS_FALLBACK.swap(false, Ordering::Relaxed) {
                info!(target: "lazymc::monitor", "Server status is complete again");
            }
            let players = OnlinePlayers {
                max,
                online,
                sample,
            };
            (version, players)
        }
        (version, _, max) => {
            if !STATUS_FALLBACK.swap(true, Ordering::Relaxed) {
                warn!(target: "lazymc::monitor", "Server status has no valid version or player count, keeping the server awake");
            } else {
                debug!(target: "lazymc::monitor", "Server status has no valid version or player count");
            }
            let version = version.unwrap_or_else(|| ServerVersion {
                name: proto::PROTO_DEFAULT_VERSION.to_string(),
                protocol: proto::PROTO_DEFAULT_PROTOCOL,
            });
            let players = OnlinePlayers {
                max: max.unwrap_or(0),
                online: 1,
                sample: vec![],
            };
            (version, players)
        }
    };

    Ok(ServerStatus {
        version,
        players,
        description,
        favicon,
    })
}

/// Read the length-prefixed status JSON string, up to `STATUS_MAX_LEN` bytes.
fn read_status_json(mut data: &[u8]) -> Result<&str, ()> {
    let len = data.read_var_i32().map_err(|_| ())?;
    let len = usize::try_from(len).map_err(|_| ())?;
    if len > STATUS_MAX_LEN || len > data.len() {
        return Err(());
    }
    std::str::from_utf8(&data[..len]).map_err(|_| ())
}

/// Append the plain text of a JSON text component: its `text`, then its `extra` children.
fn push_component_text(component: &Value, out: &mut String) {
    match component {
        Value::String(text) => out.push_str(text),
        Value::Number(number) => out.push_str(&number.to_string()),
        Value::Bool(value) => out.push_str(if *value { "true" } else { "false" }),
        Value::Array(parts) => parts.iter().for_each(|part| push_component_text(part, out)),
        Value::Object(map) => {
            if let Some(text) = map.get("text") {
                push_component_text(text, out);
            }
            if let Some(extra) = map.get("extra") {
                push_component_text(extra, out);
            }
        }
        Value::Null => {}
    }
}

/// Wait for a status response.
async fn wait_for_status_timeout(
    client: &Client,
    stream: &mut TcpStream,
) -> Result<ServerStatus, ()> {
    let status = wait_for_status(client, stream);
    tokio::time::timeout(Duration::from_secs(STATUS_TIMEOUT), status)
        .await
        .map_err(|_| ())?
}

/// Wait for a status response.
async fn wait_for_ping(client: &Client, stream: &mut TcpStream, token: u64) -> Result<(), ()> {
    // Get stream reader, set up buffer
    let (mut reader, mut _writer) = stream.split();
    let mut buf = BytesMut::new();

    loop {
        // Read packet from stream
        let (packet, _raw) = match packet::read_packet(client, &mut buf, &mut reader).await {
            Ok(Some(packet)) => packet,
            Ok(None) => break,
            Err(_) => continue,
        };

        // Catch ping response
        if packet.id == packets::status::CLIENT_PING {
            let ping = PingResponse::decode(&mut packet.data.as_slice()).map_err(|_| ())?;

            // Ping token must match
            if ping.time == token {
                return Ok(());
            } else {
                debug!(target: "lazymc", "Got unmatched ping response when polling server status by ping");
            }
        }
    }

    // Some error occurred
    Err(())
}

/// Wait for a status response.
async fn wait_for_ping_timeout(
    client: &Client,
    stream: &mut TcpStream,
    token: u64,
) -> Result<(), ()> {
    let status = wait_for_ping(client, stream, token);
    tokio::time::timeout(Duration::from_secs(PING_TIMEOUT), status)
        .await
        .map_err(|_| ())?
}

#[cfg(test)]
mod tests {
    use minecraft_protocol::encoder::EncoderWriteExt;

    use std::time::Duration;

    use minecraft_protocol::data::server_status::{OnlinePlayers, ServerVersion};
    use minecraft_protocol::version::v1_20_3::status::ServerStatus;

    use super::{apply_poll, decode_status};
    use crate::config::Config;
    use crate::server::{Server, State};

    /// A status response payload: the JSON as a length-prefixed string.
    fn payload(json: &str) -> Vec<u8> {
        let mut data = Vec::new();
        data.write_var_i32(json.len() as i32).unwrap();
        data.extend_from_slice(json.as_bytes());
        data
    }

    fn description(json: &str) -> String {
        decode_status(&payload(json)).unwrap().description
    }

    #[test]
    fn string_description() {
        // Minecraft 1.20.3+ without styling.
        let json = r#"{"version":{"name":"1.21.1","protocol":767},"players":{"max":20,"online":0},"description":"A Minecraft Server"}"#;
        assert_eq!(description(json), "A Minecraft Server");
    }

    #[test]
    fn object_description() {
        // Vanilla 1.20.1.
        let json = r#"{"description":{"text":"A Minecraft Server"},"players":{"max":20,"online":0},"version":{"name":"1.20.1","protocol":763}}"#;
        assert_eq!(description(json), "A Minecraft Server");
    }

    #[test]
    fn forge_object_description() {
        // Forge 1.20.1, with its extra fields.
        let json = r#"{"enforcesSecureChat":true,"forgeData":{"channels":[],"mods":[],"truncated":false,"fmlNetworkVersion":3,"d":"\u0000\u0000"},"description":{"text":"A Minecraft Server"},"players":{"max":20,"online":0},"version":{"name":"1.20.1","protocol":763}}"#;
        let status = decode_status(&payload(json)).unwrap();
        assert_eq!(status.description, "A Minecraft Server");
        assert_eq!(status.version.protocol, 763);
        assert_eq!(status.players.max, 20);
    }

    #[test]
    fn styled_object_description() {
        // Paper 1.20.3+ with a coloured MOTD (an Adventure component).
        let json = r#"{"description":{"extra":[{"color":"green","text":"Hello"},{"bold":true,"extra":[" ",{"text":"world"}],"text":""}],"text":""},"players":{"max":20,"online":0},"version":{"name":"1.21.1","protocol":767}}"#;
        assert_eq!(description(json), "Hello world");
    }

    #[test]
    fn legacy_codes_are_kept() {
        let json = r#"{"description":{"text":"§aHello"},"players":{"max":20,"online":0},"version":{"name":"1.20.1","protocol":763}}"#;
        assert_eq!(description(json), "\u{a7}aHello");
    }

    #[test]
    fn array_description() {
        let json = r#"{"description":["A ",{"text":"Minecraft"},{"translate":"x"}," Server",1],"players":{"max":20,"online":0},"version":{"name":"1.20.1","protocol":763}}"#;
        assert_eq!(description(json), "A Minecraft Server1");
    }

    #[test]
    fn favicon_is_kept() {
        let json = r#"{"description":{"text":""},"favicon":"data:image/png;base64,AA==","players":{"max":1,"online":0},"version":{"name":"1.20.1","protocol":763}}"#;
        let status = decode_status(&payload(json)).unwrap();
        assert_eq!(
            status.favicon.as_deref(),
            Some("data:image/png;base64,AA==")
        );
    }

    #[test]
    fn missing_or_null_description_is_empty() {
        let missing =
            r#"{"players":{"max":20,"online":0},"version":{"name":"1.20.1","protocol":763}}"#;
        assert_eq!(description(missing), "");
        let null = r#"{"description":null,"players":{"max":20,"online":0},"version":{"name":"1.20.1","protocol":763}}"#;
        assert_eq!(description(null), "");
    }

    #[test]
    fn missing_players_keeps_the_server_awake() {
        // For example Paper with setHidePlayers(true).
        let json = r#"{"description":{"text":"hi"},"version":{"name":"1.20.1","protocol":763}}"#;
        let status = decode_status(&payload(json)).unwrap();
        assert_eq!(status.description, "hi");
        assert_eq!(status.version.protocol, 763);
        assert_eq!(status.players.online, 1);
    }

    #[test]
    fn malformed_players_or_missing_version_keeps_the_server_awake() {
        let json = r#"{"description":"hi","players":{"max":"many"},"version":{"name":"1.20.1","protocol":763}}"#;
        assert_eq!(decode_status(&payload(json)).unwrap().players.online, 1);

        let json = r#"{"description":"hi","players":{"max":20,"online":0}}"#;
        let status = decode_status(&payload(json)).unwrap();
        assert_eq!(status.players.online, 1);
        assert_eq!(status.players.max, 20);
        assert_eq!(
            status.version.protocol,
            crate::proto::PROTO_DEFAULT_PROTOCOL
        );

        // Not even an object: still a status response, so still awake.
        assert_eq!(decode_status(&payload("[]")).unwrap().players.online, 1);
    }

    #[test]
    fn malformed_sample_keeps_the_real_count() {
        // A MOTD plugin's fake sample entry (no uuid) must not turn 0 online into 1.
        let json = r#"{"description":"hi","players":{"max":20,"online":0,"sample":[{"name":"Welcome!"},{"name":"fh","id":"4566e69f-c907-48ee-8d71-d7ba5aa00d20"}]},"version":{"name":"1.20.1","protocol":763}}"#;
        let status = decode_status(&payload(json)).unwrap();
        assert_eq!(status.players.online, 0);
        assert_eq!(status.players.max, 20);
        assert_eq!(status.players.sample.len(), 1);
        assert_eq!(status.players.sample[0].name, "fh");
    }

    fn status_with(online: u32) -> ServerStatus {
        ServerStatus {
            version: ServerVersion {
                name: "1.20.1".into(),
                protocol: 763,
            },
            players: OnlinePlayers {
                max: 20,
                online,
                sample: vec![],
            },
            description: String::new(),
            favicon: None,
        }
    }

    #[tokio::test]
    async fn ping_only_poll_keeps_a_stale_zero_player_server_awake() {
        let config: Config = toml::from_str(
            "[server]\ncommand = \"true\"\n[time]\nsleep_after = 1\nminimum_online_time = 0\n",
        )
        .unwrap();
        let server = Server::default();
        let mut ping_only = false;

        // A player was on, then the last status that decoded said nobody is.
        apply_poll(&config, &server, Ok(Some(status_with(1))), &mut ping_only).await;
        apply_poll(&config, &server, Ok(Some(status_with(0))), &mut ping_only).await;
        assert_eq!(server.state(), State::Started);
        tokio::time::sleep(Duration::from_millis(1100)).await;
        assert!(server.should_sleep(&config).await);

        // Now only pings get through: the cached 0 may be stale, so stay awake.
        apply_poll(&config, &server, Ok(None), &mut ping_only).await;
        assert!(ping_only);
        assert_eq!(server.state(), State::Started);
        assert!(!server.should_sleep(&config).await);

        // A readable status again clears the ping-only state.
        apply_poll(&config, &server, Ok(Some(status_with(0))), &mut ping_only).await;
        assert!(!ping_only);
    }

    #[test]
    fn status_over_32_kib_decodes() {
        // Old Forge packs list every mod in the status.
        let mods: Vec<String> = (0..1500)
            .map(|i| format!(r#"{{"modid":"mod{:05}","version":"1.0.0"}}"#, i))
            .collect();
        let json = format!(
            r#"{{"description":{{"text":"modded"}},"players":{{"max":20,"online":0}},"version":{{"name":"1.12.2","protocol":340}},"modinfo":{{"type":"FML","modList":[{}]}}}}"#,
            mods.join(",")
        );
        assert!(json.len() > 40 * 1024, "{} bytes", json.len());
        let status = decode_status(&payload(&json)).unwrap();
        assert_eq!(status.description, "modded");
        assert_eq!(status.players.online, 0);
    }

    #[test]
    fn status_over_1_mib_is_refused() {
        let json = format!(
            r#"{{"description":"{}"}}"#,
            "x".repeat(super::STATUS_MAX_LEN)
        );
        assert!(decode_status(&payload(&json)).is_err());
    }

    #[test]
    fn deep_nesting_is_an_error_not_a_crash() {
        let depth = 10_000;
        let json = format!(
            r#"{{"description":{}"x"{},"players":{{"max":1,"online":0}},"version":{{"name":"1.20.1","protocol":763}}}}"#,
            r#"{"extra":["#.repeat(depth),
            "]}".repeat(depth)
        );
        assert!(decode_status(&payload(&json)).is_err());
    }

    #[test]
    fn invalid_status_is_an_error() {
        assert!(decode_status(&payload("not json")).is_err());
        assert!(decode_status(&[]).is_err());
        // Length prefix longer than the data, and a negative length.
        assert!(decode_status(&[0x05, b'{', b'}']).is_err());
        assert!(decode_status(&[0xff, 0xff, 0xff, 0xff, 0x0f]).is_err());
    }
}
