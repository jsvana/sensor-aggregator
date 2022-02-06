use std::time::{Duration, SystemTime};

use anyhow::{anyhow, Result};
use influxdb_client::{Client, Point, Precision, TimestampOptions};
use paho_mqtt as mqtt;
use serde::Deserialize;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "sensor-aggregator", about = "todo", rename_all = "kebab-case")]
struct Args {
    /// MQTT broker address
    #[structopt(long, default_value = "tcp://localhost:1883")]
    broker_address: String,

    /// InfluxDB server address
    #[structopt(long, default_value = "http://localhost:9999")]
    influxdb_address: String,

    /// InfluxDB token
    #[structopt(long)]
    influxdb_token: String,

    /// InfluxDB bucket
    #[structopt(long)]
    influxdb_bucket: String,

    /// InfluxDB org
    #[structopt(long)]
    influxdb_org: String,
}

#[derive(Debug, Deserialize)]
struct Measurement {
    room: String,
    temperature: f64,
    humidity: f64,
}

async fn handler(client: &Client, msg: mqtt::Message) -> Result<bool> {
    let payload = std::str::from_utf8(msg.payload())?;
    let measurement: Measurement = serde_json::from_str(payload)?;
    println!("{:?}", measurement);

    let timestamp = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)?
        .as_millis();
    let points = vec![Point::new("room_measurement")
        .tag("room", measurement.room.as_str())
        .field("temperature", measurement.temperature)
        .field("humidity", measurement.humidity)
        .timestamp(timestamp as i64)];

    client
        .insert_points(&points, TimestampOptions::FromPoint)
        .await
        .map_err(|e| anyhow!("Error submitting point: {:?}", e))?;

    Ok(true)
}

fn try_reconnect(cli: &mqtt::Client) -> bool {
    println!("Connection lost. Waiting to retry connection");
    for _ in 0..12 {
        std::thread::sleep(Duration::from_millis(5000));
        if cli.reconnect().is_ok() {
            println!("Successfully reconnected");
            return true;
        }
    }
    println!("Unable to reconnect after several attempts.");
    false
}

fn sub_id(id: i32) -> mqtt::Properties {
    mqtt::properties![
        mqtt::PropertyCode::SubscriptionIdentifier => id
    ]
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let args = Args::from_args();

    let create_opts = mqtt::CreateOptionsBuilder::new()
        .mqtt_version(mqtt::MQTT_VERSION_5)
        .server_uri(args.broker_address)
        .client_id("sensor-aggregator")
        .finalize();

    let mut cli = mqtt::Client::new(create_opts)?;
    let rx = cli.start_consuming();

    let lwt = mqtt::MessageBuilder::new()
        .topic("lwt")
        .payload("Sync consumer v5 lost connection")
        .finalize();

    let conn_opts = mqtt::ConnectOptionsBuilder::new()
        .mqtt_version(mqtt::MQTT_VERSION_5)
        .clean_session(false)
        .will_message(lwt)
        .finalize();

    let rsp = cli.connect(conn_opts)?;

    if let Some(conn_rsp) = rsp.connect_response() {
        println!(
            "Connected to: '{}' with MQTT version {}",
            conn_rsp.server_uri, conn_rsp.mqtt_version
        );

        if !conn_rsp.session_present {
            println!("Subscribing to topics...");
            cli.subscribe_with_options("home/sensors", 0, None, sub_id(1))?;
        }
    }

    let client = Client::new(args.influxdb_address, args.influxdb_token)
        .with_org_id(args.influxdb_org)
        .with_bucket(args.influxdb_bucket)
        .with_precision(Precision::MS);
    //.insert_to_stdout();

    println!("Waiting for messages...");
    for msg in rx.iter() {
        if let Some(msg) = msg {
            handler(&client, msg).await?;
        } else if cli.is_connected() || !try_reconnect(&cli) {
            break;
        }
    }

    if cli.is_connected() {
        println!("Disconnecting");
        cli.disconnect(None)?;
    }

    println!("Exiting");

    Ok(())
}
