use std::time::Duration;

use anyhow::{anyhow, Result};
use dht11::Dht11;
use paho_mqtt::Client;
use rppal::gpio::{Gpio, IoPin, Mode};
use rppal::hal::Delay;
use serde::Serialize;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "sensor", about = "todo", rename_all = "kebab-case")]
struct Args {
    /// GPIO port to use to communicate with DHT11
    #[structopt(long, default_value = "4")]
    dht11_port: u8,

    /// MQTT broker address
    #[structopt(long, default_value = "tcp://localhost:1883")]
    broker_address: String,

    /// Number of seconds between each sensor check
    #[structopt(long, default_value = "5")]
    check_interval_seconds: u64,
}

#[derive(Serialize)]
struct Measurement {
    temperature: f32,
    humidity: f32,
}

impl From<dht11::Measurement> for Measurement {
    fn from(measurement: dht11::Measurement) -> Self {
        Self {
            temperature: measurement.temperature as f32 / 10.0 * 1.8 + 32.0,
            humidity: measurement.humidity as f32 / 10.0,
        }
    }
}

fn read_and_submit_measurement(
    sensor: &mut Dht11<IoPin>,
    client: &Client,
    delay: &mut Delay,
) -> Result<()> {
    let measurement: Measurement = sensor
        .perform_measurement(delay)
        .map_err(|e| anyhow!("Error reading from sensor: {:?}", e))?
        .into();

    let payload = serde_json::to_string(&measurement)?;

    let msg = paho_mqtt::MessageBuilder::new()
        .topic("home/sensors")
        .payload(payload)
        .qos(1)
        .finalize();

    client.publish(msg)?;

    println!(
        "Temperature: {}degF, humidity: {}%",
        measurement.temperature, measurement.humidity
    );

    Ok(())
}

fn main() -> Result<()> {
    let args = Args::from_args();

    let pin = Gpio::new()?.get(args.dht11_port)?.into_io(Mode::Output);
    let mut delay = Delay::new();
    let mut sensor = Dht11::new(pin);

    let client = paho_mqtt::Client::new(args.broker_address)?;
    client.connect(None)?;

    loop {
        if let Err(e) = read_and_submit_measurement(&mut sensor, &client, &mut delay) {
            println!("Error reading or submitting measurement: {:?}", e);
        }

        std::thread::sleep(Duration::from_secs(args.check_interval_seconds));
    }
}
