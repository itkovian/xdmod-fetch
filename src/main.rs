use clap::{App, Arg, ArgMatches};
use futures::stream::FuturesUnordered;
use futures::{StreamExt, TryStreamExt};
use log::{debug, info};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::Consumer;
use rdkafka::message::{BorrowedMessage, OwnedMessage};
use rdkafka::Message;
use std::path::{Path, PathBuf};
use tokio::prelude::*;
use tokio::sync::mpsc::{channel, Receiver, Sender};

fn setup_logging(debug: bool, logfile: Option<&str>) -> Result<(), log::SetLoggerError> {
    let level_filter = if debug {
        log::LevelFilter::Debug
    } else {
        log::LevelFilter::Info
    };

    let base_config = fern::Dispatch::new()
        .format(|out, message, record| {
            out.finish(format_args!(
                "[{}][{}][{}] {}",
                chrono::Local::now().to_rfc3339(),
                record.target(),
                record.level(),
                message
            ))
        })
        .level(level_filter);

    match logfile {
        Some(filename) => {
            let r = fern::log_reopen(&PathBuf::from(filename), Some(libc::SIGHUP)).unwrap();
            base_config.chain(r)
        }
        None => base_config.chain(std::io::stdout()),
    }
    .apply()
}

async fn record_borrowed_message_receipt(msg: &BorrowedMessage<'_>) {
    // Simulate some work that must be done in the same order as messages are
    // received; i.e., before truly parallel processing can begin.
    info!("Message received: {}", msg.offset());
}

async fn fetch(bootstrap_servers: String, sender: Sender<String>) {
    let mut cfg = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap_servers)
        .set("security.protocol", "SSL")
        .set("ssl.ca.location", "/etc/ipa/ca.crt")
        .set("ssl.key.location", "/etc/ipa/quattor/keys/host.key")
        .set(
            "ssl.certificate.location",
            "/etc/ipa/quattor/certs/host.pem",
        )
        .to_owned();

    let consumer: StreamConsumer = cfg
        .set("group.id", "xdmod-test")
        .create()
        .expect("Cannot create streamconsumer");

    consumer.subscribe(&["xdmod"]);

    // Create the outer pipeline on the message stream.
    let stream_processor = consumer.start().try_for_each(|borrowed_message| {
        let mut owned_sender = sender.clone();
        async move {
            // Process each message
            record_borrowed_message_receipt(&borrowed_message).await;
            // Borrowed messages can't outlive the consumer they are received from, so they need to
            // be owned in order to be sent to a separate thread.
            let owned_message = borrowed_message.detach();
            tokio::spawn(async move {
                if let Some(Ok(payload)) = owned_message.payload_view::<str>() {
                    owned_sender.send(payload.to_string()).await;
                }
            });
            Ok(())
        }
    });

    info!("Starting event loop");
    stream_processor.await.expect("stream processing failed");
    info!("Stream processing terminated");
}

async fn process_messages(mut receiver: Receiver<String>) {

    // just write to a sinmple file

    while let Some(payload) = receiver.recv().await {
        tokio::fs::write("/tmp/xdmod-fetch.out", payload).await;
    }
}

#[tokio::main]
async fn main() {
    let matches = App::new("xdmod-fetch")
        .version("0.1")
        .author("Andy Georges <itkovian@gmail.com>")
        .about("Fetch Slurm job lines from Kafka and prep them for shredding in XDMoD")
        .arg(
            Arg::with_name("bootstrap.servers")
                .long("bootstrap.servers")
                .takes_value(true)
                .required(true),
        )
        .get_matches();

    setup_logging(true, Some("/tmp/xdmod-fetch.log"));

    info!("Logging stup done");

    let bootstrap_servers = matches.value_of("bootstrap.servers").unwrap().to_owned();
    let (mut sender, mut receiver): (Sender<String>, Receiver<String>) = channel(1000);

    info!("Channel created");

    tokio::join!(
        tokio::spawn(async move { fetch(bootstrap_servers.to_string(), sender).await }),
        tokio::spawn(async move { process_messages(receiver).await })
    );


}
