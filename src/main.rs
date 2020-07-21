use clap::{App, Arg, ArgMatches};
use futures::stream::FuturesUnordered;
use futures::{StreamExt, TryStreamExt};
use log::{debug, info};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::Consumer;
use rdkafka::message::{BorrowedMessage, OwnedMessage};
use rdkafka::Message;
use std::fs::OpenOptions;
use std::io::prelude::*;
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
            info!("Message received: {}", borrowed_message.offset());
            // We need to own this message
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

async fn process_messages(dir: &Path, basename: String, mut receiver: Receiver<String>) {
    // just write to a simple file
    while let Some(payload) = receiver.recv().await {
        info!("Processing payload");
        // TODO: check for empty payloads
        let fields: Vec<&str> = payload.split('|').collect();
        let day: Vec<&str> = fields[12].split('T').collect();
        let filename = dir
            .clone()
            .join(format!("{}.{}", basename, day[0].replace("-", "")));
        info!("Writing to file {:?}", filename);
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(filename)
            .unwrap();

        writeln!(file, "{}", payload);
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
                .required(true)
                .help("Kafka servers to talk to"),
        )
        .arg(
            Arg::with_name("shred.dir")
                .long("shred.dir")
                .takes_value(true)
                .default_value("/tmp")
                .help("Directory to create the files for shredding"),
        )
        .arg(
            Arg::with_name("basename")
                .long("basename")
                .takes_value(true)
                .default_value("xdmod")
                .help("Basename of the file to shred (excluding date stamp YYYYMMDD)"),
        )
        .arg(
            Arg::with_name("logfile")
                .long("logfile")
                .short("l")
                .takes_value(true)
                .help("Log file name."),
        )
        .get_matches();

    setup_logging(true, matches.value_of("logfile"));

    info!("Logging setup done");

    let bootstrap_servers = matches.value_of("bootstrap.servers").unwrap().to_owned();
    let shreddir = Path::new(matches.value_of("shred.dir").unwrap()).to_owned();
    let basename = matches.value_of("basename").unwrap().to_owned();
    let (mut sender, mut receiver): (Sender<String>, Receiver<String>) = channel(1000);

    info!("Channel created");

    tokio::join!(
        tokio::spawn(async move { fetch(bootstrap_servers.to_string(), sender).await }),
        tokio::spawn(async move { process_messages(&shreddir, basename, receiver).await })
    );
}
