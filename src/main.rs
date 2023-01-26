use async_nats::{
    jetstream::{self, consumer::AckPolicy, message::Info, Message},
    Client,
};
use clap::Parser;
use color_eyre::eyre::{ensure, eyre, Report, Result};
use core::fmt;
use futures::StreamExt;
use serde_json::Value;
use sqlx::{types::JsonValue, PgPool, Postgres, QueryBuilder};

#[derive(clap::ValueEnum, Clone)]
enum Type {
    Bytes,
    JSON,
}

impl fmt::Display for Type {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Type::Bytes => write!(f, "bytea"),
            Type::JSON => write!(f, "jsonb"),
        }
    }
}

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Config {
    // Connection to string for database receiving data
    #[arg(short = 'd')]
    db_connection_string: String,

    // NATS server address
    #[arg(short = 's')]
    nats_server: String,

    // NATS server username
    #[arg(long = "user")]
    nats_username: String,

    // NATS server password
    #[arg(long = "password")]
    nats_password: String,

    #[arg(long = "consumer-name", default_value_t = String::from("natsql"))]
    nats_consumer_name: String,

    #[arg(value_enum, long = "type", default_value_t = Type::Bytes)]
    data_type: Type,

    // NATS stream to consume
    stream: String,

    // NATS subject within <steam> to subscribe to
    subject_prefix: String,
}

#[derive(Debug)]
enum Payload {
    Bytes(Vec<u8>),
    Json(JsonValue),
}

#[derive(Debug)]
struct Data<'a> {
    info: Info<'a>,
    message: &'a Message,
    payload: Payload,
}

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    color_eyre::install()?;
    let config = Config::parse();

    // TODO: Deal with DB going away?
    // IDEA: Do we have a `natsql init --type json <table-name>` and then auto-detect type from
    //       existing database?
    // TODO: How to deal with migrations?

    let client = nats_connect(&config).await?;
    println!("Connected to NATS.");

    let db = postgres_connect(&config).await?;
    ensure_database(&config, &db).await?;
    println!("Connected to Postgres.");

    let jetstream = jetstream::new(client);

    // Get stream and ensure the consumer
    let consumer = jetstream
        .get_stream(config.stream)
        .await?
        .create_consumer(jetstream::consumer::pull::Config {
            durable_name: Some(config.nats_consumer_name),
            filter_subject: config.subject_prefix.clone() + ".*",
            ack_policy: AckPolicy::All,
            max_ack_pending: 5_000,
            ..Default::default()
        })
        .await?;

    let insert = format!(
        r#"INSERT INTO natsql."{}"(time, sequence, subject, msg) "#,
        config.subject_prefix
    );

    loop {
        let mut insert_q: QueryBuilder<Postgres> = QueryBuilder::new(insert.clone());

        println!("Pulling");
        let messages: Vec<Message> = consumer
            .batch()
            .max_messages(5_000)
            .expires(std::time::Duration::from_secs(1))
            .messages()
            .await
            // TODO: Handle specific errors
            .expect("Batch subscription failure.")
            .map(|msg| msg.expect("Unexpected operational error from NATS"))
            // TDOO: Handle specific errors with concrete errors land?
            .collect()
            .await;

        // TODO: This is CPU limited by parsing JSON ... multi-thread?
        let data: Vec<Data> = messages
            .iter()
            .filter_map(|msg| {
                let info = msg
                    .info()
                    .expect("Non-Jetstream message from a Jetstream consumer?");

                let payload: Payload = match config.data_type {
                    Type::Bytes => Payload::Bytes(msg.payload.to_vec()),
                    Type::JSON => match serde_json::from_slice::<Value>(&msg.payload) {
                        Err(_) => {
                            // Ignore meassages that are not valid JSON
                            println!("Invalid JSON. Sequence {}", info.stream_sequence);
                            return None;
                        }
                        Ok(v) => Payload::Json(v),
                    },
                };

                Some(Data {
                    info: info.clone(),
                    message: msg,
                    payload,
                })
            })
            .collect();

        insert_q.push_values(data.iter(), |mut b, d| {
            b.push_bind(d.info.published);
            b.push_bind(d.info.stream_sequence as i64);
            b.push_bind(&d.message.subject);

            match &d.payload {
                Payload::Bytes(d) => b.push_bind(d),
                Payload::Json(d) => b.push_bind(d),
            };
        });

        if let Some(m) = data.last() {
            println!("Inserting {} records", data.len());
            insert_q.build().execute(&db).await?;

            // Note: This *will* ACK invalid messages to avoid getting stuck by them.
            // In other words, messages will be ignored if not data type valid (e.g., invalid JSON)
            m.message.ack().await?;
        } else {
            println!("No messages to insert.");
        }
    }
}

async fn nats_connect(config: &Config) -> Result<Client> {
    async_nats::ConnectOptions::with_user_and_password(
        config.nats_username.clone(),
        config.nats_password.clone(),
    )
    .connect(config.nats_server.clone())
    .await
    .map_err(Report::msg)
}

async fn postgres_connect(config: &Config) -> Result<PgPool> {
    PgPool::connect(&config.db_connection_string)
        .await
        .map_err(Report::msg)
}

async fn ensure_database(config: &Config, db: &PgPool) -> Result<()> {
    println!("Ensure natsql schema exists.");
    sqlx::query!("CREATE SCHEMA IF NOT EXISTS natsql")
        .persistent(false)
        .execute(db)
        .await?;

    // TODO: Create indexes?
    println!("Ensure subject's table exists");

    // TODO: A unique primary key in order? -- Maybe sequence?
    QueryBuilder::new(format!(
        r#"CREATE TABLE IF NOT EXISTS natsql."{}" (
             time TIMESTAMPTZ NOT NULL,
             sequence BIGINT NOT NULL,
             subject TEXT NOT NULL,
             msg {} NOT NULL
           )"#,
        config.subject_prefix, config.data_type
    ))
    .build()
    .persistent(false)
    .execute(db)
    .await?;

    // Make sure existing table data type matches runtime parameters
    let data_type = sqlx::query_scalar!(
        "SELECT data_type
         FROM information_schema.columns
         WHERE table_schema ='natsql' 
           AND table_name = $1
           AND column_name = 'msg'",
        config.subject_prefix
    )
    .persistent(false)
    .fetch_one(db)
    .await?
    .ok_or_else(|| eyre!("Invalid table schema."))?;

    ensure!(
        data_type == config.data_type.to_string(),
        format!(
            "Table '{}' has wrong message data type. Found: {}, Expected: {}",
            config.subject_prefix, data_type, config.data_type
        )
    );

    Ok(())
}
