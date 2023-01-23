use async_nats::jetstream;
use clap::Parser;
use futures::StreamExt;
use sqlx::PgPool;
use std::str::from_utf8;

use serde_json::json;

#[derive(clap::ValueEnum, Clone)]
enum Type {
    Bytes,
    JSON,
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

    // DB table to insert data into
    table: String,
}

async fn main() -> Result<(), async_nats::Error> {
    let config = Config::parse();

    //
    // DATABASE
    //
    let pool = PgPool::connect("postgres://postgres:diode@128.46.199.238").await?;

    println!("Running database migrations");
    sqlx::migrate!("./migrations").run(&pool).await?;

    //
    // END DATABASE
    //

    println!("Connecting to NATS.");
    let client = async_nats::ConnectOptions::with_user_and_password(
        config.nats_username,
        config.nats_password,
    )
    .connect(config.nats_server)
    .await?;

    println!("Connected");

    let jetstream = jetstream::new(client);

    // Get stream and ensure the consumer
    let consumer = jetstream
        .get_stream(config.stream)
        .await?
        .create_consumer(jetstream::consumer::pull::Config {
            durable_name: Some(config.nats_consumer_name),
            filter_subject: config.subject_prefix + ".*",
            ..Default::default()
        })
        .await?;

    // let mut stream = consumer.messages().await?;
    //

    loop {
        println!("Start pull");
        let mut messages = consumer
            .fetch()
            .max_messages(500)
            //  .expires(Duration::from_secs(1))
            .messages()
            .await?;
        println!("End pull");

        let mut times: Vec<time::OffsetDateTime> = vec![];
        let mut subjects = vec![];
        let mut data = vec![];

        println!("Start loop");
        while let Some(message) = messages.next().await {
            let message = message?;

            // TODO: Check for error and just skip message
            let d = match config.data_type {
                Type::Bytes => message.payload,
                Type::JSON => json!(from_utf8(&message.payload)?),
            };

            times.push(message.info()?.published);
            subjects.push(message.subject.clone());
            data.push(d);

            // println!(
            //     "got messages on subject {} with payload {:?}",
            //     message.subject,
            //     from_utf8(&message.payload)?
            // );
            //
            message.ack().await?;
        }
        println!("End loop");

        println!("Len: {}", times.len());
        if times.len() > 0 {
            println!("Start query");
            sqlx::query!(
                r#"
            INSERT INTO natsql (time, subject, msg)
            SELECT * FROM UNNEST($1::timestamptz[], $2::text[], $3::jsonb[])
        "#,
                &times,
                &subjects,
                &data,
            )
            .execute(&pool)
            .await?;
            println!("End query");
        }
    }
}
