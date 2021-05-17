#[macro_use]
extern crate log;
extern crate chrono;
extern crate env_logger;
extern crate kafka;
extern crate zerocopy;

use std::time::{Duration};
use std::convert::{TryFrom, TryInto};
use std::str;

use zerocopy::AsBytes;
use zerocopy::byteorder::U64;
use kafka::producer::{Producer, Record, RequiredAcks};
use kafka::error::Error as KafkaError;
use std::{time, thread};
use chrono::Local;
use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};
use byteorder::{ByteOrder, LittleEndian};

/// This program demonstrates sending single message through a
/// `Producer`.  This is a convenient higher-level client that will
/// fit most use cases.
fn main() {
    env_logger::init();

    let producer_handle = thread::spawn(move || {
        let broker = "localhost:9092".to_owned();
        let produce_topic = "rs2j";
        produce(produce_topic, vec![broker.clone()])
    });
    thread::spawn(|| {
        let consume_topic = "j2rs".to_owned();
        let broker = "localhost:9092".to_owned();
        let group = "my-group".to_owned();
        consume(group, consume_topic, vec![broker])
    });

    info!("Spawned");
    producer_handle.join().unwrap();
}


fn produce(topic: &str, brokers: Vec<String>) -> Result<(), KafkaError> {
    info!("Starting producer to topic {:?}", topic);
    let sleep_duration = time::Duration::from_secs(4);
    let mut producer =
        Producer::from_hosts(brokers)
            // ~ give the brokers one second time to ack the message
            .with_ack_timeout(Duration::from_secs(1))
            // ~ require only one broker to ack the message
            .with_required_acks(RequiredAcks::One)
            // ~ build the producer with the above settings
            .create()?;

    let mut index: u64 = 0;
    loop {
        let now = Local::now();
        let message = "hello, kafka from Rust 🧡 at ".to_owned() + &now.format("%Y-%m-%d %H:%M:%S").to_string();
        let data = message.as_bytes();
        info!("Sending to topic {:?}", topic);
        // let mut buf = [0; 8];
        // LittleEndian::write_u64(&mut buf, index);
        // let key: [u8] = buf.try_into()?;
        let record = Record {
            topic,
            partition: -1,
            key: index.as_bytes(),
            value: data,
        };
        match producer.send(&record) {
            Ok(()) => (),
            Err(e) => error!("Failed to produce message: {:?}", e),
        };
        thread::sleep(sleep_duration);
        index += 1;
    }
}


fn consume(group: String, topic: String, brokers: Vec<String>) -> Result<(), KafkaError> {
    info!("Starting consumer from topic {:?}", topic);
    let mut con = Consumer::from_hosts(brokers)
        .with_topic(topic)
        .with_group(group)
        .with_fallback_offset(FetchOffset::Earliest)
        .with_offset_storage(GroupOffsetStorage::Kafka)
        .create()?;

    loop {
        let mss = con.poll()?;
        if mss.is_empty() {
            // info!("No messages available right now.");
            continue;
        }

        for ms in mss.iter() {
            for m in ms.messages() {
                let s = match str::from_utf8(m.value) {
                    Ok(v) => v,
                    Err(e) => {
                        error!("Invalid UTF-8 sequence: {}", e);
                        "__invalid__"
                    }
                };
                info!("Consumed {}:{}@{}: key={:?}, data={:?}", ms.topic(), ms.partition(), m.offset, m.key, s);
            }
            let _ = con.consume_messageset(ms);
        }
        con.commit_consumed()?;
    }
}
