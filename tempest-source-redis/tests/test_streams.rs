use tempest_source::prelude::{Source, SourceBuilder};
use tempest_source_redis::prelude::*;

use std::thread::sleep;
use std::time::Duration;

#[test]
fn test_claim_pending() {
    // Create source builder here
    let key = "test-claim-pending";
    let source_builder = RedisStreamSourceBuilder::default()
        .uri("redis://127.0.0.1/0")
        .key(key)
        .group("t1")
        .prime(move || -> Vec<RedisStreamPrime> {
            let mut msgs: Vec<RedisStreamPrime> = Vec::new();
            for i in 0..1000 {
                msgs.push(RedisStreamPrime::Msg(
                    "*".into(),
                    vec![("k".into(), "v".into()), ("i".into(), i.to_string())],
                ));
            }
            msgs
        })
        .pending_handler(RedisStreamPendingHandler::new(
            1000usize,
            1usize,
            RedisStreamPendingAction::Claim,
        ));

    let mut source = source_builder.build();

    let _ = &source.setup();

    // should've polled 10 messages
    let _results = &source.poll();
    // println!("{:?}", results);
    sleep(Duration::from_millis(1100));
    // run monitor to reclaim
    &source.monitor();
    let size = &source.reclaimed_size();
    // println!("reclaimed size: {:?}", &size);
    // should pull from the reclaimed queue
    let _ = &source.poll();
    // sleep here
    assert_eq!(&(size - 10), &source.reclaimed_size());
}
