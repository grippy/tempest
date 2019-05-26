pub enum RedisQueueType {
    LPOP,
    RPOP,
}

pub struct RedisQueueSource<'a> {
    key: &'a str,
    queue_type: RedisQueueType,
}