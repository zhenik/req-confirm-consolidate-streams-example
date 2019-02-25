# PoC consolidating streams: request and confirm
![diagram](imgs/poc_stream_subscriptions.png)

## Case specific
1. `Request` sent, receives `Response` with `confirmation` OK or FAIL status, within time window for processing(join)
2. `Request` sent, receives `Response` with `confirmation` OK or FAIL status, but after time-out(INTERNAL) for time window for processing(join) 
3. `Request` sent, `Response` with`confirmation` never arrived 

Making stateful operations with kafka streams requires always windowed. 
-> That is reason, why we can not track requests without responses.
We assume that `late response` is equal to `never-come response`, but these two cases have different side affect on Bypass.    

## Questions
1. How to handle all types of errors within async boundaries ?
2. Downstream state to database or REST(local state, cache, ram limit) is enough ?

### Requirements
1. Co-partitions topics
2. Always window (KStream -> KStream)

Other: 
1. Join is key-based
2. [Join types](https://docs.confluent.io/current/streams/developer-guide/dsl-api.html#kstream-kstream-join): inner, left, outer

A windowing state store is used to store all the records received so far within the defined window boundary. 
In aggregating operations, a windowing state store is used to store the latest aggregation results per window. 
Old records in the state store are purged after the specified window retention period. 
Kafka Streams guarantees to keep a window for at least this specified time; the default value is one day and can be changed via `Materialized#withRetention()`.


### Diagram stream processing
![diagram](./imgs/diagram-topology.png)

### Topology stream processing
Upload [here](https://zz85.github.io/kafka-streams-viz/)