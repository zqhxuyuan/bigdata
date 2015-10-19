# storm-realtime

Storm real-time computation for statistical indicators based business requirements.
The entire process starts from Kafka spout who emits event messages,
and messages are passed to downstream bolt component.
Final computation results are persisted to Redis.
