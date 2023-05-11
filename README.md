# Distributed Key-Value Storage System implemented in Java. 

## Summary
- High-performance client-server communication via socket.
- Consistent Hashing and External configuration service improve scalability and load-balancing of the server cluster. 
- Replication (replicas) improve fault-tolerance and achieves eventual consistency. 
- Leader Election (Raft) makes more a distributed and reliable system. 

## Commands for Demo
### Build the project

```bash
ant
```

### Run unit tests

```bash
ant test
```

### Add new servers

```bash
java -jar m4-server.jar -p 9091 -d kv1
java -jar m4-server.jar -p 9092 -d kv2 -b 127.0.0.1:9091
java -jar m4-server.jar -p 9093 -d kv3 -b 127.0.0.1:9092
java -jar m4-server.jar -p 9094 -d kv4 -b 127.0.0.1:9091

java -jar m4-server.jar -p 9095 -d kv2 -b 127.0.0.1:9092
```

### Client Commands
```bash
java -jar m4-client.jar -b 8080
>>> connect 127.0.0.1 9092 
>>> put key1 val1 
>>> put key2 val2 
>>> get key1 
>>> disconnect
```

