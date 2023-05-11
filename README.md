# Group 28 Milestone 4


## Commands used in demo

### Build the project

```bash
ant
```

### Run unit tests

```bash
ant test
```

### Start the servers

```bash
java -jar m4-server.jar -p 9091 -d kv1
java -jar m4-server.jar -p 9092 -d kv2 -b 127.0.0.1:9091
java -jar m4-server.jar -p 9093 -d kv3 -b 127.0.0.1:9092
java -jar m4-server.jar -p 9094 -d kv4 -b 127.0.0.1:9091



java -jar m4-server.jar -p 9095 -d kv2 -b 127.0.0.1:9092
```
