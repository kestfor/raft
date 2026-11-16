```sh
grpcurl -plaintext -d '{}' 127.0.0.1:19001 raft.v1.RaftService/GetStatus | jq
grpcurl -plaintext -d '{}' 127.0.0.1:19002 raft.v1.RaftService/GetStatus | jq
grpcurl -plaintext -d '{}' 127.0.0.1:19003 raft.v1.RaftService/GetStatus | jq
```

```sh
grpcurl -plaintext -d '{}' 127.0.0.1:9001 raft.v1.RaftService/GetStatus | jq
grpcurl -plaintext -d '{}' 127.0.0.1:9002 raft.v1.RaftService/GetStatus | jq
grpcurl -plaintext -d '{}' 127.0.0.1:9003 raft.v1.RaftService/GetStatus | jq
```
