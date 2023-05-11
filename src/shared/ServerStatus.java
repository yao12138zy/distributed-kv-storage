package shared;

public enum ServerStatus {
    Unknown,
    Ready,
    WriteLock,
    QueuedJoin,
    QueuedShutdown,
}
