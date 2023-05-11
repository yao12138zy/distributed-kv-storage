package app_kvServer.consensus.raft;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

// This class is written with the help of an LLM, ChatGPT-3.5 Alpha
public class RaftTimeout {
    private final ScheduledExecutorService scheduler;
    private ScheduledFuture<?> timeoutTask;
    public RaftTimeout() {
        scheduler = Executors.newScheduledThreadPool(1);
    }
    public void startTimeout(long timeoutDurationMs, Runnable timeoutCallback) {
        if (timeoutTask != null && !timeoutTask.isDone()) {
            timeoutTask.cancel(true);
        }
        timeoutTask = scheduler.schedule(timeoutCallback, timeoutDurationMs, java.util.concurrent.TimeUnit.MILLISECONDS);
    }
    public void cancelTimeout() {
        if (timeoutTask != null && !timeoutTask.isDone()) {
            timeoutTask.cancel(true);
        }
    }

    public void stop() {
        scheduler.shutdown();
    }
}
