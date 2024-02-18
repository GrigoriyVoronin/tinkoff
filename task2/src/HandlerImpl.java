import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class HandlerImpl implements Handler{
    private final Duration timeout;
    private final Client client;
    private final ExecutorService threadPool;

    public HandlerImpl(Duration timeout, Client client, int threads) {
        this.timeout = timeout;
        this.client = client;
        this.threadPool = Executors.newFixedThreadPool(threads);
    }

    @Override
    public Duration timeout() {
        return timeout;
    }

    @Override
    public void performOperation() {
        Event data = client.readData();
        CompletableFuture<?>[] futures = new CompletableFuture[data.recipients().size()];
        List<Address> recipients = data.recipients();
        for (int i = 0; i < recipients.size(); i++) {
            Address r = recipients.get(i);
            futures[i] = CompletableFuture.runAsync(() -> sendData(r, data.payload()), threadPool);
        }

        CompletableFuture.allOf(futures).join();
    }

    private void sendData(Address address, Payload payload) {
        Result result = client.sendData(address, payload);
        if (result == Result.ACCEPTED) {
            return;
        }
        if (result == Result.REJECTED) {
            Executor delayedEx = CompletableFuture.delayedExecutor(timeout.toMillis(), TimeUnit.MILLISECONDS);
            CompletableFuture.runAsync(() -> sendData(address, payload), delayedEx).join();
        }
    }
}
