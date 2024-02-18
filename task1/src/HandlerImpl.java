import org.jetbrains.annotations.Nullable;

import java.time.Duration;
import java.util.concurrent.*;
import java.util.function.Supplier;

public class HandlerImpl implements Handler {
    private final Executor threadPool;
    private final long timeout;
    private final Client client;

    public HandlerImpl(Duration timeout, Client client, int threads) {
        this.timeout = timeout.toMillis();
        this.client = client;
        this.threadPool = Executors.newFixedThreadPool(threads);
    }

    @Override
    public ApplicationStatusResponse performOperation(String id) {
        // 100 ms to process
        return getFirstResponse(id, now() + timeout - 100).toApplicationStatusResponse();
    }

    private ResponseInfo getFirstResponse(String id, long endTs) {
        var f1 = requestAsync(() -> client.getApplicationStatus1(id), endTs);
        var f2 = requestAsync(() -> client.getApplicationStatus2(id), endTs);

        ResponseInfo result1 = ResponseInfo.EMPTY;
        ResponseInfo result2 = ResponseInfo.EMPTY;
        long startTs = now();
        do {
            if (f1.isDone()) {
                result1 = getResultFromFuture(f1, startTs);
                if (result1.isSuccess()) {
                    return result1;
                }
            }
            if (f2.isDone()) {
                result2 = getResultFromFuture(f2, startTs);
                if (result2.isSuccess()) {
                    return result2;
                }
            }
        } while (!f1.isDone() || !f2.isDone());
        return result1.merge(result2);
    }

    private ResponseInfo getResultFromFuture(CompletableFuture<ResponseInfo> f1, long startTs) {
        try {
            return f1.get();
        } catch (Exception e) {
            return new ResponseInfo(null, 1, startTs, now() - startTs);
        }
    }

    private CompletableFuture<ResponseInfo> requestAsync(Supplier<Response> executed, long endTs) {
        return CompletableFuture.supplyAsync(() -> tryGetResponseWithRetry(executed, endTs), threadPool)
                .orTimeout(timeout, TimeUnit.MILLISECONDS);
    }

    private ResponseInfo tryGetResponseWithRetry(Supplier<Response> executed, long endTs) {
        var retriesCounter = 0;
        var lastRequestTimeStart = 0L;
        var delay = 0L;
        while (true) {
            lastRequestTimeStart = now();
            var currentTimeout = endTs - lastRequestTimeStart;
            retriesCounter+=1;
            var exWithDelay = CompletableFuture.delayedExecutor(delay, TimeUnit.MILLISECONDS, threadPool);

            try {
                var response = CompletableFuture.supplyAsync(executed, exWithDelay)
                        .get(currentTimeout, TimeUnit.MILLISECONDS);
                var duration = now() - lastRequestTimeStart;

                if (response instanceof Response.Success success) {
                    return new ResponseInfo(success, retriesCounter, lastRequestTimeStart,duration);
                } else if (response instanceof Response.Failure) {
                    return new ResponseInfo(null, retriesCounter, lastRequestTimeStart, duration);
                } else if (response instanceof Response.RetryAfter retry) {
                    delay = retry.delay().toMillis();
                } else {
                    throw new RuntimeException("Unknown response " + response.getClass().getName());
                }
            } catch (Exception ex) {
                return new ResponseInfo(null, retriesCounter, lastRequestTimeStart, now() - lastRequestTimeStart);
            }
        }
    }

    private static long now() {
        return System.currentTimeMillis();
    }

    private record ResponseInfo(
            @Nullable Response.Success response,
            int retries,
            long lastRequestStart,
            long lastRequestDuration)
    {
        public static final ResponseInfo EMPTY = new ResponseInfo(null, 0, 0 ,0);
        public boolean isSuccess() {
            return response != null;
        }

        public ResponseInfo merge(ResponseInfo other) {
            var fResponse = response;
            if (fResponse == null) {
                fResponse = other.response;
            }
            var fLastDuration = lastRequestDuration;
            var fLastStart = lastRequestStart;
            if (lastRequestStart < other.lastRequestStart) {
                fLastDuration = other.lastRequestDuration;
                fLastStart = other.lastRequestStart;
            }
            return new ResponseInfo(
                    fResponse,
                    retries + other.retries,
                    fLastStart,
                    fLastDuration
            );
        }

        public ApplicationStatusResponse toApplicationStatusResponse() {
            if (isSuccess()) {
                return new ApplicationStatusResponse.Success(response.applicationId(), response.applicationStatus());
            } else {
                return new ApplicationStatusResponse.Failure(Duration.ofMillis(lastRequestDuration), retries);
            }
        }
    }
}
