package client2;

import io.swagger.client.ApiClient;
import io.swagger.client.ApiException;
import io.swagger.client.ApiResponse;
import io.swagger.client.api.SkiersApi;
import io.swagger.client.model.LiftRide;
import model.LiftRideEvent;
import org.apache.commons.lang3.concurrent.EventCountCircuitBreaker;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/***
 *  Post Task of each thread, with log information stored in log queue
 */
class PostTaskWithLog implements Runnable {
    private final CountDownLatch latch;
    private final ApiClient apiClient;
    private final BlockingQueue<LiftRideEvent> eventQueue;
    private final AtomicInteger successfulRequests;
    private final int maxRetries;
    private final int requestsPerThread;
    private final BlockingQueue<String> logQueue;
    private final EventCountCircuitBreaker circuitBreaker;

    public PostTaskWithLog(BlockingQueue<LiftRideEvent> eventQueue, int requestsPerThread, ApiClient apiClient, CountDownLatch latch,
                           AtomicInteger successfulRequests, int maxRetries, BlockingQueue<String> logQueue, EventCountCircuitBreaker circuitBreaker) {
        this.latch = latch;
        this.apiClient = apiClient;
        this.requestsPerThread = requestsPerThread;
        this.eventQueue = eventQueue;
        this.successfulRequests = successfulRequests;
        this.maxRetries = maxRetries;
        this.logQueue = logQueue;
        this.circuitBreaker = circuitBreaker;
    }

    @Override
    public void run() {
        SkiersApi skiersApi = new SkiersApi(apiClient);
        int count = 0;
        while (count < requestsPerThread)  {
            long start = System.currentTimeMillis(); // before sending the request, take a timestamp
            int attempt = 0;
            boolean success = false;
            LiftRideEvent event;
            try {
                event = eventQueue.take(); // Blocks until an element is available
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            while (attempt < maxRetries && !success) {
                if (circuitBreaker.incrementAndCheckState()) {
                    try {
                        LiftRide liftRide = new LiftRide();
                        liftRide.setLiftID(event.getLiftID());
                        liftRide.setTime(event.getTime());
                        ApiResponse<Void> response = skiersApi.writeNewLiftRideWithHttpInfo(
                                liftRide,
                                event.getResortID(), event.getSeasonID(), event.getDayID(),
                                event.getSkierID());

                        count ++;
                        if (response.getStatusCode() == 201) {
                            long end = System.currentTimeMillis();// when the HTTP 200/201 response is received, take another timestamp
                            long latency = end - start;
                            // Prepare log entry
                            // System.out.println("Lift ride logged successfully " );
                            successfulRequests.incrementAndGet();
                            String logEntry = start + ",POST," + latency + "," + response.getStatusCode();
                            logQueue.put(logEntry);
                            success = true;
                        } else if (response.getStatusCode() >= 400 && response.getStatusCode() < 600) {
                            System.err.println("Attempt " + (attempt + 1) + " failed: HTTP " + response.getStatusCode());
                            attempt++;
                        }
                    } catch (ApiException | InterruptedException e) {
                        System.out.println("API exception during attempt ");
                        attempt++;
                        try {
                            Thread.sleep(Math.min((long) Math.pow(2, attempt) * 100, 5000)); // Exponential backoff
                        } catch (InterruptedException ie) {
                            System.out.println("Thread interrupted during backoff");
                            Thread.currentThread().interrupt();
                        }
                    }
                } else {
                    // Circuit breaker open, wait before retrying
                    System.out.println("Circuit breaker open, waiting before retrying...");
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ie) {
                        System.out.println("Thread interrupted while waiting for circuit breaker");
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }
        latch.countDown();
    }
}

