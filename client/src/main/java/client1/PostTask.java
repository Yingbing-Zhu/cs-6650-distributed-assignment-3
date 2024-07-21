package client1;

import io.swagger.client.ApiClient;
import io.swagger.client.ApiException;
import io.swagger.client.ApiResponse;
import io.swagger.client.api.SkiersApi;
import io.swagger.client.model.LiftRide;
import model.LiftRideEvent;

import org.apache.commons.lang3.concurrent.EventCountCircuitBreaker;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/***
 *  Post Task of each thread
 */
class PostTask implements Runnable {
    private final ApiClient apiClient;
    private final CountDownLatch latch;
    private final BlockingQueue<LiftRideEvent> eventQueue;
    private final AtomicInteger successfulRequests;
    private final int maxRetries;
    private final int requestsPerThread;
    private final EventCountCircuitBreaker circuitBreaker;

    public PostTask(BlockingQueue<LiftRideEvent> eventQueue, int requestsPerThread, ApiClient apiClient, CountDownLatch latch,
                    AtomicInteger successfulRequests, int maxRetries, EventCountCircuitBreaker circuitBreaker) {
        this.apiClient = apiClient;
        this.requestsPerThread = requestsPerThread;
        this.eventQueue = eventQueue;
        this.latch = latch;
        this.successfulRequests = successfulRequests;
        this.maxRetries = maxRetries;
        this.circuitBreaker = circuitBreaker;
    }

    @Override
    public void run() {
        SkiersApi skiersApi = new SkiersApi(apiClient);
        int count = 0;
        while (count < requestsPerThread)  {
            int attempt = 0;
            boolean success = false;
            LiftRideEvent event;
            try {
                event = eventQueue.take(); // Blocks until an element is available
//                System.out.println("Processing event: " + event);
            } catch (InterruptedException e) {
                System.out.println("Thread interrupted while taking from queue");
                Thread.currentThread().interrupt();
                break;
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
                            // System.out.println("Lift ride logged successfully" + event.getSkierID());
                            successfulRequests.incrementAndGet();
                            success = true;
                        } else if (response.getStatusCode() >= 400 && response.getStatusCode() < 600) {
                            System.err.println("Attempt " + (attempt + 1) + " failed: HTTP " + response.getStatusCode());
                            attempt++;
                        }
                    } catch ( ApiException e) {
                        System.out.println("API exception during attempt ");
                        attempt++;
                        try {
                            Thread.sleep((long) Math.pow(2, attempt) * 100); // Exponential backoff
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

