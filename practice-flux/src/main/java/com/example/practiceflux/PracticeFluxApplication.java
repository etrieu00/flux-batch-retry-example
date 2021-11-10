package com.example.practiceflux;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.event.EventListener;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Collectors;

@SpringBootApplication
public class PracticeFluxApplication {

  public static void main(String[] args) {
    SpringApplication.run(PracticeFluxApplication.class, args);
  }

  @Autowired
  private WebClient webClient;

  /**
   * Runs when application is ready.
   * It will make 50 requests to itself
   * Requests are made in batches of 3
   * When the 3 requests are done it will make another set of requests
   * Failed requests will make another request to a different endpoint which may time out and fail
   */
  @EventListener(ApplicationReadyEvent.class)
  public void applicationStartUp() {
    // Commented Version
    Flux.range(0, 50)
      // Batch size of 3 is put into a flux
      .window(3)
      // Flux are concat so they have to go in sequential order for each batch
      .concatMap(group ->
        // Make a request with each element
        group.flatMap(data ->
          // First endpoint where most elements pass and don't fail
          webClient.get()
            .uri("http://localhost:8080/test?name=" + data)
            .retrieve()
            // On error emit an exception
            .onStatus(HttpStatus::isError,
              res -> Mono.error(new IllegalStateException("There was an error making this request.")))
            .bodyToMono(String.class)
            // Retry once on failure after waiting 3 seconds
            .retryWhen(Retry.backoff(1, Duration.ofSeconds(3)))
            // Make request to a different endpoint instead since it failed twice
            .onErrorResume(retry -> webClient.get()
              .uri("http://localhost:8080/test/other?name=" + data)
              .retrieve()
              // On error emit an exception
              .onStatus(HttpStatus::isError,
                res -> Mono.error(new IllegalStateException("There was an error making this request")))
              .bodyToMono(String.class)
              // Time out after 5 seconds
              .timeout(Duration.ofSeconds(5))
              // Allow up to three retries
              .retryWhen(Retry.backoff(2, Duration.ofSeconds(3))))
            // If there is error provided a different message
            .onErrorResume(last -> Mono.just("Good bye " + data)))
          // Collect the results
          .collectList()
          // Print out how long it took for the batch to finish
          .doOnNext(result -> System.out.println(result + ", " + LocalDateTime.now())))
      // Remove the message from the responses
      .map(list -> list.stream()
        .map(message -> message.contains("Hello ")
          ? message.substring(6)
          : "-" + message.substring(9)).collect(Collectors.toList()))
      // Collect back to list of list
      .collectList()
      // Print out the results one batch per line
      .subscribe(results -> results.forEach(System.out::println));
  }

  /**
   * No detailed comments
   * Maybe I'll write some unit tests later
   */
  @Service
  private class ExampleService {

    /**
     * Runs when application is ready.
     * It will make 50 requests to itself
     * Requests are made in batches of 3
     * When the 3 requests are done it will make another set of requests
     * Failed requests will make another request to a different endpoint which may time out and fail
     */
    private Mono<List<List<String>>> makeBatchRequests() {
      return Flux.range(0, 50)
        .window(3)
        .concatMap(group ->
          group.flatMap(data ->
            webClient.get()
              .uri("http://localhost:8080/test?name=" + data)
              .retrieve()
              .onStatus(HttpStatus::isError,
                res -> Mono.error(new IllegalStateException("There was an error making this request.")))
              .bodyToMono(String.class)
              .retryWhen(Retry.backoff(1, Duration.ofSeconds(3)))
              .onErrorResume(retry -> webClient.get()
                .uri("http://localhost:8080/test/other?name=" + data)
                .retrieve()
                .onStatus(HttpStatus::isError,
                  res -> Mono.error(new IllegalStateException("There was an error making this request")))
                .bodyToMono(String.class)
                .timeout(Duration.ofSeconds(5))
                .retryWhen(Retry.backoff(2, Duration.ofSeconds(3))))
              .onErrorResume(last -> Mono.just("Good bye " + data)))
            .collectList()
            .doOnNext(result -> System.out.println(result + ", " + LocalDateTime.now())))
        .map(list -> list.stream()
          .map(message -> message.contains("Hello ")
            ? message.substring(6)
            : "-" + message.substring(9)).collect(Collectors.toList()))
        .collectList();
    }
  }

  /**
   * Create a Webclient bean
   *
   * @param webClientBuilder A builder for a reactive web client
   * @return web client bean
   */
  @Bean
  public WebClient myWebClient(WebClient.Builder webClientBuilder) {
    return webClientBuilder.build();
  }

  @RestController
  @RequestMapping("/test")
  private class ExampleController {

    /**
     * Test endpoint that fails when the number is 13, 17, 23, 24
     *
     * @param name element number
     * @return Hello #
     */
    @GetMapping
    @ResponseStatus(HttpStatus.OK)
    public Mono<String> exampleEndpoint(@RequestParam final Integer name) {
      final int randomDelay = ((int) (Math.random() * 7));
      if (List.of(13, 17, 23, 24).contains(name)) {
        System.out.println("[/test -> FAILED] E: " + name + " -> " + LocalDateTime.now());
        throw new RuntimeException("Unlucky number was rejected");
      }
      System.out.println("[/test -> delay: " + randomDelay + " ] E: " + name + " -> " + LocalDateTime.now());
      return Mono.just("Hello " + name).delayElement(Duration.ofSeconds(randomDelay));
    }

    /**
     * Test endpoint that has a long delay
     *
     * @param name element number
     * @return Hello #
     */
    @GetMapping("/other")
    @ResponseStatus(HttpStatus.OK)
    public Mono<String> exampleEndpointOther(@RequestParam final Integer name) {
      final int randomDelay = ((int) (Math.random() * 4) + 4);
      System.out.println("[/test/other -> delay: " + randomDelay + " ] E: " + name + " -> " + LocalDateTime.now());
      return Mono.just("Hello " + name).delayElement(Duration.ofSeconds(randomDelay));
    }

    /**
     * Exception handler for run time errors
     *
     * @param e exception thrown
     * @return an expected response
     */
    @ExceptionHandler(RuntimeException.class)
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public Mono<String> runtimeExceptionHandler(Exception e) {
      return Mono.just(e.getMessage());
    }
  }
}
