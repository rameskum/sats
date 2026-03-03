package com.sats.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Arrays;

/**
 * Resolves the list of Kafka topics to consume by calling a REST endpoint at runtime.
 * The result is fetched once when the {@link SatsKafkaListener} is registered by Spring Kafka.
 * <p>
 * The endpoint must return a JSON array of topic name strings, e.g.:
 * {@code ["topic-a", "topic-b", "topic-c"]}
 */
@Component
@Slf4j
public class TopicProvider {

    private final String topicRegistryUrl;
    private final ObjectMapper objectMapper;
    private final HttpClient httpClient;

    public TopicProvider(
            @Value("${sats.consumer.topic-registry-url}") String topicRegistryUrl,
            ObjectMapper objectMapper
    ) {
        this.topicRegistryUrl = topicRegistryUrl;
        this.objectMapper = objectMapper;
        this.httpClient = HttpClient.newHttpClient();
    }

    /**
     * Fetches the topic list from the configured REST endpoint.
     * Called via SpEL ({@code #{@topicProvider.getTopics()}}) during listener registration.
     *
     * @return array of Kafka topic names
     * @throws IllegalStateException if the endpoint is unreachable or returns invalid data
     */
    public String[] getTopics() {
        log.info("Fetching Kafka topics from registry: {}", topicRegistryUrl);
        try {
            var request = HttpRequest.newBuilder()
                    .uri(URI.create(topicRegistryUrl))
                    .GET()
                    .build();

            var response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() != 200) {
                throw new IllegalStateException(
                        "Topic registry returned HTTP " + response.statusCode());
            }

            String[] topics = objectMapper.readValue(response.body(), String[].class);
            log.info("Resolved {} topic(s) from registry: {}", topics.length, Arrays.toString(topics));
            return topics;

        } catch (IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Failed to fetch topics from registry: " + topicRegistryUrl, e);
        }
    }
}
