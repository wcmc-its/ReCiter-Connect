package reciter.connect.vivo.api.client;

import javax.net.ssl.SSLException;

import org.springframework.http.MediaType;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.retry.annotation.Retryable;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.ExchangeFilterFunction;
import org.springframework.web.reactive.function.client.WebClient;

import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;
import reactor.netty.http.client.HttpClient;
import reciter.connect.vivo.api.client.model.exception.CustomWebClientResponseException;

@Slf4j
@Service
public class VivoClient {

    private WebClient webClient;

    private static final String vivoApiUsername = System.getenv("VIVO_API_USERNAME").trim();
    private static final String vivoApiPassword = System.getenv("VIVO_API_PASSWORD").trim();
    private static final String vivoBaseUrl = System.getenv("VIVO_BASE_URL");

    public VivoClient(WebClient.Builder webClientBuilder) {
        try {
            SslContext sslContext = SslContextBuilder.forClient().trustManager(InsecureTrustManagerFactory.INSTANCE)
                    .build();
            HttpClient httpClient = HttpClient.create().secure(t -> t.sslContext(sslContext));
            this.webClient = webClientBuilder
                    .clientConnector(new ReactorClientHttpConnector(httpClient))
                    //.filter(VivoClient.errorHandlingFilter())
                    .baseUrl(VivoClient.vivoBaseUrl)
                    .build();
        } catch (SSLException e) {
            log.error("SSLException", e);
        }
    }

    @Retryable(maxAttempts = 5, value = RuntimeException.class, 
        backoff = @Backoff(random = true, delay = 2000, maxDelay = 15000), listeners = {"retryListener"})
    public String vivoUpdateApi(String updateQuery) {
        LinkedMultiValueMap<String, String> body = new LinkedMultiValueMap<>();
        //body.add("email", VivoClient.vivoApiUsername);
        //body.add("password", VivoClient.vivoApiPassword);
        body.add("update", updateQuery);
        
         return this.webClient.post()
                    .uri(uriBuilder -> uriBuilder
                        .path("/vivo/update")
                    .build())
                    .contentType(MediaType.APPLICATION_FORM_URLENCODED)
                    .body(BodyInserters.fromFormData(body))
                    .exchangeToMono(response -> {
                        if (response.statusCode() != null && (response.statusCode().is5xxServerError() || response.statusCode().is4xxClientError())) {
                            return response.bodyToMono(String.class)
                                    .flatMap(errorBody -> {
                                        return Mono.error(new CustomWebClientResponseException(errorBody,response.statusCode()));
                                        });
                        }
                        else {
                            return response.toEntity(String.class);
                        }
                    })
                    //.flatMap(response -> response.toEntity(String.class))
                    .block()
                    .getBody();
    }

    public static ExchangeFilterFunction errorHandlingFilter() {
        return ExchangeFilterFunction.ofResponseProcessor(clientResponse -> {
            if(clientResponse.statusCode()!=null && (clientResponse.statusCode().is5xxServerError() || clientResponse.statusCode().is4xxClientError()) ) {
                 return clientResponse.bodyToMono(String.class)
                         .flatMap(errorBody -> {
                             return Mono.error(new CustomWebClientResponseException(errorBody,clientResponse.statusCode()));
                             });
            }else {
                return Mono.just(clientResponse);
            }
        });
    }

    @Retryable(maxAttempts = 5, value = RuntimeException.class, 
        backoff = @Backoff(random = true, delay = 2000, maxDelay = 15000), listeners = {"retryListener"})
    public String vivoQueryApi(String query) {

        LinkedMultiValueMap<String, String> body = new LinkedMultiValueMap<>();
        //body.add("email", VivoClient.vivoApiUsername);
        //body.add("password", VivoClient.vivoApiPassword);
        body.add("query", query);
        
         return this.webClient.post()
                    .uri(uriBuilder -> uriBuilder
                        .path("/vivo/query")
                    .build())
                    .contentType(MediaType.APPLICATION_FORM_URLENCODED)
                    .header("Accept", "application/sparql-results+json")
                    .body(BodyInserters.fromFormData(body))
                    .exchangeToMono(response -> {
                        if (response.statusCode() != null && (response.statusCode().is5xxServerError() || response.statusCode().is4xxClientError())) {
                            return response.bodyToMono(String.class)
                                    .flatMap(errorBody -> {
                                        return Mono.error(new CustomWebClientResponseException(errorBody,response.statusCode()));
                                        });
                        }
                        else {
                            return response.toEntity(String.class);
                        }
                    })
                    //.flatMap(response -> response.toEntity(String.class))
                    .block()
                    .getBody();
    }
    
}
