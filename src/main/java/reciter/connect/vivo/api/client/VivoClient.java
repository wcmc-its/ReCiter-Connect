package reciter.connect.vivo.api.client;

import java.nio.charset.Charset;
import java.time.Duration;
import java.util.List;

import javax.net.ssl.SSLException;

import org.springframework.http.MediaType;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.stereotype.Service;
import org.springframework.util.Base64Utils;
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

    private final String vivoApiUsername = System.getenv("VIVO_API_USERNAME");
    private final String vivoApiPassword = System.getenv("VIVO_API_PASSWORD");

    public VivoClient(WebClient.Builder webClientBuilder) {
        try {
            SslContext sslContext = SslContextBuilder.forClient().trustManager(InsecureTrustManagerFactory.INSTANCE)
                    .build();
            HttpClient httpClient = HttpClient.create().secure(t -> t.sslContext(sslContext));
            this.webClient = webClientBuilder
                    .clientConnector(new ReactorClientHttpConnector(httpClient))
                    .filter(VivoClient.errorHandlingFilter())
                    .baseUrl("https://vivo-dev.weill.cornell.edu")
                    .build();
        } catch (SSLException e) {
            log.error("SSLException", e);
        }
    }

    public String vivoUpdateApi(String updateQuery) {

        LinkedMultiValueMap<String, String> body = new LinkedMultiValueMap<>();
        body.add("email", vivoApiUsername);
        body.add("password", vivoApiPassword);
        body.add("update", updateQuery);
        
         return this.webClient.post()
                    .uri(uriBuilder -> uriBuilder
                        .path("/api/sparqlUpdate")
                    .build())
                    .contentType(MediaType.APPLICATION_FORM_URLENCODED)
                    .body(BodyInserters.fromFormData(body))
                    .exchange()
                    .flatMap(response -> response.toEntity(String.class))
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


    public String vivoQueryApi(String query) {

        LinkedMultiValueMap<String, String> body = new LinkedMultiValueMap<>();
        body.add("email", vivoApiUsername);
        body.add("password", vivoApiPassword);
        body.add("query", query);
        
         return this.webClient.post()
                    .uri(uriBuilder -> uriBuilder
                        .path("/api/sparqlQuery")
                    .build())
                    .contentType(MediaType.APPLICATION_FORM_URLENCODED)
                    .header("Accept", "application/sparql-results+json")
                    .body(BodyInserters.fromFormData(body))
                    .exchange()
                    .flatMap(response -> response.toEntity(String.class))
                    .block()
                    .getBody();
    }
    
}
