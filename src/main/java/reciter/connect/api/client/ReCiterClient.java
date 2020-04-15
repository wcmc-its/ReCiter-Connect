package reciter.connect.api.client;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reciter.connect.api.client.model.ArticleRetrievalModel;
import reciter.connect.api.client.model.exception.ApiException;

@Slf4j
@Service
//@ControllerAdvice
public class ReCiterClient {
    
    @Autowired
    private WebClient webClient;

    public Mono<ArticleRetrievalModel> getPublicationsByUid(String uid) {
        log.info("Getting list of publications from reciter for uid: " + uid);
        return this.webClient
            .get()
            .uri(uriBuilder -> uriBuilder
                .path("/reciter/article-retrieval/by/uid")
                .queryParam("uid", uid)
            .build())
            .retrieve()
            .onStatus(HttpStatus::is4xxClientError, clientResponse -> {
                log.error("Error calling article retrieval api: " + clientResponse.statusCode().getReasonPhrase());
                return Mono.error(new ApiException(clientResponse.statusCode()));
            })
            .onStatus(HttpStatus::is5xxServerError, clientResponse -> {
                log.error("Error calling article retrieval api: " + clientResponse.statusCode().getReasonPhrase());
                return Mono.error(new ApiException(clientResponse.statusCode()));
             })
            .bodyToMono(ArticleRetrievalModel.class);
            //.bodyToMono(new ParameterizedTypeReference<List<ReCiterArticleFeature>>(){});
    }


    public Flux<ArticleRetrievalModel> getPublicationsByGroup(List<String> uids) {
        log.info("Getting list of publications from reciter for uids: " + uids);
        return Flux.fromIterable(uids)
            .parallel()
            .runOn(Schedulers.elastic())
            .flatMap(this::getPublicationsByUid)
            .doOnError(ex -> log.error("Encountered issue calling the api", ex.getMessage()))
            .ordered((uid1, uid2) -> uid1.hashCode() - uid2.hashCode());
    }

    @ExceptionHandler(WebClientResponseException.class)
    public ResponseEntity<String> handleWebClientResponseException(WebClientResponseException ex) {
        log.error("Error from WebClient - Status {}, Body {}", ex.getRawStatusCode(), ex.getResponseBodyAsString(), ex);
        return ResponseEntity.status(ex.getRawStatusCode()).body(ex.getResponseBodyAsString());
    }
}