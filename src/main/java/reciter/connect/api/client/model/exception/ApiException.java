package reciter.connect.api.client.model.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.reactive.function.client.WebClientException;

import lombok.Data;

@Data
public class ApiException extends WebClientException {

    /**
     *
     */
    private static final long serialVersionUID = -284426264844067598L;
    private final HttpStatus httpStatus;

    public ApiException(HttpStatus status) {
        super(status.getReasonPhrase());
        this.httpStatus = status;
    }
    

}