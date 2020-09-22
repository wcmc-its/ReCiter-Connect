package reciter.connect.vivo.api.client.model.exception;

import org.springframework.http.HttpStatus;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class CustomWebClientResponseException extends Throwable {

    String errorBody;
    HttpStatus statusCode;

}
