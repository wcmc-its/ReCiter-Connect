package reciter.connect.api.client.model;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonFormat.Feature;

import lombok.Data;
import reciter.engine.analysis.ReCiterArticleFeature;

@Data
public class ArticleRetrievalModel {
    @JsonProperty(required = true, value = "personIdentifier")
    private String personIdentifier;
    @JsonProperty(required = true, value = "dateAdded")
    private String dateAdded;
    @JsonProperty(required = true, value = "dateUpdated")
    private String dateUpdated;
    @JsonFormat(with = Feature.ACCEPT_SINGLE_VALUE_AS_ARRAY)
    private List<ReCiterArticleFeature> reCiterArticleFeatures;
}