package reciter.connect.api.client.model;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonFormat.Feature;

import lombok.Data;
import reciter.engine.analysis.ReCiterArticleFeature;

@Data
public class ArticleRetrievalModel {
    @JsonFormat(with = Feature.ACCEPT_SINGLE_VALUE_AS_ARRAY)
    private List<ReCiterArticleFeature> reCiterArticleFeatures;
}