package reciter.connect.vivo.sdb.publications.service;

import java.util.List;

import reciter.connect.api.client.model.ArticleRetrievalModel;
import reciter.engine.analysis.ReCiterArticleFeature;

public interface VivoPublicationsService {

    void isPublicationsExist(List<ArticleRetrievalModel> articles);
    void importPublications(List<ReCiterArticleFeature> articles, String uid, String dateUpdated);
    void syncPublications(List<ReCiterArticleFeature> articles, List<Long> vivoPubs);
    
}