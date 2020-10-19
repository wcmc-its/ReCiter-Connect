package reciter.connect.vivo.sdb.publications.service;

import java.util.List;
import java.util.concurrent.Callable;

import org.vivoweb.harvester.util.repo.SDBJenaConnect;

import reciter.connect.api.client.model.ArticleRetrievalModel;
import reciter.engine.analysis.ReCiterArticleFeature;

public interface VivoPublicationsService {

    String publicationsExist(List<ArticleRetrievalModel> articles);
    void importPublications(List<ReCiterArticleFeature> articles, String uid, String dateUpdated, SDBJenaConnect vivoJena);
    void syncPublications(List<ReCiterArticleFeature> articles, List<Long> vivoPubs, SDBJenaConnect vivoJena);
    Callable<String> getCallable(List<ArticleRetrievalModel> articles);
    
}