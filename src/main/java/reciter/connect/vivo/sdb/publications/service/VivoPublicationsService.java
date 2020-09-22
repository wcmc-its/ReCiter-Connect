package reciter.connect.vivo.sdb.publications.service;

import java.util.List;

import reciter.connect.api.client.model.ArticleRetrievalModel;

public interface VivoPublicationsService {

    void importPublications(List<ArticleRetrievalModel> articles);
    void syncPublications(List<ArticleRetrievalModel> articles);
    
}