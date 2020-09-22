package reciter.connect.vivo.sdb.publications;

import java.util.Collections;
import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import reciter.connect.api.client.model.ArticleRetrievalModel;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class VivoPublicationsMain {
    
    private List<ArticleRetrievalModel> articles = Collections.emptyList();

    



}