package reciter.connect.vivo.sdb.query;

import java.io.IOException;

import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.springframework.beans.factory.annotation.Autowired;
import org.vivoweb.harvester.util.repo.SDBJenaConnect;

import reciter.connect.database.mysql.jena.JenaConnectionFactory;

@org.springframework.stereotype.Component
public class SDBQueryInterface {

    @Autowired
	private JenaConnectionFactory jcf;

    public void query() {
        String sparqlQuery = 
        "SELECT  ?s ?p ?o \n" +
        "WHERE \n" +
        "{ \n" +
        "?s ?p ?o . \n" +
        "} LIMIT 3";
        SDBJenaConnect vivoJena = jcf.getConnectionfromPool("vitro-kb-2");
        ResultSet rs;
        try {
            rs = vivoJena.executeSelectQuery(sparqlQuery);
            while(rs.hasNext())
            {
                QuerySolution qs =rs.nextSolution();
                if(qs.get("s") != null) {
                    System.out.println(qs.get("s").toString());
                }
                
            }
        } catch(IOException e) {
            System.out.println(e);
        }
        jcf.returnConnectionToPool(vivoJena, "vitro-kb-2");
    }

}