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
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ?g ?p ?o \n");
		sb.append("WHERE { \n");
		sb.append("GRAPH ?g { \n");
		sb.append("?s ?p ?o . \n");
		sb.append("}} LIMIT 3");
        SDBJenaConnect vivoJena = jcf.getConnectionfromPool("dataSet");
        ResultSet rs;
        try {
            rs = vivoJena.executeSelectQuery(sb.toString(), true);
            while(rs.hasNext())
            {
                QuerySolution qs =rs.nextSolution();
                System.out.println(qs.getResource("g").getURI());
                if(qs.get("s") != null) {
                    System.out.println(qs.get("s").toString());
                }
                
            }
        } catch(IOException e) {
            System.out.println(e);
        }
        jcf.returnConnectionToPool(vivoJena, "dataSet");
    }

}