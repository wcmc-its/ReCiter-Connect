package org.vivoweb.harvester.operations;

import java.io.IOException;
import java.util.List;

import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.vivoweb.harvester.util.repo.SDBJenaConnect;

import lombok.extern.slf4j.Slf4j;
import reciter.connect.database.mysql.jena.JenaConnectionFactory;
import reciter.connect.database.mysql.jena.LegacyJenaConnectionFactory;
import reciter.connect.vivo.api.client.VivoClient;
import reciter.connect.vivo.sdb.VivoGraphs;
import reciter.connect.vivo.sdb.query.QueryConstants;

@Slf4j
@Component
public class Kb2SyncUtils {
    
    @Autowired
    private JenaConnectionFactory jcf;

    @Autowired
    private LegacyJenaConnectionFactory ljcf;
    
    @Autowired
    private VivoClient vivoClient;

    private String ingestType = System.getenv("INGEST_TYPE");

    public void syncOverview(List<String> peopleCwids) {
        SDBJenaConnect legacyVivoJena = this.ljcf.getConnectionfromPool("dataSet");
        SDBJenaConnect vivoJena = this.jcf.getConnectionfromPool("dataSet");
        for(String cwid : peopleCwids) {
            StringBuilder sb = new StringBuilder();
            sb.append(QueryConstants.getSparqlPrefixQuery());
            sb.append("SELECT ?overview \n");
            sb.append("WHERE { \n");
            sb.append("GRAPH <" + VivoGraphs.DEFAULT_KB_2_GRAPH + "> {\n");
            sb.append("<http://vivo.med.cornell.edu/individual/cwid-" + cwid + "> core:overview ?overview . \n");
            sb.append("}}");

            try {
                ResultSet rs = legacyVivoJena.executeSelectQuery(sb.toString(), true);
                while(rs.hasNext()) {
                    QuerySolution qs = rs.nextSolution();
                    if(qs.get("overview") != null) {
                        log.info("Starting import of overview statement for " + cwid);
                        log.info("Overview Statement" + qs.get("overview").toString());
                        //Start import
                        sb.setLength(0);
                        sb.append(QueryConstants.getSparqlPrefixQuery());
                        sb.append("INSERT DATA { \n");
                        sb.append("GRAPH <" + VivoGraphs.DEFAULT_KB_2_GRAPH + "> { \n");
                        sb.append("<" + JenaConnectionFactory.nameSpace + "cwid-" + cwid + "> core:overview \"" + qs.get("overview").toString().trim().replaceAll("([\\\\\\\\\"])", "\\\\$1").replaceAll("'", "\'").replaceAll("\"","\\\"").replaceAll("(\\r|\\n|\\r\\n)+", "\\\\n") + "\" . \n");
                        sb.append("}}");

                        vivoJena.executeUpdateQuery(sb.toString(), true);
                    }
                }      
            } catch (IOException e) {
                log.error("Error connecting to SDBJena", e);
            } 
        }
    }
}
