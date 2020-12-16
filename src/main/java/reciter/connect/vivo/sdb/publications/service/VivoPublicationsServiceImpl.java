package reciter.connect.vivo.sdb.publications.service;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.jena.query.QueryParseException;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.StopWatch;
import org.vivoweb.harvester.util.repo.SDBJenaConnect;

import lombok.extern.slf4j.Slf4j;
import reciter.connect.api.client.model.ArticleRetrievalModel;
import reciter.connect.database.mysql.jena.JenaConnectionFactory;
import reciter.connect.vivo.IngestType;
import reciter.connect.vivo.api.client.VivoClient;
import reciter.connect.vivo.sdb.VivoGraphs;
import reciter.connect.vivo.sdb.query.QueryConstants;
import reciter.engine.analysis.ReCiterArticleAuthorFeature;
import reciter.engine.analysis.ReCiterArticleFeature;
import reciter.engine.analysis.ReCiterArticleFeature.ArticleKeyword;
import reciter.engine.analysis.ReCiterArticleFeature.ArticleKeyword.KeywordType;
import reciter.model.pubmed.MedlineCitationJournalISSN;

@Slf4j
@Service
public class VivoPublicationsServiceImpl implements VivoPublicationsService {

    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    private Calendar cal = Calendar.getInstance();

    @Autowired
    private JenaConnectionFactory jcf;
    
    @Autowired
    private VivoClient vivoClient;

    private String ingestType = System.getenv("INGEST_TYPE");

    @Override
    public void importPublications(List<ReCiterArticleFeature> articles, String uid, String dateUpdated, SDBJenaConnect vivoJena) {
        StopWatch stopWatch = new StopWatch("Publications import to VIVO");
        stopWatch.start("Publications import to VIVO");
        StringBuilder sb = new StringBuilder();
        sb.append(QueryConstants.getSparqlPrefixQuery());
        sb.append("INSERT DATA { GRAPH <" + VivoGraphs.PUBLICATIONS_GRAPH + ">{ \n");
        for (ReCiterArticleFeature articleFeature : articles) {
            final String publicationUrl = "<" + JenaConnectionFactory.nameSpace + "pubid" + articleFeature.getPmid()
                    + ">";
            sb.append(publicationUrl + " core:DateTimeValue \"" + this.sdf.format(new Date()) + "\" . \n");
            sb.append(publicationUrl + " rdf:type obo:BFO_0000001 . \n");
            sb.append(publicationUrl + " rdf:type obo:BFO_0000002 . \n");
            sb.append(publicationUrl + " rdf:type obo:IAO_0000030 . \n");
            sb.append(publicationUrl + " rdf:type obo:BFO_0000031 . \n");
            // Absract mapped to bibo:abstract
            if (articleFeature.getPublicationAbstract() != null
                    && !articleFeature.getPublicationAbstract().isEmpty()) {
                sb.append(publicationUrl + " bibo:abstract \""
                        + articleFeature.getPublicationAbstract().trim().replaceAll("([\\\\\\\\\"])", "\\\\$1").replaceAll("'", "\'").replaceAll("\"","\\\"").replaceAll("(\\r|\\n|\\r\\n)+", "\\\\n")
                        + "\" .\n");
            }
            // articleTitle → rdfs:label
            if (articleFeature.getArticleTitle() != null) {
                sb.append(publicationUrl + " rdfs:label \""
                        + articleFeature.getArticleTitle().trim().replaceAll("([\\\\\\\\\"])", "\\\\$1").replaceAll("'", "\'").replaceAll("\"","\\\"").replaceAll("(\\r|\\n|\\r\\n)+", "\\\\n")
                        + "\" .\n");
            }
            // doi → bibo:doi
            if (articleFeature.getDoi() != null && !articleFeature.getDoi().isEmpty()) {
                sb.append(publicationUrl + " bibo:doi \"" + articleFeature.getDoi().trim()
                        .replaceAll("([\\\\\\\\\"])", "\\\\$1").replaceAll("\\s+", "") + "\" .\n");
            }
            // pmcid → core:pmcid
            if (articleFeature.getPmcid() != null) {
                sb.append(publicationUrl + " core:pmcid \"" + articleFeature.getPmcid() + "\" .\n");
            }
            // pmid → bibo:pmid
            sb.append(publicationUrl + " bibo:pmid \"" + articleFeature.getPmid() + "\" .\n");

            // issue → bibo:issue
            if (articleFeature.getIssue() != null) {
                sb.append(publicationUrl + " bibo:issue \"" + articleFeature.getIssue() + "\" .\n");
            }
            // scopusDocID → wcmc:scopusDocId
            if (articleFeature.getScopusDocID() != null) {
                sb.append(publicationUrl + " wcmc:scopusDocId \"" + articleFeature.getScopusDocID() + "\" .\n");
            }
            // volume → bibo:volume
            if (articleFeature.getVolume() != null) {
                sb.append(publicationUrl + " bibo:volume \"" + articleFeature.getVolume() + "\" .\n");
            }
            // pages -> bibo:pages
            if (articleFeature.getPages() != null) {
                sb.append(publicationUrl + " bibo:pages \"" + articleFeature.getPages() + "\" .\n");
            }
            // timesCited
            if (articleFeature.getTimesCited() != null && articleFeature.getTimesCited() > 0 && !dateUpdated.isEmpty()) {
                Date citationDate = null;
                try {
                    citationDate = this.sdf.parse(dateUpdated);
                    
                } catch (ParseException e) {
                    log.error("ParseException", e);
                }
                String citeDate = this.sdf.format(citationDate);
                sb.append(publicationUrl + " <http://purl.org/spar/c4o/hasGlobalCitationFrequency> <"
                        + JenaConnectionFactory.nameSpace + "citation-pubid" + articleFeature.getPmid() + "> .\n");
                sb.append("<" + JenaConnectionFactory.nameSpace + "citation-pubid" + articleFeature.getPmid()
                        + "> rdf:type <http://purl.org/spar/c4o/GlobalCitationCount> . \n");
                sb.append("<" + JenaConnectionFactory.nameSpace + "citation-pubid" + articleFeature.getPmid()
                + "> rdf:type <http://www.w3.org/2002/07/owl#Thing> . \n");
                sb.append("<" + JenaConnectionFactory.nameSpace + "citation-pubid" + articleFeature.getPmid()
                + "> vitro:mostSpecificType <http://purl.org/spar/c4o/GlobalCitationCount> . \n");
                sb.append("<" + JenaConnectionFactory.nameSpace + "citation-pubid" + articleFeature.getPmid()
                        + "> rdfs:label \"" + articleFeature.getTimesCited() + "\" . \n");
                // Citation date
                /*if (citationDate != null) {
                    this.cal.setTime(citationDate);
                    int month = this.cal.get(Calendar.MONTH) + 1; // Since calender month start with 0
                    sb.append("<" + JenaConnectionFactory.nameSpace + "citation-pubid" + articleFeature.getPmid()
                            + "> core:dateTimeValue <" + JenaConnectionFactory.nameSpace + "daymonthyear"
                            + this.cal.get(Calendar.DAY_OF_MONTH) + (month < 10 ? ("0" + month) : (month))
                            + this.cal.get(Calendar.YEAR) + "> . \n");
                    sb.append("<" + JenaConnectionFactory.nameSpace + "daymonthyear"
                            + this.cal.get(Calendar.DAY_OF_MONTH) + (month < 10 ? ("0" + month) : (month))
                            + this.cal.get(Calendar.YEAR) + "> rdf:type core:DateTimeValue . \n");
                    sb.append("<" + JenaConnectionFactory.nameSpace + "daymonthyear"
                            + this.cal.get(Calendar.DAY_OF_MONTH) + (month < 10 ? ("0" + month) : (month))
                            + this.cal.get(Calendar.YEAR) + "> vitro:mostSpecificType core:DateTimeValue . \n");
                    sb.append("<" + JenaConnectionFactory.nameSpace + "daymonthyear"
                            + this.cal.get(Calendar.DAY_OF_MONTH) + (month < 10 ? ("0" + month) : (month))
                            + this.cal.get(Calendar.YEAR)
                            + "> core:dateTimePrecision core:yearMonthDayPrecision . \n");
                    sb.append("<" + JenaConnectionFactory.nameSpace + "daymonthyear"
                            + this.cal.get(Calendar.DAY_OF_MONTH) + (month < 10 ? ("0" + month) : (month))
                            + this.cal.get(Calendar.YEAR) + "> core:dateTime \"" + citeDate
                            + "T00:00:00\"^^<http://www.w3.org/2001/XMLSchema#dateTime> . \n");
                }*/
            }
            // MeshMajor 
            if(articleFeature.getArticleKeywords() != null && !articleFeature.getArticleKeywords().isEmpty()) {
                for(ArticleKeyword keyword: articleFeature.getArticleKeywords()) {
                    if(keyword.getType() == KeywordType.MESH_MAJOR) {
                        sb.append(publicationUrl + " core:freetextKeyword \"" + keyword.getKeyword().replaceAll("'", "\'") + "\" .\n");
                    }
                }
            }

            // Cover Date
            if (articleFeature.getPublicationDateStandardized() != null) {
                Date standardDate = null;
                try {
                    standardDate = this.sdf.parse(articleFeature.getPublicationDateStandardized());
                } catch (ParseException e) {
                    log.error("ParseException", e);
                }
                if (standardDate != null) {
                    this.cal.setTime(standardDate);
                    int month = this.cal.get(Calendar.MONTH) + 1; // Since calender month start with 0
                    sb.append(publicationUrl + " core:dateTimeValue <" + JenaConnectionFactory.nameSpace
                            + "daymonthyear" + this.cal.get(Calendar.DAY_OF_MONTH)
                            + (month < 10 ? ("0" + month) : (month)) + this.cal.get(Calendar.YEAR) + ">  . \n");
                    sb.append("<" + JenaConnectionFactory.nameSpace + "daymonthyear"
                            + this.cal.get(Calendar.DAY_OF_MONTH) + (month < 10 ? ("0" + month) : (month))
                            + this.cal.get(Calendar.YEAR) + "> rdf:type core:DateTimeValue . \n");
                    sb.append("<" + JenaConnectionFactory.nameSpace + "daymonthyear"
                            + this.cal.get(Calendar.DAY_OF_MONTH) + (month < 10 ? ("0" + month) : (month))
                            + this.cal.get(Calendar.YEAR) + "> vitro:mostSpecificType core:DateTimeValue . \n");
                    sb.append("<" + JenaConnectionFactory.nameSpace + "daymonthyear"
                            + this.cal.get(Calendar.DAY_OF_MONTH) + (month < 10 ? ("0" + month) : (month))
                            + this.cal.get(Calendar.YEAR)
                            + "> core:dateTimePrecision core:yearMonthDayPrecision . \n");
                    sb.append("<" + JenaConnectionFactory.nameSpace + "daymonthyear"
                            + this.cal.get(Calendar.DAY_OF_MONTH) + (month < 10 ? ("0" + month) : (month))
                            + this.cal.get(Calendar.YEAR) + "> core:dateTime \""
                            + articleFeature.getPublicationDateStandardized()
                            + "T00:00:00\"^^<http://www.w3.org/2001/XMLSchema#dateTime> . \n");
                }
            }
            // Journal
            String journalIdentifier = journalIdentifier(articleFeature);
            sb.append(publicationUrl + " core:hasPublicationVenue  <" + JenaConnectionFactory.nameSpace + "journal"
                    + journalIdentifier + ">  . \n");
            sb.append("<" + JenaConnectionFactory.nameSpace + "journal"
                    + journalIdentifier + "> rdf:type obo:BFO_0000001 . \n");
            sb.append("<" + JenaConnectionFactory.nameSpace + "journal"
                    + journalIdentifier + "> rdf:type obo:BFO_0000002 . \n");
            sb.append("<" + JenaConnectionFactory.nameSpace + "journal"
                    + journalIdentifier + "> rdf:type obo:BFO_0000031 . \n");
            sb.append("<" + JenaConnectionFactory.nameSpace + "journal"
                    + journalIdentifier + "> rdf:type obo:IAO_0000030 . \n");
            sb.append("<" + JenaConnectionFactory.nameSpace + "journal"
                    + journalIdentifier + "> rdf:type bibo:Collection . \n");
            sb.append("<" + JenaConnectionFactory.nameSpace + "journal"
                    + journalIdentifier + "> rdf:type <http://www.w3.org/2002/07/owl#Thing> . \n");
            sb.append("<" + JenaConnectionFactory.nameSpace + "journal"
                    + journalIdentifier + "> rdf:type bibo:Periodical . \n");
            sb.append("<" + JenaConnectionFactory.nameSpace + "journal"
                    + journalIdentifier + "> rdf:type bibo:Journal . \n");
            sb.append("<" + JenaConnectionFactory.nameSpace + "journal"
                    + journalIdentifier + "> vitro:mostSpecificType bibo:Journal . \n");
            sb.append("<" + JenaConnectionFactory.nameSpace + "journal"
                    + journalIdentifier + "> core:title \"" + articleFeature.getJournalTitleVerbose().replaceAll("'", "\'").replaceAll("\"","\\\\\"") + "\" . \n");
            sb.append("<" + JenaConnectionFactory.nameSpace + "journal"
                    + journalIdentifier + "> rdfs:label \"" + articleFeature.getJournalTitleVerbose().replaceAll("'", "\'").replaceAll("\"","\\\\\"") + "\" . \n");
            if (articleFeature.getJournalTitleISOabbreviation() != null)
                sb.append("<" + JenaConnectionFactory.nameSpace + "journal"
                    + journalIdentifier + "> wcmc:ISOAbbreviation \"" + articleFeature.getJournalTitleISOabbreviation().trim()
                        + "\" . \n");
            
            //Issn
            if(articleFeature.getIssn() != null && !articleFeature.getIssn().isEmpty()) {
                for(MedlineCitationJournalISSN issn: articleFeature.getIssn()) {
                    if(issn.getIssntype().equalsIgnoreCase("Electronic")) {
                        sb.append("<" + JenaConnectionFactory.nameSpace + "journal"
                        + journalIdentifier + "> bibo:eissn \"" + issn.getIssn()
                        + "\" . \n");
                    }
                    if(issn.getIssntype().equalsIgnoreCase("Linking")) {
                        sb.append("<" + JenaConnectionFactory.nameSpace + "journal"
                        + journalIdentifier + "> wcmc:lissn \"" + issn.getIssn()
                        + "\" . \n");
                    }
                    if(issn.getIssntype().equalsIgnoreCase("Print")) {
                        sb.append("<" + JenaConnectionFactory.nameSpace + "journal"
                        + journalIdentifier + "> bibo:issn \"" + issn.getIssn()
                        + "\" . \n");
                    }
                }
            }
            sb.append("<" + JenaConnectionFactory.nameSpace + "journal"
                    + journalIdentifier + "> <http://vivoweb.org/ontology/core#publicationVenueFor> " + publicationUrl + " . \n");

            //Publication Type
            if(articleFeature.getPublicationType() != null && articleFeature.getPublicationType().getPublicationTypeCanonical() != null) {
                sb.append(publicationUrl + " rdf:type core:InformationResource . \n");
                sb.append(publicationUrl + " rdf:type bibo:Document . \n");
                sb.append(publicationUrl + " rdf:type bibo:Article . \n");
                if(articleFeature.getPublicationType().getPublicationTypeCanonical().equalsIgnoreCase("Editorial Article")) {
                    sb.append(publicationUrl + " rdf:type core:EditorialArticle . \n");
                    sb.append(publicationUrl + " vitro:mostSpecificType core:EditorialArticle . \n");
                    sb.append(publicationUrl + " vitro:mostSpecificType core:EditorialArticle . \n");
                    sb.append(publicationUrl + " rdf:type <http://www.w3.org/2002/07/owl#Thing> . \n");
                }
                else if(articleFeature.getPublicationType().getPublicationTypeCanonical().equalsIgnoreCase("Letter")) {
                    sb.append(publicationUrl + " rdf:type fabio:Letter . \n");
                    sb.append(publicationUrl + " vitro:mostSpecificType fabio:Letter . \n");
                    sb.append(publicationUrl + " vitro:mostSpecificType fabio:Letter . \n");
                    sb.append(publicationUrl + " rdf:type <http://www.w3.org/2002/07/owl#Thing> . \n");
                }
                else if(articleFeature.getPublicationType().getPublicationTypeCanonical().equalsIgnoreCase("Conference Paper")) {
                    sb.append(publicationUrl + " rdf:type core:ConferencePaper . \n");
                    sb.append(publicationUrl + " vitro:mostSpecificType core:ConferencePaper . \n");
                    sb.append(publicationUrl + " vitro:mostSpecificType core:ConferencePaper . \n");
                    sb.append(publicationUrl + " rdf:type <http://www.w3.org/2002/07/owl#Thing> . \n");
                }
                else if(articleFeature.getPublicationType().getPublicationTypeCanonical().equalsIgnoreCase("Review")) {
                    sb.append(publicationUrl + " rdf:type core:Review . \n");
                    sb.append(publicationUrl + " vitro:mostSpecificType core:Review . \n");
                    sb.append(publicationUrl + " vitro:mostSpecificType core:Review . \n");
                    sb.append(publicationUrl + " rdf:type <http://www.w3.org/2002/07/owl#Thing> . \n");
                }
                else if(articleFeature.getPublicationType().getPublicationTypeCanonical().equalsIgnoreCase("Academic Article")) {
                    sb.append(publicationUrl + " rdf:type bibo:AcademicArticle . \n");
                    sb.append(publicationUrl + " vitro:mostSpecificType bibo:AcademicArticle . \n");
                    sb.append(publicationUrl + " vitro:mostSpecificType bibo:AcademicArticle . \n");
                    sb.append(publicationUrl + " rdf:type <http://www.w3.org/2002/07/owl#Thing> . \n");
                }
                else if(articleFeature.getPublicationType().getPublicationTypeCanonical().equalsIgnoreCase("Article")) {
                    sb.append(publicationUrl + " rdf:type bibo:Article . \n");
                    sb.append(publicationUrl + " vitro:mostSpecificType bibo:Article . \n");
                    sb.append(publicationUrl + " vitro:mostSpecificType bibo:Article . \n");
                    sb.append(publicationUrl + " rdf:type <http://www.w3.org/2002/07/owl#Thing> . \n");
                }
                else if(articleFeature.getPublicationType().getPublicationTypeCanonical().equalsIgnoreCase("Comment")) {
                    sb.append(publicationUrl + " rdf:type fabio:Comment . \n");
                    sb.append(publicationUrl + " vitro:mostSpecificType fabio:Comment . \n");
                    sb.append(publicationUrl + " vitro:mostSpecificType fabio:Comment . \n");
                    sb.append(publicationUrl + " rdf:type <http://www.w3.org/2002/07/owl#Thing> . \n");
                }
                else if(articleFeature.getPublicationType().getPublicationTypeCanonical().equalsIgnoreCase("In Process")) {
                    sb.append(publicationUrl + " rdf:type wcmc:InProcess . \n");
                    sb.append(publicationUrl + " vitro:mostSpecificType wcmc:InProcess . \n");
                    sb.append(publicationUrl + " vitro:mostSpecificType wcmc:InProcess . \n");
                    sb.append(publicationUrl + " rdf:type <http://www.w3.org/2002/07/owl#Thing> . \n");
                }
                else if(articleFeature.getPublicationType().getPublicationTypeCanonical().equalsIgnoreCase("PubMed.ConferencePaper")) {
                    sb.append(publicationUrl + " rdf:type core:ConferencePaper . \n");
                    sb.append(publicationUrl + " vitro:mostSpecificType core:ConferencePaper . \n");
                    sb.append(publicationUrl + " vitro:mostSpecificType core:ConferencePaper . \n");
                    sb.append(publicationUrl + " rdf:type <http://www.w3.org/2002/07/owl#Thing> . \n");
                }
                else if(articleFeature.getPublicationType().getPublicationTypeCanonical().equalsIgnoreCase("Report")) {
                    sb.append(publicationUrl + " rdf:type bibo:Report . \n");
                    sb.append(publicationUrl + " vitro:mostSpecificType bibo:Report . \n");
                    sb.append(publicationUrl + " vitro:mostSpecificType bibo:Report . \n");
                    sb.append(publicationUrl + " rdf:type <http://www.w3.org/2002/07/owl#Thing> . \n");
                }
            }
            //Author Assignment
            int targetAuthorCount = 0;
            if(articleFeature.getReCiterArticleAuthorFeatures() != null && !articleFeature.getReCiterArticleAuthorFeatures().isEmpty()) {
                for(ReCiterArticleAuthorFeature reCiterArticleAuthorFeature: articleFeature.getReCiterArticleAuthorFeatures()) {
                    if(reCiterArticleAuthorFeature.isTargetAuthor())
                        targetAuthorCount++;
                    
                    if(reCiterArticleAuthorFeature.isTargetAuthor() && targetAuthorCount == 1) {
                        sb.append(publicationUrl + " core:relatedBy <" + JenaConnectionFactory.nameSpace + "pubid" + articleFeature.getPmid() + "authorship" + reCiterArticleAuthorFeature.getRank() + "> . \n");
                        sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + articleFeature.getPmid() + "authorship" + reCiterArticleAuthorFeature.getRank() + "> rdf:type obo:BFO_0000001 . \n");
                        sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + articleFeature.getPmid() + "authorship" + reCiterArticleAuthorFeature.getRank() + "> rdf:type obo:BFO_0000002 . \n");
                        sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + articleFeature.getPmid() + "authorship" + reCiterArticleAuthorFeature.getRank() + "> rdf:type obo:BFO_0000020 . \n");
                        sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + articleFeature.getPmid() + "authorship" + reCiterArticleAuthorFeature.getRank() + "> rdf:type <http://www.w3.org/2002/07/owl#Thing> . \n");
                        sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + articleFeature.getPmid() + "authorship" + reCiterArticleAuthorFeature.getRank() + "> rdf:type core:Relationship . \n");
                        sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + articleFeature.getPmid() + "authorship" + reCiterArticleAuthorFeature.getRank() + "> rdf:type core:Authorship . \n");
                        sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + articleFeature.getPmid() + "authorship" + reCiterArticleAuthorFeature.getRank() + "> vitro:mostSpecificType core:Authorship . \n");
                        sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + articleFeature.getPmid() + "authorship" + reCiterArticleAuthorFeature.getRank() + "> core:relates " + publicationUrl + " . \n");
                        sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + articleFeature.getPmid() + "authorship" + reCiterArticleAuthorFeature.getRank() + "> core:rank \"" + reCiterArticleAuthorFeature.getRank() + "\"^^xsd:integer . \n");
                        //Linking vcard of the person
                        sb.append("<" + JenaConnectionFactory.nameSpace + "cwid-" + uid + "> rdf:type foaf:Person . \n");
                        sb.append("<" + JenaConnectionFactory.nameSpace + "cwid-" + uid + "> core:relatedBy <" + JenaConnectionFactory.nameSpace + "pubid" + articleFeature.getPmid() + "authorship" + reCiterArticleAuthorFeature.getRank() + "> . \n");
                        sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + articleFeature.getPmid() + "authorship" + reCiterArticleAuthorFeature.getRank() + "> core:relates <" + JenaConnectionFactory.nameSpace + "cwid-" + uid + "> . \n");
                        sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + articleFeature.getPmid() + "authorship" + reCiterArticleAuthorFeature.getRank() + "> core:relates <" + JenaConnectionFactory.nameSpace + "arg2000028-" + uid + "> . \n");
                        sb.append("<" + JenaConnectionFactory.nameSpace + "arg2000028-" + uid + "> obo:ARG_2000029 <" + JenaConnectionFactory.nameSpace + "pubid" + articleFeature.getPmid() + "authorship" + reCiterArticleAuthorFeature.getRank() + "> . \n");
                        sb.append("<" + JenaConnectionFactory.nameSpace + "arg2000028-" + uid + "> core:relatedBy <" + JenaConnectionFactory.nameSpace + "pubid" + articleFeature.getPmid() + "authorship" + reCiterArticleAuthorFeature.getRank() + "> . \n");
                        sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + articleFeature.getPmid() + "authorship" + reCiterArticleAuthorFeature.getRank() + "> obo:ARG_2000028 <" + JenaConnectionFactory.nameSpace + "arg2000028-" + uid + "> . \n");
                    } else {
                        String personIdentifier = getExternalPersonIdentifier(reCiterArticleAuthorFeature);
                        sb.append(publicationUrl + " core:relatedBy <" + JenaConnectionFactory.nameSpace + "pubid" + articleFeature.getPmid() + "authorship" + reCiterArticleAuthorFeature.getRank() + "> . \n");
                        sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + articleFeature.getPmid() + "authorship" + reCiterArticleAuthorFeature.getRank() + "> rdf:type obo:BFO_0000001 . \n");
                        sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + articleFeature.getPmid() + "authorship" + reCiterArticleAuthorFeature.getRank() + "> rdf:type obo:BFO_0000002 . \n");
                        sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + articleFeature.getPmid() + "authorship" + reCiterArticleAuthorFeature.getRank() + "> rdf:type obo:BFO_0000020 . \n");
                        sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + articleFeature.getPmid() + "authorship" + reCiterArticleAuthorFeature.getRank() + "> rdf:type <http://www.w3.org/2002/07/owl#Thing> . \n");
                        sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + articleFeature.getPmid() + "authorship" + reCiterArticleAuthorFeature.getRank() + "> rdf:type core:Relationship . \n");
                        sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + articleFeature.getPmid() + "authorship" + reCiterArticleAuthorFeature.getRank() + "> rdf:type core:Authorship . \n");
                        sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + articleFeature.getPmid() + "authorship" + reCiterArticleAuthorFeature.getRank() + "> vitro:mostSpecificType core:Authorship . \n");
                        sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + articleFeature.getPmid() + "authorship" + reCiterArticleAuthorFeature.getRank() + "> core:relates " + publicationUrl + " . \n");
                        sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + articleFeature.getPmid() + "authorship" + reCiterArticleAuthorFeature.getRank() + "> core:relates <" + JenaConnectionFactory.nameSpace + "person" + personIdentifier +"> .\n");
                        sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + articleFeature.getPmid() + "authorship" + reCiterArticleAuthorFeature.getRank() + "> core:rank \"" + reCiterArticleAuthorFeature.getRank() + "\"^^xsd:integer . \n");
                        sb.append("<" + JenaConnectionFactory.nameSpace + "person" + personIdentifier +"> rdf:type <http://purl.obolibrary.org/obo/BFO_0000001> . \n");
                        sb.append("<" + JenaConnectionFactory.nameSpace + "person" + personIdentifier +"> rdf:type <http://purl.obolibrary.org/obo/BFO_0000002> . \n");
                        sb.append("<" + JenaConnectionFactory.nameSpace + "person" + personIdentifier +"> rdf:type <http://purl.obolibrary.org/obo/IAO_0000030> . \n");
                        sb.append("<" + JenaConnectionFactory.nameSpace + "person" + personIdentifier +"> rdf:type <http://purl.obolibrary.org/obo/BFO_0000031> . \n");
                        sb.append("<" + JenaConnectionFactory.nameSpace + "person" + personIdentifier +"> rdf:type <http://purl.obolibrary.org/obo/ARG_2000379> . \n");
                        sb.append("<" + JenaConnectionFactory.nameSpace + "person" + personIdentifier +"> rdf:type <http://www.w3.org/2006/vcard/ns#Kind> . \n");
                        sb.append("<" + JenaConnectionFactory.nameSpace + "person" + personIdentifier +"> rdf:type <http://www.w3.org/2002/07/owl#Thing> . \n");
                        sb.append("<" + JenaConnectionFactory.nameSpace + "person" + personIdentifier +"> rdf:type <http://www.w3.org/2006/vcard/ns#Individual> . \n");
                        sb.append("<" + JenaConnectionFactory.nameSpace + "person" + personIdentifier +"> vitro:mostSpecificType <http://www.w3.org/2006/vcard/ns#Individual> . \n");
                        sb.append("<" + JenaConnectionFactory.nameSpace + "person" + personIdentifier +"> core:relatedBy <" + JenaConnectionFactory.nameSpace + "pubid" + articleFeature.getPmid() + "authorship" + reCiterArticleAuthorFeature.getRank() + "> . \n");
                        sb.append("<" + JenaConnectionFactory.nameSpace + "person" + personIdentifier +"> <http://www.w3.org/2006/vcard/ns#hasName> <" + JenaConnectionFactory.nameSpace + "hasName-person" + personIdentifier + "> . \n");
                        sb.append("<" + JenaConnectionFactory.nameSpace + "hasName-person" + personIdentifier + "> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2006/vcard/ns#Explanatory> . \n");
                        sb.append("<" + JenaConnectionFactory.nameSpace + "hasName-person" + personIdentifier + "> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2006/vcard/ns#Addressing> . \n");
                        sb.append("<" + JenaConnectionFactory.nameSpace + "hasName-person" + personIdentifier + "> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#Thing> . \n");
                        sb.append("<" + JenaConnectionFactory.nameSpace + "hasName-person" + personIdentifier + "> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2006/vcard/ns#Communication> . \n");
                        sb.append("<" + JenaConnectionFactory.nameSpace + "hasName-person" + personIdentifier + "> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2006/vcard/ns#Identification> . \n");
                        sb.append("<" + JenaConnectionFactory.nameSpace + "hasName-person" + personIdentifier + "> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2006/vcard/ns#Name> . \n");
                        sb.append("<" + JenaConnectionFactory.nameSpace + "hasName-person" + personIdentifier + "> vitro:mostSpecificType <http://www.w3.org/2006/vcard/ns#Name> . \n");
                        if(reCiterArticleAuthorFeature.getFirstName() != null)
                            sb.append("<" + JenaConnectionFactory.nameSpace + "hasName-person" + personIdentifier + "> <http://www.w3.org/2006/vcard/ns#givenName> \"" + reCiterArticleAuthorFeature.getFirstName().replaceAll("'", "\'") + "\" . \n");
                        if(reCiterArticleAuthorFeature.getLastName() != null)
                            sb.append("<" + JenaConnectionFactory.nameSpace + "hasName-person" + personIdentifier + "> <http://www.w3.org/2006/vcard/ns#familyName> \"" + reCiterArticleAuthorFeature.getLastName().replaceAll("'", "\'") + "\" . \n");
                    }
                }
                //case when no target author identified
                if(targetAuthorCount == 0) {
                    sb.append(publicationUrl + " core:relatedBy <" + JenaConnectionFactory.nameSpace + "pubid" + articleFeature.getPmid() + "authorship" + articleFeature.getPmid() + "> . \n");
                    sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + articleFeature.getPmid() + "authorship" + articleFeature.getPmid() + "> rdf:type obo:BFO_0000001 . \n");
                    sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + articleFeature.getPmid() + "authorship" + articleFeature.getPmid() + "> rdf:type obo:BFO_0000002 . \n");
                    sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + articleFeature.getPmid() + "authorship" + articleFeature.getPmid() + "> rdf:type obo:BFO_0000020 . \n");
                    sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + articleFeature.getPmid() + "authorship" + articleFeature.getPmid() + "> rdf:type <http://www.w3.org/2002/07/owl#Thing> . \n");
                    sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + articleFeature.getPmid() + "authorship" + articleFeature.getPmid() + "> rdf:type core:Relationship . \n");
                    sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + articleFeature.getPmid() + "authorship" + articleFeature.getPmid() + "> rdf:type core:Authorship . \n");
                    sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + articleFeature.getPmid() + "authorship" + articleFeature.getPmid() + "> vitro:mostSpecificType core:Authorship . \n");
                    sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + articleFeature.getPmid() + "authorship" + articleFeature.getPmid() + "> core:relates " + publicationUrl + " . \n");
                    //sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + articleFeature.getPmid() + "authorship" + articleFeature.getPmid() + "> core:rank \"" + reCiterArticleAuthorFeature.getRank() + "\"^^xsd:integer . \n");
                    //Linking vcard of the person
                    sb.append("<" + JenaConnectionFactory.nameSpace + "cwid-" + uid + "> rdf:type foaf:Person . \n");
                    sb.append("<" + JenaConnectionFactory.nameSpace + "cwid-" + uid + "> core:relatedBy <" + JenaConnectionFactory.nameSpace + "pubid" + articleFeature.getPmid() + "authorship" + articleFeature.getPmid() + "> . \n");
                    sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + articleFeature.getPmid() + "authorship" + articleFeature.getPmid() + "> core:relates <" + JenaConnectionFactory.nameSpace + "cwid-" + uid + "> . \n");
                    sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + articleFeature.getPmid() + "authorship" + articleFeature.getPmid() + "> core:relates <" + JenaConnectionFactory.nameSpace + "arg2000028-" + uid + "> . \n");
                    sb.append("<" + JenaConnectionFactory.nameSpace + "arg2000028-" + uid + "> obo:ARG_2000029 <" + JenaConnectionFactory.nameSpace + "pubid" + articleFeature.getPmid() + "authorship" + articleFeature.getPmid() + "> . \n");
                    sb.append("<" + JenaConnectionFactory.nameSpace + "arg2000028-" + uid + "> core:relatedBy <" + JenaConnectionFactory.nameSpace + "pubid" + articleFeature.getPmid() + "authorship" + articleFeature.getPmid() + "> . \n");
                    sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + articleFeature.getPmid() + "authorship" + articleFeature.getPmid() + "> obo:ARG_2000028 <" + JenaConnectionFactory.nameSpace + "arg2000028-" + uid + "> . \n");
                }
            } else {
                log.info("Publication " + articleFeature.getPmid() + " has not author record. Thus assigning to the uid.");
                //case when there is no author record
                sb.append(publicationUrl + " core:relatedBy <" + JenaConnectionFactory.nameSpace + "pubid" + articleFeature.getPmid() + "authorship" + articleFeature.getPmid() + "> . \n");
                sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + articleFeature.getPmid() + "authorship" + articleFeature.getPmid() + "> rdf:type obo:BFO_0000001 . \n");
                sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + articleFeature.getPmid() + "authorship" + articleFeature.getPmid() + "> rdf:type obo:BFO_0000002 . \n");
                sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + articleFeature.getPmid() + "authorship" + articleFeature.getPmid() + "> rdf:type obo:BFO_0000020 . \n");
                sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + articleFeature.getPmid() + "authorship" + articleFeature.getPmid() + "> rdf:type <http://www.w3.org/2002/07/owl#Thing> . \n");
                sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + articleFeature.getPmid() + "authorship" + articleFeature.getPmid() + "> rdf:type core:Relationship . \n");
                sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + articleFeature.getPmid() + "authorship" + articleFeature.getPmid() + "> rdf:type core:Authorship . \n");
                sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + articleFeature.getPmid() + "authorship" + articleFeature.getPmid() + "> vitro:mostSpecificType core:Authorship . \n");
                sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + articleFeature.getPmid() + "authorship" + articleFeature.getPmid() + "> core:relates " + publicationUrl + " . \n");
                //sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + articleFeature.getPmid() + "authorship" + articleFeature.getPmid() + "> core:rank \"" + reCiterArticleAuthorFeature.getRank() + "\"^^xsd:integer . \n");
                //Linking vcard of the person
                sb.append("<" + JenaConnectionFactory.nameSpace + "cwid-" + uid + "> rdf:type foaf:Person . \n");
                sb.append("<" + JenaConnectionFactory.nameSpace + "cwid-" + uid + "> core:relatedBy <" + JenaConnectionFactory.nameSpace + "pubid" + articleFeature.getPmid() + "authorship" + articleFeature.getPmid() + "> . \n");
                sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + articleFeature.getPmid() + "authorship" + articleFeature.getPmid() + "> core:relates <" + JenaConnectionFactory.nameSpace + "cwid-" + uid + "> . \n");
                sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + articleFeature.getPmid() + "authorship" + articleFeature.getPmid() + "> core:relates <" + JenaConnectionFactory.nameSpace + "arg2000028-" + uid + "> . \n");
                sb.append("<" + JenaConnectionFactory.nameSpace + "arg2000028-" + uid + "> obo:ARG_2000029 <" + JenaConnectionFactory.nameSpace + "pubid" + articleFeature.getPmid() + "authorship" + articleFeature.getPmid() + "> . \n");
                sb.append("<" + JenaConnectionFactory.nameSpace + "arg2000028-" + uid + "> core:relatedBy <" + JenaConnectionFactory.nameSpace + "pubid" + articleFeature.getPmid() + "authorship" + articleFeature.getPmid() + "> . \n");
                sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + articleFeature.getPmid() + "authorship" + articleFeature.getPmid() + "> obo:ARG_2000028 <" + JenaConnectionFactory.nameSpace + "arg2000028-" + uid + "> . \n");

            }
            sb.append(publicationUrl + " <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"ReCiter Connect\" . \n");
        }
        sb.append("}}");
        //log.info(sb.toString());

        if(ingestType.equals(IngestType.SDB_DIRECT.toString())) {
            try {
                vivoJena.executeUpdateQuery(sb.toString(), true);
            } catch(IOException e) {
                log.error("Error connecting to SDBJena");
            }
            catch(QueryParseException qpe) {
                log.error("QueryParseException", qpe);
                log.error("ERROR: The pub is for " + uid);
            }
        } else {
            try{
                String response = this.vivoClient.vivoUpdateApi(sb.toString());
                log.info(response);
            } catch(Exception  e) {
                log.info("Api Exception", e);
            }
        }
        stopWatch.stop();
        log.info("Publication import for " + uid + " took " + stopWatch.getTotalTimeSeconds()+"s");

    }

    @Override
    public void syncPublications(List<ReCiterArticleFeature> articles, List<Long> vivoPubs, SDBJenaConnect vivoJena) {
        StringBuilder sb = new StringBuilder();
        for(Long pmid: vivoPubs) {
            ReCiterArticleFeature reciterPub = articles.stream()
                                                        .filter(pub -> pub.getPmid() == pmid)
                                                        .findAny()
                                                        .orElse(null);
            sb.append(QueryConstants.getSparqlPrefixQuery());
            sb.append("select ?citationCount ?pubType ?pmcid \n");
            sb.append("where { \n");
            sb.append("GRAPH <" + VivoGraphs.PUBLICATIONS_GRAPH + "> {\n");
            sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + pmid + "> <http://purl.org/spar/c4o/hasGlobalCitationFrequency> ?citation . \n");
            sb.append("?citation rdfs:label ?citationCount . \n");
            sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + pmid + "> vitro:mostSpecificType ?pubType . \n");
            sb.append("OPTIONAL {<" + JenaConnectionFactory.nameSpace + "pubid" + pmid + "> core:pmcid ?pmcid . }\n");
            sb.append("}}");
            
            try {
				ResultSet rs = vivoJena.executeSelectQuery(sb.toString(), true);
				while(rs.hasNext()) {
                    QuerySolution qs = rs.nextSolution();
                    if(reciterPub != null) {
                        if(qs.get("pmcid") != null) {
                            if(!reciterPub.getPmcid().equalsIgnoreCase(qs.get("pmcid").toString())) {
                                sb.setLength(0);
                                sb.append(QueryConstants.getSparqlPrefixQuery());
                                sb.append("WITH <" + VivoGraphs.PUBLICATIONS_GRAPH + "> \n");
                                sb.append("DELETE { \n");
                                sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + pmid + "> core:pmcid ?pmcid . \n");
                                sb.append("} \n");
                                sb.append("INSERT { \n");
                                sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + pmid + "> core:pmcid \"" + reciterPub.getPmcid() + "\" . \n");
                                sb.append("} \n");
                                sb.append("WHERE { \n");
                                sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + pmid + "> core:pmcid ?pmcid . \n");
                                sb.append("}");
                                log.info("PMCID was updated for publication - " + pmid + " from " + qs.get("pmcid").toString() + " to " + reciterPub.getPmcid());
                                if(ingestType.equals(IngestType.VIVO_API.toString())) {
                                    String response = this.vivoClient.vivoUpdateApi(sb.toString());
                                    log.info(response);
                                } else if(ingestType.equals(IngestType.SDB_DIRECT.toString())) {
                                    try {
                                        vivoJena.executeUpdateQuery(sb.toString(), true);
                                    } catch(IOException e) {
                                        log.error("Error connecting to SDBJena");
                                    }
                                    catch(QueryParseException qpe) {
                                        log.error("QueryParseException", qpe);
                                        log.error("ERROR: The pub is " + pmid);
                                    }
                                }
                            } else {
                                log.info("PMCID is in sync for publication - " + pmid);
                            }
                        }
                        if(qs.get("citationCount") != null) {
                            //Remove citation date
                            /*sb.setLength(0);
                            sb.append(QueryConstants.getSparqlPrefixQuery());
                            sb.append("WITH <" + VivoGraphs.PUBLICATIONS_GRAPH + "> \n");
                            sb.append("DELETE { \n");
                            sb.append("<" + JenaConnectionFactory.nameSpace + "citation-pubid" + pmid + "> core:dateTimeValue ?citationDate . \n");
                            sb.append("} \n");
                            sb.append("WHERE { \n");
                            sb.append("<" + JenaConnectionFactory.nameSpace + "citation-pubid" + pmid + "> core:dateTimeValue ?citationDate . \n");
                            sb.append("}");
                            log.info("Citation Date was removed for publication - " + pmid);
                            
                            try {
                                vivoJena.executeUpdateQuery(sb.toString(), true);
                            } catch(IOException e) {
                                log.error("Error connecting to SDBJena");
                            }
                            catch(QueryParseException qpe) {
                                log.error("QueryParseException", qpe);
                                log.error("ERROR: The pub is " + pmid);
                            }*/
                            if(reciterPub.getTimesCited() != Long.parseLong(qs.get("citationCount").toString())) {
                                sb.setLength(0);
                                sb.append(QueryConstants.getSparqlPrefixQuery());
                                sb.append("WITH <" + VivoGraphs.PUBLICATIONS_GRAPH + "> \n");
                                sb.append("DELETE { \n");
                                sb.append("<" + JenaConnectionFactory.nameSpace + "citation-pubid" + pmid + "> rdfs:label ?count . \n");
                                sb.append("} \n");
                                sb.append("INSERT { \n");
                                sb.append("<" + JenaConnectionFactory.nameSpace + "citation-pubid" + pmid + "> rdfs:label \"" + reciterPub.getTimesCited() + "\" . \n");
                                sb.append("} \n");
                                sb.append("WHERE { \n");
                                sb.append("<" + JenaConnectionFactory.nameSpace + "citation-pubid" + pmid + "> rdfs:label ?count . \n");
                                sb.append("}");
                                log.info("Citation Count was updated for publication - " + pmid + " from " + qs.get("citationCount").toString() + " to " + reciterPub.getTimesCited());
                                
                                try {
                                    vivoJena.executeUpdateQuery(sb.toString(), true);
                                } catch(IOException e) {
                                    log.error("Error connecting to SDBJena");
                                }
                                catch(QueryParseException qpe) {
                                    log.error("QueryParseException", qpe);
                                    log.error("ERROR: The pub is " + pmid);
                                }
                                
                            } else {
                                log.info("Times Cited is in sync for publication - " + pmid);
                            }
                        }
                        if(qs.get("pubType") != null) {
                            if(!reciterPub.getPublicationType().getPublicationTypeCanonical().contains(qs.get("pubType").toString().replace("http://purl.org/ontology/bibo/", "")
                            .replace("http://vivoweb.org/ontology/core#", "").replace("http://purl.org/spar/fabio", "").replace("http://weill.cornell.edu/vivo/ontology/wcmc#", "").substring(0,6))) {
                                sb.setLength(0);
                                sb.append(QueryConstants.getSparqlPrefixQuery());
                                sb.append("WITH <" + VivoGraphs.PUBLICATIONS_GRAPH + "> \n");
                                sb.append("DELETE { \n");
                                sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + pmid + "> rdf:type ?type . \n");
                                sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + pmid + "> vitro:mostSpecificType ?mostSpecificType . \n");
                                sb.append("} \n");
                                sb.append("INSERT { \n");
                                sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + pmid + "> rdf:type core:InformationResource . \n");
                                sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + pmid + "> rdf:type bibo:Document . \n");
                                sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + pmid + "> rdf:type bibo:Article . \n");
                                if(reciterPub.getPublicationType().getPublicationTypeCanonical().equalsIgnoreCase("Editorial Article")) {
                                    sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + pmid + "> rdf:type core:EditorialArticle . \n");
                                    sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + pmid + "> vitro:mostSpecificType core:EditorialArticle . \n");
                                }
                                else if(reciterPub.getPublicationType().getPublicationTypeCanonical().equalsIgnoreCase("Letter")) {
                                    sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + pmid + "> rdf:type fabio:Letter . \n");
                                    sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + pmid + "> vitro:mostSpecificType fabio:Letter . \n");
                                }
                                else if(reciterPub.getPublicationType().getPublicationTypeCanonical().equalsIgnoreCase("Conference Paper")) {
                                    sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + pmid + "> rdf:type core:ConferencePaper . \n");
                                    sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + pmid + "> vitro:mostSpecificType core:ConferencePaper . \n");
                                }
                                else if(reciterPub.getPublicationType().getPublicationTypeCanonical().equalsIgnoreCase("Review")) {
                                    sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + pmid + "> rdf:type core:Review . \n");
                                    sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + pmid + "> vitro:mostSpecificType core:Review . \n");
                                }
                                else if(reciterPub.getPublicationType().getPublicationTypeCanonical().equalsIgnoreCase("Academic Article")) {
                                    sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + pmid + "> rdf:type bibo:AcademicArticle . \n");
                                    sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + pmid + "> vitro:mostSpecificType bibo:AcademicArticle . \n");
                                }
                                else if(reciterPub.getPublicationType().getPublicationTypeCanonical().equalsIgnoreCase("Article")) {
                                    sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + pmid + "> rdf:type bibo:Article . \n");
                                    sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + pmid + "> vitro:mostSpecificType bibo:Article . \n");
                                }
                                else if(reciterPub.getPublicationType().getPublicationTypeCanonical().equalsIgnoreCase("Comment")) {
                                    sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + pmid + "> rdf:type fabio:Comment . \n");
                                    sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + pmid + "> vitro:mostSpecificType fabio:Comment . \n");
                                }
                                else if(reciterPub.getPublicationType().getPublicationTypeCanonical().equalsIgnoreCase("In Process")) {
                                    sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + pmid + "> rdf:type wcmc:InProcess . \n");
                                    sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + pmid + "> vitro:mostSpecificType wcmc:InProcess . \n");
                                }
                                else if(reciterPub.getPublicationType().getPublicationTypeCanonical().equalsIgnoreCase("PubMed.ConferencePaper")) {
                                    sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + pmid + "> rdf:type core:ConferencePaper . \n");
                                    sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + pmid + "> vitro:mostSpecificType core:ConferencePaper . \n");
                                }
                                else if(reciterPub.getPublicationType().getPublicationTypeCanonical().equalsIgnoreCase("Report")) {
                                    sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + pmid + "> rdf:type bibo:Report . \n");
                                    sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + pmid + "> vitro:mostSpecificType bibo:Report . \n");
                                }
                                sb.append("} \n");
                                sb.append("WHERE { \n");
                                sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + pmid + "> rdf:type ?type . \n");
                                sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + pmid + "> vitro:mostSpecificType ?mostSpecificType . \n");
                                sb.append("}");
                                if(ingestType.equals(IngestType.VIVO_API.toString())) {
                                    String response = this.vivoClient.vivoUpdateApi(sb.toString());
                                    log.info(response);
                                } else if(ingestType.equals(IngestType.SDB_DIRECT.toString())) {
                                    try {
                                        vivoJena.executeUpdateQuery(sb.toString(), true);
                                    } catch(IOException e) {
                                        log.error("Error connecting to SDBJena");
                                    }
                                    catch(QueryParseException qpe) {
                                        log.error("QueryParseException", qpe);
                                        log.error("ERROR: The pub is " + pmid);
                                    }
                                }
                            } else {
                                log.info("Publications Type is in sync for publication - " + pmid);
                            }
                        }
                    }
                                                        
                }
                //Mesh Major
                List<String> vivoMeshMajor = new ArrayList<>();
                sb.setLength(0);
                sb.append(QueryConstants.getSparqlPrefixQuery());
                sb.append("select ?mesh \n");
                sb.append("where { \n");
                sb.append("GRAPH <" + VivoGraphs.PUBLICATIONS_GRAPH + "> {\n");
                sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + pmid + "> core:freetextKeyword ?mesh . \n");
                sb.append("}}");

                rs = vivoJena.executeSelectQuery(sb.toString(), true);
                if(reciterPub != null && reciterPub.getArticleKeywords() != null && !reciterPub.getArticleKeywords().isEmpty()) {
                    List<String> reciterMeshMajor = reciterPub.getArticleKeywords()
                                                    .stream()
                                                    .filter(mesh -> mesh.getType() ==  KeywordType.MESH_MAJOR)
                                                    .map(ArticleKeyword::getKeyword)
                                                    .collect(Collectors.toList());
                    while(rs.hasNext()) {
                        QuerySolution qs = rs.nextSolution();
                        vivoMeshMajor.add(qs.get("mesh").toString());
                    }   
                    reciterMeshMajor.removeAll(vivoMeshMajor);
                    if(!reciterMeshMajor.isEmpty()) {
                        sb.setLength(0);
                        sb.append(QueryConstants.getSparqlPrefixQuery());
                        sb.append("INSERT DATA { \n");
                        sb.append("GRAPH <" + VivoGraphs.PUBLICATIONS_GRAPH + "> { \n");
                        for(String meshMajor: reciterMeshMajor) {
                            sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + pmid + "> core:freetextKeyword \"" + meshMajor + "\" . \n");
                        }
                        sb.append("}}");
                        if(ingestType.equals(IngestType.VIVO_API.toString())) {
                            String response = this.vivoClient.vivoUpdateApi(sb.toString());
                            log.info(response);
                        } else if(ingestType.equals(IngestType.SDB_DIRECT.toString())) {
                            try {
                                vivoJena.executeUpdateQuery(sb.toString(), true);
                            } catch(IOException e) {
                                log.error("Error connecting to SDBJena");
                            }
                            catch(QueryParseException qpe) {
                                log.error("QueryParseException", qpe);
                                log.error("ERROR: The pub is " + pmid);
                            }
                        }
                    } else {
                        log.info("Mesh Major is in sync for publication - " + pmid);
                    }
                }
            } catch(Exception e) {
                log.error("Error connecting to SDBJena", e);
            }
            
            sb.setLength(0);
        }
    }

    private String journalIdentifier(ReCiterArticleFeature reCiterArticleFeature) {
        if (reCiterArticleFeature.getJournalTitleISOabbreviation() != null
                && !reCiterArticleFeature.getJournalTitleISOabbreviation().isEmpty()) {
            return DigestUtils.md5Hex(reCiterArticleFeature.getJournalTitleISOabbreviation().toLowerCase());
        }
        return DigestUtils.md5Hex(reCiterArticleFeature.getJournalTitleVerbose().toLowerCase());

    }

    private String getExternalPersonIdentifier(ReCiterArticleAuthorFeature reCiterArticleAuthorFeature) {
        return DigestUtils.md5Hex(reCiterArticleAuthorFeature.getFirstName() + reCiterArticleAuthorFeature.getLastName()).toLowerCase();

    }

    @Override
    public String publicationsExist(List<ArticleRetrievalModel> articles) {
        SDBJenaConnect vivoJena = this.jcf.getConnectionfromPool("dataSet");
        if(articles != null && !articles.isEmpty()) {
            for (ArticleRetrievalModel articleRetrievalModel : articles) {
                if(articleRetrievalModel.getReCiterArticleFeatures() != null && !articleRetrievalModel.getReCiterArticleFeatures().isEmpty()) {
                    log.info("*******************Starting publication import for " + articleRetrievalModel.getPersonIdentifier() + "************************");
                    StringBuilder sb = new StringBuilder();
                    List<Long> vivoPublications = new ArrayList<>();
                    sb.append(QueryConstants.getSparqlPrefixQuery());
                    sb.append("select ?pubs \n");
                    sb.append("where { \n");
                    sb.append("GRAPH <" + VivoGraphs.PUBLICATIONS_GRAPH + "> {\n");
                    sb.append("<" + JenaConnectionFactory.nameSpace + "cwid-" + articleRetrievalModel.getPersonIdentifier() + "> core:relatedBy ?authorship . \n");
                    sb.append("?authorship core:relates ?publication . \n");
                    sb.append("?publication rdf:type core:InformationResource . \n");
                    sb.append("?publication rdf:type bibo:Document . \n");
                    sb.append("?publication rdf:type bibo:Article . \n");
                    sb.append("BIND(REPLACE(STR(?publication), \"https://vivo.med.cornell.edu/individual/pubid\", \"\") AS ?pubs) \n");
                    sb.append("}}");
                    
                    try {
                        ResultSet rs = vivoJena.executeSelectQuery(sb.toString(), true);
                        while(rs.hasNext()) {
                            QuerySolution qs = rs.nextSolution();
                            vivoPublications.add(Long.parseLong(qs.get("pubs").toString()));
                        }
                    } catch(IOException e) {
                        log.error("Error connecting to SDBJena");
                    }
                    List<Long> reciterPublications = articleRetrievalModel.getReCiterArticleFeatures()
                                                        .stream()
                                                        .map(ReCiterArticleFeature::getPmid)
                                                        .collect(Collectors.toList());

                    List<Long> pubsDifferences = new ArrayList<>(reciterPublications);
                    pubsDifferences.removeAll(vivoPublications);
                    if(!pubsDifferences.isEmpty()) {
                        log.info("Some publications does not exist in VIVO. Importing them now for " + articleRetrievalModel.getPersonIdentifier() +  " List: " + pubsDifferences.toString());
                        if(pubsDifferences.size() != articleRetrievalModel.getReCiterArticleFeatures().size()) {
                            List<ReCiterArticleFeature> newPublications = articleRetrievalModel.getReCiterArticleFeatures()
                                                                            .stream()
                                                                            .filter(pub -> pubsDifferences.contains(pub.getPmid()))
                                                                            .collect(Collectors.toList());
                            checkPublicationExistInVivo(newPublications, articleRetrievalModel.getPersonIdentifier(), vivoJena);
                            importPublications(newPublications, articleRetrievalModel.getPersonIdentifier(), articleRetrievalModel.getDateUpdated(), vivoJena);
                        } else {
                            checkPublicationExistInVivo(articleRetrievalModel.getReCiterArticleFeatures(), articleRetrievalModel.getPersonIdentifier(), vivoJena);
                            importPublications(articleRetrievalModel.getReCiterArticleFeatures(), articleRetrievalModel.getPersonIdentifier(), articleRetrievalModel.getDateUpdated(), vivoJena);
                        }
                    } else {
                        log.info("All publications from ReCiter exists in VIVO for " + articleRetrievalModel.getPersonIdentifier());
                    }
                    //Check for publications that needs to be deleted
                    List<Long> vivoPubs = new ArrayList<>(vivoPublications);
                    syncPublications(articleRetrievalModel.getReCiterArticleFeatures(), vivoPubs, vivoJena);
                    deletePublicationsVivo(vivoPubs, reciterPublications, vivoJena, articleRetrievalModel.getPersonIdentifier());
                    log.info("*******************Ending publication import for " + articleRetrievalModel.getPersonIdentifier() + "************************");
                }

            }
        }
        this.jcf.returnConnectionToPool(vivoJena, "dataSet");
        return "Publications fetch completed";
    }

    private void deletePublicationsVivo(List<Long> vivoPubs, List<Long> reciterPubs, SDBJenaConnect vivoJena, String uid) {
        vivoPubs.removeAll(reciterPubs);
        if(!vivoPubs.isEmpty()) {
            for(Long pmid: vivoPubs) {
                StringBuilder sb = new StringBuilder();
                sb.append(QueryConstants.getSparqlPrefixQuery());
                sb.append("WITH <" + VivoGraphs.PUBLICATIONS_GRAPH + "> \n");
                sb.append("DELETE { \n");
                sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + pmid + "> ?p ?o . \n");
                sb.append("?authorship ?p1 ?o1 .");
                //sb.append("?citation ?p2 ?o2 . \n");
                sb.append("<" + JenaConnectionFactory.nameSpace + "cwid-" + uid + "> core:relatedBy ?authorship . \n");
                sb.append("} \n");
                sb.append("WHERE { \n");
                sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + pmid + "> core:relatedBy ?authorship . \n");
                sb.append("?authorship core:relates <" + JenaConnectionFactory.nameSpace + "cwid-" + uid + "> . \n");
                sb.append("<" + JenaConnectionFactory.nameSpace + "cwid-" + uid + "> rdf:type foaf:Person . \n");
                //sb.append("OPTIONAL {<" + JenaConnectionFactory.nameSpace + "pubid" + pmid + "> <http://purl.org/spar/c4o/hasGlobalCitationFrequency> ?citation . }\n");
                sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + pmid + "> ?p ?o . \n");
                sb.append("?authorship ?p1 ?o1 .");
                //sb.append("OPTIONAL {?citation ?p2 ?o2 . }\n");
                sb.append("} \n");

                log.info("Deleting publication " + pmid + " from VIVO");
                if(ingestType.equals(IngestType.VIVO_API.toString())) {
                    try{
                        String response = this.vivoClient.vivoUpdateApi(sb.toString());
                        log.info(response);
                    } catch(Exception  e) {
                        log.info("Api Exception", e);
                    }
                } else if(ingestType.equals(IngestType.SDB_DIRECT.toString())) {
                    try {
                        vivoJena.executeUpdateQuery(sb.toString(), true);
                    } catch(IOException e) {
                        log.error("Error connecting to SDBJena");
                    }
                    catch(QueryParseException qpe) {
                        log.error("QueryParseException", qpe);
                        log.error("ERROR: The pub is " + pmid);
                    }
                }
            }
        } else {
            log.info("No Publications to delete from VIVO");
        }
    }

    private void checkPublicationExistInVivo(List<ReCiterArticleFeature> articles, String uid,  SDBJenaConnect vivoJena) {
        Iterator<ReCiterArticleFeature> it = articles.iterator();
        while(it.hasNext()) {
            ReCiterArticleFeature article = it.next();
            StringBuilder sb = new StringBuilder();
            sb.append(QueryConstants.getSparqlPrefixQuery());
            sb.append("SELECT (count(?o) as ?count) \n");
            sb.append("WHERE { \n");
            sb.append("GRAPH <" + VivoGraphs.PUBLICATIONS_GRAPH + "> {\n");
            sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + article.getPmid() + "> rdf:type ?o . \n");
            sb.append("}}");

            
			
			ResultSet rs;
            try {
                rs = vivoJena.executeSelectQuery(sb.toString(), true);
                QuerySolution qs = rs.nextSolution();
                int count = Integer.parseInt(qs.get("count").toString().replace("^^http://www.w3.org/2001/XMLSchema#integer", ""));
                if(count > 0) {
                    it.remove();
                    log.info("Publication " + article.getPmid() + " already exist in VIVO. Updating authorship - ");
                    syncAuthorship(article, uid, vivoJena);
                }
                    
            } catch (IOException e) {
                log.error("Error connecting to SDBJena", e);
            } 
        }
    }

    private void syncAuthorship(ReCiterArticleFeature article, String uid, SDBJenaConnect vivoJena) {
        int targetAuthorCount = 0;
            if(article.getReCiterArticleAuthorFeatures() != null && !article.getReCiterArticleAuthorFeatures().isEmpty()) {
                for(ReCiterArticleAuthorFeature reCiterArticleAuthorFeature: article.getReCiterArticleAuthorFeatures()) {
                    if(reCiterArticleAuthorFeature.isTargetAuthor())
                        targetAuthorCount++;
                    
                    if(reCiterArticleAuthorFeature.isTargetAuthor() && targetAuthorCount == 1) {
                        StringBuilder sb = new StringBuilder();
                        sb.append(QueryConstants.getSparqlPrefixQuery());
                        sb.append("WITH <" + VivoGraphs.PUBLICATIONS_GRAPH + "> \n");
                        sb.append("DELETE { \n");
                        sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + article.getPmid() + "authorship" + reCiterArticleAuthorFeature.getRank() + "> core:relates ?person . \n");
                        sb.append("?person ?p ?o . \n");
                        sb.append("?vcard ?p1 ?o1 . \n");
                        sb.append("} \n");
                        sb.append("INSERT { \n");
                        sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + article.getPmid() + "authorship" + reCiterArticleAuthorFeature.getRank() + "> core:relates <" + JenaConnectionFactory.nameSpace + "cwid-" + uid + "> . \n");
                        sb.append("<" + JenaConnectionFactory.nameSpace + "cwid-" + uid + "> core:relatedBy <" + JenaConnectionFactory.nameSpace + "pubid" + article.getPmid() + "authorship" + reCiterArticleAuthorFeature.getRank() + "> . \n");
                        sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + article.getPmid() + "authorship" + reCiterArticleAuthorFeature.getRank() + "> core:relates <" + JenaConnectionFactory.nameSpace + "arg2000028-" + uid + "> . \n");
                        sb.append("<" + JenaConnectionFactory.nameSpace + "arg2000028-" + uid + "> obo:ARG_2000029 <" + JenaConnectionFactory.nameSpace + "pubid" + article.getPmid() + "authorship" + reCiterArticleAuthorFeature.getRank() + "> . \n");
                        sb.append("<" + JenaConnectionFactory.nameSpace + "arg2000028-" + uid + "> core:relatedBy <" + JenaConnectionFactory.nameSpace + "pubid" + article.getPmid() + "authorship" + reCiterArticleAuthorFeature.getRank() + "> . \n");
                        sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + article.getPmid() + "authorship" + reCiterArticleAuthorFeature.getRank() + "> obo:ARG_2000028 <" + JenaConnectionFactory.nameSpace + "arg2000028-" + uid + "> . \n");
                        sb.append("} \n");
                        sb.append("WHERE { \n");
                        sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + article.getPmid() + "> core:relatedBy ?authorship . \n");
                        sb.append("?authorship core:relates ?person . \n");
                        sb.append("?person rdf:type vcard:Individual . \n");
                        sb.append("?person vcard:hasName ?vcard . \n");
                        sb.append("} \n");

                        log.info("Synching authorship for publication " + article.getPmid());
                        try {
                            vivoJena.executeUpdateQuery(sb.toString(), true);
                        } catch(IOException e) {
                            log.error("Error connecting to SDBJena");
                        }
                    }
                }
        } else {
            log.info("Publication " + article.getPmid() + " has not author record. Thus assigning to the uid.");
            StringBuilder sb = new StringBuilder();
            sb.append(QueryConstants.getSparqlPrefixQuery());
            sb.append("INSERT DATA { \n");
            sb.append("GRAPH <" + VivoGraphs.PUBLICATIONS_GRAPH + "> {\n");
            //case when there is no author record
            sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + article.getPmid() + "> core:relatedBy <" + JenaConnectionFactory.nameSpace + "pubid" + article.getPmid() + "authorship" + article.getPmid() + "> . \n");
            sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + article.getPmid() + "authorship" + article.getPmid() + "> rdf:type obo:BFO_0000001 . \n");
            sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + article.getPmid() + "authorship" + article.getPmid() + "> rdf:type obo:BFO_0000002 . \n");
            sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + article.getPmid() + "authorship" + article.getPmid() + "> rdf:type obo:BFO_0000020 . \n");
            sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + article.getPmid() + "authorship" + article.getPmid() + "> rdf:type <http://www.w3.org/2002/07/owl#Thing> . \n");
            sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + article.getPmid() + "authorship" + article.getPmid() + "> rdf:type core:Relationship . \n");
            sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + article.getPmid() + "authorship" + article.getPmid() + "> rdf:type core:Authorship . \n");
            sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + article.getPmid() + "authorship" + article.getPmid() + "> vitro:mostSpecificType core:Authorship . \n");
            sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + article.getPmid() + "authorship" + article.getPmid() + "> core:relates <" + JenaConnectionFactory.nameSpace + "pubid" + article.getPmid() + "> . \n");
            //sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + articleFeature.getPmid() + "authorship" + articleFeature.getPmid() + "> core:rank \"" + reCiterArticleAuthorFeature.getRank() + "\"^^xsd:integer . \n");
            //Linking vcard of the person
            sb.append("<" + JenaConnectionFactory.nameSpace + "cwid-" + uid + "> rdf:type foaf:Person . \n");
            sb.append("<" + JenaConnectionFactory.nameSpace + "cwid-" + uid + "> core:relatedBy <" + JenaConnectionFactory.nameSpace + "pubid" + article.getPmid() + "authorship" + article.getPmid() + "> . \n");
            sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + article.getPmid() + "authorship" + article.getPmid() + "> core:relates <" + JenaConnectionFactory.nameSpace + "cwid-" + uid + "> . \n");
            sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + article.getPmid() + "authorship" + article.getPmid() + "> core:relates <" + JenaConnectionFactory.nameSpace + "arg2000028-" + uid + "> . \n");
            sb.append("<" + JenaConnectionFactory.nameSpace + "arg2000028-" + uid + "> obo:ARG_2000029 <" + JenaConnectionFactory.nameSpace + "pubid" + article.getPmid() + "authorship" + article.getPmid() + "> . \n");
            sb.append("<" + JenaConnectionFactory.nameSpace + "arg2000028-" + uid + "> core:relatedBy <" + JenaConnectionFactory.nameSpace + "pubid" + article.getPmid() + "authorship" + article.getPmid() + "> . \n");
            sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + article.getPmid() + "authorship" + article.getPmid() + "> obo:ARG_2000028 <" + JenaConnectionFactory.nameSpace + "arg2000028-" + uid + "> . \n");
            sb.append("}}");
            try {
                vivoJena.executeUpdateQuery(sb.toString(), true);
            } catch(IOException e) {
                log.error("Error connecting to SDBJena");
            }
        }
    }

    @Override
    public Callable<String> getCallable(List<ArticleRetrievalModel> articles) {
        return new Callable<String>() {
            public String call() throws Exception {
                return publicationsExist(articles);
            }
        };
    }
}