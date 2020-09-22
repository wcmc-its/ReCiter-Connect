package reciter.connect.vivo.sdb.publications.service;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.jena.query.QueryParseException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.vivoweb.harvester.util.repo.SDBJenaConnect;

import lombok.extern.slf4j.Slf4j;
import reciter.connect.api.client.model.ArticleRetrievalModel;
import reciter.connect.database.mysql.jena.JenaConnectionFactory;
import reciter.connect.vivo.sdb.VivoGraphs;
import reciter.connect.vivo.sdb.query.QueryConstants;
import reciter.engine.analysis.ReCiterArticleAuthorFeature;
import reciter.engine.analysis.ReCiterArticleFeature;
import reciter.engine.analysis.ReCiterArticleFeature.ArticleKeyword;
import reciter.engine.analysis.ReCiterArticleFeature.ArticleKeyword.KeywordType;

@Slf4j
@Service
public class VivoPublicationsServiceImpl implements VivoPublicationsService {

    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    private Calendar cal = Calendar.getInstance();

    @Autowired
	private JenaConnectionFactory jcf;

    @Override
    public void importPublications(List<ArticleRetrievalModel> articles) {
        StringBuilder sb = new StringBuilder();
        StringBuilder inf = new StringBuilder();
        sb.append(QueryConstants.getSparqlPrefixQuery());
        inf.append(QueryConstants.getSparqlPrefixQuery());
        sb.append("INSERT DATA { GRAPH <" + VivoGraphs.PUBLICATIONS_GRAPH + ">{ \n");
        inf.append("INSERT DATA { GRAPH <" + VivoGraphs.PUBLICATIONS_GRAPH + ">{ \n");
        for (ArticleRetrievalModel articleRetrievalModel : articles) {
            for (ReCiterArticleFeature articleFeature : articleRetrievalModel.getReCiterArticleFeatures()) {
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
                            + articleFeature.getPublicationAbstract().trim().replaceAll("([\\\\\\\\\"])", "\\\\$1").replaceAll("'", "\'")
                            + "\" .\n");
                }
                // articleTitle → rdfs:label
                if (articleFeature.getArticleTitle() != null) {
                    sb.append(publicationUrl + " rdfs:label \""
                            + articleFeature.getArticleTitle().trim().replaceAll("([\\\\\\\\\"])", "\\\\$1").replaceAll("'", "\'")
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
                if (articleFeature.getTimesCited() != null && articleFeature.getTimesCited() > 0) {
                    Date citationDate = null;
                    try {
                        citationDate = this.sdf.parse(articleRetrievalModel.getDateUpdated());
                    } catch (ParseException e) {
                        log.error("ParseException", e);
                    }
                    sb.append(publicationUrl + " <http://purl.org/spar/c4o/hasGlobalCitationFrequency> <"
                            + JenaConnectionFactory.nameSpace + "citation-pubid" + articleFeature.getPmid() + "> .\n");
                    sb.append("<" + JenaConnectionFactory.nameSpace + "citation-pubid" + articleFeature.getPmid()
                            + "> rdf:type <http://purl.org/spar/c4o/GlobalCitationCount> . \n");
                    sb.append("<" + JenaConnectionFactory.nameSpace + "citation-pubid" + articleFeature.getPmid()
                            + "> rdfs:label \"" + articleFeature.getTimesCited() + "\" . \n");
                    // Citation date
                    if (citationDate != null) {
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
                                + this.cal.get(Calendar.YEAR) + "> core:dateTime \"" + citationDate
                                + "T00:00:00\"^^<http://www.w3.org/2001/XMLSchema#dateTime> . \n");
                    }
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
                        + journalIdentifier + "> core:title \"" + articleFeature.getJournalTitleVerbose().replaceAll("'", "\'") + "\" . \n");
                sb.append("<" + JenaConnectionFactory.nameSpace + "journal"
                        + journalIdentifier + "> rdfs:label \"" + articleFeature.getJournalTitleVerbose().replaceAll("'", "\'") + "\" . \n");
                if (articleFeature.getJournalTitleISOabbreviation() != null)
                    sb.append("<" + JenaConnectionFactory.nameSpace + "journal"
                        + journalIdentifier + "> wcmc:ISOAbbreviation \"" + articleFeature.getJournalTitleISOabbreviation().trim()
                            + "\" . \n");
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
                        inf.append(publicationUrl + " vitro:mostSpecificType core:EditorialArticle . \n");
                        inf.append(publicationUrl + " rdf:type <http://www.w3.org/2002/07/owl#Thing> . \n");
                    }
                    else if(articleFeature.getPublicationType().getPublicationTypeCanonical().equalsIgnoreCase("Letter")) {
                        sb.append(publicationUrl + " rdf:type fabio:Letter . \n");
                        sb.append(publicationUrl + " vitro:mostSpecificType fabio:Letter . \n");
                        inf.append(publicationUrl + " vitro:mostSpecificType fabio:Letter . \n");
                        inf.append(publicationUrl + " rdf:type <http://www.w3.org/2002/07/owl#Thing> . \n");
                    }
                    else if(articleFeature.getPublicationType().getPublicationTypeCanonical().equalsIgnoreCase("Conference Paper")) {
                        sb.append(publicationUrl + " rdf:type core:ConferencePaper . \n");
                        sb.append(publicationUrl + " vitro:mostSpecificType core:ConferencePaper . \n");
                        inf.append(publicationUrl + " vitro:mostSpecificType core:ConferencePaper . \n");
                        inf.append(publicationUrl + " rdf:type <http://www.w3.org/2002/07/owl#Thing> . \n");
                    }
                    else if(articleFeature.getPublicationType().getPublicationTypeCanonical().equalsIgnoreCase("Review")) {
                        sb.append(publicationUrl + " rdf:type core:Review . \n");
                        sb.append(publicationUrl + " vitro:mostSpecificType core:Review . \n");
                        inf.append(publicationUrl + " vitro:mostSpecificType core:Review . \n");
                        inf.append(publicationUrl + " rdf:type <http://www.w3.org/2002/07/owl#Thing> . \n");
                    }
                    else if(articleFeature.getPublicationType().getPublicationTypeCanonical().equalsIgnoreCase("Academic Article")) {
                        sb.append(publicationUrl + " rdf:type bibo:AcademicArticle . \n");
                        sb.append(publicationUrl + " vitro:mostSpecificType bibo:AcademicArticle . \n");
                        inf.append(publicationUrl + " vitro:mostSpecificType bibo:AcademicArticle . \n");
                        inf.append(publicationUrl + " rdf:type <http://www.w3.org/2002/07/owl#Thing> . \n");
                    }
                    else if(articleFeature.getPublicationType().getPublicationTypeCanonical().equalsIgnoreCase("Article")) {
                        sb.append(publicationUrl + " rdf:type bibo:Article . \n");
                        sb.append(publicationUrl + " vitro:mostSpecificType bibo:Article . \n");
                        inf.append(publicationUrl + " vitro:mostSpecificType bibo:Article . \n");
                        inf.append(publicationUrl + " rdf:type <http://www.w3.org/2002/07/owl#Thing> . \n");
                    }
                    else if(articleFeature.getPublicationType().getPublicationTypeCanonical().equalsIgnoreCase("Comment")) {
                        sb.append(publicationUrl + " rdf:type fabio:Comment . \n");
                        sb.append(publicationUrl + " vitro:mostSpecificType fabio:Comment . \n");
                        inf.append(publicationUrl + " vitro:mostSpecificType fabio:Comment . \n");
                        inf.append(publicationUrl + " rdf:type <http://www.w3.org/2002/07/owl#Thing> . \n");
                    }
                    else if(articleFeature.getPublicationType().getPublicationTypeCanonical().equalsIgnoreCase("In Process")) {
                        sb.append(publicationUrl + " rdf:type wcmc:InProcess . \n");
                        sb.append(publicationUrl + " vitro:mostSpecificType wcmc:InProcess . \n");
                        inf.append(publicationUrl + " vitro:mostSpecificType wcmc:InProcess . \n");
                        inf.append(publicationUrl + " rdf:type <http://www.w3.org/2002/07/owl#Thing> . \n");
                    }
                    else if(articleFeature.getPublicationType().getPublicationTypeCanonical().equalsIgnoreCase("PubMed.ConferencePaper")) {
                        sb.append(publicationUrl + " rdf:type core:ConferencePaper . \n");
                        sb.append(publicationUrl + " vitro:mostSpecificType core:ConferencePaper . \n");
                        inf.append(publicationUrl + " vitro:mostSpecificType core:ConferencePaper . \n");
                        inf.append(publicationUrl + " rdf:type <http://www.w3.org/2002/07/owl#Thing> . \n");
                    }
                    else if(articleFeature.getPublicationType().getPublicationTypeCanonical().equalsIgnoreCase("Report")) {
                        sb.append(publicationUrl + " rdf:type bibo:Report . \n");
                        sb.append(publicationUrl + " vitro:mostSpecificType bibo:Report . \n");
                        inf.append(publicationUrl + " vitro:mostSpecificType bibo:Report . \n");
                        inf.append(publicationUrl + " rdf:type <http://www.w3.org/2002/07/owl#Thing> . \n");
                    }
                }
                //Author Assignment
                int targetAuthorCount = 0;
                if(articleFeature.getReCiterArticleAuthorFeatures() != null && !articleFeature.getReCiterArticleAuthorFeatures().isEmpty()) {
                    for(ReCiterArticleAuthorFeature reCiterArticleAuthorFeature: articleFeature.getReCiterArticleAuthorFeatures()) {
                        if(reCiterArticleAuthorFeature.isTargetAuthor())
                            targetAuthorCount++;
                        
                        if(reCiterArticleAuthorFeature.isTargetAuthor() && targetAuthorCount == 1) {
                            sb.append(publicationUrl + " core:relatedBy <" + JenaConnectionFactory.nameSpace + "pubid" + articleFeature.getPmid() + "authorship" + reCiterArticleAuthorFeature.getRank() + ">. \n");
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
                            sb.append("<" + JenaConnectionFactory.nameSpace + "cwid-" + articleRetrievalModel.getPersonIdentifier() + "> core:relatedBy <" + JenaConnectionFactory.nameSpace + "pubid" + articleFeature.getPmid() + "authorship" + reCiterArticleAuthorFeature.getRank() + "> . \n");
                            sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + articleFeature.getPmid() + "authorship" + reCiterArticleAuthorFeature.getRank() + "> core:relates <" + JenaConnectionFactory.nameSpace + "cwid-" + articleRetrievalModel.getPersonIdentifier() + "> . \n");
                            sb.append("<" + JenaConnectionFactory.nameSpace + "pubid" + articleFeature.getPmid() + "authorship" + reCiterArticleAuthorFeature.getRank() + "> core:relates <" + JenaConnectionFactory.nameSpace + "arg2000028-" + articleRetrievalModel.getPersonIdentifier() + "> . \n");
                            sb.append("<" + JenaConnectionFactory.nameSpace + "arg2000028-" + articleRetrievalModel.getPersonIdentifier() + "> obo:ARG_2000029 <" + JenaConnectionFactory.nameSpace + "pubid" + articleFeature.getPmid() + "authorship" + reCiterArticleAuthorFeature.getRank() + "> . \n");
                            sb.append("<" + JenaConnectionFactory.nameSpace + "arg2000028-" + articleRetrievalModel.getPersonIdentifier() + "> core:relatedBy <" + JenaConnectionFactory.nameSpace + "pubid" + articleFeature.getPmid() + "authorship" + reCiterArticleAuthorFeature.getRank() + "> . \n");
                            inf.append("<" + JenaConnectionFactory.nameSpace + "pubid" + articleFeature.getPmid() + "authorship" + reCiterArticleAuthorFeature.getRank() + "> obo:ARG_2000028 <" + JenaConnectionFactory.nameSpace + "arg2000028-" + articleRetrievalModel.getPersonIdentifier() + "> . \n");
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
                }
                sb.append(publicationUrl + " <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"ReCiter Connect\" . \n");
            }
        }
        sb.append("}}");
        inf.append("}}");
        SDBJenaConnect vivoJena = this.jcf.getConnectionfromPool("wcmcPublications");
        //log.info(sb.toString());
		try {
			vivoJena.executeUpdateQuery(sb.toString(), true);
		} catch(IOException e) {
			log.error("Error connecting to SDBJena");
        }
        catch(QueryParseException qpe) {
            log.error("QueryParseException", qpe);
        }
		this.jcf.returnConnectionToPool(vivoJena, "wcmcPublications");
		
		log.info("Inserting inference Triples now");
		
		
		vivoJena = this.jcf.getConnectionfromPool("vitro-kb-inf");
		//log.info(inf.toString());
		try {
			vivoJena.executeUpdateQuery(inf.toString(), true);
		} catch(IOException e) {
			log.error("Error connecting to SDBJena");
		}
		this.jcf.returnConnectionToPool(vivoJena, "vitro-kb-inf");
		

    }

    @Override
    public void syncPublications(List<ArticleRetrievalModel> articles) {
        // TODO Auto-generated method stub

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

}