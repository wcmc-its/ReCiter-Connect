package reciter.connect.vivo.sdb.publications.util;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.ibm.icu.text.SimpleDateFormat;

import org.apache.jena.query.QueryParseException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.StopWatch;
import org.vivoweb.harvester.util.repo.SDBJenaConnect;

import lombok.extern.slf4j.Slf4j;
import reciter.connect.beans.vivo.AuthorBean;
import reciter.connect.beans.vivo.PublicationBean;
import reciter.connect.database.mysql.MysqlConnectionFactory;
import reciter.connect.database.mysql.jena.JenaConnectionFactory;
import reciter.connect.vivo.api.client.VivoClient;
import reciter.connect.vivo.sdb.VivoGraphs;
import reciter.connect.vivo.sdb.query.QueryConstants;

@Slf4j
@Component
public class ScopusOnlyPubsImport {
    
    @Autowired
    private MysqlConnectionFactory mycf;

    @Autowired
    private VivoClient vivoClient;

    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    private Calendar cal = Calendar.getInstance();
    
    private List<PublicationBean> scopusOnlyPubs = new ArrayList<>();

    @Autowired
    private JenaConnectionFactory jcf;

    public void getScopusOnlyPubs(List<String> peopleCwids) {
        Connection con = this.mycf.getConnectionfromPool();
        
        StringBuilder selectQuery = new StringBuilder();
        selectQuery.append("select distinct d1.wcmc_document_pk,'artcle' as pubtype,scopus_doc_id, \n");
        selectQuery.append("concat('https://vivo.weill.cornell.edu/individual/nn',scopus_doc_id) as articleURI, \n");
        selectQuery.append("cast(case \n");
        selectQuery.append("when nlmisoabbreviation is not null then md5(nlmisoabbreviation) \n");
        selectQuery.append("when nlmAbbreviationNew is not null then md5(nlmAbbreviationNew) \n");
        selectQuery.append("else md5(publication_name) \n");
        selectQuery.append("end as char) as journaURI, \n");
        selectQuery.append("publication_name as journal_name,da1.wcmc_authorship_rank as rank,a1.cwid, \n");
        selectQuery.append("case \n");
        selectQuery.append("when issn is not null then issn \n");
        selectQuery.append("when eissn is not null then eissn \n");
        selectQuery.append("else null \n");
        selectQuery.append("end as issn,doi,issue,volume,pages,cover_date, \n");
        selectQuery.append("case \n");
        selectQuery.append("when substring(cover_date,3,2) = '01' and substring(cover_date,5,2) = '01' then 'year' \n");
        selectQuery.append("when substring(cover_date,3,2) = '01' then 'yearMonth' \n");
        selectQuery.append("else 'yearMonthDay' \n");
        selectQuery.append("end as datePrecision,title \n");
        selectQuery.append("from wcmc_authorship a1 \n");
        selectQuery.append("join wcmc_document_authorship da1 on da1.wcmc_authorship_fk = a1.wcmc_authorship_pk \n");
        selectQuery.append("join wcmc_document d1 on d1.wcmc_document_pk = da1.wcmc_document_fk \n");
        selectQuery.append("left join scopusIDsInReCiter r on r.scopusDocID = scopus_doc_id \n");
        selectQuery.append("left join NLM n on n.nlmabbreviation = d1.nlmabbreviationNEW \n");
        selectQuery.append("where cwid is not null and pmid is null and scopusDocID is null and a1.cwid not like '\\_%' and (pubtype like 'ar' or pubtype like '%Journal Article%')");

        log.info(selectQuery.toString());
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {	
                ps = con.prepareStatement(selectQuery.toString());
                rs = ps.executeQuery();
                while(rs.next()) {
                    PublicationBean pb = new PublicationBean();
                    if(rs.getString(1) != null)
                        pb.setPublicationId(Integer.parseInt(rs.getString(1)));
                    if(rs.getString(3) != null)
                        pb.setScopusDocId(rs.getString(3));
                    if(rs.getString(5) != null)
                        pb.setJournalHash(rs.getString(5));
                    if(rs.getString(6) != null)
                        pb.setJournal(rs.getString(6));
                    if(rs.getString(7) != null && rs.getString(8) != null) {
                        AuthorBean author = new AuthorBean();
                        author.setAuthorshipRank(Integer.parseInt(rs.getString(7)));
                        author.setCwid(rs.getString(8));
                        Set<AuthorBean> authors = new HashSet<>();
                        authors.add(author);
                        pb.setAuthorList(authors);
                    }
                    if(rs.getString(9) != null)
                        pb.setIssn(rs.getString(9));
                    if(rs.getString(10) != null)
                        pb.setDoi(rs.getString(10));
                    if(rs.getString(11) != null)
                        pb.setIssue(rs.getString(11));
                    if(rs.getString(12) != null)
                        pb.setVolume(rs.getString(12));
                    if(rs.getString(13) != null)
                        pb.setPages(rs.getString(13));
                    if(rs.getString(14) != null)
                        pb.setCoverDate(rs.getString(14));
                    if(rs.getString(15) != null)
                        pb.setDatePrecision(rs.getString(15));
                    if(rs.getString(16) != null)
                        pb.setTitle(rs.getString(16));
                    
                    this.scopusOnlyPubs.add(pb);
                    
                }
                log.info("Stay here");
        }
        catch(SQLException e) {
            log.error("SQLException" , e);
        }
        finally {
            try{
                if(ps!=null)
                    ps.close();
                if(rs!=null)
                    rs.close();
                if(con != null)
                    this.mycf.returnConnectionToPool(con);
            }
            catch(Exception e) {
                log.error("Exception",e);
            }
            
        }
        importPublications(peopleCwids);
    }

    public void importPublications(List<String> peopleCwids) {
        SDBJenaConnect vivoJena = this.jcf.getConnectionfromPool("dataSet");
        StopWatch stopWatch = new StopWatch("Publications import to VIVO");
        stopWatch.start("Publications import to VIVO");
        StringBuilder sb = new StringBuilder();
        for (PublicationBean articleFeature : this.scopusOnlyPubs) {
            if(articleFeature.getAuthorList() != null && !articleFeature.getAuthorList().isEmpty()
                &&
                peopleCwids.contains(articleFeature.getAuthorList().iterator().next().getCwid())) {
                log.info("Importing scopus publication with scopusDocId: " + articleFeature.getScopusDocId());
                sb.append(QueryConstants.getSparqlPrefixQuery());
                sb.append("INSERT DATA { GRAPH <" + VivoGraphs.DEFAULT_KB_2_GRAPH + "> { \n");
                final String publicationUrl = "<" + JenaConnectionFactory.nameSpace + "pubidnn" + articleFeature.getScopusDocId()
                        + ">";
                sb.append(publicationUrl + " core:DateTimeValue \"" + this.sdf.format(new Date()) + "\" . \n");
                sb.append(publicationUrl + " rdf:type obo:BFO_0000001 . \n");
                sb.append(publicationUrl + " rdf:type obo:BFO_0000002 . \n");
                sb.append(publicationUrl + " rdf:type obo:IAO_0000030 . \n");
                sb.append(publicationUrl + " rdf:type obo:BFO_0000031 . \n");
                // articleTitle → rdfs:label
                if (articleFeature.getTitle() != null) {
                    sb.append(publicationUrl + " rdfs:label \""
                            + articleFeature.getTitle().trim().replaceAll("([\\\\\\\\\"])", "\\\\$1").replaceAll("'", "\'").replaceAll("\"","\\\"").replaceAll("(\\r|\\n|\\r\\n)+", "\\\\n")
                            + "\" .\n");
                }
                // doi → bibo:doi
                if (articleFeature.getDoi() != null && !articleFeature.getDoi().isEmpty()) {
                    sb.append(publicationUrl + " bibo:doi \"" + articleFeature.getDoi().trim()
                            .replaceAll("([\\\\\\\\\"])", "\\\\$1").replaceAll("\\s+", "") + "\" .\n");
                }

                // issue → bibo:issue
                if (articleFeature.getIssue() != null && !articleFeature.getIssue().trim().isEmpty()) {
                    sb.append(publicationUrl + " bibo:issue \"" + articleFeature.getIssue() + "\" .\n");
                }
                // scopusDocID → wcmc:scopusDocId
                if (articleFeature.getScopusDocId() != null) {
                    sb.append(publicationUrl + " wcmc:scopusDocId \"" + articleFeature.getScopusDocId() + "\" .\n");
                }
                // volume → bibo:volume
                if (articleFeature.getVolume() != null && !articleFeature.getVolume().trim().isEmpty()) {
                    sb.append(publicationUrl + " bibo:volume \"" + articleFeature.getVolume() + "\" .\n");
                }
                // pages -> bibo:pages
                if (articleFeature.getPages() != null && !articleFeature.getPages().trim().isEmpty()) {
                    sb.append(publicationUrl + " bibo:pages \"" + articleFeature.getPages() + "\" .\n");
                }

                // Cover Date
                if (articleFeature.getCoverDate() != null) {
                    Date standardDate = null;
                    try {
                        standardDate = this.sdf.parse(articleFeature.getCoverDate());
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
                                + articleFeature.getCoverDate()
                                + "T00:00:00\"^^<http://www.w3.org/2001/XMLSchema#dateTime> . \n");
                    }
                }
                // Journal
                String journalIdentifier = articleFeature.getJournalHash();
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
                        + journalIdentifier + "> core:title \"" + articleFeature.getJournal().replaceAll("'", "\'").replaceAll("\"","\\\\\"") + "\" . \n");
                sb.append("<" + JenaConnectionFactory.nameSpace + "journal"
                        + journalIdentifier + "> rdfs:label \"" + articleFeature.getJournal().replaceAll("'", "\'").replaceAll("\"","\\\\\"") + "\" . \n");
                
                //Issn
                if(articleFeature.getIssn() != null && !articleFeature.getIssn().isEmpty()) {
                    sb.append("<" + JenaConnectionFactory.nameSpace + "journal"
                    + journalIdentifier + "> bibo:issn \"" + articleFeature.getIssn()
                    + "\" . \n");
                }
                sb.append("<" + JenaConnectionFactory.nameSpace + "journal"
                        + journalIdentifier + "> <http://vivoweb.org/ontology/core#publicationVenueFor> " + publicationUrl + " . \n");

                //Publication Type
                sb.append(publicationUrl + " rdf:type core:InformationResource . \n");
                sb.append(publicationUrl + " rdf:type bibo:Document . \n");
                sb.append(publicationUrl + " rdf:type bibo:Article . \n");
                sb.append(publicationUrl + " vitro:mostSpecificType bibo:Article . \n");
                sb.append(publicationUrl + " rdf:type <http://www.w3.org/2002/07/owl#Thing> . \n");
                    
                //Author Assignment
                if(articleFeature.getAuthorList() != null && !articleFeature.getAuthorList().isEmpty()) {
                    for(AuthorBean author : articleFeature.getAuthorList()) {
                        sb.append(publicationUrl + " core:relatedBy <" + JenaConnectionFactory.nameSpace + "pubidnn" + articleFeature.getScopusDocId() + "authorship" + author.getAuthorshipRank() + "> . \n");
                        sb.append("<" + JenaConnectionFactory.nameSpace + "pubidnn" + articleFeature.getScopusDocId() + "authorship" + author.getAuthorshipRank() + "> rdf:type obo:BFO_0000001 . \n");
                        sb.append("<" + JenaConnectionFactory.nameSpace + "pubidnn" + articleFeature.getScopusDocId() + "authorship" + author.getAuthorshipRank() + "> rdf:type obo:BFO_0000002 . \n");
                        sb.append("<" + JenaConnectionFactory.nameSpace + "pubidnn" + articleFeature.getScopusDocId() + "authorship" + author.getAuthorshipRank() + "> rdf:type obo:BFO_0000020 . \n");
                        sb.append("<" + JenaConnectionFactory.nameSpace + "pubidnn" + articleFeature.getScopusDocId() + "authorship" + author.getAuthorshipRank() + "> rdf:type <http://www.w3.org/2002/07/owl#Thing> . \n");
                        sb.append("<" + JenaConnectionFactory.nameSpace + "pubidnn" + articleFeature.getScopusDocId() + "authorship" + author.getAuthorshipRank() + "> rdf:type core:Relationship . \n");
                        sb.append("<" + JenaConnectionFactory.nameSpace + "pubidnn" + articleFeature.getScopusDocId() + "authorship" + author.getAuthorshipRank() + "> rdf:type core:Authorship . \n");
                        sb.append("<" + JenaConnectionFactory.nameSpace + "pubidnn" + articleFeature.getScopusDocId() + "authorship" + author.getAuthorshipRank() + "> vitro:mostSpecificType core:Authorship . \n");
                        sb.append("<" + JenaConnectionFactory.nameSpace + "pubidnn" + articleFeature.getScopusDocId() + "authorship" + author.getAuthorshipRank() + "> core:relates " + publicationUrl + " . \n");
                        sb.append("<" + JenaConnectionFactory.nameSpace + "pubidnn" + articleFeature.getScopusDocId() + "authorship" + author.getAuthorshipRank() + "> core:rank \"" + author.getAuthorshipRank() + "\"^^xsd:integer . \n");
                        //Linking vcard of the person
                        sb.append("<" + JenaConnectionFactory.nameSpace + "cwid-" + author.getCwid() + "> rdf:type foaf:Person . \n");
                        sb.append("<" + JenaConnectionFactory.nameSpace + "cwid-" + author.getCwid() + "> core:relatedBy <" + JenaConnectionFactory.nameSpace + "pubidnn" + articleFeature.getScopusDocId() + "authorship" + author.getAuthorshipRank() + "> . \n");
                        sb.append("<" + JenaConnectionFactory.nameSpace + "pubidnn" + articleFeature.getScopusDocId() + "authorship" + author.getAuthorshipRank() + "> core:relates <" + JenaConnectionFactory.nameSpace + "cwid-" + author.getCwid() + "> . \n");
                        sb.append("<" + JenaConnectionFactory.nameSpace + "pubidnn" + articleFeature.getScopusDocId() + "authorship" + author.getAuthorshipRank() + "> core:relates <" + JenaConnectionFactory.nameSpace + "arg2000028-" + author.getCwid() + "> . \n");
                        sb.append("<" + JenaConnectionFactory.nameSpace + "arg2000028-" + author.getCwid() + "> obo:ARG_2000029 <" + JenaConnectionFactory.nameSpace + "pubidnn" + articleFeature.getScopusDocId() + "authorship" + author.getAuthorshipRank() + "> . \n");
                        sb.append("<" + JenaConnectionFactory.nameSpace + "arg2000028-" + author.getCwid() + "> core:relatedBy <" + JenaConnectionFactory.nameSpace + "pubidnn" + articleFeature.getScopusDocId() + "authorship" + author.getAuthorshipRank() + "> . \n");
                        sb.append("<" + JenaConnectionFactory.nameSpace + "pubidnn" + articleFeature.getScopusDocId() + "authorship" + author.getAuthorshipRank() + "> obo:ARG_2000028 <" + JenaConnectionFactory.nameSpace + "arg2000028-" + author.getCwid() + "> . \n");
                    }
                }
                sb.append(publicationUrl + " <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"ReCiter Connect\" . \n");
                sb.append("}}");
                //log.info(sb.toString());
                
                /*try {
                    String response = this.vivoClient.vivoUpdateApi(sb.toString());
                    log.info(response);
                } catch(Exception e) {
                    log.error("Error connecting to SDBJena", e);
                }*/
                 
                try {
                    vivoJena.executeUpdateQuery(sb.toString(), true);
                } catch(IOException e) {
                    log.error("Error connecting to SDBJena");
                }
                catch(QueryParseException qpe) {
                    log.error("QueryParseException", qpe);
                }
                sb.setLength(0);

               
            } else {
                log.info("Not Importing scopus publication with scopusDocId: " + articleFeature.getScopusDocId());
            }
            
        }
        
        
        stopWatch.stop();
        log.info("Publication import took " + stopWatch.getTotalTimeSeconds()+"s");
        this.jcf.returnConnectionToPool(vivoJena, "dataSet");

    }

}
