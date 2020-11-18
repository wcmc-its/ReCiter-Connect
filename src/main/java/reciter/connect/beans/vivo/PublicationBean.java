package reciter.connect.beans.vivo;

import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

/**
 * @author szd2013
 * <p> This is the publication bean class containing all the information pertaining to publication <p>
 */
@AllArgsConstructor
@NoArgsConstructor
public class PublicationBean {
	
	private int publicationId;
	private String scopusDocId;
	private int pmid;
	private String pmcid;
	private String doi;
	private String journal;
	private String journalHash;
	private String coverDate;
	private String datePrecision;
	private String pages;
	private String volume;
	private String issue;
	private int citationCount;
	private String issn;
	private String eissn;
	private String nlmabbreviation;
	private String publicationAbstract;
	private Set<String> meshMajor;
	private String language;
	private Set<String> funding;
	private Set<String> pubtype;
	private String isbn10;
	private String isbn13;
	private String title;
	public String vivoPubTypeDeduction;
	private String pubmedXmlContent = null;
	private Set<AuthorBean> authorList;
	
	private static Logger log = LoggerFactory.getLogger(PublicationBean.class);
	
	
	public Set<AuthorBean> getAuthorList() {
		return authorList;
	}
	public void setAuthorList(Set<AuthorBean> authorList) {
		this.authorList = authorList;
	}
	public int getPublicationId() {
		return publicationId;
	}
	public void setPublicationId(int publicationId) {
		this.publicationId = publicationId;
	}
	public String getScopusDocId() {
		return scopusDocId;
	}
	public void setScopusDocId(String scopusDocId) {
		this.scopusDocId = scopusDocId;
	}
	public int getPmid() {
		return pmid;
	}
	public void setPmid(int pmid) {
		this.pmid = pmid;
	}
	public String getPmcid() {
		return pmcid;
	}
	public void setPmcid(String pmcid) {
		this.pmcid = pmcid;
	}
	public String getDoi() {
		return doi;
	}
	public void setDoi(String doi) {
		this.doi = doi;
	}
	public String getJournal() {
		return journal;
	}
	public void setJournal(String journal) {
		this.journal = journal;
	}
	public String getCoverDate() {
		return coverDate;
	}
	public void setCoverDate(String coverDate) {
		this.coverDate = coverDate;
	}
	public String getPages() {
		return pages;
	}
	public void setPages(String pages) {
		this.pages = pages;
	}
	public String getVolume() {
		return volume;
	}
	public void setVolume(String volume) {
		this.volume = volume;
	}
	public String getIssue() {
		return issue;
	}
	public void setIssue(String issue) {
		this.issue = issue;
	}
	public int getCitationCount() {
		return citationCount;
	}
	public void setCitationCount(int citationCount) {
		this.citationCount = citationCount;
	}
	public String getIssn() {
		return issn;
	}
	public void setIssn(String issn) {
		this.issn = issn;
	}
	public String getEissn() {
		return eissn;
	}
	public void setEissn(String eissn) {
		this.eissn = eissn;
	}
	public String getNlmabbreviation() {
		return nlmabbreviation;
	}
	public void setNlmabbreviation(String nlmabbreviation) {
		this.nlmabbreviation = nlmabbreviation;
	}
	public String getPublicationAbstract() {
		return publicationAbstract;
	}
	public void setPublicationAbstract(String publicationAbstract) {
		this.publicationAbstract = publicationAbstract;
	}
	public Set<String> getMeshMajor() {
		return meshMajor;
	}
	public void setMeshMajor(Set<String> meshMajor) {
		this.meshMajor = meshMajor;
	}
	public String getLanguage() {
		return language;
	}
	public void setLanguage(String language) {
		this.language = language;
	}
	public Set<String> getFunding() {
		return funding;
	}
	public void setFunding(Set<String> funding) {
		this.funding = funding;
	}
	public Set<String> getPubtype() {
		return pubtype;
	}
	public void setPubtype(Set<String> pubtype) {
		this.pubtype = pubtype;
	}
	public String getIsbn10() {
		return isbn10;
	}
	public void setIsbn10(String isbn10) {
		this.isbn10 = isbn10;
	}
	public String getIsbn13() {
		return isbn13;
	}
	public void setIsbn13(String isbn13) {
		this.isbn13 = isbn13;
	}
	public String getTitle() {
		return title;
	}
	public void setTitle(String title) {
		this.title = title;
	}
	public String getVivoPubTypeDeduction() {
		return vivoPubTypeDeduction;
	}
	public void setVivoPubTypeDeduction(String vivoPubTypeDeduction) {
		this.vivoPubTypeDeduction = vivoPubTypeDeduction;
	}
	
	public String getPubmedXmlContent() {
		return pubmedXmlContent;
	}
	public void setPubmedXmlContent(String pubmedXmlContent) {
		this.pubmedXmlContent = pubmedXmlContent;
	}
	public String getPubtypeStr() {
		String str = null;
		if (this.pubtype.size() > 0) {
			str = StringUtils.join(this.pubtype, '|');
		}
		return str;
	}
	
	
	public void printPublication() {
		log.info("***********************************");
		log.info("Scopus Doc ID: " + this.scopusDocId);
		log.info("Publication Title: " + this.title);
		log.info("Publication Type: " + getPubtypeStr());
		log.info("Authors: ");
		for(AuthorBean ab: this.authorList) {
			log.info("Authorship Pk - " + ab.getAuthorshipPk() + " Rank - " + ab.getAuthorshipRank() + " - " + ab.getAuthName() + " CWID - " + ab.getCwid());
		}
		log.info("***********************************");
	}
	
	

    /**
     * @return String return the journalHash
     */
    public String getJournalHash() {
        return journalHash;
    }

    /**
     * @param journalHash the journalHash to set
     */
    public void setJournalHash(String journalHash) {
        this.journalHash = journalHash;
    }

    /**
     * @return String return the datePrecision
     */
    public String getDatePrecision() {
        return datePrecision;
    }

    /**
     * @param datePrecision the datePrecision to set
     */
    public void setDatePrecision(String datePrecision) {
        this.datePrecision = datePrecision;
    }

}
