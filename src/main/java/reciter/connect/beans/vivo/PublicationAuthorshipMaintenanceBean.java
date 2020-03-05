package reciter.connect.beans.vivo;

import java.util.Set;

import reciter.connect.beans.vivo.AuthorBean;

public class PublicationAuthorshipMaintenanceBean {
	
	private int wcmcDocumentPk;
	private String pubUrl;
	private String authorshipUrl;
	private String authorUrl;
	
	public int getWcmcDocumentPk() {
		return wcmcDocumentPk;
	}
	public void setWcmcDocumentPk(int wcmcDocumentPk) {
		this.wcmcDocumentPk = wcmcDocumentPk;
	}
	public String getAuthorUrl() {
		return authorUrl;
	}
	public void setAuthorUrl(String authorUrl) {
		this.authorUrl = authorUrl;
	}
	public String getPubUrl() {
		return pubUrl;
	}
	public void setPubUrl(String pubUrl) {
		this.pubUrl = pubUrl;
	}
	public String getAuthorshipUrl() {
		return authorshipUrl;
	}
	public void setAuthorshipUrl(String authorshipUrl) {
		this.authorshipUrl = authorshipUrl;
	}
	
	@Override
	public String toString() {
		return this.pubUrl + " " + this.authorshipUrl + " " + " " + this.authorUrl;
	}
	
}
	