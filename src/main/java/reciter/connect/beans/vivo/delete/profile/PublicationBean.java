package reciter.connect.beans.vivo.delete.profile;

public class PublicationBean {
	
	private String pubUrl;
	private String authorshipUrl;
	private String authorUrl;
	private boolean additionalWcmcAuthorFlag = false;
	
	
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
	public boolean isAdditionalWcmcAuthorFlag() {
		return additionalWcmcAuthorFlag;
	}
	public void setAdditionalWcmcAuthorFlag(boolean additionalWcmcAuthorFlag) {
		this.additionalWcmcAuthorFlag = additionalWcmcAuthorFlag;
	}
	
	@Override
	public String toString() {
		return this.pubUrl + " " + this.authorshipUrl + " " + this.additionalWcmcAuthorFlag;
	}
}

