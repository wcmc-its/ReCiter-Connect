package reciter.connect.beans.vivo;

import lombok.Data;

/**
 * @author szd2013
 * <p><b><i> This author bean class stores all information related to WCMC authors <p><b><i>
 */
@Data
public class AuthorBean {
	
	private int authorshipPk;
	private String cwid;
	private String authName;
	private String givenName;
	private String surname;
	private String initials;
	private Long scopusAuthorId;
	private int authorshipRank;
}
