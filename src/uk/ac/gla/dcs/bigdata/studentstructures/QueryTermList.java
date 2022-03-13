package uk.ac.gla.dcs.bigdata.studentstructures;

import java.io.Serializable;
import java.util.List;

public class QueryTermList implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -55692001707102953L;

	List<QueryArticleInfo> queryTermList;

	public QueryTermList() {}

	public QueryTermList(List<QueryArticleInfo> queryTermList) {
		super();
		this.queryTermList = queryTermList;
	}

	public List<QueryArticleInfo> getQueryTermList() {
		return queryTermList;
	}

	public void setQueryTermList(List<QueryArticleInfo> queryTermList) {
		this.queryTermList = queryTermList;
	}

}
