package uk.ac.gla.dcs.bigdata.studentstructures;

import java.io.Serializable;
import java.util.List;

import uk.ac.gla.dcs.bigdata.providedstructures.Query;

public class QueryArticleInfo implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7527511785419710826L;
	
	Query originalQuery;
	List<String> termList;
	List<Integer> termFrequency;

	public QueryArticleInfo() {}

	public QueryArticleInfo(Query originalQuery, List<String> termList, List<Integer> termFrequency) {
		super();
		this.originalQuery = originalQuery;
		this.termList = termList;
		this.termFrequency = termFrequency;
	}

	public Query getOriginalQuery() {
		return originalQuery;
	}

	public void setOriginalQuery(Query originalQuery) {
		this.originalQuery = originalQuery;
	}

	public List<String> getTermList() {
		return termList;
	}

	public void setTermList(List<String> termList) {
		this.termList = termList;
	}

	public List<Integer> getTermFrequency() {
		return termFrequency;
	}

	public void setTermFrequency(List<Integer> termFrequency) {
		this.termFrequency = termFrequency;
	}

}
