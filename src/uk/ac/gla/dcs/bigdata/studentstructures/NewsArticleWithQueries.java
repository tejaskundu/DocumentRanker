package uk.ac.gla.dcs.bigdata.studentstructures;

import java.io.Serializable;
import java.util.List;

import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;

public class NewsArticleWithQueries implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7307169617045242330L;

	NewsArticle originalArticle; //The original NewsArticle object
	List<QueryArticleInfo> queriesList; //List storing the term list and term frequency for all the queries for the news article.

	public NewsArticleWithQueries() {
	}

	public NewsArticleWithQueries(NewsArticle originalArticle, List<QueryArticleInfo> queriesList) {
		super();
		this.originalArticle = originalArticle;
		this.queriesList = queriesList;
	}

	public NewsArticle getOriginalArticle() {
		return originalArticle;
	}

	public void setOriginalArticle(NewsArticle originalArticle) {
		this.originalArticle = originalArticle;
	}

	public List<QueryArticleInfo> getQueriesList() {
		return queriesList;
	}

	public void setQueriesList(List<QueryArticleInfo> queriesList) {
		this.queriesList = queriesList;
	}

}
