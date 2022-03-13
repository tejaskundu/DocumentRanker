package uk.ac.gla.dcs.bigdata.studentstructures;

import java.io.Serializable;
import java.util.List;

import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;

public class NewsArticleTokens implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -59592287447189311L;

	List<String> tokens; // the token for the contents of the article body
	NewsArticle article; // The original NewsArticle itself
	int articleLength; // Length of the current document(article)

	public NewsArticleTokens() {
	}

	public NewsArticleTokens(NewsArticle article, List<String> tokens, int articleLenght) {
		this.tokens = tokens;
		this.article = article;
		this.articleLength = articleLenght;
	}

	public int getArticleLength() {
		return articleLength;
	}

	public void setArticleLength(int articleLength) {
		this.articleLength = articleLength;
	}

	public NewsArticle getArticle() {
		return article;
	}

	public void setArticle(NewsArticle article) {
		this.article = article;
	}

	public List<String> getTokens() {
		return tokens;
	}

	public void setTokens(List<String> tokens) {
		this.tokens = tokens;
	}

}
