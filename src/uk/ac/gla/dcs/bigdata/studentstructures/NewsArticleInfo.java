package uk.ac.gla.dcs.bigdata.studentstructures;

import java.io.Serializable;
import java.util.List;

import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;


public class NewsArticleInfo implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5457542745305954843L;

	List<String> paragraph; // the contents of the article body
	NewsArticle article;
	
	public NewsArticleInfo() {}
	
	public NewsArticleInfo(NewsArticle article,List<String> paragraph) {
		this.paragraph = paragraph;
		this.article = article;
	}
	
	public NewsArticle getArticle() {
		return article;
	}

	public void setArticle(NewsArticle article) {
		this.article = article;
	}

	public List<String> getParagraph() {
		return paragraph;
	}

	public void setParagraph(List<String> paragraph) {
		this.paragraph = paragraph;
	}

}
