package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.MapFunction;

import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticleTokens;

public class ArticleTokensToDocumentLengthMap implements MapFunction<NewsArticleTokens,Integer>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 6901480832981785124L;

	@Override
	public Integer call(NewsArticleTokens value) throws Exception {
		return value.getArticleLength();
	}

}
