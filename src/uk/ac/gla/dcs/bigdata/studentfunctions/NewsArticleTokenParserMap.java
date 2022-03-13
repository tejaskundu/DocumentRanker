package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.function.MapFunction;

import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticleInfo;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticleTokens;

public class NewsArticleTokenParserMap implements MapFunction<NewsArticleInfo, NewsArticleTokens> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 976001320133002067L;

	public NewsArticleTokenParserMap() {}

	@Override
	public NewsArticleTokens call(NewsArticleInfo value) throws Exception {
		// Creating the object for the provided TextPreProcessor
		TextPreProcessor processor = new TextPreProcessor();
		List<String> tokens = new ArrayList<String>();

		List<String> paragraphs = value.getParagraph();
		for (String para : paragraphs) {
			List<String> outputTokens = processor.process(para);
			// Checking if the result tokens list is not zero
			if (outputTokens.size() != 0)
				// Adding all the result tokens in the final token list for that news article.
				tokens.addAll(outputTokens);
		}
		return new NewsArticleTokens(value.getArticle(), tokens, tokens.size());
	}

}
