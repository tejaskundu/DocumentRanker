package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.function.MapFunction;

import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticleInfo;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticleTokens;

public class NewsArticleParagraphFormatterMap implements MapFunction<NewsArticle,NewsArticleInfo>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 8865044520745466199L;
	
	ArrayList<String> paragraph; // the contents of the article body

	@Override
	public NewsArticleInfo call(NewsArticle t) throws Exception {
		paragraph = new ArrayList<String>();
		int noOfPargraphs = 0;

		try {
			if(t.getTitle() != null) {
			paragraph.add(t.getTitle()); //Appending title along with the contents
			List<ContentItem> Content = t.getContents();
			for (ContentItem content : Content) {
				if (content != null) {
					//selecting elements that have a non-null subtype and that subtype is listed as paragraphs
					//storing first 5 paragraphs in paragraph
					if (content.getSubtype() != null && content.getSubtype().equalsIgnoreCase("paragraph")
							&& noOfPargraphs <= 4) {
						paragraph.add(content.getContent());
						noOfPargraphs++;
					}
				}

			}
			}
		} catch (NullPointerException e) {
			return new NewsArticleInfo(t, paragraph);
		}

		return new NewsArticleInfo(t, paragraph);
	}

}
