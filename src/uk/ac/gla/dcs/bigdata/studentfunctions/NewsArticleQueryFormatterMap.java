package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;

import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticleTokens;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticleWithQueries;
import uk.ac.gla.dcs.bigdata.studentstructures.QueryArticleInfo;

public class NewsArticleQueryFormatterMap implements MapFunction<NewsArticleTokens, NewsArticleWithQueries> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -5517041744621945450L;
	Broadcast<List<Query>> broadcastQueries;

	public NewsArticleQueryFormatterMap(Broadcast<List<Query>> broadcastQueries) {
		this.broadcastQueries = broadcastQueries;
	}

	@Override
	public NewsArticleWithQueries call(NewsArticleTokens value) throws Exception {
		List<QueryArticleInfo> queriesList = new ArrayList<QueryArticleInfo>();
		List<String> termList;
		List<Integer> termFrequency;

		// Storing the broadcasted value for the list of queries.
		List<Query> queryList = broadcastQueries.value();
		for (Query queryItem : queryList) {
			// Creating a new term list for the each of the query.
			termList = new ArrayList<String>();
			// Creating a new term frequency list for the each of the query.
			termFrequency = new ArrayList<Integer>();
			for (String term : queryItem.getQueryTerms()) {
				// Adding the term in the term list.
				termList.add(term);
				// Checking if token present in the news article tokens list.
				if (value.getTokens().contains(term)) {
					// Counting the no. of times the term comes in the token list of the article and
					// adding it the term frequency list.
					termFrequency.add(Collections.frequency(value.getTokens(), term));
				} else {
					// Adding 0 if the term not present in the news article token list
					termFrequency.add(0);
				}
			}
			// Adding in the list as a new object which stores the query, its respective
			// terms and the term frequency for each of them for the news article.
			queriesList.add(new QueryArticleInfo(queryItem, termList, termFrequency));

		}

		return new NewsArticleWithQueries(value.getArticle(), queriesList);
	}

}
