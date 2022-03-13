package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.MapFunction;

import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticleWithQueries;
import uk.ac.gla.dcs.bigdata.studentstructures.QueryTermList;

public class QueryFrequencyMap implements MapFunction<NewsArticleWithQueries,QueryTermList> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -5478335522018272478L;

	@Override
	public QueryTermList call(NewsArticleWithQueries value) throws Exception {
		//Returning the QueryTermList object which contains the list of the queries, their respective term list and frequencies
		return new QueryTermList(value.getQueriesList());
	}

}
