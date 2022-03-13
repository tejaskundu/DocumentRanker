package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.function.ReduceFunction;

import uk.ac.gla.dcs.bigdata.studentstructures.QueryArticleInfo;
import uk.ac.gla.dcs.bigdata.studentstructures.QueryTermList;

public class QueryTermListReducer implements ReduceFunction<QueryTermList> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -2311470777653119501L;

	@Override
	public QueryTermList call(QueryTermList v1, QueryTermList v2) throws Exception {
		List<QueryArticleInfo> queriesList = new ArrayList<QueryArticleInfo>();
		// Storing the term list of each query
		List<String> termList;
		// Storing the term frequency of the respective term for each query
		List<Integer> termFrequency;

		List<QueryArticleInfo> querySet1 = v1.getQueryTermList();
		List<QueryArticleInfo> querySet2 = v2.getQueryTermList();

		for (QueryArticleInfo item : querySet1) {
			termList = new ArrayList<String>();
			termFrequency = new ArrayList<Integer>();
			QueryArticleInfo item2 = querySet2.get(querySet1.indexOf(item));
			// Iterating each term and adding the term frequency for that term for each
			// QueryTermList object
			for (String term : item.getTermList()) {
				termList.add(term);
				int termIndex = item.getTermList().indexOf(term);
				termFrequency.add(item.getTermFrequency().get(termIndex) + item2.getTermFrequency().get(termIndex));
			}
			// Creating a new QueryArticleInfo object with updated term frequency and adding
			// it to the queries list
			queriesList.add(new QueryArticleInfo(item.getOriginalQuery(), termList, termFrequency));
		}
		// Creating a new updated QueryTermList object and returning it.
		return new QueryTermList(queriesList);
	}

}
