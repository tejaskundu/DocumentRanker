package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;

import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;
import uk.ac.gla.dcs.bigdata.providedutilities.DPHScorer;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticleTokens;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticleWithQueries;
import uk.ac.gla.dcs.bigdata.studentstructures.QueryArticleInfo;

public class ArticleDPHScoreMap implements MapFunction<QueryArticleInfo, DocumentRanking> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -613020992629066679L;

	Broadcast<Double> broadcastAvgDocLenCorpus;
	Broadcast<Long> broadcastTotalCorpusDocs;
	Broadcast<List<NewsArticleTokens>> broadcastDocLen;
	Broadcast<List<NewsArticleWithQueries>> broadcastDocTF;

	public ArticleDPHScoreMap(Broadcast<Double> broadcastAvgDocLenCorpus, Broadcast<Long> broadcastTotalCorpusDocs,
			Broadcast<List<NewsArticleTokens>> broadcastDocLen,
			Broadcast<List<NewsArticleWithQueries>> broadcastDocTF) {
		this.broadcastAvgDocLenCorpus = broadcastAvgDocLenCorpus;
		this.broadcastTotalCorpusDocs = broadcastTotalCorpusDocs;
		this.broadcastDocLen = broadcastDocLen;
		this.broadcastDocTF = broadcastDocTF;
	}

	@Override
	public DocumentRanking call(QueryArticleInfo value) throws Exception {
		Query currentQuery = value.getOriginalQuery();
		List<RankedResult> articleScores = new ArrayList<>();

		// Get average length of documents in corpus
		double avgDocLenInCorpus = broadcastAvgDocLenCorpus.value();

		// Get total number of documents in corpus
		long totalDocsInCorpus = broadcastTotalCorpusDocs.value();

		// Get list of documents mapped with queries, that will give term frequency for each query for score calculation
		List<NewsArticleWithQueries> articles = broadcastDocTF.value();
		
		for (NewsArticleWithQueries obj : articles) {
			double avgScore = 0.0;
			
			// Get total term frequency for the current query
			QueryArticleInfo queryInfo = obj.getQueriesList().stream().filter(
					query -> query.getOriginalQuery().getOriginalQuery().equals(currentQuery.getOriginalQuery()))
					.collect(Collectors.toList()).get(0);
			
			// Get total length of current document
			int currDocLen = broadcastDocLen.value().stream()
					.filter(article -> article.getArticle().getId().equals(obj.getOriginalArticle().getId()))
					.collect(Collectors.toList()).get(0).getArticleLength();
			
			// Get size of the terms in current query
			int size = queryInfo.getTermList().size();
			
			for (int i = 0; i < size; i++) {
				double score = 0.0;
				
				// Get term frequency of the term in current document
				short tfInCurrDoc = queryInfo.getTermFrequency().get(i).shortValue();
				
				// Calculate DPH score for the article
				try {
					score = DPHScorer.getDPHScore(tfInCurrDoc, value.getTermFrequency().get(i), currDocLen,
							avgDocLenInCorpus, totalDocsInCorpus);
				} catch (ArithmeticException e) {
					score = 0.0;
				}
				
				// Check if the score calculated is invalid
				if (Double.isNaN(score) || Double.isInfinite(score))
					score = 0.0;
				
				// Sum score to get score across all query terms in the current query for the current document 
				avgScore = score + avgScore;
			}
			
			// Calculate average score of document against the query
			double averageScore = (1.0 * avgScore) / size;
			
			// Check if score not zero, and only then add the document to the result list 
			if (averageScore != 0.0) {
				RankedResult result = new RankedResult(obj.getOriginalArticle().getId(), obj.getOriginalArticle(),
						averageScore);
				articleScores.add(result);
			}
		}
		
		// Sorting in descending order of DPH score
		Collections.sort(articleScores);
		Collections.reverse(articleScores);
		
		return new DocumentRanking(currentQuery, articleScores);
	}

}
