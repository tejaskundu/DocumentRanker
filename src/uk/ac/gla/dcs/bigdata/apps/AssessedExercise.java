package uk.ac.gla.dcs.bigdata.apps;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import uk.ac.gla.dcs.bigdata.providedfunctions.NewsFormaterMap;
import uk.ac.gla.dcs.bigdata.providedfunctions.QueryFormaterMap;
import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.studentfunctions.ArticleDPHScoreMap;
import uk.ac.gla.dcs.bigdata.studentfunctions.ArticleTokensToDocumentLengthMap;
import uk.ac.gla.dcs.bigdata.studentfunctions.DistanceBetweenArticleMap;
import uk.ac.gla.dcs.bigdata.studentfunctions.IntSumReducer;
import uk.ac.gla.dcs.bigdata.studentfunctions.NewsArticleParagraphFormatterMap;
import uk.ac.gla.dcs.bigdata.studentfunctions.NewsArticleQueryFormatterMap;
import uk.ac.gla.dcs.bigdata.studentfunctions.NewsArticleTokenParserMap;
import uk.ac.gla.dcs.bigdata.studentfunctions.QueryFrequencyMap;
import uk.ac.gla.dcs.bigdata.studentfunctions.QueryTermListReducer;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticleInfo;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticleTokens;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticleWithQueries;
import uk.ac.gla.dcs.bigdata.studentstructures.QueryArticleInfo;
import uk.ac.gla.dcs.bigdata.studentstructures.QueryTermList;

/**
 * This is the main class where your Spark topology should be specified.
 * 
 * By default, running this class will execute the topology defined in the
 * rankDocuments() method in local mode, although this may be overriden by the
 * spark.master environment variable.
 * 
 * @author Richard
 *
 */
public class AssessedExercise {

	public static void main(String[] args) {
		// The code submitted for the assessed exerise may be run in either local or
		// remote modes
		// Configuration of this will be performed based on an environment variable
		String sparkMasterDef = System.getenv("SPARK_MASTER");
		if (sparkMasterDef == null) {
			File hadoopDIR = new File("resources/hadoop/"); // represent the hadoop directory as a Java file so we can
															// get an absolute path for it
			System.setProperty("hadoop.home.dir", hadoopDIR.getAbsolutePath()); // set the JVM system property so that
																				// Spark finds it
			sparkMasterDef = "local[2]"; // default is local mode with two executors
		}

		String sparkSessionName = "BigDataAE"; // give the session a name

		// Create the Spark Configuration
		SparkConf conf = new SparkConf().setMaster(sparkMasterDef).setAppName(sparkSessionName);

		// Create the spark session
		SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

		// Get the location of the remote input queries
		String queryFile = System.getenv("BIGDATA_QUERIES");
		if (queryFile == null)
			// Get the location of the local input queries
			queryFile = "data/queries.list"; // default is a sample with 3 queries

		// Get the location of the remote input news articles
		String newsFile = System.getenv("BIGDATA_NEWS");
		if (newsFile == null)
			// Get the location of the local input news articles
			newsFile = "data/TREC_Washington_Post_collection.v3.example.json"; // default is a sample of 5000 news
																				// articles

		// Call the student's code
		List<DocumentRanking> results = rankDocuments(spark, queryFile, newsFile);

		// Close the spark session
		spark.close();

		String out = System.getenv("BIGDATA_RESULTS");
		String resultsDIR = "results/";
		if (out != null)
			resultsDIR = out;

		// Check if the code returned any results
		if (results == null)
			System.err
					.println("Topology return no rankings, student code may not be implemented, skiping final write.");
		else {

			// Write the ranking for each query as a new file
			for (DocumentRanking rankingForQuery : results) {
				rankingForQuery.write(new File(resultsDIR).getAbsolutePath());
			}
		}

		try {
			BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream(new File(resultsDIR).getAbsolutePath() + "/SPARK.DONE")));
			writer.write(String.valueOf(System.currentTimeMillis()));
			writer.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public static List<DocumentRanking> rankDocuments(SparkSession spark, String queryFile, String newsFile) {

		// Load queries and news articles
		Dataset<Row> queriesjson = spark.read().text(queryFile);
		Dataset<Row> newsjson = spark.read().text(newsFile); // read in files as string rows, one row per article

		newsjson = newsjson.repartition(24);

		// Perform an initial conversion from Dataset<Row> to Query and NewsArticle Java
		// objects
		Dataset<Query> queries = queriesjson.map(new QueryFormaterMap(), Encoders.bean(Query.class)); // this converts
																										// each row into
																										// a Query
		Dataset<NewsArticle> news = newsjson.map(new NewsFormaterMap(), Encoders.bean(NewsArticle.class)); // this
																											// converts
																											// each row
																											// into a
																											// NewsArticle

		// ----------------------------------------------------------------
		// Your Spark Topology should be defined here
		// ----------------------------------------------------------------

		// Dataset that stores (NewsArticle obj, first 5 filtered paragraphList along
		// with title) for
		// every NewsArticle
		Dataset<NewsArticleInfo> newsArticlesWithParagraphs = news.map(new NewsArticleParagraphFormatterMap(),
				Encoders.bean(NewsArticleInfo.class));

		// Dataset that stores (NewsArticle obj, TokenList after tokenization of the
		// paragraphs, DocumentLength) for every NewsArticle
		Dataset<NewsArticleTokens> NewsArticlesWithTokens = newsArticlesWithParagraphs
				.map(new NewsArticleTokenParserMap(), Encoders.bean(NewsArticleTokens.class));

		// Dataset that filters and stores the DocumentLength for every NewsArticle.
		Dataset<Integer> articlesLength = NewsArticlesWithTokens.map(new ArticleTokensToDocumentLengthMap(),
				Encoders.INT());

		// Stores the entireDocumentLengthOfTheCorpus
		Integer totalCorpusLength = articlesLength.reduce(new IntSumReducer());

		long totalDocsInCorpus = newsArticlesWithParagraphs.count();

		double averageDocumentLengthInCorpus = (1.0 * totalCorpusLength) / totalDocsInCorpus;

		Broadcast<List<Query>> broadcastqueries = JavaSparkContext.fromSparkContext(spark.sparkContext())
				.broadcast(queries.collectAsList());

		// Dataset that stores NewsArticleQueries i.e. (NewsArticle, queriesList) for
		// each article(5000), where queriesList stores (QueryArticleInfo) for each
		// query(3).
		// QueryArticleInfo stores the (OriginalQuery obj,termList,termFreq) for that
		// NewsArticle and that query.
		Dataset<NewsArticleWithQueries> articleQueryTermFreq = NewsArticlesWithTokens
				.map(new NewsArticleQueryFormatterMap(broadcastqueries), Encoders.bean(NewsArticleWithQueries.class));

		// Dataset that filter out and stores QueryTermList that contains a queryList
		// for all the queries,where queriesList stores (QueryArticleInfo) for each
		// query.
		// QueryArticleInfo stores the (OriginalQuery obj,termList,termFreq) for each
		// NewsArticle for that query.
		Dataset<QueryTermList> queryTermList = articleQueryTermFreq.map(new QueryFrequencyMap(),
				Encoders.bean(QueryTermList.class));

		// Reduces to a single obj i.e. QueryTermList that stores a queryList for every
		// query,where queriesList stores (QueryArticleInfo) for each query.
		// QueryArticleInfo stores the (OriginalQuery
		// obj,termList,totaltermFreqInCorpus) for that Query.
		QueryTermList totalTermFreqinDocs = queryTermList.reduce(new QueryTermListReducer());

		List<QueryArticleInfo> queryArticleInfo = totalTermFreqinDocs.getQueryTermList();

		// Broadcasting parameters required for DPHScorer method
		// Stores average document length in corpus
		Broadcast<Double> broadcastAvgDocLenCorpus = JavaSparkContext.fromSparkContext(spark.sparkContext())
				.broadcast(averageDocumentLengthInCorpus);
		// Stores total number of documents in corpus
		Broadcast<Long> broadcastTotalCorpusDocs = JavaSparkContext.fromSparkContext(spark.sparkContext())
				.broadcast(totalDocsInCorpus);
		// Stores news articles with term frequency of each term of each query
		Broadcast<List<NewsArticleWithQueries>> broadcastTFLen = JavaSparkContext.fromSparkContext(spark.sparkContext())
				.broadcast(articleQueryTermFreq.collectAsList());
		// Stores articles with each article length
		Broadcast<List<NewsArticleTokens>> broadcastDocLen = JavaSparkContext.fromSparkContext(spark.sparkContext())
				.broadcast(NewsArticlesWithTokens.collectAsList());
		// Stores total term frequency of each term in each query across the corpus
		Dataset<QueryArticleInfo> totalTFInCorpus = spark.createDataset(queryArticleInfo,
				Encoders.bean(QueryArticleInfo.class));
		
		// Dataset holding list of articles against each query along with the DPH score of each article.
		// The mapper calculates the DPH score of each article against the query.
		Dataset<DocumentRanking> scoresMap = totalTFInCorpus.map(new ArticleDPHScoreMap(broadcastAvgDocLenCorpus,
				broadcastTotalCorpusDocs, broadcastDocLen, broadcastTFLen), Encoders.bean(DocumentRanking.class));

		// Dataset holding relevant documents against each query
		// The mapper returns list of articles against each query based on the text distance calculated and checked of relelvance.
		Dataset<DocumentRanking> relevantRankedResults = scoresMap.map(new DistanceBetweenArticleMap(),
				Encoders.bean(DocumentRanking.class));

		// -----------------------------------------------------------------
		// driver code
		// -----------------------------------------------------------------
		List<DocumentRanking> queryAndRankedResultList = relevantRankedResults.collectAsList();

		return queryAndRankedResultList; // replace this with the the list of DocumentRanking output by
											// your topology
	}

}
