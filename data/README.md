This archive contains version 3 of the first TREC Washington Post Collection.

The initial version (v1) contained duplicate entries with the same document identifier ("id" field in the JSON object.), which were removed in version 2.  That version still contained a number of near-duplicate documents which have been removed in v3.  Additionally, v3 adds 154,418 new documents from 2018 and 2019.

This package contains:

1. the WashingtonPost collection, a single file where each line is a JSON object representing a document,
2. a list of near-duplicate documents removed in v3,
3. some example Python scripts for tagging the data with Spacy (https://spacy.io) and indexing with ElasticSearch (https:/elastic.co).

The scripts require Python 3 and depend on pip packages for spacy, elasticsearch, and tqdm.  There is a requirements.txt file in the scripts directory.

Version 3 incorporates (decorporates?) near-duplicate removal, as documented in the TREC 2019 News Track overview.  A near-duplicate detection system was implemented based on the classic minhash and locality-sensitive hashing algorithms.  This system clusters all documents into near-duplicate clusters with a cluster representative that is the earliest version of that document in the collection.  Samples of the near duplicate clusters were hand-checked for over- and under-clustering, primarily using article titles (which is why the document duplicates file includes them) but also examining article content to control for false negatives.  This process reduced the v2 collection from 595.037 documents to 515,530.  Note that there is no perfect detection process and certainly some near-duplicates remain and some non-duplicates were removed.

You can confirm that you have v3, with 515,530 documents from v2 and 154,418 new documents using the POSIX wc(1) tool:

	wc -l data/TREC_Washington_Post_collection.v3.jl 
	671947 data/TREC_Washington_Post_collection.v3.jl
	
The script/wapo-print-docids.py script also prints just the "id" field from each document:

	scripts/wapo-print-docids.py data/TREC_Washington_Post_collection.v2.jl | wc -l
	671947

The file scripts/wapo-near-duplicates lists the near-duplicates.  The first field is the cluster representative, and the second is the document id of the member referred to in that line.  The remainder of the line is the headline of the article.  Sorting the file by the first field or the third-through-last fields are useful ways of understanding the true-positive and true-negative behavior of the detector.  You will find that the v3 documents collection only includes document IDs from the first column:

	cat wapo-near-duplicates| awk '{print $1}' | sort | uniq > nd-reps
	./wapo-print-docids.py ../data/TREC_Washington_Post_collection.v3.jl | sort > v3.docids
	diff nd-reps v3.docids
	(no output)

The new 2018-2018 came with a slightly different JSON schema, which has been normalized to match the schema in the v1 and v2 documents:

1. The ids have a somewhat different format.
2. There was a distinction in v1 and v2 between "articles" and "blogs" which no longer exists.
3. Titles were not retained in the content block series; we copy them back in for consistency.
4. The same for the date, byline, and kicker fields.
5. The byline was dropped, we have replaced it with the first author.
6. The author field was restructured into an array, which we changed back into a single string with semicolons.
7. The date was reformatted to ISO5601.  We reformatted it back to a milliseconds stamp to match v2.

We include this information for completeness; it may mean that bylines are inaccurate or that our datestamps are incorrect.
