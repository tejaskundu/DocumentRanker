package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.function.MapFunction;

import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;
import uk.ac.gla.dcs.bigdata.providedutilities.TextDistanceCalculator;

public class DistanceBetweenArticleMap implements MapFunction<DocumentRanking, DocumentRanking> {
    /**
     * 
     */
    private static final long serialVersionUID = -2778136839713226364L;

    @Override
    public DocumentRanking call(DocumentRanking value) throws Exception {
        // Storing the complete RankedResult List for that Query.
        List < RankedResult > rankedDocs = value.getResults();
        List < RankedResult > filteredDocs = new ArrayList<RankedResult>();
       
        for (int index = 0; index < rankedDocs.size(); index++) {
            RankedResult focusDoc = rankedDocs.get(index);
            
            Boolean isRedundant = false;
        
            for (int j = 0; j < filteredDocs.size(); j++) {
                if (focusDoc.getArticle().getTitle() != null && filteredDocs.get(j).getArticle().getTitle() != null
    					&& TextDistanceCalculator.similarity(focusDoc.getArticle().getTitle(),
    							filteredDocs.get(j).getArticle().getTitle()) < 0.5) {
                    // If any of the previous articles are similar to this one, mark as redundant and break to avoid unnecessary iterations.
                    isRedundant = true;
                    break;
                }
            }

            // If the focusDoc is not redundant, add it to the filteredDocs list.
            if (!isRedundant) {
                filteredDocs.add(focusDoc);
            }

            // If the filteredDocs list size has reached 10, break from the loop and return the list.
            if (filteredDocs.size() == 10) {
                break;
            }
        }

        return new DocumentRanking(value.getQuery(), filteredDocs);
    }
}