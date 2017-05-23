package lab3;

import org.apache.mahout.cf.taste.common.NoSuchItemException;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.eval.RecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.eval.RMSRecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.recommender.GenericItemBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.impl.similarity.file.FileItemSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.ItemSimilarity;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;

/**
 * RMS hybrid  4.450498311341717
 * which is better than two previous models from homework
 a --> RMS = 4.565067227703982
 b --> RMS b 4.916006472152124 (Pearson)
 RMS b (Log-likelihood) 5.0229205078638905

 Zadatak 1 was pretty unclear and undefined, I used simple PearsonSimilarity for Item similarity
 from Mahout. Creating new matrix and implemeting ItemSimilarity interface seems overkill, since all hybrid
 system requires
 */
public class createSimMat {

    public static final String hybrid_path = "/home/lpp/Downloads/jester_hybrid_item_similarity.csv";

    public static RecommenderBuilder hybrid() {
        return new RecommenderBuilder() {
            @Override
            public Recommender buildRecommender(DataModel dataModel) throws TasteException {
                ItemSimilarity similarity = new FileItemSimilarity(
                        new File(hybrid_path));
                return new
                        GenericItemBasedRecommender(dataModel, similarity);
            }
        };
    }

    public static void main(String[] args) throws Exception {
        DataModel model = new FileDataModel(
                new File("/home/lpp/Downloads/jester_ratings.dat"), "\\t+");

        ItemSimilarity lucene_similarity = new FileItemSimilarity(
                new File("/home/lpp/Downloads/jester_item_similarity.csv"));

        ItemSimilarity pearson_similarity = new PearsonCorrelationSimilarity(model);

        double alpha = 0.5;

        try(PrintWriter writer = new PrintWriter(new FileWriter(hybrid_path))){
            for (int i = 1; i <= 150; ++i){
                for(int j = i + 1; j <= 150; ++j)
                    try {
                        // Both are already normalized to [-1, 1] as requested by Mahout API,
                        // thus simple affine combinations suffices
                        double sim = alpha * lucene_similarity.itemSimilarity(i, j)
                                + (1 - alpha) * pearson_similarity.itemSimilarity(i, j);
                        if (!Double.isNaN(sim))
                            writer.println(i + "," + j + "," + sim);
                    } catch (NoSuchItemException ex) {}
            }
        }

        RecommenderEvaluator recEvaluator = new
                RMSRecommenderEvaluator();
        System.out.println("RMS hybrid " + recEvaluator.evaluate(hybrid(), null, model,0.7, 0.5));
    }
}
