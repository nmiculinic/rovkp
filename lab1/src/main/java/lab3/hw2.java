package lab3;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.eval.RecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.eval.RMSRecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.ThresholdUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericItemBasedRecommender;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.LogLikelihoodSimilarity;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.impl.similarity.file.FileItemSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.ItemBasedRecommender;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.ItemSimilarity;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

import java.io.File;
import java.util.List;

/**
 * Kojih 10 preporuka je za korisnika s ID-jem 220 je izračunao prvi, a koje drugi preporučitelj?

 preporučitelja?
 Recommender a
 RecommendedItem[item:36, value:9.23486]
 RecommendedItem[item:43, value:7.8958974]
 RecommendedItem[item:96, value:7.8057303]
 RecommendedItem[item:22, value:7.726936]
 RecommendedItem[item:37, value:7.3715916]
 RecommendedItem[item:42, value:7.369285]
 RecommendedItem[item:94, value:7.125086]
 RecommendedItem[item:122, value:6.873373]
 RecommendedItem[item:86, value:6.7745705]
 RecommendedItem[item:129, value:6.739036]
 Recommender b
 RecommendedItem[item:105, value:4.0365195]
 RecommendedItem[item:89, value:3.8970373]
 RecommendedItem[item:53, value:3.8832562]
 RecommendedItem[item:35, value:3.812178]
 RecommendedItem[item:32, value:3.7961242]
 RecommendedItem[item:72, value:3.7941797]
 RecommendedItem[item:104, value:3.7748928]
 RecommendedItem[item:129, value:3.7536328]
 RecommendedItem[item:114, value:3.7383087]
 RecommendedItem[item:108, value:3.7021122]

 - Koje preporučitelj ima bolju kvalitetu?
 a --> RMS = 4.565067227703982
 b --> RMS b 4.916006472152124 (Pearson)

 Ocito bolji je pristup a, tj. prvi pristup

 - Je li za ove ulazne podatke bolje koristiti mjeru log-likelihood ili Pearsonovu korelaciju u slučaju drugog
 RMS b (Log-likelihood) 5.0229205078638905, stoga personov  je bolji za ovaj dataset!

 */
public class hw2 {
    public static RecommenderBuilder part1() {
        return new RecommenderBuilder() {
            @Override
            public Recommender buildRecommender(DataModel dataModel) throws TasteException {
                ItemSimilarity similarity = new FileItemSimilarity(
                        new File("/home/lpp/Downloads/jester_item_similarity.csv"));
                return new
                        GenericItemBasedRecommender(dataModel, similarity);
            }
        };
    }

    public static RecommenderBuilder part2() {
        return new RecommenderBuilder() {
            @Override
            public Recommender buildRecommender(DataModel dataModel) throws TasteException {
                UserSimilarity similarity = new PearsonCorrelationSimilarity(dataModel);
//                UserSimilarity similarity = new LogLikelihoodSimilarity(dataModel);
                UserNeighborhood neighborhood = new
                        ThresholdUserNeighborhood(0.1, similarity, dataModel);
                return new GenericUserBasedRecommender(dataModel, neighborhood, similarity);
            }
        };
    }


    public static void main(String[] args) throws Exception {
        DataModel model = new FileDataModel(
                new File("/home/lpp/Downloads/jester_ratings.dat"), "\\t+");

        Recommender a = part1().buildRecommender(model);
        Recommender b = part2().buildRecommender(model);

        System.out.println("Recommender a");
        List<RecommendedItem> recommendations = a.recommend(220, 10);
        for (RecommendedItem recommendation : recommendations) {
            System.out.println(recommendation);
        }

        System.out.println("Recommender b");
        recommendations = b.recommend(220, 10);
        for (RecommendedItem recommendation : recommendations) {
            System.out.println(recommendation);
        }


        RecommenderEvaluator recEvaluator = new
                RMSRecommenderEvaluator();
        System.out.println("RMS a " + recEvaluator.evaluate(part1(), null, model,0.7, 0.5));
        System.out.println("RMS b " + recEvaluator.evaluate(part2(), null, model,0.7, 0.5));
    }
}
