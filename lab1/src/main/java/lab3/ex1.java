package lab3;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.eval.IRStatistics;
import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.eval.RecommenderEvaluator;
import org.apache.mahout.cf.taste.eval.RecommenderIRStatsEvaluator;
import org.apache.mahout.cf.taste.impl.eval.GenericRecommenderIRStatsEvaluator;
import org.apache.mahout.cf.taste.impl.eval.RMSRecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
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
import org.apache.mahout.cf.taste.recommender.UserBasedRecommender;

import org.apache.mahout.cf.taste.similarity.ItemSimilarity;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

import java.io.File;
import java.util.List;

public class ex1 {

    public static void main(String[] args) throws Exception {
        main3(args);
    }

    public static void main3(String[] args) throws Exception {

        DataModel model = new FileDataModel(
                new File("/home/lpp/Downloads/ml-latest-small/ratings.csv"));
        RecommenderBuilder builder = new RecommenderBuilder() {
            @Override
            public Recommender buildRecommender(DataModel model) throws
                    TasteException {

                //inicijaliziraj sličnost korisnika koristeći mjeru Pearsonove korelacije
                UserSimilarity similarity = new
                        PearsonCorrelationSimilarity(model);
                //inicijaliziraj slične korisnike kao one koji su unutar zadane razine
                UserNeighborhood neighborhood = new
                        ThresholdUserNeighborhood(0.5, similarity, model);
                //vrati preporučitelja kao preporučitelja temeljenog na suradnji
                return new GenericUserBasedRecommender(model, neighborhood,
                        similarity);
            }
        };

        RecommenderEvaluator recEvaluator = new
                RMSRecommenderEvaluator();
        double score = recEvaluator.evaluate(builder, null, model,
                0.7, 1.0);
        System.out.println("RMS" + score);

        //izračunaj odziv i preciznost
        RecommenderIRStatsEvaluator statsEvaluator
                = new GenericRecommenderIRStatsEvaluator();
        IRStatistics stats = statsEvaluator.evaluate(
                builder, null, model, null, 2,
                GenericRecommenderIRStatsEvaluator.CHOOSE_THRESHOLD, 1.0);
        System.out.println(stats.getPrecision());
        System.out.println(stats.getRecall());
    }

    public static void main2(String[] args) throws Exception {
        //inicijaliziraj model učitavanjem podataka o korisnicima učitavanjem izdatoteke
        DataModel model = new FileDataModel(
                new File("/home/lpp/Downloads/ml-latest-small/ratings.csv"));

        //inicijaliziraj sličnost objekata
        ItemSimilarity similarity = new FileItemSimilarity(
                new File("/home/lpp/Downloads/ml-latest-small/item-similarity.csv"));

        //inicijaliziraj preporučitelja kao preporučitelja temeljenog na sličnosti objekata
        ItemBasedRecommender recommender = new
                GenericItemBasedRecommender(model, similarity);

        //izračunaj i ispiši 10 preporuka za korisnika s ID-jem 620
        List<RecommendedItem> recommendations = recommender.recommend(620, 10);
        for (RecommendedItem recommendation : recommendations) {
            System.out.println(recommendation);
        }
    }

    public static void main1(String[] args) throws Exception {
        //inicijaliziraj model učitavanjem podataka o korisnicima
        DataModel model = new FileDataModel(new
                File("/home/lpp/Downloads/ml-latest-small/ratings.csv"));

        //inicijaliziraj sličnost korisnika koristeći mjeru log-likelihood
        UserSimilarity similarity = new LogLikelihoodSimilarity(model);

        //inicijaliziraj slične korisnike kao 5 najsličnijih po mjeri loglikelihood
        UserNeighborhood neighborhood = new NearestNUserNeighborhood(5,
                similarity, model);

        //inicijaliziraj preporučitelja kao preporučitelja temeljenog nasuradnji korisnika

        UserBasedRecommender recommender = new
                GenericUserBasedRecommender(model, neighborhood, similarity);

        //izračunaj i ispiši 10 preporuka za korisnika s ID-jem 620
        List<RecommendedItem> recommendations = recommender.recommend(620, 10);
        for (RecommendedItem recommendation : recommendations) {
            System.out.println(recommendation);
        }
    }
}
