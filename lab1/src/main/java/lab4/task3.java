package lab4;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;
import java.util.NoSuchElementException;

public class task3 {
    public static void main(String[] args) throws Exception{
        SparkConf conf = new SparkConf().setAppName("SparkStreaming");
        try {
            conf.get("spark.master");
        } catch (NoSuchElementException ex) {
            conf.setMaster("local[*]");
        }

        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(3));
        JavaDStream<String> records = jssc.socketTextStream("localhost", 10002);

        JavaPairDStream<Station, Integer> rdd = records
                .filter(Pollution::isParsable)
                .map(Pollution::new)
                .mapToPair(d -> new Tuple2<>(new Station(d.getLongitude(), d.getLatitude()), d.getOzone()))
                .reduceByKeyAndWindow(Math::min, Durations.seconds(45), Durations.seconds(15));

        rdd.dstream().saveAsTextFiles(args[0], "txt");
        jssc.start();
        jssc.awaitTermination();
    }

}

