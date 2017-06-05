package lab4;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * Created by lpp on 6/5/17.
 */
public class hw3 {

    public static void main(String[] args) throws Exception {
        System.out.println("Usao");
        SparkConf conf = new SparkConf().setAppName("SparkStreamingTotalDistanceDriver");
        try {
            conf.get("spark.master");
        } catch (NoSuchElementException ex) {
            conf.setMaster("local[2]");
        }
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
        JavaDStream<String> records = jssc.socketTextStream("localhost", SensorStreamGenerator.PORT);
        JavaPairDStream<Integer, Double> result = records.
                filter(SensorscopeReading::isParsable).
                map(SensorscopeReading::new).
                mapToPair(sr -> new Tuple2<>(sr.getStationID(), sr.getSolarCurrent())).
                reduceByKeyAndWindow(Math::max, Durations.seconds(60), Durations.seconds(10));

        result.dstream().saveAsTextFiles("/tmp/spark/job", "txt");
        jssc.start();
        jssc.awaitTermination();
    }
}
