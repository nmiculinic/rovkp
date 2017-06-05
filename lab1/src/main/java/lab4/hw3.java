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
 *
 * Koliko često (u sekundama) nastaje novi direktorij na disku?
 * 10 sekundi
 - Koliko često (u sekundama) se pokreće izračun?
 - 10 sekundi
 - Može li vrijednost parametra solarPanelCurrent neke stanice biti manja u nekom direktoriju nego u
 njegovom neposrednom prethodniku. Zašto?
 - Moze, zbog prirode rolling max
 - Kako se kreću vrijednosti parametra solarPanelCurrent neke postaje u prva 3 direktorija koji su nastali?
 Zašto?
 (100,99.713)
 (100,99.713)
 (100,99.713)
 (100,99.713)
 (100,99.713)
 (100,99.713)
 (100,99.103)
 (100,99.103)
 (100,98.444)
 (100,98.126)
 (100,98.126)

 Prvih par vrijednosti je isto zbog vece velicine windowa, nego slide duration
 */
public class hw3 {

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName("hw3");
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

        result.dstream().saveAsTextFiles("/tmp/spark/job", "");
        jssc.start();
        jssc.awaitTermination();
    }
}
