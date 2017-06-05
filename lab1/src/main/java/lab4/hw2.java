package lab4;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.NoSuchElementException;


/**
 * Created by lpp on 6/5/17.
 */
public class hw2 {
    public static void main(String[] args) throws IOException {

        SparkConf sparkConf = new SparkConf().setAppName("hw2");

        try{
            sparkConf.get("spark.master");
        }catch (NoSuchElementException e){
            sparkConf.setMaster("local");
        }

        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaRDD<String> records = sc.textFile(args[0]);
        JavaRDD<USBabyNameRecord> rdd = records.filter(USBabyNameRecord::isParsable).map(USBabyNameRecord::new);
        BufferedWriter writer = new BufferedWriter(new FileWriter("/home/rovkp/results.txt"));

        writer.write("\nNajnepopularnije zensko ime: \n");
        writer.write(rdd.filter(d -> d.getGender().equals("F")).sortBy(USBabyNameRecord::getCount, true, 1).first().toString() + "\n");

        writer.write("\n10 najpopularnijih muskih imena:  \n");
        rdd.filter(d -> d.getGender().equals("M")).sortBy(USBabyNameRecord::getCount, false, 1).top(10).forEach(d -> {
            try {
                writer.write(d.toString() + "\n");
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        writer.write("\nU kojoj drzavi je 1946 rodeno najvise djece oba spola:  \n");
        rdd.filter(d -> d.getYear().equals(1946));

        writer.write("\nKretanje zenske djece kroz godine:  \n");
        rdd.filter(d -> d.getGender().equals("F"));

        writer.write("\nKretanje postotka imena mary kroz godine:  \n");
        rdd.filter(d -> d.getName().equals("Mary"));

        writer.write("\nUkupni broj djece svugdje:  \n");
        rdd.mapToPair(d -> new Tuple2<>(d.getId(), d.getCount())).reduceByKey((x, y) -> x + y).foreach(d -> {try {
            writer.write(d.toString() + "\n");
        } catch (IOException e) {
            e.printStackTrace();}
        });

        writer.write("\nBroj razlicitih imena:  \n");
        System.out.println(rdd.map(d -> d.getName()).distinct().count());
        writer.close();
    }
}
