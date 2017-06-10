package lab4;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.Map;
import java.util.NoSuchElementException;

public class task2 {
    public static void main(String[] args) throws Exception {
        SparkConf sparkConf = new SparkConf().setAppName("Newborns");
        try {
            sparkConf.get("spark.master");
        } catch (NoSuchElementException e) {
            sparkConf.setMaster("local[*]");
        }

        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaRDD<String> records = sc.textFile("/home/lpp/Downloads/DeathRecords.csv");
        JavaRDD<USDeathRecord> rdd = records.filter(USDeathRecord::isParsable).map(USDeathRecord::new).cache();

        try (PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter("/tmp/rovkp_task2.txt")))) {

//        1. Koliko je ženskih osoba umrlo u lipnju kroz čitav period?
//        2. Koji dan u tjednu je umrlo najviše muških osoba starijih od 50 godina?
//        3. Koliko osoba je bilo podvrgnuto obdukciji nakon smrti?
//        4. Kakvo je kretanje broja umrlih muškaraca u dobi između 45 i 65 godina po mjesecima ? Rezultat je (sortirana)
//        lista tipa Pair2 (ključ je redni broj mjeseca, a vrijednost je broj umrlih muškaraca)
//        5. Kakvo je kretanje postotka umrlih oženjenih muškaraca u dobi između 45 i 65 godina po mjesecima?
//                Rezultat je (sortiran) skup tipa Pair2 (ključ je redni broj mjeseca, a vrijednost je postotak).
//        6. Koji je ukupni broj umrlih u nesreći (kod 1) u cjelokupnom periodu?
//        7. Koliki je broj različitih godina starosti umrlih osoba koji se pojavljuju u zapisima?

            // important fields : monthofdeath, sex, age, maritalstatus, dayofweekdeath, mannerdeath, autopsy

            writer.println("Zena umrlo u lipnju: " +
                    rdd.filter(x -> x.getGender().equals("F") && x.getMonthOfDeath() == 6).count());

            Map<Integer, Long> dayOfTheWeek = rdd.filter(x -> x.getAge() > 50).mapToPair(x -> new Tuple2<>(x.getDayOfWeekOfDeath(), 1))
                    .countByKey();
            Long max_val = new Long(0);
            Integer sol = -1;
            for (Map.Entry kv : dayOfTheWeek.entrySet()) {
                // writer.println(kv.getKey() + " " + kv.getValue());
                if ((Long) kv.getValue() > max_val) {
                    max_val = (Long) kv.getValue();
                    sol = (Integer) kv.getKey();
                }
            }

            writer.println("Dan u tjednu sa najvise muskih umrlih starijih od 50: " + sol + ", " + max_val + " ljudi");
            writer.println("Broj osoba na obdukciji: " + rdd.filter(x -> x.getAutopsy().equals("Y")).count());

            writer.println("\nKretanje broja umrlih muskih izmedu 45-65 po mjesecima sortirano (mjesec, broj umrlih): ");
            JavaRDD<USDeathRecord> rdd45_65 = rdd.filter(x -> 45 <= x.getAge() && x.getAge() < 65).cache();
            Map<Integer, Long> md = rdd45_65
                    .mapToPair(x -> new Tuple2<>(x.getMonthOfDeath(), 1))
                    .countByKey();

            for (int i = 1; i <=12; ++i) {
                writer.println(i + " " + md.get(i));
            }

            writer.println("\nKretanje postotka umrlih ozenjenih muskih 45-65 po mjesecima (mjesec, postotak): ");
            JavaRDD<USDeathRecord> rddfil = rdd45_65
                    .filter(x -> x.getGender().equals("M") && x.getMaritialStatus().equals("M")).cache();
            Long size = rddfil.count();
            Map<Integer, Double> md2 = rddfil
                    .mapToPair(x -> new Tuple2<>(x.getMonthOfDeath(),1.0/size))
                    .reduceByKey((x,y) -> x + y)
                    .collectAsMap();

            for (int i = 1; i <=12; ++i) {
                writer.println(i + " " + md2.get(i));
            }

            writer.println("Ukupno umrlih u nesreci: " + rdd.filter(x->x.getMannerOfDeath() == 1).count());
            writer.println("Broj razlicitih godina starosti: " + rdd.map(USDeathRecord::getAge).distinct().count());
            writer.close();
        }
    }
}

