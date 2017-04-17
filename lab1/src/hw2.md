Zadatak 1

```java
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// - Koliko različitih vozila se nalazi u ulaznoj datoteci?  9834
// - - Koliko je trajala najdulja ukupna vožnja jednog taksija? Koja je minimalna, a koja najdulja vožnja tog taksija?
// Taxi FFFECF7... 60 najkraca, 3660 najdulja, ukupon 134640 sve voznje
//  - Koje ste promjene morali napraviti na izvornom kodu prilikom uvođenja optimizacijske funkcije Combine?
// Dodati samo combiner klasu koja je ekvivalentna reduceru
//  with combiner hadoop jar target/lab1-1.0-SNAPSHOT.jar lab2_1 trip_data.csv out.00  9.06s user 0.22s system 166% cpu 5.570 total
// without combiner hadoop jar target/lab1-1.0-SNAPSHOT.jar lab2_1 trip_data.csv out.01  9.79s user 0.35s system 153% cpu 6.618 total

public class lab2_1 {

    public static class Statistics implements  Writable {
        Double min, max, sum;

        public Statistics(Double min, Double max, Double sum) {
            this.min = min;
            this.max = max;
            this.sum = sum;
        }

        public Statistics(Double a) {
            this(a,a,a);
        }

        public Statistics() {
            this(null);
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeDouble(min);
            dataOutput.writeDouble(max);
            dataOutput.writeDouble(sum);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            min = dataInput.readDouble();
            max = dataInput.readDouble();
            sum = dataInput.readDouble();
        }

        public void combine(Statistics other) {
            if (this.min == null) {
                this.min = other.min;
                this.max = other.max;
                this.sum = 0.0;
            }
            this.min = Math.min(this.min, other.min);
            this.max = Math.max(this.max, other.max);
            this.sum += other.sum;
        }

        @Override
        public String toString() {
            return "(" + min+ ", " + max + ") total: " + sum;
        }
    }

    public static class TotalTimeMapper
            extends Mapper<LongWritable, Text, Text, Statistics> {

        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            if (key.get() == 0)
                return;
            String[] record = value.toString().split(",");
            Text medallion = new Text(record[0]);
            Statistics trip_time = new Statistics(Double.parseDouble(record[8]));
            context.write(medallion,trip_time);
        }
    }

    public static class DriveTimeReducer
            extends Reducer<Text,Statistics,Text,Statistics> {
        public void reduce(Text key, Iterable<Statistics> values,
                           Context context
        ) throws IOException, InterruptedException {
            Statistics sol = new Statistics();
            for (Statistics val : values) {
                sol.combine(val);
            }
            context.write(key, sol);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "drive time lab 2.1");
        job.setJarByClass(lab2_1.class);
        job.setMapperClass(TotalTimeMapper.class);
        job.setCombinerClass(DriveTimeReducer.class);
        job.setReducerClass(DriveTimeReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Statistics.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
```

```java
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

// medallion,hack_license,vendor_id,rate_code,
// store_and_fwd_flag,pickup_datetime,dropoff_datetime,passenger_count,
// trip_time_in_secs,trip_distance,pickup_longitude,pickup_latitude,
// dropoff_longitude,dropoff_latitude

//Koliko je vožnji realizirano u pojedinom području, tj. u užem centru i u širem gradskom području, i to tako da
//        je broj putnika bio 1, 2-3 putnika ili 4 i više putnika?
//
//0 .
//        525517 ./part-r-00000 (outer city, 1 putnik)
//        151509 ./part-r-00001 (2-3 putnika)
//        184797 ./part-r-00002 (4+ putnika)
//        244814 ./part-r-00003 (inner city)
//        70458 ./part-r-00004 (..)
//        86294 ./part-r-00005 (..)
// Kategorije su definirane u kodu u funkciji valueToPartition
//        - Koje ste promjene morali napraviti na izvornom kodu prilikom uvođenja funkcije Partition?
//
// Paziti da se mapperi ispravno particioniraju na reducere (kojih sada ima 6 zbog particija)
//        - Koliko je vožnji navedeno u svakoj podskupini?
//
// Nije mi bas jasno pitanje...odgovor je ekvivalentan prvom pitanju

public class lab2_2 {
    public static class PartitioningMapper
            extends Mapper<LongWritable, Text, IntWritable, Text> {

        private static boolean isInnerCenter(String[] record) {
            Double longitude = Double.parseDouble(record[12]);
            Double latitude = Double.parseDouble(record[13]);

            if (!isInInnerCenter(longitude, latitude)) return false;

            longitude = Double.parseDouble(record[10]);
            latitude = Double.parseDouble(record[11]);

            if (!isInInnerCenter(longitude, latitude)) return false;

            return true;
        }

        private static boolean isInInnerCenter(Double longitude, Double latitude) {
            if (longitude < -74. || longitude > - 73.95)
                return false;
            if (latitude < 40.75 || latitude > 40.8)
                return false;
            return true;
        }

        public static int valueToPartition(Text value) {
            String[] record = value.toString().split(",");
            Integer passenger_count = Integer.parseInt(record[7]);

            int sol = 0;
            if (isInnerCenter(record))
                sol = 3;

            switch (passenger_count) {
                case 1:
                    break;
                case 2:
                case 3:
                    sol += 1;
                    break;
                default:
                    sol += 2;
            }
            return sol;
        }

        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            if (key.get() == 0)
                return;
            context.write(new IntWritable(valueToPartition(value)), value);
        }
    }

    public static class TypePartitioner extends Partitioner<IntWritable, Text> {
        @Override
        public int getPartition(IntWritable intWritable, Text text, int i) {
            return intWritable.get();
        }
    }

    public static class IdentityReducer extends Reducer<IntWritable, Text, NullWritable, Text> {
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            context.write(NullWritable.get(), new Text("medallion,hack_license,vendor_id,rate_code," +
                    "store_and_fwd_flag,pickup_datetime,dropoff_datetime,passenger_count," +
                    "trip_time_in_secs,trip_distance,pickup_longitude,pickup_latitude," +
                    "dropoff_longitude,dropoff_latitude"));
            for (Text value : values) {
                context.write(NullWritable.get(), value);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        FileSystem.get(conf).delete(new Path(args[1]), true);

        Job job = Job.getInstance(conf, "drive time lab 2.1");
        job.setJarByClass(lab2_1.class);
        job.setMapperClass(PartitioningMapper.class);
        job.setPartitionerClass(TypePartitioner.class);
        job.setReducerClass(IdentityReducer.class);
        job.setNumReduceTasks(6);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
```
