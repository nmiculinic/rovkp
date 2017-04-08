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

//  with combiner hadoop jar target/lab1-1.0-SNAPSHOT.jar lab2_1 trip_data.csv out.00  9.06s user 0.22s system 166% cpu 5.570 total
// without combiner hadoop jar target/lab1-1.0-SNAPSHOT.jar lab2_1 trip_data.csv out.01  9.79s user 0.35s system 153% cpu 6.618 total

public class lab2_1 {

    private static class Statistics implements  Writable {
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
