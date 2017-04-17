import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
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

//Koliko ste MapReduce poslova izvršili u vašem kôdu?
// 2 MapReduce posla
//        Koliko je različitih taksija realiziralo vožnje u pojedinom području, tj. u užem centru i u širem gradskom području,
//        i to tako da je broj putnika bio 1, 2-3 putnika ili 4 i više putnika?
// Isti odgovor kao i u 2.2
public class lab2_3 {
    public static Path TMPDIR = new Path("/tmp/nmiculinic/");

    public static class MMMaper extends Mapper<LongWritable, Text, Text, lab2_1.Statistics> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (key.get() == 0)
                return;

            String[] record = value.toString().split(",");
            Text medallion = new Text(record[0] + "_" + lab2_2.PartitioningMapper.valueToPartition(value));
            lab2_1.Statistics trip_time = new lab2_1.Statistics(Double.parseDouble(record[8]));
            context.write(medallion,trip_time);
        }
    }

    public static class Partitioneeeer extends Partitioner<Text, lab2_1.Statistics> {
        @Override
        public int getPartition(Text text, lab2_1.Statistics statistics, int i) {
            String s = text.toString();
            s = s.substring(s.length() - 1);
            return Integer.parseInt(s);
        }
    }

    public static class Reeeducer extends Reducer<Text, lab2_1.Statistics, Text, lab2_1.Statistics> {
        @Override
        protected void reduce(Text key, Iterable<lab2_1.Statistics> values, Context context) throws IOException, InterruptedException {
            lab2_1.Statistics sol = new lab2_1.Statistics();
            for (lab2_1.Statistics val : values) {
                sol.combine(val);
            }
            String kk = key.toString();
            kk = kk.substring(0, kk.length() - 2);
            context.write(new Text(kk), sol);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        FileSystem.get(conf).delete(new Path(args[1]), true);
        FileSystem.get(conf).delete(TMPDIR, true);

        Job first = Job.getInstance(conf, "drive time lab 2.2");
        first.setJarByClass(lab2_3.class);
        first.setMapperClass(lab2_2.PartitioningMapper.class);
        first.setPartitionerClass(lab2_2.TypePartitioner.class);
        first.setReducerClass(lab2_2.IdentityReducer.class);
        first.setNumReduceTasks(6);

        first.setOutputKeyClass(IntWritable.class);
        first.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(first, new Path(args[0]));

        FileOutputFormat.setOutputPath(first, TMPDIR);

        int code = first.waitForCompletion(true) ? 0 : 1;
        if (code == 0) {
            Job second = Job.getInstance(conf, "drive time lab 2.3");
            second.setJarByClass(lab2_3.class);
            second.setMapperClass(MMMaper.class);
            second.setReducerClass(Reeeducer.class);
            second.setPartitionerClass(Partitioneeeer.class);

            second.setNumReduceTasks(6);
            second.setOutputKeyClass(Text.class);
            second.setOutputValueClass(lab2_1.Statistics.class);


            FileInputFormat.addInputPath(second, TMPDIR);
            FileOutputFormat.setOutputPath(second, new Path(args[1]));
            code = second.waitForCompletion(true) ? 0 : 1;
        }
//        FileSystem.get(conf).delete(TMPDIR, true);
        System.exit(code);
    }

}
