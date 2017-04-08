import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class lab2_1 {
    public static class TotalTimeMapper
            extends Mapper<LongWritable, Text, Text, TupleWritable> {

        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            if (key.get() == 0)
                return;
            String[] record = value.toString().split(",");
            Text medallion = new Text(record[0]);
            DoubleWritable trip_time = new DoubleWritable(Double.parseDouble(record[8]));
            TupleWritable out = new TupleWritable(new DoubleWritable[]{trip_time, trip_time, trip_time});
            System.out.println(medallion.toString() + "..." + ((DoubleWritable) out.get(0)).get());
            context.write(medallion,out);
        }
    }

    public static class DriveTimeReducer
            extends Reducer<Text,TupleWritable,Text,TupleWritable> {
        private double getDouble(TupleWritable t, int i) {
            return  ((DoubleWritable) t.get(i)).get();
        }
        public void reduce(Text key, Iterable<TupleWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            Double min = null;
            Double sum = 0.;
            Double max = null;
            for (TupleWritable val : values) {
                if (min == null) {
                    min = getDouble(val, 0);
                    max = getDouble(val, 1);
                }
                min = Math.min(min, getDouble(val, 0));
                max = Math.max(max, getDouble(val, 1));
                sum += getDouble(val, 2);
            }
            System.out.println(key.toString() + " __ " + min +"," + max+" -->" + sum);
            context.write(key, new TupleWritable(new Writable[]{new DoubleWritable(min), new DoubleWritable(max), new DoubleWritable(sum)}));
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
        job.setOutputValueClass(TupleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
