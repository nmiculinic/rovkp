/*
- Koje funkcije sadrži vaš MapReduce program?
Mapper, reducer i partitioner
- Koje tipove podataka ste definirali kao izlaz iz MapReduce programa?
Izlaz mapera je Int (sat) i Text(cijela voznja), dok je izlaz reducera definiran u zadatku
*/

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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class l2_2 {

    public static int getHoD(String[] record) throws ParseException {
        String dateTimeString = record[2];
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date dateTime = format.parse(dateTimeString);
        Calendar cal = Calendar.getInstance();
        cal.setTime(dateTime);
        return cal.get(Calendar.HOUR_OF_DAY);
    }

    public static class HourMapper
            extends Mapper<LongWritable, Text, IntWritable, Text> {

        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] record = value.toString().split(",");
            try {
                context.write(new IntWritable(l2_2.getHoD(record)), value);
            } catch (ParseException ex) {
                System.err.println(ex);
            }
        }
    }

    public static class HourPartitioner extends Partitioner<IntWritable, Text> {
        @Override
        public int getPartition(IntWritable intWritable, Text text, int numPartitions) {
            return intWritable.get();
        }
    }

    public static class HourReducer
            extends Reducer<IntWritable, Text, NullWritable, Text> {

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<String, Integer> drives = new HashMap<>();
            Map<String, Double> profits = new HashMap<>();

            for (Text value : values) {
                String[] record = value.toString().split(",");
                double profit = l2_1.profit(record);
                Double lon_in = Double.parseDouble(record[6]);
                Double lat_in = Double.parseDouble(record[7]);
                int[] cellId = l2_1.getCellId(lat_in, lon_in);
                String cell = "Cell " + "(" + cellId[0] + ", " + cellId[1] + ")";

                drives.put(cell, 1 + (drives.get(cell) != null ? drives.get(cell) : 0));
                profits.put(cell, profit + (profits.get(cell) != null ? profits.get(cell) : 0.0));
            }

            context.write(NullWritable.get(), new Text(key.toString()));
            String max_drive_cell = Collections.max(drives.entrySet(), Comparator.comparing(Map.Entry::getValue)).getKey();
            context.write(NullWritable.get(), new Text(max_drive_cell + " -> " + drives.get(max_drive_cell)));

            String max_profits_cell = Collections.max(profits.entrySet(), Comparator.comparing(Map.Entry::getValue)).getKey();
            context.write(NullWritable.get(), new Text(max_profits_cell + " -> " + profits.get(max_profits_cell)));
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        System.exit(executeTask2(conf, new Path(args[0]), new Path(args[1])));
    }

    public static int executeTask2(Configuration conf, Path in, Path out) throws IOException, InterruptedException, ClassNotFoundException {
        Job job = Job.getInstance(conf, "Lab 2.2");
        job.setJarByClass(l2_2.class);
        job.setMapperClass(HourMapper.class);
        job.setPartitionerClass(HourPartitioner.class);
        job.setReducerClass(HourReducer.class);
        job.setNumReduceTasks(24);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, in);
        FileSystem.get(conf).delete(out, true);
        FileOutputFormat.setOutputPath(job, out);

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
