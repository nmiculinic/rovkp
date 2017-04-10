import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// medallion,hack_license,vendor_id,rate_code,
// store_and_fwd_flag,pickup_datetime,dropoff_datetime,passenger_count,
// trip_time_in_secs,trip_distance,pickup_longitude,pickup_latitude,
// dropoff_longitude,dropoff_latitude

public class lab2_3 {
    public static Path TMPDIR = new Path("/tmp/nmiculinic/");
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
            second.setMapperClass(lab2_1.TotalTimeMapper.class);
            second.setCombinerClass(lab2_1.DriveTimeReducer.class);
            second.setReducerClass(lab2_1.DriveTimeReducer.class);

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
