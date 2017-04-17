import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import javax.print.DocFlavor;
import java.io.IOException;
//
//- Koje funkcije sadrži vaš MapReduce program (map, reduce, partitioner, combiner, itd.)?
// Samo mapper
// - Koliko izlaznih datoteka je nastalo nakon izvođenja vašeg MapReduce programa?
// 3 izlazne datoteke
// wc -l *
//           685290 part-m-00000
//           684945 part-m-00001
//           590542 part-m-00002
//           0 _SUCCESS
//           1960777 total
// Ukupno 1 960 777 je zadovoljilo kriterija
//
// - Koliko zapisa u ulaznoj datoteci nije zadovoljilo zadane kriterije?
// 1999999 /home/lpp/Downloads/sorted_data.csv ukupno zapisa
// 39222 nije zadovoljilo kriterija (razlika)

public class l2_1 {

    private static final double GRID_WIDTH = 0.008983112;
    private static final double GRID_LENGTH = 0.011972;

    private static final double BEGIN_LON = -74.913585;
    private static final double BEGIN_LAT = 41.474937;

    public static int[] getCellId(double lat, double lon) {
        int[] sol = new int[2];
        sol[0] = (int) (((lon - BEGIN_LON) / GRID_LENGTH) + 1);
        sol[1] = (int) (((BEGIN_LAT - lat) / GRID_WIDTH) + 1);
        return sol;
    }

    public static boolean isInArea(double lat, double lon){
        int[] cell = getCellId(lat, lon);
        if (cell[0] < 0 || cell[0] > 150)
            return false;
        if (cell[1] < 0 || cell[1] > 150)
            return  false;
        return true;
    }


    public static boolean isInArea(String[] record) {
        Double lon_in = Double.parseDouble(record[6]);
        Double lat_in = Double.parseDouble(record[7]);
        Double log_out = Double.parseDouble(record[8]);
        Double lat_out = Double.parseDouble(record[9]);
        return isInArea(lat_in, lon_in) && isInArea(lat_out, log_out);
    }

    public static double profit(String[] record) {
        return Double.parseDouble(record[16]);
    }

    public static class FilterMapper
            extends Mapper<LongWritable, Text, NullWritable , Text> {

        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] record = value.toString().split(",");
            if (profit(record) <= 0) return;
            if (!isInArea(record)) return;

            context.write(NullWritable.get(), value);
        }
    }

    public static int executeTask1(Configuration conf, Path in, Path out) throws Exception{
        Job job = Job.getInstance(conf, "lab 2.1");
        job.setJarByClass(l2_1.class);
        job.setMapperClass(FilterMapper.class);
        job.setNumReduceTasks(0);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, in);
        FileSystem.get(conf).delete(out, true);
        FileOutputFormat.setOutputPath(job, out);
        return  job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        System.exit(executeTask1(conf, new Path(args[0]), new Path(args[1])));
    }
}
