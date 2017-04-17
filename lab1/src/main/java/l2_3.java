/*

- Kolika je veličina ulaznog skupa podataka?
32GB
- Koliko ste MapReduce poslova izvršili u vašem kôdu?
2
- Koje ste promjene napravili za prvi, a koje za drugi MapReduce posao?
Extraktao sam metode iz l2_1 i l2_2 da ih mogu lijepse zvati. Ulancao sam ih s provjerom izlaznog koa

- Koje su prednosti, a koji nedostaci ovakvog sažimanja međurezultata?
Filtracijom je working set smanjen za drugi ulancani posao te je brzi. 

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

public class l2_3 {

    public static Path TMPDIR = new Path("nmiculinic/tmp");

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int code = l2_1.executeTask1(conf, new Path(args[0]), TMPDIR);
        if (code == 0) {
            code = l2_2.executeTask2(conf, TMPDIR, new Path(args[1]));
        }
        System.exit(code);
    }

}

