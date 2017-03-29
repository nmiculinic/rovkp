import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;

import java.util.Random;

public class lab3 {
    public static void main(String [] args) throws Exception{
        Configuration conf = new Configuration ();
        final Random rnd = new Random();
        final Path path = new Path("ocitanja.bin");
        try(SequenceFile.Writer writer = SequenceFile.createWriter(conf,
                SequenceFile.Writer.file(path),
                SequenceFile.Writer.keyClass(IntWritable.class),
                SequenceFile.Writer.valueClass(FloatWritable.class))) {
            for (int i = 1; i <= 100; ++i) {
                for (int j = 0; j < 10000; ++j)
                    writer.append(new IntWritable(i),new FloatWritable(100 * rnd.nextFloat()));
            }
        }

        try (SequenceFile.Reader reader = new SequenceFile.Reader(conf,
                SequenceFile.Reader.file(path))){
            int[] cnt = new int[101];
            double[] sum = new double[101];
            final IntWritable key = new IntWritable();
            final FloatWritable value = new FloatWritable();
            while (reader.next(key, value)){
                cnt[key.get()]++;
                sum[key.get()] += value.get();
            }

            for (int i = 1; i <= 100; ++i) {
                System.out.println("Senzor " + i + " :" + sum[i]/cnt[i]);
            }
        }
    }
}
