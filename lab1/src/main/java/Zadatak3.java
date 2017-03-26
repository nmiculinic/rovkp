import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;


import java.util.Arrays;

public class Zadatak3 {
    public static void main(String [] args) throws Exception{
        Configuration conf = new Configuration ();
        try (FileSystem hdfs = FileSystem.get(conf);
             LocalFileSystem lfs = LocalFileSystem.getLocal(conf)){
            for (Path p: Arrays.asList(
                    new Path("/user/lpp/gutenberg.zip"),
                    new Path("/usr/lib/hadoop/gutenberg.zip")
            )) {
                System.out.println("hdfs " + p + " " + hdfs.exists(p));
                System.out.println("local fs " + p + " " + lfs.exists(p));
            }
        }
    }
}
