import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;

/**
 - Koja je veličina konačne datoteke gutenberg_books.txt?
 391 MB
 - Koliko je ukupno redaka pročitano?
 8481553

 - U slučaju kad bi tu datoteku pohranili na HDFS s veličinom blokova 128 MB i faktorom replikacije 3, koliko bi
 se ukupno blokova stvorilo na HDFS-u?
 3(faktor replikacije) x 4 (ceil(391/128)) = 12 blokova
 - Koliko vremena se izvodio program? Oko 5 sekundi
 Kakvo bi bilo očekivano vrijeme izvođenja kada bi se taj program izvršavao
 na Hadoopovom grozdu, uz pohranu na HDFS? Ovisi o clusteru...za ovako male podatke imamo overhead komunikacije i
 map-reduce job..Ajmo reci uz dovoljni broj masina 1s.
 */
public class Zadatak2 {
    public static void main(String [] args) throws Exception{
        Charset charset = Charset.forName("ISO-8859-1");
        Path start = Paths.get("/usr/lib/hadoop/gutenberg");
        BufferedWriter bw = Files.newBufferedWriter(Paths.get("/usr/lib/hadoop/gutenberg_books.txt"), charset);
        final int[] num_lines = {0, 0};

        long startTime = System.currentTimeMillis();
        Files.walkFileTree(start, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                    throws IOException
            {
                try {
                    List<String> lines = Files.readAllLines(file, charset);
                    for (String line: lines)
                        bw.write(line);
                        bw.newLine();
                    num_lines[0] += lines.size();
                    num_lines[1] ++;
                }
                catch (Exception ex) {
                    System.out.println(file);
                    System.err.println(ex);
                }
                return FileVisitResult.CONTINUE;
            }
            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException e)
                    throws IOException
            {
                    return FileVisitResult.CONTINUE;
            }
        });

        bw.flush();
        bw.close();
        System.out.println("Total lines " + num_lines[0]);
        System.out.println("Total files " + num_lines[1]);
        System.out.println("Total time " + (System.currentTimeMillis() - startTime)/1000.0 + "s");
    }
}
