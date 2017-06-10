package lab4;

import java.io.File;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.stream.Stream;


public class task1 {
    private static String polucijaPath = "/home/lpp/Downloads/pollutionData/";
    private static String polucijaWritePath = "/home/lpp/Downloads/pollutionData-all.csv";

    public static void main(String[] args) throws IOException {

        FilenameFilter textFilter = (dir, name) -> name.endsWith(".csv");
        Stream<String> total_lines = Stream.empty();
        for (File fn: new File(polucijaPath).listFiles(textFilter)){
            total_lines = Stream.concat(total_lines, Files.lines(Paths.get(fn.getCanonicalPath())));
        }

        FileWriter fw = new FileWriter(polucijaWritePath);
        total_lines.filter(Pollution::isParsable).
                map(Pollution::new).
                sorted(Comparator.comparing(Pollution::getTimestamp)).
                forEach(pollution -> writeToFile(fw, pollution.toString() + "\n"));
    }

    private static void writeToFile(FileWriter fw, String line) {
        try {
            fw.write(line);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

}
