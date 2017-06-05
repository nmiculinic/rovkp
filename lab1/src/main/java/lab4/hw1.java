package lab4;

import java.io.IOException;

import java.io.File;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;
/*
- Koliko je bilo ulaznih datoteka senesorscope-monitor-xx.txt? 98
- Koliko se zapisa nalazi u izlaznoj datoteci? 4726564
- Kolika je veličina izlazne datoteke? 392 MB

*/

public class hw1 {


    private static String senzorPath = "/home/lpp/Downloads/senzor.csv";
    private static String senzorWritePath = "/home/lpp/Downloads/out.csv";

    public static void main(String[] args) throws IOException {

        File f = new File(senzorPath);

        FilenameFilter textFilter = new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.endsWith(".txt") ? true : false;
            }
        };

        File[] files = f.listFiles(textFilter);

        Stream<String> sol = Stream.empty();
        for (File fn : files) {
            sol = Stream.concat(sol, Files.lines(Paths.get(fn.getCanonicalPath())));
        }

        FileWriter fw = new FileWriter(senzorWritePath);
        sol.filter(line -> SensorscopeReading.isParsable(line)).
                map(line -> new SensorscopeReading(line)).
                sorted(SensorscopeReading.TIME_COMP).
                forEach(senzor -> writeToFile(fw, senzor.toString()));
    }

    private static void writeToFile(FileWriter fw, String line) {
        try {
            fw.write(line);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

}
