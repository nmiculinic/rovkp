package lab3;

import org.apache.commons.lang3.StringEscapeUtils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by lpp on 5/20/17.
 */
public class hw1 {

    public static Map<Integer, String> readFile(String file) throws Exception{
        Map<Integer, String> sol = new HashMap<>();
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = br.readLine()) != null) {
                Integer key = Integer.parseInt(line.substring(0, line.length() - 1));
                StringBuffer sb = new StringBuffer();
                while ((line = br.readLine()) != null) {
                    if (line.length() > 0) {
                        sb.append(line);
                    } else {
                        sol.put(key, StringEscapeUtils.unescapeXml(sb.toString().toLowerCase().replaceAll("\\<.*?\\>", "")));
                        break;
                    }
                }
            }
        }
        return sol;
    }

    public static void main(String[] args) throws Exception {
        Map<Integer, String> jokes = readFile("/home/lpp/Downloads/jester_items.dat");
        for (Map.Entry<Integer, String> ent: jokes.entrySet()) {
            System.out.println(ent.getKey() + " " + ent.getValue());
        }
    }

}
