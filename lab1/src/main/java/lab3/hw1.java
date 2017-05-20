package lab3;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Koliko se zapisa nalazi u izlaznoj datoteci?
 * lab1 cat /tmp/item_similarity.csv| wc -l                                                                                                                                   G: master @102b6d4
 8358

 - Koja šala je najsličnija šali s ID-jem 1?
 1,87,0.44775397

 - Vidite li zašto su te dvije šale slične?
 1 a man visits the doctor. the doctor says, "i have bad news for you.
 you have cancer and alzheimer's disease".the man replies, "well, thank god i don't have cancer!"

 87 a man who recently completed a routine physical examination receives a phone call from his doctor.
 the doctor says, "i have some good news and some bad news." the man says, "okay, give me the good news first." the
 doctor says, "the good news is: you have 24 hours to live." the man replies, "shit! that's the good news? then
 what's the bad news?"the doctor says, "the bad news is: i forgot to call you yesterday."

 Yep, obje ukljucuju doktora i imaju podudarnih rijeci medu sobom.

 - Što mislite, hoće li preporuka po sadržaju imati smisla u slučaju ovih šala?
Mislim da hoce (sudeci po proslom primjeru)
 **/
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
        StandardAnalyzer analyzer = new StandardAnalyzer();
        Directory index = new RAMDirectory();

        // Step 2: Creating index
        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        FieldType idFieldType = new FieldType();
        idFieldType.setStored(true);
        idFieldType.setTokenized(false);
        idFieldType.setIndexOptions(IndexOptions.NONE);

        IndexWriter w = new IndexWriter(index, config);
        for (Map.Entry<Integer, String> entry: jokes.entrySet()) {
            Document doc = new Document();
            doc.add(new Field("ID", entry.getKey().toString(), idFieldType));
            doc.add(new TextField("text", entry.getValue(), Field.Store.YES));
            w.addDocument(doc);

            System.out.println(entry.getKey() + " " + entry.getValue());
        }
        w.close();

        // Step 3: Similarity matrix
        IndexReader reader = DirectoryReader.open(index);
        IndexSearcher searcher = new IndexSearcher(reader);

        QueryParser qp = new QueryParser("text", analyzer);

        float[][] sol = new float[jokes.size()][jokes.size()];

        for (Map.Entry<Integer, String> entry: jokes.entrySet()) {
            Query q = qp.parse(QueryParser.escape(entry.getValue()));
            TopDocs docs = searcher.search(q, jokes.size());

            for(ScoreDoc hit: docs.scoreDocs) {
                Document d = searcher.doc(hit.doc);
                sol[entry.getKey() - 1][Integer.parseInt(d.get("ID")) -1] = hit.score;
            }
        }

        // Step 4: normalization
        for (int i = 0; i < jokes.size(); ++i) {
            float div = sol[i][i];
            for (int j = 0; j < jokes.size(); ++j)
                sol[i][j] /= div;
        }

        for (int i = 0; i < jokes.size(); ++i) {
            for (int j = i + 1; j < jokes.size(); ++j) {
                float val = sol[i][j] + sol[j][i];
                val /= 2;
                sol[i][j] = val;
                sol[j][i] = val;
            }
        }

        // Step 5: Writing data
        try(PrintWriter writer = new PrintWriter(new FileWriter("/tmp/item_similarity.csv"))){
            writer.println("ID1,ID2,slicnost");
            for (int i = 0; i < jokes.size(); ++i) {
                for (int j = i + 1; j < jokes.size(); ++j) {
                    if (sol[i][j] > 0) {
                        writer.println((i + 1) + "," + (j + 1) + "," + sol[i][j]);
                    }
                }
            }
        }
    }

}
