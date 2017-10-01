import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.*;

public class JSONConverter {

    public static void main(String[] args) throws IOException, JSONException {

        JSONArray jsonArray = new JSONArray();

        BufferedReader br = new BufferedReader(new FileReader("path_to_your_output_part-r-00000"));
        String line = br.readLine();
        FileWriter fileWriter = new FileWriter("path_you_want_to_store_result.json");

        BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);

        while (line != null) {
            JSONObject article = new JSONObject();
            String[] title_emotion_count = line.split("\t");
            JSONObject emotionList = new JSONObject();

            emotionList.put(title_emotion_count[1], title_emotion_count[2]);
            article.put("title", title_emotion_count[0]);
            for (int i = 0; i < 2; i++) {
                line = br.readLine();
                title_emotion_count = line.split("\t");
                emotionList.put(title_emotion_count[1], title_emotion_count[2]);
            }
            article.put("data", emotionList);
            jsonArray.put(article);

            line = br.readLine();
        }

        bufferedWriter.write(jsonArray.toString());

        br.close();
        bufferedWriter.close();

    }
}
