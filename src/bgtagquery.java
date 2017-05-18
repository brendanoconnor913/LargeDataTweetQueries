import clarifai2.api.ClarifaiBuilder;
import clarifai2.api.ClarifaiClient;
import clarifai2.dto.input.ClarifaiInput;
import clarifai2.dto.input.image.ClarifaiImage;
import clarifai2.dto.model.output.ClarifaiOutput;
import clarifai2.dto.prediction.Concept;
import org.apache.commons.lang.ArrayUtils;
import org.apache.derby.iapi.services.io.ArrayUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import org.json.*;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Created by brendan on 4/13/17.
 */
public class bgtagquery {
    // function used to parse json to get the background image url from the tweet
    private static String jsonparse(String s) {
        String url = "";
        JSONObject jo = new JSONObject(s);
        url = jo.getJSONObject("user").get("profile_use_background_image").toString();
        if(url.equals("true")) {
            url = jo.getJSONObject("user").get("profile_background_image_url_https").toString();
        }
        else {
            url = "";
        }
        return url;
    }

    // function to check if tweet has a background image on the profile (from tweet)
    private static boolean hasImage(String s) {
        String url = "";
        JSONObject jo = new JSONObject(s);
        url = jo.getJSONObject("user").get("profile_use_background_image").toString();
        if(url.equals("true")) {
            url = jo.getJSONObject("user").get("profile_background_image_url_https").toString();
            return !url.equals("null");
        }
        else {
            return false;
        }
    }

    // function to check if the background image is one of twitters default images
    private static boolean notDefaultTheme(String s) {
        String url = "";
        JSONObject jo = new JSONObject(s);
        url = jo.getJSONObject("user").get("profile_background_image_url_https").toString();
        return !url.contains("https://abs.twimg.com/images/themes/");
    }

    // Calls api on background image url and determines if it has a person centric tag
    private static Integer containsPerson(String s) {
        String[] keywords = {"women", "woman", "man","people","together",
        "face","person","sexy","girl","boy","adult","romance","family","young"};
        String url = jsonparse(s);
        try {
            ClarifaiClient client = new ClarifaiBuilder("wnM3yiH8oLs0nVgeDumXGUDPk6XXRGm7wNrKL-Dk", "aqGNEQHcZg0AgdSnWXqoL1JuqO0RxOpw8QF4gsWH").buildSync();

            final List<ClarifaiOutput<Concept>> predictionResults =
                    client.getDefaultModels().generalModel()
                            .predict()
                            .withInputs(
                                    ClarifaiInput.forImage(ClarifaiImage.of(url))
                            )
                            .executeSync()
                            .get();

            for(ClarifaiOutput<Concept> co : predictionResults) {
                for(int i = 0; i < 4; i++) {
                    String label = co.data().get(i).name();
                    if(ArrayUtils.contains(keywords, label)) {
                        client.close();
                        System.out.println("Person background Identified");
                        return 1;
                    }
                }
            }
            client.close();
            System.out.println("Non-person background Identified");
            return 0;
        }
        catch(Exception e) {
            System.err.println("One of the API calls failed");
        }
        return 0;
    }

    // helper function to determine if tweet has hashtag
    private static boolean hasHashTags(String s) {
        String ht = "";
        JSONObject jo = new JSONObject(s);
        ht = jo.getJSONObject("entities").get("hashtags").toString();
        if(!ht.equals("[]")) {
            return !ht.equals("null");
        }
        else {
            return false;
        }
    }

    // Extracts the hashtags from the tweet
    private static String[] getHTs(String s) {
        JSONObject jo = new JSONObject(s);
        JSONArray ahts = jo.getJSONObject("entities").getJSONArray("hashtags");
        String[] hts = new String[ahts.length()];
        for(int i = 0;i<ahts.length();i++) {
            JSONObject fhts = new JSONObject(ahts.get(i).toString());
            hts[i] = fhts.get("text").toString();
        }
        return hts;
    }

    public static void main(String args[]) {
        // initialize environment
        JavaSparkContext sc = new JavaSparkContext("local", "bgquery");
        JavaRDD<String> textFile = sc.textFile("data/output.txt");

        // Filter out tweets w/o image or w/ default themes
        JavaPairRDD<String, Integer> categorized = textFile
                .filter(s -> hasImage(s))
                .filter(s -> notDefaultTheme(s))
                .mapToPair(s -> new Tuple2<>(s, containsPerson(s)));

        // get "person" labeled tweets and their HT then product a sorted count for ht usage
        JavaPairRDD<String, Integer> peopleHTs = categorized
                .filter(t ->(t._2==1))
                .filter(t ->hasHashTags(t._1))
                .flatMap(t -> Arrays.asList(getHTs(t._1)).iterator())
                .mapToPair(ht -> new Tuple2<>(ht, 1))
                .reduceByKey((a,b) -> a + b)
                // sort by value
                .mapToPair(t -> new Tuple2<>(t._2, t._1))
                .sortByKey(false)
                .mapToPair(t -> new Tuple2<>(t._2, t._1));

        // same as above but with non person background
        JavaPairRDD<String, Integer> nonPeopleHTs = categorized
                .filter(t ->(t._2==0))
                .filter(t ->hasHashTags(t._1))
                .flatMap(t -> Arrays.asList(getHTs(t._1)).iterator())
                .mapToPair(ht -> new Tuple2<>(ht, 1))
                .reduceByKey((a,b) -> a + b)
                .mapToPair(t -> new Tuple2<>(t._2, t._1))
                .sortByKey(false)
                .mapToPair(t -> new Tuple2<>(t._2, t._1));

        // save output
        peopleHTs.saveAsTextFile("topPersonHTs");
        nonPeopleHTs.saveAsTextFile("topNonPersonHTs");
    }
}
