package spark.streaming.tp22;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import java.util.concurrent.TimeoutException;
import java.util.Arrays;

public class Stream {
    public static void main(String[] args) throws StreamingQueryException, TimeoutException {
        // Initialisation de la session [cite: 539-544]
        SparkSession spark = SparkSession
                .builder()
                .appName("NetworkWordCount")
                .master("local[*]")
                .getOrCreate();

        // Lecture du flux (Pages 16-17) [cite: 547-555]
        Dataset<String> lines = spark
                .readStream()
                .format("socket")
                .option("host", "localhost")
                .option("port", 9999)
                .load()
                .as(Encoders.STRING());

        // Découpage en mots (Page 17) [cite: 557-560]
        Dataset<String> words = lines.flatMap(
                (String x) -> Arrays.asList(x.split(" ")).iterator(),
                Encoders.STRING());

        // Comptage (Page 17) [cite: 561-563]
        Dataset<Row> wordCounts = words.groupBy("value").count();

        // Affichage (Page 17) [cite: 566-571]
        StreamingQuery query = wordCounts.writeStream()
                .outputMode("complete")
                .format("console")
                .trigger(Trigger.ProcessingTime("1 second"))
                .start();

        query.awaitTermination();
    }
}