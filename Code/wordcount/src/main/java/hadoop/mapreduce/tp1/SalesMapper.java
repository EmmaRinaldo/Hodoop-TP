package hadoop.mapreduce.tp1;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class SalesMapper extends Mapper<Object, Text, Text, FloatWritable> {

    private Text store = new Text();
    private FloatWritable cost = new FloatWritable();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // On convertit la ligne en chaîne de caractères
        String line = value.toString();
        
        // On découpe la ligne selon les tabulations ("\t")
        // Si vos données sont séparées par des espaces, utilisez split(" ") ou split("\\s+")
        String[] fields = line.split("\t");

        // On vérifie qu'on a bien assez de colonnes pour éviter les erreurs
        if (fields.length > 4) {
            String storeName = fields[2]; // La colonne Magasin
            float price = Float.parseFloat(fields[4]); // La colonne Cout

            store.set(storeName);
            cost.set(price);

            context.write(store, cost);
        }
    }
}