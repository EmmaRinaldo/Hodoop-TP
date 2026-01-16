package hadoop.mapreduce.tp1;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class SalesReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {

    private FloatWritable result = new FloatWritable();

    public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
        float sum = 0;
        
        // On parcourt toutes les ventes pour ce magasin spécifique
        for (FloatWritable val : values) {
            sum += val.get();
        }
        
        result.set(sum);
        context.write(key, result);
    }
}
