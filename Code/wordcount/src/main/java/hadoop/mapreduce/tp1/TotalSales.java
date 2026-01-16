package hadoop.mapreduce.tp1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TotalSales {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Total Sales Per Store");
        
        job.setJarByClass(TotalSales.class);
        
        // On définit nos nouvelles classes Mapper et Reducer
        job.setMapperClass(SalesMapper.class);
        job.setCombinerClass(SalesReducer.class); // On peut utiliser le même reducer comme combiner ici
        job.setReducerClass(SalesReducer.class);
        
        // ATTENTION : Le type de la clé reste Text, mais la valeur est maintenant FloatWritable
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}