package advanced.customwritable;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DataAnalysisReducer extends Reducer<CompositeKey, IntWritable, CompositeKey, IntWritable>{

    public void reduce(CompositeKey key, Iterable<IntWritable> values, Context context) throws IOException,
            InterruptedException {

        // Inciando os valores do count
        int count = 0;

        // Loop
        for (IntWritable v : values) {
            count += v.get ();
        }
        // Resultado final
        context.write(key, new IntWritable (count));
    }
}





