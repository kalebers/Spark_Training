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

public class DataAnalysisReducer extends Reducer<CompositeKey, DataAnalysisWritable, CompositeKey, DataAnalysisWritable>{

    public void reduce(CompositeKey key, Iterable<DataAnalysisWritable> values, Context context) throws IOException,
            InterruptedException {

        // Inciando os valores para pegar o maior preço
        float max_Cost = Float.MIN_VALUE;

        // Loop
        for (DataAnalysisWritable v : values) {
            if (v.getCost () > max_Cost){
                max_Cost = v.getCost ();
            }
        }
        // Resultado final
        context.write(key, new DataAnalysisWritable (max_Cost));
    }
}





