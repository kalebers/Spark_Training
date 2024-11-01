package advanced.customwritable;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

public class DataAnalysisReducer extends Reducer<CompositeKey, DataAnalysisWritable, CompositeKey, FloatWritable>{

    public void reduce(CompositeKey key, Iterable<DataAnalysisWritable> values, Context context) throws IOException,
            InterruptedException {

        // Inciando os valores para pegar o número de pesos e o valor dos pesos
        float n = 0.0f;
        float nWeight = 0.0f;

        // Loop
        for (DataAnalysisWritable v : values) {
            n += v.getN ();
            nWeight += v.getValueWeight ();
        }
        // Resultado final
        context.write(key, new FloatWritable (nWeight/n));
    }
}





