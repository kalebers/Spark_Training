package advanced.customwritable;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DataAnalysisReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
            InterruptedException {

        // Contador
        int cont = 0;

        // Loop
        for (IntWritable v : values) {
            cont += v.get();

            }

            // Resultado final
            context.write(key, new IntWritable(cont));
        }
    }





