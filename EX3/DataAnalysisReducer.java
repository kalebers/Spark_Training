package advanced.customwritable;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class DataAnalysisReducer extends Reducer<Text, DataAnalysisWritable, Text, DataAnalysisWritable> {

    public void reduce(Text key, Iterable<DataAnalysisWritable> values, Context context) throws IOException,
            InterruptedException {

        // Maior valor
        float max_flow = Float.MIN_VALUE;


        // Loop
        for (DataAnalysisWritable v : values) {
            if (v.getQuantity() > max_flow){
                max_flow = v.getQuantity();
            }
        }

        // Resultado final
        context.write(key, new DataAnalysisWritable(max_flow));
    }
}





