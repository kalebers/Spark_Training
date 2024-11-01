package advanced.customwritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

public class DataAnalysisCombainer extends Reducer<CompositeKey, DataAnalysisWritable , CompositeKey, DataAnalysisWritable>{

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

