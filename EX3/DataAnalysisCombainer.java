package advanced.customwritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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

public class DataAnalysisCombainer extends Reducer<Text, DataAnalysisWritable, Text, DataAnalysisWritable> {
    // Funcao de reduce
    public void reduce(Text key, Iterable<DataAnalysisWritable> values, Context context)
            throws IOException, InterruptedException {

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

