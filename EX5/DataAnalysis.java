package advanced.customwritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;
import java.io.IOException;

public class DataAnalysis {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        // Arquivo de entrada
        Path input = new Path(files[0]);

        // Arquivo de saída
        Path output = new Path(files[1]);

        // Criacao do job e seu nome
        Job j = new Job(c, "dataanalysis-estudante");

        // Registrando classe
        j.setJarByClass(DataAnalysis.class);
        j.setMapperClass(DataAnalysisMapper.class);
        j.setReducerClass(DataAnalysisReducer.class);

        // Definição do tipo de saída
        j.setOutputKeyClass(CompositeKey.class);
        j.setOutputValueClass(FloatWritable.class);

        // Arquivos de entrada e saída
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // Lanca o job e aguarda sua execucao
        System.exit(j.waitForCompletion(true ) ? 0 : 1);

    }

}
