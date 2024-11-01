package advanced.customwritable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.io.StringWriter;

public class DataAnalysisMapper extends Mapper<Object, Text, Text, IntWritable> {

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        // Obtendo linha
        String linha = value.toString();

        // Quebrando em colunas
        String[] colunas = linha.split(";");

        // Selecionando apenas mercadorias do Brazil
        if (colunas[0].equals("Brazil")) {

            String merchandise = colunas[3];

            // Chave = merchandise, value: 1
            context.write(new Text(merchandise), new IntWritable(1));
        }
    }
}






