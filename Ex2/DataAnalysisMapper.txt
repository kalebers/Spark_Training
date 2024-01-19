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

        // Verifica se na coluna[1] contem a string year, se tiver substitui por uma string vazia
        if (colunas[1].equals ("year")){
            colunas[1] = "";
        }else {
            String year = colunas[1];

            // Chave = year, value: 1
            context.write (new Text (year), new IntWritable (1));
        }
    }
}







