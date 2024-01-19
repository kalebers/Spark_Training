package advanced.customwritable;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.io.StringWriter;

public class DataAnalysisMapper extends Mapper<LongWritable, Text, CompositeKey, IntWritable> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // Obtendo linha
        String linha = value.toString();

        // Quebrando em colunas
        String[] colunas = linha.split(";");

        // Verifica se a coluna é igual a year ou flow, se for atribui uma string vazia
        if (colunas[1].equals ("year") || colunas[4].equals ("flow")) {
            colunas[1] = "";
            colunas[4] = "";

        }else{

            // Criando a chave composta
            CompositeKey cntry = new CompositeKey (colunas[4], colunas[1]);

            // Escrevendo chave composta = flow, year, valor = 1
            context.write (cntry, new IntWritable (1));
        }
    }
}



