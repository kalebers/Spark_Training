package advanced.customwritable;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.io.StringWriter;

public class DataAnalysisMapper extends Mapper<LongWritable, Text, CompositeKey, DataAnalysisWritable> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // Obtendo linha
        String linha = value.toString();

        // Quebrando em colunas
        String[] colunas = linha.split(";");

        // Fazendo um tratamento para substituir os valores vazio para 0 e verificar se contém a string weight_kg
        if (colunas[6].equals ("") || colunas[6].equals ("weight_kg")){
            colunas[6] = "0";
        }else {

            // Criando os valores
            float weight = Float.parseFloat (colunas[6]);
            DataAnalysisWritable val = new DataAnalysisWritable (1, weight);

            // Criando a chave composta
            CompositeKey cntry = new CompositeKey (colunas[1], colunas[3]);

            // Escrevendo chave composta = year, merchandise, valor = weight
            context.write (cntry, val);
        }
    }
}



