package advanced.customwritable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.io.StringWriter;

public class DataAnalysisMapper extends Mapper<Object, Text, Text, DataAnalysisWritable> {

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        // Obtendo linha
        String linha = value.toString();

        // Quebrando em colunas
        String[] colunas = linha.split(";");

        if (colunas[0].equals("Brazil") && colunas[1].equals ("2016") && colunas[4].equals ("Import")) {

            String merchandise = colunas[3];
            if (colunas[8].equals("")) {
                colunas[8] = "0";
                System.out.println(colunas[8]);
            } else {
                float quantity = Float.parseFloat (colunas[8]);
                System.out.println("Quantidade: " + colunas[8]);

                // Chave = merchandise, value: quantity
                context.write(new Text(merchandise), new DataAnalysisWritable(quantity));
            }
        }
    }
}



