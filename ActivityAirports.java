package rdd.math;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.Map;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;


public class ActivityAirports {
    public static void main(String args []){
        // Setando o nível de LOG
        Logger.getLogger("org").setLevel(Level.ERROR);

        // habilita o uso de n threads
        SparkConf conf = new SparkConf().setAppName("USA_Activity_Airportss").setMaster("local[*]");

        // cria o contexto da aplicacao
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Carregando arquivo
        JavaRDD<String> rdd = sc.textFile("in/airports.text");

        // Quebrar cada linha em palavras e seleciona as que tem USA
        //JavaRDD<String> resultado = rdd.flatMap(line -> Arrays.asList(line.split(",")).iterator());
        JavaRDD<String> resultado = rdd.filter(line -> Arrays.asList(line.split(",")).get(3).equals("\"United States\""));
        //JavaRDD<Object> resultado = rdd.map(line -> Arrays.asList(line.split(",")).get(3).equals("\"United States\""));

        for(String r:resultado.collect()){
            System.out.println("* "+r);
        }

        // Filtrar para Estados Unidos
        //Map<String, Long> resultado = palavras.filter(palavra -> palavra[2] == "United States");
        //JavaRDD<String> resultado = palavras.
    }

}
