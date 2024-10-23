import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Calendar;
import java.util.List;
import java.util.stream.IntStream;

public class Main {
    public static boolean isInCircle(Double x, Double y) {
        return Math.sqrt(x*x + y*y) <= 1;
    }

    public static void main(String[] args) {
        System.out.println("program initialized");
        long startTime = Calendar.getInstance().getTimeInMillis();


        // Paso 1: Abrir una conexión con el cluster de Spark
        //      El obj JavaSparkContext es el que refleja una conexión con el cluster
        //      Para crearlo necesitamos suministrar un objeto del tipo SparkConf

        final SparkConf connectionConfig = new SparkConf();
        connectionConfig.setAppName("PiCalc");
        connectionConfig.setMaster("local[2]"); // Típicamente spark://IP:7077

        JavaSparkContext connection = new JavaSparkContext(connectionConfig);
        // Para local, poner "local" y arranca cada vez q le doy a play. Opcional... "local[n]" selecciona cuantos cores usa.
        // Paso 2: Convertir mis datos en un objeto RDD
        //      Los RDD se generan desde la conexión al cluster.
        Integer N = 100000000;
        List<Integer> dartThrows = List.of(N);
        JavaRDD<Integer> throwsRDD = connection.parallelize(dartThrows);

        // Paso 3: Aplicar map-reduce

        long n = throwsRDD.flatMap(numberOfThrows -> IntStream.range(0, N).iterator())
                .map(throwNumber -> new Double[] { Math.random(), Math.random() })
                .filter(coordinates -> Main.isInCircle(coordinates[0], coordinates[1]))
                .count();

        // Paso 4: Procesar el resultado, o sea, guardarlo en una BDD, fichero, imprimirlo, etc.

        System.out.println("(funcional) Pi vale masomeno: " + (double) 4*n/N);
        System.err.println((double) (Calendar.getInstance().getTimeInMillis() - startTime)/1000);

        // Paso 5: Cerrar conexión con el cluster
        connection.close();
    }
}
