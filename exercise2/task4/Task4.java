import org.apache.commons.math3.geometry.partitioning.BSPTreeVisitor;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;


public class Task4 {

    public static void main(String[] args) {
        String inputFileCustomer = args[0];
        String inputFileOrders = args[1];
        String output = args[2];

        String master = "local[4]";

        /*
	     * Initializes a Spark context.
	     */
        SparkConf conf = new SparkConf()
                .setAppName(Task4.class.getName())
                .setMaster(master);

        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        // RDD construction
        JavaRDD<String[]> customers = sc.textFile(inputFileCustomer).map(string -> string.split("\\|"));
        JavaRDD<String[]> orders = sc.textFile(inputFileOrders).map(string -> string.split("\\|"));

        Map<String,String> joinKey = customers.mapToPair(strings -> new Tuple2<>(strings[0],strings[0])).collectAsMap();
        JavaRDD<String> results = orders.filter(strings -> joinKey.containsKey(strings[1])).map(strings ->joinKey.get(strings[1])+","+ strings[8]);

        FileUtils.deleteQuietly(new File("temporaryOutput"));
        results.coalesce(1).saveAsTextFile("temporaryOutput");
        File wrongPlaceOutput = new File("temporaryOutput/part-00000");
        File realOutput = new File(output);
        if (realOutput.exists()){
            realOutput.delete();
        }
        wrongPlaceOutput.renameTo(realOutput);
        FileUtils.deleteQuietly(new File("temporaryOutput"));
    }
}