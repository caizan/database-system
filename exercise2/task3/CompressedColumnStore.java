import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

public class CompressedColumnStore {
    private static class StringWithType implements java.io.Serializable{
        private String string;
        private String type;

        private StringWithType(String string, String type) {
            this.string = string;
            this.type = type;
        }

        private StringWithType(){
            this.string = "";
            this.type = "";
        }

        private int compareTo(String value){
            int result;
            switch (type) {
                case "Int":
                    Integer intNumber = Integer.parseInt(this.string);
                    Integer intValue = Integer.parseInt(value);
                    result = intNumber.compareTo(intValue);
                    break;
                case "Float":
                    Float floatNumber = Float.parseFloat(this.string);
                    Float floatValue = Float.parseFloat(value);
                    result = floatNumber.compareTo(floatValue);
                    break;
                default:
                    result = this.string.compareTo(value);
                    break;
            }
            return result;
        }

        private boolean testWith(String operator, String value){
            int test = this.compareTo(value);
            switch(operator){
                case ("="):return test == 0;
                case ("<="):return test <= 0;
                case (">="):return test >= 0;
                case ("<"):return test < 0;
            }
            return test > 0;
        }
    }

    public static void main(String[] args) {
        String input = args[0];
        String output = args[1];
        String schema = args[2];
        String projectionList = args[3];
        String whereList = args[4];
        String compressedCol = args[5];

        String[] temp = schema.split(",");

        HashMap<String,Integer> schemeIndex = new HashMap<>();
        String[][] realSchema = new String[temp.length][];
        for (int i = 0; i < temp.length; i++) {
            realSchema[i] = temp[i].split(":");
            schemeIndex.put(realSchema[i][0],i);
        }

        String[] realProjectionList = projectionList.split(",");
        temp = whereList.split(",");
        String[][] realWhereList = new String[temp.length][];
        for (int i = 0; i < temp.length; i++) {
            realWhereList[i] = temp[i].split("\\|");
        }

        String master = "local[4]";

        /*
	     * Initializes a Spark context.
	     */
        SparkConf conf = new SparkConf()
                .setAppName(ColumnStore.class.getName())
                .setMaster(master);
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        // RDD construction
        JavaRDD<String> lines = sc.textFile(input);
        JavaPairRDD<StringWithType,Long>[] splits = new JavaPairRDD[realSchema.length];
        for (int i = 0; i < realSchema.length; i++) {
            final int j = i;
            if (realSchema[i][1].equals("String")){
                JavaPairRDD<StringWithType,Long> split = lines.map(s -> new StringWithType(s.split(",")[j],"String")).zipWithIndex();
                splits[j] = split;
            }else if (realSchema[i][1].equals("Int")){
                JavaPairRDD<StringWithType,Long> split = lines.map(s -> new StringWithType(s.split(",")[j],"Int")).zipWithIndex();
                splits[j] = split;
            }else{
                JavaPairRDD<StringWithType,Long> split = lines.map(s -> new StringWithType(s.split(",")[j],"Float")).zipWithIndex();
                splits[j] = split;
            }
        }

        int compressedColIndex = schemeIndex.get(compressedCol);
        Map<String, Integer> dictionary;
        dictionary = splits[compressedColIndex].mapToPair(tuple ->new Tuple2<String,Integer>(tuple._1().string,tuple._1().string.hashCode())).collectAsMap();
        JavaPairRDD<Integer, Long> compressed = splits[compressedColIndex].mapToPair(tuple->new Tuple2<Integer, Long>(dictionary.get(tuple._1().string),tuple._2()));

        JavaPairRDD<StringWithType,Long>[] filteredRdd = new JavaPairRDD[realWhereList.length];
        JavaPairRDD<Long, StringWithType>[] filteredRddKeyFirst = new JavaPairRDD[realWhereList.length];
        for (int i = 0; i < realWhereList.length; i++) {
            String[] strings = realWhereList[i];
            if (!(strings[0].equals(compressedCol)&&strings[1].equals("="))){
                int columnIndex = schemeIndex.get(strings[0]);
                filteredRdd[i] = splits[columnIndex].filter(value -> value._1().testWith(strings[1],strings[2]));
            } else {
                Integer value = dictionary.get(strings[2]);
                filteredRdd[i] = compressed.filter(tuple -> tuple._1() == value).mapToPair(tuple -> new Tuple2<StringWithType, Long>(new StringWithType(),tuple._2()));
            }

        }
        for (int i = 0; i < filteredRdd.length; i++) {
            filteredRddKeyFirst[i] = filteredRdd[i].mapToPair(value -> new Tuple2<>(value._2(),value._1()));
        }
        JavaPairRDD<Long,String> result = filteredRddKeyFirst[0].mapToPair(tuple -> new Tuple2<>(tuple._1(),tuple._2().string));
        JavaPairRDD<Long,Tuple2<String, StringWithType>> intermediaryResult = null;
        for (int i = 1; i < filteredRddKeyFirst.length; i++) {
            intermediaryResult = result.join(filteredRddKeyFirst[i]);
            result = intermediaryResult.mapToPair(tuple-> new Tuple2<>(tuple._1(),""));
        }
        for (int i = 0; i < realProjectionList.length; i++) {
            int columnNumber = schemeIndex.get(realProjectionList[i]);
            intermediaryResult = result.join(splits[columnNumber].mapToPair(value->new Tuple2<>(value._2(),value._1())));
            result = intermediaryResult.mapToPair(tuple-> new Tuple2<>(tuple._1(),tuple._2()._1()+","+tuple._2()._2().string));
        }
        JavaRDD<String> finalResult = result.map(tuple -> tuple._2().substring(1));

        FileUtils.deleteQuietly(new File("temporaryOutput"));
        finalResult.coalesce(1).saveAsTextFile("temporaryOutput");
        File wrongPlaceOutput = new File("temporaryOutput/part-00000");
        File realOutput = new File(output);
        if (realOutput.exists()){
            realOutput.delete();
        }
        wrongPlaceOutput.renameTo(realOutput);
        FileUtils.deleteQuietly(new File("temporaryOutput"));
    }
}
