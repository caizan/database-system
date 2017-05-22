import java.io.*;

public class task3 {
    public static void main(String[] args) throws IOException{
        rangeBF rangeBF = new rangeBF(0.1);

        BufferedReader stream = new BufferedReader(new FileReader("vpetri/exercise1/file1.tsv"));
        BufferedReader queries = new BufferedReader(new FileReader("vpetri/exercise1/task3/task3_queries.txt"));
        BufferedWriter out = new BufferedWriter(new FileWriter("vpetri/exercise1/task3/out3.txt"));

        String query = queries.readLine();
        String update = stream.readLine();

        int ipSource = ipToInt(update.split("\t")[0]);
        boolean response = false;
        int left = ipToInt(query.split("\t")[0]);
        int right = ipToInt(query.split("\t")[1]);

        while (update != null){
            rangeBF.insertValue(ipSource);
            update = stream.readLine();
            if (update != null && update.length()>1){
                ipSource = ipToInt(update.split("\t")[0]);
            }
        }
        while (query!=null){
            response = rangeBF.existsInRange(left,right);
            if (response)
                out.write("1");
            else
                out.write("0");
            query = queries.readLine();
            if (query != null && query.length()>1){
                left = ipToInt(query.split("\t")[0]);
                right = ipToInt(query.split("\t")[1]);
                out.write(",");
            }
        }
        stream.close();
        queries.close();
        out.close();
    }

    public static int ipToInt(String IP){
        int toReturn = Integer.MIN_VALUE;
        String[] parts = IP.split("\\.");

        for (int i = 0; i < 4; i++) {
            toReturn += Integer.parseInt(parts[3-i])* Math.pow(256,i);
        }
        return toReturn;
    }
}
