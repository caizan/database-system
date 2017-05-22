import java.io.*;

public class task2 {
    public static void main (String[] args) throws IOException {
        betterFrequencyEstimator frequencyEstimator = new betterFrequencyEstimator((int)Math.pow(10,7),(float)0.1,(float)0.01,(float)0.9);
        int timer = 0;

        BufferedReader stream = new BufferedReader(new FileReader("vpetri/exercise1/file1.tsv"));
        BufferedReader queries = new BufferedReader(new FileReader("vpetri/exercise1/task2/task2_queries.txt"));
        BufferedWriter out = new BufferedWriter(new FileWriter("vpetri/exercise1/task2/out2.txt"));

        String query = queries.readLine();
        String update = stream.readLine();

        int queryTimer = Integer.parseInt(query.split("\t")[0]);
        int ipSource = ipToInt(update.split("\t")[0]);
        int response = 0;

        while (update != null && query != null){
            if (timer == queryTimer){
                response = frequencyEstimator.getFreqEstimation(ipToInt(query.split("\t")[1]));
                out.write(Integer.toString(response));
                query = queries.readLine();
                if (query != null && query.length()>1){
                    out.write(",");
                    queryTimer = Integer.parseInt(query.split("\t")[0]);
                }
            }
            frequencyEstimator.addArrival(ipSource);
            timer++;
            update = stream.readLine();
            if (update != null && update.length()>1){
                ipSource = ipToInt(update.split("\t")[0]);
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
