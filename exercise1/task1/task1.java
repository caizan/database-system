import java.io.*;

public class task1 {
    public static void main (String[] args) throws IOException{
        JumpingWindow jumpingWindow = new JumpingWindow(10000,0.01);
        int timer = 0;

        BufferedReader stream = new BufferedReader(new FileReader("vpetri/exercise1/file1.tsv"));
        BufferedReader queries = new BufferedReader(new FileReader("vpetri/exercise1/task1/task1_queries.txt"));
        BufferedWriter out = new BufferedWriter(new FileWriter("vpetri/exercise1/task1/out1.txt"));

        String query = queries.readLine();
        String update = stream.readLine();

        int queryTimer = Integer.parseInt(query.split("\t")[0]);
        int ipSource = Integer.parseInt(update.split("\\.")[0]);
        int response = 0;

        while (update != null && query != null){
            if (timer == queryTimer){
                if (Integer.parseInt(query.split("\t")[2]) == 0){
                    response = jumpingWindow.getFreqEstimation(Integer.parseInt(query.split("\t")[1].split("\\.")[0]));
                } else {
                    response = jumpingWindow.getFreqEstimation(Integer.parseInt(query.split("\t")[1].split("\\.")[0]),Integer.parseInt(query.split("\t")[2]));
                }
                out.write(Integer.toString(response));
                query = queries.readLine();
                if (query != null && query.length()>1){
                    out.write(",");
                    queryTimer = Integer.parseInt(query.split("\t")[0]);
                }
            }
            jumpingWindow.insertEvent(ipSource);
            timer++;
            update = stream.readLine();
            if (update != null && update.length()>1){
                ipSource = Integer.parseInt(update.split("\\.")[0]);
            }
        }
        stream.close();
        queries.close();
        out.close();
    }
}
