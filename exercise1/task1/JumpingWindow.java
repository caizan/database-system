import java.awt.*;
import java.security.Key;
import java.util.ArrayList;

public class JumpingWindow {
    private int windowSizeW;
    private double epsilon;
    private int subWindowSizeWsub;
    private int subWindowNumber;
    private int[][] Sums;
    private int runningSumCounter;
    private int KeyNumber = 255;

    public JumpingWindow(int windowSizeW, double epsilon) {
        this.windowSizeW = windowSizeW;
        this.epsilon = epsilon;
        this.subWindowSizeWsub =(int) (2*epsilon*windowSizeW);
        this.subWindowNumber = windowSizeW / subWindowSizeWsub + 1;
        this.Sums = new int[KeyNumber][subWindowNumber];
        this.runningSumCounter = 0;

    }

    void insertEvent(int srcIP){
        runningSumCounter++;
        if (runningSumCounter%subWindowSizeWsub ==0){
            for (int i = 0; i < KeyNumber; i++) {
                for (int j = subWindowNumber - 1; j > 0; j--) {
                    Sums[i][j] = Sums[i][j-1];
                }
                Sums[i][0]=0;
            }
            runningSumCounter = 0;
        }
        Sums[srcIP][0]++;
    }


    int getFreqEstimation(int srcIP, int queryWindowSizeW1){
        int WindowsNumber = queryWindowSizeW1 / subWindowSizeWsub;
        int sumToReturn = 0;
        for (int i = 0; i < WindowsNumber; i++) {
            sumToReturn += Sums[srcIP][i];
        }
        return sumToReturn + Sums[srcIP][WindowsNumber]/2;
    }


    int getFreqEstimation(int srcIP){
        int sumToReturn = 0;
        for (int i = 0; i < subWindowNumber - 1; i++) {
            sumToReturn += Sums[srcIP][i];
        }
        return sumToReturn + Sums[srcIP][subWindowNumber - 1] / 2;
    }
}
