import java.util.*;

public class Skyline {
    public static ArrayList<Tuple> mergePartitions(ArrayList<ArrayList<Tuple>> partitions){
        ArrayList<Tuple> mergedList = new ArrayList<>();
        for (ArrayList<Tuple> partition: partitions)
            mergedList.addAll(partition);
        return mergedList;
    }


    public static ArrayList<Tuple> dcSkyline(ArrayList<Tuple> inputList, int blockSize){
        ArrayList<Tuple> Skyline = new ArrayList<>(inputList);
        while (Skyline.size() > blockSize){
            ArrayList<ArrayList<Tuple>> Partitions = new ArrayList<>();
            ArrayList<ArrayList<Tuple>> tempPartitions = new ArrayList<>();
            for (int i = 0; i < Skyline.size()/blockSize; i++) {
                ArrayList<Tuple> Sublist = new ArrayList<>(Skyline.subList(i*blockSize, (i+1)*blockSize));
                Partitions.add(Sublist);
            }
            Partitions.add(new ArrayList<Tuple>(Skyline.subList(((Skyline.size()/blockSize)*blockSize),Skyline.size())));
            for (ArrayList<Tuple> partition : Partitions) {
                tempPartitions.add(new ArrayList<>(nlSkyline(partition)));
            }
            Skyline = new ArrayList<>(mergePartitions(tempPartitions));
        }
        Skyline = new ArrayList<>(nlSkyline(Skyline));
        return Skyline;
    }

    public static ArrayList<Tuple> nlSkyline(ArrayList<Tuple> partition) {
        ArrayList<Tuple> inputList = new ArrayList<>(partition);
        ArrayList<Tuple> Window = new ArrayList<>();
        for (Tuple tuple : inputList) {
            Iterator<Tuple> iter = Window.iterator();
            boolean survived = true;
            while(iter.hasNext()){
                Tuple candidate = iter.next();
                if (candidate.dominates(tuple)) {
                    survived = false;
                    break;
                }
                if (tuple.dominates(candidate))
                    iter.remove();
            }
            if (survived)
                Window.add(tuple);
        }
        return Window;
    }

}

class Tuple {
    private int price;
    private int age;

    public Tuple(int price, int age){
        this.price = price;
        this.age = age;
    }

    public boolean dominates(Tuple other){
        return (this.getAge() < other.getAge() || this.getPrice() < other.getPrice()) && (this.getAge() <= other.getAge() && this.getPrice() <= other.getPrice());
    }

    public boolean isIncomparable(Tuple other){
        return !this.dominates(other) && !other.dominates(this);
    }

    public int getPrice() {
        return price;
    }

    public int getAge() {
        return age;
    }

    public String toString(){
        return price + "," + age;
    }

    public boolean equals(Object o) {
        if(o instanceof Tuple) {
            Tuple t = (Tuple)o;
            return this.price == t.price && this.age == t.age;
        } else {
            return false;
        }
    }
}