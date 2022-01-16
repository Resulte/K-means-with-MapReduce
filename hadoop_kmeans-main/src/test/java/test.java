import java.util.ArrayList;

public class test {
    public static void main(String args[]){
        ArrayList<Double> t = new ArrayList<Double>();
        t.add(1.23);
        t.add(4.66);
        for(Double d:t){
            System.out.println(d);
        }
        for(int i=0;i<t.size();i++){
            t.set(i, t.get(i) / 2);
        }
        for(int i=0;i<t.size();i++){
            System.out.println(t.get(i));
        }
    }
}
