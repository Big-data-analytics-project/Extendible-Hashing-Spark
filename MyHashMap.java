import java.io.Serializable;
import java.util.*;


public class MyHashMap<K,V> implements Serializable{
    private List<Data<K,V>> datalist;

    //constructor
    public MyHashMap(){
        this.datalist=new ArrayList<Data<K,V>>();
    }

    public void addData(Data<K,V> x){
        for(int i=0; i<datalist.size(); i++){
            Data temp=datalist.get(i);
            if(temp.key.equals(x.key)){ //check if the same key exists before adding it.
                datalist.remove(i);
                break;
            }
        }
        datalist.add(x);
    }

    public V getData(K key){ //return data based on key
        for(int i=0; i<this.datalist.size(); i++){
            Data temp = datalist.get(i);
            if (key.equals(temp.key)) {
                return (V) temp.value;
            }
        }
        return null;
    }

    public int getSize(){
        return datalist.size();
    }

    @Override
    public String toString() {
        return "" + datalist;
    }
}