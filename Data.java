public class Data<K,V> {
    public K key;
    public V value;

    public Data(K key, V value){
        this.key = key;
        this.value = value;
    }

    public void setKey(K key){
        this.key = key;
    }

    public K getKey(){
        return this.key;
    }

    public void setValue(V value){
        this.value = value;
    }

    public V getValue(){
        return this.value;
    }

    @Override
    public String toString() {
        return  "(key=" + key + ", value=" + value + ")";
    }
}
