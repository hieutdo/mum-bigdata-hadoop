import java.util.ArrayList;
import java.util.List;

public class GroupByPair<K, V> {
    private K key;
    private List<V> values;

    public GroupByPair(K key) {
        this.key = key;
        this.values = new ArrayList<>();
    }

    public K getKey() {
        return key;
    }

    public void setKey(K key) {
        this.key = key;
    }

    public List<V> getValues() {
        return values;
    }

    public void setValues(List<V> values) {
        this.values = values;
    }

    public void addValue(V v) {
        this.values.add(v);
    }

    @Override
    public String toString() {
        return "< " + key + " , " + values + " >";
    }
}
