import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class WordCount {
    private int numOfInputSplits;
    private int numOfReducers;

    private List<Mapper> mappers = new ArrayList<>();
    private List<Reducer> reducers = new ArrayList<>();

    public WordCount(int numOfInputSplits, int numOfReducers) throws IOException {
        this.numOfInputSplits = numOfInputSplits;
        this.numOfReducers = numOfReducers;

        for (int i = 0; i < numOfReducers; i++) {
            reducers.add(new Reducer());
        }

        for (int i = 0; i < numOfInputSplits; i++) {
            Mapper mapper = new Mapper("w1d3/input" + i + ".txt");
            mapper.map();

            List<KeyValuePair<String, Integer>> output = mapper.getOutput();
            Map<Integer, List<KeyValuePair<String, Integer>>> reducerMap = mapper.getReducerMap();

            for (KeyValuePair<String, Integer> pair : output) {
                int reducerIndex = getPartition(pair.getKey());

                reducers.get(reducerIndex).addMapperOutput(pair);

                if (!reducerMap.containsKey(reducerIndex)) {
                    reducerMap.put(reducerIndex, new ArrayList<>());
                }
                reducerMap.get(reducerIndex).add(pair);
            }

            this.mappers.add(mapper);
        }
    }

    public void print() throws IOException {
        int numOfMappers = this.mappers.size();

        System.out.println("Number of Input-Splits: " + this.numOfInputSplits);
        System.out.println("Number of Reducers: " + this.numOfReducers);

        for (int i = 0; i < numOfMappers; i++) {
            System.out.println("Mapper " + i + " Input");
            System.out.println(this.mappers.get(i).getInputContent());
        }

        for (int i = 0; i < numOfMappers; i++) {
            System.out.println("Mapper " + i + " Output");
            this.mappers.get(i).getOutput().forEach(System.out::println);
        }

        for (int i = 0; i < numOfMappers; i++) {
            Map<Integer, List<KeyValuePair<String, Integer>>> reducerMap = this.mappers.get(i).getReducerMap();
            for (int j = 0; j < this.numOfReducers; j++) {
                System.out.println("Pairs send from Mapper " + i + " Reducer " + j);
                if (reducerMap.get(j) != null) {
                    reducerMap.get(j).forEach(System.out::println);
                }
            }
        }

        for (int i = 0; i < this.numOfReducers; i++) {
            Reducer reducer = this.reducers.get(i);
            reducer.createReducerInput();

            System.out.println("Reducer " + i + " input");
            reducer.getReducerInput().forEach(System.out::println);
        }

        for (int i = 0; i < this.numOfReducers; i++) {
            Reducer reducer = this.reducers.get(i);
            reducer.reduce();

            System.out.println("Reducer " + i + " output");
            reducer.getReducerOutput().forEach(System.out::println);
        }
    }

    public int getPartition(String key) {
        return key.hashCode() % this.numOfReducers;
    }

    public static void main(String[] args) throws IOException {
//        org.myorg.WordCount wc = new org.myorg.WordCount(3, 4);
//        wc.print();
    }
}
