import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class Reducer {
    private List<KeyValuePair<String, Integer>> mapperOutput = new ArrayList<>();
    private List<GroupByPair<String, Integer>> reducerInput = new ArrayList<>();
    private List<KeyValuePair<String, Integer>> reducerOutput = new ArrayList<>();

    public void addMapperOutput(KeyValuePair<String, Integer> pair) {
        mapperOutput.add(pair);
    }

    public List<KeyValuePair<String, Integer>> getReducerOutput() {
        return reducerOutput;
    }

    public List<GroupByPair<String, Integer>> getReducerInput() {
        return reducerInput;
    }

    public void createReducerInput() {
        GroupByPair<String, Integer> groupByPair = null;

        this.mapperOutput.sort(Comparator.comparing(KeyValuePair::getKey));

        for (KeyValuePair<String, Integer> pair : this.mapperOutput) {
            String key = pair.getKey();
            Integer value = pair.getValue();

            if (groupByPair == null || !groupByPair.getKey().equals(key)) {
                groupByPair = new GroupByPair<>(key);
                reducerInput.add(groupByPair);
            }

            groupByPair.addValue(value);
        }
    }

    public void reduce() {
        this.reducerOutput = this.reducerInput.stream()
                .map(groupPair -> {
                    int sum = groupPair.getValues().stream().mapToInt(a -> a).sum();
                    return new KeyValuePair<>(groupPair.getKey(), sum);
                })
                .collect(Collectors.toList());
    }
}
