import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Mapper {
    private String filename;
    private List<KeyValuePair<String, Integer>> output;
    private Map<Integer, List<KeyValuePair<String, Integer>>> reducerMap = new HashMap<>();

    public Mapper(String filename) throws IOException {
        this.filename = filename;
    }

    public String getInputContent() throws IOException {
        return Files.lines(Paths.get(this.filename)).collect(Collectors.joining("\n"));
    }

    public void map() throws IOException {
        this.output = Files.lines(Paths.get(this.filename))
                .map(line -> line.replace("-", " ").split("\\s"))
                .flatMap(Arrays::stream)
                .filter(word -> {
                    if (!word.isEmpty() && word.matches("[^0-9_]+")) {
                        final int periodIndex = word.indexOf(".");
                        return periodIndex < 0 || periodIndex == word.length() - 1;
                    }
                    return false;
                })
                .map(word -> word.replaceAll("[^a-zA-Z]", "").toLowerCase())
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()))
                .entrySet()
                .stream()
                .map(entry -> new KeyValuePair<>(entry.getKey(), entry.getValue().intValue()))
                .collect(Collectors.toList());
    }

    public List<KeyValuePair<String, Integer>> getOutput() {
        return output;
    }

    public Map<Integer, List<KeyValuePair<String, Integer>>> getReducerMap() {
        return reducerMap;
    }
}
