import java.util.Arrays;

public class WaveArray {
    public static boolean isWaveArray(int[] arr) {
        int result = Arrays.stream(arr)
                .map(x -> (x % 2) + 2)
                .reduce(1, (x, y) -> {
                    if (x == 0 || x == y) {
                        return 0;
                    }
                    return y;
                });
        return result != 0;
    }

    public static void main(String[] args) {
        System.out.println(isWaveArray(new int[]{1, 2, 3, 4}));
        System.out.println(isWaveArray(new int[]{1, 3, 3, 4}));
    }
}
