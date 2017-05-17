import java.util.Arrays;

public class BeautifulArray {
    public static void main(String[] args) {
//        System.out.println(isBeautifulArray(new int[]{9, 7, 8, 125, 63, 41}));
        System.out.println(isBeautifulArray(new int[]{9, 7, 10, 125, 63, 41}));
    }

    private static boolean isBeautifulArray(int[] ints) {
        int result = Arrays.stream(ints)
                .map(x -> (x % 6 == 0 || x % 10 == 0 || x % 15 == 0) ? 0 : 1)
                .reduce(1, (x, y) -> x * y);
        return result == 1;
    }
}
