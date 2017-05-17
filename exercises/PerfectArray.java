import java.util.Arrays;

public class PerfectArray {
    static boolean isPerfectArray(int[] arr) {
        return Arrays.stream(arr).reduce((x, y) -> {
            if (x == -1 || y <= 0 || y % x != 0) return -1;
            return x;
        }).getAsInt() != -1;
    }

    public static void main(String[] args) {
        System.out.println(isPerfectArray(new int[]{3, 6, 9, 12}));
        System.out.println(isPerfectArray(new int[]{1, 2, 3, 4}));
        System.out.println(isPerfectArray(new int[]{2, 2, 4, 6}));
        System.out.println(isPerfectArray(new int[]{3, 15, -4, 21}));
        System.out.println(isPerfectArray(new int[]{3, 6, 5, 9}));
        System.out.println(isPerfectArray(new int[]{3, 6, 9, 0}));
    }
}
