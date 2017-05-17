import java.util.Arrays;

public class NiceArrayHas4Not3 {
    public static boolean isNiceArray(int[] arr) {
        int result = Arrays.stream(arr)
                .map(x -> x == 3 || x == 4 ? x : 0)
                .reduce(1, (x, y) -> {
                    if (x == -1 || y == 0) return x;
                    if ((x == 3 && y == 4) || (x == 4 && y == 3)) return -1;
                    return y;
                });
//        System.out.println(result);
        return result != -1;
    }

    public static void main(String[] args) {
        System.out.println(isNiceArray(new int[]{7, 6, 2, 3, 1}));
        System.out.println(isNiceArray(new int[]{7, 6, 2, 4, 1}));
        System.out.println(isNiceArray(new int[]{3, 6, 2, 3, 4}));
        System.out.println(isNiceArray(new int[]{3, 4, 2, 3, 4, 7, 4}));
        System.out.println(isNiceArray(new int[]{1, 6, 2, 8, 2, 9}));
        System.out.println(isNiceArray(new int[]{4}));
        System.out.println(isNiceArray(new int[]{7, 6, 2, 4, 1}));
//        System.out.println("-----------");
    }
}
