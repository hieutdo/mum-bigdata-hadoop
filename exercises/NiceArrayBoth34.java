import java.util.Arrays;

public class NiceArrayBoth34 {
    public static boolean isNiceArray(int[] arr) {
        int result = Arrays.stream(arr)
                .map(x -> x == 3 || x == 4 ? x : 0)
                .reduce(1, (x, y) -> {
                    if (x == 3 || (x == 4 && y == 0)) return x;
                    return y;

//                    if (y == 3) {
//                        return 2;
//                    }
//                    if (y == 4 && x == 1) {
//                        return 0;
//                    }
//                    return x;
                });
        return result != 4;
    }

    public static void main(String[] args) {
        System.out.println(isNiceArray(new int[]{7, 6, 2, 3, 1}));
        System.out.println(isNiceArray(new int[]{7, 6, 2, 3, 4}));
        System.out.println(isNiceArray(new int[]{7, 6, 2, 4, 3}));
        System.out.println(isNiceArray(new int[]{3, 4, 2, 3, 4, 7, 4}));
        System.out.println(isNiceArray(new int[]{1, 6, 2, 8, 2, 9}));
        System.out.println("-----------");
        System.out.println(isNiceArray(new int[]{4}));
        System.out.println(isNiceArray(new int[]{7, 6, 2, 4, 1}));
    }
}
