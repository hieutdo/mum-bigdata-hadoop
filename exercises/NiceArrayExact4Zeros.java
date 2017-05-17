import java.util.Arrays;

class NiceArrayExact4Zeros {
    static boolean isNiceArray(int[] arr) {
        int result = Arrays.stream(arr)
                .map(x -> x == 0 ? 1 : 0)
                .reduce(0, (x, y) -> {
                    if ((x == 4 && y == 1) || (x < 4 && y == 0)) return 0;
                    return x + y;
                });
        System.out.println(result);
        return result == 4;
    }

    public static void main(String[] args) {
//        System.out.println(isNiceArray(new int[]{0, 0, 0, 0}));
//        System.out.println(isNiceArray(new int[]{0, 0, 0, 1, 2, 3, 0, 0, 0, 0}));
//        System.out.println(isNiceArray(new int[]{0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0}));
//        System.out.println("-------------------------");
        System.out.println(isNiceArray(new int[]{0, 0, 0, 0, 0, 1, 2, 0, 0, 0, 0, 0}));
//        System.out.println(isNiceArray(new int[]{0, 0, 0}));
//        System.out.println(isNiceArray(new int[]{1, 2, 3}));
    }
}