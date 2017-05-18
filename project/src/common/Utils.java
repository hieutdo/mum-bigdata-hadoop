package common;

import java.text.DecimalFormat;

public class Utils {
    public static final DecimalFormat decimalFormat = new DecimalFormat("##.###");

    public static double formatDouble(double num) {
        return Double.parseDouble(decimalFormat.format(num));
    }

}
