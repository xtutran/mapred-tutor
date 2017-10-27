package com.mapred.parser;

public class StringUtils {
    public static String[] split(String s, char delimeter) {

        int count = 1;

        for (int i = 0; i < s.length(); i++)
            if (s.charAt(i) == delimeter)
                count++;

        String[] array = new String[count];

        int a = -1;
        int b = 0;

        for (int i = 0; i < count; i++) {

            while (b < s.length() && s.charAt(b) != delimeter)
                b++;

            array[i] = s.substring(a + 1, b);
            a = b;
            b++;
        }

        return array;
    }

    public static int parseInt(String value, String ignore) {

        if (value.isEmpty() || ignore.equals(value)) {
            return 0;
        }

        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException pe) {
            return 0;
        }
    }
}
