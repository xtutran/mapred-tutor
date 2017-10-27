package com.mapred.core.secondarysort;

import com.mapred.core.GenreKey;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class KeyComparator extends WritableComparator {
    protected KeyComparator() {
        super(GenreKey.class, true);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        GenreKey ip1 = (GenreKey) w1;
        GenreKey ip2 = (GenreKey) w2;

        String mc1 = ip1.getMainCategory();
        String mc2 = ip2.getMainCategory();
        int c = mc1.compareTo(mc2);
        if (c != 0) {
            return c;
        }

        return -GenreKey.compare(ip1.getValue(), ip2.getValue());
    }
}
