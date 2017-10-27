package com.mapred.core.secondarysort;

import com.mapred.core.GenreKey;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupComparator extends WritableComparator {
    protected GroupComparator() {
        super(GenreKey.class, true);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        GenreKey ip1 = (GenreKey) w1;
        GenreKey ip2 = (GenreKey) w2;

        int c = ip1.getMainCategory().compareTo(ip2.getMainCategory());
        if (c != 0) {
            return c;
        }
        return ip1.getGenre().compareTo(ip2.getGenre());

    }
}
