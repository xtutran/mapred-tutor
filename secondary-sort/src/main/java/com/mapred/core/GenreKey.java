package com.mapred.core;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author tutx1
 */
public class GenreKey implements WritableComparable<GenreKey> {

    private String mainCategory;
    private String genre;
    private double value;

    public GenreKey() {
    }

    public GenreKey(String mainCategory, String genre) {
        this.mainCategory = mainCategory;
        this.genre = genre;
    }

    public void set(GenreKey key) {
        setMainCategory(key.mainCategory);
        setGenre(key.genre);
    }

    public void set(String mainCategory, String genre) {
        this.mainCategory = mainCategory;
        this.genre = genre;
    }

    @Override
    public String toString() {
        if (value > 0) {
            return mainCategory + ";" + genre + ";" + value;
        } else {
            return mainCategory + ";" + genre;
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // TODO Auto-generated method stub
        out.writeUTF(mainCategory);
        out.writeUTF(genre);
        out.writeDouble(value);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        // TODO Auto-generated method stub
        mainCategory = in.readUTF();
        genre = in.readUTF();
        value = in.readDouble();
    }

    @Override
    public int compareTo(GenreKey o) {

        int c = this.mainCategory.compareTo(o.mainCategory);
        if (c != 0) {
            return c;
        }
        c = this.genre.compareTo(o.genre);
        if (c != 0) {
            return c;
        }

        return compare(this.value, o.value);
    }

    public String getMainCategory() {
        return mainCategory;
    }

    public void setMainCategory(String mainCategory) {
        this.mainCategory = mainCategory;
    }

    public String getGenre() {
        return genre;
    }

    public void setGenre(String genre) {
        this.genre = genre;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }

    public static int compare(double a, double b) {
        return (a < b ? -1 : (a == b ? 0 : 1));
    }
}
