package advanced.customwritable;

import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// Criando as chaves compostas
public class CompositeKey implements WritableComparable<CompositeKey> {
    String merchandise;
    String year;

    public CompositeKey() {
    }
    public CompositeKey(String merchandise, String year) {
        this.merchandise = merchandise;
        this.year = year;
    }
    // Escrevendo valores das chaves
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeString (out, merchandise);
        WritableUtils.writeString (out, year);
    }
    // Lendo valores das chaves
    public void readFields(DataInput in) throws IOException {
        this.merchandise = WritableUtils.readString (in);
        this.year = WritableUtils.readString (in);
    }
    // Comprando se existe valores vazios
    public int compareTo(CompositeKey pop) {
        if (pop == null)
            return 0;
        int intcnt = merchandise.compareTo (pop.merchandise);
        return intcnt == 0 ? year.compareTo (pop.year) : intcnt;
    }

    @Override
    public String toString() {
        return merchandise.toString () + ":" + year.toString ();
    }
}
