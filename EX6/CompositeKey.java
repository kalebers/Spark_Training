package advanced.customwritable;

import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// Criando as chaves compostas
public class CompositeKey implements WritableComparable<CompositeKey> {
    String merchandise;
    String weight;

    public CompositeKey() {
    }

    public CompositeKey(String merchandise, String weight) {
        this.merchandise = merchandise;
        this.weight = weight;
    }
    // Escrevendo as chaves
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeString (out, merchandise);
        WritableUtils.writeString (out, weight);
    }
    // Lendo as chaves
    public void readFields(DataInput in) throws IOException {
        this.merchandise = WritableUtils.readString (in);
        this.weight = WritableUtils.readString (in);
    }
    // Comparando para ver se exite valores vazios
    public int compareTo(CompositeKey pop) {
        if (pop == null)
            return 0;
        int intcnt = merchandise.compareTo (pop.merchandise);
        return intcnt == 0 ? weight.compareTo (pop.weight) : intcnt;
    }

    @Override
    public String toString() {
        return merchandise.toString () + " peso: " + weight.toString ();
    }
}
