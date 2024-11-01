package advanced.customwritable;

import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// Criando as chaves compostas
public class CompositeKey implements WritableComparable<CompositeKey> {
    String flow;
    String year;

    public CompositeKey() {
    }

    public CompositeKey(String flow, String year) {
        this.flow = flow;
        this.year = year;
    }
    // Escrevendo valores das chaves
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeString (out, flow);
        WritableUtils.writeString (out, year);
    }
    // Lendo valores das chaves
    public void readFields(DataInput in) throws IOException {
        this.flow = WritableUtils.readString (in);
        this.year = WritableUtils.readString (in);
    }
    // Comparando para verificar se existe valores vazios
    public int compareTo(CompositeKey pop) {
        if (pop == null)
            return 0;
        int intcnt = flow.compareTo (pop.flow);
        return intcnt == 0 ? year.compareTo (pop.year) : intcnt;
    }

    @Override
    public String toString() {
        return flow.toString () + ":" + year.toString () + " Quantidade = ";
    }
}
