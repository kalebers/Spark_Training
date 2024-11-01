package advanced.customwritable;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// Criando valores
public class DataAnalysisWritable implements Writable {

    private int n;
    private float valueWeight;

    public DataAnalysisWritable() {
    }

    public DataAnalysisWritable(int n ,float valueWeight) {
        this.n = n;
        this.valueWeight = valueWeight;
    }

    public int getN() {
        return n;
    }

    public void setN(int n) {
        this.n = n;
    }
    public float getValueWeight() {
        return valueWeight;
    }

    public void setValueWeight(float valueWeight) {
        this.valueWeight = valueWeight;
    }


    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF (String.valueOf (n));
        out.writeUTF (String.valueOf (valueWeight));
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        n = Integer.parseInt (in.readUTF ());
        valueWeight = Float.parseFloat (in.readUTF ());
    }
}
