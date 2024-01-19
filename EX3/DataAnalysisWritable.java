package advanced.customwritable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DataAnalysisWritable implements Writable {

    private float quantity;

    public DataAnalysisWritable() {
    }

    public DataAnalysisWritable(float quantity) {
        this.quantity = quantity;
    }

    public float getQuantity() {
        return quantity;
    }

    public void setQuantity(float quantity) {
        this.quantity = quantity;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(String.valueOf(quantity));
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        quantity = Float.parseFloat(in.readUTF());
    }

    @Override
    public String toString() {
        return "DataAnalysisWritable{" +
                "quantity=" + quantity +
                '}';
    }
}
