package advanced.customwritable;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// Criando valores
public class DataAnalysisWritable implements Writable {

    private float cost;

    public DataAnalysisWritable() {
    }

    public DataAnalysisWritable(float cost) {
        this.cost = cost;
    }
    // Pegando preço
    public float getCost() {
        return cost;
    }
    // Setando cost
    public void setCost(float cost) {
        this.cost = cost;
    }
    // Escrevendo valores cost
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF (String.valueOf (cost));
    }
    // Lendo valores cost
    @Override
    public void readFields(DataInput in) throws IOException {
        cost = Float.parseFloat (in.readUTF ());
    }

    @Override
    public String toString() {
        return "DataAnalysisWritable{" +
                "cost=" + cost +
                '}';
    }
}
