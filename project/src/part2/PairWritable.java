package part2;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PairWritable implements WritableComparable<PairWritable> {
    private String term;
    private String neighbor;

    public PairWritable() {
        this.term = "";
        this.neighbor = "";
    }

    public PairWritable(String term, String neighbor) {
        this.term = term;
        this.neighbor = neighbor;
    }

    public String getTerm() {
        return term;
    }

    public String getNeighbor() {
        return neighbor;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.term);
        dataOutput.writeUTF(this.neighbor);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.term = dataInput.readUTF();
        this.neighbor = dataInput.readUTF();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PairWritable that = (PairWritable) o;

        if (term != null ? !term.equals(that.term) : that.term != null) return false;
        return neighbor != null ? neighbor.equals(that.neighbor) : that.neighbor == null;
    }

    @Override
    public int hashCode() {
        int result = term != null ? term.hashCode() : 0;
        result = 31 * result + (neighbor != null ? neighbor.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "(" + term + ", " + neighbor + ")";
    }

    @Override
    public int compareTo(PairWritable o) {
        int result = term.compareTo(o.term);
        return result == 0 ? neighbor.compareTo(o.neighbor) : result;
    }
}
