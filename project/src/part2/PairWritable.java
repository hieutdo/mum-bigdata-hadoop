package part2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PairWritable implements WritableComparable<PairWritable> {
    private Text w;
    private Text u;

    public PairWritable() {
        this.w = new Text();
        this.u = new Text();
    }

    public PairWritable(Text w, Text u) {
        this.w = w;
        this.u = u;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.w.write(dataOutput);
        this.u.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.w.readFields(dataInput);
        this.u.readFields(dataInput);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PairWritable that = (PairWritable) o;

        if (w != null ? !w.equals(that.w) : that.w != null) return false;
        return u != null ? u.equals(that.u) : that.u == null;
    }

    @Override
    public int hashCode() {
        int result = w != null ? w.hashCode() : 0;
        result = 31 * result + (u != null ? u.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "(" + w + ", " + u + ")";
    }

    @Override
    public int compareTo(PairWritable o) {
        int result = w.compareTo(o.w);
        return result == 0 ? u.compareTo(o.u) : result;
    }
}
