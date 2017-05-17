package part1.AverageComputation;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SumCountWritable implements Writable {
    private LongWritable sum;
    private IntWritable count;

    public SumCountWritable() {
        this.sum = new LongWritable();
        this.count = new IntWritable();
    }

    public SumCountWritable(LongWritable sum, IntWritable count) {
        this.sum = sum;
        this.count = count;
    }

    public LongWritable getSum() {
        return sum;
    }

    public IntWritable getCount() {
        return count;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.sum.write(dataOutput);
        this.count.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.sum.readFields(dataInput);
        this.count.readFields(dataInput);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SumCountWritable that = (SumCountWritable) o;

        if (sum != null ? !sum.equals(that.sum) : that.sum != null) return false;
        return count != null ? count.equals(that.count) : that.count == null;
    }

    @Override
    public int hashCode() {
        int result = sum != null ? sum.hashCode() : 0;
        result = 31 * result + (count != null ? count.hashCode() : 0);
        return result;
    }
}
