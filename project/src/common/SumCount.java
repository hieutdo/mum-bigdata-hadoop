package common;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SumCount implements Writable {
    private long sum;
    private int count;

    public SumCount() {
        this.sum = 0;
        this.count = 0;
    }

    public SumCount(long sum, int count) {
        this.sum = sum;
        this.count = count;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(this.sum);
        dataOutput.writeInt(this.count);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.sum = dataInput.readLong();
        this.count = dataInput.readInt();
    }

    public long getSum() {
        return sum;
    }

    public void setSum(long sum) {
        this.sum = sum;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SumCount sumCount = (SumCount) o;

        if (sum != sumCount.sum) return false;
        return count == sumCount.count;
    }

    @Override
    public int hashCode() {
        int result = (int) (sum ^ (sum >>> 32));
        result = 31 * result + count;
        return result;
    }
}
