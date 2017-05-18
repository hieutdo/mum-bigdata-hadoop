package common;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Pair implements WritableComparable<Pair> {
    private String left;
    private String right;

    public Pair() {
        this.left = "";
        this.right = "";
    }

    public Pair(String left, String right) {
        this.left = left;
        this.right = right;
    }

    public String getLeft() {
        return left;
    }

    public String getRight() {
        return right;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.left);
        dataOutput.writeUTF(this.right);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.left = dataInput.readUTF();
        this.right = dataInput.readUTF();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Pair that = (Pair) o;

        if (left != null ? !left.equals(that.left) : that.left != null) return false;
        return right != null ? right.equals(that.right) : that.right == null;
    }

    @Override
    public int hashCode() {
        int result = left != null ? left.hashCode() : 0;
        result = 31 * result + (right != null ? right.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "(" + left + ", " + right + ")";
    }

    @Override
    public int compareTo(Pair o) {
        int result = left.compareTo(o.left);
        return result == 0 ? right.compareTo(o.right) : result;
    }
}
