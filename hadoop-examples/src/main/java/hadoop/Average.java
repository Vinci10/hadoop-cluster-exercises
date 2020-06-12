package hadoop;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Average implements Writable {

    private float number;
    private int count;

    public Average() {
        super();
    }

    public Average(float number, int count) {
        this.number = number;
        this.count = count;
    }

    public float getNumber() {
        return number;
    }

    public int getCount() {
        return count;
    }

    @Override
    public String toString() {
        return String.valueOf(number);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        WritableUtils.writeString(dataOutput, String.valueOf(number));
        WritableUtils.writeVInt(dataOutput, count);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        number = Float.parseFloat(WritableUtils.readString(dataInput));
        count = WritableUtils.readVInt(dataInput);
    }
}