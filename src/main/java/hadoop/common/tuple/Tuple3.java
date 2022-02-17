package hadoop.common.tuple;

import hadoop.common.utils.CasterUtil;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Tuple3 implements Tuple {

    public Integer _1;

    public Integer _2;


    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this._1);
        out.writeInt(this._2);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this._1 = in.readInt();
        this._2 = in.readInt();
    }

    public Tuple3(Integer _1, Integer _2) {
        this._1 = _1;
        this._2 = _2;
    }

    public Tuple3() {
    }

    @Override
    public String toString() {
        return "Tuple3{" +
                "_1=" + _1 +
                ", _2=" + _2 +
                '}';
    }
}
