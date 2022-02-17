package hadoop.common.tuple;

import hadoop.common.utils.CasterUtil;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Tuple2<V1, V2> implements Tuple{

    public V1 _1;

    public V2 _2;

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(String.valueOf(this._1));
        out.writeUTF(String.valueOf(this._2));

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        try {
            this._1 = (V1) CasterUtil.cast(this._1.getClass(), in.readUTF());
            this._2 = (V2) CasterUtil.cast(this._2.getClass(), in.readUTF());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public Tuple2(V1 _1, V2 _2) {
        this._1 = _1;
        this._2 = _2;
    }

    @Override
    public String toString() {
        return "Tuple2{" +
                "_1=" + _1 +
                ", _2=" + _2 +
                '}';
    }

    public Tuple2(){}


}
