package hadoop;

import hadoop.common.tuple.Tuple3;
import hadoop.common.utils.CasterUtil;

public class Main {
    public static void main(String[] args) throws Exception {
        Integer cast = (Integer)CasterUtil.cast(Integer.class, "1");
        System.out.println(cast);
    }
}
