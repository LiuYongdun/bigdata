package hadoop.common.utils;

import java.lang.reflect.Method;

public class CasterUtil {

    public static Object cast(Class<?> clazz, String value) throws Exception {

        if(clazz.getName().equals("java.lang.String"))
            return value;

        if(clazz.getSuperclass().getName().equals("java.lang.Number")){
            Method valueOf = clazz.getMethod("valueOf", String.class);
            valueOf.setAccessible(true);

            return valueOf.invoke(clazz, value);
        }

        throw new Exception(String.format("unable to cast string %s to class %s", value, clazz.getName()));
    }
}
