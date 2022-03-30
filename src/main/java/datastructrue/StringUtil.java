package datastructrue;

public class StringUtil {
    public static String generateSpace(int size){
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < size; i++) {
            sb.append(" ");
        }
        return sb.toString();
    }

    public static String generateSpace(Double size){
        return generateSpace(size.intValue());
    }
}
