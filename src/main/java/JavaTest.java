import java.util.ArrayList;
import java.util.LinkedList;
import java.util.regex.Pattern;

public class JavaTest {

    public static void main(String[] args) {
        Solution solution = new Solution();
        System.out.println(solution.myAtoi("403"));
    }




}
class Solution {
    public int myAtoi(String s) {
        String trimS = s.trim();
        String[] splits = trimS.split("");

        if(splits.length==0)
            return 0;

        boolean isPositive = !splits[0].equals("-");
        int startIdx=splits[0].matches("[0-9]")?0:1;

        ArrayList<String> nums = new ArrayList<>();

        for (int i = startIdx; i < splits.length; i++) {
            if(splits[i].matches("[0-9]"))
                nums.add(0,splits[i]);
            else
                break;
        }

        Double result=0.0;

        for (int i = 0; i < nums.size(); i++) {
            result+=(nums.get(i).toCharArray()[0]-'0')*Math.pow(10,i);
        }

        result=isPositive?result:-result;
        if(result<=-Math.pow(2,31))
            return (int)-Math.pow(2,31);
        if(result>=Math.pow(2,31)-1)
            return (int)Math.pow(2,31)-1;
        return (int)result.longValue();
    }
}