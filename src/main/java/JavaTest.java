import java.util.LinkedList;

public class JavaTest {

    public static void main(String[] args) {
        LinkedList<String> list=new LinkedList<>();
        list.add("1");
        list.add("2");
        list.add("3");
        list.add("4");

        for (int i = 0; i < list.size(); i++) {
            System.out.println(list.get(i));
        }

        for (int i = list.size() - 1; i >= 0; i--) {
            System.out.println(list.get(i));
        }
    }
}
