import datastructrue.BinaryTree;

import java.util.*;

public class JavaTest {
    public static void main(String[] args) {
        BinaryTree<String, String> tree = new BinaryTree<>();

        Scanner scanner = new Scanner(System.in);
        System.out.println("waiting for input ...");
        while (true){
            String next = scanner.next();
            tree.put(next,next);
            tree.iterateTree();
            tree.show();
        }
    }
}


class Solution {
    public boolean canPartitionKSubsets(int[] nums, int k) {
        Arrays.sort(nums);
        HashMap<Integer, Integer> usedMap = new HashMap<>();

        int sum = Arrays.stream(nums).sum();
        if(sum%k!=0)
            return false;

        int result=sum/k;

        for (int i = nums.length - 1; i >= 0; i--) {
            int curValue=nums[i];
            usedMap.put(i,i);
            int idx=i-1;
            while (idx>0){
                if(usedMap.get(idx)!=null){
                    if(curValue+nums[idx]>result){
                        idx-=1;
                    }
                    if(curValue+nums[idx]<=result){
                        curValue=curValue+nums[idx];
                        usedMap.put(idx,idx);
                    }
                }
            }

            if(curValue!=result)
                return false;
        }

        return true;

    }
}