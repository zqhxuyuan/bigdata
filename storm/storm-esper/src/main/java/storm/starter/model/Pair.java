package storm.starter.model;

import java.io.Serializable;
import java.util.*;

/**
 * Created by zhengqh on 15/9/21.
 */
public class Pair<L,R> implements Serializable{
    private L left;
    private R right;

    public Pair(L left, R right) {
        this.left = left;
        this.right = right;
    }

    public L getLeft() {
        return left;
    }

    public void setLeft(L left) {
        this.left = left;
    }

    public R getRight() {
        return right;
    }

    public void setRight(R right) {
        this.right = right;
    }

    @Override
    public String toString() {
        return "[" + left.toString() + "," + right.toString() + "]";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Pair<L, R> pair = (Pair<L, R>) o;
        return Objects.equals(getLeft(), pair.getLeft()) &&
                Objects.equals(getRight(), pair.getRight());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getLeft(), getRight());
    }

    public static void main(String[] args) {
        Pair<String,String> p1 = new Pair<>("a","b");
        Pair<String,String> p2 = new Pair<>("a","b");
        System.out.println(p1 == p2); //false

        List<Pair<String,String>> list = new ArrayList<>();
        list.add(p1);
        list.add(p2);
        System.out.println(list.size()); //2

        Set<Pair<String,String>> set = new HashSet<>();
        for(Pair p : list){
            set.add(p);
        }
        System.out.println(set.size());  //1
    }
}
