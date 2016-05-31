package com.zqh.elasticsearch.example;

/**
 * Created by hadoop on 15-1-5.
 */
public class User {

    public int age;
    public String name;
    public long weight;
    public boolean married;

    public User(){
        this.age=11;
        this.name = "default";
        this.weight = 1000l;
        this.married = false;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getWeight() {
        return weight;
    }

    public void setWeight(long weight) {
        this.weight = weight;
    }

    public boolean isMarried() {
        return married;
    }

    public void setMarried(boolean married) {
        this.married = married;
    }
}
