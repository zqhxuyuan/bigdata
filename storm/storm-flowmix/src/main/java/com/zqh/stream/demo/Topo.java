package com.zqh.stream.demo;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by zhengqh on 16/6/30.
 */
public class Topo {

    public static void main(String[] args) {
        List<Component> componentList = new ArrayList();
        componentList.add(new Component("A").setDestination("B"));
        componentList.add(new Component("B").setDestination("C").setSource("A"));
        componentList.add(new Component("C").setDestination("D").setSource("B"));
        componentList.add(new Component("D").setSource("C"));

        for(Component c : componentList) {
            System.out.println(c);
        }




//        Builder builder = new Builder();
//        builder.addComponent("A", new Component("A"));
    }

    public static class Builder {

        List<Component> componentList = new ArrayList();

        public Builder addComponent(String name, Component component){
            componentList.add(component);
            return this;
        }
    }
}

class Component {
    private String name;
    private String source;
    private String destination;

    public Component() {
    }

    public Component(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public Component setName(String name) {
        this.name = name;
        return this;
    }

    public String getSource() {
        return source;
    }

    public Component setSource(String source) {
        this.source = source;
        return this;
    }

    public String getDestination() {
        return destination;
    }

    public Component setDestination(String destination) {
        this.destination = destination;
        return this;
    }

    @Override
    public String toString() {
        return this.source + "->[" + this.name + "]->"  + this.destination;
    }
}
