package com.zqh.cep.esper.tutorial;

import com.alibaba.fastjson.JSON;

/**
 * Creating a Java Event Class
 *
 * Java classes are a good choice for representing events, however Map-based or XML event representations can also be good choices depending on
 * your architectural requirements.
 *
 * A sample Java class that represents an order event is shown below. A simple plain-old Java class that provides getter-methods for access to
 * event properties works best:
 *
 * @author doctor
 *
 * @time 2015年5月28日 下午3:59:02
 */
public class OrderEvent {
    private String itemName;
    private double price;

    public OrderEvent(String itemName, double price) {
        this.itemName = itemName;
        this.price = price;
    }

    public String getItemName() {
        return itemName;
    }

    public double getPrice() {
        return price;
    }

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }
}
