package com.zqh.zookeeper.dist.barrier;

import java.util.Random;

/**
 * 启动三个线程, 首先完成三次create, 当三个线程都分别create后, 再分别执行delete.
 * 在Barrier中使用mutex保证create都完成之后才能进行delete.
 * @author http://agapple.iteye.com/blog/1111377
 *
 */
public class BarrierTest {

    public static void main(String args[]) throws Exception {
        for (int i = 0; i < 3; i++) {
        	BarrierProcess p = new BarrierProcess("Thread-" + i, new Barrier("/test/barrier", 3));
            p.start();
        }
    }
}

class BarrierProcess extends Thread {

    private String  name;
    private Barrier barrier;

    public BarrierProcess(String name, Barrier barrier){
        this.name = name;
        this.barrier = barrier;
    }

    @Override
    public void run() {
        try {
            barrier.enter(name);
            System.out.println(name + " enter");
            Thread.sleep(1000 + new Random().nextInt(2000));
            barrier.leave(name);
            System.out.println(name + " leave");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
