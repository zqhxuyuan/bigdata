package storm.starter.slidingwindow;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SlidingWindowCache<T> implements Serializable {
	private static final long serialVersionUID = 1L;

    //Map的key是slotNumber. 初始化时会创建slotNumber个KV. value为这个slot内的所有数据.
	private Map<Integer,List<T>> tupMap = new HashMap<Integer,List<T>>();
	private int headSlot;
	private int tailSlot;
	private int slotNum;
	
	public SlidingWindowCache(int slotNum){
		if(slotNum<2) throw new IllegalArgumentException("Window length in slots must be at least two (you requested " + slotNum + ")");
		this.slotNum = slotNum;
		
		for(int i = 0; i < slotNum; i++){
			tupMap.put(i, null);
		}
		headSlot = 0;
		tailSlot = (headSlot+1) % this.slotNum;
	}
	
	public void add(T t){
        //当前slot的所有事件记录
		List<T> objs = tupMap.get(headSlot);
		if(objs == null){
			objs = new ArrayList<T>();
		}
        //添加到列表中
		objs.add(t);
        //放到Map里
		tupMap.put(headSlot, objs);
	}
	
	/**
	 * 获取窗口内的消息，并向前移动窗口
	 * @return
	 */
	public List<T> getAndAdvanceWindow(){
		int i = headSlot;
		List<T> windowedTuples = new ArrayList<T>();

        //i为当前slot. 获取map中对应slot的value, 是个List,保存的是tuple list.
		if(tupMap.get(i)!=null){
			windowedTuples.addAll(tupMap.get(i));
		}

        //上面只是获取当前slot内的tuple list, 还要获取其他slot的tuple list.  即最终获取到的是整个窗口内所有slot的tuple list.
		while((i = slotAfter(i))!=headSlot){
			if(tupMap.get(i)!=null){
				windowedTuples.addAll(tupMap.get(i));
			}
		}
		
		advanceHead();
		return windowedTuples;
	}
	
	/**
	 * 向前移动窗口
	 */
	private void advanceHead() {
		printList(tupMap.get(headSlot));
	    headSlot = tailSlot;
        //把下一个slot的全部数据清空.
	    wipeSlot(headSlot);
	    tailSlot = slotAfter(tailSlot);
	}
	
	public int slotAfter(int slot){
		return (slot+1) % slotNum;
	}
	
	public void wipeSlot(int slot){
		tupMap.put(slot, null);
	}

	public static void main(String[] args){
		int slotNum = 3;
		SlidingWindowCache<String> slideWindow = new SlidingWindowCache<String>(slotNum);
		List<String> windowList = null;
		
		for (int i = 0 ; i < 19; i++){
			String str = new String("message"+i);
			
			if(i==4){
				windowList = slideWindow.getAndAdvanceWindow();
				slideWindow.printList(windowList);
			}else if(i==8){
				windowList = slideWindow.getAndAdvanceWindow();
				slideWindow.printList(windowList);
			}else if(i==12){
				windowList = slideWindow.getAndAdvanceWindow();
				slideWindow.printList(windowList);
			}else if(i==16){
				windowList = slideWindow.getAndAdvanceWindow();
				slideWindow.printList(windowList);
			}
			slideWindow.add(str);
		}
		
		windowList = slideWindow.getAndAdvanceWindow();
		slideWindow.printList(windowList);
	}
	
	
	public void printList(List<T> list){
		StringBuilder sb = new StringBuilder();
		if(list!=null){
			for(T str: list){
				sb.append(str).append("\t");
			}
		}
		System.out.println(sb.toString());
	}
}
