package priv.geekliu.graduation.struct;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

@SuppressWarnings("hiding")
public class PartitionTable<Long, Data> extends HashMap<Long, Data>{
	private static final long serialVersionUID = 1L;
	private List<Data> hotSet = null;
	public PartitionTable(int hotDataSize){
		this.hotSet = new ArrayList<>();
	}
	public void putInto(Data record){
		hotSet.add(record);
	}
	public List<Data> getHotSet(){	
		return hotSet;
	}
}
