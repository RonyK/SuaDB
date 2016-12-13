package suadb.query;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import suadb.record.CID;

/**
 * Created by Rony on 2016-12-09.
 */
public class Region
{
	private List<Integer> low;
	private List<Integer> high;
	
	public Region(List<Integer> coordinates)
	{
		int size = coordinates.size();

		low = new ArrayList<>();
		high = new ArrayList<>();
		low.addAll(coordinates.subList(0, size / 2));
		high.addAll(coordinates.subList(size / 2, size));
	}
	
	public Region(List<Integer> low, List<Integer> high)
	{
		this.low = low;
		this.high = high;
	}
	
	public List<Integer> low()
	{
		return low;
	}
	
	public List<Integer> high()
	{
		return high;
	}
	
	@Override
	public String toString()
	{
		String lows = String.join(",", low.stream().map(dim -> Integer.toString(dim)).collect(Collectors.toList()));
		String highs = String.join(",", high.stream().map(dim -> Integer.toString(dim)).collect(Collectors.toList()));
		return lows + "," + highs;
	}
	
	public int compareTo(CID cid)
	{
		List<Integer> coordinate = cid.toList();
		
		for (int i = 0; i < coordinate.size(); i++)
		{
			int value = coordinate.get(i);
			if(value < low.get(i))
			{
				return -1;
			}else if(value > high.get(i))
			{
				return 1;
			}
		}
		
		return 0;
	}
	
	
	@Override
	public boolean equals(Object obj)
	{
		if(obj instanceof Region)
		{
			Region o = (Region)obj;
			return low.equals(o.low) && high.equals(o.high);
		}
		
		return false;
	}
}
