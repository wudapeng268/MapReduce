package nju.edu.cn;


public class LongString implements Comparable<LongString>{

	public String value;
	
	public LongString(String value){
		this.value = value;
	}
		
	public LongString(LongString ls){
		this.value = ls.value;
	}
		
	@Override
	public boolean equals(Object o){
		return this.value.equals(((LongString)o));
	}

	@Override
	public int compareTo(LongString o) {
		if(this.value.equals(o.value)){
			return 0;
		} else if (this.value.length() < o.value.length()
				|| (this.value.length() == o.value.length() && this.value.compareTo(o.value) < 0)){
			return -1;
		} else{
			return 1;
		}
	}
}
