package com.rogers.cdc.api.mutations;

//TODO: Do we really need this class?
public class Column {
	 // ColumnMetadata metadata;
	  public Object value;
	  public String name;
	  
	  public Object getValue() {
		return value;
	}
	public void setValue(Object value) {
		this.value = value;
	}
	
	  //public String type; 
	  protected Column(){
		  
	  }
	  public Column(Object v){
		  value = v;
	  }
	  @Override
	    public boolean equals(Object ob) {
	        if (ob == null) return false;
	        if (ob.getClass() != getClass()) return false;
	        if (ob == this) return true;
	 
	  	   Column other = (Column)ob;
	  	   return this.value.equals(other.getValue());
	  	  
	    }

      
}
