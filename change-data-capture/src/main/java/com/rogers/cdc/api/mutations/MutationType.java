package com.rogers.cdc.api.mutations;

public enum MutationType {
	UNKNOWN("UnknowMutation"), 
	UPDATE("UpdateMutation"),
	INSERT("InsertMutation"),
	DELETE("DeleteMutation"),
	PKUPDATE("PkUpdateMutation");
	
	private final String name;
	  private MutationType(final String name) {
	        this.name = name;
	    }
	  @Override
	    public String toString() {
	        return name;
	    }
}
