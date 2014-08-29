package org.pentaho.di.essbase.core;

public class DimensionField {
	
	private String DimensionName;
	private String FieldName;
	private String FieldType;
	
	public DimensionField() {
		
	}
	
	public DimensionField(String dimensionName, String fieldName, String fieldType) {
		this.DimensionName=dimensionName;
		this.FieldName=fieldName;
		this.FieldType=fieldType;
	}
	
	public String getDimensionName() {
		return this.DimensionName;
	}
	
	public String getFieldName() {
		return this.FieldName;
	}
	
	public String getFieldType() {
		return this.FieldType;
	}

}
