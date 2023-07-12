/*
 * Copyright (c) 2010 Pentaho Corporation.  All rights reserved. 
 * This software was developed by Pentaho Corporation and is provided under the terms 
 * of the GNU Lesser General Public License, Version 2.1. You may not use 
 * this file except in compliance with the license. If you need a copy of the license, 
 * please go to http://www.gnu.org/licenses/lgpl-2.1.txt. The Original Code is Pentaho 
 * Data Integration.  The Initial Developer is Pentaho Corporation.
 *
 * Software distributed under the GNU Lesser Public License is distributed on an "AS IS" 
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or  implied. Please refer to 
 * the license for the specific language governing your rights and limitations.
 */
package org.pentaho.di.trans.steps.essbase.dataoutput;
/*
 *   This file is part of EssbaseKettlePlugin.
 */

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Counter;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.essbase.core.DimensionField;
import org.pentaho.di.essbase.core.EssbaseHelper;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.shared.SharedObjectInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.w3c.dom.Node;

@Step(id = "EssbaseDataOutput", image = "EssbaseDataOutput.svg", name = "Essbase Data Output", description="", categoryDescription="Oracle Essbase")
public class EssbaseDataOutputMeta extends BaseStepMeta 
    implements StepMetaInterface {
    
    private DatabaseMeta databaseMeta;
    private String app = "";
    private String cube = "";
    private String commitSize="1";
    private String measureType = "";
    private boolean clearCube;
    private List< DimensionField > fields = new ArrayList < DimensionField >();
    private DimensionField measureField = new DimensionField();
    
    
    public EssbaseDataOutputMeta() {
        super();
    }
    
    /**
     * @return Returns the database.
     */
    public final DatabaseMeta getDatabaseMeta() {
        return databaseMeta;
    }
    
    /**
     * @param database The database to set.
     */
    public final void setDatabaseMeta(final DatabaseMeta database) {
        this.databaseMeta = database;
    }
    
    public final void loadXML(final Node stepnode, 
            final List < DatabaseMeta > databases, 
            final Map < String, Counter > counters) throws KettleXMLException {
        readData(stepnode, databases);
    }

    public final Object clone() {
        EssbaseDataOutputMeta retval = (EssbaseDataOutputMeta) super.clone();
        return retval;
    }
    
    private void readData(final Node stepnode, 
            final List < ? extends SharedObjectInterface > databases)
            throws KettleXMLException {
        try {
        	String con = XMLHandler.getTagValue(stepnode, "connection"); //$NON-NLS-1$
        		databaseMeta = DatabaseMeta.findDatabase(databases, con);
            this.app = XMLHandler.getTagValue(stepnode, "app");
            this.cube = XMLHandler.getTagValue(stepnode, "cube");
            this.commitSize = XMLHandler.getTagValue(stepnode, "commit");
            measureType = XMLHandler.getTagValue(stepnode, "measuretype"); 
            clearCube = XMLHandler.getTagValue(stepnode, "clearcube").equals("Y") ? true : false; 
            this.fields = new ArrayList < DimensionField >();
            
            Node fields = XMLHandler.getSubNode(stepnode,"fields");
            int nrFields = XMLHandler.countNodes(fields,"field");
            for (int i=0;i<nrFields;i++) {
                Node fnode = XMLHandler.getSubNodeByNr(fields, "field", i);
                String dimensionName = XMLHandler.getTagValue(fnode, "dimensionname");
                String fieldName = XMLHandler.getTagValue(fnode, "fieldname");
                String fieldType = XMLHandler.getTagValue(fnode, "fieldtype");
                this.fields.add(new DimensionField(dimensionName,fieldName,fieldType));
            }
            
            Node measures = XMLHandler.getSubNode(stepnode,"measures");
            int nrMeasures = XMLHandler.countNodes(measures,"measure");
            
            // v4 code review (matt): 
            // --------------------------
            //   It's a bit weird to break in the for loop. 
            //   I think this was done for compatibility reasons.
            //
            for (int i=0;i<nrMeasures;) {
                Node fnode = XMLHandler.getSubNodeByNr(measures, "measure", i);
                String measureName = XMLHandler.getTagValue(fnode, "measurename");
                String fieldName = XMLHandler.getTagValue(fnode, "measurefieldname");
                String fieldType = XMLHandler.getTagValue(fnode, "measurefieldtype");
                this.measureField = new DimensionField(measureName,fieldName,fieldType);
                break;
            }
            
            
        } catch (Exception e) {
            throw new KettleXMLException("Unable to load step info from XML", e);
        }
    }

    public void setDefault() {
    }

    public final void getFields(final RowMetaInterface row, final String origin, 
            final RowMetaInterface[] info, final StepMeta nextStep, 
            final VariableSpace space) throws KettleStepException {
        //if (databaseMeta == null) 
        //    throw new KettleStepException("There is no Palo database server connection defined");
        

        //final PaloHelper helper = new PaloHelper(databaseMeta);
        //try {
        //    helper.connect();
        //    try {
        //        RowMetaInterface rowMeta = helper.getDataRowMeta(this.cube, this.fields, this.measureField);
        //        row.addRowMeta(rowMeta);
        //    } finally {
        //        helper.disconnect();
        //    }
        //} catch (Exception e) {
        //    throw new KettleStepException(e);
        //}
    }

    public final String getXML() {
        StringBuffer retval = new StringBuffer();
        
        retval.append("    ").append(XMLHandler.addTagValue("connection", databaseMeta == null ? "" : databaseMeta.getName()));
        retval.append("    ").append(XMLHandler.addTagValue("app", this.app));
        retval.append("    ").append(XMLHandler.addTagValue("cube", this.cube));
        retval.append("    ").append(XMLHandler.addTagValue("commit", this.commitSize));
        retval.append("    ")
            .append(XMLHandler.addTagValue("measuretype", measureType));
        retval.append("    ")
        .append(XMLHandler.addTagValue("clearcube", clearCube)); 
    
        retval.append("    <fields>").append(Const.CR);
        for (DimensionField field : this.fields) {
            retval.append("      <field>").append(Const.CR);
            retval.append("        ").append(XMLHandler.addTagValue("dimensionname",field.getDimensionName()));
            retval.append("        ").append(XMLHandler.addTagValue("fieldname",field.getFieldName()));
            retval.append("        ").append(XMLHandler.addTagValue("fieldtype",field.getFieldType()));
            retval.append("      </field>").append(Const.CR);
        }
        retval.append("    </fields>").append(Const.CR);
        
        retval.append("    <measures>").append(Const.CR);
        // The two condition checks for the Measure is a workaround for a bug.
        // If you opened a transformation where the measure was not completely defined
        // then Kettle displayed an error saying the argument can not be null.
        if (measureField.getDimensionName() != "") {
            retval.append("      <measure>").append(Const.CR);
            retval.append("        ").append(XMLHandler.addTagValue("measurename",measureField.getDimensionName()));
            if (measureField.getFieldName() == "")
                retval.append("        ").append(XMLHandler.addTagValue("measurefieldname","CHOOSE FIELD"));
            else
                retval.append("        ").append(XMLHandler.addTagValue("measurefieldname",measureField.getFieldName()));
            retval.append("        ").append(XMLHandler.addTagValue("measurefieldtype",measureField.getFieldType()));
            retval.append("      </measure>").append(Const.CR);
        }
        retval.append("    </measures>").append(Const.CR);
            
        return retval.toString();
    }

    public void readRep(Repository rep, ObjectId idStep, List<DatabaseMeta> databases, Map<String, Counter> counters) throws KettleException {
        try {
            databaseMeta = rep.loadDatabaseMetaFromStepAttribute(idStep, "id_connection", databases);
            this.app = rep.getStepAttributeString(idStep, "app");
            this.cube = rep.getStepAttributeString(idStep, "cube");
            this.commitSize = rep.getStepAttributeString(idStep, "commit");
            this.measureType = rep.getStepAttributeString(idStep, "measuretype"); 
            this.clearCube = rep.getStepAttributeBoolean(idStep, "clearcube");
            
            int nrFields = rep.countNrStepAttributes(idStep, "dimensionname");
            
            for (int i=0;i<nrFields;i++) {
                String dimensionName = rep.getStepAttributeString (idStep, i, "dimensionname");
                String fieldName = rep.getStepAttributeString (idStep, i, "fieldname");
                String fieldType = rep.getStepAttributeString (idStep, i, "fieldtype");
                this.fields.add(new DimensionField(dimensionName,fieldName,fieldType));
            }
            
            String measureName = rep.getStepAttributeString(idStep, "measurename");
            String measureFieldName = rep.getStepAttributeString(idStep, "measurefieldname");
            String measureFieldType = rep.getStepAttributeString(idStep, "measurefieldtype");
            this.measureField= new DimensionField(measureName,measureFieldName,measureFieldType);
        } catch (Exception e) {
            throw new KettleException("Unexpected error reading step information from the repository", e);
        }
    }

    public void saveRep(Repository rep, ObjectId idTransformation, ObjectId idStep) throws KettleException {
        try {
            rep.saveDatabaseMetaStepAttribute(idTransformation, idStep, "id_connection", databaseMeta);
            rep.saveStepAttribute(idTransformation, idStep, "app", this.app);
            rep.saveStepAttribute(idTransformation, idStep, "cube", this.cube);
            rep.saveStepAttribute(idTransformation, idStep, "commit", this.commitSize);
            rep.saveStepAttribute(idTransformation, idStep, "measuretype", this.measureType);
            rep.saveStepAttribute(idTransformation, idStep, "clearcube", this.clearCube); 
            rep.saveStepAttribute(idTransformation, idStep, "measurename", this.measureField.getDimensionName());
            rep.saveStepAttribute(idTransformation, idStep, "measurefieldname", this.measureField.getFieldName());
            rep.saveStepAttribute(idTransformation, idStep, "measurefieldtype", this.measureField.getFieldType());
            
            for (int i=0;i<this.fields.size();i++) {
                rep.saveStepAttribute(idTransformation, idStep, i, "dimensionname", this.fields.get(i).getDimensionName());
                rep.saveStepAttribute(idTransformation, idStep, i, "fieldname", this.fields.get(i).getFieldName());
                rep.saveStepAttribute(idTransformation, idStep, i, "fieldtype", this.fields.get(i).getFieldType());
            }
            if (databaseMeta!=null) rep.insertStepDatabase(idTransformation, idStep, databaseMeta.getObjectId());
            
        } catch (Exception e) {
            throw new KettleException("Unable to save step information to the repository for idStep=" + idStep, e);
        }
    }


    public final void check(final List < CheckResultInterface > remarks, 
            final TransMeta transMeta, final StepMeta stepMeta, 
            final RowMetaInterface prev, final String input[], 
            final String output[], final RowMetaInterface info) {
        
        CheckResult cr;
        
        if (databaseMeta != null) {
            cr = new CheckResult(CheckResultInterface.TYPE_RESULT_OK, "Connection exists", stepMeta);
            remarks.add(cr);

            final EssbaseHelper helper = new EssbaseHelper(databaseMeta);
            try {
                helper.connect();
                cr = new CheckResult(CheckResultInterface.TYPE_RESULT_OK, "Connection to database OK", stepMeta);
                remarks.add(cr);
                
                if (!Const.isEmpty(this.app)) {
                    cr = new CheckResult(CheckResultInterface.TYPE_RESULT_OK, "The name of the application is chosen", stepMeta);
                    remarks.add(cr);
                } else {
                    cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, "The name of the application is missing.", stepMeta);
                    remarks.add(cr);
                }

                if (!Const.isEmpty(this.cube)) {
                    cr = new CheckResult(CheckResultInterface.TYPE_RESULT_OK, "The name of the cube is entered", stepMeta);
                    remarks.add(cr);
                } else {
                    cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, "The name of the cube is missing.", stepMeta);
                    remarks.add(cr);
                }
                
                if(this.measureField==null || Const.isEmpty(this.measureField.getFieldName())) {
                    cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, "Measure field is empty.",stepMeta);
                    remarks.add(cr);
                } else {
                    if(Const.isEmpty(this.measureField.getFieldType())){
                        cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, "Measure field type is empty.",stepMeta);
                        remarks.add(cr);
                    }
                }
                if(this.fields == null || this.fields.size()==0) {
                    cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, "Data Output Fields are empty.",stepMeta);
                    remarks.add(cr);
                } else {
                    for(DimensionField field : this.fields) {
                        if(Const.isEmpty(field.getFieldName())) {
                            cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, "Input field for dimension "+field.getDimensionName()+" is empty.",stepMeta);
                            remarks.add(cr);
                        }
                        if(Const.isEmpty(field.getFieldType())) {
                            cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, "Input field type for dimension "+field.getDimensionName()+" is empty.",stepMeta);
                            remarks.add(cr);
                        }
                    }
                }                

            } catch (KettleException e) {
                cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, "An error occurred: " + e.getMessage(), stepMeta);
                remarks.add(cr);
            } finally {
                helper.disconnect();
            }
        } else {
            cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, "Please select or create a connection to use", stepMeta);
            remarks.add(cr);
        }
        
    }

    public final StepInterface getStep(final StepMeta stepMeta, 
            final StepDataInterface stepDataInterface, final int cnr, 
            final TransMeta transMeta, final Trans trans) {
        
        return new EssbaseDataOutput(stepMeta, stepDataInterface, cnr, 
                transMeta, trans);
    }

    public final StepDataInterface getStepData() {
        try {
            return new EssbaseDataOutputData(this.databaseMeta);
        } catch (Exception e) {
            return null;
        }
    }
    
    public final DatabaseMeta[] getUsedDatabaseConnections() {
        if (databaseMeta != null) {
            return new DatabaseMeta[] {databaseMeta};
        } else {
            return super.getUsedDatabaseConnections();
        }
    }
    
    public boolean supportsErrorHandling()
    {
        return true;
    }
    
    public String getApplication() {
        return this.app;
    }

    public String getCube() {
        return this.cube;
    }
    
    public String getCommitSize() {
    	return this.commitSize;
    }

    /**
     * @param cube the cube name to set
     */
    public void setApplication(String app) {
    	this.app=app;
    }
    
    public void setCube(String cube) {
        this.cube = cube;
    }
    public void setCommitSize(String commitSize) {
    	this.commitSize=commitSize;
    }
    public final String getMeasureType() {
        return measureType;
    }
    public final void setMeasureType(String measureType) {
        this.measureType = measureType;
    }
    public List < DimensionField > getFields() {
        return this.fields;
    }
    public void setLevels(List < DimensionField > fields) {
        this.fields = fields; 
    }
    
    public DimensionField getMeasure() {
        return this.measureField;
    }
    public void setMeasureField(DimensionField measureField) {
        this.measureField = measureField; 
    }
    public void setClearCube(boolean create) {
        this.clearCube = create;
    }
    public boolean getClearCube() {
        return this.clearCube;
    }

    public StringBuffer getRowHeader() {
    	StringBuffer rowHeader = new StringBuffer();
    	/*for (int i=0; i<getFields().size(); i++) {
    			rowHeader.append( ""+getFields().get(i).getDimensionName()+" " );
    	}
    	rowHeader.append("*data*\n");*/
    	return rowHeader;
    }
}
