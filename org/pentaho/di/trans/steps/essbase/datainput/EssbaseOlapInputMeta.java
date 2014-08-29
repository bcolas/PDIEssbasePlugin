/*******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2012 by Pentaho : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.di.trans.steps.essbase.datainput;

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
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.essbase.core.DimensionField;
import org.pentaho.di.essbase.core.EssbaseHelper;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.shared.SharedObjectInterface;
import org.pentaho.di.trans.DatabaseImpact;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.steps.essbase.dataoutput.EssbaseDataOutputData;
import org.w3c.dom.Node;

/*
 * Created on 2-jun-2003
 *
 */
@Step(id = "EssbaseOlapInput", image = "EssbaseDataInput.png", name = "Essbase Olap Input", description="", categoryDescription="Oracle Essbase")
public class EssbaseOlapInputMeta extends BaseStepMeta implements StepMetaInterface
{
	private String mdx;
    private DatabaseMeta databaseMeta;
    private String app = "";
    private String cube = "";
	private EssbaseOlapInputData data;
	
    private boolean variableReplacementActive;
    public EssbaseOlapInputMeta()
	{
		super();
	}
	
    /**
     * @return Returns the variableReplacementActive.
     */
    public boolean isVariableReplacementActive()
    {
        return variableReplacementActive;
    }

    /**
     * @param variableReplacementActive The variableReplacementActive to set.
     */
    public void setVariableReplacementActive(boolean variableReplacementActive)
    {
        this.variableReplacementActive = variableReplacementActive;
    }

	
	
	
	public void loadXML(Node stepnode, List<DatabaseMeta> databases, Map<String, Counter> counters)
		throws KettleXMLException
	{
		readData(stepnode, databases);
	}

	public Object clone()
	{
		EssbaseOlapInputMeta retval = (EssbaseOlapInputMeta)super.clone();
		return retval;
	}
	
	private void readData(Node stepnode, List<? extends SharedObjectInterface> databases)
		throws KettleXMLException
	{
		try
		{
			this.databaseMeta = DatabaseMeta.findDatabase( databases, XMLHandler.getTagValue(stepnode, "connection"));
            this.app = XMLHandler.getTagValue(stepnode, "app");
            this.cube = XMLHandler.getTagValue(stepnode, "cube");
			this.mdx = XMLHandler.getTagValue(stepnode, "mdx");
            this.variableReplacementActive = "Y".equals(XMLHandler.getTagValue(stepnode, "variables_active"));
		}
		catch(Exception e)
		{
			throw new KettleXMLException("Unable to load step info from XML", e);
		}
	}

	public void setDefault()
	{
		mdx =  "SELECT \n" 
		     + "[Product].Levels(0).Members on columns, \n"
             + "CrossJoin([Year].CHILDREN , [Market].CHILDREN) on rows \n"
             + "from Demo.Basic";
		variableReplacementActive=false;
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
    
    public void getFields(RowMetaInterface row, String origin, RowMetaInterface[] info, StepMeta nextStep, VariableSpace space) throws KettleStepException 
    {
		
		RowMetaInterface add=null;
		
		try
		{
			add = data.outputRowMeta;
		}
		catch(Exception dbe)
		{
			throw new KettleStepException("Unable to get query result for MDX query: "+Const.CR+mdx, dbe);
		}

		// Set the origin
		//
		for (int i=0;i<add.size();i++)
		{
			ValueMetaInterface v=add.getValueMeta(i);
			v.setOrigin(origin);
		}
		
		row.addRowMeta( add );
	}

	public String getXML()
	{
        StringBuffer retval = new StringBuffer();
		
        retval.append("    ").append(XMLHandler.addTagValue("connection", this.databaseMeta == null ? "" : this.databaseMeta.getName()));
        retval.append("    ").append(XMLHandler.addTagValue("app", this.app));
        retval.append("    ").append(XMLHandler.addTagValue("cube", this.cube));
		retval.append("    "+XMLHandler.addTagValue("mdx", this.mdx));
        retval.append("    "+XMLHandler.addTagValue("variables_active",  this.variableReplacementActive));
        
		return retval.toString();
	}

	public void readRep(Repository rep, ObjectId id_step, List<DatabaseMeta> databases, Map<String, Counter> counters)
		throws KettleException
	{
		try
		{
			this.databaseMeta = rep.loadDatabaseMetaFromStepAttribute(id_step, "connection", databases);
            this.app = rep.getStepAttributeString(id_step, "app");
            this.cube = rep.getStepAttributeString(id_step, "cube");
			this.mdx = rep.getStepAttributeString (id_step, "mdx");
            this.variableReplacementActive = rep.getStepAttributeBoolean(id_step, "variables_active");
		}
		catch(Exception e)
		{
			throw new KettleException("Unexpected error reading step information from the repository", e);
		}
	}
	
	public void saveRep(Repository rep, ObjectId id_transformation, ObjectId id_step)
		throws KettleException
	{
		try
		{

			rep.saveDatabaseMetaStepAttribute(id_transformation, id_step, "connection", this.databaseMeta);
            rep.saveStepAttribute(id_transformation, id_step, "app", this.app);
            rep.saveStepAttribute(id_transformation, id_step, "cube", this.cube);            
			rep.saveStepAttribute(id_transformation, id_step, "mdx", this.mdx);
            rep.saveStepAttribute(id_transformation, id_step, "variables_active", this.variableReplacementActive);

		}
		catch(Exception e)
		{
			throw new KettleException("Unable to save step information to the repository for id_step="+id_step, e);
		}
	}

	public void check(List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta, RowMetaInterface prev, String input[], String output[], RowMetaInterface info)
	{
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
                    cr = new CheckResult(CheckResultInterface.TYPE_RESULT_OK, "The name of the cube is chosen", stepMeta);
                    remarks.add(cr);
                } else {
                    cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, "The name of the cube is missing.", stepMeta);
                    remarks.add(cr);
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

	public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta transMeta, Trans trans)
	{
		data = (EssbaseOlapInputData) stepDataInterface;
		return new EssbaseOlapInput(stepMeta, stepDataInterface, cnr, transMeta, trans);
	}

	public StepDataInterface getStepData()
	{
		try {
            return new EssbaseOlapInputData(this.databaseMeta);
        } catch (Exception e) {
            return null;
        }
	}

	public void analyseImpact(List<DatabaseImpact> impact, TransMeta transMeta, StepMeta stepMeta, RowMetaInterface prev, String input[], String output[], RowMetaInterface info) throws KettleStepException
	{
		// you can't really analyze the database impact since it runs on a remote XML/A server
	}
    
	public RowMeta createRowMeta(String[] headerValues, String[][] cellValues) {
		RowMeta outputRowMeta = new RowMeta();

        for (int i=0;cellValues != null && cellValues.length > 0 && i<cellValues[0].length;i++)
        {
            String name ="";
            if (Const.isEmpty(headerValues)) {
                name = "Column" + i;
            }
            else {
                name = headerValues[i];
            }

            ValueMetaInterface valueMeta=new ValueMeta(name,ValueMetaInterface.TYPE_STRING);

            outputRowMeta.addValueMeta(valueMeta);

        }
        return outputRowMeta;
	}
	
	public String getApplication() {
        return this.app;
    }

    public String getCube() {
        return this.cube;
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
	
	public String getMdx() {
        return mdx;
    }

    public void setMdx(String mdx) {
        this.mdx = mdx;
    }

}
