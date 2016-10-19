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
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.shared.SharedObjectInterface;
import org.pentaho.di.trans.DatabaseImpact;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepIOMeta;
import org.pentaho.di.trans.step.StepIOMetaInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.step.errorhandling.Stream;
import org.pentaho.di.trans.step.errorhandling.StreamIcon;
import org.pentaho.di.trans.step.errorhandling.StreamInterface;
import org.pentaho.di.trans.step.errorhandling.StreamInterface.StreamType;
import org.pentaho.di.trans.steps.essbase.dataoutput.EssbaseDataOutputData;
import org.pentaho.di.trans.steps.olapinput.OlapData;
import org.pentaho.di.trans.steps.olapinput.OlapHelper;
import org.pentaho.di.trans.steps.tableinput.TableInputMeta;
import org.w3c.dom.Node;

import com.healthmarketscience.jackcess.scsu.Debug;

/*
 * Created on 2-jun-2003
 *
 */
@Step(id = "EssbaseOlapInput", image = "EssbaseDataInput.png", name = "Essbase Olap Input", description="", categoryDescription="Oracle Essbase")
public class EssbaseOlapInputMeta extends BaseStepMeta implements StepMetaInterface
{
	private static Class<?> PKG = EssbaseOlapInputMeta.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$
	
	private String mdx;
    private DatabaseMeta databaseMeta;
    private String app = "";
    private String cube = "";
	private EssbaseOlapInputData data;
	
	/** Should I execute once per row? */
    private boolean executeEachInputRow;
	
    private boolean variableReplacementActive;
    
    private boolean suppressZeroActive;
    
    
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
    public void setSuppressZeroActive(boolean suppressZeroActive)
    {
        this.suppressZeroActive = suppressZeroActive;
    }
    
    /**
     * @return Returns the variableReplacementActive.
     */
    public boolean isSuppressZeroActive()
    {
        return suppressZeroActive;
    }

    /**
     * @param variableReplacementActive The variableReplacementActive to set.
     */
    public void setVariableReplacementActive(boolean variableReplacementActive)
    {
        this.variableReplacementActive = variableReplacementActive;
    }
    
    /**
     * @return Returns true if the step should be run per row
     */
    public boolean isExecuteEachInputRow()
    {
        return executeEachInputRow;
    }

    /**
     * @param oncePerRow true if the step should be run per row
     */
    public void setExecuteEachInputRow(boolean oncePerRow)
    {
        this.executeEachInputRow = oncePerRow;
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
			String con = XMLHandler.getTagValue(stepnode, "connection"); //$NON-NLS-1$
			databaseMeta = DatabaseMeta.findDatabase(databases, con);
			
            this.app = XMLHandler.getTagValue(stepnode, "app");
            this.cube = XMLHandler.getTagValue(stepnode, "cube");
			this.mdx = XMLHandler.getTagValue(stepnode, "mdx");
			
			String lookupFromStepname = XMLHandler.getTagValue(stepnode, "lookup"); //$NON-NLS-1$
	        StreamInterface infoStream = getStepIOMeta().getInfoStreams().get(0);
	        infoStream.setSubject(lookupFromStepname);

			this.executeEachInputRow       = "Y".equals(XMLHandler.getTagValue(stepnode, "execute_each_row"));
            this.variableReplacementActive = "Y".equals(XMLHandler.getTagValue(stepnode, "variables_active"));
            this.suppressZeroActive = "Y".equals(XMLHandler.getTagValue(stepnode, "suppresszero_active"));
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
			initData(space);
			
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
		
        retval.append("    ").append(XMLHandler.addTagValue("connection", databaseMeta == null ? "" : databaseMeta.getName()));
        retval.append("    ").append(XMLHandler.addTagValue("app", this.app));
        retval.append("    ").append(XMLHandler.addTagValue("cube", this.cube));
		retval.append("    "+XMLHandler.addTagValue("mdx", this.mdx));
		
		StreamInterface infoStream = getStepIOMeta().getInfoStreams().get(0);
        retval.append("    "+XMLHandler.addTagValue("lookup", infoStream.getStepname())); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        retval.append("    "+XMLHandler.addTagValue("execute_each_row",   this.executeEachInputRow));
        retval.append("    "+XMLHandler.addTagValue("variables_active",  this.variableReplacementActive));
        retval.append("    "+XMLHandler.addTagValue("suppresszero_active",  this.suppressZeroActive));
        
		return retval.toString();
	}

	public void readRep(Repository rep, ObjectId id_step, List<DatabaseMeta> databases, Map<String, Counter> counters)
		throws KettleException
	{
		try
		{
			databaseMeta = rep.loadDatabaseMetaFromStepAttribute(id_step, "id_connection", databases);
			// Compatibility with previous version
			if (databaseMeta == null)
				databaseMeta = rep.loadDatabaseMetaFromStepAttribute(id_step, "connection", databases);
            this.app = rep.getStepAttributeString(id_step, "app");
            this.cube = rep.getStepAttributeString(id_step, "cube");
			this.mdx = rep.getStepAttributeString (id_step, "mdx");
			
			String lookupFromStepname =  rep.getStepAttributeString (id_step, "lookup"); //$NON-NLS-1$
	        StreamInterface infoStream = getStepIOMeta().getInfoStreams().get(0);
	        infoStream.setSubject(lookupFromStepname);

            this.executeEachInputRow = rep.getStepAttributeBoolean(id_step, "execute_each_row");
            this.variableReplacementActive = rep.getStepAttributeBoolean(id_step, "variables_active");
            this.suppressZeroActive = rep.getStepAttributeBoolean(id_step, "suppresszero_active");
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
			rep.saveDatabaseMetaStepAttribute(id_transformation, id_step, "id_connection", databaseMeta);
            rep.saveStepAttribute(id_transformation, id_step, "app", this.app);
            rep.saveStepAttribute(id_transformation, id_step, "cube", this.cube);            
			rep.saveStepAttribute(id_transformation, id_step, "mdx", this.mdx);
			
			StreamInterface infoStream = getStepIOMeta().getInfoStreams().get(0);
            rep.saveStepAttribute(id_transformation, id_step, "lookup",  infoStream.getStepname()); //$NON-NLS-1$
            rep.saveStepAttribute(id_transformation, id_step, "execute_each_row", this.executeEachInputRow);
            rep.saveStepAttribute(id_transformation, id_step, "variables_active", this.variableReplacementActive);
            rep.saveStepAttribute(id_transformation, id_step, "suppresszero_active", this.suppressZeroActive);
            
            // Also, save the step-database relationship!
            if (databaseMeta != null) {
            	rep.insertStepDatabase(id_transformation, id_step, databaseMeta.getObjectId());
            }
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
        
        // See if we have an informative step...
        StreamInterface infoStream = getStepIOMeta().getInfoStreams().get(0);
        if (!Const.isEmpty(infoStream.getStepname())) 
		{
			boolean found=false;
			for (int i=0;i<input.length;i++)
			{
				if (infoStream.getStepname().equalsIgnoreCase(input[i])) found=true;
			}
			if (found)
			{
				cr = new CheckResult(CheckResultInterface.TYPE_RESULT_OK, "Previous step to read info from ["+infoStream.getStepname()+"] is found.", stepMeta);
				remarks.add(cr);
			}
			else
			{
				cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, "Previous step to read info from ["+infoStream.getStepname()+"] is not found.", stepMeta);
				remarks.add(cr);
			}
			
			// Count the number of ? in the SQL string:
			int count=0;
			for (int i=0;i<this.mdx.length();i++)
			{
				char c = this.mdx.charAt(i);
				if (c=='\'') // skip to next quote!
				{
					do
					{
						i++;
						c = this.mdx.charAt(i);
					}
					while (c!='\'');
				}
				if (c=='?') count++;
			}
			// Verify with the number of informative fields...
			if (info!=null)
			{
				if(count == info.size())
				{
					cr = new CheckResult(CheckResultInterface.TYPE_RESULT_OK, "This step is expecting and receiving "+info.size()+" fields of input from the previous step.", stepMeta);
					remarks.add(cr);
				}
				else
				{
					cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, "This step is receiving "+info.size()+" but not the expected "+count+" fields of input from the previous step.", stepMeta);
					remarks.add(cr);
				}
			}
			else
			{
				cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, "Input step name is not recognized!", stepMeta);
				remarks.add(cr);
			}
		}
		else
		{
			if (input.length>0)
			{
				cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, "Step is not expecting info from input steps.", stepMeta);
				remarks.add(cr);
			}
			else
			{
				cr = new CheckResult(CheckResultInterface.TYPE_RESULT_OK, "No input expected, no input provided.", stepMeta);
				remarks.add(cr);
			}
			
		}
	}
	
	/**
	 * @param steps optionally search the info step in a list of steps
	 */
	public void searchInfoAndTargetSteps(List<StepMeta> steps)
	{
      for (StreamInterface stream : getStepIOMeta().getInfoStreams()) {
        stream.setStepMeta( StepMeta.findStep(steps, (String)stream.getSubject()) );
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
	
	public final DatabaseMeta[] getUsedDatabaseConnections() {
        if (databaseMeta != null) {
            return new DatabaseMeta[] {databaseMeta};
        } else {
            return super.getUsedDatabaseConnections();
        }
    }

	public void analyseImpact(List<DatabaseImpact> impact, TransMeta transMeta, StepMeta stepMeta, RowMetaInterface prev, String input[], String output[], RowMetaInterface info) throws KettleStepException
	{
		// you can't really analyze the database impact since it runs on a remote XML/A server
	}
    
	public RowMeta createRowMeta(String[] headerValues, Object[][] cellValues) {
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

            ValueMetaInterface valueMeta=null;
            if (cellValues[0][i] instanceof Double)
            	valueMeta = new ValueMeta(name,ValueMetaInterface.TYPE_BIGNUMBER);
            else 
            	valueMeta = new ValueMeta(name,ValueMetaInterface.TYPE_STRING);

            outputRowMeta.addValueMeta(valueMeta);

        }
        return outputRowMeta;
	}
	
	public void initData(VariableSpace space) throws Exception {
    	
    	if (data == null){
    		data = (EssbaseOlapInputData)getStepData();
    	}
		
		String mdx = this.getMdx();
        if(this.isVariableReplacementActive()) mdx = space.environmentSubstitute(this.getMdx());

		data.helper = new EssbaseHelper(this.getDatabaseMeta());
		data.helper.openCubeView("Test", this.getApplication(), this.getCube());
		data.helper.executeMDX(this.mdx);
		
		data.helper.createRectangularOutput(this.suppressZeroActive);
		data.outputRowMeta = this.createRowMeta(data.helper.getHeaders(), data.helper.getRows()).clone(); 
    		
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
    
    /**
     * Returns the Input/Output metadata for this step.
     * The generator step only produces output, does not accept input!
     */
    public StepIOMetaInterface getStepIOMeta() {
        if (ioMeta==null) {

            ioMeta = new StepIOMeta(true, true, false, false, false, false);
        
            StreamInterface stream = new Stream(StreamType.INFO, null, BaseMessages.getString(PKG, "EssbaseOlapInputMeta.InfoStream.Description"), StreamIcon.INFO, null);
            ioMeta.addStream(stream);
        }
        
        return ioMeta;
    }
    
    public void resetStepIoMeta() {
        // Do nothing, don't reset as there is no need to do this.
    };
    
    /**
     * For compatibility, wraps around the standard step IO metadata
     * @param stepMeta The step where you read lookup data from
     */
    public void setLookupFromStep(StepMeta stepMeta) {
    	getStepIOMeta().getInfoStreams().get(0).setStepMeta(stepMeta);
    }

    /**
     * For compatibility, wraps around the standard step IO metadata
     * @return The step where you read lookup data from
     */
    public StepMeta getLookupFromStep() {
    	return getStepIOMeta().getInfoStreams().get(0).getStepMeta();
    }

}

