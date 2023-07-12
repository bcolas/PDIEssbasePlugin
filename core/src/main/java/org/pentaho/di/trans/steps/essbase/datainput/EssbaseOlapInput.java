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

import java.sql.ResultSet;

import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.RowSet;
import org.pentaho.di.core.exception.KettleDatabaseException;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.essbase.core.EssbaseHelper;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.steps.essbase.dataoutput.EssbaseDataOutputData;

import com.essbase.api.base.EssException;

/**
 * Reads information from Essbase using MDX
 * 
 * @author Benoit COLAS
 * @since 30/07/2014
 */
public class EssbaseOlapInput extends BaseStep implements StepInterface
{
	private EssbaseOlapInputMeta meta;
	private EssbaseOlapInputData data;
	
	public EssbaseOlapInput(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans)
	{
		super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
	}
	
	private RowMetaAndData readStepFrom() throws KettleException
    {
		if (log.isDetailed()) logDetailed("Reading from step [" + data.infoStream.getStepname() + "]");

        RowMetaInterface parametersMeta = new RowMeta();
        Object[] parametersData = new Object[] {};

        RowSet rowSet = findInputRowSet(data.infoStream.getStepname());
        if (rowSet!=null) 
        {
	        Object[] rowData = getRowFrom(rowSet); // rows are originating from "lookup_from"
	        while (rowData!=null)
	        {
	            parametersData = RowDataUtil.addRowData(parametersData, parametersMeta.size(), rowData);
	            parametersMeta.addRowMeta(rowSet.getRowMeta());
	            
	            rowData = getRowFrom(rowSet); // take all input rows if needed!
	        }
	        
	        if (parametersMeta.size()==0)
	        {
	            throw new KettleException("Expected to read parameters from step ["+data.infoStream.getStepname()+"] but none were found.");
	        }
        }
        else
        {
            throw new KettleException("Unable to find rowset to read from, perhaps step ["+data.infoStream.getStepname()+"] doesn't exist. (or perhaps you are trying a preview?)");
        }
	
        RowMetaAndData parameters = new RowMetaAndData(parametersMeta, parametersData);

        return parameters;
    }
	
	public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException
	{
		boolean sendToErrorRow=false;
		String errorMessage = null;
		
	  if (first) // we just got started
	  {
	    Object[] parameters;
	    RowMetaInterface parametersMeta;
		  first=false;
	            
		  // Make sure we read data from source steps...
	    if (data.infoStream.getStepMeta()!=null)
	    {
	     	if (meta.isExecuteEachInputRow())
	      {
	     		if (log.isDetailed()) logDetailed("Reading single row from stream [" + data.infoStream.getStepname() + "]");
	        data.rowSet = findInputRowSet(data.infoStream.getStepname());
	        if (data.rowSet==null) {
	         	throw new KettleException("Unable to find rowset to read from, perhaps step ["+data.infoStream.getStepname()+"] doesn't exist. (or perhaps you are trying a preview?)");
	        }
	        parameters = getRowFrom(data.rowSet);
	        parametersMeta = data.rowSet.getRowMeta();
	      }
	     	else
	     	{
	     		if (log.isDetailed()) logDetailed("Reading query parameters from stream [" + data.infoStream.getStepname() + "]");
	     		RowMetaAndData rmad = readStepFrom(); // Read values from previous step
	    		parameters = rmad.getData();
	     		parametersMeta = rmad.getRowMeta();
	     	}
	     	if (parameters!=null)
	     	{
	     		if (log.isDetailed()) logDetailed("Query parameters found = " + parametersMeta.getString(parameters));
	     	}
	    }
	    else
	    {
       	parameters = new Object[] {};
       	parametersMeta = new RowMeta();
      }
	            
      if (meta.isExecuteEachInputRow() && ( parameters==null || parametersMeta.size()==0) )
      {
      	setOutputDone(); // signal end to receiver(s)
      	return false; // stop immediately, nothing to do here.
      }
	        
      try {
       	boolean success = doQuery(parametersMeta, parameters);
        if (!success) 
        { 
          return false; 
        }
      }
      catch (Exception e) 
  		{  
				if (getStepMeta().isDoingErrorHandling())
				{
					sendToErrorRow = true;
					errorMessage = "Failed to add Data Row: " + parameters + "\n"+e.toString();
				}
				else
				{
					throw new KettleException("Failed to add Data Row: " + parameters + "\n"+e.toString(), e);
				}
			 
				if (sendToErrorRow)
				{
					// Simply add this row to the error row
					putError(getInputRowMeta(), parameters, 1, errorMessage, null, "EOI001");
				}
			}        
		} 

	  boolean done = false;
	  if (meta.isExecuteEachInputRow()) // Try to get another row from the input stream
	  {
	   	Object[] nextRow = getRowFrom(data.rowSet);
	   	if (nextRow == null) // Nothing more to get!
	   	{
	   		done = true;
	   	}
	   	else
	   	{
	  		// First close the previous query, otherwise we run out of cursors!
	   		/*closePreviousQuery();*/
        try {
         	boolean success = doQuery(data.rowSet.getRowMeta(), nextRow); // OK, perform a new query
         	if (!success) 
         	{ 
         		return false; 
         	}
        }
	  		catch (Exception e) 
		  	{  
					if (getStepMeta().isDoingErrorHandling())
					{
						sendToErrorRow = true;
						errorMessage = "Failed to add Data Row: " + nextRow + "\n"+e.toString();
					}
					else
					{
						throw new KettleException("Failed to add Data Row: " + nextRow + "\n"+e.toString(), e);
					}
				 
					if (sendToErrorRow)
					{
						// Simply add this row to the error row
						putError(getInputRowMeta(), nextRow, 1, errorMessage, null, "EOI001");
					}
				}
	    }
	  }
	  else
	  {
	   	done = true;
	  }

	  if (done)
	  {
	  	setOutputDone(); // signal end to receiver(s)
	   	return false; // end of data or error.
	  }
		
		return true;
		
	}
	
	private boolean doQuery(RowMetaInterface parametersMeta, Object[] parameters) throws KettleDatabaseException, KettleValueException, EssException, KettleStepException, KettleException
    {
		String mdxquery = null;
		if(meta.isVariableReplacementActive()) mdxquery = this.environmentSubstitute(meta.getMdx());
		else mdxquery = meta.getMdx();
		
		if (parameters!=null) {
			for (int i=0; i<parametersMeta.size(); i++) {
				if (log.isDetailed()) logDetailed("MDX query replace "+parametersMeta.getString(parameters, i));
				mdxquery = mdxquery.replaceFirst("\\?", parametersMeta.getString(parameters, i));
			}
		}
		
		data.helper.openCubeView(this.getStepname(), meta.getApplication(), meta.getCube());
		
		if (log.isDetailed()) logDetailed("MDX query : "+mdxquery);
    if (parametersMeta.isEmpty()) {
    	data.helper.executeMDX(mdxquery);
    } else {
    	data.helper.executeMDX(mdxquery);
    }

		data.helper.createRectangularOutput(meta.isSuppressZeroActive());
		data.outputRowMeta = meta.createRowMeta(data.helper.getHeaders(), data.helper.getRows()).clone(); 
	
		data.rowNumber = 0;

		for (;data.rowNumber < data.helper.getRowValues().size();data.rowNumber++) {
			Object[] row = data.helper.getRows()[data.rowNumber];
			Object[] outputRowData = RowDataUtil.allocateRowData(row.length);
			outputRowData = row;
    		putRow(data.outputRowMeta, outputRowData);
		}
		data.helper.closeCubeView();
		return true;
    }
    
	public void dispose(StepMetaInterface smi, StepDataInterface sdi)
	{
		this.logDebug("closing Cube View and disconnecting from Essbase");
		data = (EssbaseOlapInputData) sdi;
		//data.helper.closeCubeView();
	    data.helper.disconnect();

	    super.dispose(smi, sdi);
	}

	public boolean init(StepMetaInterface smi, StepDataInterface sdi)
	{
		meta=(EssbaseOlapInputMeta)smi;
		data=(EssbaseOlapInputData)sdi;

		if (super.init(smi, sdi)) {
			data.infoStream = meta.getStepIOMeta().getInfoStreams().get(0);
			try {
		        this.logDebug("Connecting to database " + meta.getDatabaseMeta());
		        data.helper = new EssbaseHelper(meta.getDatabaseMeta());
		        //data.helper.connect();
		        //data.helper.openCubeView(this.getStepname(), meta.getApplication(), meta.getCube());

		        return true;
		      } catch (Exception e) {
		        logError("An error occurred, processing will be stopped: " + e.getMessage());
		        setErrors(1);
		        stopAll();
		      }
		    }
		    return false;
	}
	
}