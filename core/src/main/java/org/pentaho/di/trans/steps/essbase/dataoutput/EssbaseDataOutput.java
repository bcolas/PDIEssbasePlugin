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
 *   This file is part of EssbaseKettlePlugin
 */

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.essbase.core.EssbaseHelper;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

/**
 *
 */
public class EssbaseDataOutput extends BaseStep implements StepInterface {

	private static Class<?> PKG = EssbaseDataOutputMeta.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$
	
	private EssbaseDataOutputMeta meta;
	private EssbaseDataOutputData data;

	public EssbaseDataOutput(final StepMeta stepMeta, final StepDataInterface stepDataInterface, final int copyNr, final TransMeta transMeta, final Trans trans) {
		super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
	}

	public final boolean processRow(final StepMetaInterface smi, final StepDataInterface sdi) throws KettleException {

		meta = (EssbaseDataOutputMeta) smi;
		data = (EssbaseDataOutputData) sdi;
    
		boolean sendToErrorRow=false;
		String errorMessage = null;

		Object[] r = getRow(); // this also waits for a previous step to be
                           // finished.
		
		if (first) {
			first = false;
			this.logBasic("First Row Analysis.");
			
			if (getInputRowMeta() != null)
			{
				data.outputMeta = getInputRowMeta().clone();
			}
			else
			{
				// If the stream is empty, then row metadata probably hasn't been received. In this case, use
				// the design-time algorithm to calculate the output metadata.
				data.outputMeta = getTransMeta().getPrevStepFields(getStepMeta());
			}
			
			data.indexes = new int[meta.getFields().size() + 1];
			for (int i = 0; i < data.indexes.length - 1; i++) {
				//data.indexes[i] = getInputRowMeta().indexOfValue(meta.getFields().get(i).getFieldName());
				data.indexes[i] = data.outputMeta.indexOfValue(meta.getFields().get(i).getFieldName());
			}
			//data.indexes[data.indexes.length - 1] = getInputRowMeta().indexOfValue(meta.getMeasure().getFieldName());
			data.indexes[data.indexes.length - 1] = data.outputMeta.indexOfValue(meta.getMeasure().getFieldName());
			this.logBasic("First Row Ok.");

			if (meta.getClearCube()) {
				try {
					data.helper.clearCube(data.helper.getCube());
				} catch (Exception ex) {
					throw new KettleException("Failed to clear Cube");
				}
			}
			data.rows = meta.getRowHeader();
		}
		
		if (r == null) { // no more input to be expected...
			this.logDebug("No More Rows.");
			if (data.rows.length()>0)
			{
				try {
					this.logBasic("Last commit.");
					data.helper.loadData(data.helper.getCube(), data.rows);
				}
				catch (Exception e)
				{
					this.logError("Failed to add Data Row: " +StringUtils.countMatches("\n", data.rows.toString()) +"rows\n"+ data.rows.toString(), e);
				}
			}
			setOutputDone();
			return false;
		}

		String row = "";
		try {
			Object[] newRow = new Object[meta.getFields().size() + 1];
			row = "";
			for (int i = 0; i < data.indexes.length; i++) {
				if (i == data.indexes.length - 1) {
					if (meta.getMeasureType().equals("Numeric")) {
						if (getInputRowMeta().getValueMeta(data.indexes[i]).getType()==ValueMetaInterface.TYPE_NUMBER)
							newRow[i] = getInputRowMeta().getNumber(r, data.indexes[i]);
						else if (getInputRowMeta().getValueMeta(data.indexes[i]).getType()==ValueMetaInterface.TYPE_BIGNUMBER)
							newRow[i] = getInputRowMeta().getBigNumber(r, data.indexes[i]);
						else if (getInputRowMeta().getValueMeta(data.indexes[i]).getType()==ValueMetaInterface.TYPE_INTEGER)
							newRow[i] = getInputRowMeta().getInteger(r, data.indexes[i]);
					}
					else
						newRow[i] = getInputRowMeta().getString(r, data.indexes[i]);
					this.logDebug("Measure value (type :"+getInputRowMeta().getValueMeta(data.indexes[i]).getType()+"): "+newRow[i]);
				}
				else
					newRow[i] = getInputRowMeta().getString(r, data.indexes[i]);
				row += newRow[i]+" ";
			}
			writeToEssBase(newRow);
      
			putRow(data.outputMeta, r);       // in case we want it to go further...
			incrementLinesOutput();
			if (checkFeedback(getLinesOutput())) 
			{
				if(log.isBasic()) logBasic(BaseMessages.getString(PKG, "EssbaseDataOutput.Log.LineNumber")+getLinesOutput()); //$NON-NLS-1$
			}
		} 
		catch (Exception e) 
		{  
			if (getStepMeta().isDoingErrorHandling())
			{
				sendToErrorRow = true;
				errorMessage = "Failed to add Data Row: " + row + "\n"+e.toString();
			}
			else
			{
				throw new KettleException("Failed to add Data Row: " + row + "\n"+e.toString(), e);
			}
		 
			if (sendToErrorRow)
			{
				// Simply add this row to the error row
				putError(getInputRowMeta(), r, 1, errorMessage, null, "EDO001");
			}
		}
		return true;
	}
	
	protected Object[] writeToEssBase(Object[] row) throws Exception
	{
		String debugRow = "";
		for (int i = 0; i < row.length; i++) {
	 		/*try {
	 			Double.parseDouble((String) row[i]);
	 			data.rows.append( '"'+((String)row[i])+'"' );
	 		} 
	 		catch (Exception e) {
	 			data.rows.append( row[i] );
	 		}*/
	 		if (i<row.length-1) {
	 			data.rows.append( "\""+((String)row[i])+"\" " );
	 			debugRow += "\""+((String)row[i])+"\" ";
	 		}
	 		else {
	 			data.rows.append( row[i] );
	 			data.rows.append("\n");
	 			debugRow += row[i]+"\n";
	 		}
		}
		
		// Get a commit counter per prepared statement to keep track of separate tables, etc. 
	    //
		Integer commitCounter = data.commitCounterMap.get(data.helper.getCube().getName());
	    if (commitCounter==null) {
	    	commitCounter=Integer.valueOf(1);
	    } else {
	    	commitCounter++;
	    }
	    data.commitCounterMap.put(data.helper.getCube().getName(), Integer.valueOf(commitCounter.intValue()));

	    // Perform a commit if needed
	 	//
	 	if ((data.commitSize>0) && ((commitCounter%data.commitSize)==0)) 
	 	{
	 		this.logBasic("Commit "+commitCounter+" lines.");
	 		this.logDebug("Insert Row(s) : \n"+data.rows);
	 		data.helper.loadData(data.helper.getCube(), data.rows);
	 		
	 		//data.rows = new StringBuffer();
	 		data.rows = ((EssbaseDataOutputMeta) meta).getRowHeader();
	 		// Clear the batch/commit counter...
			//
			data.commitCounterMap.put(data.helper.getCube().getName(), Integer.valueOf(0));
	 	}
		
		return row;
	}

  public final boolean init(StepMetaInterface smi, StepDataInterface sdi) {
    meta = (EssbaseDataOutputMeta) smi;
    data = (EssbaseDataOutputData) sdi;

    if (super.init(smi, sdi)) {
      try {
        this.logDebug("Meta Fields: " + meta.getFields().size());
        this.logDebug("Connecting to database " + meta.getDatabaseMeta());
        this.logDebug("Commit size " + meta.getCommitSize());
        data.commitSize = Integer.parseInt(meta.getCommitSize());
        data.helper = new EssbaseHelper(meta.getDatabaseMeta());
        data.helper.connect();
        this.logDebug("Initializing Cube " + meta.getApplication() +"/" + meta.getCube());
        data.helper.initCube(meta.getApplication(), meta.getCube());
        return true;
      } catch (Exception e) {
        logError("An error occurred, processing will be stopped: " + e.getMessage());
        setErrors(1);
        stopAll();
      }
    }
    return false;
  }

  public void dispose(StepMetaInterface smi, StepDataInterface sdi) {
	data = (EssbaseDataOutputData) sdi;
	try {
		data.helper.closeCube(data.helper.getCube());
	}
	catch (Exception e)
	{
		this.logError("Failed to add Data Row: " +StringUtils.countMatches("\n", data.rows.toString()) +"rows\n"+ data.rows.toString(), e);
	}
    data.helper.disconnect();
    super.dispose(smi, sdi);
  }
}
