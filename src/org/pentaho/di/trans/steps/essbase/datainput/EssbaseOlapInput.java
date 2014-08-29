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

import org.pentaho.di.core.exception.KettleDatabaseException;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.essbase.core.EssbaseHelper;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.steps.essbase.dataoutput.EssbaseDataOutputData;

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
	
	public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException
	{
	    try {
	        
		if (first) // we just got started
		{
			first=false;
			String mdxquery = null;
			if(meta.isVariableReplacementActive()) mdxquery = this.environmentSubstitute(meta.getMdx());
			else mdxquery = meta.getMdx();
			data.helper.executeMDX(mdxquery);
			
			data.helper.createRectangularOutput();
			data.outputRowMeta = meta.createRowMeta(data.helper.getHeaders(), data.helper.getRows()).clone(); 
			
			data.rowNumber = 0;
		}

		for (;data.rowNumber < data.helper.getRowValues().size();data.rowNumber++) {
		    String[] row = data.helper.getRows()[data.rowNumber];
	        Object[] outputRowData = RowDataUtil.allocateRowData(row.length);
	        outputRowData = row;
	        
	        putRow(data.outputRowMeta, outputRowData);
	        
		}
        
        setOutputDone(); // signal end to receiver(s)
        return false; // end of data or error.
        
        
		}
	    catch (Exception e) {
	        logError("An error occurred, processing will be stopped",e);
            setErrors(1);
            stopAll();
            return false;
        }
	}
    
	public void dispose(StepMetaInterface smi, StepDataInterface sdi)
	{
		data = (EssbaseOlapInputData) sdi;
		data.helper.closeCubeView();
	    data.helper.disconnect();

	    super.dispose(smi, sdi);
	}

	public boolean init(StepMetaInterface smi, StepDataInterface sdi)
	{
		meta=(EssbaseOlapInputMeta)smi;
		data=(EssbaseOlapInputData)sdi;

		if (super.init(smi, sdi)) {
		      try {
		        this.logDebug("Connecting to database " + meta.getDatabaseMeta());
		        data.helper = new EssbaseHelper(meta.getDatabaseMeta());
		        //data.helper.connect();
		        data.helper.openCubeView(this.getStepname(), meta.getApplication(), meta.getCube());

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
