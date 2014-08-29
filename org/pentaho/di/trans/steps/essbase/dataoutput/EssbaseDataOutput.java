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

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.essbase.core.EssbaseHelper;
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

  private EssbaseDataOutputMeta meta;
  private EssbaseDataOutputData data;

  public EssbaseDataOutput(final StepMeta stepMeta, final StepDataInterface stepDataInterface, final int copyNr, final TransMeta transMeta, final Trans trans) {
    super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
  }

  public final boolean processRow(final StepMetaInterface smi, final StepDataInterface sdi) throws KettleException {

    meta = (EssbaseDataOutputMeta) smi;
    data = (EssbaseDataOutputData) sdi;

    Object[] r = getRow(); // this also waits for a previous step to be
                           // finished.
    if (r == null) { // no more input to be expected...
      this.logDebug("No More Rows.");
      setOutputDone();
      return false;
    }
    if (first) {
      first = false;
      this.logBasic("First Row Analysis.");
      data.indexes = new int[meta.getFields().size() + 1];
      for (int i = 0; i < data.indexes.length - 1; i++) {
        data.indexes[i] = getInputRowMeta().indexOfValue(meta.getFields().get(i).getFieldName());
      }
      data.indexes[data.indexes.length - 1] = getInputRowMeta().indexOfValue(meta.getMeasure().getFieldName());
      this.logBasic("First Row Ok.");

      if (meta.getClearCube()) {
        try {
          data.helper.clearCube(data.helper.getCube());
        } catch (Exception ex) {
          throw new KettleException("Failed to clear Cube");
        }
      }
    }

    String row = "";
    try {
      Object[] newRow = new Object[meta.getFields().size() + 1];
      for (int i = 0; i < data.indexes.length; i++) {
        if (i == data.indexes.length - 1)
          if (meta.getMeasureType().equals("Numeric"))
            newRow[i] = getInputRowMeta().getNumber(r, data.indexes[i]);
          else
            newRow[i] = getInputRowMeta().getString(r, data.indexes[i]);
        else
          newRow[i] = getInputRowMeta().getString(r, data.indexes[i]);
      }
      try {
    	  this.logDebug("Loading row " + newRow);
    	  data.helper.loadData(data.helper.getCube(), newRow);
    	  incrementLinesOutput();
      } catch (Exception ex) {
        for (int k = 0; k < newRow.length; k++)
          row += " " + getInputRowMeta().getString(r, k);
        throw ex;
      }
    } catch (Exception e) {
      throw new KettleException("Failed to add Data Row: " + row, e);
    }
    return true;
  }

  public final boolean init(StepMetaInterface smi, StepDataInterface sdi) {
    meta = (EssbaseDataOutputMeta) smi;
    data = (EssbaseDataOutputData) sdi;

    if (super.init(smi, sdi)) {
      try {
        this.logDebug("Meta Fields: " + meta.getFields().size());
        this.logDebug("Connecting to database " + meta.getDatabaseMeta());
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
    data.helper.disconnect();
    super.dispose(smi, sdi);
  }
}
