/*
 * 
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


import java.util.HashMap;
import java.util.Map;

import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.database.EssbaseDatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.essbase.core.EssbaseHelper;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

public class EssbaseDataOutputData extends BaseStepData 
implements StepDataInterface {
    public EssbaseHelper helper;
    public RowMetaInterface outputMeta;
    
    public int[] indexes;
    public StringBuffer rows;
    
    public Map<String, Integer> commitCounterMap;
	  public int commitSize;
	
    public EssbaseDataOutputData(DatabaseMeta databaseMeta) throws KettleException {
        super();
        if(!(databaseMeta.getDatabaseInterface() instanceof EssbaseDatabaseMeta)) {
            throw new KettleException ("A connection of type Essbase is expected");
        }
        this.helper = new EssbaseHelper(databaseMeta);
        
        commitCounterMap = new HashMap<String, Integer>();
    }
}
