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

import org.pentaho.di.core.RowSet;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.database.EssbaseDatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.essbase.core.EssbaseHelper;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.errorhandling.StreamInterface;

/**
 * @author Benoit COLAS
 * @since 30/07/2014
 */
public class EssbaseOlapInputData extends BaseStepData implements StepDataInterface
{
	public EssbaseHelper helper;
	public RowMetaInterface outputRowMeta;
	public RowSet rowSet;
	
	public int rowNumber;
	
	public StreamInterface infoStream;
	
	public EssbaseOlapInputMeta meta;
	
	public EssbaseOlapInputData()
	{
		super();
	}
	
	public EssbaseOlapInputData(DatabaseMeta databaseMeta) throws KettleException {
    super();
    if(!(databaseMeta.getDatabaseInterface() instanceof EssbaseDatabaseMeta)) {
        throw new KettleException ("A connection of type Essabse is expected");
    }
    this.helper = new EssbaseHelper(databaseMeta);
  }
}
