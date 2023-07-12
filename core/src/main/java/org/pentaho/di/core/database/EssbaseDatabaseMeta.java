/*
 * Copyright (c) 2014 Pentaho Corporation.  All rights reserved. 
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

package org.pentaho.di.core.database;

import org.pentaho.di.core.database.BaseDatabaseMeta;
import org.pentaho.di.core.plugins.DatabaseMetaPlugin;
import org.pentaho.di.core.row.ValueMetaInterface;

/**
 * Contains Database Connection information through static final members for an Oracle EssBase MOLAP database.
 * These connections are typically custom-made.
 * That means that reading, writing, etc, is not done through JDBC. 
 * 
 * @author Benoit COLAS
 * @since  28-Jul-2014
 */

@DatabaseMetaPlugin(type = "ESSBASE", typeDescription = "Oracle EssBase")
public class EssbaseDatabaseMeta extends BaseDatabaseMeta implements DatabaseInterface
{	
	public int[] getAccessTypeList()
	{
		return new int[] { DatabaseMeta.TYPE_ACCESS_PLUGIN };
	}
	
	public int getDefaultDatabasePort()
	{
		return 13080;
	}

	/**
	 * @return Whether or not the database can use auto increment type of fields (pk)
	 */
	public boolean supportsAutoInc()
	{
		return false;
	}
	
	public String getDriverClass()
	{
			return null;
	}

    public String getURL(String hostname, String port, String databaseName)
    {
		return null;
	}

	/**
	 * @return true if the database supports bitmap indexes
	 */
	public boolean supportsBitmapIndex()
	{
		return false;
	}

	/**
	 * @return true if the database supports synonyms
	 */
	public boolean supportsSynonyms()
	{
		return false;
	}

	/**
	 * Generates the SQL statement to add a column to the specified table
	 * @param tablename The table to add
	 * @param v The column defined as a value
	 * @param tk the name of the technical key field
	 * @param use_autoinc whether or not this field uses auto increment
	 * @param pk the name of the primary key field
	 * @param semicolon whether or not to add a semi-colon behind the statement.
	 * @return the SQL statement to add a column to the specified table
	 */
	public String getAddColumnStatement(String tablename, ValueMetaInterface v, String tk, boolean use_autoinc, String pk, boolean semicolon)
	{
		return null;
	}

	/**
	 * Generates the SQL statement to modify a column in the specified table
	 * @param tablename The table to add
	 * @param v The column defined as a value
	 * @param tk the name of the technical key field
	 * @param use_autoinc whether or not this field uses auto increment
	 * @param pk the name of the primary key field
	 * @param semicolon whether or not to add a semi-colon behind the statement.
	 * @return the SQL statement to modify a column in the specified table
	 */
	public String getModifyColumnStatement(String tablename, ValueMetaInterface v, String tk, boolean use_autoinc, String pk, boolean semicolon)
	{
		return null;
	}

	public String getFieldDefinition(ValueMetaInterface v, String tk, String pk, boolean use_autoinc, boolean add_fieldname, boolean add_cr)
	{
	    return null;
	}
	
	public String [] getReservedWords()
	{
		return null;
	}
    
    public String[] getUsedLibraries()
    {
        return new String[] { "ess_es_server.jar", "ess_japi.jar", "ess_maxl.jar" };
    }
    
	public String getDatabaseFactoryName() {
		return "org.pentaho.di.essbase.core.EssbaseHelper";
	}

	/**
	 * @return true if this is a relational database you can explore.
	 * Return false for SAP, PALO, etc.
	 */
	public boolean isExplorable() {
		return false;
	}
}
