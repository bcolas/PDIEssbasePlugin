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

package org.pentaho.di.job.entries.essbase;

import static org.pentaho.di.job.entry.validator.AndValidator.putValidators;
import static org.pentaho.di.job.entry.validator.JobEntryValidatorUtils.andValidator;
import static org.pentaho.di.job.entry.validator.JobEntryValidatorUtils.notBlankValidator;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;

import org.apache.commons.vfs.FileObject;
import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.database.EssbaseDatabaseMeta;
import org.pentaho.di.core.exception.KettleDatabaseException;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entry.JobEntryBase;
import org.pentaho.di.job.entry.JobEntryInterface;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.resource.ResourceEntry;
import org.pentaho.di.resource.ResourceEntry.ResourceType;
import org.pentaho.di.resource.ResourceReference;
import org.w3c.dom.Node;
import org.pentaho.di.essbase.core.EssbaseHelper;



/**
 * This defines an MAXL job entry.
 *
 * @author Benoit COLAS
 * @since 30-07-2014
 *
 */

@org.pentaho.di.core.annotations.JobEntry(id="EssbaseMAXLEntry", categoryDescription="i18n:org.pentaho.di.job:JobCategory.Category.Scripting",
                                          i18nPackageName="org.pentaho.di.job.entries.essbase", 
                                          image="MAXL.png", name="JobMAXL.Name",	description="JobMAXL.TooltipsDesc")
public class JobEntryMAXL extends JobEntryBase implements Cloneable, JobEntryInterface
{
	private static Class<?> PKG = JobEntryMAXL.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$

	private String maxl;
	private DatabaseMeta databaseMeta;
	private boolean useVariableSubstitution = false;
	private boolean maxlfromfile=false;
	private String maxlfilename;
	private boolean sendOneStatement=false;

	public JobEntryMAXL(String n)
	{
		super(n, "");
		maxl=null;
		databaseMeta=null;
		setID(-1L);
	}

	public JobEntryMAXL()
	{
		this("");
	}

    public Object clone()
    {
        JobEntryMAXL je = (JobEntryMAXL) super.clone();
        return je;
    }

	public String getXML()
	{
        StringBuffer retval = new StringBuffer(200);

		retval.append(super.getXML());

		retval.append("      ").append(XMLHandler.addTagValue("maxl",      maxl));
		retval.append("      ").append(XMLHandler.addTagValue("useVariableSubstitution", useVariableSubstitution ? "T" : "F"));
		retval.append("      ").append(XMLHandler.addTagValue("sqlfromfile", maxlfromfile ? "T" : "F"));
		retval.append("      ").append(XMLHandler.addTagValue("sqlfilename",      maxlfilename));
		retval.append("      ").append(XMLHandler.addTagValue("sendOneStatement", sendOneStatement ? "T" : "F"));
		
		retval.append("      ").append(XMLHandler.addTagValue("databaseMeta", databaseMeta==null?null:databaseMeta.getName()));

		return retval.toString();
	}

	public void loadXML(Node entrynode, List<DatabaseMeta> databases, List<SlaveServer> slaveServers, Repository rep) throws KettleXMLException
	{
		try
		{
			super.loadXML(entrynode, databases, slaveServers);
			maxl           = XMLHandler.getTagValue(entrynode, "maxl");
			String dbname = XMLHandler.getTagValue(entrynode, "databaseMeta");
			String sSubs  = XMLHandler.getTagValue(entrynode, "useVariableSubstitution");

			if (sSubs != null && sSubs.equalsIgnoreCase("T"))
				useVariableSubstitution = true;
			databaseMeta    = DatabaseMeta.findDatabase(databases, dbname);
			
			
			String smaxl  = XMLHandler.getTagValue(entrynode, "maxlfromfile");
			if (smaxl != null && smaxl.equalsIgnoreCase("T"))
				maxlfromfile = true;
			
			maxlfilename    = XMLHandler.getTagValue(entrynode, "maxlfilename");
			
			String sOneStatement  = XMLHandler.getTagValue(entrynode, "sendOneStatement");
			if (sOneStatement != null && sOneStatement.equalsIgnoreCase("T"))
				sendOneStatement = true;

		}
		catch(KettleException e)
		{
			throw new KettleXMLException("Unable to load job entry of type 'maxl' from XML node", e);
		}
	}

	public void loadRep(Repository rep, ObjectId id_jobentry, List<DatabaseMeta> databases, List<SlaveServer> slaveServers) throws KettleException
	{
		try
		{
			maxl = rep.getJobEntryAttributeString(id_jobentry, "maxl");
			String sSubs = rep.getJobEntryAttributeString(id_jobentry, "useVariableSubstitution");
			if (sSubs != null && sSubs.equalsIgnoreCase("T"))
				useVariableSubstitution = true;
			
			String smaxl = rep.getJobEntryAttributeString(id_jobentry, "maxlfromfile");
			if (smaxl != null && smaxl.equalsIgnoreCase("T"))
				maxlfromfile = true;
			
			String ssendOneStatement = rep.getJobEntryAttributeString(id_jobentry, "sendOneStatement");
			if (ssendOneStatement != null && ssendOneStatement.equalsIgnoreCase("T"))
				sendOneStatement = true;
			
			maxlfilename = rep.getJobEntryAttributeString(id_jobentry, "maxlfilename");
			
			databaseMeta = rep.loadDatabaseMetaFromJobEntryAttribute(id_jobentry, "databaseMeta", "id_database", databases);
		}
		catch(KettleDatabaseException dbe)
		{
			throw new KettleException("Unable to load job entry of type 'sql' from the repository with id_jobentry="+id_jobentry, dbe);
		}
	}

	// Save the attributes of this job entry
	//
	public void saveRep(Repository rep, ObjectId id_job) throws KettleException
	{
		try
		{
			rep.saveDatabaseMetaJobEntryAttribute(id_job, getObjectId(), "databaseMeta", "id_database", databaseMeta);
			
			rep.saveJobEntryAttribute(id_job, getObjectId(), "maxl", maxl);
			rep.saveJobEntryAttribute(id_job, getObjectId(), "useVariableSubstitution", useVariableSubstitution ? "T" : "F" );
			rep.saveJobEntryAttribute(id_job, getObjectId(), "maxlfromfile", maxlfromfile ? "T" : "F" );
			rep.saveJobEntryAttribute(id_job, getObjectId(), "maxlfilename", maxlfilename);
			rep.saveJobEntryAttribute(id_job, getObjectId(), "sendOneStatement", sendOneStatement ? "T" : "F" );
		}
		catch(KettleDatabaseException dbe)
		{
			throw new KettleException("Unable to save job entry of type 'sql' to the repository for id_job="+id_job, dbe);
		}
	}

	public void setMAXL(String maxl)
	{
		this.maxl = maxl;
	}

	public String getMAXL()
	{
		return maxl;
	}
	
	 public String getMAXLFilename()
	 {
	    return maxlfilename;
	 }

	 public void setMAXLFilename(String maxlfilename)
	{
		this.maxlfilename = maxlfilename;
	}
	 
	public boolean getUseVariableSubstitution()
	{
		return useVariableSubstitution;
	}

	public void setUseVariableSubstitution(boolean subs)
	{
		useVariableSubstitution = subs;
	}
	
	public void setMAXLFromFile(boolean maxlfromfilein)
	{
		maxlfromfile = maxlfromfilein;
	}
	public boolean getMAXLFromFile()
	{
		return maxlfromfile;
	}
	
	public boolean isSendOneStatement()
	{
		return sendOneStatement;
	}
	public void setSendOneStatement(boolean sendOneStatementin)
	{
		sendOneStatement = sendOneStatementin;
	}
	
	public void setDatabase(DatabaseMeta database) throws KettleException
	{
		if(!(database.getDatabaseInterface() instanceof EssbaseDatabaseMeta)) {
            throw new KettleException ("A connection of type Essabse is expected");
        }
		this.databaseMeta = database;
	}

	public DatabaseMeta getDatabase()
	{
		return databaseMeta;
	}

	public Result execute(Result previousResult, int nr)
	{
		Result result = previousResult;

		if (databaseMeta!=null)
		{
			final EssbaseHelper helper = new EssbaseHelper(databaseMeta);
			FileObject MAXLfile=null;
			try
			{
				String myMAXL = null;
				helper.connect();
				helper.openMaxlSession(this.getObjectName());
				
				if(maxlfromfile)
				{
					if(maxlfilename==null)
						throw new KettleDatabaseException(BaseMessages.getString(PKG, "JobMAXL.NoMAXLFileSpecified"));
					
					try{
						String realfilename=environmentSubstitute(maxlfilename);
						MAXLfile=KettleVFS.getFileObject(realfilename, this);
						if(!MAXLfile.exists()) 
						{
							logError(BaseMessages.getString(PKG, "JobMAXL.MAXLFileNotExist",realfilename));
							throw new KettleDatabaseException(BaseMessages.getString(PKG, "JobMAXL.MAXLFileNotExist",realfilename));
						}
						if(isDetailed()) logDetailed(BaseMessages.getString(PKG, "JobMAXL.MAXLFileExists",realfilename));
						
						InputStream IS = KettleVFS.getInputStream(MAXLfile);
						try {
							InputStreamReader BIS = new InputStreamReader(new BufferedInputStream(IS, 500));
							StringBuffer lineStringBuffer = new StringBuffer(256);
							lineStringBuffer.setLength(0);
  						
							BufferedReader buff = new BufferedReader(BIS);
							String sLine = null;
							myMAXL=Const.CR;;
  
							while((sLine=buff.readLine())!=null) 
							{
								if(Const.isEmpty(sLine))
								{
									myMAXL= myMAXL +  Const.CR;	
								}
								else
								{
									myMAXL=myMAXL+  Const.CR + sLine;
								}
							}
						} finally {
							IS.close();
						}
					}catch (Exception e)
					{
						throw new KettleDatabaseException(BaseMessages.getString(PKG, "JobMAXL.ErrorRunningMAXLfromFile"),e);
					}
					
				}else
				{
					myMAXL=maxl;
				}
				if(!Const.isEmpty(myMAXL))
				{
					// let it run
					if (useVariableSubstitution) myMAXL = environmentSubstitute(myMAXL);
					if(isDetailed()) logDetailed(BaseMessages.getString(PKG, "JobMAXL.Log.MAXLStatement",myMAXL));
					if(sendOneStatement)
						helper.execStatement(myMAXL);
					else
						helper.execStatements(myMAXL);
				}
			}
			catch(Exception e)
			{
				result.setNrErrors(1);
				logError( BaseMessages.getString(PKG, "JobMAXL.ErrorRunJobEntry",e.getMessage()));
			}
			finally
			{
				helper.disconnect();
				if(MAXLfile!=null) 
				{
					try{
					MAXLfile.close();
					}catch(Exception e){}
				}
			}
		}
		else
		{
			result.setNrErrors(1);
			logError( BaseMessages.getString(PKG, "JobMAXL.NoDatabaseConnection"));
		}

		if (result.getNrErrors()==0)
		{
			result.setResult(true);
		}
		else
		{
			result.setResult(false);
		}

		return result;
	}

	public boolean evaluates()
	{
		return true;
	}

	public boolean isUnconditional()
	{
		return true;
	}

    public DatabaseMeta[] getUsedDatabaseMetas()
    {
        return new DatabaseMeta[] { databaseMeta, };
    }

    public List<ResourceReference> getResourceDependencies(JobMeta jobMeta) {
      List<ResourceReference> references = super.getResourceDependencies(jobMeta);
      if (databaseMeta != null) {
        ResourceReference reference = new ResourceReference(this);
        reference.getEntries().add( new ResourceEntry(databaseMeta.getHostname(), ResourceType.SERVER));
        reference.getEntries().add( new ResourceEntry(databaseMeta.getDatabaseName(), ResourceType.DATABASENAME));
        references.add(reference);
      }
      return references;
    }

    @Override
    public void check(List<CheckResultInterface> remarks, JobMeta jobMeta)
    {
      andValidator().validate(this, "MAXL", remarks, putValidators(notBlankValidator())); //$NON-NLS-1$
    }


}
