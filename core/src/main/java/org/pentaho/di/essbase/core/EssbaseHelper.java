package org.pentaho.di.essbase.core;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.ArrayList;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.database.DatabaseTestResults;
import org.pentaho.di.core.database.DatabaseFactoryInterface;
import org.pentaho.di.core.database.SqlCommentScrubber;
import org.pentaho.di.core.exception.KettleDatabaseException;

import com.essbase.api.session.IEssbase;
import com.essbase.api.datasource.IEssOlapServer;
import com.essbase.api.datasource.IEssOlapApplication;
import com.essbase.api.datasource.IEssCube;
import com.essbase.api.datasource.IEssOlapFileObject;
import com.essbase.api.datasource.IEssMaxlSession;
import com.essbase.api.dataquery.IEssCubeView;
import com.essbase.api.dataquery.IEssOpMdxQuery;
import com.essbase.api.dataquery.IEssMdDataSet;
import com.essbase.api.dataquery.IEssMdAxis;
import com.essbase.api.dataquery.IEssMdMember;
import com.essbase.api.base.EssException;
import com.essbase.api.base.IEssIterator;
import com.essbase.api.base.IEssValueAny;
import com.essbase.api.domain.IEssDomain;
import com.essbase.api.metadata.IEssDimension;

public class EssbaseHelper implements DatabaseFactoryInterface {
	
	public static boolean connectingToEssbase = false;
	private DatabaseMeta databaseMeta;
	private IEssbase ess = null;
	private IEssDomain dom = null;
  private IEssOlapServer olapSvr = null;
    
  private IEssCube cube = null;
  private IEssMaxlSession session = null;
    
  private IEssCubeView cubeView = null; 
  private IEssMdDataSet mddata = null;
  private List<String> headerValues = null;
  private List<List<Object>> rowValues = null;
    
	final private LogLevel essbaseAPILogLevel;
	
	// Needed by the Database dialog
	public EssbaseHelper(){
		this.essbaseAPILogLevel = LogLevel.NOTHING;
	}
	
	public EssbaseHelper(final DatabaseMeta databaseMeta) {
		this(databaseMeta, LogLevel.NOTHING);
	}

	public EssbaseHelper(final DatabaseMeta databaseMeta, final LogLevel logLevel) {
		this.databaseMeta = databaseMeta;

		// Map Kettle log levels to EssBaseAPI log levels
		this.essbaseAPILogLevel = logLevel;
	}
	
	/**
	 * Test connection to a Essbase server
	 * @return Report of test connection
	 * @throws KettleException In case something goes wrong
	 */
	
	public String getConnectionTestReport(DatabaseMeta databaseMeta) throws KettleDatabaseException {
		StringBuffer report = new StringBuffer();

		EssbaseHelper helper = new EssbaseHelper(databaseMeta, LogLevel.ERROR);
		try {
			helper.connect();

			// If the connection was successful
			//
			report.append("Test Connection to Essbase server [").append(databaseMeta.getName()).append("] OK.").append(Const.CR);

		} catch (KettleException e) {
			report.append("Unable to connect to the Essbase server: ").append(e.getMessage()).append(Const.CR);
			report.append(Const.getStackTracker(e));
		}
		finally
		{
			helper.disconnect();	
		}

		return report.toString();
	}
	
	public DatabaseTestResults getConnectionTestResults( DatabaseMeta databaseMeta ) throws KettleDatabaseException {
		DatabaseTestResults results = new DatabaseTestResults();
		results.setMessage(getConnectionTestReport(databaseMeta));
		results.setSuccess(true);
		return results;
	}
	
	/**
	 * Connect to a Essbase server
	 * @throws KettleException In case something goes wrong
	 */
	public final void connect() throws KettleException {
		while (EssbaseHelper.connectingToEssbase) { 
			try {
				Thread.sleep(100);
			} catch(Exception ex) {}
		}
		assert databaseMeta != null;
		
		try {

			EssbaseHelper.connectingToEssbase = true;
			
			this.ess = IEssbase.Home.create(IEssbase.JAPI_VERSION);
			String s_provider = "http://"+databaseMeta.environmentSubstitute(databaseMeta.getHostname())+":"+databaseMeta.environmentSubstitute(databaseMeta.getDatabasePortNumberString())+"/aps/JAPI";
			this.dom = ess.signOn(databaseMeta.environmentSubstitute(databaseMeta.getUsername()), databaseMeta.environmentSubstitute(databaseMeta.getPassword()), false, null, s_provider);

			this.olapSvr = (IEssOlapServer)dom.getOlapServer(databaseMeta.environmentSubstitute(databaseMeta.getDatabaseName()));
            this.olapSvr.connect();

			EssbaseHelper.connectingToEssbase = false;

			if (this.olapSvr == null) {
				throw new KettleException("The specified OLAP Server with name '" 
						+ databaseMeta.environmentSubstitute(databaseMeta.getDatabaseName())
						+ "' could not be found");
			}
		} catch (Exception e) {
			EssbaseHelper.connectingToEssbase = false;

			throw new KettleException("Unexpected error while connecting to "
					+ "the Essbase server: "+e.getMessage(), e);
		}
	}
	
	/**
	 * Disconnect from the Essbase server
	 */
	public final void disconnect() {
		assert this.olapSvr != null;
		try{
			if (this.session != null)
				this.session.close();
			if (this.olapSvr != null && this.olapSvr.isConnected() == true) {
				this.olapSvr.disconnect();
			}
		} catch (EssException e1) {}
		try {
			if (this.ess != null && this.ess.isSignedOn() == true)
				this.ess.signOff();
		} catch (EssException e2) {}
	}
	
	public void openCubeView(String name, String appName, String cubeName) throws KettleException, EssException {
		this.openCubeView(name, appName, cubeName, false);
	}
	
	public void openCubeView(String name, String appName, String cubeName, boolean dispUnknown) throws KettleException, EssException {
		if (!(this.ess != null && this.dom != null)) {
			this.connect();
		}
		this.cubeView = dom.openCubeView(name, databaseMeta.environmentSubstitute(databaseMeta.getDatabaseName()), appName, cubeName);
		if (dispUnknown) {
			this.cubeView.setDisplayUnknownMembers(dispUnknown);
			this.cubeView.updatePropertyValues();
		}
	}
	
	public void executeMDX(String mdxquery) throws EssException {
		boolean bDataLess = false;
		boolean bNeedCellAttributes = false;
		boolean bHideData = true;
		
		IEssOpMdxQuery op = this.cubeView.createIEssOpMdxQuery();
		op.setQuery(bDataLess, bHideData, mdxquery, bNeedCellAttributes, IEssOpMdxQuery.EEssMemberIdentifierType.NAME);
		// Enable XMLA Mode to true if you want the MDX Results according to the XMLA Standards.
		// If below statement with "false" setting is not executed, Query will by default be executed in non-XMLA Mode (according to Essbase Standards).
		op.setXMLAMode(false);

		/*
		 * These 3 options fetch Text based Cell values (if any as in case of
		 * Smartlist, Date & Formatted String Based cells) This are applicable
		 * with Essbase version 11.1.1.0.0 and above with a cube that supports "Textual
		 * Measures"
		 */
		op.setNeedFormattedCellValue(true);
		op.setNeedSmartlistName(true);
		op.setNeedFormatString(false);
		op.setNeedProcessMissingCells(true);

		/*
		 * Enable this option if you want to see Meaningless cells. Applicable
		 * with Varying Attribute based outlines and effective with Essbase 11.1.1.0.0
		 * and above only.
		 */
		op.setNeedMeaninglessCells(false);

		this.cubeView.performOperation(op);

		mddata = this.cubeView.getMdDataSet();
	}
	
	/**
     * Outputs one row per tuple on the rows axis.
     * @throws KettleDatabaseException in case some or other error occurs 
	 */
    public void createRectangularOutput(boolean suppressZero) throws EssException {
    	this.headerValues = new ArrayList < String >();
    	this.rowValues = new ArrayList();
    	List<String> pov = new ArrayList < String >();
        if (this.mddata != null ) {
        	IEssMdAxis[] axes = mddata.getAllAxes();

    		IEssMdAxis slicerOrPovAxis = null;
    		IEssMdAxis columnAxis = null;
    		IEssMdAxis rowAxis = null;
    		IEssMdAxis pageAxis = null;

    		// If slicer axis is present then it will be 1st axis.
    		int axisIndex = 0;
    		if (axes[axisIndex].isSlicerAxis()) {
    			slicerOrPovAxis = axes[axisIndex++];
    		}
    		columnAxis = axes[axisIndex++];

    		// Check if row axis are part of result...
    		if (axisIndex < axes.length) { // row axis is present.
    			rowAxis = axes[axisIndex++];
    		}
    		// Check if page axis are part of result...
    		if (axisIndex < axes.length) { // page axis is present.
    			pageAxis = axes[axisIndex];
    		}

    		if (slicerOrPovAxis != null) {
    			IEssMdMember[] mem = slicerOrPovAxis.getAllTupleMembers(0);
    			int i = 0;
    			for (i = 0; i < mem.length; i++) {
    				this.headerValues.add("pov"+i);
    				pov.add(mem[i].getName());
    			}
    		}

    		if (pageAxis != null) {
    			/*
    			 * Identify #of Pages, i.e., #of Tupes in PageAxis. For every Page,
    			 * print the Grid belonging to that page.
    			 */
    			for (int i = 0; i < pageAxis.getTupleCount(); i++) {
    				IEssMdMember[] mem = pageAxis.getAllTupleMembers(i);
    				int j;
    				for (j = 0; j < mem.length; j++) {
    					this.headerValues.add("page"+i);
    					pov.add(mem[j].getName());
    				}

    				process_Rows_Columns(mddata, pov, columnAxis, rowAxis, i, suppressZero);
    			}
    		} else if (rowAxis != null) {
    			process_Rows_Columns(mddata, pov, columnAxis, rowAxis, 0, suppressZero);
    		}
        }
    }
    
    private void process_Rows_Columns (IEssMdDataSet mddata, List<String> pov, IEssMdAxis columnAxis, IEssMdAxis rowAxis, int pageNum, boolean suppressZero) throws EssException {
		int cols = columnAxis.getTupleCount();
		int rows = rowAxis.getTupleCount();
		List<List<String>> col = new ArrayList();
		if (cols <= 0 || rows <= 0)
			return;
		/*{
			throw new EssException("This Sample has limited support for processing this MDX result in Grid form " +
									"because this has "+cols+" columns & "+rows+" rows."); 
		}*/
		IEssMdMember[] mem = rowAxis.getAllTupleMembers(0);
		for (int i = 0; i < mem.length; i++)
			this.headerValues.add("Row"+i);
		
		mem = columnAxis.getAllTupleMembers(0);
		for (int i = 0; i < mem.length; i++)
			this.headerValues.add("Col"+i);
		
		for (int j = 0; j < cols; j++) {
			List<String> colValue = new ArrayList <String>();
			mem = columnAxis.getAllTupleMembers(j);
			addMemberToList(colValue, mem);
			col.add(colValue);
		}
		this.headerValues.add("Value");
		int k = pageNum * cols * rows;
		for (int i = 0; i < rows; i++) {
			List<String> section = new ArrayList < String >();
			for (int m=0; m < pov.size(); m++)
				section.add(pov.get(m));
			mem = rowAxis.getAllTupleMembers(i);
			addMemberToList(section, mem);
			/*for (int l = 0; l < mem.length; l++) {
				int propcnt = mem[l].getCountProperties();
				for (int propInd = 0; propInd < propcnt; propInd++) {
					IEssValueAny propval = mem[l].getPropertyValueAny(propInd);
					System.out.print(mem[l].getPropertyName(propInd) + ", "
							+ propval + " ");
				}
			}*/
			for (int j = 0; j < cols; j++) {
				boolean addToResult = true;
				List<Object> rowValue = new ArrayList <Object>();
				for (int o=0; o<section.size(); o++)
					rowValue.add(section.get(o));
				for (int q=0; q<col.get(j).size();q++)
					rowValue.add(col.get(j).get(q));
				if (mddata.isMissingCell(k)) {
					rowValue.add(new String(""));
				} 
				else if (mddata.isNoAccessCell(k)) {
					rowValue.add(new String(""));
				} 
				/*else if (mddata.getCellType(k)==IEssMdDataSet.CELLTYPE_DOUBLE) {
					rowValue.add(new Double(mddata.getCellValue(k)));
				}*/
				else {
					String fmtdCellTxtVal = mddata.getFormattedValue(k);
					if (fmtdCellTxtVal != null && fmtdCellTxtVal.length() > 0) {
						rowValue.add(fmtdCellTxtVal);
					} 
					else {
						double val = mddata.getCellValue(k);
						if (suppressZero && val == 0)
							addToResult = false;
						rowValue.add(String.valueOf(val));
					}
				}
				if (addToResult)
					this.rowValues.add(rowValue);
				k++;
			}
		}
    }
    
    private void addMemberToList(List<String> list, IEssMdMember[] mem) throws EssException {
    	for (int i = 0; i < mem.length; i++)
			list.add(mem[i].getName());
    }
    
    public List<String> getHeaderValues() {
    	return this.headerValues;
    }
    
    public String[] getHeaders() {
    	String[] headers = new String[this.headerValues.size()];
    	for (int i=0; i<this.headerValues.size(); i++)
    		headers[i] = this.headerValues.get(i);
    	return headers;
    }
    
    public List<List<Object>> getRowValues() {
	    return this.rowValues;
	}
    
	public Object[][] getRows() {
		Object[][] rows = null;
		if (this.rowValues.size()>0) {
			rows = new Object[this.rowValues.size()][this.rowValues.get(0).size()];
			for (int i=0; i<this.rowValues.size(); i++) 
				for (int j=0; j<this.rowValues.get(i).size(); j++)
					rows[i][j]=this.rowValues.get(i).get(j);
		}
		else
			rows = new Object[0][0];
	    return rows;
	}
	
	public void closeCubeView() {
		try {
			if (this.cubeView != null) this.cubeView.close();
		}
		catch (EssException ee) { }
	}
	
	public void openMaxlSession(String id) throws EssException {
		this.session = olapSvr.openMaxlSession(id);
	}
	
	public boolean execStatement(String maxl) throws EssException {
		return this.session.execute(maxl);
	}
	
	public String getMessages() throws EssException {
		String m ="";
		if (this.session != null)
		{
			ArrayList<String> messages = this.session.getMessages();
		
			for (String message: messages) {
				m+=message+"\n";
			}
		}
		else m+="Session null : pas de message\n";
		return m;
	}
	
	public boolean execStatements(String script) throws EssException {
		String all = script;//SqlCommentScrubber.removeComments(script);
		
		String[] statements = all.split(";");
		String stat;
		int nrstats = 0;
		   
		for(int i=0;i<statements.length;i++) {
			stat = statements[i];
		    if (!Const.onlySpaces(stat))
		    {
		    	String maxl=Const.trim(stat);
		    	nrstats++;
		        this.execStatement(stat);
		    }
		}		       
		return true;
	}
	
	/**
	 * Get list of OLAP applications for the given Essbase server
	 * @return List of application names
	 * @throws EssException In case something goes wrong
	 */
	
	public List<String> getApplications() throws EssException {
		List < String > names = new ArrayList < String >();
		IEssIterator olapApps = this.olapSvr.getApplications();
		for (int i = 0; i < olapApps.getCount(); i++) {
			IEssOlapApplication app = (IEssOlapApplication)olapApps.getAt(i);
			names.add(app.getName());
		}
        return names;
	}
	
	/**
	 * Get list of cubes for a given OLAP application
	 * @return List of cube names
	 * @throws EssException In case something goes wrong
	 */
	
	public List<String> getCubes(String app) throws EssException {
		List < String > names = new ArrayList < String >();
		IEssOlapApplication olapApp = this.olapSvr.getApplication(app);
		IEssIterator cubes = olapApp.getCubes();
		for (int i = 0; i < cubes.getCount(); i++) {
			IEssCube cube = (IEssCube)cubes.getAt(i);
			names.add(cube.getName());
		}
        return names;
	}
	
	/**
	 * Get list of cube dimensions for a given cube
	 * @return List of dimension names
	 * @throws EssException In case something goes wrong
	 */
	
	public List<String> getCubeDimensions(String appName, String cubeName) throws EssException {
		List < String > names = new ArrayList < String >();
		IEssOlapApplication olapApp = this.olapSvr.getApplication(appName);
		IEssCube cube = olapApp.getCube(cubeName);
		IEssIterator dims = cube.getDimensions();
		for (int i = 0; i < dims.getCount(); i++) {
			IEssDimension dim = (IEssDimension)dims.getAt(i);
			names.add(dim.getName());
		}
        return names;
	}
	
	/**
	 * @return return the instanciated iEssCube initialized by initCube() method
	 */
	
	public IEssCube getCube() {
		return this.cube;
	}
	
	/**
	 * Initialize a given cube of a given application
	 * @throws EssException In case something goes wrong
	 */
	
	public void initCube(String appName, String cubeName) throws EssException {
		IEssOlapApplication olapApp = this.olapSvr.getApplication(appName);
		this.cube = olapApp.getCube(cubeName);
	}
	
	/**
	 * Clear the data of a cube
	 * @throws EssException In case something goes wrong
	 */
	
	public void clearCube(IEssCube cube) throws EssException {
		cube.clearAllData();
	}
	
	/**
	 * Load the data in a cube
	 * @throws EssException In case something goes wrong
	 */
		
	public void loadData(IEssCube cube, StringBuffer rows) throws EssException {
		if (cube.getCubeType().equals(IEssCube.EEssCubeType.ASO_INT_VALUE))
			return;
		else			
			cube.loadData(rows.toString(), IEssOlapFileObject.TYPE_ALL, null, false, true, true);
	}
	
	/**
	 * Prepare cube for data loading
	 * This method is used in a sequence beginDataload, sendString, endDataload
	 * @throws EssException In case something goes wrong
	 */
	
	public void beginDataload(IEssCube cube) throws EssException {
		cube.beginDataload(true, false, true, null, IEssOlapFileObject.TYPE_ALL, 0);
	}
	
	/**
	 * Prepare cube for dimension building
	 * This method is used in a sequence beginDataload, sendString, endDataload
	 * @throws EssException In case something goes wrong
	 */
	
	public void beginBuildDim(IEssCube cube) throws EssException {
		cube.beginStreamBuildDim( null, IEssOlapFileObject.TYPE_RULES, IEssCube.ESS_INCDIMBUILD_ALL,  null);
	}
	
	
	/**
	 * Send data in a cube
	 * This method is used in a sequence beginDataload, sendString, endDataload
	 * @throws EssException In case something goes wrong
	 */
	
	public void sendString(IEssCube cube, Object[] row) throws EssException {
		StringBuffer data = new StringBuffer();
		for (int i = 0; i < row.length; i++) {
		   data.append( row[i] );
		   if (i<row.length-1) data.append( " " );
		}
		cube.sendString(data.toString());
	}
	
	/**
	 * Close the process of loading data in a cube
	 * This method is used in a sequence beginDataload, sendString, endDataload
	 * @throws EssException In case something goes wrong
	 */
	
	public void endDataload(IEssCube cube) throws EssException {
		cube.endDataload();
	}
	
	/**
	 * Close the process of building dimension of a cube
	 * This method is used in a sequence beginDataload, sendString, endDataload
	 * @throws EssException In case something goes wrong
	 */
	
	public void endBuildDim(IEssCube cube) throws EssException {
		cube.endStreamBuildDim(null,true);
	}
	
	/**
	 * Close a cube after use
	 * @throws EssException In case something goes wrong
	 */
	
	public void closeCube(IEssCube cube) throws EssException {
		cube.endUpdate();
	}
}