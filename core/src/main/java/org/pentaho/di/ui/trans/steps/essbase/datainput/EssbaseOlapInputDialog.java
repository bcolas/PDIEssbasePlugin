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

package org.pentaho.di.ui.trans.steps.essbase.datainput;

import java.util.List;

import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.FocusAdapter;
import org.eclipse.swt.events.FocusEvent;
import org.eclipse.swt.events.KeyAdapter;
import org.eclipse.swt.events.KeyEvent;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.MouseAdapter;
import org.eclipse.swt.events.MouseEvent;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.program.Program;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Link;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Props;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.essbase.core.DimensionField;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.TransPreviewFactory;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.errorhandling.StreamInterface;
import org.pentaho.di.ui.core.dialog.EnterNumberDialog;
import org.pentaho.di.ui.core.dialog.EnterTextDialog;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.core.dialog.PreviewRowsDialog;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.StyledTextComp;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.trans.dialog.TransPreviewProgressDialog;
import org.pentaho.di.ui.trans.step.BaseStepDialog;
import org.pentaho.di.ui.trans.steps.olapinput.MDXValuesHighlight;
import org.pentaho.di.trans.steps.essbase.datainput.EssbaseOlapInputMeta;
import org.pentaho.di.trans.steps.essbase.datainput.EssbaseOlapInputData;;

public class EssbaseOlapInputDialog extends BaseStepDialog implements StepDialogInterface {
	private static Class<?> PKG = EssbaseOlapInputMeta.class; // for i18n purposes,
														// needed by
														// Translator2!!
														// $NON-NLS-1$

	private EssbaseOlapInputMeta meta;
	
	private CCombo addConnectionLine;
	
	private Combo comboApplication;
	private Label labelApplication;
	private Combo comboCube;
	private Label labelCube;
	
	private Label wlMDX;
	private StyledTextComp wMDX;
	private FormData fdlMDX, fdMDX;

	private MDXValuesHighlight lineStyler = new MDXValuesHighlight();

	private Label wlPosition;
	private FormData fdlPosition;

	private Label wlVariables;
	private Button wVariables;
	private FormData fdlVariables, fdVariables;
	
	private Label wlSuppress;
	private Button wSuppress;
	private FormData fdlSuppress, fdSuppress;
	
	private Label wlStepfrom;
	private CCombo wStepfrom;
	private FormData fdlStepfrom, fdStepfrom;
    private Listener lsStepfrom;
	
	private Label wlEachRow;
    private Button wEachRow;
    private FormData fdlEachRow, fdEachRow;


	public EssbaseOlapInputDialog(Shell parent, Object in, TransMeta transMeta,	String sname) {
		super(parent, (BaseStepMeta) in, transMeta, sname);
		this.meta = (EssbaseOlapInputMeta) in;
	}

	public String open() {
		Shell parent = getParent();
		Display display = parent.getDisplay();

		shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);
		props.setLook(shell);
		setShellImage(shell, meta);

		ModifyListener lsMod = new ModifyListener() {
			public void modifyText(ModifyEvent e) {
				meta.setChanged();
			}
		};
		changed = meta.hasChanged();

		FormLayout formLayout = new FormLayout();
		formLayout.marginWidth = Const.FORM_MARGIN;
		formLayout.marginHeight = Const.FORM_MARGIN;

		shell.setLayout(formLayout);
		shell.setText(BaseMessages.getString(PKG, "EssbaseDataInputDialog.EssbaseDataInput")); //$NON-NLS-1$

		int middle = props.getMiddlePct();
		int margin = Const.MARGIN;

		FormData fd;
		// Stepname line
		//
		wlStepname = new Label(shell, SWT.RIGHT);
		wlStepname.setText(BaseMessages.getString(PKG,
				"EssbaseDataInputDialog.StepName")); //$NON-NLS-1$
		props.setLook(wlStepname);
		fdlStepname = new FormData();
		fdlStepname.left = new FormAttachment(0, 0);
		fdlStepname.right = new FormAttachment(middle, -margin);
		fdlStepname.top = new FormAttachment(0, margin);
		wlStepname.setLayoutData(fdlStepname);
		wStepname = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
		wStepname.setText(stepname);
		props.setLook(wStepname);
		wStepname.addModifyListener(lsMod);
		fdStepname = new FormData();
		fdStepname.left = new FormAttachment(middle, 0);
		fdStepname.top = new FormAttachment(0, margin);
		fdStepname.right = new FormAttachment(100, 0);
		wStepname.setLayoutData(fdStepname);

		// The connection
		//
		addConnectionLine = addConnectionLine(shell, wStepname, Const.MIDDLE_PCT, margin);

		// Application
		//
		labelApplication = new Label(shell, SWT.RIGHT);
	    fd = new FormData();
	    fd.left = new FormAttachment(0, 0);
	    fd.right = new FormAttachment(middle, -margin);
	    fd.top = new FormAttachment(addConnectionLine, margin);
	    labelApplication.setLayoutData(fd);

	    comboApplication = new Combo(shell, SWT.READ_ONLY);
	    fd = new FormData();
	    fd.left = new FormAttachment(middle, 0);
	    fd.right = new FormAttachment(100, 0);
	    fd.top = new FormAttachment(addConnectionLine, margin);
	    comboApplication.setLayoutData(fd);

		// The cube
		//
	    labelCube = new Label(shell, SWT.RIGHT);
	    fd = new FormData();
	    fd.left = new FormAttachment(0, 0);
	    fd.right = new FormAttachment(middle, -margin);
	    fd.top = new FormAttachment(comboApplication, margin);
	    labelCube.setLayoutData(fd);

	    comboCube = new Combo(shell, SWT.READ_ONLY);
	    fd = new FormData();
	    fd.left = new FormAttachment(middle, 0);
	    fd.right = new FormAttachment(100, 0);
	    fd.top = new FormAttachment(comboApplication, margin);
	    comboCube.setLayoutData(fd);
	    
	    // Some buttons
	 	//
	 	wOK = new Button(shell, SWT.PUSH);
	 	wOK.setText(BaseMessages.getString(PKG, "System.Button.OK")); //$NON-NLS-1$
	 	wPreview = new Button(shell, SWT.PUSH);
	 	wPreview.setText(BaseMessages.getString(PKG, "System.Button.Preview")); //$NON-NLS-1$
	 	wCancel = new Button(shell, SWT.PUSH);
	 	wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel")); //$NON-NLS-1$

	 	setButtonPositions(new Button[] { wOK, wPreview, wCancel }, margin, null);
	 	
	 	// Execute for each row?
        wlEachRow = new Label(shell, SWT.RIGHT);
        wlEachRow.setText(BaseMessages.getString(PKG, "EssbaseDataInputDialog.ExecuteForEachRow")); //$NON-NLS-1$
        props.setLook(wlEachRow);
        fdlEachRow = new FormData();
        fdlEachRow.left = new FormAttachment(0, 0);
        fdlEachRow.right = new FormAttachment(middle, -margin);
        fdlEachRow.bottom = new FormAttachment(wOK, -8 *margin);
        wlEachRow.setLayoutData(fdlEachRow);
        wEachRow = new Button(shell, SWT.CHECK);
        props.setLook(wEachRow);
        fdEachRow = new FormData();
        fdEachRow.left = new FormAttachment(middle, 0);
        fdEachRow.right = new FormAttachment(100, 0);
        fdEachRow.bottom = new FormAttachment(wOK, -8 *margin);
        wEachRow.setLayoutData(fdEachRow);
        SelectionAdapter lsSelMod = new SelectionAdapter()
        {
            public void widgetSelected(SelectionEvent arg0)
            {
                meta.setChanged();
            }
        };
        wEachRow.addSelectionListener(lsSelMod);
        
        // Read date from...
     	wlStepfrom=new Label(shell, SWT.RIGHT);
     	wlStepfrom.setText(BaseMessages.getString(PKG, "EssbaseDataInputDialog.InsertDataFromStep")); //$NON-NLS-1$
      	props.setLook(wlStepfrom);
     	fdlStepfrom=new FormData();
     	fdlStepfrom.left = new FormAttachment(0, 0);
     	fdlStepfrom.right= new FormAttachment(middle, -margin);
     	fdlStepfrom.bottom = new FormAttachment(wEachRow, -margin);
     	wlStepfrom.setLayoutData(fdlStepfrom);
     	wStepfrom=new CCombo(shell, SWT.BORDER );
      	props.setLook(wStepfrom);

      	List<StepMeta> previousSteps = transMeta.findPreviousSteps(transMeta.findStep(stepname));
      	for (StepMeta stepMeta : previousSteps) {
     		wStepfrom.add(stepMeta.getName());
     	}
     		
     	wStepfrom.addModifyListener(lsMod);
     	fdStepfrom=new FormData();
     	fdStepfrom.left = new FormAttachment(middle, 0);
     	fdStepfrom.right= new FormAttachment(100, 0);
     	fdStepfrom.bottom = new FormAttachment(wEachRow, -margin);
     	wStepfrom.setLayoutData(fdStepfrom);
		
		// Suppress Zero and Null ?
		//
		wlSuppress = new Label(shell, SWT.RIGHT);
		wlSuppress.setText(BaseMessages.getString(PKG, "EssbaseDataInputDialog.SuppressZero")); //$NON-NLS-1$
		fdlSuppress = new FormData();
		fdlSuppress.left = new FormAttachment(0, 0);
		fdlSuppress.right = new FormAttachment(middle, -margin);
		fdlSuppress.bottom = new FormAttachment(wStepfrom, -margin);
		wlSuppress.setLayoutData(fdlSuppress);
		wSuppress = new Button(shell, SWT.CHECK);
		
		wSuppress.setToolTipText(BaseMessages.getString(PKG, "EssbaseDataInputDialog.SuppressZero.Tooltip")); //$NON-NLS-1$
		fdSuppress = new FormData();
		fdSuppress.left = new FormAttachment(middle, 0);
		fdSuppress.right = new FormAttachment(100, 0);
		fdSuppress.bottom = new FormAttachment(wStepfrom, -margin);
		wSuppress.setLayoutData(fdSuppress);
		
		// Replace variables in MDX?
		//
		wlVariables = new Label(shell, SWT.RIGHT);
		wlVariables.setText(BaseMessages.getString(PKG, "EssbaseDataInputDialog.ReplaceVariables")); //$NON-NLS-1$
		fdlVariables = new FormData();
		fdlVariables.left = new FormAttachment(0, 0);
		fdlVariables.right = new FormAttachment(middle, -margin);
		fdlVariables.bottom = new FormAttachment(wSuppress, -margin);
		wlVariables.setLayoutData(fdlVariables);
		wVariables = new Button(shell, SWT.CHECK);
		
		wVariables.setToolTipText(BaseMessages.getString(PKG, "EssbaseDataInputDialog.ReplaceVariables.Tooltip")); //$NON-NLS-1$
		fdVariables = new FormData();
		fdVariables.left = new FormAttachment(middle, 0);
		fdVariables.right = new FormAttachment(100, 0);
		fdVariables.bottom = new FormAttachment(wSuppress, -margin);
		wVariables.setLayoutData(fdVariables);
		wVariables.addSelectionListener(new SelectionAdapter() {
			public void widgetSelected(SelectionEvent arg0) {
				setSQLToolTip();
			}
		});
		
		wlPosition = new Label(shell, SWT.NONE);
		fdlPosition = new FormData();
		fdlPosition.left = new FormAttachment(0, 0);
		fdlPosition.right = new FormAttachment(50, 0);
		fdlPosition.bottom = new FormAttachment(wVariables, -2 * margin);
		wlPosition.setLayoutData(fdlPosition);	 		
	 		
	    // Table line...
	 	//
	 	wlMDX = new Label(shell, SWT.NONE);
	 	wlMDX.setText(BaseMessages.getString(PKG, "EssbaseDataInputDialog.MDX")); //$NON-NLS-1$
	 	fdlMDX = new FormData();
	 	fdlMDX.left = new FormAttachment(0, 0);
	 	fdlMDX.top = new FormAttachment(comboCube, 2 * margin);
	 	wlMDX.setLayoutData(fdlMDX);

	 	wMDX = new StyledTextComp(transMeta, shell, SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL, "");
	 	wMDX.addModifyListener(lsMod);
	 	fdMDX = new FormData();
	 	fdMDX.left = new FormAttachment(0, 0);
	 	fdMDX.top = new FormAttachment(wlMDX, margin);
	 	fdMDX.right = new FormAttachment(100, -2*margin);
	 	fdMDX.bottom = new FormAttachment(wlPosition, -margin);
	 	wMDX.setLayoutData(fdMDX);

	 	wMDX.addModifyListener(new ModifyListener() {
			public void modifyText(ModifyEvent arg0) {
				setSQLToolTip();
				setPosition();
			}
		});

		wMDX.addKeyListener(new KeyAdapter() {
			public void keyPressed(KeyEvent e) {
				setPosition();
			}

			public void keyReleased(KeyEvent e) {
				setPosition();
			}
		});
		wMDX.addFocusListener(new FocusAdapter() {
			public void focusGained(FocusEvent e) {
				setPosition();
			}

			public void focusLost(FocusEvent e) {
				setPosition();
			}
		});
		wMDX.addMouseListener(new MouseAdapter() {
			public void mouseDoubleClick(MouseEvent e) {
				setPosition();
			}

			public void mouseDown(MouseEvent e) {
				setPosition();
			}

			public void mouseUp(MouseEvent e) {
				setPosition();
			}
		});

		// Text Higlighting
		lineStyler = new MDXValuesHighlight();
		wMDX.addLineStyleListener(lineStyler);

		// Add listeners
		lsCancel = new Listener() {
			public void handleEvent(Event e) {
				cancel();
			}
		};
		lsPreview = new Listener() {
			public void handleEvent(Event e) {
				preview();
			}
		};
		lsOK = new Listener() {
			public void handleEvent(Event e) {
				ok();
			}
		};
		lsStepfrom = new Listener() { 
			public void handleEvent(Event e) { 
				setFags(); 
			}
		};

		wCancel.addListener(SWT.Selection, lsCancel);
		wPreview.addListener(SWT.Selection, lsPreview);
		wOK.addListener(SWT.Selection, lsOK);
		wStepfrom.addListener(SWT.Selection, lsStepfrom);
        wStepfrom.addListener(SWT.FocusOut,  lsStepfrom);

		lsDef = new SelectionAdapter() {
			public void widgetDefaultSelected(SelectionEvent e) {
				ok();
			}
		};

		wStepname.addSelectionListener(lsDef);
		
		addConnectionLine.addSelectionListener(new SelectionAdapter() {
			public void widgetSelected(SelectionEvent e) {
				doSelectConnection(true);
		     }
		});
		comboApplication.addSelectionListener(new SelectionAdapter() {
			public void widgetSelected(SelectionEvent e) {
		          doSelectApplication();
		    }
		});
		
		labelApplication.setText(BaseMessages.getString(PKG, "EssbaseDataInputDialog.SelectApplication"));
	    labelCube.setText(BaseMessages.getString(PKG, "EssbaseDataInputDialog.SelectCube"));
	    
	    this.fillStoredData();
	    this.doSelectConnection(false);
	    
	    props.setLook(labelApplication);
	    props.setLook(labelCube);
	    props.setLook(comboApplication);
	    props.setLook(comboCube);
	    props.setLook(addConnectionLine);	 
	 	props.setLook(wlMDX);
	 	props.setLook(wMDX, Props.WIDGET_STYLE_FIXED);
		props.setLook(wlPosition);	 	
		props.setLook(wlVariables);		
		props.setLook(wVariables);		

		// Detect X or ALT-F4 or something that kills this window...
		shell.addShellListener(new ShellAdapter() {
			public void shellClosed(ShellEvent e) {
				cancel();
			}
		});

		getData();
		meta.setChanged(changed);

		// Set the shell size, based upon previous time...
		setSize();

		shell.open();
		while (!shell.isDisposed()) {
			if (!display.readAndDispatch())
				display.sleep();
		}
		return stepname;
	}
	
	private void fillStoredData() {
	    if (stepname != null)
	    	wStepname.setText(stepname);

	    int index = addConnectionLine.indexOf(meta.getDatabaseMeta() != null ? meta.getDatabaseMeta().getName() : "");
	    if (index >= 0)
	      addConnectionLine.select(index);

	    if (meta.getApplication() != null) {
	        comboApplication.add(meta.getApplication());
	        comboApplication.select(0);
	      }
	    
	    if (meta.getCube() != null) {
	      comboCube.add(meta.getCube());
	      comboCube.select(0);
	    }

	    String[] fieldNames = null;
	    try {
	      RowMetaInterface r = transMeta.getPrevStepFields(stepname);
	      fieldNames = r.getFieldNames();
	    } catch (Exception e) {  }

	  }

	  private void doSelectConnection(boolean clearCurrentData) {
	    try {
	      if (clearCurrentData) {
	        comboApplication.removeAll();
	        comboCube.removeAll();
	      }

	      if (addConnectionLine.getText() != null) {
	        DatabaseMeta dbMeta = transMeta.findDatabase(addConnectionLine.getText());
	        if (dbMeta != null) {
	          EssbaseOlapInputData data = new EssbaseOlapInputData(dbMeta);
	          data.helper.connect();
	          List<String> apps = data.helper.getApplications();
	          for (String appName : apps) {
	            if (comboApplication.indexOf(appName) == -1)
	              comboApplication.add(appName);
	          }
	          data.helper.disconnect();
	        }
	      }
	    } catch (Exception ex) {
	      new ErrorDialog(shell, BaseMessages.getString(PKG, "EssbaseDataInputDialog.RetreiveAppsErrorTitle"), BaseMessages.getString(PKG, "EssbaseDataInputDialog.RetreiveAppsError"), ex);
	    }
	  }
	  
	  private void doSelectApplication() {
		  try {
		      comboCube.removeAll();
		      if (addConnectionLine.getText() != null) {
		          DatabaseMeta dbMeta = transMeta.findDatabase(addConnectionLine.getText());
		          if (comboApplication.getText() != null) {
		        	  String appName = comboApplication.getText();
		        	  if (dbMeta != null) {
		        		  EssbaseOlapInputData data = new EssbaseOlapInputData(dbMeta);
		        		  data.helper.connect();
		        		  List<String> cubes = data.helper.getCubes(appName);
		        		  for (String cubeName : cubes) {
		        			  if (comboCube.indexOf(cubeName) == -1)
		        				  comboCube.add(cubeName);
		        		  }
		        		  data.helper.disconnect();
		        	  }
		          }
		      }
		  } catch (Exception ex) {
		      new ErrorDialog(shell, BaseMessages.getString(PKG, "EssbaseDataInputDialog.RetreiveCubesErrorTitle"), BaseMessages.getString(PKG, "EssbaseDataInputDialog.RetreiveCubesError"), ex);
		  }	
	  }

	public void setPosition() {

		String scr = wMDX.getText();
		int linenr = wMDX.getLineAtOffset(wMDX.getCaretOffset()) + 1;
		int posnr = wMDX.getCaretOffset();

		// Go back from position to last CR: how many positions?
		int colnr = 0;
		while (posnr > 0 && scr.charAt(posnr - 1) != '\n'
				&& scr.charAt(posnr - 1) != '\r') {
			posnr--;
			colnr++;
		}
		wlPosition.setText(BaseMessages.getString(PKG,
				"EssbaseDataInputDialog.Position.Label", "" + linenr, "" + colnr));

	}

	protected void setSQLToolTip() {
		if (wVariables.getSelection())
			wMDX.setToolTipText(transMeta.environmentSubstitute(wMDX.getText()));
	}

	/**
	 * Copy information from the meta-data input to the dialog fields.
	 */
	public void getData() {
		this.wMDX.setText(Const.NVL(meta.getMdx(), ""));
		this.wVariables.setSelection(meta.isVariableReplacementActive());
		this.wSuppress.setSelection(meta.isSuppressZeroActive());
		
		StreamInterface infoStream = meta.getStepIOMeta().getInfoStreams().get(0);
	    if (infoStream.getStepMeta()!=null)
        {
            this.wStepfrom.setText(infoStream.getStepname());
            this.wEachRow.setSelection(meta.isExecuteEachInputRow());
        }
        else
        {
            this.wEachRow.setEnabled(false);
            this.wlEachRow.setEnabled(false);
        }

		this.wStepname.selectAll();
	}

	private void cancel() {
		stepname = null;
		meta.setChanged(changed);
		dispose();
	}

	private void getInfo(EssbaseOlapInputMeta meta) {
		meta.setDatabaseMeta(transMeta.findDatabase(addConnectionLine.getText()));
		meta.setApplication(this.comboApplication.getText());
	    meta.setCube(this.comboCube.getText());
		meta.setMdx(wMDX.getText());
		StreamInterface infoStream = meta.getStepIOMeta().getInfoStreams().get(0);
        infoStream.setStepMeta( transMeta.findStep( wStepfrom.getText() ) );
        meta.setExecuteEachInputRow(wEachRow.getSelection());
		meta.setVariableReplacementActive(wVariables.getSelection());
		meta.setSuppressZeroActive(this.wSuppress.getSelection());
		meta.setChanged(true);
	}

	private void ok() {
		if (Const.isEmpty(wStepname.getText()))
			return;

		stepname = wStepname.getText(); // return value
		// copy info to TextFileInputMeta class (input)

		getInfo(this.meta);

		dispose();
	}
	
	private void setFags()
    {
        if (wStepfrom.getText() != null && wStepfrom.getText().length() > 0)
        {
            // The foreach check box... 
            wEachRow.setEnabled(true);
            wlEachRow.setEnabled(true);
            
            // The preview button...
            wPreview.setEnabled(false);
        }
        else
        {
            // The foreach check box... 
            wEachRow.setEnabled(false);
            wEachRow.setSelection(false);
            wlEachRow.setEnabled(false);
            
            // The preview button...
            wPreview.setEnabled(true);
        }
        
    }

	/**
	 * Preview the data generated by this step. This generates a transformation
	 * using this step & a dummy and previews it.
	 * 
	 */
	private void preview() {
		// Create the table input reader step...
		EssbaseOlapInputMeta oneMeta = new EssbaseOlapInputMeta();
		getInfo(oneMeta);

		TransMeta previewMeta = TransPreviewFactory
				.generatePreviewTransformation(transMeta, oneMeta, wStepname
						.getText());

		EnterNumberDialog numberDialog = new EnterNumberDialog(
				shell,
				props.getDefaultPreviewSize(),
				BaseMessages.getString(PKG, "EssbaseDataInputDialog.EnterPreviewSize"), BaseMessages.getString(PKG, "EssbaseDataInputDialog.NumberOfRowsToPreview")); //$NON-NLS-1$ //$NON-NLS-2$
		int previewSize = numberDialog.open();
		if (previewSize > 0) {
			TransPreviewProgressDialog progressDialog = new TransPreviewProgressDialog(
					shell, previewMeta, new String[] { wStepname.getText() },
					new int[] { previewSize });
			progressDialog.open();

			Trans trans = progressDialog.getTrans();
			String loggingText = progressDialog.getLoggingText();

			if (!progressDialog.isCancelled()) {
				if (trans.getResult() != null
						&& trans.getResult().getNrErrors() > 0) {
					EnterTextDialog etd = new EnterTextDialog(shell,
							BaseMessages.getString(PKG,
									"System.Dialog.PreviewError.Title"),
							BaseMessages.getString(PKG,
									"System.Dialog.PreviewError.Message"),
							loggingText, true);
					etd.setReadOnly();
					etd.open();
				}
			}

			PreviewRowsDialog prd = new PreviewRowsDialog(shell, transMeta,
					SWT.NONE, wStepname.getText(), progressDialog
							.getPreviewRowsMeta(wStepname.getText()),
					progressDialog.getPreviewRows(wStepname.getText()),
					loggingText);
			prd.open();
		}
	}
}
