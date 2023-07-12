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

package org.pentaho.di.ui.job.entries.essbase;

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
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.FileDialog;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Props;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entries.essbase.JobEntryMAXL;
import org.pentaho.di.job.entry.JobEntryDialogInterface;
import org.pentaho.di.job.entry.JobEntryInterface;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.ui.core.gui.WindowProperty;
import org.pentaho.di.ui.core.widget.StyledTextComp;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.job.dialog.JobDialog;
import org.pentaho.di.ui.job.entry.JobEntryDialog;
import org.pentaho.di.ui.trans.step.BaseStepDialog;
//import org.pentaho.di.ui.trans.steps.tableinput.SQLValuesHighlight;



/**
 * This dialog allows you to edit the MAXL job entry settings. (select the essbase connection and the maxl script to be executed)
 * 
 * @author Benoit COLAS
 * @since 30-07-2014
 */
public class JobEntryMAXLDialog extends JobEntryDialog implements JobEntryDialogInterface
{
	private static Class<?> PKG = JobEntryMAXL.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$

	private static final String[] FILETYPES = new String[] { BaseMessages.getString(PKG, "JobMAXL.Filetype.Maxl"), BaseMessages.getString(PKG, "JobMAXL.Filetype.Text"), BaseMessages.getString(PKG, "JobMAXL.Filetype.All") };

	
    private Label wlName;

    private Text wName;

    private FormData fdlName, fdName;

    private CCombo wConnection;

    private Label wlUseSubs;

    private Button wUseSubs;
    
    private Button wMAXLFromFile;
    
    private Label wlMAXLFromFile;

    private FormData fdlUseSubs, fdUseSubs;
    
    private FormData fdlMAXLFromFile, fdMAXLFromFile;

    private Label wlMAXL;

    private StyledTextComp wMAXL;

    private FormData fdlMAXL, fdMAXL;

    private Label wlPosition;

    private FormData fdlPosition;

    private Button wOK, wCancel;

    private Listener lsOK, lsCancel;

    private JobEntryMAXL jobEntry;

    private Shell shell;

    private SelectionAdapter lsDef;

    private boolean changed;
    
    private Label wlUseOneStatement;

    private Button wSendOneStatement;
    
    private FormData fdlUseOneStatement, fdUseOneStatement;

	// File
	private Label wlFilename;
	private Button wbFilename;
	private TextVar wFilename;
	private FormData fdlFilename, fdbFilename, fdFilename;
	
    public JobEntryMAXLDialog(Shell parent, JobEntryInterface jobEntryInt, Repository rep, JobMeta jobMeta)
    {
        super(parent, jobEntryInt, rep, jobMeta);
        jobEntry = (JobEntryMAXL) jobEntryInt;
        if (this.jobEntry.getName() == null)
            this.jobEntry.setName(BaseMessages.getString(PKG, "JobMAXL.Name.Default"));
    }

    public JobEntryInterface open()
    {
        Shell parent = getParent();
        Display display = parent.getDisplay();

        shell = new Shell(parent, props.getJobsDialogStyle());
        props.setLook(shell);
        JobDialog.setShellImage(shell, jobEntry);

        ModifyListener lsMod = new ModifyListener()
        {
            public void modifyText(ModifyEvent e)
            {
                jobEntry.setChanged();
            }
        };
        changed = jobEntry.hasChanged();

        FormLayout formLayout = new FormLayout();
        formLayout.marginWidth = Const.FORM_MARGIN;
        formLayout.marginHeight = Const.FORM_MARGIN;

        shell.setLayout(formLayout);
        shell.setText(BaseMessages.getString(PKG, "JobMAXL.Title"));

        int middle = props.getMiddlePct();
        int margin = Const.MARGIN;

        wOK = new Button(shell, SWT.PUSH);
        wOK.setText(BaseMessages.getString(PKG, "System.Button.OK"));
        wCancel = new Button(shell, SWT.PUSH);
        wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));

        BaseStepDialog.positionBottomButtons(shell, new Button[] { wOK, wCancel }, margin, null);

        // Filename line
        wlName = new Label(shell, SWT.RIGHT);
        wlName.setText(BaseMessages.getString(PKG, "JobMAXL.Name.Label"));
        props.setLook(wlName);
        fdlName = new FormData();
        fdlName.left = new FormAttachment(0, 0);
        fdlName.right = new FormAttachment(middle, 0);
        fdlName.top = new FormAttachment(0, margin);
        wlName.setLayoutData(fdlName);
        wName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(wName);
        wName.addModifyListener(lsMod);
        fdName = new FormData();
        fdName.left = new FormAttachment(middle, 0);
        fdName.top = new FormAttachment(0,  margin);
        fdName.right = new FormAttachment(100, 0);
        wName.setLayoutData(fdName);
        
		// Connection line
		wConnection = addConnectionLine(shell, wName, middle, margin);
		if (jobEntry.getDatabase()==null && jobMeta.nrDatabases()==1) wConnection.select(0);
		wConnection.addModifyListener(lsMod);
		

        // MAXL from file?
        wlMAXLFromFile = new Label(shell, SWT.RIGHT);
        wlMAXLFromFile.setText(BaseMessages.getString(PKG, "JobMAXL.MAXLFromFile.Label"));
        props.setLook(wlMAXLFromFile);
        fdlMAXLFromFile = new FormData();
        fdlMAXLFromFile.left = new FormAttachment(0, 0);
        fdlMAXLFromFile.top = new FormAttachment(wConnection,2*margin);
        fdlMAXLFromFile.right = new FormAttachment(middle, -margin);
        wlMAXLFromFile.setLayoutData(fdlMAXLFromFile);
        wMAXLFromFile = new Button(shell, SWT.CHECK);
        props.setLook(wMAXLFromFile);
        wMAXLFromFile.setToolTipText(BaseMessages.getString(PKG, "JobMAXL.MAXLFromFile.Tooltip"));
        fdMAXLFromFile = new FormData();
        fdMAXLFromFile.left = new FormAttachment(middle, 0);
        fdMAXLFromFile.top = new FormAttachment(wConnection, 2*margin);
        fdMAXLFromFile.right = new FormAttachment(100, 0);
        wMAXLFromFile.setLayoutData(fdMAXLFromFile);
        wMAXLFromFile.addSelectionListener(new SelectionAdapter()
        {
            public void widgetSelected(SelectionEvent e)
            {
            	activeMAXLFromFile();	
                jobEntry.setChanged();
            }
        });


		// Filename line
		wlFilename = new Label(shell, SWT.RIGHT);
		wlFilename.setText(BaseMessages.getString(PKG, "JobMAXL.Filename.Label"));
		props.setLook(wlFilename);
		fdlFilename = new FormData();
		fdlFilename.left = new FormAttachment(0, 0);
		fdlFilename.top = new FormAttachment(wMAXLFromFile,  margin);
		fdlFilename.right = new FormAttachment(middle, -margin);
		wlFilename.setLayoutData(fdlFilename);

		wbFilename = new Button(shell, SWT.PUSH | SWT.CENTER);
		props.setLook(wbFilename);
		wbFilename.setText(BaseMessages.getString(PKG, "System.Button.Browse"));
		fdbFilename = new FormData();
		fdbFilename.right = new FormAttachment(100, 0);
		fdbFilename.top = new FormAttachment(wMAXLFromFile, margin);
		wbFilename.setLayoutData(fdbFilename);

		wFilename = new TextVar(jobMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
		props.setLook(wFilename);
		wFilename.setToolTipText(BaseMessages.getString(PKG, "JobMAXL.Filename.Tooltip"));
		wFilename.addModifyListener(lsMod);
		fdFilename = new FormData();
		fdFilename.left = new FormAttachment(middle, 0);
		fdFilename.top = new FormAttachment(wMAXLFromFile,  margin);
		fdFilename.right = new FormAttachment(wbFilename, -margin);
		wFilename.setLayoutData(fdFilename);

		// Whenever something changes, set the tooltip to the expanded version:
		wFilename.addModifyListener(new ModifyListener()
		{
			public void modifyText(ModifyEvent e)
			{
				wFilename.setToolTipText(jobMeta.environmentSubstitute(wFilename.getText()));
			}
		});

		wbFilename.addSelectionListener(new SelectionAdapter()
		{
			public void widgetSelected(SelectionEvent e)
			{
				FileDialog dialog = new FileDialog(shell, SWT.OPEN);
				dialog.setFilterExtensions(new String[] { "*.maxl", "*.txt", "*" });
				if (wFilename.getText() != null)
				{
					dialog.setFileName(jobMeta.environmentSubstitute(wFilename.getText()));
				}
				dialog.setFilterNames(FILETYPES);
				if (dialog.open() != null)
				{
					wFilename.setText(dialog.getFilterPath() + Const.FILE_SEPARATOR
						+ dialog.getFileName());
				}
			}
		});

        // Send one MAXL Statement?
        wlUseOneStatement = new Label(shell, SWT.RIGHT);
        wlUseOneStatement.setText(BaseMessages.getString(PKG, "JobMAXL.SendOneStatement.Label"));
        props.setLook(wlUseOneStatement);
        fdlUseOneStatement = new FormData();
        fdlUseOneStatement.left = new FormAttachment(0, 0);
        fdlUseOneStatement.top = new FormAttachment(wbFilename, margin);
        fdlUseOneStatement.right = new FormAttachment(middle, -margin);
        wlUseOneStatement.setLayoutData(fdlUseOneStatement);
        wSendOneStatement = new Button(shell, SWT.CHECK);
        props.setLook(wSendOneStatement);
        wSendOneStatement.setToolTipText(BaseMessages.getString(PKG, "JobMAXL.SendOneStatement.Tooltip"));
        fdUseOneStatement = new FormData();
        fdUseOneStatement.left = new FormAttachment(middle, 0);
        fdUseOneStatement.top = new FormAttachment(wbFilename, margin);
        fdUseOneStatement.right = new FormAttachment(100, 0);
        wSendOneStatement.setLayoutData(fdUseOneStatement);
        wSendOneStatement.addSelectionListener(new SelectionAdapter()
        {
            public void widgetSelected(SelectionEvent e)
            {
                jobEntry.setChanged();
            }
        });
		
        // Use variable substitution?
        wlUseSubs = new Label(shell, SWT.RIGHT);
        wlUseSubs.setText(BaseMessages.getString(PKG, "JobMAXL.UseVariableSubst.Label"));
        props.setLook(wlUseSubs);
        fdlUseSubs = new FormData();
        fdlUseSubs.left = new FormAttachment(0, 0);
        fdlUseSubs.top = new FormAttachment(wSendOneStatement, margin);
        fdlUseSubs.right = new FormAttachment(middle, -margin);
        wlUseSubs.setLayoutData(fdlUseSubs);
        wUseSubs = new Button(shell, SWT.CHECK);
        props.setLook(wUseSubs);
        wUseSubs.setToolTipText(BaseMessages.getString(PKG, "JobMAXL.UseVariableSubst.Tooltip"));
        fdUseSubs = new FormData();
        fdUseSubs.left = new FormAttachment(middle, 0);
        fdUseSubs.top = new FormAttachment(wSendOneStatement, margin);
        fdUseSubs.right = new FormAttachment(100, 0);
        wUseSubs.setLayoutData(fdUseSubs);
        wUseSubs.addSelectionListener(new SelectionAdapter()
        {
            public void widgetSelected(SelectionEvent e)
            {
                jobEntry.setUseVariableSubstitution(!jobEntry.getUseVariableSubstitution());
                jobEntry.setChanged();
            }
        });

        wlPosition = new Label(shell, SWT.NONE);
        wlPosition.setText(BaseMessages.getString(PKG, "JobMAXL.LineNr.Label", "0"));
        props.setLook(wlPosition);
        fdlPosition = new FormData();
        fdlPosition.left = new FormAttachment(0, 0);
        fdlPosition.right= new FormAttachment(100, 0);
        fdlPosition.bottom = new FormAttachment(wOK, -margin);
        wlPosition.setLayoutData(fdlPosition);

        // Script line
        wlMAXL = new Label(shell, SWT.NONE);
        wlMAXL.setText(BaseMessages.getString(PKG, "JobMAXL.Script.Label"));
        props.setLook(wlMAXL);
        fdlMAXL = new FormData();
        fdlMAXL.left = new FormAttachment(0, 0);
        fdlMAXL.top = new FormAttachment(wUseSubs, margin);
        wlMAXL.setLayoutData(fdlMAXL);

        wMAXL=new StyledTextComp(jobEntry, shell, SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL, "");
        props.setLook(wMAXL, Props.WIDGET_STYLE_FIXED);
        wMAXL.addModifyListener(lsMod);
        fdMAXL = new FormData();
        fdMAXL.left = new FormAttachment(0, 0);
        fdMAXL.top = new FormAttachment(wlMAXL, margin);
        fdMAXL.right = new FormAttachment(100, -10);
        fdMAXL.bottom = new FormAttachment(wlPosition, -margin);
        wMAXL.setLayoutData(fdMAXL);

        // Add listeners
        lsCancel = new Listener()
        {
            public void handleEvent(Event e)
            {
                cancel();
            }
        };
        lsOK = new Listener()
        {
            public void handleEvent(Event e)
            {
                ok();
            }
        };

        wCancel.addListener(SWT.Selection, lsCancel);
        wOK.addListener(SWT.Selection, lsOK);

        lsDef = new SelectionAdapter()
        {
            public void widgetDefaultSelected(SelectionEvent e)
            {
                ok();
            }
        };

        wName.addSelectionListener(lsDef);

        // Detect X or ALT-F4 or something that kills this window...
        shell.addShellListener(new ShellAdapter()
        {
            public void shellClosed(ShellEvent e)
            {
                cancel();
            }
        });

        wMAXL.addModifyListener(new ModifyListener()
        {
            public void modifyText(ModifyEvent arg0)
            {
                setPosition();
            }

	        }
	    );
		
		wMAXL.addKeyListener(new KeyAdapter(){
			public void keyPressed(KeyEvent e) { setPosition(); }
			public void keyReleased(KeyEvent e) { setPosition(); }
			} 
		);
		wMAXL.addFocusListener(new FocusAdapter(){
			public void focusGained(FocusEvent e) { setPosition(); }
			public void focusLost(FocusEvent e) { setPosition(); }
			}
		);
		wMAXL.addMouseListener(new MouseAdapter(){
			public void mouseDoubleClick(MouseEvent e) { setPosition(); }
			public void mouseDown(MouseEvent e) { setPosition(); }
			public void mouseUp(MouseEvent e) { setPosition(); }
			}
		);
		wMAXL.addModifyListener(lsMod);
		
		
		// Text Higlighting
		//wMAXL.addLineStyleListener(new SQLValuesHighlight());
		

        getData();
        activeMAXLFromFile();

        BaseStepDialog.setSize(shell);

        shell.open();
        props.setDialogSize(shell, "JobMAXLDialogSize");
        while (!shell.isDisposed())
        {
            if (!display.readAndDispatch())
                display.sleep();
        }
        return jobEntry;
    }

	public void setPosition(){
		
		String scr = wMAXL.getText();
		int linenr = wMAXL.getLineAtOffset(wMAXL.getCaretOffset())+1;
		int posnr  = wMAXL.getCaretOffset();
				
		// Go back from position to last CR: how many positions?
		int colnr=0;
		while (posnr>0 && scr.charAt(posnr-1)!='\n' && scr.charAt(posnr-1)!='\r')
		{
			posnr--;
			colnr++;
		}
		wlPosition.setText(BaseMessages.getString(PKG, "JobMAXL.Position.Label",""+linenr,""+colnr));

	}
    public void dispose()
    {
        WindowProperty winprop = new WindowProperty(shell);
        props.setScreen(winprop);
        shell.dispose();
    }

    /**
     * Copy information from the meta-data input to the dialog fields.
     */
    public void getData()
    {
        if (jobEntry.getName() != null)
            wName.setText(jobEntry.getName());
        if (jobEntry.getMAXL() != null)
            wMAXL.setText(jobEntry.getMAXL());
        DatabaseMeta dbinfo = jobEntry.getDatabase();
        if (dbinfo != null && dbinfo.getName() != null)
            wConnection.setText(dbinfo.getName());
        else
            wConnection.setText("");

        wUseSubs.setSelection(jobEntry.getUseVariableSubstitution());
        wMAXLFromFile.setSelection(jobEntry.getMAXLFromFile());
        wSendOneStatement.setSelection(jobEntry.isSendOneStatement());
        
        if (jobEntry.getMAXLFilename() != null)
        	wFilename.setText(jobEntry.getMAXLFilename());
        
        
        
        wName.selectAll();
    }
    private void activeMAXLFromFile()
    {
    	wlFilename.setEnabled(wMAXLFromFile.getSelection());
    	wFilename.setEnabled(wMAXLFromFile.getSelection());
    	wbFilename.setEnabled(wMAXLFromFile.getSelection());
    	wMAXL.setEnabled(!wMAXLFromFile.getSelection());
    	wlMAXL.setEnabled(!wMAXLFromFile.getSelection());
    	wlPosition.setEnabled(!wMAXLFromFile.getSelection());
    	
    	
    }

    private void cancel()
    {
        jobEntry.setChanged(changed);
        jobEntry = null;
        dispose();
    }

    private void ok()
    {
 	   if(Const.isEmpty(wName.getText())) 
      {
			MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR );
			mb.setText(BaseMessages.getString(PKG, "System.StepJobEntryNameMissing.Title"));
			mb.setMessage(BaseMessages.getString(PKG, "System.JobEntryNameMissing.Msg"));
			mb.open(); 
			return;
      }
        jobEntry.setName(wName.getText());
        jobEntry.setMAXL(wMAXL.getText());
        jobEntry.setUseVariableSubstitution(wUseSubs.getSelection());
        jobEntry.setMAXLFromFile(wMAXLFromFile.getSelection());
        jobEntry.setMAXLFilename(wFilename.getText());
        jobEntry.setSendOneStatement(wSendOneStatement.getSelection());
        try {
        	jobEntry.setDatabase(jobMeta.findDatabase(wConnection.getText()));
        }
        catch (KettleException ke) {
        	MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR );
			mb.setText("Error");
			mb.setMessage(ke.getMessage());
			mb.open(); 
			return;
        }
        dispose();
    }
}
