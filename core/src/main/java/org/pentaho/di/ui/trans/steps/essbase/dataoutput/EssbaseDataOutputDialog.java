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
package org.pentaho.di.ui.trans.steps.essbase.dataoutput;

/*
 *   This file is part of EssbaseKettlePlugin.
 */

import java.util.ArrayList;
import java.util.List;

import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.essbase.core.DimensionField;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.trans.steps.essbase.dataoutput.EssbaseDataOutputData;
import org.pentaho.di.trans.steps.essbase.dataoutput.EssbaseDataOutputMeta;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

public class EssbaseDataOutputDialog extends BaseStepDialog implements StepDialogInterface {
  private static Class<?> PKG = EssbaseDataOutputMeta.class; // for i18n purposes,
                                                          // needed by
                                                          // Translator2!!
                                                          // $NON-NLS-1$

  public static void main(String[] args) {
    try {
    	EssbaseDataOutputDialog window = new EssbaseDataOutputDialog(null, new EssbaseDataOutputMeta(), null, "noname");
      window.open();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private EssbaseDataOutputMeta meta;

  private TableView          tableViewFields;
  private Text               textStepName;
  private Text               textCommit;
  private Combo              comboApplication;
  private Combo              comboCube;
  private Combo              comboMeasureType;
  private Label              labelStepName;
  private Label              labelApplication;
  private Label              labelCube;
  private Label              labelCommit;
  private Label              labelMeasureType;
  private Button             buttonClearFields;
  private Button             buttonGetFields;
  private Button             buttonOk;
  private Button             buttonCancel;
  private Button             buttonClearCube;
  private Label              labelClearCube;
  private CCombo             addConnectionLine;
  private ColumnInfo[]       colinf;

  public EssbaseDataOutputDialog(Shell parent, Object in, TransMeta transMeta, String sname) {
    super(parent, (BaseStepMeta) in, transMeta, sname);
    this.meta = (EssbaseDataOutputMeta) in;
  }

  public String open() {

    final Display display = getParent().getDisplay();
    shell = new Shell(getParent(), SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);
    props.setLook(shell);
    setShellImage(shell, meta);
    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout(formLayout);
    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    FormData fd;

    labelStepName = new Label(shell, SWT.RIGHT);
    fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.right = new FormAttachment(middle, -margin);
    fd.top = new FormAttachment(0, margin);
    labelStepName.setLayoutData(fd);

    textStepName = new Text(shell, SWT.BORDER);
    fd = new FormData();
    fd.left = new FormAttachment(middle, 0);
    fd.right = new FormAttachment(100, 0);
    fd.top = new FormAttachment(0, margin);
    textStepName.setLayoutData(fd);

    addConnectionLine = addConnectionLine(shell, textStepName, Const.MIDDLE_PCT, margin);

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
    
    labelCommit = new Label(shell, SWT.RIGHT);
    fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.right = new FormAttachment(middle, -margin);
    fd.top = new FormAttachment(comboCube, margin);
    labelCommit.setLayoutData(fd);
    
    textCommit = new Text(shell, SWT.BORDER);
    fd = new FormData();
    fd.left = new FormAttachment(middle, 0);
    fd.right = new FormAttachment(100, 0);
    fd.top = new FormAttachment(comboCube, margin);
    textCommit.setLayoutData(fd);
    
    labelClearCube = new Label(shell, SWT.RIGHT);
    fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.right = new FormAttachment(middle, -margin);
    fd.top = new FormAttachment(textCommit, margin);
    labelClearCube.setLayoutData(fd);

    buttonClearCube = new Button(shell, SWT.CHECK);
    fd = new FormData();
    fd.left = new FormAttachment(middle, 0);
    fd.right = new FormAttachment(100, 0);
    fd.top = new FormAttachment(textCommit, margin);
    buttonClearCube.setLayoutData(fd);

    labelMeasureType = new Label(shell, SWT.RIGHT);
    fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.right = new FormAttachment(middle, -margin);
    fd.top = new FormAttachment(buttonClearCube, margin);
    labelMeasureType.setLayoutData(fd);

    comboMeasureType = new Combo(shell, SWT.READ_ONLY | SWT.FILL);
    fd = new FormData();
    fd.left = new FormAttachment(middle, 0);
    fd.right = new FormAttachment(100, 0);
    fd.top = new FormAttachment(buttonClearCube, margin);
    comboMeasureType.setLayoutData(fd);

    ModifyListener lsMod = new ModifyListener() {
      public void modifyText(ModifyEvent e) {
        meta.setChanged();
      }
    };

    colinf = new ColumnInfo[] { new ColumnInfo(getLocalizedColumn(0), ColumnInfo.COLUMN_TYPE_TEXT, false, true)
                              , new ColumnInfo(getLocalizedColumn(1), ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] {}, true),
    // new ColumnInfo(getLocalizedColumn(2), ColumnInfo.COLUMN_TYPE_CCOMBO, new
    // String[] {"String","Number"}, true)
    };

    tableViewFields = new TableView(null, shell, SWT.NONE | SWT.BORDER, colinf, 10, lsMod, props);

    tableViewFields.setSize(477, 105);
    tableViewFields.setBounds(5, 250, 477, 105);
    tableViewFields.setSortable(false);
    tableViewFields.table.removeAll();
    fd = new FormData();
    fd.left = new FormAttachment(0, margin);
    fd.top = new FormAttachment(comboMeasureType, 3 * margin);
    fd.right = new FormAttachment(100, -150);
    fd.bottom = new FormAttachment(100, -50);
    tableViewFields.setLayoutData(fd);

    buttonGetFields = new Button(shell, SWT.NONE);
    fd = new FormData();
    fd.left = new FormAttachment(tableViewFields, margin);
    fd.top = new FormAttachment(comboMeasureType, 3 * margin);
    fd.right = new FormAttachment(100, 0);
    buttonGetFields.setLayoutData(fd);

    buttonClearFields = new Button(shell, SWT.NONE);
    fd = new FormData();
    fd.left = new FormAttachment(tableViewFields, margin);
    fd.top = new FormAttachment(buttonGetFields, margin);
    fd.right = new FormAttachment(100, 0);
    buttonClearFields.setLayoutData(fd);

    buttonOk = new Button(shell, SWT.CENTER);
    buttonCancel = new Button(shell, SWT.CENTER);
    buttonOk.setText(BaseMessages.getString("System.Button.OK"));
    buttonCancel.setText(BaseMessages.getString("System.Button.Cancel"));
    setButtonPositions(new Button[] { buttonOk, buttonCancel }, margin, null);

    buttonGetFields.addSelectionListener(new SelectionAdapter() {
      public void widgetSelected(SelectionEvent e) {
        doGetFields();
      }
    });
    buttonClearFields.addSelectionListener(new SelectionAdapter() {
      public void widgetSelected(SelectionEvent e) {
        doClearFields();

      }
    });
    buttonCancel.addSelectionListener(new SelectionAdapter() {
      public void widgetSelected(SelectionEvent e) {
        cancel();
      }
    });
    buttonOk.addSelectionListener(new SelectionAdapter() {
      public void widgetSelected(SelectionEvent e) {
        ok();
      }
    });
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
    comboCube.addSelectionListener(new SelectionAdapter() {
      public void widgetSelected(SelectionEvent e) {
        doSelectCube();
      }
    });

    this.fillLocalizedData();
    this.fillStoredData();
    this.doSelectConnection(false);

    props.setLook(tableViewFields);
    props.setLook(textStepName);
    props.setLook(comboApplication);
    props.setLook(comboCube);
    props.setLook(textCommit);
    props.setLook(comboMeasureType);
    props.setLook(labelStepName);
    props.setLook(labelApplication);
    props.setLook(labelCube);
    props.setLook(labelCommit);
    props.setLook(labelMeasureType);
    props.setLook(buttonClearFields);
    props.setLook(buttonGetFields);
    props.setLook(buttonOk);
    props.setLook(buttonCancel);
    props.setLook(addConnectionLine);
    props.setLook(buttonClearCube);
    props.setLook(labelClearCube);

    shell.addShellListener(new ShellAdapter() {
      public void shellClosed(ShellEvent e) {
        cancel();
      }
    });
    meta.setChanged(changed);
    setSize();
    shell.open();

    while (!shell.isDisposed()) {
      if (!display.readAndDispatch())
        display.sleep();
    }
    return stepname;
  }

  private String getLocalizedColumn(int columnIndex) {
    switch (columnIndex) {
    case 0:
      return BaseMessages.getString(PKG, "EssbaseDataOutputDialog.ColumnDimension");
    case 1:
      return BaseMessages.getString(PKG, "EssbaseDataOutputDialog.ColumnField");
    case 2:
      return BaseMessages.getString(PKG, "EssbaseDataOutputDialog.ColumnType");
    default:
      return "";
    }
  }

  private void fillLocalizedData() {
    labelStepName.setText(BaseMessages.getString(PKG, "EssbaseDataOutputDialog.StepName"));
    shell.setText(BaseMessages.getString(PKG, "EssbaseDataOutputDialog.EssbaseDataOutput"));
    buttonGetFields.setText(BaseMessages.getString(PKG, "EssbaseDataOutputDialog.GetFields"));
    buttonClearFields.setText(BaseMessages.getString(PKG, "EssbaseDataOutputDialog.ClearFields"));
    labelApplication.setText(BaseMessages.getString(PKG, "EssbaseDataOutputDialog.SelectApplication"));
    labelCube.setText(BaseMessages.getString(PKG, "EssbaseDataOutputDialog.SelectCube"));
    labelCommit.setText(BaseMessages.getString(PKG, "EssbaseDataOutputDialog.CommitSize.Label"));
    labelClearCube.setText(BaseMessages.getString(PKG, "EssbaseDataOutputDialog.ClearCube"));
    labelMeasureType.setText(BaseMessages.getString(PKG, "EssbaseDataOutputDialog.SelectMeasureType"));
  }

  private void fillStoredData() {
    if (stepname != null)
      textStepName.setText(stepname);

    
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
    
    textCommit.setText(meta.getCommitSize());

    if (meta.getMeasureType() != null) {
      comboMeasureType.add(meta.getMeasureType());
      comboMeasureType.select(0);
    }

    comboMeasureType.setItems(new String[] { "Numeric", "String" });
    comboMeasureType.select(0);
    if (meta.getMeasureType() != null) {
      int indexType = comboMeasureType.indexOf(meta.getMeasureType());
      if (indexType >= 0)
        comboMeasureType.select(indexType);
    }

    tableViewFields.table.removeAll();

    if (meta.getFields().size() > 0) {
      for (DimensionField level : meta.getFields()) {
        tableViewFields.add(level.getDimensionName(), level.getFieldName());// ,level.getFieldType());
      }
    }

    String[] fieldNames = null;
    try {
      RowMetaInterface r = transMeta.getPrevStepFields(stepname);
      fieldNames = r.getFieldNames();
    } catch (Exception e) {
    }
    tableViewFields.setColumnInfo(1, new ColumnInfo("Field", ColumnInfo.COLUMN_TYPE_CCOMBO, fieldNames, true));

    if (meta.getMeasure() != null && meta.getMeasure().getDimensionName()!=null && meta.getMeasure().getFieldName()!=null) {
      final TableItem item = new TableItem(tableViewFields.table, SWT.NONE);
      item.setText(1, meta.getMeasure().getDimensionName());
      item.setText(2, meta.getMeasure().getFieldName());
      // item.setText(3,meta.getMeasure().getFieldType());
      item.setForeground(Display.getCurrent().getSystemColor(SWT.COLOR_DARK_GREEN));
    }
    tableViewFields.setRowNums();
    tableViewFields.optWidth(true);

    buttonClearCube.setSelection(meta.getClearCube());

  }

  private void doSelectConnection(boolean clearCurrentData) {
    try {
      if (clearCurrentData) {
        tableViewFields.table.removeAll();
        comboApplication.removeAll();
        comboCube.removeAll();
      }

      if (addConnectionLine.getText() != null) {
        DatabaseMeta dbMeta = transMeta.findDatabase(addConnectionLine.getText());
        if (dbMeta != null) {
          EssbaseDataOutputData data = new EssbaseDataOutputData(dbMeta);
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
      new ErrorDialog(shell, BaseMessages.getString(PKG, "EssbaseDataOutputDialog.RetreiveAppsErrorTitle"), BaseMessages.getString(PKG, "EssbaseDataOutputDialog.RetreiveAppsError"), ex);
    }
  }

  private void fillPreviousFieldTableViewColumn() throws KettleException {
    RowMetaInterface r = transMeta.getPrevStepFields(stepname);
    if (r != null) {
      String[] fieldNames = r.getFieldNames();
      colinf[1] = new ColumnInfo(getLocalizedColumn(1), ColumnInfo.COLUMN_TYPE_CCOMBO, fieldNames, true);
    }
  }

  private void doGetFields() {
	  try {
		  List<String> cubeDimensions = null;
		  if (comboCube.getText() != null && comboCube.getText() != "") {
			  if (addConnectionLine.getText() != null) {
				  DatabaseMeta dbMeta = transMeta.findDatabase(addConnectionLine.getText());
				  if (comboApplication.getText() != null && comboCube.getText() != null) {
					  String appName = comboApplication.getText();
					  if (dbMeta != null) {
						  EssbaseDataOutputData data = new EssbaseDataOutputData(dbMeta);
						  data.helper.connect();
						  cubeDimensions = data.helper.getCubeDimensions(appName, comboCube.getText());
						  data.helper.disconnect();
					  }
				  } else {
				        new ErrorDialog(shell, BaseMessages.getString(PKG, "System.Dialog.GetFieldsFailed.Title"), BaseMessages.getString(PKG, "System.Dialog.GetFieldsFailed.Message"), new Exception(BaseMessages.getString(PKG,
				            "EssbaseDataOutputDialog.SelectCubeFirstError")));
				  }
			  }
		  }
		  tableViewFields.table.removeAll();

		  for (int i = 0; i < cubeDimensions.size(); i++) {
			  final TableItem item = new TableItem(tableViewFields.table, SWT.NONE);
			  item.setText(1, cubeDimensions.get(i));
			  // item.setText(3, "String");
		  }
		  final TableItem item = new TableItem(tableViewFields.table, SWT.NONE);
		  item.setText(1, "Cube Measure");
		  item.setForeground(Display.getCurrent().getSystemColor(SWT.COLOR_DARK_GREEN));

		  tableViewFields.removeEmptyRows();
		  tableViewFields.setRowNums();
		  tableViewFields.optWidth(true);

		  this.fillPreviousFieldTableViewColumn();

	  } catch (Exception e) {
		  new ErrorDialog(shell, BaseMessages.getString(PKG, "System.Dialog.GetFieldsFailed.Title"), BaseMessages.getString(PKG, "System.Dialog.GetFieldsFailed.Message"), e);
	  }
  }

  private void doClearFields() {
    tableViewFields.table.removeAll();
  }
  
  private void doSelectApplication() {
	  try {
	      tableViewFields.table.removeAll();
	      comboCube.removeAll();
	      if (addConnectionLine.getText() != null) {
	          DatabaseMeta dbMeta = transMeta.findDatabase(addConnectionLine.getText());
	          if (comboApplication.getText() != null) {
	        	  String appName = comboApplication.getText();
	        	  if (dbMeta != null) {
	        		  EssbaseDataOutputData data = new EssbaseDataOutputData(dbMeta);
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
	      new ErrorDialog(shell, BaseMessages.getString(PKG, "EssbaseDataOutputDialog.RetreiveCubesErrorTitle"), BaseMessages.getString(PKG, "EssbaseDataOutputDialog.RetreiveCubesError"), ex);
	  }	
  }

  private void doSelectCube() {
	  tableViewFields.table.removeAll();
  }

  private void cancel() {
    stepname = null;
    meta.setChanged(changed);
    dispose();
  }

  private void ok() {
    try {
      getInfo(this.meta);
      dispose();
    } catch (KettleException e) {
      new ErrorDialog(shell, BaseMessages.getString(PKG, "EssbaseDataOutputDialog.FailedToSaveDataErrorTitle"), BaseMessages.getString(PKG, "EssbaseDataOutputDialog.FailedToSaveDataError"), e);
    }
  }

  private void getInfo(EssbaseDataOutputMeta myMeta) throws KettleException {
    stepname = textStepName.getText();
    List<DimensionField> fields = new ArrayList<DimensionField>();

    for (int i = 0; i < tableViewFields.table.getItemCount(); i++) {

      DimensionField field = new DimensionField(tableViewFields.table.getItem(i).getText(1), tableViewFields.table.getItem(i).getText(2), ""// tableViewFields.table.getItem(i).getText(3)
      );

      if (i != tableViewFields.table.getItemCount() - 1) {
        // if(tableViewFields.table.getItem(i).getText(3)!="String")
        // throw new
        // KettleException("Dimension input field must be from String type");
        fields.add(field);
      } else
        myMeta.setMeasureField(field);
    }
    myMeta.setApplication(this.comboApplication.getText());
    myMeta.setCube(this.comboCube.getText());
    myMeta.setCommitSize(this.textCommit.getText());
    myMeta.setMeasureType(this.comboMeasureType.getText());
    myMeta.setLevels(fields);
    myMeta.setClearCube(this.buttonClearCube.getSelection());
    myMeta.setDatabaseMeta(transMeta.findDatabase(addConnectionLine.getText()));
    myMeta.setChanged(true);
  }
}
