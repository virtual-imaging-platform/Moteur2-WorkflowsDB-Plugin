/* Copyright CNRS-CREATIS
 *
 * Rafael Silva
 * rafael.silva@creatis.insa-lyon.fr
 * http://www.rafaelsilva.com
 *
 * This software is a grid-enabled data-driven workflow manager and editor.
 *
 * This software is governed by the CeCILL  license under French law and
 * abiding by the rules of distribution of free software.  You can  use,
 * modify and/ or redistribute the software under the terms of the CeCILL
 * license as circulated by CEA, CNRS and INRIA at the following URL
 * "http://www.cecill.info".
 *
 * As a counterpart to the access to the source code and  rights to copy,
 * modify and redistribute granted by the license, users are provided only
 * with a limited warranty  and the software's author,  the holder of the
 * economic rights,  and the successive licensors  have only  limited
 * liability.
 *
 * In this respect, the user's attention is drawn to the risks associated
 * with loading,  using,  modifying and/or developing or reproducing the
 * software by the user in light of its specific status of free software,
 * that may mean  that it is complicated to manipulate,  and  that  also
 * therefore means  that it is reserved for developers  and  experienced
 * professionals having in-depth computer knowledge. Users are therefore
 * encouraged to load and test the software's suitability as regards their
 * requirements in conditions enabling the security of their systems and/or
 * data to be ensured and,  more generally, to use and operate it in the
 * same conditions as regards security.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL license and that you accept its terms.
 */
package fr.insalyon.creatis.moteur.plugins.workflowsdb;

import fr.cnrs.i3s.moteur2.data.Data;
import fr.cnrs.i3s.moteur2.data.DataItem;
import fr.cnrs.i3s.moteur2.data.DataLine;
import fr.cnrs.i3s.moteur2.execution.Workflow;
import fr.cnrs.i3s.moteur2.execution.WorkflowListener;
import fr.cnrs.i3s.moteur2.log.Log;
import fr.cnrs.i3s.moteur2.processor.OutputPort;
import fr.cnrs.i3s.moteur2.processor.Processor;
import fr.insalyon.creatis.moteur.plugins.workflowsdb.dao.DAOFactory;
import fr.insalyon.creatis.moteur.plugins.workflowsdb.dao.WorkflowsDAO;
import fr.insalyon.creatis.moteur.plugins.workflowsdb.dao.bean.ProcessorBean;
import fr.insalyon.creatis.moteur.plugins.workflowsdb.dao.bean.WorkflowBean;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Date;
import java.util.HashMap;

/**
 *
 * @author Rafael Silva
 */
public class WorkflowsDBListener implements WorkflowListener {

    public static final String TAG = "[WorkflowsDB Plugin] ";
    private static Log logger = new Log();
    private WorkflowsDAO workflowDAO;
    private WorkflowBean workflowBean;

    public WorkflowsDBListener(Workflow workflow) {

        try {

            workflowDAO = DAOFactory.getDAOFactory().getWorkflowDAO();
            String path = new File("").getAbsolutePath();
            String workflowID = path.substring(path.lastIndexOf("/") + 1, path.length());
            String user = "Default User";
            workflowBean = new WorkflowBean(workflowID, workflow.getName(), user,
                    new Date(), Status.Queued.name());
            if (!workflowDAO.exists(workflowID)) {

                File userFile = new File("user.txt");
                if (userFile.exists()) {

                    BufferedReader br = new BufferedReader(
                            new InputStreamReader(
                            new DataInputStream(
                            new FileInputStream(userFile))));
                    String line = br.readLine().split("/")[5];
                    user = line.substring(line.lastIndexOf("=") + 1);
                    br.close();
                }

                workflowDAO.add(workflowBean);
            } else {
                logger.warning(TAG + "workflow " + workflowID + " exists IGNORING it!");
            }

        } catch (java.io.IOException ex) {
            logger.warning(TAG + ex.getMessage());
        } catch (fr.insalyon.creatis.moteur.plugins.workflowsdb.exceptions.DAOException ex) {
            logger.warning(TAG + ex.getMessage());
        }
    }

    @Override
    public void executionStarted(Workflow workflow, int id, int key) {

        try {

            logger.print(TAG + "now workflow execution status is running!");
            workflowDAO.updateStatus(workflowBean.getId(), Status.Running);
        } catch (fr.insalyon.creatis.moteur.plugins.workflowsdb.exceptions.DAOException ex) {
            logger.warning(TAG + ex.getMessage());
        }
    }

    @Override
    public void executionCompleted(Workflow workflow, boolean completed) {

        try {

            if (completed) {
                workflowBean.setMajorStatus(Status.Completed.name());
            } else {
                workflowBean.setMajorStatus(Status.Killed.name());
            }

            workflowBean.setFinishTime(new Date());
            workflowDAO.update(workflowBean);
        } catch (fr.insalyon.creatis.moteur.plugins.workflowsdb.exceptions.DAOException ex) {
            logger.warning(TAG + ex.getMessage());
        }
        //workflowDAO.close();
    }

    @Override
    public void processorRun(Workflow workflow, Processor processor) {

        updateProcessor(processor);
    }

    @Override
    public void processorRan(Workflow workflow, Processor processor, int nruns, boolean completed, DataLine line, HashMap<OutputPort, Data> produced) {

        updateProcessor(processor);
    }

    @Override
    public void processorFailed(Workflow workflow, Processor processor, int nfailures, boolean completed, String error) {
    }

    @Override
    public void processorReceived(Workflow workflow, Processor processor, String port, DataItem item) {

        try {

            String path = item.dataString();
            if (processor.isOutput() && hasValidData(path)) {

                String type = "String";
                if (path.startsWith("lfn://")) {

                    path = new URI(item.dataString().toString()).getPath();
                    type = "URI";
                }

                workflowDAO.addOutput(workflowBean.getId(), path, processor.getName(), type);
                logger.print(TAG + "Added output '" + path + "'");
            } else if (processor.isInput() && !processor.isConstant() && hasValidData(path)) {

                String type = "String";
                if (path.startsWith("lfn://")) {

                    path = new URI(item.dataString().toString()).getPath();
                    type = "URI";
                }

                workflowDAO.addInput(workflowBean.getId(), path, processor.getName(), type);
                logger.print(TAG + "Added input '" + path + "'");
            }

            updateProcessor(processor);
        } catch (java.net.URISyntaxException ex) {
            logger.warning(TAG + ex.getMessage());
        } catch (fr.insalyon.creatis.moteur.plugins.workflowsdb.exceptions.DAOException ex) {
            if (!ex.getMessage().contains("duplicate key value")) {
                logger.warning(TAG + ex.getMessage());
            }
        }
    }

    private boolean hasValidData(String path) {
        return path != null && !path.equals("<eoa>")
                && !path.equals("void") && !path.equals("null");
    }

    private void updateProcessor(Processor processor) {

        if (!processor.isInput() && !processor.isOutput() && !processor.isConstant() && !processor.isBoring()) {
            try {

                ProcessorBean pb = workflowDAO.getProcessor(workflowBean.getId(), processor.getName());
                pb.update(processor.getNRuns(), processor.getNpending(), processor.getNFailures());
                workflowDAO.updateProcessor(pb);
            } catch (fr.insalyon.creatis.moteur.plugins.workflowsdb.exceptions.DAOException ex) {
                if (ex.getMessage().toLowerCase().contains("no data")) {
                    try {
                        workflowDAO.addProcessor(
                                new ProcessorBean(workflowBean.getId(), processor.getName(),
                                processor.getNRuns(), processor.getNpending(), processor.getNFailures()));
                    } catch (fr.insalyon.creatis.moteur.plugins.workflowsdb.exceptions.DAOException ex1) {
                        logger.warning(TAG + ex1.getMessage());
                    }
                } else {
                    logger.warning(TAG + ex.getMessage());
                }
            }
        }
    }
}
