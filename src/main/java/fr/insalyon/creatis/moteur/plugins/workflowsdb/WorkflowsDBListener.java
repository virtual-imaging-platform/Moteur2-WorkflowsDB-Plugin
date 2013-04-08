/* Copyright CNRS-CREATIS
 *
 * Rafael Ferreira da Silva
 * rafael.silva@creatis.insa-lyon.fr
 * http://www.rafaelsilva.com
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
import fr.cnrs.i3s.moteur2.execution.WorkflowListener;
import fr.cnrs.i3s.moteur2.log.Log;
import fr.cnrs.i3s.moteur2.processor.OutputPort;
import fr.insalyon.creatis.moteur.plugins.workflowsdb.bean.DataType;
import fr.insalyon.creatis.moteur.plugins.workflowsdb.bean.Input;
import fr.insalyon.creatis.moteur.plugins.workflowsdb.bean.InputID;
import fr.insalyon.creatis.moteur.plugins.workflowsdb.bean.Output;
import fr.insalyon.creatis.moteur.plugins.workflowsdb.bean.OutputID;
import fr.insalyon.creatis.moteur.plugins.workflowsdb.bean.Processor;
import fr.insalyon.creatis.moteur.plugins.workflowsdb.bean.ProcessorID;
import fr.insalyon.creatis.moteur.plugins.workflowsdb.bean.Workflow;
import fr.insalyon.creatis.moteur.plugins.workflowsdb.bean.WorkflowStatus;
import fr.insalyon.creatis.moteur.plugins.workflowsdb.dao.InputDAO;
import fr.insalyon.creatis.moteur.plugins.workflowsdb.dao.OutputDAO;
import fr.insalyon.creatis.moteur.plugins.workflowsdb.dao.ProcessorDAO;
import fr.insalyon.creatis.moteur.plugins.workflowsdb.dao.WorkflowDAO;
import fr.insalyon.creatis.moteur.plugins.workflowsdb.dao.WorkflowsDBDAOException;
import fr.insalyon.creatis.moteur.plugins.workflowsdb.dao.WorkflowsDBDAOFactory;
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
 * @author Rafael Ferreira da Silva
 */
public class WorkflowsDBListener implements WorkflowListener {

    public static final String TAG = "[WorkflowsDB Plugin] ";
    private static Log logger = new Log();
    private WorkflowDAO workflowDAO;
    private ProcessorDAO processorDAO;
    private OutputDAO outputDAO;
    private InputDAO inputDAO;
    private String workflowID;

    public WorkflowsDBListener(fr.cnrs.i3s.moteur2.execution.Workflow workflow) {

        try {

            workflowDAO = WorkflowsDBDAOFactory.getInstance().getWorkflowDAO();
            processorDAO = WorkflowsDBDAOFactory.getInstance().getProcessorDAO();
            inputDAO = WorkflowsDBDAOFactory.getInstance().getInputDAO();
            outputDAO = WorkflowsDBDAOFactory.getInstance().getOutputDAO();

            String path = new File("").getAbsolutePath();
            workflowID = path.substring(path.lastIndexOf("/") + 1, path.length());

            Workflow workflowBean = workflowDAO.get(workflowID);

            if (workflowBean == null) {

                String user = "Default User";
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
                workflowBean = new Workflow(workflowID, user, WorkflowStatus.Queued,
                        new Date(), null, null, null, null, null);
                workflowDAO.add(workflowBean);
            }
        } catch (java.io.IOException ex) {
            logger.warning(TAG + ex.getMessage());
        } catch (WorkflowsDBDAOException ex) {
            logger.warning(TAG + ex.getMessage());
        }
    }

    @Override
    public void executionStarted(fr.cnrs.i3s.moteur2.execution.Workflow workflow, int id, int key) {

        try {
            Workflow workflowBean = workflowDAO.get(workflowID);
            workflowBean.setStatus(WorkflowStatus.Running);
            workflowDAO.update(workflowBean);

        } catch (WorkflowsDBDAOException ex) {
            logger.warning(TAG + ex.getMessage());
        }
    }

    @Override
    public void executionCompleted(fr.cnrs.i3s.moteur2.execution.Workflow workflow, boolean completed) {

        try {
            Workflow workflowBean = workflowDAO.get(workflowID);

            if (completed) {
                workflowBean.setStatus(WorkflowStatus.Completed);
            } else {
                workflowBean.setStatus(WorkflowStatus.Killed);
            }
            workflowBean.setFinishedTime(new Date());
            workflowDAO.update(workflowBean);

        } catch (WorkflowsDBDAOException ex) {
            logger.warning(TAG + ex.getMessage());
        }
    }

    @Override
    public void processorRun(fr.cnrs.i3s.moteur2.execution.Workflow workflow,
            fr.cnrs.i3s.moteur2.processor.Processor processor) {

        try {
            updateProcessor(processor, workflowDAO.get(workflowID));

        } catch (WorkflowsDBDAOException ex) {
            logger.warning(TAG + ex.getMessage());
        }
    }

    @Override
    public void processorRan(fr.cnrs.i3s.moteur2.execution.Workflow workflow,
            fr.cnrs.i3s.moteur2.processor.Processor processor, int nruns,
            boolean completed, DataLine line, HashMap<OutputPort, Data> produced) {

        try {
            updateProcessor(processor, workflowDAO.get(workflowID));

        } catch (WorkflowsDBDAOException ex) {
            logger.warning(TAG + ex.getMessage());
        }
    }

    @Override
    public void processorFailed(fr.cnrs.i3s.moteur2.execution.Workflow workflow,
            fr.cnrs.i3s.moteur2.processor.Processor processor, int nfailures,
            boolean completed, String error) {
    }

    @Override
    public void processorReceived(fr.cnrs.i3s.moteur2.execution.Workflow workflow,
            fr.cnrs.i3s.moteur2.processor.Processor processor, String port, DataItem item) {

        try {
            Workflow workflowBean = workflowDAO.get(workflowID);
            String path = item.dataString();

            if (processor.isOutput() && hasValidData(path)) {

                DataType type = DataType.String;
                if (path.startsWith("lfn://")) {
                    path = new URI(item.dataString().toString()).getPath();
                    type = DataType.URI;
                }
                outputDAO.add(new Output(new OutputID(workflowBean.getId(), path, processor.getName()), type, port));
                logger.print(TAG + "Added output '" + path + "'");

            } else if (processor.isInput() && !processor.isConstant() && hasValidData(path)) {

                DataType type = DataType.String;
                if (path.startsWith("lfn://")) {
                    path = new URI(item.dataString().toString()).getPath();
                    type = DataType.URI;
                }
                inputDAO.add(new Input(new InputID(workflowBean.getId(), path, processor.getName()), type));
                logger.print(TAG + "Added input '" + path + "'");
            }
            updateProcessor(processor, workflowBean);

        } catch (java.net.URISyntaxException ex) {
            logger.warning(TAG + ex.getMessage());
        } catch (WorkflowsDBDAOException ex) {
            if (!ex.getMessage().contains("duplicate key value")) {
                logger.warning(TAG + ex.getMessage());
            }
        }
    }

    private boolean hasValidData(String path) {
        return path != null && !path.equals("<eoa>")
                && !path.equals("void") && !path.equals("null");
    }

    private void updateProcessor(fr.cnrs.i3s.moteur2.processor.Processor processor, Workflow workflowBean) {

        if (!processor.isInput() && !processor.isOutput() && !processor.isConstant() && !processor.isBoring()) {
            try {

                Processor p = processorDAO.get(workflowBean.getId(), processor.getName());

                if (p != null) {
                    p.setCompleted(processor.getNRuns());
                    p.setQueued(processor.getNpending());
                    p.setFailed(processor.getNFailures());
                    processorDAO.update(p);

                } else {
                    processorDAO.add(new Processor(
                            new ProcessorID(workflowBean.getId(), processor.getName()),
                            processor.getNRuns(), processor.getNpending(), processor.getNFailures()));
                }
            } catch (WorkflowsDBDAOException ex) {
                logger.warning(TAG + ex.getMessage());

            }
        }
    }
}
