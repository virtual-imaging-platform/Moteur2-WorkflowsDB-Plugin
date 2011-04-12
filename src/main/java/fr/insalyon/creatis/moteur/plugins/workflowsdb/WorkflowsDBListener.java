/* Copyright CNRS-CREATIS
 *
 * Rafael Silva
 * rafael.silva@creatis.insa-lyon.fr
 * http://www.creatis.insa-lyon.fr/~silva
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
import fr.insalyon.creatis.moteur.plugins.workflowsdb.dao.bean.WorkflowBean;
import fr.insalyon.creatis.moteur.plugins.workflowsdb.exceptions.DAOException;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Date;
import java.util.HashMap;

/**
 *
 * @author Rafael Silva
 */
public class WorkflowsDBListener implements WorkflowListener {

    private enum Status {

        Completed, Running, Killed
    };
    private static Log logger = new Log();
    private String workflowPath;
    private WorkflowsDAO workflowDAO;
    private WorkflowBean workflowBean;

    public WorkflowsDBListener() {
        String path = new File("").getAbsolutePath();
        this.workflowPath = path.substring(path.lastIndexOf("/") + 1, path.length());
        workflowDAO = DAOFactory.getDAOFactory().getWorkflowDAO();
    }

    @Override
    public void executionStarted(Workflow workflow, int id, int key) {
        try {
            BufferedReader br = new BufferedReader(new InputStreamReader(
                    new DataInputStream(new FileInputStream("user.txt"))));
            String line = br.readLine().split("/")[5];
            String user = line.substring(line.lastIndexOf("=") + 1);
            workflowBean = new WorkflowBean(workflowPath, workflow.getName(), user, new Date(), Status.Running.toString(), "Execution Started", id, key);
            workflowDAO.add(workflowBean);

        } catch (IOException ex) {
            logger.warning("[WorkflowListener] " + ex.getMessage());
        } catch (DAOException ex) {
            logger.warning("[WorkflowListener] " + ex.getMessage());
        }
    }

    @Override
    public void executionCompleted(Workflow workflow, boolean completed) {
        try {
            if (completed) {
                workflowBean.setMajorStatus(Status.Completed.toString());
            } else {
                workflowBean.setMajorStatus(Status.Killed.toString());
            }
            workflowBean.setFinishTime(new Date());
            workflowDAO.update(workflowBean);

        } catch (DAOException ex) {
            logger.warning("[WorkflowListener] " + ex.getMessage());
        }
    }

    @Override
    public void processorRun(Workflow workflow, Processor processor) {
        try {
            workflowBean.setMinorStatus("Executing Processor \"" + processor.getName() + "\"");
            workflowDAO.update(workflowBean);

        } catch (DAOException ex) {
            logger.warning("[WorkflowListener] " + ex.getMessage());
        }
    }

    @Override
    public void processorRan(Workflow workflow, Processor processor, int nruns, boolean completed, DataLine line, HashMap<OutputPort, Data> produced) {
        try {
            if (produced != null) {
                for (Data d : produced.values()) {
                    if (d.toString().startsWith("lfn://")) {
                        String path = new URI(d.toString()).getPath();
                        logger.print("[WorkflowListener] Adding output '" + path + "'");
                        workflowDAO.addOutput(workflowBean.getId(), path);
                    }
                }
            }
        } catch (URISyntaxException ex) {
            logger.warning("[WorkflowListener] " + ex.getMessage());
        } catch (DAOException ex) {
            if (!ex.getMessage().contains("The statement was aborted because it would have caused a duplicate key value")) {
                logger.warning("[WorkflowListener] " + ex.getMessage());
            }
        }
    }

    @Override
    public void processorFailed(Workflow workflow, Processor processor, int nfailures, boolean completed, String error) {
    }

    @Override
    public void processorReceived(Workflow workflow, Processor processor, String port, DataItem item) {
    }
}
