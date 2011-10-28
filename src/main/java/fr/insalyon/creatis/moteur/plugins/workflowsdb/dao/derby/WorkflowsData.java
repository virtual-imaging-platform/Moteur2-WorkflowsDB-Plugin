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
package fr.insalyon.creatis.moteur.plugins.workflowsdb.dao.derby;

import fr.cnrs.i3s.moteur2.log.Log;
import fr.insalyon.creatis.moteur.plugins.workflowsdb.Configuration;
import fr.insalyon.creatis.moteur.plugins.workflowsdb.WorkflowsDBListener;
import fr.insalyon.creatis.moteur.plugins.workflowsdb.dao.AbstractData;
import fr.insalyon.creatis.moteur.plugins.workflowsdb.dao.WorkflowsDAO;
import fr.insalyon.creatis.moteur.plugins.workflowsdb.dao.bean.ProcessorBean;
import fr.insalyon.creatis.moteur.plugins.workflowsdb.dao.bean.WorkflowBean;
import fr.insalyon.creatis.moteur.plugins.workflowsdb.exceptions.DAOException;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;

/**
 *
 * @author Rafael Silva
 */
public class WorkflowsData extends AbstractData implements WorkflowsDAO {

    private static Log logger = new Log();
    private static WorkflowsData instance;
    private final String DRIVER = "org.apache.derby.jdbc.ClientDriver";
    private String DBURL = "jdbc:derby://" + Configuration.HOST
            + ":" + Configuration.PORT + "/";

    /**
     * Gets an unique instance of the class WorkflowData
     * @return Unique instance of WorkflowData
     */
    public synchronized static WorkflowsData getInstance() {
        if (instance == null) {
            instance = new WorkflowsData();
        }
        return instance;
    }

    private WorkflowsData() {
        try {
            connect();
            createTables();

        } catch (SQLException ex) {
            logger.warning(WorkflowsDBListener.TAG + ex.getMessage());
        }
    }

    @Override
    protected synchronized void connect() throws SQLException {
        try {
            Class.forName(DRIVER);
            connection = DriverManager.getConnection(DBURL + Configuration.DB_PATH
                    + ";create=true");
            connection.setAutoCommit(true);

        } catch (SQLException ex) {
            connection = DriverManager.getConnection(DBURL + Configuration.DB_PATH);
            connection.setAutoCommit(true);

        } catch (ClassNotFoundException ex) {
            logger.warning(WorkflowsDBListener.TAG + ex.getMessage());
        }
    }

    private void createTables() {
        try {
            Statement stat = connection.createStatement();
            stat.executeUpdate("CREATE TABLE Workflows ("
                    + "id VARCHAR(255), "
                    + "simulation_name VARCHAR(255), "
                    + "application VARCHAR(255), "
                    + "username VARCHAR(255), "
                    + "launched TIMESTAMP, "
                    + "finish_time TIMESTAMP, "
                    + "status VARCHAR(50), "
                    + "minor_status VARCHAR(100), "
                    + "PRIMARY KEY (id)"
                    + ")");
            stat.executeUpdate("CREATE INDEX username_workflow_idx "
                    + "ON Workflows(username)");
        } catch (SQLException ex) {
            if (!ex.getMessage().contains("Table/View 'WORKFLOWS' already exists")) {
                logger.print(WorkflowsDBListener.TAG + ex.getMessage());
            }
        }

        try {
            Statement stat = connection.createStatement();
            stat.executeUpdate("CREATE TABLE Outputs ("
                    + "workflow_id VARCHAR(255), "
                    + "path VARCHAR(255), "
                    + "processor VARCHAR(255), "
                    + "type VARCHAR(20), "
                    + "PRIMARY KEY (workflow_id, processor, path), "
                    + "FOREIGN KEY(workflow_id) REFERENCES Workflows(id) "
                    + "ON DELETE CASCADE"
                    + ")");
        } catch (SQLException ex) {
            if (!ex.getMessage().contains("Table/View 'OUTPUTS' already exists")) {
                logger.print(WorkflowsDBListener.TAG + ex.getMessage());
            }
        }

        try {
            Statement stat = connection.createStatement();
            stat.executeUpdate("CREATE TABLE Inputs ("
                    + "workflow_id VARCHAR(255), "
                    + "path VARCHAR(255), "
                    + "processor VARCHAR(255), "
                    + "type VARCHAR(20), "
                    + "PRIMARY KEY (workflow_id, processor, path), "
                    + "FOREIGN KEY(workflow_id) REFERENCES Workflows(id) "
                    + "ON DELETE CASCADE"
                    + ")");
        } catch (SQLException ex) {
            if (!ex.getMessage().contains("Table/View 'INPUTS' already exists")) {
                logger.print(WorkflowsDBListener.TAG + ex.getMessage());
            }
        }

        try {
            Statement stat = connection.createStatement();
            stat.executeUpdate("CREATE TABLE Processors ("
                    + "workflow_id VARCHAR(255), "
                    + "processor VARCHAR(255), "
                    + "completed INTEGER, "
                    + "queued INTEGER, "
                    + "failed INTEGER, "
                    + "PRIMARY KEY (workflow_id, processor), "
                    + "FOREIGN KEY(workflow_id) REFERENCES Workflows(id) "
                    + "ON DELETE CASCADE"
                    + ")");
        } catch (SQLException ex) {
            if (!ex.getMessage().contains("Table/View 'PROCESSORS' already exists")) {
                logger.print(WorkflowsDBListener.TAG + ex.getMessage());
            }
        }
    }

    /**
     * Close the database connection
     */
    @Override
    public synchronized void close() {
        try {
            connection.close();
        } catch (SQLException ex) {
            logger.warning(WorkflowsDBListener.TAG + ex.getMessage());
        }
    }

    @Override
    public synchronized boolean exists(String workflowID) throws DAOException {
        try {
            PreparedStatement ps = prepareStatement(
                    "SELECT id FROM Workflows WHERE id = ?");

            ps.setString(1, workflowID);

            ResultSet rs = ps.executeQuery();

            return rs.next();

        } catch (SQLException ex) {
            throw new DAOException(ex.getMessage());
        }
    }

    /**
     * Add a new workflow to the database
     * @param workflow Workflow bean
     * @throws DAOException
     */
    @Override
    public synchronized void add(WorkflowBean workflow) throws DAOException {
        try {
            PreparedStatement ps = prepareStatement(
                    "INSERT INTO Workflows(id, application, username, launched, "
                    + "status) "
                    + "VALUES (?, ?, ?, ?, ?)");

            ps.setString(1, workflow.getId());
            ps.setString(2, workflow.getApplication());
            ps.setString(3, workflow.getUser());
            ps.setTimestamp(4, new Timestamp(workflow.getStartTime().getTime()));
            ps.setString(5, workflow.getMajorStatus());

            execute(ps);

        } catch (SQLException ex) {
            throw new DAOException(ex.getMessage());
        }
    }

    /**
     * Update a workflow at the database
     * @param workflow Workflow bean
     * @throws DAOException
     */
    @Override
    public synchronized void update(WorkflowBean workflow) throws DAOException {
        try {
            PreparedStatement ps = prepareStatement("UPDATE "
                    + "Workflows "
                    + "SET finish_time=?, status=? "
                    + "WHERE id=?");

            ps.setTimestamp(1, new Timestamp(workflow.getFinishTime().getTime()));
            ps.setString(2, workflow.getMajorStatus());
            ps.setString(3, workflow.getId());

            executeUpdate(ps);

        } catch (SQLException ex) {
            throw new DAOException(ex.getMessage());
        }
    }

    /**
     * 
     * @param workflowID Workflow identification
     * @param path
     * @param processor
     * @param type
     * @throws DAOException
     */
    @Override
    public synchronized void addOutput(String workflowID, String path,
            String processor, String type) throws DAOException {
        try {
            PreparedStatement ps = prepareStatement(
                    "INSERT INTO Outputs(workflow_id, path, processor, type) "
                    + "VALUES (?, ?, ?, ?)");

            ps.setString(1, workflowID);
            ps.setString(2, path);
            ps.setString(3, processor);
            ps.setString(4, type);

            execute(ps);

        } catch (DAOException ex) {
            if (!ex.getMessage().contains("duplicate key value")) {
                throw ex;
            }
        } catch (SQLException ex) {
            throw new DAOException(ex.getMessage());
        }
    }

    /**
     * 
     * @param workflowID Workflow identification
     * @param path
     * @param processor
     * @param type
     * @throws DAOException
     */
    @Override
    public synchronized void addInput(String workflowID, String path,
            String processor, String type) throws DAOException {
        try {
            PreparedStatement ps = prepareStatement(
                    "INSERT INTO Inputs(workflow_id, path, processor, type) "
                    + "VALUES (?, ?, ?, ?)");

            ps.setString(1, workflowID);
            ps.setString(2, path);
            ps.setString(3, processor);
            ps.setString(4, type);

            execute(ps);

        } catch (DAOException ex) {
            if (!ex.getMessage().contains("duplicate key value")) {
                throw ex;
            }
        } catch (SQLException ex) {
            throw new DAOException(ex.getMessage());
        }
    }

    /**
     * 
     * @param workflowID
     * @param name
     * @return
     * @throws DAOException 
     */
    @Override
    public ProcessorBean getProcessor(String workflowID, String name) throws DAOException {

        try {
            PreparedStatement ps = prepareStatement(
                    "SELECT workflow_id, processor, completed, queued, failed "
                    + "FROM Processors WHERE workflow_id = ? AND processor = ?");

            ps.setString(1, workflowID);
            ps.setString(2, name);

            ResultSet rs = ps.executeQuery();

            if (rs.next()) {
                return new ProcessorBean(
                        rs.getString("workflow_id"),
                        rs.getString("processor"),
                        rs.getInt("completed"),
                        rs.getInt("queued"),
                        rs.getInt("failed"));

            } else {
                throw new DAOException("No data");
            }

        } catch (SQLException ex) {
            throw new DAOException(ex.getMessage());
        }
    }

    /**
     * 
     * @param processorBean
     * @throws DAOException 
     */
    @Override
    public void addProcessor(ProcessorBean processorBean) throws DAOException {
        
        try {
            PreparedStatement ps = prepareStatement(
                    "INSERT INTO Processors"
                    + "(workflow_id, processor, completed, queued, failed) "
                    + "VALUES (?, ?, ?, ?, ?)");

            ps.setString(1, processorBean.getWorkflowID());
            ps.setString(2, processorBean.getName());
            ps.setInt(3, processorBean.getCompleted());
            ps.setInt(4, processorBean.getQueued());
            ps.setInt(5, processorBean.getFailed());

            execute(ps);

        } catch (SQLException ex) {
            throw new DAOException(ex.getMessage());
        }
    }

    /**
     * 
     * @param processorBean
     * @throws DAOException 
     */
    @Override
    public void updateProcessor(ProcessorBean processorBean) throws DAOException {
        
        try {
            PreparedStatement ps = prepareStatement(
                    "UPDATE Processors SET "
                    + "completed = ?, queued = ?, failed = ? "
                    + "WHERE workflow_id = ? AND processor = ?");

            ps.setInt(1, processorBean.getCompleted());
            ps.setInt(2, processorBean.getQueued());
            ps.setInt(3, processorBean.getFailed());
            ps.setString(4, processorBean.getWorkflowID());
            ps.setString(5, processorBean.getName());

            executeUpdate(ps);

        } catch (SQLException ex) {
            throw new DAOException(ex.getMessage());
        }
    }
}
