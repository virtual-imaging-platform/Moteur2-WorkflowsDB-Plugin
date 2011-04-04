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
package fr.insalyon.creatis.moteur.plugins.workflowsdb.dao.derby;

import fr.insalyon.creatis.moteur.plugins.workflowsdb.dao.WorkflowsDAO;
import fr.insalyon.creatis.moteur.plugins.workflowsdb.dao.bean.WorkflowBean;
import fr.insalyon.creatis.moteur.plugins.workflowsdb.exceptions.DAOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import org.apache.log4j.Logger;

/**
 *
 * @author Rafael Silva
 */
public class WorkflowsData implements WorkflowsDAO {

    private static Logger logger = Logger.getLogger(WorkflowsData.class);
    public static WorkflowsData instance;
    private final String DRIVER = "org.apache.derby.jdbc.ClientDriver";
    private final String DBURL = "jdbc:derby://localhost:1527/";
    private final String DBPATH = "/var/www/workflows-db";
    private Connection connection;

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
            Class.forName(DRIVER);
            connection = DriverManager.getConnection(DBURL + DBPATH + ";create=true");
            connection.setAutoCommit(true);
            createTables();

        } catch (SQLException ex) {
            try {
                connection = DriverManager.getConnection(DBURL + DBPATH);
                connection.setAutoCommit(true);
                createTables();

            } catch (SQLException ex1) {
                logger.error(ex1.getMessage());
            }
        } catch (ClassNotFoundException ex) {
            logger.error(ex.getMessage());
        }
    }

    private void createTables() {
        try {
            Statement stat = connection.createStatement();
            stat.executeUpdate("CREATE TABLE Workflows ("
                    + "id VARCHAR(255), "
                    + "application VARCHAR(255), "
                    + "username VARCHAR(255), "
                    + "launched TIMESTAMP, "
                    + "finish_time TIMESTAMP, "
                    + "status VARCHAR(50), "
                    + "minor_status VARCHAR(100), "
                    + "moteur_id INTEGER, "
                    + "moteur_key INTEGER, "
                    + "PRIMARY KEY (id)"
                    + ")");
            stat.executeUpdate("CREATE INDEX username_workflow_idx "
                    + "ON Workflows(username)");
        } catch (SQLException ex) {
            try {
                logger.info("Table Workflows already created!");
                connection.createStatement().executeUpdate("ALTER TABLE Workflows "
                        + "ADD COLUMN finish_time TIMESTAMP");
            } catch (SQLException ex1) {
                logger.info("Column finish_time already created!");
            }
        }
    }

    /**
     * Close the database connection
     */
    public synchronized void close() {
        try {
            connection.close();
        } catch (SQLException ex) {
            logger.error(ex.getMessage());
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
            PreparedStatement ps = connection.prepareStatement(
                    "INSERT INTO Workflows(id, application, username, launched, "
                    + "status, minor_status, moteur_id, moteur_key) "
                    + "VALUES (?, ?, ?, ?, ?, ?, ?, ?)");

            ps.setString(1, workflow.getId());
            ps.setString(2, workflow.getApplication());
            ps.setString(3, workflow.getUser());
            ps.setTimestamp(4, new Timestamp(workflow.getStartTime().getTime()));
            ps.setString(5, workflow.getMajorStatus());
            ps.setString(6, workflow.getMinorStatus());
            ps.setInt(7, workflow.getMoteurID());
            ps.setInt(8, workflow.getMoteurKey());
            ps.execute();

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
            PreparedStatement stat = connection.prepareStatement("UPDATE "
                    + "Workflows "
                    + "SET application=?, username=?, launched=?, "
                    + "finish_time=?, status=?, minor_status=? "
                    + "WHERE id=?");

            stat.setString(1, workflow.getApplication());
            stat.setString(2, workflow.getUser());
            stat.setTimestamp(3, new Timestamp(workflow.getStartTime().getTime()));
            stat.setTimestamp(4, new Timestamp(workflow.getFinishTime().getTime()));
            stat.setString(5, workflow.getMajorStatus());
            stat.setString(6, workflow.getMinorStatus());
            stat.setString(7, workflow.getId());
            stat.executeUpdate();

        } catch (SQLException ex) {
            throw new DAOException(ex.getMessage());
        }
    }
}
