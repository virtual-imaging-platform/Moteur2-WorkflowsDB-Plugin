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

import fr.cnrs.i3s.moteur2.execution.Workflow;
import fr.cnrs.i3s.moteur2.execution.WorkflowListener;
import fr.cnrs.i3s.moteur2.plugins.ListenerFactoryInterface;
import fr.cnrs.i3s.moteur2.plugins.MoteurPlugins;
import fr.cnrs.i3s.moteur2.plugins.PluginException;
import fr.insalyon.creatis.moteur.plugins.workflowsdb.Configuration.Factory;
import java.util.HashMap;
import net.xeoh.plugins.base.annotations.PluginImplementation;

/**
 *
 * @author Rafael Silva
 */
@PluginImplementation
public class WorkflowsDBListenerFactory implements ListenerFactoryInterface {

    public WorkflowsDBListenerFactory() {

        HashMap<String, String> parameters = MoteurPlugins.getPluginDescriptor(this).getParameters();

        String factory = parameters.get("factory");
        String host = parameters.get("host");
        String port = parameters.get("port");
        String path = parameters.get("path");
        String dbName = parameters.get("db_name");
        String dbUser = parameters.get("db_user");
        String dbPassword = parameters.get("db_password");

        Configuration.FACTORY = factory != null ? Factory.valueOf(factory.toUpperCase()) : Factory.DERBY;
        Configuration.HOST = host != null ? host : "localhost";
        Configuration.PORT = port != null ? new Integer(port) : 1527;
        Configuration.DB_PATH = path != null ? path : "/var/www/workflows-db";
        Configuration.DB_NAME = dbName != null ? dbName : "database-name";
        Configuration.DB_USER = dbUser != null ? dbUser : "mysql-user";
        Configuration.DB_PASSWORD = dbPassword != null ? dbPassword : "secret";
    }

    @Override
    public WorkflowListener createWorkflowListner(Workflow workflow) throws PluginException {
        return new WorkflowsDBListener(workflow);
    }
}
