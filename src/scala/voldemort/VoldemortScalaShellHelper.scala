/*
 * Implements many of the core-functionality for the scala shell
 */

package voldemort

import scala.tools.nsc.Settings
import scala.tools.nsc.interpreter.ILoop

import org.apache.commons.lang.StringUtils;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.JsonDecoder;
import org.apache.commons.lang.mutable.MutableInt;
// voldemort related imports
import voldemort.client.DefaultStoreClient;
import voldemort.VoldemortClientShell;
import voldemort.client.ClientConfig;
import voldemort.client.protocol.RequestFormatType;


object VoldemortScalaShellHelper {
	private var clientShell: VoldemortClientShell = null
	private var printCommands: Boolean = false


	/* mutator(s) for private variable(s) */
	def setPrintCommands(newVal: Boolean): Unit = this.printCommands = newVal

	def setClientShell(client: VoldemortClientShell): Unit = {
		clientShell = client
	}

	/* accessor(s) for private variable(s) */
	def getPrintCommands: Boolean = this.printCommands

	def getClientShell(): VoldemortClientShell = clientShell

	/*  Additional nice-to have methods...can save a lot of time */
	def createClientConfig(bootStrapUrl: String): ClientConfig = {
		new ClientConfig().setBootstrapUrls(bootStrapUrl).setEnableLazy(false).setRequestFormatType(RequestFormatType.VOLDEMORT_V3)
	}

	def createClientShell(storeName: String, bootStrapUrl: String):VoldemortClientShell = {
		new VoldemortClientShell(createClientConfig(bootStrapUrl), storeName,
                                                              null,
                                                              System.out,
                                                              System.err)
	}

	def getStoreClient(): DefaultStoreClient[Object, Object] = clientShell.getStoreClient()

	/* Used for evaluating commands to the client shell from scala itself
	 * Currently ugle \" marks are needed for adding strings"	
	 * Could do this programmatically but the Key/Value should be able to take any object type
	 * It is recommended that put and get operations are done directly by instantiating the StoreClient */
	def evaluate(line: String): Boolean = 
		evaluate(clientShell, line)

	def evaluate(clientShell: VoldemortClientShell, line: String): Boolean = 
		clientShell.evaluateCommand(line, this.printCommands)
}