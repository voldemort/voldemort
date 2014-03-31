/*
 * Simple Scala Shell
 */

package voldemort

import scala.tools.nsc.Settings
import scala.tools.nsc.interpreter.ILoop


import org.apache.commons.lang.StringUtils
import org.apache.avro.Schema
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.io.JsonDecoder
import org.apache.commons.lang.mutable.MutableInt
// voldemort related imports
import voldemort.VoldemortClientShell
import voldemort.client.ClientConfig
import voldemort.client.protocol.RequestFormatType
import voldemort.VoldemortScalaShellHelper

object VoldemortScalaShell extends App {
	val settings = new Settings
	settings.usejavacp.value = true
	settings.deprecation.value = true

	new ClientLoop().process(settings)
}

class ClientLoop extends ILoop {
	// these are the modules pre-imported into the shell
	addThunk {
		intp.beQuietDuring {
			intp.addImports("java.lang.Math._", "java.lang._", "voldemort._",
				"java.util.List", "org.apache.commons.lang.StringUtils",
				"voldemort.VoldemortScalaShellHelper._")
		}
	}

	override def printWelcome() {
		echo("Scala shell for Project-Voldemort")
	}

	override def command(line: String): Result = {
		var success = false
		if (VoldemortScalaShellHelper.getClientShell != null) {
			success |= VoldemortScalaShellHelper.evaluate(line)
		}
		if (success) null else {	// TODO: Return a new 'null' Result type
			val result = super.command(line)
			result
		}
	}
}
