package science.atlarge.grade10.cli.commands

import science.atlarge.grade10.cli.CliState
import science.atlarge.grade10.cli.Command

object NavigateCommand : Command {

    override val name: String
        get() = "navigate"
    override val shortHelpMessage: String
        get() = "navigate to a different execution phase for analysis"
    override val longHelpMessage: String
        get() = ""

    override fun process(arguments: List<String>, cliState: CliState) {
        if (arguments.size != 1) {
            println("Incorrect number of arguments to the \"${name}\" command: ${arguments.size}")
            println("Correct usage: ${name} path-to-phase")
        } else {
            val phase = cliState.resolvePhaseOrPrintError(arguments[0]) ?: return
            cliState.currentPhase = phase
            println("Now at phase: ${phase.path}")
        }
    }

}
