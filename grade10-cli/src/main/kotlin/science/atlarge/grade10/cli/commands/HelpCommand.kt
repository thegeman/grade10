package science.atlarge.grade10.cli.commands

import science.atlarge.grade10.cli.CliState
import science.atlarge.grade10.cli.Command
import science.atlarge.grade10.cli.CommandRegistry

object HelpCommand : Command {

    override val name: String
        get() = "help"
    override val shortHelpMessage: String
        get() = "display this message"
    override val longHelpMessage: String
        get() = ""

    override fun process(arguments: List<String>, cliState: CliState) {
        if (arguments.isEmpty()) displayShortHelp()
        else displayLongHelp(arguments)
    }

    private fun displayShortHelp() {
        println("The following commands are available:")

        val sortedCommands = CommandRegistry.commands.sortedBy(Command::name)
        val maxCommandLength = sortedCommands.map { it.name.length }.max() ?: 0
        sortedCommands.forEach { commandProcessor ->
            println("    ${commandProcessor.name.padEnd(maxCommandLength)}    ${commandProcessor.shortHelpMessage}")
        }

        println("Use \"help [command]\" to get additional information about a command")
    }

    private fun displayLongHelp(arguments: List<String>) {
        if (arguments.size != 1) {
            println("Incorrect number of arguments to the \"help\" command: ${arguments.size}")
            println("Correct usage: help [command]")
            return
        }

        val commandName = arguments[0]
        val command = CommandRegistry[commandName]
        if (command == null) {
            println("Cannot display help message for unknown command: \"$commandName\"")
            return
        }

        println(command.longHelpMessage)
    }

}
