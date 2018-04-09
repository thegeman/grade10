package science.atlarge.grade10.cli

import science.atlarge.grade10.cli.commands.*

object CommandRegistry {

    private val commandMap = mutableMapOf<String, Command>()

    val commands
        get() = commandMap.values.toSet()

    operator fun get(commandName: String): Command? = commandMap[commandName]

    fun registerCommand(command: Command) {
        commandMap[command.name] = command
    }

    fun registerBuiltinCommands() {
        listOf(
                ExportBottlenecksCommand,
                ExportPerformanceIssuesCommand,
                ExportRawMetricDataCommand,
                ExportResourceAttributionCommand,
                ExportSampledMetricDataCommand,
                HelpCommand,
                ListCommand,
                ListResourcesCommand,
                NavigateCommand
        ).forEach { registerCommand(it) }
    }

    fun processCommand(commandName: String, arguments: List<String>, cliState: CliState) {
        val command = this[commandName]
        if (command == null) {
            println("Unknown command: \"$commandName\"")
        } else {
            command.process(arguments, cliState)
        }
    }

}

interface Command {

    val name: String
    val shortHelpMessage: String
    val longHelpMessage: String

    fun process(arguments: List<String>, cliState: CliState)

}
