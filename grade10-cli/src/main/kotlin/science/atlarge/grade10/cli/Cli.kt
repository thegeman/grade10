package science.atlarge.grade10.cli

import science.atlarge.grade10.Grade10Job
import science.atlarge.grade10.Grade10JobResult
import science.atlarge.grade10.Grade10PlatformRegistry
import science.atlarge.grade10.cli.util.MetricList
import science.atlarge.grade10.examples.giraph.GiraphPlatform
import science.atlarge.grade10.examples.powergraph.PowergraphPlatform
import science.atlarge.grade10.model.execution.Phase
import java.nio.file.Path
import java.nio.file.Paths

class Cli {

    fun main(args: Array<String>) {
        println("Welcome to the Grade10 CLI!")
        println()

        val platform: String
        val version: String
        val inputPaths: List<Path>
        val outputPath: Path
        if (args.size != 4) {
            val platformAndVersion = pollPlatformAndVersion()
            platform = platformAndVersion.first
            version = platformAndVersion.second
            inputPaths = pollInputPaths()
            outputPath = pollOutputPath()
        } else {
            platform = args[0]
            version = args[1]
            inputPaths = args[2].split(",").map { Paths.get(it) }
            outputPath = Paths.get(args[3])
        }

        if (!outputPath.toFile().exists()) {
            outputPath.toFile().mkdirs()
        }

        val grade10PlatformJob = Grade10PlatformRegistry[platform, version]!!.createJob(inputPaths, outputPath)
        val grade10Job = Grade10Job(grade10PlatformJob)
        val grade10JobResult = grade10Job.execute()

        println()
        println("Completed analysis of the input job!")
        println("Explore the results interactively by issuing commands. Enter \"help\" for a list of available commands.")
        println()

        val cliState = CliState(platform, version, inputPaths, outputPath, grade10JobResult)
        while (true) {
            print("> ")
            val fullCommand = readLine() ?: break

            val commandSplits = fullCommand.split(' ')
            val command = commandSplits[0]
            val commandArgs = commandSplits.subList(1, commandSplits.size)
            CommandRegistry.processCommand(command, commandArgs, cliState)

            println()
        }
    }

    private fun pollPlatformAndVersion(): Pair<String, String> {
        val platformVersionMap = Grade10PlatformRegistry.listPlatformNamesAndVersions()
                .groupBy({ it.first }, { it.second })
        val platform = pollPlatform(platformVersionMap.keys)
        val version = pollVersion(platformVersionMap[platform]!!.toSet())
        return platform to version
    }

    private fun pollPlatform(platforms: Set<String>): String {
        val platformsSorted = platforms.toSortedSet()
        var selectedPlatform: String?

        do {
            println("Enter the name of the platform under test (\"?\" to list available platforms):")
            selectedPlatform = readLine()

            if (selectedPlatform == "?") {
                println("Select one of the following available platforms:")
                platformsSorted.forEach { platform -> println("\t$platform") }
                selectedPlatform = null
            } else if (selectedPlatform !in platforms) {
                println("Unknown platform: \"$selectedPlatform\", select one of the following:")
                platformsSorted.forEach { platform -> println("\t$platform") }
                selectedPlatform = null
            }
        } while (selectedPlatform == null)
        println()

        return selectedPlatform
    }

    private fun pollVersion(versions: Set<String>): String {
        val versionsSorted = versions.toSortedSet()
        var selectedVersion: String?

        do {
            println("Enter the version of the platform under test (\"?\" to list available versions):")
            selectedVersion = readLine()

            if (selectedVersion == "?") {
                println("Select one of the following available versions:")
                versionsSorted.forEach { version -> println("\t$version") }
                selectedVersion = null
            } else if (selectedVersion !in versions) {
                println("Unknown version: \"$selectedVersion\", select one of the following:")
                versionsSorted.forEach { version -> println("\t$version") }
                selectedVersion = null
            }
        } while (selectedVersion == null)
        println()

        return selectedVersion
    }

    private fun pollInputPaths(): List<Path> {
        var inputPaths: List<Path>?
        do {
            println("Enter the path(s) to a job's input data, separated by comma's:")
            val inputPathString = readLine()
            if (inputPathString == null) {
                throw IllegalStateException()
            } else {
                inputPaths = inputPathString.split(",").filter { it.isNotBlank() }.map { Paths.get(it) }
                if (inputPaths.isEmpty()) {
                    println("Given list of input paths is empty")
                    inputPaths = null
                } else {
                    val pathsCopy = inputPaths
                    for (path in pathsCopy) {
                        if (!path.toFile().exists()) {
                            println("Given input path does not exist: \"${path.toAbsolutePath()}\"")
                            inputPaths = null
                            break
                        } else if (!path.toFile().isDirectory) {
                            println("Given input path is not a directory: \"${path.toAbsolutePath()}\"")
                            inputPaths = null
                            break
                        }
                    }
                }
            }
        } while (inputPaths == null)
        println()

        return inputPaths
    }

    private fun pollOutputPath(): Path {
        var outputPath: Path?
        do {
            println("Enter a path to store output data of the performance analysis:")
            outputPath = readLine()?.let { Paths.get(it) }

            if (outputPath == null) {
                throw IllegalStateException()
            } else if (outputPath.toFile().exists() && !outputPath.toFile().isDirectory) {
                println("Given output path does exists but is not a directory: \"${outputPath.toAbsolutePath()}\"")
                outputPath = null
            }
        } while (outputPath == null)
        println()

        return outputPath
    }

}

class CliState(
        val platform: String,
        val version: String,
        val inputPaths: List<Path>,
        val outputPath: Path,
        val grade10JobResult: Grade10JobResult
) {

    var currentPhase = grade10JobResult.executionModel.rootPhase
    val metricList = MetricList.fromResourceModel(grade10JobResult.resourceModel)

    fun phaseOutputPath(phase: Phase): Path {
        val subdirectories = generateSequence(phase, Phase::parent)
                .toList()
                .asReversed()
                .drop(1) // Skip root phase
                .map { p ->
                    if (p.type.repeatability.isRepeatable) "${p.type.name}__${p.instanceId}"
                    else p.name
                }
        return subdirectories
                .fold(outputPath.resolve("phases")) { currentPath, subdirectory ->
                    currentPath.resolve(subdirectory)
                }
    }

}

fun main(args: Array<String>) {
    GiraphPlatform.register()
    PowergraphPlatform.register()
    CommandRegistry.registerBuiltinCommands()
    Cli().main(args)
}
