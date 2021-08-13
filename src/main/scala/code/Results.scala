package code

import queries.NumberOfConsumers
import queries.ConsumedBeverages
import queries.AvailableBeverages
import queries.PartitionsAndViews
import queries.TableProperties
import queries.RemoveRowP1
import queries.RemoveRowP2
import conn.util.LocalHiveCon
import org.apache.spark.sql.DataFrame

import scala.io.StdIn


object Results {
    private val rows = 5000
    private val cols = 100
    private val vert = false

    private var finished = false

    private val inPrompt = "Please tell me which problem you'd like to view: "

    def main(args: Array[String]): Unit = {
        executeDemoQueries
        
        while (!finished) {
            printScenarioSelection

            val c = StdIn.readLine(inPrompt)

            if (c.length > 0)
                handleInput(c.head)
            else
                handleInput(' ')
        }
    }

    def handleInput: Char => Unit = input => { input match {
        case '1' => clearShowQ(NumberOfConsumers.queries)
        case '2' => clearShowQ(ConsumedBeverages.queries)
        case '3' => clearShowQ(AvailableBeverages.queries)
        case '4' => clearShowQ(PartitionsAndViews.queries)
        case '5' => clearShowQ(TableProperties.queries)
        case '6' => resetScreen
                    showQ(RemoveRowP1.queries)
                    showQ(RemoveRowP2.queries)
        case 'q' => println("Thank you...Come again?")
                    LocalHiveCon.stop
                    Thread.sleep(550l)
                    endProgramExecution
        case  x  => println(s"Unrecognized option '$x' -- please select from the options above")
    }
        if (!finished)
            waitUntilReady
    }

    def executeDemoQueries: Unit = {
        showQ(ConsumedBeverages.queries)
        showQ(AvailableBeverages.queries)
        waitUntilReady
    }

    def printScenarioSelection: Unit = {
        resetScreen
        println("1 -> Problem 1\t\t2 -> Problem 2")
        println("3 -> Problem 3\t\t4 -> Problem 4")
        println("5 -> Problem 5\t\t6 -> The Kobayashi Maru")
        println("\nq -> Quit")
        println("\n\n\n")
    }

    def showQ: Seq[DataFrame] => Unit = q => q.foreach(_.show(rows, cols, vert))

    def clearShowQ: Seq[DataFrame] => Unit = q => {resetScreen; showQ(q)}

    def waitUntilReady: Unit = {
        StdIn.readLine("[Press Enter to Clear the Screen]")
    }

    def endProgramExecution: Unit = finished = true

    def resetScreen: Unit = print("\u001B[2J\u001B[0;0H")
}