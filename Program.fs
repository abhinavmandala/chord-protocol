module Program
open System

[<EntryPoint>]
let main argv =
    if Array.length argv = 2 then
        let numNodes = Int32.Parse argv.[0]
        let numRequests = Int32.Parse argv.[1]

        try
            ChordActor.SimulatorToSendRequests(numNodes, numRequests)

        with :? FormatException ->
            printfn "Invalid number of nodes or Invalid number of requests."
            1
    else
        printfn "Usage: dotnet run <numNodes> <numRequests>"
        1