#r "nuget: Akka.FSharp, 1.5.13"
#r "nuget: Akka, 1.5.13"

open System
open Akka.FSharp
open System.Threading

// Node message types for operations like create, join, stabilize
type NodeMsg =
    | Create
    | Join of int
    | GetSuccessorNode of int
    | ReceiveSuccessorNode of int
    | StabilizeChord
    | GetPredecessorNode
    | ReceivePredecessorNode of int
    | NotifyPeers of int
    | FixFingerTables
    | GetSuccessorInFinger of int * int * int
    | UpdateFingerTable of int * int
    | Request
    | RequestMsg
    | GetSuccessorOfKey of int * int * int
    | IdentifyKey of int

// Message type representing the count of processed nodes with hop counts and request numbers.
type CountTotalNoOfHops = NoOfNodesProcessed of int * int * int

// Actor to calculate the average number of hops across nodes.
let noOfHops numOfNodes (mailbox: Actor<_>) =
    // State variables for tracking hops and requests.
    let mutable totalHopCount = 0
    let mutable totalNumOfRequests = 0
    let mutable noOfNodesProcessed = 0

    let rec loop () =
        actor {
            let! message = mailbox.Receive()

            // Handle messages with node processing information.
            match message with
            | NoOfNodesProcessed(nodeID, hopCount, numRequest) ->
                // Accumulate total hops and requests.
                totalHopCount <- totalHopCount + hopCount
                totalNumOfRequests <- totalNumOfRequests + numRequest
                noOfNodesProcessed <- noOfNodesProcessed + 1

                // Calculate and print average hops when all nodes have reported.
                if (noOfNodesProcessed = numOfNodes) then
                    printfn "Average Number of Hops : %f" ((float totalHopCount) / (float totalNumOfRequests))
                    // Terminate the actor system when done.
                    mailbox.Context.System.Terminate() |> ignore

            return! loop ()
        }

    loop ()



// Helper function to construct an actor path for a given node ID.
let getPeerPath path =
    let actorPath = @"akka://ChordProtocol/user/" + string path
    actorPath

// Helper functions to determine the correct successor or predecessor of a node.
// They take into account the circular nature of the Chord ring.
let checkExcludePredecessorSuccessor chordRingRange predecessor currentNodeValue successor =
    let updatedSuccessor =
        if (successor < predecessor) then
            successor + chordRingRange
        else
            successor

    let updatedSuccessorVal =
        if ((currentNodeValue < predecessor) && (predecessor > successor)) then
            (currentNodeValue + chordRingRange)
        else
            currentNodeValue

    (predecessor = successor)
    || ((updatedSuccessorVal > predecessor) && (updatedSuccessorVal < updatedSuccessor))

let checkExcludePredecessorIncludeSuccessor chordRingRange predecessor currentNodeValue successor =
    let updatedSuccessor =
        if (successor < predecessor) then
            successor + chordRingRange
        else
            successor

    let updatedSuccessorValue =
        if ((currentNodeValue < predecessor) && (predecessor > successor)) then
            (currentNodeValue + chordRingRange)
        else
            currentNodeValue

    (predecessor = successor)
    || ((updatedSuccessorValue > predecessor)
        && (updatedSuccessorValue <= updatedSuccessor))

// Function to create and manage a Chord node's lifecycle and interactions.
let createChord (nodeID: int) m maxNumReq hopCntRef (mailbox: Actor<_>) =
    // Initialize node state variables.
    let chordRingRange = int (Math.Pow(2.0, float m))
    let mutable predecessorID = -1
    let mutable fingerTable = Array.create m -1
    let mutable next = 0
    let mutable totalHopCount = 0
    let mutable numOfRequests = 0

    let rec loop () =
        actor {
            let! message = mailbox.Receive()
            let sender = mailbox.Sender()

            match message with
            // Node creation logic, including scheduling stabilization.
            | Create ->
                predecessorID <- -1

                for i = 0 to m - 1 do
                    fingerTable.[i] <- nodeID

                mailbox.Context.System.Scheduler.ScheduleTellRepeatedly(
                    TimeSpan.FromMilliseconds(0.0),
                    TimeSpan.FromMilliseconds(500.0),
                    mailbox.Self,
                    StabilizeChord
                )

                mailbox.Context.System.Scheduler.ScheduleTellRepeatedly(
                    TimeSpan.FromMilliseconds(0.0),
                    TimeSpan.FromMilliseconds(500.0),
                    mailbox.Self,
                    FixFingerTables
                )

            // Logic for joining a node to an existing Chord network.
            | Join(exsNode) ->
                predecessorID <- -1
                let exsNodePath = getPeerPath exsNode
                let exsNodeRef = mailbox.Context.ActorSelection exsNodePath
                exsNodeRef <! GetSuccessorNode(nodeID)

            | GetSuccessorNode(id) ->
                if (checkExcludePredecessorIncludeSuccessor chordRingRange nodeID id fingerTable.[0]) then
                    let newNodePath = getPeerPath id
                    let newNodeRef = mailbox.Context.ActorSelection newNodePath
                    newNodeRef <! ReceiveSuccessorNode(fingerTable.[0])
                else
                    let mutable i = m - 1

                    while (i >= 0) do
                        if (checkExcludePredecessorSuccessor chordRingRange nodeID fingerTable.[i] id) then
                            let closestPrecNodeID = fingerTable.[i]
                            let closestPrecNodePath = getPeerPath closestPrecNodeID
                            let closestPrecNodeRef = mailbox.Context.ActorSelection closestPrecNodePath
                            closestPrecNodeRef <! GetSuccessorNode(id)
                            i <- -1

                        i <- i - 1

            | ReceiveSuccessorNode(succesorID) ->
                for i = 0 to m - 1 do
                    fingerTable.[i] <- succesorID

                mailbox.Context.System.Scheduler.ScheduleTellRepeatedly(
                    TimeSpan.FromMilliseconds(0.0),
                    TimeSpan.FromMilliseconds(500.0),
                    mailbox.Self,
                    StabilizeChord
                )

                mailbox.Context.System.Scheduler.ScheduleTellRepeatedly(
                    TimeSpan.FromMilliseconds(0.0),
                    TimeSpan.FromMilliseconds(500.0),
                    mailbox.Self,
                    FixFingerTables
                )

            // Stabilization logic for maintaining Chord properties.
            | StabilizeChord ->
                let succID = fingerTable.[0]
                let succPath = getPeerPath succID
                let succRef = mailbox.Context.ActorSelection succPath
                succRef <! GetPredecessorNode

            | GetPredecessorNode -> sender <! ReceivePredecessorNode(predecessorID)

            | ReceivePredecessorNode(x) ->
                if
                    ((x <> -1)
                     && (checkExcludePredecessorSuccessor chordRingRange nodeID x fingerTable.[0]))
                then
                    fingerTable.[0] <- x

                let succID = fingerTable.[0]
                let succPath = getPeerPath succID
                let succRef = mailbox.Context.ActorSelection succPath
                succRef <! NotifyPeers(nodeID)

            | NotifyPeers(exsNode) ->
                if
                    ((predecessorID = -1)
                     || (checkExcludePredecessorSuccessor chordRingRange predecessorID exsNode nodeID))
                then
                    predecessorID <- exsNode

            // Periodically executed logic to fix finger table entries.
            | FixFingerTables ->
                next <- next + 1

                if (next >= m) then
                    next <- 0

                let fingerValue = nodeID + int (Math.Pow(2.0, float (next)))
                mailbox.Self <! GetSuccessorInFinger(nodeID, next, fingerValue)

            | GetSuccessorInFinger(originNodeID, next, id) ->
                if (checkExcludePredecessorIncludeSuccessor chordRingRange nodeID id fingerTable.[0]) then
                    let originNodePath = getPeerPath originNodeID
                    let originNodeRef = mailbox.Context.ActorSelection originNodePath
                    originNodeRef <! UpdateFingerTable(next, fingerTable.[0])
                else
                    let mutable i = m - 1

                    while (i >= 0) do
                        if (checkExcludePredecessorSuccessor chordRingRange nodeID fingerTable.[i] id) then
                            let closestPrecNodeID = fingerTable.[i]
                            let closestPrecNodePath = getPeerPath closestPrecNodeID
                            let closestPrecNodeRef = mailbox.Context.ActorSelection closestPrecNodePath
                            closestPrecNodeRef <! GetSuccessorInFinger(originNodeID, next, id)
                            i <- -1

                        i <- i - 1

            | UpdateFingerTable(next, fingerSuccessor) -> fingerTable.[next] <- fingerSuccessor

            // Request processing logic to handle key lookups.
            | Request ->
                if (numOfRequests < maxNumReq) then
                    mailbox.Self <! RequestMsg
                    mailbox.Context.System.Scheduler.ScheduleTellOnce(TimeSpan.FromSeconds(1.), mailbox.Self, Request)
                else
                    hopCntRef <! NoOfNodesProcessed(nodeID, totalHopCount, numOfRequests)

            | RequestMsg ->
                let key = (System.Random()).Next(chordRingRange)
                mailbox.Self <! GetSuccessorOfKey(nodeID, key, 0)

            | GetSuccessorOfKey(originNodeID, id, numHops) ->
                if (id = nodeID) then
                    let originNodePath = getPeerPath originNodeID
                    let originNodeRef = mailbox.Context.ActorSelection originNodePath
                    originNodeRef <! IdentifyKey(numHops)
                elif (checkExcludePredecessorIncludeSuccessor chordRingRange nodeID id fingerTable.[0]) then
                    let originNodePath = getPeerPath originNodeID
                    let originNodeRef = mailbox.Context.ActorSelection originNodePath
                    originNodeRef <! IdentifyKey(numHops)
                else
                    let mutable i = m - 1

                    while (i >= 0) do
                        if (checkExcludePredecessorSuccessor chordRingRange nodeID fingerTable.[i] id) then
                            let closestPrecNodeID = fingerTable.[i]
                            let closestPrecNodePath = getPeerPath closestPrecNodeID
                            let closestPrecNodeRef = mailbox.Context.ActorSelection closestPrecNodePath
                            closestPrecNodeRef <! GetSuccessorOfKey(originNodeID, id, numHops + 1)
                            i <- -1

                        i <- i - 1

            // Logic for identifying the node responsible for a key.
            | IdentifyKey(hopCount) ->
                if (numOfRequests < maxNumReq) then
                    totalHopCount <- totalHopCount + hopCount
                    numOfRequests <- numOfRequests + 1

            return! loop ()
        }

    loop ()

// The main function to simulate the Chord protocol in action.
let SimulatorToSendRequests (numOfNodes, numOfRequests) =
    // Create the actor system named "ChordProtocol".
    let system = System.create "ChordProtocol" (Configuration.load ())

    let numOfNodes = numOfNodes
    let numOfRequests = numOfRequests

    printfn "No of Nodes = %A , No of Requests = %A" numOfNodes numOfRequests
    // Initialize and prepare variables for the simulation.
    let m = 20
    let chordRingRange = int (Math.Pow(2.0, float m))

    let hopCntRef = spawn system "noOfHops" (noOfHops numOfNodes)

    // Array to store node IDs and references for all nodes in the system.
    let nodeKeys = Array.create numOfNodes -1
    let nodeReferencesArray = Array.create numOfNodes null
    let mutable currentNode = 0

    while (currentNode < numOfNodes) do
        try
            let nodeID = (Random()).Next(chordRingRange)
            nodeKeys.[currentNode] <- nodeID

            nodeReferencesArray.[currentNode] <-
                spawn system (string nodeID) (createChord nodeID m numOfRequests hopCntRef)

            if (currentNode = 0) then
                nodeReferencesArray.[currentNode] <! Create
            else
                nodeReferencesArray.[currentNode] <! Join(nodeKeys.[0])

            currentNode <- currentNode + 1
            Thread.Sleep(500)
        with _ ->
            ()

    Thread.Sleep(30000)

    for nodeReference: Akka.Actor.IActorRef in nodeReferencesArray do
        nodeReference <! Request
        Thread.Sleep(500)

    system.WhenTerminated.Wait()
    0
