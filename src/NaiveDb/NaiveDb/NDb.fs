module NDb

open NaiveDb.SSTable

open Maybe

type internal DbMsg =
| Put of (string * string)
| Get of (string * AsyncReplyChannel<Option<string>>)


type internal DbState = {
    Path: string
    Tables: List<Table>
    Current: Map<string,string>
    MemTables: List<Map<string,string>>
}

type Db (path:string) =
    let readTables path =
        []

    let agent = MailboxProcessor.Start(fun inbox ->
        let rec loop state = async {
            printfn "loop start state: %A" state.Current
            let! msg = inbox.Receive()
            match msg with
            | Get (k, rc) -> 
                maybe { 
                    let! x = state.Current.TryFind k 
                    return x
                    } |> rc.Reply
                return! loop state
            | Put (k, v) -> 
                return! loop { state with Current = (Map.add k v state.Current) }
        }
        loop {  Path = path; 
                Tables = readTables (path); 
                Current = Map.empty; 
                MemTables = [] })
            
    member this.put key value = 
        Put (key, value) |> agent.Post 

    member this.tryGet (key:string) : Async<Option<string>> =
        async { return! agent.PostAndAsyncReply (fun rc -> Get (key, rc)) }

    member this.get key =
        match agent.PostAndReply(fun rc -> Get(key, rc)) with
        | Some s -> s
        | None -> failwith "key not found"



let db = Db ""
db.put "one" "hello"
db.put "two" "world"
db.get "one"
db.get "two"
db.get "three"
Async.RunSynchronously (db.tryGet "one") 

let openOrCreate path = 
    Db path