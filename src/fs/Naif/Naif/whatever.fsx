
type Agent<'T> = MailboxProcessor<'T>

let counter = 
    new Agent<_>(fun inbox ->
        let rec loop n =
            async { printfn "n = %d, waiting..." n
                    let! msg = inbox.Receive()
                    return! loop(n + msg)}
        loop 0)
                     
counter.Start()

counter.Post(1)

type internal msg = Increment of int | Fetch of AsyncReplyChannel<int> | Stop

type CountingAgent() =
    let counter = MailboxProcessor.Start(fun inbox ->
            let rec loop n =
                async { let! msg = inbox.Receive()
                        match msg with
                        | Increment m ->
                            return! loop( n + m)
                        | Stop ->
                            return ()
                        | Fetch replyChannel ->
                            do replyChannel.Reply n
                            return! loop n}
            loop(0))
    //counter.Error.Add (fun e -> printfn "error: %" e)
    member this.Increment(n) = counter.Post(Increment n)
    member this.Stop() = counter.Post Stop
    member this.Fetch()  = counter.PostAndReply(fun rc -> Fetch rc)

    

let c = CountingAgent()
c.Increment 5
c.Increment 2
c.Fetch()
//c.Stop()
c.Fetch()


open System.Windows.Forms

let form = new Form()
form.Visible <- true
form.TopMost <- true
form.Text <- "Hellow form"

form.Dispose()


type Message = 
    | Message1
    | Message2 of int
    | Message3 of string

let agent = 
    MailboxProcessor.Start(fun inbox ->
        let rec loop() =
            inbox.Scan(function
                | Message1 -> 
                    Some (async{do printfn "message1!"
                                return! loop()})
                | Message2 n -> 
                    Some (async{do printfn "message2! %A" n
                                return! loop()})
                | Message3 _ -> 
                    None)
        loop())    


agent.Post Message1
agent.Post (Message2(5))

agent.Post(Message3("hey"))
agent.Post (Message2(7))
//agent.UnsafeMessageQueueContents

open  Microsoft.FSharp.Quotations

let oneExpr = <@ 1 @>

let plusExpr = <@ 1 + 1 @>


let pickle (i : int32) =
    (   byte (i &&& 0xFF),
        byte (i >>> 8 &&& 0xFF),
        byte (i >>> 16 &&& 0xFF),
        byte (i >>> 24 &&& 0xFF))

let unpickle (a : byte, b : byte, c : byte, d : byte) =
    let a' = int a
    let b' = int b
    let c' = int c
    let d' = int d

    a' ||| (b' <<< 8) ||| (c' <<< 16) ||| (d' <<< 24)



let p = pickle 12345677
p
unpickle p



