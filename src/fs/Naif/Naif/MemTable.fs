module MemTable
open System.IO
open System
open System.Text

type Agent<'T> = MailboxProcessor<'T>

let getBytes s = System.Text.Encoding.UTF8.GetBytes(s : string) : byte[]

let toBytes(key, value) =
    let all = seq {
                let keyBytes = getBytes key
                let valueBytes = getBytes value
                yield BitConverter.GetBytes keyBytes.Length 
                yield keyBytes
                yield BitConverter.GetBytes valueBytes.Length
                yield valueBytes
            } 

    Array.concat all

let private logWrite (fs:FileStream, pair) =
    let write bytes = 
        fs.Write(bytes, 0, bytes.Length)
        fs.Flush()
        bytes

    match pair with
    | (key:string, value:string) ->
        try
            toBytes(key, value) 
            |> write     
            |> Array.length
            |> Some
        with
            | ex -> None

type MemTableMsg =
| Put of (string * string)
| Length of AsyncReplyChannel<int64>


let agent path = 
    Agent.Start(fun inbox -> 
        let rec loop fs =
            async { let! msg = inbox.Receive ()
                    match msg with
                    | Put (k, v) -> 
                        logWrite (fs,  (k, v)) |> ignore
                        return! loop fs
                    | Length reply -> 
                        reply.Reply fs.Length 
                        return! loop fs }

        loop (File.OpenWrite path) )

type MemTable =  
    { Cache : Map<string, string>; File : FileStream; Size : int64 }
    member this.add (k,v) =
        match logWrite (this.File, (k,v)) with
        | Some n -> 
            let currentSize = this.Size + (int64)n
            { this with Cache = this.Cache.Add(k, v); Size = currentSize }

        | None -> this
    member x.get key =
        x.Cache.TryFind key
    member x.close () =
        x.File.Close()
        x.File.Dispose()
    member x.complete () = 
        File.Move(x.File.Name, x.File.Name + "_old")
        x
    interface System.IDisposable with
        member x.Dispose() =
            x.File.Dispose()

type MemTableResult =
    | Mt of MemTable
    | Sst of MemTable

type MemTable with
    member x.add2 (k, v) =
        match logWrite (x.File, (k,v)) with
        | Some n -> 
            Sst x
        | None -> Mt x

let readLog (fs:Stream) =
    let readBytes (fs:Stream, num) =
        let bytes = Array.init num (fun i -> 0uy)
        let read = fs.Read(bytes, 0, num)
        bytes

    let readInt (fs:Stream) = 
        let toInt bytes = BitConverter.ToInt32(bytes, 0)
        readBytes (fs, 4) |> toInt
    
    let readString (fs:Stream, num) =
        readBytes (fs, num) |> System.Text.Encoding.UTF8.GetString

    let rec read m:Map<string,string> =
        let l = readInt fs
        if(l = 0) then
            m
        else
            let k = readString (fs, l)
            let l = readInt fs
            let v = readString (fs, l)
            read (m.Add (k, v))
    read Map.empty

let openFile s =
    if (File.Exists s) then
        use readFs = File.OpenRead s
        let log = readLog readFs
        let pos = readFs.Position;
        readFs.Close()
        //readFs.Dispose()

        let fs = File.Open(s, FileMode.Append)
        { Cache = log; File = fs; Size = pos }
    else
        { Cache = Map.empty; File = File.OpenWrite(s); Size = 0L }

let closeFile (fs:FileStream) = 
    fs.Close()
    fs.Dispose();

