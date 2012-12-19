module MemTable
open System.IO
open System
open System.Text

let private getBytes s = Encoding.UTF8.GetBytes(s : string) : byte[]

let private toBytes(key, value) =
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
        File.Move(x.File.Name, x.File.Name + "XX")
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



//
//
//let addToMemTable (fs, key, value) =
//    //{ mt with Cache =  mt.Cache.Add(key, value) }    
//    let lw pair = logWrite (fs, pair)
//    match lw (key, value) with
//    | Some n ->
//        printfn "written %A " n
////        if(n > 1024 * 4) then
////            printf "big"
////            let cache = mt.Cache
////            mt <- { mt with Cache = Map.empty }
////            //write cache to disk
////            //Map.map (fun (k, v) -> toBytes (k, v)) 
////            cache
////            |> Map.toList
////            |> List.map (fun (k, v) -> toBytes (k, v)) 
////            |> Array.concat
////            |> (fun x -> File.WriteAllBytes (@"c:\dump\naif\sstable1.sst", x))
//
//            //ignore // create sstable from cache
//        
//    | None -> printf "could not write to log"
//    { mt with Cache =  mt.Cache.Add(key, value) }  

let closeFile (fs:FileStream) = 
    fs.Close()
    fs.Dispose();

