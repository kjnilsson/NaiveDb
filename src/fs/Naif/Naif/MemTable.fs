module MemTable
open System.IO
open System
open System.Text

type MemTable =
    { Cache : Map<string, string> }
    
let mutable private mt = { Cache = Map.empty }

let mutable private fs : FileStream = null

let openFile s =
    if(fs = null) then
        fs <- File.OpenWrite s
    else
        fs.Close()
        fs <- File.Open(s, FileMode.Create)


let private getBytes s = Encoding.UTF8.GetBytes(s : string) : byte[]

let private logWrite pair =
    let write bytes = fs.Write(bytes, 0, bytes.Length)
    
    
    match pair with
    | (key:string, value:string) ->
        try
            let all = seq {
                let keyBytes = getBytes key
                let valueBytes = getBytes value
                yield BitConverter.GetBytes keyBytes.Length 
                yield keyBytes
                yield BitConverter.GetBytes valueBytes.Length
                yield valueBytes
            } 

            let allBytes = Array.concat all

            allBytes |> write     

            fs.Flush()
            
            Some(allBytes.Length)
        with
            | ex -> None


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


let addToMemTable (key, value) =
    mt <- { mt with Cache =  mt.Cache.Add(key, value) }    
    match logWrite (key, value) with
    | Some n -> 
        printfn "written %A " n
        if(n > 1024 * 4) then
            printf "big"
            let cache = mt.Cache
            mt <- { mt with Cache = Map.empty }
            //write cache to disk
            //Map.map (fun (k, v) -> toBytes (k, v)) 
            cache
            |> Map.toList
            |> List.map (fun (k, v) -> toBytes (k, v)) 
            |> Array.concat
            |> (fun x -> File.WriteAllBytes (@"c:\dump\naif\sstable1.sst", x))

            //ignore // create sstable from cache
        
    | None -> printf "could not write to log"

let closeFile() = 
    fs.Close()
    fs.Dispose();

let get(key) =
    mt.Cache.TryFind key
