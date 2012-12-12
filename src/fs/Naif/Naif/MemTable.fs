module MemTable
open System.IO
open System
open System.Text

type MemTable =
    { Cache : Map<string, string> }
    
let mutable mt = { Cache = Map.empty }

let mutable fs : FileStream = null

let openFile s =
    if(fs = null) then
        fs <- File.OpenWrite s
    else
        fs.Close()
        fs <- File.Open(s, FileMode.Create)

let logWrite pair =
    let write bytes = fs.Write(bytes, 0, bytes.Length)
    let getBytes s = Encoding.UTF8.GetBytes(s : string) : byte[]
    
    match pair with
    | (key:string, value:string) ->
        try
            let keyBytes = getBytes key
            let valueBytes = getBytes value

            let all = seq {
                yield BitConverter.GetBytes keyBytes.Length 
                yield keyBytes
                yield BitConverter.GetBytes valueBytes.Length
                yield valueBytes
            } 

            let allb = Array.concat all

            allb |> write     

            fs.Flush()
            Some()
        with
            | ex -> None

let addToMemTable (key, value) =
    mt <- { mt with Cache =  mt.Cache.Add(key, value) }    
    logWrite (key, value)

let closeFile() = 
    fs.Close()
    fs.Dispose();

