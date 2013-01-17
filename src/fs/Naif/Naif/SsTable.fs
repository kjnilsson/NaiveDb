module SsTables

open System
open System.IO

open MemTable

let toInt b = System.BitConverter.ToInt32 (b, 0)

let readInt (fs:FileStream) = 
    let bytes = Array.init 4 (fun i -> 0uy)
    fs.Read(bytes, 0, 4) |> ignore
    toInt bytes

let readString (fs:FileStream, length) =
    let bytes = Array.init length (fun i -> 0uy)
    let read = fs.Read(bytes, 0, length)
    if(read <> length) then
        None
    else
        Some (System.Text.Encoding.UTF8.GetString bytes)
//let readBytes

type SsTable =
    { Index : Map<string, int64>; File : FileStream }
    member this.get key =
        let offset = this.Index.TryFind key
        let seeked = this.File.Seek(offset.Value, SeekOrigin.Begin)
        let bytes = Array.init 4 (fun i -> 0uy)
        this.File.Read(bytes, 0, 4) |> ignore
        let length = toInt bytes
        let bytes = Array.init length (fun i -> 0uy)
        this.File.Read(bytes, 0, length) |> ignore
        System.Text.Encoding.UTF8.GetString bytes

    interface System.IDisposable with
        member this.Dispose() =
            this.File.Close()
            this.File.Dispose()

let strToBytes (str:string) =
    System.Text.Encoding.UTF8.GetBytes str

let toBytes (n:int) = 
    System.BitConverter.GetBytes n

open MemTable

let readIndex path =
    Map.empty

let add (mt:MemTable, path, name) =
    let sstFilePath = Path.Combine(path, name + ".sst")
    let indexPath = Path.Combine(path, name + ".index")
    use fsindex = File.OpenWrite(indexPath)
    use fstable = File.OpenWrite(sstFilePath)

    let cache = Map.map (fun k v -> (strToBytes v)) mt.Cache

    let rec write (list, offset) =
        match list with
        | (key, value:byte[])::tail -> 
            //write key
            let key' = strToBytes key
            fsindex.Write(key', 0, key'.Length)
            //write data
            fstable.Write(value, 0, value.Length)
            //write offset
            let off = toBytes offset
            fsindex.Write(off, 0, off.Length)
            //recurse
            write (tail, offset + value.Length)
        | _ -> ()

    write (Map.toList cache, 0)
    
    fsindex.Close()
    fstable.Close()

    //return sstable
    { Index = readIndex indexPath; File = File.OpenRead sstFilePath }

let openTables path = 
    List.empty<SsTable>
//
//let openRead (mt:MemTable) = 
//    //let mutable index = Map.empty<string, int64>
//    let fs = File.OpenRead(mt.File.Name)
//    let rec read (index:Map<string, int64>) =
//        let l = readInt fs
//        match readString (fs, l) with
//        | None -> index
//        | Some x -> 
//            read index.Add ()
//    let index = read Map.empty<string, int64>
//
//        
//    //write data len prefixed
//    //write index    
//    { Index = Map.empty<string, int64>; File = File.Create(@"c:\dfa") }
     

