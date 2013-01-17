#load "Library1.fs"
#load "MemTable.fs"
#load "SsTable.fs"

open System
open System.IO
open MemTable
open SsTables

let mutable tables = SsTables.openTables @"c:\dump\naif\"


let put (k, v, mt:MemTable.MemTable) =
    let m = mt.add(k, v)
    if (m.Size > 4096L) then
        m.close()
        printf "sstable to be created"
        tables <- SsTables.add (m, @"c:\dump\naif\","sstbl1") :: tables
        let old = m.complete ()
        (MemTable.openFile @"c:\dump\naif\mt.log", old)
    else
        (m, m)

let mutable mt = MemTable.openFile @"c:\dump\naif\mt.log"
let mutable old = mt

let (c, o) = put (Guid.NewGuid().ToString(), "123412dfasqwerqwerwqerwerwqwerdfsfdggar4a341234", mt)

mt <- c 
old <- o

printfn "%A" old.File.Name

mt.close ()
List.iter (fun (i:SsTable) -> i.File.Close()) tables

let m1 = Map.empty
m1.Add(123, 123)
m1.Add(124, 124)

let b = Array.init 256 (fun i -> 0uy) //byte suffix

let gb s = System.Text.Encoding.UTF8.GetBytes(s : string) : byte[]

let arrs = seq {
        yield gb "laskjdflsdkfj"
        yield gb "karladjflkasdf"
        yield gb "asdfasdfasdf"
    }

Array.concat arrs
open System
open System.IO
open System.ComponentModel
open System.Text

let toInt b = System.BitConverter.ToInt32 (b, 0)

let mutable map = Map.empty
map <- map.Add("b", "test")
map <- map.Add("a", "test")
map


open System.IO

let fs = File.Open( @"c:\dump\naif\mm1.log", FileMode.Open)
let _m = readLog fs
fs.Close()
fs.Dispose();
_m

let b1 = System.Text.Encoding.UTF8.GetBytes("blah ha")
Array.blit b1 0 b 0 b1.Length

let a22 = [|gb "asdf"; gb "karl"|]

Array.concat [gb "asdf"; gb "karl"]

let m' = Map.empty<string, string>
let m'' = m'.Add ("key", "value")

Map.map (fun k (v:string) -> v.ToUpper()) m''

let m3 = Map.toList (m''.Add ("yp", "bp"))

let rec write (list, offset) =
    match list with
    | (key, value:string)::tail -> 
        printfn "%A %A %A" key value offset
        write (tail, offset + value.Length)
    | _ -> printfn "nothig"

write (m3, 0)
