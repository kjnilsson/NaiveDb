// Learn more about F# at http://fsharp.net. See the 'F# Tutorial' project
// for more guidance on F# programming.

#load "Library1.fs"
#load "MemTable.fs"
open Naif
//open MemTable
open System
open System.IO
open MemTable

let put (k, v, mt:MemTable.MemTable) =
    let m = mt.add(k, v)
    if (m.Size > 4096L) then
        m.close()
        m.complete()
        printf "sstable to be created"
        MemTable.openFile @"c:\dump\naif\mm14.log"
    else
        m

let mutable mt = MemTable.openFile @"c:\dump\naif\mm14.log"
//File.OpenRead @"c:\dump\naif\mm10.log"
mt <- put (Guid.NewGuid().ToString(), "123412341234", mt)
//mt <- mt.add ("ruth", "7803253514")
//mt <- mt.add ("jasper", "0903253514")
printfn "%A" mt.File.Name
mt.close ()

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