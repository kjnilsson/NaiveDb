// Learn more about F# at http://fsharp.net. See the 'F# Tutorial' project
// for more guidance on F# programming.

#load "Library1.fs"
#load "MemTable.fs"
open Naif
//open MemTable

MemTable.openFile @"c:\dump\naif\mm1.log"
MemTable.addToMemTable("karl", "nilsson")
MemTable.addToMemTable("hey", "ho")
MemTable.addToMemTable("ban", "ana")
MemTable.closeFile()


let b = Array.init 256 (fun i -> 0uy) //byte suffix

let gb s = System.Text.Encoding.UTF8.GetBytes(s : string) : byte[]

let arrs = seq {
        yield gb "laskjdflsdkfj"
        yield gb "karladjflkasdf"
        yield gb "asdfasdfasdf"
    }

Array.concat arrs


let b1 = System.Text.Encoding.UTF8.GetBytes("blah ha")
Array.blit b1 0 b 0 b1.Length

let a22 = [|gb "asdf"; gb "karl"|]

Array.concat [gb "asdf"; gb "karl"]