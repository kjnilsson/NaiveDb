#r @"C:\code\github\kjnilsson\NaiveDb\src\NaiveDb\packages\FSharpx.Core.1.7.8\lib\40\FSharpx.Core.dll"

open System.IO
open FSharpx.Collections

let first = LazyList.ofSeq(seq { 0..3..30 } |> Seq.map (fun x -> (x, "first") ))
let second = LazyList.ofSeq(seq { 0..2..30 }  |> Seq.map (fun x -> (x, "second")))
let third = LazyList.ofSeq(seq { 0..30 }  |> Seq.map (fun x -> (x, "third")))

module List =
    let contains x (list:List<'T>) = 
        let rec inner l =
            match l with
            | h :: t -> if (h = x) then true
                        else inner t
            | _ -> false
        inner list

module LazyList =
    let takeWhileAsList pred (list:LazyList<'T>) =
        let rec inner l (r:List<'T>) =
                match l with
                | (LazyList.Cons(h,t)) -> 
                    if (pred h) then
                        inner t (h :: r)
                    else 
                        (List.rev r, l)
                | _ -> (List.rev r, l)
        inner list List.empty 

    let takeTail count (list:LazyList<'T>) =
        let rec inner l (r:List<'T>) c =
                match l with
                | (LazyList.Cons(h,t)) -> 
                    if (c < count) then
                        inner t (h :: r) (c + 1)
                    else 
                        (List.rev r, l)
                | _ -> (List.rev r, l)
        inner list List.empty 0

    //merges two sorted sequences of key value pairs in batches
    let mergeSorted a b =
        let rec inner a' b' =
            LazyList.ofSeq( seq {
                match takeTail 25 a' with
                | ([], _) -> yield! b'
                | (f, tail_a) -> 
                    let (flastkey, _) = f.[f.Length - 1]
                
                    let (s, tail_b) = takeWhileAsList (fun (key, _) -> key <= flastkey) b'
                    //filter s not to contain keys in f
                    let fkeys = List.map (fun (k, _) -> k) f
                    let s' = List.filter (fun (k, _) -> (List.contains k fkeys) = false) s
                
                    yield! List.append f s' |> List.sortBy (fun (key, _) -> key)
                    yield! inner tail_a tail_b 
                })

        inner a b

    let mergeMany (lists:List<LazyList<_>>) =
        List.reduce (fun a x -> mergeSorted a x) lists
    


LazyList.mergeMany [first;second;third]
|> Seq.toList
|> (printfn "%A")

LazyList.mergeSorted first <| LazyList.mergeSorted second third
|> Seq.toList
|> (printfn "%A")

#load "Slice.fs"
open NaiveDb
open System.IO

//SSTable? 
let readData fn =
    let rec readLoop fs =
        seq {
            match Slice.tryRead fs with
            | Some sd -> 
                yield sd
                yield! readLoop fs
            | None -> fs.Close() }
    File.OpenRead fn |> readLoop

let guid() =
    System.Guid.NewGuid().ToString()

let randSlice i =
    let k = guid();
    let v = guid() + guid();
    Slice.toBytes k v

let createTestFile name =
    use f = File.OpenWrite (Path.Combine(@"c:\dump\", name))
    Seq.init 100 randSlice
    |> Seq.sort
    |> Seq.iter (fun x -> f.Write(x, 0, x.Length))



let createTestFiles n =
    for i = 0 to n do
        createTestFile (sprintf "test%i.dat" i)

createTestFiles 10

readData @"c:\dump\data.dat"

let readDataAsLazyList fn = LazyList.ofSeq (readData fn)
                            |> LazyList.map (fun s -> (s.Key, s.Value))

let toStr bytes =
    System.Text.Encoding.UTF8.GetString bytes

let writeLazyList fn list =
    use f = File.OpenWrite fn
    list
    |> LazyList.map (fun (k, v) -> Slice.toBytes (toStr k) (toStr v))
    |> LazyList.iter (fun b -> f.Write(b, 0, b.Length))
    


Directory.GetFiles(@"c:\dump\", "test*")
|> Array.toList
|> List.map readDataAsLazyList 
|> LazyList.mergeMany
|> (writeLazyList @"c:\dump\test_level1.dat")


readData @"c:\dump\test_level1.dat"
|> Seq.iter (printfn "%A")