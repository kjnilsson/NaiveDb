namespace NaiveDb

open System.IO

type SliceData = {
    Key : byte[]
    Value : byte[]
}
    with member this.TotalLength =
            this.Key.Length + this.Value.Length

module Slice =
    
    let (|Number|_|) (t:int) (a:int) =
        if(t = a) then Some a
        else None
            
    let (|Less|_|) (t:int) (a:int) =
        if(t > a) then Some a
        else None
            
    let private read (s:Stream) n =
        let buffer = Array.zeroCreate n
        match s.Read(buffer, 0, n) with
        | Number n x -> Some buffer
        | _ -> None
        
    let private readInt32 (s:Stream) =
        match read s 4 with
        | Some buf -> Some (System.BitConverter.ToInt32 (buf, 0))
        | _ -> None

    let private getStrBytes (key:string) = System.Text.Encoding.UTF8.GetBytes key

    let private getIntBytes (n:int) = System.BitConverter.GetBytes n
    
    let private strToBytes s = 
        let bytes = getStrBytes s
        let lenBytes = Array.length bytes |> getIntBytes
        (lenBytes, bytes)
            
    let private serialize key value =
        let squash (a,b) (c,d) = [a;b;c;d]
        let key' = strToBytes key
        let value' = strToBytes value
        squash key' value'
        |> Array.concat
   
    let private readPart (s:Stream) =
        match readInt32 s with
        | Some len ->
            let buffer = Array.zeroCreate len
            read s len
        | None -> None

    let tryRead (s:Stream) =
        let key = readPart s
        let value = readPart s
        match (key, value) with
        | (Some k, Some v) -> Some { Key = k; Value = v }
        | _ -> None

    let toBytes k v =
        serialize k v