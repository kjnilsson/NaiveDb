module Maybe

type MaybeBuilder() =
    member this.Bind(m, f) = Option.bind f m
    member this.Return m = Some m
    member this.ReturnFrom x = x

let maybe = MaybeBuilder()

type FirstBuilder() =
    let bind f m =
        match m with
        | Some x -> m
        | None -> f m
         
    member this.Bind(m, f) = bind f m
    member this.Return m = None


let first = new FirstBuilder()

let testMaybe i =
    if( i < 0) then
        None
    else
        Some i

first {
    let! x = testMaybe -2
    let! y = testMaybe 3
    return y

}
    

