namespace NaiveDb.Tests
open Xunit
open NaiveDb
open System.IO

module Assert =
    let isNone r =
        match r with
        | Some _ -> false
        | None -> true

module SliceTests =
    [<Fact>]
    let ``tryRead should return None if end of stream``() =
        let ms = new MemoryStream();
        let result = Slice.tryRead ms
        Assert.True (Assert.isNone result)

    [<Fact>]
    let ``tryRead should return Some if enough data available``() =
        let ms = new MemoryStream();
        let data = Slice.toBytes "some_key" "some_value"
        ms.Write(data, 0, data.Length)
        ms.Position <- 0L

        let result = Slice.tryRead ms
        
        Assert.False (Assert.isNone result)