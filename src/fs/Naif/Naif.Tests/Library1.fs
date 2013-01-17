module Naif.Tests

open NUnit.Framework
open Xunit


    
    [<Fact>]
let should() = 
    let sut = "hello"
    Assert.Equal<string>("hello", sut)