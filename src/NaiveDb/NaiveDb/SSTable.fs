namespace NaiveDb

module SSTable =
    type Table = {
        FileName: string
        Level: int32
        StartKey: byte[]
        EndKey: byte[]
    }
        

