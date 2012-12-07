using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Naive
{
    public interface INaiveDb<T> where T: class
    {
        void Put(string key, T value);
        T Get(string key);
        void Delete(string key);
        void Close();
    }

    public class NaiveDb
    {
        public static INaiveDb<T> New<T>(DirectoryInfo directory) where T : class
        {
            return new NaiveDbImpl<T>(directory);
        }

        internal class NaiveDbImpl<T> : INaiveDb<T> where T : class
        {
            private FileStream fs;

            internal NaiveDbImpl(DirectoryInfo directory)
            {
                this.fs =  File.Create(Path.Combine(directory.FullName, "naive.db"));
            }

            public void Put(string key, T value)
            {
                throw new NotImplementedException();
            }

            public T Get(string key)
            {
                throw new NotImplementedException();
            }

            public void Delete(string key)
            {
                throw new NotImplementedException();
            }

            public void Close()
            {
                fs.Close();
                fs.Dispose();
            }
        }
    }

    internal class MemTable
    {
    }

    internal class SSTable
    {
    }
}
