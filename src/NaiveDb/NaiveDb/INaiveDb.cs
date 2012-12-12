using NaiveDb;
using Newtonsoft.Json;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Naive
{
    public interface INaiveDb<T> : IDisposable where T: class
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
            
            private string folder;
            private bool disposed;
            private MemTable currentMemtable;
            private BlockingCollection<WriteTask> writeTasks = new BlockingCollection<WriteTask>(10);
            private Task writer;

            internal NaiveDbImpl(DirectoryInfo directory)
            {
                this.folder = Path.Combine(directory.FullName, typeof(T).Name.ToLower());
                Directory.CreateDirectory(folder);
                
                this.currentMemtable = new MemTable(folder.ToString());

                this.writer = Task.Factory.StartNew(() =>
                {
                    foreach (var t in writeTasks.GetConsumingEnumerable())
                    {
                        this.currentMemtable.Write(t.Key, t.Value);
                    }
                }, TaskCreationOptions.LongRunning);
            }

            public void Put(string key, T value)
            {
                var payLoad = Serialize(value);
                writeTasks.Add(new WriteTask(key, payLoad));
            }

            class WriteTask
            {
                public string Key { get; set; }
                public string Value { get; set; }

                public WriteTask(string key, string value)
                {
                    this.Key = key;
                    this.Value = value;
                }
            }

            private string Serialize(T value)
            {
                return JsonConvert.SerializeObject(value, 
                    new JsonSerializerSettings 
                    { 
                        TypeNameHandling = TypeNameHandling.Auto 
                    });
            }

            public T Get(string key)
            {
                var stringValue = this.currentMemtable.Read(key);
                return JsonConvert.DeserializeObject<T>(stringValue);
            }

            public void Delete(string key)
            {
                throw new NotImplementedException();
            }

            public void Close()
            {
                this.Dispose();
            }

            public void Dispose()
            {
                if (disposed) return;

                this.disposed = true;
            
                this.currentMemtable.Dispose();
            }
        }
    }

}
