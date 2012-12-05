using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NaiveDb
{
    public interface INaiveDb<T> where T: class
    {
        void Put(string key, T value);
        T Get(string key);
        void Delete(string key);
    }
}
