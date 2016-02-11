using System;
using System.Collections.Generic;
using System.Linq;

namespace TheWheel.Domain
{
    public class Bag<TKey, TValue> : IDictionary<TKey, TValue>
    {
        public Bag()
            : this(8)
        {

        }

        public Bag(int initialCapacity)
        {
            list = new KeyValuePair<TKey, TValue>[initialCapacity];
        }

        KeyValuePair<TKey, TValue>[] list;
        KeyValuePair<TKey, TValue> lastAccessed;
        private int count;
        private IEqualityComparer<TKey> comparer = EqualityComparer<TKey>.Default;

        public void Add(TKey key, TValue value)
        {
            Add(new KeyValuePair<TKey, TValue>(key, value));
        }

        public bool ContainsKey(TKey key)
        {
            var hashcode = GetPreferredIndex(key);
            lock (list)
            {
                for (int i = 0; i < list.Length; i++)
                {
                    if (comparer.Equals(list[(i + hashcode) % list.Length].Key, key))
                        return true;
                }
            }
            return false;
        }

        public ICollection<TKey> Keys
        {
            get { return list.Where(kvp => comparer.Equals(kvp.Key, default(TKey))).Select(kvp => kvp.Key).ToList(); }
        }

        public bool Remove(TKey key)
        {
            var hashcode = GetPreferredIndex(key);
            lock (list)
            {
                for (int i = 0; i < list.Length; i++)
                {
                    if (comparer.Equals(list[(i + hashcode) % list.Length].Key, key))
                    {
                        list[(i + hashcode) % list.Length] = default(KeyValuePair<TKey, TValue>);
                        if (comparer.Equals(lastAccessed.Key, key))
                            lastAccessed = default(KeyValuePair<TKey, TValue>);
                        return true;
                    }
                }
            }
            return false;
        }

        public bool TryGetValue(TKey key, out TValue value)
        {
            if (comparer.Equals(lastAccessed.Key, key))
            {
                value = lastAccessed.Value;
                return true;
            }
            var hashcode = GetPreferredIndex(key);
            lock (list)
            {
                for (int i = 0; i < list.Length; i++)
                {
                    if (comparer.Equals(list[(i + hashcode) % list.Length].Key, key))
                    {
                        value = list[(i + hashcode) % list.Length].Value;
                        return true;
                    }
                }
            }
            value = default(TValue);
            return false;
        }

        private int GetPreferredIndex(TKey key)
        {
            var hashcode = (key.GetHashCode() % list.Length);
            if (hashcode < 0)
                hashcode += list.Length;
            return hashcode;
        }

        public ICollection<TValue> Values
        {
            get { return list.Where(kvp => comparer.Equals(kvp.Key, default(TKey))).Select(kvp => kvp.Value).ToList(); }
        }

        public TValue this[TKey key]
        {
            get
            {
                TValue value;
                if (TryGetValue(key, out value))
                    return value;
                return default(TValue);
            }
            set
            {
                var hashcode = GetPreferredIndex(key);
                lock (list)
                {
                    for (int i = 0; i < list.Length; i++)
                    {
                        if (comparer.Equals(list[(i + hashcode) % list.Length].Key, key))
                        {
                            list[(i + hashcode) % list.Length] = new KeyValuePair<TKey, TValue>(key, value);
                            return;
                        }
                    }
                }
                Add(key, value);
            }
        }

        public void Add(KeyValuePair<TKey, TValue> item)
        {
            if (item.Key == null)
                throw new ArgumentNullException("key");
            var hashcode = GetPreferredIndex(item.Key);
            lock (list)
            {
                for (int i = 0; i < list.Length; i++)
                {

                    if (!comparer.Equals(list[(i + hashcode) % list.Length].Key, default(TKey)))
                    {
                        if (i < list.Length - 1)
                            continue;

                        var tmp = list;
                        list = new KeyValuePair<TKey, TValue>[list.Length * 2];
                        for (int j = 0; j < tmp.Length; j++)
                        {
                            Add(tmp[j]);
                        }
                        Add(item);
                        lastAccessed = item;
                        return;
                    }
                    list[(i + hashcode) % list.Length] = item;
                    count++;
                    lastAccessed = item;
                    break;
                }
            }
        }

        public void Clear()
        {
            lock (list)
            {
                Array.Clear(list, 0, list.Length);
                lastAccessed = default(KeyValuePair<TKey, TValue>);
                count = 0;
            }
        }

        public bool Contains(KeyValuePair<TKey, TValue> item)
        {
            var hashcode = GetPreferredIndex(item.Key);
            lock (list)
            {
                for (int i = 0; i < list.Length; i++)
                {
                    if (comparer.Equals(list[(i + hashcode) % list.Length].Key, item.Key))
                        return EqualityComparer<TValue>.Default.Equals(list[(i + hashcode) % list.Length].Value, item.Value);
                }
            }
            return false;
        }

        public void CopyTo(KeyValuePair<TKey, TValue>[] array, int arrayIndex)
        {
            Array.Copy(list, 0, array, arrayIndex, list.Length);
        }

        public int Count
        {
            get { return count; }
        }

        public bool IsReadOnly
        {
            get { return false; }
        }

        public bool Remove(KeyValuePair<TKey, TValue> item)
        {
            return Remove(item.Key);
        }

        public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
        {
            for (int i = 0; i < list.Length; i++)
            {
                if (!comparer.Equals(list[i].Key, default(TKey)))
                    yield return list[i];
            }
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}
