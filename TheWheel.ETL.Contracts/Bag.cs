using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;

namespace TheWheel.ETL.Contracts
{
    public class Bag<TKey1, TKey2, TValue> : Bag<TKey1, Bag<TKey2, TValue>>
    {

    }

    public class Bag<TKey1, TKey2, TKey3, TValue> : Bag<TKey1, Bag<TKey2, Bag<TKey3, TValue>>>
    {

    }

    public class Bag<TKey, TValue> : IDictionary<TKey, TValue>
    {
        public Bag()
            : this(8)
        {

        }

        public Bag(int initialCapacity)
        {
            if (initialCapacity == 0)
                initialCapacity = 8;
            list = new KeyValuePair<TKey, TValue>[initialCapacity];
            hashList = new ulong[initialCapacity];
        }

        public Bag(IDictionary<TKey, TValue> init)
            : this(init.Count)
        {
            foreach (KeyValuePair<TKey, TValue> kvp in init)
                Add(kvp);
        }
        public Bag(Bag<TKey, TValue> init)
        {
            list = (KeyValuePair<TKey, TValue>[])init.list.Clone();
            hashList = (ulong[])init.hashList.Clone();
            count = init.count;
        }
        public Bag(IEnumerable<KeyValuePair<TKey, TValue>> init)
        {
            list = init.ToArray();
            hashList = list.Select(kvp => Hash(kvp.Key)).ToArray();
            count = list.Length;
        }

        public Bag<TKey, TValue> Clone()
        {
            return new Bag<TKey, TValue>(this);
        }

        public int Capacity => hashList.Length;

        private static readonly TKey NULLKey = default(TKey);
        private KeyValuePair<TKey, TValue>[] list;
        private ulong[] hashList;
        private KeyValuePair<TKey, TValue> lastAccessed;
        private int count;
        private static IEqualityComparer<TKey> comparer = EqualityComparer<TKey>.Default;
        private ReaderWriterLockSlim locker = new ReaderWriterLockSlim(LockRecursionPolicy.SupportsRecursion);
        private const ulong hashOffset = 14695981039346656037;
        private const ulong hashPrime = 1099511628211;

        public void Add(TKey key, TValue value)
        {
            Add(new KeyValuePair<TKey, TValue>(key, value));
        }

        public TValue AddIfNotExists(TKey key, Func<TValue> valueGetter)
        {
            var hash = Hash(key);
            var shouldLock = !locker.IsReadLockHeld;
            locker.EnterReadLock();
            TValue value;
            try
            {
                if (TryGetValueWithHash(hash, out value))
                    return value;
            }
            finally
            {
                locker.ExitReadLock();
            }

            locker.EnterUpgradeableReadLock();
            try
            {
                if (!TryGetValueWithHash(hash, out value))
                {
                    Add(new KeyValuePair<TKey, TValue>(key, value = valueGetter()), hash);
                }
                return value;
            }
            finally
            {
                locker.ExitUpgradeableReadLock();
            }
        }

        public bool ContainsKey(TKey key)
        {
            return TryGetValue(key, out TValue value);
        }

        public ICollection<TKey> Keys => list.Where(kvp => !comparer.Equals(kvp.Key, NULLKey)).Select(kvp => kvp.Key).ToList();

        public TValue Remove(TKey key)
        {
            locker.EnterUpgradeableReadLock();
            var keyHash = Hash(key);
            var offset = GetPreferredIndex(keyHash);
            for (uint i = 0; i < list.LongLength; i++)
            {
                var hashIndex = (i + offset) % (ulong)list.LongLength;
                if (hashList[hashIndex] == keyHash)
                {
                    locker.EnterWriteLock();
                    KeyValuePair<TKey, TValue> value = list[hashIndex];
                    list[hashIndex] = default(KeyValuePair<TKey, TValue>);
                    hashList[hashIndex] = 0;
                    count--;
                    Reorganize((long)hashIndex);
                    locker.ExitWriteLock();
                    locker.ExitUpgradeableReadLock();
                    return value.Value;
                }
                if (hashList[hashIndex] == 0)
                    break;
            }

            locker.ExitUpgradeableReadLock();
            return default(TValue);
        }

        private void Reorganize(long hashIndex)
        {
            long touchedIndex = -1;
            do
            {
                if (touchedIndex != -1)
                {
                    hashIndex = touchedIndex;
                    touchedIndex = -1;
                }
                for (uint i = 1; i < hashList.LongLength; i++)
                {
                    var iHash = (long)(i + hashIndex) % hashList.LongLength;
                    if (hashList[iHash] == 0)
                    {
                        if (touchedIndex == iHash)
                            continue;
                        break;
                    }
                    if (touchedIndex == -1 && GetEmptyIndex(hashList[iHash]) == hashIndex)
                    {
                        hashList[hashIndex] = hashList[iHash];
                        list[hashIndex] = list[iHash];
                        list[iHash] = default(KeyValuePair<TKey, TValue>);
                        hashList[iHash] = 0;
                        // hashIndex = (ulong)iHash;
                        touchedIndex = iHash;
                    }
                }
            }
            while (touchedIndex != -1);
        }

        bool IDictionary<TKey, TValue>.Remove(TKey key)
        {
            return EqualityComparer<TValue>.Default.Equals(Remove(key), default(TValue));
        }

        public bool TryGetValues(TKey key, out IList<TValue> values)
        {
            var keyHash = Hash(key);
            values = new List<TValue>();
            for (uint i = 0; i < list.LongLength; i++)
            {
                if (hashList[i] == keyHash)
                {
                    values.Add(list[i].Value);
                }
            }

            return values.Count > 0;
        }

        private bool TryGetValueWithHash(ulong keyHash, out TValue value)
        {
            locker.EnterReadLock();
            var index = GetPreferredIndex(keyHash);
            try
            {
                for (uint i = 0; i < list.LongLength; i++)
                {
                    var hashIndex = (i + index) % (ulong)hashList.LongLength;
                    if (hashList[hashIndex] == 0)
                    {
                        value = default(TValue);
                        return false;
                    }
                    if (hashList[hashIndex] == keyHash)
                    {
                        value = list[hashIndex].Value;
                        return true;
                    }
                }

                value = default(TValue);
                return false;
            }
            finally
            {
                locker.ExitReadLock();
            }
        }

        public bool TryGetValue(TKey key, out TValue value)
        {
            return TryGetValueWithHash(Hash(key), out value);
        }

        internal static ulong Hash(TKey key)
        {
            var stringKey = key as string;
            if (stringKey != null)
            {
                var hash = hashOffset;
                var tmp = new byte[(stringKey.Length + 1) * 2];
                unsafe
                {
                    fixed (void* ptr = stringKey)
                    {
                        Marshal.Copy(new IntPtr(ptr), tmp, 0, tmp.Length);
                    }
                }

                //FNV-1 hash
                for (var i = 0; i < tmp.Length; i++)
                {
                    hash *= hashPrime;
                    hash ^= tmp[i];
                }



                return hash;
                //var hashText = new SHA256CryptoServiceProvider().ComputeHash(bytes);
                //uint hashCodeStart = BitConverter.ToUInt32(hashText, 0);
                //uint hashCodeMedium = BitConverter.ToUInt32(hashText, 8);
                //uint hashCodeEnd = BitConverter.ToUInt32(hashText, 24);
                //return hashCodeStart ^ hashCodeMedium ^ hashCodeEnd;
            }
            return Convert.ToUInt64(key.GetHashCode());
        }

        private ulong GetPreferredIndex(TKey key)
        {
            return GetPreferredIndex(Hash(key));
        }

        private ulong GetPreferredIndex(ulong hash)
        {
            var hashcode = (hash % (ulong)list.LongLength);
            if (hashcode < 0)
                hashcode += (ulong)list.LongLength;

            return hashcode;
        }

        private long GetEmptyIndex(ulong hash)
        {
            var index = GetPreferredIndex(hash);
            for (uint i = 0; i < list.Length; i++)
            {
                long correctIndex = (long)(i + index) % list.LongLength;
                if (!comparer.Equals(list[correctIndex].Key, NULLKey))
                {
                    if (i < list.Length - 1)
                        continue;
                    break;
                }
                return correctIndex;
            }
            return -1;
        }

        public ICollection<TValue> Values => list.Where(kvp => !comparer.Equals(kvp.Key, NULLKey)).Select(kvp => kvp.Value).ToList();

        public TValue this[TKey key]
        {
            get
            {
                if (TryGetValue(key, out TValue value))
                    return value;
                return default(TValue);
            }
            set
            {
                var keyHash = Hash(key);
                locker.EnterUpgradeableReadLock();
                var hashcode = GetPreferredIndex(keyHash);
                try
                {
                    for (uint i = 0; i < list.Length; i++)
                    {
                        if (comparer.Equals(list[(i + hashcode) % (ulong)list.LongLength].Key, key))
                        {
                            locker.EnterWriteLock();
                            try
                            {
                                list[(i + hashcode) % (ulong)list.Length] = new KeyValuePair<TKey, TValue>(key, value);
                                hashList[(i + hashcode) % (ulong)list.Length] = keyHash;
                            }
                            finally
                            {
                                locker.ExitWriteLock();
                            }
                            return;
                        }
                    }
                    Add(key, value);
                }
                finally
                {
                    locker.ExitUpgradeableReadLock();
                }
            }
        }

        public void Resize(int size)
        {
            locker.EnterWriteLock();

            try
            {
                KeyValuePair<TKey, TValue>[] tmp = list;
                var tmpHash = hashList;
                if (size == Capacity + 1)
                    size = Capacity * 2;
                list = new KeyValuePair<TKey, TValue>[size];
                hashList = new ulong[list.Length];
                count = 0;
                for (var j = 0; j < tmp.Length; j++)
                {
                    if (tmpHash[j] != 0)
                        Add(tmp[j], tmpHash[j]);
                }
                return;
            }
            finally
            {
                locker.ExitWriteLock();
            }
        }

        private void Add(KeyValuePair<TKey, TValue> item, ulong hash)
        {
            if (item.Key == null)
                throw new ArgumentNullException("key");
            locker.EnterWriteLock();
            var index = GetEmptyIndex(hash);
            try
            {
                if (index == -1)
                {
                    Resize(list.Length * 2);
                    index = GetEmptyIndex(hash);
                }
                list[index] = item;
                hashList[index] = hash;
                count++;
                lastAccessed = item;
            }
            finally
            {
                locker.ExitWriteLock();
            }
        }

        public void Add(KeyValuePair<TKey, TValue> item)
        {
            Add(item, Hash(item.Key));
        }

        public void Clear()
        {
            locker.EnterWriteLock();
            try
            {
                Array.Clear(list, 0, list.Length);
                Array.Clear(hashList, 0, hashList.Length);
                count = 0;
            }
            finally
            {
                locker.ExitWriteLock();
            }

        }

        public bool Contains(KeyValuePair<TKey, TValue> item)
        {
            if (TryGetValue(item.Key, out TValue value))
                return EqualityComparer<TValue>.Default.Equals(value, item.Value);

            return false;
        }

        public void CopyTo(KeyValuePair<TKey, TValue>[] array, int arrayIndex)
        {
            locker.EnterReadLock();
            Array.Copy(list, 0, array, arrayIndex, list.Length);
            locker.ExitReadLock();

        }

        public int Count => count;

        internal int Length => list.Length;

        public bool IsReadOnly => false;

        public bool Remove(KeyValuePair<TKey, TValue> item)
        {
            return Remove(item.Key) != null;
        }

        public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
        {
            locker.EnterReadLock();
            try
            {
                for (var i = 0; i < list.Length; i++)
                {
                    if (!comparer.Equals(list[i].Key, NULLKey))
                        yield return list[i];
                }
            }
            finally
            {
                locker.ExitReadLock();
            }
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}
