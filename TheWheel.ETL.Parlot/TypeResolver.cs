using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace TheWheel.ETL.Parlot
{
    public class TypeResolver
    {
        private List<Func<string, Type>> resolvers = new List<Func<string, Type>>();

        public TypeResolver()
        {
            resolvers.Add((n) =>
            {
                switch (n)
                {
                    case "string":
                        return typeof(string);
                    case "bool":
                        return typeof(bool);
                    case "int":
                        return typeof(int);
                    case "double":
                        return typeof(double);
                    case "float":
                        return typeof(float);
                    case "DateTime":
                        return typeof(DateTime);
                    case "long":
                        return typeof(long);
                    case "ulong":
                        return typeof(ulong);
                    case "uint":
                        return typeof(uint);
                    case "ushort":
                        return typeof(ushort);
                    case "char":
                        return typeof(char);
                    case "short":
                        return typeof(short);
                    default:
                        return null;
                }
            });
        }

        public Type Get(string v)
        {
            foreach (var resolver in resolvers)
            {
                var type = resolver(v);
                if (type != null)
                    return type;
            }
            return Type.GetType(v);
        }

        public void Register(Func<string, Type> resolver)
        {
            resolvers.Add(resolver);
        }
        public void Register(string name, Type type)
        {
            resolvers.Add(n => n == name ? type : null);
        }
        public void Register<T>(string name)
        {
            Register(name, typeof(T));
        }
        public void Register<T>()
        {
            Register(typeof(T).Name, typeof(T));
        }
        public void Register(Dictionary<string, Type> staticRegistration)
        {
            resolvers.Add(n => { staticRegistration.TryGetValue(n, out var result); return result; });
        }
        public void Using(Assembly assembly, string ns)
        {
            Register(assembly.GetTypes().Where(t => t.Namespace != null && t.Namespace.StartsWith(ns)).ToDictionary(t => t.Name, t => t));
        }
        public void Using(string ns)
        {
            Register(name =>
            {
                if (name.StartsWith(ns))
                    return null;
                return Get(ns + '.' + name);
            });
        }
        public void UsingAll<T>(Assembly assembly)
        {
            Register(assembly.GetTypes().Where(t => typeof(T).IsAssignableFrom(t) && !t.IsGenericType).ToDictionary(t => t.Name, t => t));
        }
        public void UsingAll(Assembly assembly)
        {
            Register(assembly.GetTypes().Where(t => t.Name[0] != '<' && !t.IsGenericType).ToDictionary(t => t.Name, t => t));
        }
        public void UsingAll(Assembly assembly, Type iface)
        {
            Register(assembly.GetTypes().Where(t => IsAssignableToGeneric(iface, t)).ToDictionary(t => t.Name, t => t));
        }

        private static bool IsAssignableToGeneric(Type iface, Type t)
        {
            if (t == null)
                return false;
            if (t.GetInterfaces().Any(i => i == iface || i.IsConstructedGenericType && i.GetGenericTypeDefinition() == iface && IsAssignableToGeneric(iface, i)))
                return true;
            return t != typeof(object) && (t.BaseType == t || IsAssignableToGeneric(iface, t.BaseType));
        }
    }
}