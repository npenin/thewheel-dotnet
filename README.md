[![Build status](https://ci.appveyor.com/api/projects/status/jdyhx1rt11slbla9)](https://ci.appveyor.com/project/npenin/thewheel-dotnet)

thewheel-dotnet
===============

Stop reinventing the wheel. It's here ;)

Lamda features
===============

Helps you to build lambda expression easily.

- And or Or on constraints `(a=> a.X).And(b=>b.Y)`
- Combine Dto building
- Dynamically access properties or properties path on objects `o.Property("Property1.Property2")`

## Simple And/Or between 2 lambda

How is it to merge the following conditions ?
```cs
Expression<Func<A,bool>> condition1=a=>a.Property1=="toto";
Expression<Func<A,bool>> condition2=a=>a.Property2=="titi";
```

That simple :
```cs
    var condition=condition1.And(condition2);
```

## Combine Dto building

Let's assume you have the following class :
```cs
public class Person
{
    public static Expression<Func<User, Person>> ToList=u=>new Person
    {
      Id=u.Id,
      FullName=u.FullName,
      Mail=u.Mail,
    };
    
    public static Expression<Func<User, Person>> ToDetails=u=>new Person
    {
      Id=u.Id,
      FullName=u.FullName,
      Mail=u.Mail,
      Phone=u.Phone,
      FirstName=u.FirstName,
      LastName=u.LastName
    };
    
    public int Id { get; set; }
    public string FullName { get; set; }
    public string Mail { get; set; }
    public string Phone { get; set; }
    public string FirstName { get; set; }
    public string LastName { get; set; }
}
```

Painful to maintain both expression isn't it ? Here is what TheWheel allows you to do :

```cs
public class Person
{
    public static Expression<Func<User, Person>> ToList=u=>new Person
    {
      Id=u.Id,
      FullName=u.FullName,
      Mail=u.Mail,
    };
    
    public static Expression<Func<User, Person>> ToDetails=ToList.Combine(u=>new Person
    {
      Phone=u.Phone,
      FirstName=u.FirstName,
      LastName=u.LastName
    });
    
    public int Id { get; set; }
    public string FullName { get; set; }
    public string Mail { get; set; }
    public string Phone { get; set; }
    public string FirstName { get; set; }
    public string LastName { get; set; }
}
```

You only need to maintain delta between the basic list and the full details.

## Apply lambda on single class

Let's assume you have the class defined in the previous sample and the following one :
```cs
public class Car
{
    public static Expression<Func<Vehicle, Car>> ToList=v=>new Car
    {
      Id=v.Id,
      Color=v.Color,
      Brand=v.Brand.Name,
      Model=v.Model,
      Owner=new Person{
        Id=v.Owner.Id,
        FullName=v.Owner.FullName,
        Mail=v.Owner.Mail,
      }
    };
    
    public string Id { get; set; }
    public string Color { get; set; }
    public string Brand { get; set; }
    public string Model { get; set; }
    public Person Owner { get; set; }
}
```

You will say : Hey I have already written such a thing ! You're right, then, let's factorize thanks to TheWheel

```cs
public class Car
{
    public static Expression<Func<Vehicle, Car>> ToList=ReflectionExpression.PreCompile<Vehicle, Car>(a => new Car
    {
      Id=v.Id,
      Color=v.Color,
      Brand=v.Brand.Name,
      Model=v.Model,
      Owner=v.Owner.AsLambda(Person.ToList).AsCompilable(),
    }).AsExpression();
    
    public string Id { get; set; }
    public string Color { get; set; }
    public string Brand { get; set; }
    public string Model { get; set; }
    public Person Owner { get; set; }
}
```

You only need to maintain delta between the basic list and the full details.
