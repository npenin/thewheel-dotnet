using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using TheWheel.Domain;

namespace TheWheel.WebApi
{
    public class BaseApiController<T> : Controller
        where T : IIdentifiable
    {
        private IQueryable<T> source;
        public BaseApiController(IQueryable<T> source)
        {
            this.source = source;
        }
        public T Get(int id)
        {
            return source.FirstOrDefault(t => t.Id == id);
        }
        public IEnumerable<T> Get()
        {
            return source;
        }
    }
}
