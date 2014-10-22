﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace TheWheel.Domain
{
    public interface ILogicDeletable : IIdentifiable
    {
        DateTime? DeletedOn { get; set; }
    }
}
