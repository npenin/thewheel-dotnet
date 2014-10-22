using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace TheWheel.Domain
{
    /// <summary>
    /// Represents an entity having information about its change tracking dates.
    /// </summary>
    public interface ITrackable
    {
        /// <summary>
        /// Gets or sets the date when this instance was created.
        /// </summary>
        /// <value>
        /// the date when this instance was created.
        /// </value>
        DateTime CreatedOn { get; set; }
        /// <summary>
        /// Gets or sets the date when this instance was updated.
        /// </summary>
        /// <value>
        /// the date when this instance was updated.
        /// </value>
        DateTime UpdatedOn { get; set; }
    }
}
