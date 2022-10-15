using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Primitives;
using TheWheel.ETL.DacPac;

namespace TheWheel.ETL.Owin
{
    public class PolicyConfiguration : IPolicyProvider
    {
        public PolicyConfiguration(IOptions<PolicyConfiguration> config)
        {
            this.Policies = config.Value.Policies;
        }

        public PolicyConfiguration()
        {
        }


        public Dictionary<string, Policy> Policies { get; set; } = new Dictionary<string, Policy>();

        internal static Regex WildCardToRegular(String value)
        {
            return new Regex("^" + Regex.Escape(value).Replace("\\?", ".").Replace("\\*", ".*") + "$", RegexOptions.Compiled);
        }


        public Policy GetPolicy(TableModel model)
        {
            EnsurePoliciesReady();
            Policy wildcard;

            if (Policies.TryGetValue(model.name, out var specific) && specific.Matches(model))
                return specific;
            else if ((wildcard = Policies.FirstOrDefault(kvp => kvp.Key != "*" && kvp.Key.Contains("*") && kvp.Value.Matches(model)).Value) != null)
                return wildcard;
            else if (Policies.TryGetValue("*", out var generic) && generic.Matches(model))
                return generic;
            else if (Policies.TryGetValue(model.type, out var type))
                return type;

            return null;
        }

        private void EnsurePoliciesReady()
        {
            foreach (var kvp in Policies)
            {
                kvp.Value.EnsureReady(kvp.Key);
            }
        }

        public bool IsAllowed(TableModel model)
        {
            return GetPolicy(model)?.Enabled ?? false;
        }

        public Task<IEnumerable<TableModel>> AllowedAsync(IEnumerable<TableModel> model)
        {
            return Task.FromResult(model.Where(IsAllowed));
        }

        public Task<bool> IsAllowedAsync(TableModel model)
        {
            return Task.FromResult(IsAllowed(model));
        }
    }

    public class Policy
    {
        internal Func<TableModel, bool> matches;
        public bool Enabled { get; set; }

        public string[] Types { get; set; }

        public bool Matches(TableModel model)
        {
            return (matches == null || matches(model)) && (Types == null || Types.Contains(model.type));
        }

        private string key;

        internal void EnsureReady(string key)
        {
            if (this.key != null || this.key == key)
                return;
            if (key == "*")
                matches = (model) => Enabled;
            else if (key.Contains("*"))
            {
                var regex = PolicyConfiguration.WildCardToRegular(key);
                matches = model => regex.IsMatch(model.name);
            }
            else
                matches = model => model.name == key;
            this.key = key;
        }
    }
}