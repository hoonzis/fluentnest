using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Nest;

namespace FluentNest
{
    public static class GroupBys
    {
        /// <summary>
        /// Creates a nested aggregation. Wraps the aggregation on which it is called by a new Terms aggregation, using the provided fieldGetter function to terms on the field.
        /// </summary>
        public static AggregationDescriptor<T> GroupBy<T>(this AggregationDescriptor<T> innerAggregation, Expression<Func<T, Object>> fieldGetter) where T : class
        {
            var v = new AggregationDescriptor<T>();
            var fieldName = fieldGetter.GetAggName(AggType.GroupBy);
            v.Terms(fieldName, tr =>
            {
                var trmAggDescriptor = new TermsAggregationDescriptor<T>();
                trmAggDescriptor.Field(fieldGetter);
                trmAggDescriptor.Size(int.MaxValue);
                return trmAggDescriptor.Aggregations(x => innerAggregation);
            });

            return v;
        }

        /// <summary>
        /// Creates a nested aggregation. Wraps the aggregation on which it is called by a new Terms aggregation, using the provided fieldName.
        /// </summary>
        public static AggregationDescriptor<T> GroupBy<T>(this AggregationDescriptor<T> innerAggregation, string key) where T : class
        {
            var v = new AggregationDescriptor<T>();
            v.Terms(key, tr =>
            {
                var trmAggDescriptor = new TermsAggregationDescriptor<T>();
                trmAggDescriptor.Field(key);
                trmAggDescriptor.Size(int.MaxValue);
                return trmAggDescriptor.Aggregations(x => innerAggregation);
            });

            return v;
        }

        /// <summary>
        /// Groups on the list of provided fields, returns multiple nested groups
        /// </summary>
        public static AggregationDescriptor<T> GroupBy<T>(this AggregationDescriptor<T> innerAggregation, IEnumerable<string> keys) where T : class
        {
            var reversedAndLowered = keys.Select(x => x.FirstCharacterToLower()).Reverse().ToList();
            var aggregations = reversedAndLowered.Aggregate(innerAggregation, (s, i) => s.GroupBy(i));
            return aggregations;
        }

        /// <summary>
        /// Retrieves the terms aggregation just by it's name
        /// </summary>
        public static IEnumerable<KeyItem> GetGroupBy(this AggregationsHelper aggs, string aggName)
        {
            if (aggs.Aggregations == null || aggs.Aggregations.Count == 0)
            {
                throw new InvalidOperationException("No aggregations available on the result");
            }

            if (!aggs.Aggregations.ContainsKey(aggName))
            {
                var availableAggregations = aggs.Aggregations.Select(x => x.Key).Aggregate((agg, x) => agg + "m" + x);
                throw new InvalidOperationException($"Aggregation {aggName} not in the result. Available aggregations: {availableAggregations}");
            }
            var itemsTerms = aggs.Terms(aggName);
            return itemsTerms.Items;
        }

        /// <summary>
        /// Retrieves the list of buckets if terms aggregation is present
        /// </summary>
        public static IEnumerable<KeyItem> GetGroupBy<T>(this AggregationsHelper aggs, Expression<Func<T, object>> fieldGetter)
        {
            var aggName = fieldGetter.GetAggName(AggType.GroupBy);
            return aggs.GetGroupBy(aggName);
        }
    }
}
