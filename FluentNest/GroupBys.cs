
namespace FluentNest
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Linq.Expressions;
    using Nest;

    public static class GroupBys
    {
        /// <summary>
        /// Creates a nested aggregation. Wraps the aggregation on which it is called by a new Terms aggregation, using the provided fieldName.
        /// </summary>
        public static AggregationContainerDescriptor<T> GroupBy<T>(this AggregationContainerDescriptor<T> innerAggregation, string fieldName) where T : class
        {
            var v = new AggregationContainerDescriptor<T>();
            v.Terms(fieldName, tr =>
            {
                var trmAggDescriptor = new TermsAggregationDescriptor<T>();
                trmAggDescriptor.Field(fieldName);
                trmAggDescriptor.Size(int.MaxValue);
                return trmAggDescriptor.Aggregations(x => innerAggregation);
            });

            return v;
        }

        /// <summary>
        /// Creates a nested aggregation. Wraps the aggregation on which it is called by a new Terms aggregation, using the provided fieldGetter function to terms on the field.
        /// </summary>
        public static AggregationContainerDescriptor<T> GroupBy<T>(this AggregationContainerDescriptor<T> innerAggregation, Expression<Func<T, object>> fieldGetter) where T : class
        {
            var fieldName = fieldGetter.GetAggName(AggType.GroupBy);
            var v = new AggregationContainerDescriptor<T>();
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
        /// Groups on the list of provided fields, returns multiple nested groups
        /// </summary>
        public static AggregationContainerDescriptor<T> GroupBy<T>(this AggregationContainerDescriptor<T> innerAggregation, IEnumerable<string> keys) where T : class
        {
            var reversedAndLowered = keys.Select(x => x.FirstCharacterToLower()).Reverse().ToList();
            var aggregations = reversedAndLowered.Aggregate(innerAggregation, (s, i) => s.GroupBy(i));
            return aggregations;
        }
        
        /// <summary>
        /// Retrieves the terms aggregation just by it's name
        /// </summary>
        public static IReadOnlyCollection<KeyedBucket<string>> GetGroupBy(this AggregateDictionary aggs, string aggName)
        {
            aggs.CheckForAggregationInResult(aggName);
            var itemsTerms = aggs.Terms(aggName);
            return itemsTerms.Buckets;
        }

        /// <summary>
        /// Retrieves the list of buckets if terms aggregation is present
        /// </summary>
        public static IEnumerable<KeyedBucket<string>> GetGroupBy<T>(this AggregateDictionary aggs, Expression<Func<T, object>> fieldGetter)
        {
            var aggName = fieldGetter.GetAggName(AggType.GroupBy);
            return aggs.GetGroupBy(aggName);
        }

        /// <summary>
        /// Checks if aggregation with given name is available on the result and throws if not
        /// </summary>
        public static void CheckForAggregationInResult(this AggregateDictionary aggs, string aggName)
        {
            if (aggs == null || aggs.Count == 0)
            {
                throw new InvalidOperationException("No aggregations available on the result");
            }

            if (!aggs.ContainsKey(aggName))
            {
                var availableAggregations = aggs.Select(x => x.Key).Aggregate((agg, x) => agg + "m" + x);
                throw new InvalidOperationException($"Aggregation {aggName} not in the result. Available aggregations: {availableAggregations}");
            }
        }
    }
}
