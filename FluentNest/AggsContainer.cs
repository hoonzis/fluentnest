using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Nest;

namespace FluentNest
{
    public class AggsContainer<T>
    {
        private readonly AggregateDictionary aggs;

        public AggsContainer(AggregateDictionary aggs)
        {
            this.aggs = aggs;
        }

        public int GetCardinality(Expression<Func<T, object>> fieldGetter, Expression<Func<T, object>> filterRule = null)
        {
            return aggs.GetCardinality(fieldGetter, filterRule);
        }

        public TValue GetSum<TValue>(Expression<Func<T, TValue>> fieldGetter, Expression<Func<T, object>> filterRule = null)
        {
            return aggs.GetSum(fieldGetter, filterRule);
        }

        public TValue GetAverage<TValue>(Expression<Func<T, TValue>> fieldGetter)
        {
            return aggs.GetAverage(fieldGetter);
        }

        public TValue GetMin<TValue>(Expression<Func<T, TValue>> fieldGetter)
        {
            return aggs.GetMin(fieldGetter);
        }

        public TValue GetMax<TValue>(Expression<Func<T, TValue>> fieldGetter)
        {
            return aggs.GetMax(fieldGetter);
        }

        public int? GetCount(Expression<Func<T, object>> fieldGetter, Expression<Func<T, object>> filterRule = null)
        {
            return aggs.GetCount(fieldGetter, filterRule);
        }

        public IEnumerable<TV> GetDistinct<TV>(Expression<Func<T, TV>> fieldGetter)
        {
            return aggs.GetDistinct(fieldGetter);
        }

        public IList<PercentileItem> GetPercentile(Expression<Func<T, object>> fieldGetter)
        {
            return aggs.GetPercentile(fieldGetter);
        }

        public StatsAggregate GetStats(Expression<Func<T, object>> fieldGetter)
        {
            return aggs.GetStats(fieldGetter);
        }

        public TValue GetFirstBy<TValue>(Expression<Func<T, TValue>> fieldGetter,
            Expression<Func<T, object>> filterRule = null)
        {
            return aggs.GetFirstBy(fieldGetter, filterRule);
        }

        public IEnumerable<KeyedBucket<string>> GetGroupBy(Expression<Func<T, object>> fieldGetter)
        {
            return aggs.GetGroupBy(fieldGetter);
        }

        public IEnumerable<TItem> GetGroupBy<TItem>(Expression<Func<T, object>> fieldGetter, Func<KeyedBucket<string>, TItem> objectTransformer)
        {
            var buckets = aggs.GetGroupBy(fieldGetter);
            return buckets.Select(objectTransformer);
        }

        public IDictionary<TKey, TValue> GetDictionary<TKey, TValue>(Expression<Func<T, TKey>> keyGetter, Func<KeyedBucket<string>, TValue> objectTransformer)
        {
            var aggName = keyGetter.GetAggName(AggType.GroupBy);
            var buckets = aggs.GetGroupBy(aggName);
            return buckets.ToDictionary(x => Filters.StringToAnything<TKey>(x.Key), objectTransformer);
        }

        public IDictionary<TKey, KeyedBucket<string>> GetDictionary<TKey>(Expression<Func<T, TKey>> keyGetter)
        {
            var aggName = keyGetter.GetAggName(AggType.GroupBy);
            var buckets = aggs.GetGroupBy(aggName);
            return buckets.ToDictionary(x => Filters.StringToAnything<TKey>(x.Key));
        }
    }
}
