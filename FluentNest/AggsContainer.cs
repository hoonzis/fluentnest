using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Nest;

namespace FluentNest
{
    public class AggsContainer<T>
    {
        private readonly AggregationsHelper aggs;

        public AggsContainer(AggregationsHelper aggs)
        {
            this.aggs = aggs;
        }

        public int GetCardinality(Expression<Func<T, object>> fieldGetter, Expression<Func<T, object>> filterRule = null)
        {
            return aggs.GetCardinality(fieldGetter, filterRule);
        }

        public K GetSum<K>(Expression<Func<T, K>> fieldGetter, Expression<Func<T, object>> filterRule = null)
        {
            return aggs.GetSum(fieldGetter, filterRule);
        }

        public K GetAverage<K>(Expression<Func<T, K>> fieldGetter)
        {
            return aggs.GetAverage(fieldGetter);
        }

        public K GetMin<K>(Expression<Func<T, K>> fieldGetter)
        {
            return aggs.GetMin(fieldGetter);
        }

        public K GetMax<K>(Expression<Func<T, K>> fieldGetter)
        {
            return aggs.GetMax(fieldGetter);
        }

        public int? GetCount(Expression<Func<T, object>> fieldGetter, Expression<Func<T, object>> filterRule = null)
        {
            return aggs.GetCount(fieldGetter, filterRule);
        }

        public IEnumerable<V> GetDistinct<V>(Expression<Func<T, V>> fieldGetter)
        {
            return aggs.GetDistinct(fieldGetter);
        }

        public IList<PercentileItem> GetPercentile(Expression<Func<T, Object>> fieldGetter)
        {
            return aggs.GetPercentile<T>(fieldGetter);
        }

        public StatsAggregate GetStats(Expression<Func<T, Object>> fieldGetter)
        {
            return aggs.GetStats(fieldGetter);
        }

        public K GetFirstBy<K>(Expression<Func<T, K>> fieldGetter,
            Expression<Func<T, object>> filterRule = null)
        {
            return aggs.GetFirstBy(fieldGetter, filterRule);
        }

        public IEnumerable<KeyedBucket> GetGroupBy(Expression<Func<T, object>> fieldGetter)
        {
            return aggs.GetGroupBy(fieldGetter);
        }

        public IEnumerable<TO> GetGroupBy<TO>(Expression<Func<T, object>> fieldGetter, Func<KeyedBucket, TO> objectTransformer)
        {
            var buckets = aggs.GetGroupBy(fieldGetter);
            return buckets.Select(objectTransformer);
        }

        public IDictionary<TK, TV> GetDictionary<TK, TV>(Expression<Func<T, TK>> keyGetter, Func<KeyedBucket, TV> objectTransformer)
        {
            var aggName = keyGetter.GetAggName(AggType.GroupBy);
            var buckets = aggs.GetGroupBy(aggName);
            return buckets.ToDictionary(x => Filters.StringToAnything<TK>(x.Key), objectTransformer);
        }

        public IDictionary<TK, KeyedBucket> GetDictionary<TK>(Expression<Func<T, TK>> keyGetter)
        {
            var aggName = keyGetter.GetAggName(AggType.GroupBy);
            var buckets = aggs.GetGroupBy(aggName);
            return buckets.ToDictionary(x => Filters.StringToAnything<TK>(x.Key));
        }
    }
}
