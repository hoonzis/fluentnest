using System;
using System.Collections.Generic;
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

        public int GetCardinality(Expression<Func<T, object>> fieldGetter) => aggs.GetCardinality(fieldGetter);

        public K GetSum<K>(Expression<Func<T, K>> fieldGetter, Expression<Func<T, object>> filterRule = null) => aggs.GetSum(fieldGetter, filterRule);

        public K GetAverage<K>(Expression<Func<T, K>> fieldGetter) => aggs.GetAverage(fieldGetter);

        public K GetMin<K>(Expression<Func<T, K>> fieldGetter) => aggs.GetMin(fieldGetter);

        public K GetMax<K>(Expression<Func<T, K>> fieldGetter) => aggs.GetMax(fieldGetter);

        public int? GetCount(Expression<Func<T, object>> fieldGetter, Expression<Func<T, object>> filterRule = null) => aggs.GetCount(fieldGetter, filterRule);

        public IEnumerable<V> GetDistinct<V>(Expression<Func<T, V>> fieldGetter) => aggs.GetDistinct(fieldGetter);

        public IList<PercentileItem> GetPercentile(Expression<Func<T, Object>> fieldGetter) => aggs.GetPercentile<T>(fieldGetter);

        public StatsMetric GetStats(Expression<Func<T, Object>> fieldGetter) => aggs.GetStats(fieldGetter);
    }
}
