using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;
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

        public int GetCardinality(Expression<Func<T, object>> fieldGetter)
        {
            return aggs.GetCardinality(fieldGetter);
        }

        public K GetSum<K>(Expression<Func<T, K>> fieldGetter, Expression<Func<T, object>> filterRule = null)
        {
            return aggs.GetSum<T,K>(fieldGetter, filterRule);
        }

        public K GetAverage<K>(Expression<Func<T, K>> fieldGetter)
        {
            return aggs.GetAverage<T,K>(fieldGetter);
        }

        public K GetMin<K>(Expression<Func<T, K>> fieldGetter)
        {
            return aggs.GetMin<T, K>(fieldGetter);
        }

        public K GetMax<K>(Expression<Func<T, K>> fieldGetter)
        {
            return aggs.GetMax<T, K>(fieldGetter);
        }

        public int? GetCount(Expression<Func<T, object>> fieldGetter, Expression<Func<T, object>> filterRule = null)
        {
            return aggs.GetCount(fieldGetter, filterRule);
        }

        public IEnumerable<V> GetDistinct<V>(Expression<Func<T, V>> fieldGetter)
        {
            return aggs.GetDistinct(fieldGetter);
        }

        public IList<PercentileItem> GetPercentile<V>(Expression<Func<T, Object>> fieldGetter)
        {
            return aggs.GetPercentile<T>(fieldGetter);
        }
    }
}
