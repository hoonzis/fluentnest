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
        private AggregationsHelper aggs;

        public AggsContainer(AggregationsHelper aggs)
        {
            this.aggs = aggs;
        }

        public K GetSum<K>(Expression<Func<T, K>> fieldGetter)
        {
            return aggs.GetSum(fieldGetter);
        }

        public int GetCardinality(Expression<Func<T, Object>> fieldGetter)
        {
            return aggs.GetCardinality(fieldGetter);
        }

        public K GetCondSum<K>(Expression<Func<T, K>> fieldGetter, Expression<Func<T, Object>> filterRule = null)
        {
            return aggs.GetCondSum<T,K>(fieldGetter, filterRule);
        }

        public double? GetAvg(Expression<Func<T, Object>> fieldGetter)
        {
            return aggs.GetAvg(fieldGetter);
        }

        public int? GetCount(Expression<Func<T, Object>> fieldGetter)
        {
            return aggs.GetCount(fieldGetter);
        }

        public int? GetCondCount(Expression<Func<T, Object>> fieldGetter, Expression<Func<T, Object>> filterRule = null)
        {
            return aggs.GetCondCount(fieldGetter, filterRule);
        }

        public IEnumerable<V> GetDistinct<V>(Expression<Func<T, V>> fieldGetter)
        {
            return aggs.GetDistinct(fieldGetter);
        }
    }
}
