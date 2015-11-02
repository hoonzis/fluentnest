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

        public K GetSum<K>(Expression<Func<T, K>> fieldGetter)
        {
            return aggs.GetSum(fieldGetter);
        }

        public int GetCardinality(Expression<Func<T, object>> fieldGetter)
        {
            return aggs.GetCardinality(fieldGetter);
        }

        public K GetSum<K>(Expression<Func<T, K>> fieldGetter, Expression<Func<T, object>> filterRule = null)
        {
            return aggs.GetSum<T,K>(fieldGetter, filterRule);
        }

        public double? GetAvg(Expression<Func<T, object>> fieldGetter)
        {
            return aggs.GetAvg(fieldGetter);
        }

        public int? GetCount(Expression<Func<T, object>> fieldGetter)
        {
            return aggs.GetCount(fieldGetter);
        }

        public int? GetCondCount(Expression<Func<T, object>> fieldGetter, Expression<Func<T, object>> filterRule = null)
        {
            return aggs.GetCondCount(fieldGetter, filterRule);
        }

        public IEnumerable<V> GetDistinct<V>(Expression<Func<T, V>> fieldGetter)
        {
            return aggs.GetDistinct(fieldGetter);
        }
    }
}
