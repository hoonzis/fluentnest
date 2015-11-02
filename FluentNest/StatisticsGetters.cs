using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;
using Nest;

namespace FluentNest
{
    public static class StatisticsGetters
    {

        /// <summary>
        /// Takes a value metric and forces a conversion to certain type
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <typeparam name="K"></typeparam>
        /// <param name="agg"></param>
        /// <returns></returns>
        private static K ValueAsUndType<K>(ValueMetric agg)
        {
            var type = typeof(K);
            if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>))
            {
                var undType = Nullable.GetUnderlyingType(type);
                var valueAsUndType = Convert.ChangeType(agg.Value, undType);
                return (K)(Object)valueAsUndType;
            }
            else
            {
                return (K)Convert.ChangeType(agg.Value, typeof(K));
            }
        }

        //public static K GetSum<T, K>(this AggregationsHelper aggs, Expression<Func<T, K>> fieldGetter)
        //{
        //    var aggName = fieldGetter.GetName();
        //    var sumAggs = aggs.Sum(aggName);
        //    return ValueAsUndType<K>(sumAggs);
        //}

        public static int GetCardinality<T>(this AggregationsHelper aggs, Expression<Func<T, object>> fieldGetter)
        {
            var aggName = fieldGetter.GetName();
            var itemsTerms = aggs.Cardinality(aggName);
            if (itemsTerms == null || !itemsTerms.Value.HasValue)
                throw new InvalidOperationException("There is not cardinality avaialble");
            return (int)itemsTerms.Value.Value;
        }

        public static K GetSum<T,K>(this AggregationsHelper aggs, Expression<Func<T, K>> fieldGetter, Expression<Func<T, object>> filterRule = null)
        {
            var sumAggName = fieldGetter.GetName();
            if (filterRule == null)
            {
                var aggName = fieldGetter.GetName();
                var sumAggs = aggs.Sum(aggName);
                return ValueAsUndType<K>(sumAggs);
            }
            else
            {
                var filterName = filterRule.GenerateFilterName();
                var filterAgg = aggs.Filter(filterName);
                var sumAgg = filterAgg.Sum(sumAggName);

                return ValueAsUndType<K>(sumAgg);
            }
        }
        
        public static double? GetAvg<T>(this AggregationsHelper aggs, Expression<Func<T, object>> fieldGetter)
        {
            var aggName = fieldGetter.GetName();
            var itemsTerms = aggs.Average(aggName);
            return itemsTerms.Value;
        }

        public static int? GetCount<T>(this AggregationsHelper aggs, Expression<Func<T, object>> fieldGetter)
        {
            var aggName = fieldGetter.GetName();
            var itemsTerms = aggs.ValueCount(aggName);
            if (!itemsTerms.Value.HasValue)
                return null;
            return (int)itemsTerms.Value;
        }

        public static int? GetCondCount<T>(this AggregationsHelper aggs, Expression<Func<T, object>> fieldGetter, Expression<Func<T, object>> filterRule = null)
        {
            var countAggName = fieldGetter.GetName();
            if (filterRule == null)
            {
                foreach (var aggregation in aggs.Aggregations)
                {
                    if (aggregation.Value is SingleBucket)
                    {
                        var bucket = aggregation.Value as SingleBucket;
                        var countAgg = bucket.ValueCount(countAggName);
                        if (countAgg != null)
                        {
                            if (!countAgg.Value.HasValue)
                                return null;
                            return (int)countAgg.Value.Value;
                        }
                    }
                }
                throw new InvalidOperationException("Couldn't find any conditioanal counts in this aggregation");
            }
            else
            {
                var condAggName = filterRule.GenerateFilterName();
                var filterAgg = aggs.Filter(condAggName);
                var sumAgg = filterAgg.Sum(countAggName);
                if (!sumAgg.Value.HasValue)
                    return null;
                return (int)sumAgg.Value;
            }
        }

        public static IEnumerable<V> GetDistinct<T, V>(this AggregationsHelper aggs, Expression<Func<T, V>> fieldGetter)
        {
            var aggName = fieldGetter.GetName();
            var itemsTerms = aggs.Terms(aggName);
            if ((typeof(V).IsEnum))
            {
                return itemsTerms.Items.Select((x => NestHelperMethods.Parse<V>(x.Key)));
            }
            else if (typeof(V) == typeof(string))
            {
                return itemsTerms.Items.Select((x => (V)(object)(x.Key)));
            }
            else
            {
                return itemsTerms.Items.Select((x => (V)(object)(x.Key)));
            }
        }
    }
}
