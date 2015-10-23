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
        public static K GetSum<T, K>(this AggregationsHelper aggs, Expression<Func<T, K>> fieldGetter)
        {
            var aggName = fieldGetter.GetName();
            var itemsTerms = aggs.Sum(aggName);
            if (itemsTerms == null || itemsTerms.Value == null)
                throw new InvalidOperationException(string.Format("Sum of field:{0} not found", aggName));
            return (K)Convert.ChangeType(itemsTerms.Value, typeof(K));
        }

        public static double? GetSum<T>(this AggregationsHelper aggs, Expression<Func<T, Object>> fieldGetter)
        {
            var aggName = fieldGetter.GetName();
            var itemsTerms = aggs.Sum(aggName);
            if (itemsTerms == null)
                return null;
            return itemsTerms.Value;
        }

        public static int GetCardinality<T>(this AggregationsHelper aggs, Expression<Func<T, Object>> fieldGetter)
        {
            var aggName = fieldGetter.GetName();
            var itemsTerms = aggs.Cardinality(aggName);
            if (itemsTerms == null || !itemsTerms.Value.HasValue)
                throw new InvalidOperationException("There is not cardinality avaialble");
            return (int)itemsTerms.Value.Value;
        }

        public static double? GetCondSum<T>(this AggregationsHelper aggs, Expression<Func<T, Object>> fieldGetter, Expression<Func<T, Object>> filterRule = null)
        {
            var sumAggName = fieldGetter.GetName();
            if (filterRule == null)
            {
                foreach (var aggregation in aggs.Aggregations)
                {
                    if (aggregation.Value is SingleBucket)
                    {
                        var bucket = aggregation.Value as SingleBucket;
                        var countAgg = bucket.Sum(sumAggName);
                        if (countAgg != null)
                        {
                            if (!countAgg.Value.HasValue)
                                return null;
                            return (int)countAgg.Value.Value;
                        }
                    }
                }
                throw new InvalidOperationException("");
            }
            else
            {
                var condAggName = filterRule.GetFieldNameFromAccessor();
                var filterAgg = aggs.Filter(condAggName);
                var sumAgg = filterAgg.Sum(sumAggName);
                return sumAgg.Value;
            }
        }

        public static double? GetAvg<T>(this AggregationsHelper aggs, Expression<Func<T, Object>> fieldGetter)
        {
            var aggName = fieldGetter.GetName();
            var itemsTerms = aggs.Average(aggName);
            return itemsTerms.Value;
        }

        public static int? GetCount<T>(this AggregationsHelper aggs, Expression<Func<T, Object>> fieldGetter)
        {
            var aggName = fieldGetter.GetName();
            var itemsTerms = aggs.ValueCount(aggName);
            if (!itemsTerms.Value.HasValue)
                return null;
            return (int)itemsTerms.Value;
        }

        public static int? GetCondCount<T>(this AggregationsHelper aggs, Expression<Func<T, Object>> fieldGetter, Expression<Func<T, Object>> filterRule = null)
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
                var condAggName = filterRule.GetFieldNameFromAccessor();
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
            else if (typeof(V) == typeof(String))
            {
                return itemsTerms.Items.Select((x => (V)(Object)(x.Key)));
            }
            else
            {
                return itemsTerms.Items.Select((x => (V)(Object)(x.Key)));
            }
        }
    }
}
