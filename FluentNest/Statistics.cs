using System;
using System.Linq.Expressions;
using Nest;

namespace FluentNest
{
    public static class Statistics
    {
        public static AggregationDescriptor<T> AndSumBy<T>(this AggregationDescriptor<T> agg, Expression<Func<T, Object>> fieldGetter) where T : class
        {
            return agg.Sum(fieldGetter.GetName(), x => x.Field(fieldGetter));
        }

        public static AggregationDescriptor<T> AndCountBy<T>(this AggregationDescriptor<T> agg, Expression<Func<T, Object>> fieldGetter) where T : class
        {
            return agg.ValueCount(fieldGetter.GetName(), x => x.Field(fieldGetter));
        }

        public static AggregationDescriptor<T> AndCardinalityBy<T>(this AggregationDescriptor<T> agg, Expression<Func<T, Object>> fieldGetter) where T : class
        {
            return agg.Cardinality(fieldGetter.GetName(), x => x.Field(fieldGetter));
        }

        public static AggregationDescriptor<T> AndCondCountBy<T>(this AggregationDescriptor<T> agg, Expression<Func<T, object>> fieldGetter, Expression<Func<T, bool>> filterRule) where T : class
        {
            var fieldName = fieldGetter.GetName();
            var filterName = filterRule.GetFieldNameFromAccessor();
            agg.Filter(filterName,
                f =>
                    f.Filter(fd => filterRule.Body.GenerateFilterDescription<T>())
                        .Aggregations(innerAgg => innerAgg.ValueCount(fieldName, field => field.Field(fieldGetter))));
            return agg;
        }

        public static DateHistogramAggregationDescriptor<T> SumBy<T>(this DateHistogramAggregationDescriptor<T> agg, Expression<Func<T, Object>> fieldGetter) where T : class
        {
            return agg.Aggregations(x => x.Sum(fieldGetter.GetName(), dField => dField.Field(fieldGetter)));
        }

        public static AggregationDescriptor<T> SumBy<T>(Expression<Func<T, object>> fieldGetter) where T : class
        {
            AggregationDescriptor<T> v = new AggregationDescriptor<T>();
            var fieldName = fieldGetter.GetName();
            var sumAggs = v.Sum(fieldName, tr => tr.Field(fieldGetter));
            return sumAggs;
        }

        public static AggregationDescriptor<T> CardinalityBy<T>(Expression<Func<T, object>> fieldGetter) where T : class
        {
            AggregationDescriptor<T> v = new AggregationDescriptor<T>();
            var fieldName = fieldGetter.GetName();
            var sumAggs = v.Cardinality(fieldName, tr => tr.Field(fieldGetter));
            return sumAggs;
        }

        public static AggregationDescriptor<T> CountBy<T>(Expression<Func<T, object>> fieldGetter) where T : class
        {
            AggregationDescriptor<T> v = new AggregationDescriptor<T>();
            var fieldName = fieldGetter.GetName();
            var sumAggs = v.ValueCount(fieldName, tr => tr.Field(fieldGetter));
            return sumAggs;
        }

        public static AggregationDescriptor<T> CondSumBy<T>(Expression<Func<T, object>> fieldGetter, Expression<Func<T, bool>> filterRule) where T : class
        {
            AggregationDescriptor<T> v = new AggregationDescriptor<T>();
            var fieldName = fieldGetter.GetName();
            var filterName = filterRule.GetFieldNameFromAccessor();
            var filtered = v.Filter(filterName,
                f =>
                    f.Filter(fd => filterRule.Body.GenerateFilterDescription<T>())
                        .Aggregations(innerAgg => innerAgg.Sum(fieldName, field => field.Field(fieldGetter))));
            return filtered;
        }

        public static AggregationDescriptor<T> AndCondSumBy<T>(this AggregationDescriptor<T> agg, Expression<Func<T, object>> fieldGetter, Expression<Func<T, bool>> filterRule) where T : class
        {
            var fieldName = fieldGetter.GetName();
            var filterName = filterRule.GetFieldNameFromAccessor();
            agg.Filter(filterName,
                f =>
                    f.Filter(fd => filterRule.Body.GenerateFilterDescription<T>())
                        .Aggregations(innerAgg => innerAgg.Sum(fieldName, field => field.Field(fieldGetter))));
            return agg;
        }

        public static AggregationDescriptor<T> CondCountBy<T>(Expression<Func<T, object>> fieldGetter, Expression<Func<T, bool>> filterRule) where T : class
        {
            AggregationDescriptor<T> v = new AggregationDescriptor<T>();
            var fieldName = fieldGetter.GetName();
            var filterName = filterRule.GetFieldNameFromAccessor();
            var filtered = v.Filter(filterName,
                f =>
                    f.Filter(fd => filterRule.Body.GenerateFilterDescription<T>())
                        .Aggregations(innerAgg => innerAgg.ValueCount(fieldName, field => field.Field(fieldGetter))));
            return filtered;
        }

        public static Nullable<K> GetSum<T,K>(this AggregationsHelper aggs, Expression<Func<T, K>> fieldGetter) where K:struct
        {
            var aggName = fieldGetter.GetName();
            var itemsTerms = aggs.Sum(aggName);
            if (itemsTerms == null || !itemsTerms.Value.HasValue)
                return null;
            return (K)Convert.ChangeType(itemsTerms.Value, typeof (K));
        }

        public static Object GetGenericSum<T>(this AggregationsHelper aggs, Expression<Func<T, Object>> fieldGetter)
        {
            var aggName = fieldGetter.GetName();
            var itemsTerms = aggs.Sum(aggName);
            if (itemsTerms == null || !itemsTerms.Value.HasValue)
                return null;
            return itemsTerms.Value;
        }

        public static int GetCardinality<T>(this AggregationsHelper aggs, Expression<Func<T, Object>> fieldGetter)
        {
            var aggName = fieldGetter.GetName();
            var itemsTerms = aggs.Cardinality(aggName);
            if(itemsTerms == null || !itemsTerms.Value.HasValue)
                throw new InvalidOperationException("There is not cardinality avaialble");
            return (int) itemsTerms.Value.Value;
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
            else {
                var condAggName = filterRule.GetFieldNameFromAccessor();
                var filterAgg = aggs.Filter(condAggName);
                var sumAgg = filterAgg.Sum(sumAggName);
                return sumAgg.Value;
            }
        }

        public static AggregationDescriptor<T> AndAvgBy<T>(this AggregationDescriptor<T> agg, Expression<Func<T, Object>> fieldGetter) where T : class
        {
            return agg.Average(fieldGetter.GetName(), x => x.Field(fieldGetter));
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

        public static int? GetCondCount<T>(this AggregationsHelper aggs, Expression<Func<T, Object>> fieldGetter, Expression<Func<T, Object>> filterRule=null)
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
                            return (int) countAgg.Value.Value;
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
                return (int) sumAgg.Value;
            }
        }
    }
}
