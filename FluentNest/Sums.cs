using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;
using Nest;

namespace FluentNest
{
    public static class Sums
    {
        public static AggregationDescriptor<T> AndSumBy<T>(this AggregationDescriptor<T> agg, Expression<Func<T, Object>> fieldGetter) where T : class
        {
            return agg.Sum(fieldGetter.GetName(), x => x.Field(fieldGetter));
        }

        public static AggregationDescriptor<T> AndCountBy<T>(this AggregationDescriptor<T> agg, Expression<Func<T, Object>> fieldGetter) where T : class
        {
            return agg.ValueCount(fieldGetter.GetName(), x => x.Field(fieldGetter));
        }

        public static DateHistogramAggregationDescriptor<T> SumBy<T>(this DateHistogramAggregationDescriptor<T> agg, Expression<Func<T, Object>> fieldGetter) where T : class
        {
            return agg.Aggregations(x => x.Sum(fieldGetter.GetName(), dField => dField.Field(fieldGetter)));
        }

        public static AggregationDescriptor<T> SumOnField<T>(Expression<Func<T, object>> fieldGetter) where T : class
        {
            AggregationDescriptor<T> v = new AggregationDescriptor<T>();
            var fieldName = fieldGetter.GetName();
            var sumAggs = v.Sum(fieldName, tr => tr.Field(fieldGetter));
            return sumAggs;
        }

        public static AggregationDescriptor<T> ConditionalSumOnField<T>(Expression<Func<T, object>> fieldGetter, Expression<Func<T, bool>> filterRule) where T : class
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

        public static double? GetSum<T>(this AggregationsHelper aggs, Expression<Func<T, Object>> fieldGetter)
        {
            var aggName = fieldGetter.GetName();
            var itemsTerms = aggs.Sum(aggName);
            return itemsTerms.Value;
        }

        public static double? GetConditionalSum<T>(this AggregationsHelper aggs, Expression<Func<T, Object>> fieldGetter, Expression<Func<T, bool>> filterRule)
        {
            var condAggName = filterRule.GetFieldNameFromAccessor();
            var sumAggName = fieldGetter.GetName();
            var filterAgg = aggs.Filter(condAggName);
            var sumAgg = filterAgg.Sum(sumAggName);
            return sumAgg.Value;
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

        public static double? GetCount<T>(this AggregationsHelper aggs, Expression<Func<T, Object>> fieldGetter)
        {
            var aggName = fieldGetter.GetName();
            var itemsTerms = aggs.ValueCount(aggName);
            return itemsTerms.Value;
        }
    }
}
