using System;
using System.Linq.Expressions;
using Nest;

namespace FluentNest
{
    public static class Statistics
    {
        public static AggregationDescriptor<T> SumBy<T>(this AggregationDescriptor<T> agg, Expression<Func<T, Object>> fieldGetter) where T : class
        {
            return agg.Sum(fieldGetter.GetName(), x => x.Field(fieldGetter));
        }

        public static AggregationDescriptor<T> SumBy<T>(this AggregationDescriptor<T> agg, Expression<Func<T, object>> fieldGetter, Expression<Func<T, bool>> filterRule) where T : class
        {
            var fieldName = fieldGetter.GetName();
            var filterName = filterRule.GenerateFilterName();
            agg.Filter(filterName,
                f =>
                    f.Filter(fd => filterRule.Body.GenerateFilterDescription<T>())
                        .Aggregations(innerAgg => innerAgg.Sum(fieldName, field => field.Field(fieldGetter))));
            return agg;
        }

        public static AggregationDescriptor<T> CountBy<T>(this AggregationDescriptor<T> agg, Expression<Func<T, Object>> fieldGetter) where T : class
        {
            return agg.ValueCount(fieldGetter.GetName(), x => x.Field(fieldGetter));
        }

        public static AggregationDescriptor<T> CountBy<T>(this AggregationDescriptor<T> agg, Expression<Func<T, object>> fieldGetter, Expression<Func<T, bool>> filterRule) where T : class
        {
            var fieldName = fieldGetter.GetName();
            var filterName = filterRule.GenerateFilterName();
            agg.Filter(filterName,
                f =>
                    f.Filter(fd => filterRule.Body.GenerateFilterDescription<T>())
                        .Aggregations(innerAgg => innerAgg.ValueCount(fieldName, field => field.Field(fieldGetter))));
            return agg;
        }

        public static AggregationDescriptor<T> CardinalityBy<T>(this AggregationDescriptor<T> agg, Expression<Func<T, Object>> fieldGetter) where T : class
        {
            return agg.Cardinality(fieldGetter.GetName(), x => x.Field(fieldGetter));
        }

        public static AggregationDescriptor<T> DistinctBy<T>(this AggregationDescriptor<T> agg, Expression<Func<T, Object>> fieldGetter) where T : class
        {
            var fieldName = fieldGetter.GetName();
            return agg.Terms(fieldName, x => x.Field(fieldGetter));
        }

        public static AggregationDescriptor<T> AverageBy<T>(this AggregationDescriptor<T> agg, Expression<Func<T, Object>> fieldGetter) where T : class
        {
            return agg.Average(fieldGetter.GetName(), x => x.Field(fieldGetter));
        }
    }
}
