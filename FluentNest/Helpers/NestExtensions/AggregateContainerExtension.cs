
namespace FluentNest
{
    using System;
    using System.Linq.Expressions;
    using Nest;

    public static class AggregateContainerNestExtension
    {
        public static AggregationContainerDescriptor<T> IntoDateHistogram<T>(this AggregationContainerDescriptor<T> innerAggregation,
                                                                            Expression<Func<T, object>> fieldGetter, DateInterval interval) where T : class
        {
            AggregationContainerDescriptor<T> v = new AggregationContainerDescriptor<T>();
            var fieldName = fieldGetter.GetName();
            v.DateHistogram(fieldName, dr =>
            {
                DateHistogramAggregationDescriptor<T> dateAggDesc = new DateHistogramAggregationDescriptor<T>();
                dateAggDesc.Field(fieldGetter).Interval(interval);
                return dateAggDesc.Aggregations(x => innerAggregation);
            });

            return v;
        }

        public static AggregationContainerDescriptor<T> IntoHistogram<T>(this AggregationContainerDescriptor<T> innerAggregation,
                                                                         Expression<Func<T, object>> fieldGetter, int interval) where T : class
        {
            AggregationContainerDescriptor<T> v = new AggregationContainerDescriptor<T>();
            var fieldName = fieldGetter.GetName();
            v.Histogram(fieldName, dr =>
            {
                HistogramAggregationDescriptor<T> dateAggDesc = new HistogramAggregationDescriptor<T>();
                dateAggDesc.Field(fieldGetter).Interval(interval);
                return dateAggDesc.Aggregations(x => innerAggregation);
            });

            return v;
        }

        public static AggregationContainerDescriptor<T> DateHistogram<T>(this AggregationContainerDescriptor<T> agg,
                                                                         Expression<Func<T, object>> fieldGetter, DateInterval dateInterval) where T : class
        {
            return agg.DateHistogram(fieldGetter.GetName(), x => x.Field(fieldGetter).Interval(dateInterval));
        }

    }
}
