using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Nest;

namespace FluentNest
{
    public static class GroupBys
    {
        public static AggregationDescriptor<T> GroupBy<T>(this AggregationDescriptor<T> innerAggregation, Expression<Func<T, Object>> fieldGetter) where T : class
        {
            AggregationDescriptor<T> v = new AggregationDescriptor<T>();
            var fieldName = fieldGetter.GetName();
            v.Terms(fieldName, tr =>
            {
                TermsAggregationDescriptor<T> trmAggDescriptor = new TermsAggregationDescriptor<T>();
                trmAggDescriptor.Field(fieldGetter);
                return trmAggDescriptor.Aggregations(x => innerAggregation);
            });

            return v;
        }

        public static AggregationDescriptor<T> CardinalityBy<T>(Expression<Func<T, object>> fieldGetter) where T : class
        {
            AggregationDescriptor<T> v = new AggregationDescriptor<T>();
            var fieldName = fieldGetter.GetName();
            var sumAggs = v.Cardinality(fieldName, tr => tr.Field(fieldGetter));
            return sumAggs;
        }

        public static AggregationDescriptor<T> DistinctBy<T>(Expression<Func<T, Object>> fieldGetter) where T : class
        {
            AggregationDescriptor<T> v = new AggregationDescriptor<T>();
            var fieldName = fieldGetter.GetName();
            v.Terms(fieldName, tr =>
            {
                TermsAggregationDescriptor<T> trmAggDescriptor = new TermsAggregationDescriptor<T>();
                trmAggDescriptor.Field(fieldGetter);
                return trmAggDescriptor;
            });

            return v;
        }

        public static AggregationDescriptor<T> GroupBy<T>(this AggregationDescriptor<T> innerAggregation, String key) where T : class
        {
            AggregationDescriptor<T> v = new AggregationDescriptor<T>();
            v.Terms(key, tr =>
            {
                TermsAggregationDescriptor<T> trmAggDescriptor = new TermsAggregationDescriptor<T>();
                trmAggDescriptor.Field(key);
                return trmAggDescriptor.Aggregations(x => innerAggregation);
            });

            return v;
        }


        public static AggregationDescriptor<T> GroupBy<T>(this AggregationDescriptor<T> innerAggregation, IEnumerable<String> keys) where T : class
        {
            var reversedAndLowered = keys.Select(x => x.FirstCharacterToLower()).Reverse().ToList();
            var aggregations = reversedAndLowered.Aggregate(innerAggregation, (s, i) => s.GroupBy(i));
            return aggregations;
        }

        public static IEnumerable<KeyItem> GetGroupBy<T>(this BucketAggregationBase aggs, Expression<Func<T, Object>> fieldGetter)
        {
            var aggName = fieldGetter.GetName();
            var itemsTerms = aggs.Terms(aggName);
            return itemsTerms.Items;
        }

        public static IEnumerable<KeyItem> GetGroupBy(this BucketAggregationBase aggs, string key)
        {
            var itemsTerms = aggs.Terms(key);
            return itemsTerms.Items;
        }

        public static IDictionary<String, KeyItem> GetDictioanry<T>(this BucketAggregationBase aggs, Expression<Func<T, Object>> fieldGetter)
        {
            var aggName = fieldGetter.GetName();
            var itemsTerms = aggs.Terms(aggName);
            return itemsTerms.Items.ToDictionary(x => x.Key);
        }

        public static IEnumerable<K> GetGroupBy<T,K>(this AggregationsHelper aggs, Expression<Func<T, Object>> fieldGetter, Func<KeyItem, K> objectTransformer)
        {
            var aggName = fieldGetter.GetName();
            var itemsTerms = aggs.Terms(aggName);
            return itemsTerms.Items.Select(objectTransformer);
        }

        public static IEnumerable<KeyItem> GetGroupBy<T>(this AggregationsHelper aggs, Expression<Func<T, Object>> fieldGetter)
        {
            var aggName = fieldGetter.GetName();
            var itemsTerms = aggs.Terms(aggName);
            return itemsTerms.Items;
        }

        public static IEnumerable<V> GetDistinct<T,V>(this AggregationsHelper aggs, Expression<Func<T, V>> fieldGetter) where V:class,IConvertible
        {
            var aggName = fieldGetter.GetName();
            var itemsTerms = aggs.Terms(aggName);
            if((typeof(V).IsEnum))
            {
                return itemsTerms.Items.Select((x => Parse<V>(x.Key)));
            }
            else if (typeof (V) == typeof (String))
            {
                return itemsTerms.Items.Select((x => (V) (Object) (x.Key)));
            }
            else
            {
                return itemsTerms.Items.Select((x => (V)(Object)(x.Key)));
            }
        }

        
        public static IEnumerable<KeyItem> GetGroupBy(this AggregationsHelper aggs, string key)
        {
            var itemsTerms = aggs.Terms(key);
            return itemsTerms.Items;
        }

        public static IDictionary<V, KeyItem> GetDictioanry<T,V>(this AggregationsHelper aggs, Expression<Func<T, Object>> fieldGetter) where V:struct,IConvertible
        {
            var aggName = fieldGetter.GetName();
            var itemsTerms = aggs.Terms(aggName);
            if ((typeof(V).IsEnum))
            {
                return itemsTerms.Items.ToDictionary(x => Parse<V>(x.Key));
            }
            return itemsTerms.Items.ToDictionary(x => (V)(Object)x.Key);
        }

        public static IDictionary<String, K> GetDictioanry<T, K>(this AggregationsHelper aggs, Expression<Func<T, Object>> fieldGetter, Func<KeyItem, K> objectTransformer)
        {
            var aggName = fieldGetter.GetName();
            var itemsTerms = aggs.Terms(aggName);
            return itemsTerms.Items.ToDictionary(x => x.Key, objectTransformer);
        }

        public static T Parse<T>(string value)
        {
            return (T)Enum.Parse(typeof(T), value);
        }

    }
}
