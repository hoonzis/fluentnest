using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Nest;

namespace FluentNest
{
    public static class NestHelperMethods
    {
        public static string FirstCharacterToLower(this string str)
        {
            if (String.IsNullOrEmpty(str) || Char.IsLower(str, 0))
                return str;

            return Char.ToLowerInvariant(str[0]) + str.Substring(1);
        }

        public static AggregationDescriptor<T> GroupBy<T>(this AggregationDescriptor<T> innerAggregation,Expression<Func<T,Object>> fieldGetter) where T:class 
        {
            AggregationDescriptor<T> v = new AggregationDescriptor<T>();
            var fieldName = GetName(fieldGetter);
            v.Terms(fieldName, tr =>
            {
                TermsAggregationDescriptor<T> trmAggDescriptor = new TermsAggregationDescriptor<T>();
                trmAggDescriptor.Field(fieldGetter);
                return trmAggDescriptor.Aggregations(x => innerAggregation);
            });

            return v;
        }

        public static AggregationDescriptor<T> IntoDateHistogram<T>(this AggregationDescriptor<T> innerAggregation, Expression<Func<T, Object>> fieldGetter,DateInterval interval) where T : class
        {
            AggregationDescriptor<T> v = new AggregationDescriptor<T>();
            var fieldName = GetName(fieldGetter);
            v.DateHistogram(fieldName, dr=>
            {
                DateHistogramAggregationDescriptor<T> dateAggDesc = new DateHistogramAggregationDescriptor<T>();
                dateAggDesc.Field(fieldGetter).Interval(interval);
                return dateAggDesc.Aggregations(x => innerAggregation);
            });

            return v;
        }

        
        public static AggregationDescriptor<T> DateHistogram<T>(this AggregationDescriptor<T> agg, Expression<Func<T, Object>> fieldGetter, DateInterval dateInterval) where T : class
        {
            return agg.DateHistogram(GetName(fieldGetter), x => x.Field(fieldGetter).Interval(dateInterval));
        }

        public static string GetName<T,K>(this Expression<Func<T, K>> exp)
        {
            MemberExpression body = exp.Body as MemberExpression;

            if (body == null)
            {
                UnaryExpression ubody = (UnaryExpression)exp.Body;
                body = ubody.Operand as MemberExpression;
            }

         
            return body.Member.Name;
        }

        public static IEnumerable<KeyItem> GetGroupBy<T>(this BucketAggregationBase aggs, Expression<Func<T, Object>> fieldGetter)
        {
            var aggName = GetName(fieldGetter);
            var itemsTerms = aggs.Terms(aggName);
            return itemsTerms.Items;
        }

        public static IDictionary<String,KeyItem> GetDictioanry<T>(this BucketAggregationBase aggs, Expression<Func<T, Object>> fieldGetter)
        {
            var aggName = GetName(fieldGetter);
            var itemsTerms = aggs.Terms(aggName);
            return itemsTerms.Items.ToDictionary(x => x.Key);
        }
        
        public static IEnumerable<KeyItem> GetGroupBy<T>(this AggregationsHelper aggs, Expression<Func<T, Object>> fieldGetter)
        {
            var aggName = GetName(fieldGetter);
            var itemsTerms = aggs.Terms(aggName);
            return itemsTerms.Items;
        }

        public static IDictionary<String, KeyItem> GetDictioanry<T>(this AggregationsHelper aggs, Expression<Func<T, Object>> fieldGetter)
        {
            var aggName = GetName(fieldGetter);
            var itemsTerms = aggs.Terms(aggName);
            return itemsTerms.Items.ToDictionary(x => x.Key);
        }

        public static IDictionary<String, K> GetDictioanry<T,K>(this AggregationsHelper aggs, Expression<Func<T, Object>> fieldGetter, Func<KeyItem,K> objectTransformer)
        {
            var aggName = GetName(fieldGetter);
            var itemsTerms = aggs.Terms(aggName);
            return itemsTerms.Items.ToDictionary(x => x.Key,v=>objectTransformer(v));
        }

        public static IList<HistogramItem> GetDateHistogram<T>(this KeyItem item, Expression<Func<T, Object>> fieldGetter)
        {
            var histogramItem = item.DateHistogram(GetName(fieldGetter));
            return histogramItem.Items;
        }

        public static IList<HistogramItem> GetDateHistogram<T>(this AggregationsHelper aggs, Expression<Func<T, Object>> fieldGetter)
        {
            var histogramItem = aggs.DateHistogram(GetName(fieldGetter));
            return histogramItem.Items;
        }

        public static SearchDescriptor<T> FilterOn<T>(this SearchDescriptor<T> searchDescriptor,Expression<Func<T, Object>> fieldGetter, object value) where T:class
        {
            if (value is String)
            {
                value = ((String)value).ToLower();
            }
            return searchDescriptor.Filter(x => x.Term(fieldGetter, value));
        }

        public static FilterContainer GenerateComparisonFilter<T>(this Expression expression, ExpressionType type) where T : class
        {
            var binaryExpression = expression as BinaryExpression;
            var fieldName = GetFieldNameFromAccessor(binaryExpression);
            var value = GetValue(binaryExpression.Right);
            var filterDescriptor = new FilterDescriptor<T>();

            if (type == ExpressionType.LessThan)
            {
                return filterDescriptor.Range(x => x.Lower((DateTime) value).OnField(fieldName));
            }
            else if(type == ExpressionType.GreaterThan)
            {
                return filterDescriptor.Range(x => x.Greater((DateTime)value).OnField(fieldName));
            }
            else if (type == ExpressionType.LessThanOrEqual)
            {
                return filterDescriptor.Range(x => x.LowerOrEquals((DateTime)value).OnField(fieldName));
            }
            else if (type == ExpressionType.GreaterThanOrEqual)
            {
                return filterDescriptor.Range(x => x.GreaterOrEquals((DateTime)value).OnField(fieldName));
            }
            throw  new NotImplementedException();
        }

        public static FilterContainer GenerateEqualityFilter<T>(this Expression expression) where T : class
        {
            var binaryExpression = expression as BinaryExpression;
            var value = GetValue(binaryExpression.Right);
            if (value is String)
            {
                value = ((String) value).ToLower();
            }
            var filterDescriptor = new FilterDescriptor<T>();
            var fieldName = GetFieldNameFromAccessor(binaryExpression);
            return filterDescriptor.Term(fieldName,value);
        }

        public static FilterContainer GenerateBoolFilter<T>(this Expression expression) where T : class
        {
            var value = GetValue(expression);
            var filterDescriptor = new FilterDescriptor<T>();
            var fieldName = GetFieldNameFromAccessor(expression);
            return filterDescriptor.Term(fieldName, value);
        }

        public static String GetFieldNameFromAccessor(this Expression expression)
        {
            String name;

            if (expression is MemberExpression)
            {
                name = (expression as MemberExpression).Member.Name;
                return FirstCharacterToLower(name);
            }
            else if (expression is BinaryExpression)
            {
                name = ((expression as BinaryExpression).Left as MemberExpression).Member.Name;
                return FirstCharacterToLower(name);
            }
            else if (expression is LambdaExpression)
            {
                var lambda = expression as LambdaExpression;
                return GetFieldNameFromAccessor(lambda.Body);
            }
            else if (expression is UnaryExpression)
            {
                var unary = expression as UnaryExpression;
                return GetFieldNameFromAccessor(unary.Operand);
            }
            
            throw  new NotImplementedException();
        }

        public static FilterContainer GenerateFilterDescription<T>(this Expression expression) where T:class
        {
            var expType = expression.NodeType;
            
            if (expType == ExpressionType.AndAlso)
            {
                var binaryExpression = expression as BinaryExpression;
                var leftFilter = GenerateFilterDescription<T>(binaryExpression.Left);
                var rightFilter = GenerateFilterDescription<T>(binaryExpression.Right);
                var filterDescriptor = new FilterDescriptor<T>();
                return filterDescriptor.And(leftFilter, rightFilter);

            }else if (expType == ExpressionType.Equal)
            {
                return GenerateEqualityFilter<T>(expression);
            }
            else if(expType == ExpressionType.LessThan || expType == ExpressionType.GreaterThan || expType == ExpressionType.LessThanOrEqual || expType == ExpressionType.GreaterThanOrEqual)
            {
                return GenerateComparisonFilter<T>(expression,expType);
            }
            else if (expType == ExpressionType.MemberAccess)
            {
                return GenerateBoolFilter<T>(expression);
            }
            throw  new NotImplementedException();
        }

        public static SearchDescriptor<T> FilterOn<T>(this SearchDescriptor<T> searchDescriptor, Expression<Func<T, bool>> filterRule) where T : class
        {
            var binaryExpression = filterRule.Body as BinaryExpression;
            return searchDescriptor.Filter(f => GenerateFilterDescription<T>(binaryExpression));           
        }

        public static SearchDescriptor<T> FilteredOn<T>(this SearchDescriptor<T> searchDescriptor, Expression<Func<T, bool>> filterRule) where T : class
        {
            var binaryExpression = filterRule.Body as BinaryExpression;
            return searchDescriptor.Query(q => q.Filtered(fil=>fil.Filter(f=>GenerateFilterDescription<T>(binaryExpression))));
        }

        private static object GetValue(Expression member)
        {
            var objectMember = Expression.Convert(member, typeof(object));
            var getterLambda = Expression.Lambda<Func<object>>(objectMember);
            var getter = getterLambda.Compile();
            return getter();
        }
    }
}