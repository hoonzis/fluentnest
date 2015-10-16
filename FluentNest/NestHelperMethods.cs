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
            return searchDescriptor.Filter(x => x.Term(fieldGetter, value));
        }

        public static FilterContainer GenerateComparisonFilter<T>(this Expression expression, ExpressionType type) where T : class
        {
            var binaryExpression = expression as BinaryExpression;
            var fieldName = GetFieldNameFromAccessor(binaryExpression);
            var value = GetValue(binaryExpression.Right);
            

            if (value is DateTime)
            {
                return GenerateComparisonFilter<T>((DateTime)value, type, fieldName);
            }
            else if (value is double)
            {
                return GenerateComparisonFilter<T>((double)value, type, fieldName);
            }
            else if (value is int)
            {
                return GenerateComparisonFilter<T>((long)(int)value, type, fieldName);
            }
            else if (value is long)
            {
                return GenerateComparisonFilter<T>((long)value, type, fieldName);
            }
            throw new InvalidOperationException("Comparison on non-supported type");
        }

        public static FilterContainer GenerateComparisonFilter<T>(DateTime value, ExpressionType type, string fieldName) where T : class
        {
            var filterDescriptor = new FilterDescriptor<T>();
            if (type == ExpressionType.LessThan)
            {
                return filterDescriptor.Range(x => x.Lower(value).OnField(fieldName));
            }
            else if (type == ExpressionType.GreaterThan)
            {
                return filterDescriptor.Range(x => x.Greater(value).OnField(fieldName));
            }
            else if (type == ExpressionType.LessThanOrEqual)
            {
                return filterDescriptor.Range(x => x.LowerOrEquals(value).OnField(fieldName));
            }
            else if (type == ExpressionType.GreaterThanOrEqual)
            {
                return filterDescriptor.Range(x => x.GreaterOrEquals(value).OnField(fieldName));
            }
            throw new NotImplementedException();
        }

        public static FilterContainer GenerateComparisonFilter<T>(long value, ExpressionType type, string fieldName) where T : class
        {
            var filterDescriptor = new FilterDescriptor<T>();
            if (type == ExpressionType.LessThan)
            {
                return filterDescriptor.Range(x => x.Lower(value).OnField(fieldName));
            }
            else if (type == ExpressionType.GreaterThan)
            {
                return filterDescriptor.Range(x => x.Greater(value).OnField(fieldName));
            }
            else if (type == ExpressionType.LessThanOrEqual)
            {
                return filterDescriptor.Range(x => x.LowerOrEquals(value).OnField(fieldName));
            }
            else if (type == ExpressionType.GreaterThanOrEqual)
            {
                return filterDescriptor.Range(x => x.GreaterOrEquals(value).OnField(fieldName));
            }
            throw new NotImplementedException();
        }

        public static FilterContainer GenerateComparisonFilter<T>(double value, ExpressionType type, string fieldName) where T : class
        {
            var filterDescriptor = new FilterDescriptor<T>();
            if (type == ExpressionType.LessThan)
            {
                return filterDescriptor.Range(x => x.Lower(value).OnField(fieldName));
            }
            else if (type == ExpressionType.GreaterThan)
            {
                return filterDescriptor.Range(x => x.Greater(value).OnField(fieldName));
            }
            else if (type == ExpressionType.LessThanOrEqual)
            {
                return filterDescriptor.Range(x => x.LowerOrEquals(value).OnField(fieldName));
            }
            else if (type == ExpressionType.GreaterThanOrEqual)
            {
                return filterDescriptor.Range(x => x.GreaterOrEquals(value).OnField(fieldName));
            }
            throw new NotImplementedException();
        }

        public static FilterContainer GenerateEqualityFilter<T>(this Expression expression) where T : class
        {
            var binaryExpression = expression as BinaryExpression;
            var value = GetValue(binaryExpression.Right);
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
                var binaryExpression = expression as BinaryExpression;
                return GetFieldNameFromAccessor(binaryExpression.Left);
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
            else if (expType == ExpressionType.Lambda)
            {
                var lambda = expression as LambdaExpression;
                return GenerateFilterDescription<T>(lambda.Body);
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

        public static SearchDescriptor<T> FilteredOn<T>(this SearchDescriptor<T> searchDescriptor, FilterContainer container) where T : class
        {
            return searchDescriptor.Query(q => q.Filtered(fil => fil.Filter(f => container)));
        }

        public static FilterContainer AndFilteredOn<T>(this FilterContainer queryDescriptor, Expression<Func<T, bool>> filterRule) where T : class
        {
            var filterDescriptor = new FilterDescriptor<T>();
            var binaryExpression = filterRule.Body as BinaryExpression;
            var newPartOfQuery = GenerateFilterDescription<T>(binaryExpression);
            return filterDescriptor.And(newPartOfQuery, queryDescriptor);            
        }

        public static FilterContainer CreateFilter<T>(Expression<Func<T, bool>> filterRule) where T : class
        {
            return GenerateFilterDescription<T>(filterRule);
        }

        private static object GetValue(Expression member)
        {
            var objectMember = Expression.Convert(member, typeof(object));
            var getterLambda = Expression.Lambda<Func<object>>(objectMember);
            var getter = getterLambda.Compile();
            return getter();
        }

        public static T Parse<T>(string value)
        {
            return (T)Enum.Parse(typeof(T), value);
        }
    }
}