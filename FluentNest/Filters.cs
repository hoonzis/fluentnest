﻿using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq.Expressions;
using System.Reflection;
using Nest;

namespace FluentNest
{
    public static class Filters
    {
        public static string FirstCharacterToLower(this string str)
        {
            if (string.IsNullOrEmpty(str) || char.IsLower(str, 0))
                return str;

            return char.ToLowerInvariant(str[0]) + str.Substring(1);
        }

        public static AggregationContainerDescriptor<T> IntoDateHistogram<T>(this AggregationContainerDescriptor<T> innerAggregation,
            Expression<Func<T, object>> fieldGetter, DateInterval interval) where T : class
        {
            AggregationContainerDescriptor<T> v = new AggregationContainerDescriptor<T>();
            var fieldName = GetName(fieldGetter);
            v.DateHistogram(fieldName, dr =>
            {
                DateHistogramAggregationDescriptor<T> dateAggDesc = new DateHistogramAggregationDescriptor<T>();
                dateAggDesc.Field(fieldGetter).Interval(interval);
                return dateAggDesc.Aggregations(x => innerAggregation);
            });

            return v;
        }

        public static AggregationContainerDescriptor<T> IntoHistogram<T>(this AggregationContainerDescriptor<T> innerAggregation,
            Expression<Func<T, Object>> fieldGetter, int interval) where T : class
        {
            AggregationContainerDescriptor<T> v = new AggregationContainerDescriptor<T>();
            var fieldName = GetName(fieldGetter);
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
            return agg.DateHistogram(GetName(fieldGetter), x => x.Field(fieldGetter).Interval(dateInterval));
        }

        private static string GetName<T, TK>(this Expression<Func<T, TK>> exp)
        {
            var body = exp.Body as MemberExpression;

            if (body == null)
            {
                var ubody = (UnaryExpression) exp.Body;
                body = ubody.Operand as MemberExpression;
            }


            return body.Member.Name;
        }

        public static string GetAggName<T, TK>(this Expression<Func<T, TK>> exp, AggType type)
        {
            var body = exp.Body as MemberExpression;

            if (body == null)
            {
                var ubody = (UnaryExpression) exp.Body;
                body = ubody.Operand as MemberExpression;
            }


            return type + body.Member.Name;
        }


        public static IList<DateHistogramBucket> GetDateHistogram<T>(this KeyedBucket item,
            Expression<Func<T, Object>> fieldGetter)
        {
            var histogramItem = item.DateHistogram(GetName(fieldGetter));
            return histogramItem.Buckets;
        }

        public static IList<DateHistogramBucket> GetDateHistogram<T>(this AggregationsHelper aggs,
            Expression<Func<T, Object>> fieldGetter)
        {
            var histogramItem = aggs.DateHistogram(GetName(fieldGetter));
            return histogramItem.Buckets;
        }

        public static IList<HistogramBucket> GetHistogram<T>(this AggregationsHelper aggs,
            Expression<Func<T, Object>> fieldGetter)
        {
            var histogramItem = aggs.Histogram(GetName(fieldGetter));
            return histogramItem.Buckets;
        }

        public static QueryContainer GenerateComparisonFilter<T>(this Expression expression, ExpressionType type)
            where T : class
        {
            var binaryExpression = (BinaryExpression)expression;

            var value = GetValue(binaryExpression);
            var memberAccessor = binaryExpression.Left as MemberExpression;
            var fieldName = GetFieldNameFromMember(memberAccessor);

            if (value is DateTime)
            {
                return GenerateComparisonFilter<T>((DateTime) value, type, fieldName);
            }
            if (value is double || value is decimal)
            {
                return GenerateComparisonFilter<T>(Convert.ToDouble(value), type, fieldName);
            }
            if (value is int || value is long)
            {
                return GenerateComparisonFilter<T>(Convert.ToInt64(value), type, fieldName);
            }
            throw new InvalidOperationException("Comparison on non-supported type");
        }

        public static QueryContainer GenerateComparisonFilter<T>(DateTime value, ExpressionType type, string fieldName)
            where T : class
        {
            var filterDescriptor = new QueryContainerDescriptor<T>();
            if (type == ExpressionType.LessThan)
            {
                return filterDescriptor.DateRange(x => x.LessThan(value).Field(fieldName));
            }
            if (type == ExpressionType.GreaterThan)
            {
                return filterDescriptor.DateRange(x => x.GreaterThan(value).Field(fieldName));
            }
            if (type == ExpressionType.LessThanOrEqual)
            {
                return filterDescriptor.DateRange(x => x.LessThanOrEquals(value).Field(fieldName));
            }
            if (type == ExpressionType.GreaterThanOrEqual)
            {
                return filterDescriptor.DateRange(x => x.GreaterThanOrEquals(value).Field(fieldName));
            }
            throw new NotImplementedException();
        }

        public static QueryContainer GenerateComparisonFilter<T>(long value, ExpressionType type, string fieldName)
            where T : class
        {
            var filterDescriptor = new QueryContainerDescriptor<T>();
            if (type == ExpressionType.LessThan)
            {
                return filterDescriptor.Range(x => x.LessThan(value).Field(fieldName));
            }
            if (type == ExpressionType.GreaterThan)
            {
                return filterDescriptor.Range(x => x.GreaterThan(value).Field(fieldName));
            }
            if (type == ExpressionType.LessThanOrEqual)
            {
                return filterDescriptor.Range(x => x.LessThanOrEquals(value).Field(fieldName));
            }
            if (type == ExpressionType.GreaterThanOrEqual)
            {
                return filterDescriptor.Range(x => x.GreaterThanOrEquals(value).Field(fieldName));
            }
            throw new NotImplementedException();
        }

        public static QueryContainer GenerateComparisonFilter<T>(double value, ExpressionType type, string fieldName)
            where T : class
        {
            var filterDescriptor = new QueryContainerDescriptor<T>();
            if (type == ExpressionType.LessThan)
            {
                return filterDescriptor.Range(x => x.LessThan(value).Field(fieldName));
            }
            if (type == ExpressionType.GreaterThan)
            {
                return filterDescriptor.Range(x => x.GreaterThan(value).Field(fieldName));
            }
            if (type == ExpressionType.LessThanOrEqual)
            {
                return filterDescriptor.Range(x => x.LessThanOrEquals(value).Field(fieldName));
            }
            if (type == ExpressionType.GreaterThanOrEqual)
            {
                return filterDescriptor.Range(x => x.GreaterThanOrEquals(value).Field(fieldName));
            }
            throw new NotImplementedException();
        }

        public static QueryContainer GenerateEqualityFilter<T>(this BinaryExpression binaryExpression) where T : class
        {           
            var value = GetValue(binaryExpression);
            var queryContainerDescriptor = new QueryContainerDescriptor<T>();
            var fieldExpression = GetFieldExpression<T>(binaryExpression.Left);
            return queryContainerDescriptor.Term(fieldExpression, value);
        }

        public static QueryContainer GenerateNotEqualFilter<T>(this BinaryExpression expression) where T : class
        {
            var equalityFilter = GenerateEqualityFilter<T>(expression);
            var filterDescriptor = new QueryContainerDescriptor<T>();
            return filterDescriptor.Bool(x => x.MustNot(equalityFilter));
        }

        public static QueryContainer GenerateBoolFilter<T>(this Expression expression) where T : class
        {
            var filterDescriptor = new QueryContainerDescriptor<T>();
            var fieldName = GenerateFilterName(expression);
            return filterDescriptor.Term(fieldName, true);
        }


        public static Expression<Func<T,object>>  GetFieldExpression<T>(this Expression expression)
        {
            var memberExpression = expression as MemberExpression;
            if (memberExpression != null)
            {
                var memberName = GetFieldNameFromMember(memberExpression);
                var param = Expression.Parameter(typeof(T));
                var body = Expression.Convert(Expression.Property(param, memberName), typeof(object));
                var exp = Expression.Lambda<Func<T, object>>(body, param);
                return exp;
            }
            if (expression is UnaryExpression)
            {
                var unary = expression as UnaryExpression;
                return GetFieldExpression<T>(unary.Operand);
            }
            throw new NotImplementedException();
        }

        public static string GetFieldNameFromMember(this MemberExpression expression)
        {
            return FirstCharacterToLower(expression.Member.Name);
        }

        public static QueryContainer GenerateFilterDescription<T>(this Expression expression) where T:class
        {
            var expType = expression.NodeType;
            
            if (expType == ExpressionType.AndAlso)
            {
                var binaryExpression = (BinaryExpression)expression;
                var leftFilter = GenerateFilterDescription<T>(binaryExpression.Left);
                var rightFilter = GenerateFilterDescription<T>(binaryExpression.Right);
                var filterDescriptor = new QueryContainerDescriptor<T>();
                return filterDescriptor.Bool(s => s.Must(leftFilter, rightFilter));
            }
            
            if (expType == ExpressionType.Or || expType == ExpressionType.OrElse)
            {
                var binaryExpression = (BinaryExpression)expression;
                var leftFilter = GenerateFilterDescription<T>(binaryExpression.Left);
                var rightFilter = GenerateFilterDescription<T>(binaryExpression.Right);
                var filterDescriptor = new QueryContainerDescriptor<T>();
                return filterDescriptor.Bool(x => x.Should(leftFilter, rightFilter).MinimumShouldMatch(1));
            }
            if (expType == ExpressionType.Equal)
            {
                return GenerateEqualityFilter<T>(expression as BinaryExpression);
            }
            if(expType == ExpressionType.LessThan || expType == ExpressionType.GreaterThan || expType == ExpressionType.LessThanOrEqual || expType == ExpressionType.GreaterThanOrEqual)
            {
                return GenerateComparisonFilter<T>(expression,expType);
            }
            if (expType == ExpressionType.MemberAccess)
            {
                var memberExpression = (MemberExpression)expression;
                //here we handle binary expressions in the from .field.hasValue
                if (memberExpression.Member.Name == "HasValue")
                {
                    var parentFieldExpression = (memberExpression.Expression as MemberExpression);
                    var parentFieldName = GetFieldNameFromMember(parentFieldExpression);

                    var filterDescriptor = new QueryContainerDescriptor<T>();
                    return filterDescriptor.Exists(x => x.Field(parentFieldName));
                }
                var isProperty = memberExpression.Member.MemberType == MemberTypes.Property;
                if (isProperty)
                {
                    var propertyType = ((PropertyInfo) memberExpression.Member).PropertyType;
                    if (propertyType == typeof (bool))
                    {
                        return GenerateBoolFilter<T>(memberExpression);
                    }
                }
            }

            if (expType == ExpressionType.Lambda)
            {
                var lambda = (LambdaExpression)expression;
                return GenerateFilterDescription<T>(lambda.Body);
            }

            if (expType == ExpressionType.NotEqual)
            {
                return GenerateNotEqualFilter<T>(expression as BinaryExpression);
            }
            
            throw  new NotImplementedException();
        }

        public static string GenerateFilterName(this Expression expression)
        {
            var expType = expression.NodeType;

            if (expType == ExpressionType.AndAlso || expType == ExpressionType.Or || expType == ExpressionType.OrElse || expType == ExpressionType.Equal 
                || expType == ExpressionType.LessThan || expType == ExpressionType.GreaterThan || expType == ExpressionType.LessThanOrEqual || expType == ExpressionType.GreaterThanOrEqual
                || expType == ExpressionType.NotEqual)
            {
                var binaryExpression = (BinaryExpression)expression;
                var leftFilterName = GenerateFilterName(binaryExpression.Left);
                var rightFilterName = GenerateFilterName(binaryExpression.Right);
                return leftFilterName + "_" + expType + "_" + rightFilterName;
            }
            else
            {
                if (expType == ExpressionType.MemberAccess)
                {
                    var memberExpression = (MemberExpression)expression;
                    //here we handle binary expressions in the from .field.hasValue
                    if (memberExpression.Member.Name == "HasValue")
                    {
                        var parentFieldExpression = (memberExpression.Expression as MemberExpression);
                        var parentFieldName = GetFieldNameFromMember(parentFieldExpression);
                        return parentFieldName + ".hasValue";
                    }
                    return GetFieldNameFromMember(memberExpression);
                }
                if (expType == ExpressionType.Lambda)
                {
                    var lambda = (LambdaExpression)expression;
                    return GenerateFilterName(lambda.Body);
                }
                if (expType == ExpressionType.Convert)
                {
                    var unary = (UnaryExpression)expression;
                    return GenerateFilterName(unary.Operand);
                }
                if (expType == ExpressionType.Constant)
                {
                    var constExp = (ConstantExpression)expression;
                    return constExp.Value.ToString();
                }
            }
            throw new NotImplementedException();
        }

        public static SearchDescriptor<T> FilterOn<T>(this SearchDescriptor<T> searchDescriptor, Expression<Func<T, bool>> filterRule) where T : class
        {
            var filterDescriptor = GenerateFilterDescription<T>(filterRule.Body);
            return searchDescriptor.Query(_ => filterDescriptor);
        }

        public static SearchDescriptor<T> FilterOn<T>(this SearchDescriptor<T> searchDescriptor, QueryContainer container) where T : class
        {
            return searchDescriptor.Query(q => container);
        }

        public static DeleteByQueryDescriptor<T> FilterOn<T>(this DeleteByQueryDescriptor<T> deleteDescriptor, QueryContainer container) where T : class
        {
            return deleteDescriptor.Query(q => container);
        }

        public static DeleteByQueryDescriptor<T> FilterOn<T>(this DeleteByQueryDescriptor<T> deleteDescriptor, Expression<Func<T, bool>> filterRule) where T : class
        {

            var filterDescriptor = GenerateFilterDescription<T>(filterRule.Body);
            return deleteDescriptor.Query(_ => filterDescriptor);
        }


        public static QueryContainer AndFilteredOn<T>(this QueryContainer queryDescriptor, Expression<Func<T, bool>> filterRule) where T : class
        {
            var filterDescriptor = new QueryContainerDescriptor<T>();
            var binaryExpression = filterRule.Body as BinaryExpression;
            var newPartOfQuery = GenerateFilterDescription<T>(binaryExpression);
            return filterDescriptor.Bool(x => x.Must(queryDescriptor, newPartOfQuery));            
        }

        public static QueryContainer AndValueWithin<T>(this QueryContainer queryDescriptor, Expression<Func<T, Object>> fieldGetter, IEnumerable<string> list) where T : class
        {
            var filterDescriptor = new QueryContainerDescriptor<T>();
            var newFilter = new QueryContainerDescriptor<T>();
            var newPartOfQuery = newFilter.Terms(terms=>terms.Terms(list).Field(fieldGetter));
            return filterDescriptor.Bool(x => x.Must(queryDescriptor, newPartOfQuery));
        }

        public static QueryContainer CreateFilter<T>(Expression<Func<T, bool>> filterRule) where T : class
        {
            return GenerateFilterDescription<T>(filterRule);
        }

        public static QueryContainer ValueWithin<T>(Expression<Func<T, object>> propertyGetter, IEnumerable<string> list) where T : class
        {
            return new QueryContainerDescriptor<T>().AndValueWithin(propertyGetter, list);
        }

        private static object GetValue(BinaryExpression binaryExpression)
        {
            var leftHand = binaryExpression.Left;
            var valueExpression = binaryExpression.Right;

            if (leftHand is UnaryExpression)
            {
                // This is necessary in order to avoid the automatic cast of enums to the underlying integer representation
                // In some cases the lambda comes in the shape (Convert(EngineType), 0), where 0 represents the first case of the EngineType enum
                // In such cases, we don't wante the value in the Terms to be 0, but rather we pass the enum value (eg. EngineType.Diesel)
                // and we let the serializer to do it's job and spit out Term("fieldName","diesel") or Term("fieldName","0") depending whether it is converting enums as integers or strings
                // or anything else
                var unaryExpression = leftHand as UnaryExpression;
                if (unaryExpression.Operand.Type.IsEnum)
                {
                    valueExpression = Expression.Convert(binaryExpression.Right, unaryExpression.Operand.Type);
                }
            }

            var objectMember = Expression.Convert(valueExpression, typeof(object));
            var getterLambda = Expression.Lambda<Func<object>>(objectMember);
            var getter = getterLambda.Compile();
            return getter();

        }

        public static T Parse<T>(string value)
        {
            return (T)Enum.Parse(typeof(T), value);
        }

        public static AggsContainer<T> AsContainer<T> (this AggregationsHelper aggs)
        {
            return new AggsContainer<T>(aggs);
        }

        public static K StringToAnything<K>(string value)
        {
            if ((typeof(K).IsEnum))
            {
                return Parse<K>(value);
            }
            else
            {
                TypeConverter typeConverter = TypeDescriptor.GetConverter(typeof(K));
                return (K)typeConverter.ConvertFromString(value);
            }
        }
    }
}