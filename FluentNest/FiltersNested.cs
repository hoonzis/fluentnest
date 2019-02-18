using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Nest;

namespace FluentNest
{
    public static partial class Filters
    {
        public static QueryContainer GenerateFilterDescriptionNestedCore<T>(MethodCallExpression expression)
           where T : class
        {
            var firstArgument = (MemberExpression)expression.Arguments.First();
            var secondArgument = expression.Arguments.Skip(1).First();
            var methodInfo = expression.Method;

            if (methodInfo.Name.Equals("any", StringComparison.InvariantCultureIgnoreCase))
            {
                var fieldname = firstArgument.Member.Name;
                var type = GetPropertyType(typeof(T), fieldname);
                var esNestedPath = fieldname;
                // check if nested attribute can be added as a check
                var nestedAttribute = new object[1]; //  typeof(T).GetProperty(fieldname)?.GetCustomAttributes(typeof(NestedAttribute), false);
                  
                Type ex = typeof(Filters);
                MethodInfo mi = ex.GetMethod("GenerateFilterDescriptionNested");
                MethodInfo miConstructed = mi.MakeGenericMethod(type);

                var filter = (QueryContainer)miConstructed.Invoke(
                    null,
                    new[] { (object) secondArgument, (object)esNestedPath});

                var newFilter = new QueryContainerDescriptor<T>();
                if (nestedAttribute != null && nestedAttribute.Length > 0)
                {
                    var newPartOfQuery = newFilter.Nested(descriptor => descriptor.Path(esNestedPath).Query(q => filter));
                    return newPartOfQuery;
                }
                else
                {
                    var newPartOfQuery = newFilter.Bool(x => x.Must(filter));
                    return newPartOfQuery;
                }
            }

            throw new Exception("Unable to resolve method");
        }

        public static QueryContainer GenerateFilterDescriptionNested<T>(this Expression expression, string pathname) where T : class
        {
            var expType = expression.NodeType;


            if (expType == ExpressionType.AndAlso)
            {
                var binaryExpression = (BinaryExpression)expression;

                // handle special cases of two comparisons on the same field which should be compiled into a range request
                if (binaryExpression.Left.NodeType.IsComparisonType() && binaryExpression.Right.NodeType.IsComparisonType())
                {
                    if (binaryExpression.Left.GetFieldName() == binaryExpression.Right.GetFieldName())
                    {
                        //we supose that on left hand and right hand we have a binary expressions
                        var leftBinary = (BinaryExpression)binaryExpression.Left;
                        var leftValue = GetValue(leftBinary);

                        var memberAccessor = leftBinary.Left as MemberExpression;
                        var fieldName = $"{pathname}.{memberAccessor.GetFieldNameFromMemberOrGetFieldNamed()}";

                        var rightBinary = (BinaryExpression)binaryExpression.Right;
                        var rightValue = GetValue(rightBinary);
                        return Ranges.GenerateRangeFilter<T>(fieldName, leftValue, leftBinary.NodeType, rightValue, rightBinary.NodeType);
                    }
                }

                var rightFilter = GenerateFilterDescriptionNested<T>(binaryExpression.Right, pathname);
                var filterDescriptor = new QueryContainerDescriptor<T>();


                // Detecting a series of And filters
                var leftSide = binaryExpression.Left;
                var accumulatedExpressions = new List<Expression>();
                while (leftSide.NodeType == ExpressionType.AndAlso)
                {

                    var asBinary = (BinaryExpression)leftSide;
                    if (asBinary.Left.NodeType != ExpressionType.AndAlso)
                    {
                        accumulatedExpressions.Add(asBinary.Left);
                    }

                    if (asBinary.Right.NodeType != ExpressionType.AndAlso)
                    {
                        accumulatedExpressions.Add(asBinary.Right);
                    }

                    leftSide = asBinary.Left;
                }

                if (accumulatedExpressions.Count > 0)
                {
                    var filters = accumulatedExpressions.Select(e => GenerateFilterDescriptionNested<T>(e, pathname)).ToList();
                    filters.Add(rightFilter);
                    return filterDescriptor.Bool(s => s.Must(filters.ToArray()));
                }

                var leftFilter = GenerateFilterDescriptionNested<T>(binaryExpression.Left, pathname);
                return filterDescriptor.Bool(s => s.Must(leftFilter, rightFilter));
            }

            if (expType == ExpressionType.Or || expType == ExpressionType.OrElse)
            {
                var binaryExpression = (BinaryExpression)expression;
                var leftFilter = GenerateFilterDescriptionNested<T>(binaryExpression.Left, pathname);
                var rightFilter = GenerateFilterDescriptionNested<T>(binaryExpression.Right, pathname);
                var filterDescriptor = new QueryContainerDescriptor<T>();
                return filterDescriptor.Bool(x => x.Should(leftFilter, rightFilter).MinimumShouldMatch(1));
            }

            if (expType == ExpressionType.Equal)
            {
                return (expression as BinaryExpression).GenerateEqualityFilter<T>(pathname);
            }

            if (expType == ExpressionType.LessThan ||
                expType == ExpressionType.GreaterThan ||
                expType == ExpressionType.LessThanOrEqual ||
                expType == ExpressionType.GreaterThanOrEqual)
            {
                return (expression as BinaryExpression).GenerateComparisonFilter<T>(expType, pathname);
            }

            if (expType == ExpressionType.MemberAccess)
            {
                var memberExpression = (MemberExpression)expression;
                //here we handle binary expressions in the from .field.hasValue
                if (memberExpression.Member.Name == "HasValue")
                {
                    var parentFieldName = memberExpression.Expression.GetFieldNameFromMemberOrGetFieldNamed(pathname);
                    var filterDescriptor = new QueryContainerDescriptor<T>();
                    return filterDescriptor.Exists(x => x.Field($"{pathname}.{parentFieldName}"));
                }

                var isProperty = memberExpression.Member.MemberType == MemberTypes.Property;

                if (isProperty)
                {
                    var propertyType = ((PropertyInfo)memberExpression.Member).PropertyType;
                    if (propertyType == typeof(bool))
                    {
                        return memberExpression.GenerateBoolFilter<T>(pathname);
                    }
                }
            }

            if (expType == ExpressionType.Lambda)
            {
                var lambda = (LambdaExpression)expression;
                return GenerateFilterDescriptionNested<T>(lambda.Body, pathname);
            }

            if (expType == ExpressionType.NotEqual)
            {
                return (expression as BinaryExpression).GenerateNotEqualFilter<T>(pathname);
            }


            throw new NotImplementedException();
        }

        public static QueryContainer GenerateNotEqualFilter<T>(this BinaryExpression expression, string pathname)
            where T : class
        {
            var equalityFilter = GenerateEqualityFilter<T>(expression, pathname);
            var filterDescriptor = new QueryContainerDescriptor<T>();
            return filterDescriptor.Bool(x => x.MustNot(equalityFilter));
        }

        public static QueryContainer GenerateBoolFilter<T>(this Expression expression, string pathname) where T : class
        {
            return GenerateBoolFilter<T>(() => $"{pathname}.{expression.GenerateFilterName()}");
        }

        private static QueryContainer GenerateBoolFilter<T>(Func<string> fieldNameFunc)
            where T : class
        {
            var filterDescriptor = new QueryContainerDescriptor<T>();
            var fieldName = fieldNameFunc();
            return filterDescriptor.Term(fieldName, true);
        }

        public static QueryContainer GenerateComparisonFilter<T>(this Expression expression, ExpressionType type, string pathname)
            where T : class
        {
            return GenerateComparisonFilter<T>(expression, type, exp => GetFieldNameFromMemberOrGetFieldNamed(exp, pathname));
        }


        private static QueryContainer GenerateComparisonFilter<T>(Expression expression, ExpressionType type, Func<Expression, string> fieldNameFunc)
            where T : class
        {
            var binaryExpression = (BinaryExpression)expression;

            var value = GetValue(binaryExpression);

            if (value == null)
            {
                // if the value is null, no filters are added
                return new QueryContainer();
            }

            var fieldName = fieldNameFunc(binaryExpression.Left); // binaryExpression.Left.GetFieldNameFromMemberOrGetFieldNamed();
            var filterDescriptor = new QueryContainerDescriptor<T>();

            switch (value)
            {
                case DateTime time:
                    return filterDescriptor.DateRange(x => x.RangeOnDate(type, time).Field(fieldName));
                case double _:
                case decimal _:
                    return filterDescriptor.Range(x => x.RangeOnNumber(type, Convert.ToDouble(value)).Field(fieldName));
                case int _:
                case long _:
                    return filterDescriptor.Range(x => x.RangeOnNumber(type, Convert.ToInt64(value)).Field(fieldName));
            }

            throw new InvalidOperationException("Comparison on non-supported type");
        }


        public static QueryContainer GenerateEqualityFilter<T>(this BinaryExpression binaryExpression, string pathname)
            where T : class
        {
            var fieldExpression = binaryExpression.Left.GetFieldExpression<T>(pathname);
            return GenerateEqualityFilter(binaryExpression, fieldExpression);
        }

        private static QueryContainer GenerateEqualityFilter<T>(BinaryExpression binaryExpression, FieldOrExpression<T> fieldExpression)
            where T : class
        {
            var value = GetValue(binaryExpression);
            if (value == null)
            {
                return GenerateNonExistenceFilter<T>(fieldExpression);
            }

            var queryContainerDescriptor = new QueryContainerDescriptor<T>();
            return fieldExpression.Expression != null
                ? queryContainerDescriptor.Term(fieldExpression.Expression, value)
                : queryContainerDescriptor.Term(fieldExpression.Field, value);
        }

        private static QueryContainer GenerateNonExistenceFilter<T>(FieldOrExpression<T> fieldExpression)
            where T : class
        {
            var queryContainerDescriptor = new QueryContainerDescriptor<T>();
            return queryContainerDescriptor.Bool(
                b => b.MustNot(m => m.Exists(e => e.Field(fieldExpression.Expression ?? fieldExpression.Field))));
        }


        private static FieldOrExpression<T> GetFieldExpression<T>(this Expression expression, string pathname)
        {
            return GetFieldExpression(
                expression,
                memberExpression =>
                {
                    var memberName = GetFieldNameFromMemberOrGetFieldNamed(memberExpression);
                    return new FieldOrExpression<T>()
                    {
                        Field = new Field(
                            $"{pathname}.{memberName}")
                    };
                });
        }

        private static FieldOrExpression<T> GetFieldExpression<T>(
            Expression expression,
            Func<MemberExpression, FieldOrExpression<T>> fieldOrExpressionFunc)
        {
            var memberExpression = expression as MemberExpression;
            if (memberExpression != null)
            {
                return fieldOrExpressionFunc(memberExpression);
            }

            if (expression is MethodCallExpression)
            {
                var memberName = GetFieldNameFromMemberOrGetFieldNamed(expression);
                if (memberName != null)
                {
                    return new FieldOrExpression<T> { Expression = null, Field = new Field(memberName) };
                }
            }

            if (expression is UnaryExpression)
            {
                var unary = expression as UnaryExpression;
                return GetFieldExpression<T>(unary.Operand);
            }

            throw new NotImplementedException();
        }

        public static string GetFieldNameFromMemberOrGetFieldNamed(this Expression expression, string pathname)
        {

            return GetFieldNameFromMemberOrGetFieldNamed(expression, (s) => $"{pathname}.{s}");
        }


        private static string GetFieldNameFromMemberOrGetFieldNamed(Expression expression, Func<string, string> generateFieldName)
        {
            switch (expression)
            {
                case UnaryExpression unaryExpression when unaryExpression.NodeType == ExpressionType.Convert:
                    return GetFieldNameFromMemberOrGetFieldNamed(unaryExpression.Operand, generateFieldName);
                case MemberExpression memberExpression:
                {
                    if (memberExpression.Expression is MemberExpression)
                    {
                        var firstFieldname = GetFieldNameFromMemberOrGetFieldNamed(
                            memberExpression.Expression,
                            generateFieldName);
                        return
                            $"{firstFieldname}.{generateFieldName(memberExpression.Member.Name.FirstCharacterToLower())}";
                    }
                    else
                    {
                        return generateFieldName(memberExpression.Member.Name.FirstCharacterToLower());
                    }
                }
            }

            var name = generateFieldName(Names.GetNameFromGetFieldNamed(expression));
            if (name != null)
            {
                return name;
            }

            throw new InvalidOperationException($"Can't get a field name for {expression}");
        }


        private static Type GetPropertyType(Type input, string name)
        {
            foreach (PropertyInfo propertyInfo in input.GetProperties())
            {
                if (propertyInfo.Name.Equals(name, StringComparison.OrdinalIgnoreCase))
                {
                    var propertyType = propertyInfo.PropertyType;

                    if (propertyType.IsGenericType)
                    {
                        return propertyInfo.PropertyType.GetGenericArguments().First();
                    }

                    return propertyType;

                }
            }

            throw new NotImplementedException("Missing nested property");
        }
    }
}