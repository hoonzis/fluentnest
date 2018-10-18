

namespace FluentNest
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Reflection;
    using System.Text;
   

    public static class ExpressionExtension
    {
        public static string GetFieldName(this Expression exp)
        {
            var binary = (BinaryExpression)exp;
            var fieldName = GetFieldNameFromMemberOrGetFieldNamed(binary.Left);
            return fieldName;
        }

        public static FieldOrExpression<T> GetFieldExpression<T>(this Expression expression)
        {
            // We don't generalize between the member & our special method as reconstructing a "fake" expression allow
            // NEST to use it's custom casing rules but we want a specific name in the other case
            // (specifying the string in Field ctor)
            var memberExpression = expression as MemberExpression;
            if (memberExpression != null)
            {
                var memberName = GetFieldNameFromMemberOrGetFieldNamed(memberExpression);
                var param = Expression.Parameter(typeof(T));
                var body = Expression.Convert(Expression.Property(param, memberName), typeof(object));
                var exp = Expression.Lambda<Func<T, object>>(body, param);
                return new FieldOrExpression<T> { Expression = exp, Field = null };
            }

            if (expression is MethodCallExpression)
            {
                var memberName = GetFieldNameFromMemberOrGetFieldNamed(expression);
                if (memberName != null)
                {
                    return new FieldOrExpression<T>(null,memberName);
                }
            }

            if (expression is UnaryExpression)
            {
                var unary = expression as UnaryExpression;
                return GetFieldExpression<T>(unary.Operand);
            }

            throw new NotImplementedException();
        }

        public static string GetFieldNameFromMemberOrGetFieldNamed(this Expression expression)
        {
            switch (expression)
            {
                case UnaryExpression unaryExpression when unaryExpression.NodeType == ExpressionType.Convert:
                    return GetFieldNameFromMemberOrGetFieldNamed(unaryExpression.Operand);
                case MemberExpression memberExpression:
                    return memberExpression.Member.Name.FirstCharacterToLower();
            }

            var name = expression.GetNameFromGetFieldNamed();
            if (name != null)
            {
                return name;
            }

            throw new InvalidOperationException($"Can't get a field name for {expression}");
        }

       
        public static string GenerateFilterName(this Expression expression)
        {
            var fromGetFieldName = expression.GetNameFromGetFieldNamed();
            if (fromGetFieldName != null)
            {
                return fromGetFieldName;
            }

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

            if (expType == ExpressionType.MemberAccess)
            {
                var memberExpression = (MemberExpression)expression;
                //here we handle binary expressions in the from .field.hasValue
                if (memberExpression.Member.Name == "HasValue")
                {
                    var parentFieldExpression = (memberExpression.Expression as MemberExpression);
                    var parentFieldName = parentFieldExpression.GetFieldNameFromMemberOrGetFieldNamed();
                    return parentFieldName + ".hasValue";
                }
                return memberExpression.GetFieldNameFromMemberOrGetFieldNamed();
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

            throw new NotImplementedException();
        }

        public static string GetNameFromGetFieldNamed(this Expression expression)
        {
            if (expression is UnaryExpression unaryExpression && unaryExpression.NodeType == ExpressionType.Convert)
            {
                return GetNameFromGetFieldNamed(unaryExpression.Operand);
            }

            if (!(expression is MethodCallExpression methodCall))
            {
                return null;
            }

            if (methodCall.Method.GetGenericMethodDefinition() != getFieldNamedMethod)
            {
                return null;
            }

            if (!(methodCall.Arguments[1] is Expression nameParam) || nameParam.Type != typeof(string))
            {
                return null;
            }

            return GetStringOf(nameParam);
        }

        private static MethodInfo getFieldNamedMethod = typeof(Names).GetMethod("GetFieldNamed");

        private static string GetStringOf(Expression expr)
        {
            if (expr.Type != typeof(string))
            {
                throw new InvalidOperationException("string only");
            }

            if (expr is ConstantExpression constant)
            {
                return (string)constant.Value;
            }

            if (expr is MemberExpression memberExpr && memberExpr.Expression is ConstantExpression target)
            {
                if (memberExpr.Member.MemberType == MemberTypes.Field)
                {
                    FieldInfo fi = (FieldInfo)memberExpr.Member;
                    return (string)fi.GetValue(target.Value);
                }
                else
                {
                    PropertyInfo pi = (PropertyInfo)memberExpr.Member;
                    return (string)pi.GetValue(target.Value);
                }
            }

            if (expr is BinaryExpression methodCall && methodCall.NodeType == ExpressionType.Add)
            {
                var left = GetStringOf(methodCall.Left);
                var right = GetStringOf(methodCall.Right);
                return left + right;
            }

            var lambda = (Expression<Func<string>>)Expression.Lambda(expr);
            return lambda.Compile().Invoke();
        }

    }
}
