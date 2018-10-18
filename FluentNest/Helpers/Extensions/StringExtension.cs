namespace FluentNest
{
    using System;
    using System.ComponentModel;

    public static class StringExtension
    {
        public static T ParseEnum<T>(this string value)
        {
            return (T)Enum.Parse(typeof(T), value);
        }

        public static K StringToAnything<K>(this string value)
        {
            if ((typeof(K).IsEnum))
            {
                return ParseEnum<K>(value);
            }
            else
            {
                TypeConverter typeConverter = TypeDescriptor.GetConverter(typeof(K));
                return (K)typeConverter.ConvertFromString(value);
            }
        }

        public static string FirstCharacterToLower(this string str)
        {
            if (string.IsNullOrEmpty(str) || char.IsLower(str, 0))
                return str;

            return char.ToLowerInvariant(str[0]) + str.Substring(1);
        }
    }    
}
