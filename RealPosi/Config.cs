using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace RealPosi
{
    public static class Config
    {
        static string strFilePath = "Config.cfg";  //配置文件路径
        static SortedList<string, string> AllConfig_Sort = new SortedList<string, string>();   //所有配置项的集合

        public static void Load_ConfigFile()
        {
            FileStream fs = new FileStream(strFilePath, FileMode.Open);
            StreamReader fr = new StreamReader(fs);
            string line;
            while ((line = fr.ReadLine()) != null)
            {
                line = line.Replace(" ", "").Trim();
                if (line.IndexOf('#') != -1)
                    line = line.Substring(0, line.IndexOf('#'));
                if (!line.StartsWith("#") && line.Length > 1)
                {
                    //Console.WriteLine(line.ToString());
                    int ofIndex = line.IndexOf('=');
                    string key = line.Substring(0, ofIndex).Trim();
                    string value = line.Substring(ofIndex, line.Length - ofIndex).Trim().TrimStart('=').Trim().Trim('"');
                    //Console.WriteLine(key + "|" + value);
                    AllConfig_Sort.Add(key, value);
                }
            }
        }

        public static string Load_MySQLConnection()
        {
            return AllConfig_Sort["MySQLConnection"].ToString();
        }




    }
}
