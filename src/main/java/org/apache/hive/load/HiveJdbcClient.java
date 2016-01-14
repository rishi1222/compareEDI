package org.apache.hive.load;

/**
 * Created by rishikapoor on 14/10/2015.
 */



/**
 * Created by rishikapoor on 05/10/2015.
 */


import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;
import org.apache.jena.service.Base;

public class HiveJdbcClient extends Base {

    private static String driverName = "org.apache.hive.jdbc.HiveDriver";


    public void run() {

        try {
            try {
                Class.forName(driverName);
            } catch (ClassNotFoundException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                System.exit(1);
            }
            Connection con = DriverManager.getConnection("jdbc:hive2://hadoop-titan.int.westgroup.com:10000/linked_data;user=platform;password=novus");
            String xmllanguage = "XMLLanguage";
            String xmllanguagename = "XMLLanguageName";
            String rdflanguage = "RDFLanguage";
            String rdflanguagename = "RDFLanguageName";

            final Statement stmt = con.createStatement();
            ResultSet res = null;

            stmt.executeUpdate("drop table " + xmllanguage);
            stmt.executeUpdate("drop table " + xmllanguagename);
            stmt.executeUpdate("drop table " + rdflanguage);
            stmt.executeUpdate("drop table " + rdflanguagename);

            stmt.executeUpdate("create table " + xmllanguage + " (LanguageId int, BaseLanguageId int, LanguageUniqueName string, LanguageScriptId int , LanguageGeographyId int ,activeFrom string) row format delimited fields terminated by ';' stored as textfile ");
            stmt.executeUpdate("create table " + xmllanguagename + " (effectiveFrom string, LangVal string, LanguageNameType string,Language string, LanguageNameSequence String, languageNameMetadataViewId String) row format delimited fields terminated by ';' stored as textfile ");
            stmt.executeUpdate("create table " + rdflanguage + " (LanguageId String, activeFrom string, BaseLanguageId int, LanguageGeographyId int, LanguageUniqueName String, LanguageScriptId int) row format delimited fields terminated by ';' stored as textfile ");
            stmt.executeUpdate("create table " + rdflanguagename + " (effectiveFrom string, LangVal string, LanguageNameType string,Language string, LanguageNameSequence String, languageNameMetadataViewId String) row format delimited fields terminated by ';' stored as textfile ");


            // show tables
            String sql = "show tables '" + xmllanguage + "'";
            System.out.println("Running: " + sql);
            res = stmt.executeQuery(sql);

            if (res.next()) {
                System.out.println(res.getString(1));
            }

            // load data into table
            // NOTE: filepath has to be local to the hive server
            // NOTE: /tmp/a.txt is a ctrl-A separated file with two fields per line

            String filepath1 = "'/user/platform/rishi_langXml/Lang-r-00000'";
            sql = "LOAD DATA INPATH " + filepath1 + " OVERWRITE INTO TABLE " + xmllanguage;
            System.out.println("Running: " + sql);
            stmt.executeUpdate(sql);


            // regular hive query
            sql = "select count(1) from " + xmllanguage;
            System.out.println("Running: " + sql);
            res = stmt.executeQuery(sql);
            while (res.next()) {
                System.out.println(res.getString(1));
            }

            String filepath = "'/user/platform/rishi_langXml/LangName-r-00000'";
            sql = "LOAD DATA INPATH " + filepath + " OVERWRITE INTO TABLE " + xmllanguagename;
            System.out.println("Running: " + sql);
            stmt.executeUpdate(sql);


            sql = "select count(1) from " + xmllanguagename;
            System.out.println("Running: " + sql);
            res = stmt.executeQuery(sql);
            while (res.next()) {
                System.out.println(res.getString(1));
            }


            String filepath2 = "'/user/platform/rishi_langRdf/Lang-r-00000'";
            sql = "LOAD DATA INPATH " + filepath2 + " OVERWRITE INTO TABLE " + rdflanguage;
            System.out.println("Running: " + sql);
            stmt.executeUpdate(sql);


            sql = "select count(1) from " + rdflanguage;
            System.out.println("Running: " + sql);
            res = stmt.executeQuery(sql);
            while (res.next()) {
                System.out.println(res.getString(1));
            }

            String filepath3 = "'/user/platform/rishi_langRdf/LangName-r-00000'";
            sql = "LOAD DATA INPATH " + filepath3 + " OVERWRITE INTO TABLE " + rdflanguagename;
            System.out.println("Running: " + sql);
            stmt.executeUpdate(sql);


            sql = "select count(1) from " + rdflanguagename;
            System.out.println("Running: " + sql);
            res = stmt.executeQuery(sql);
            while (res.next()) {
                System.out.println(res.getString(1));
            }


                    System.out.println("Test case for LanguageName");
                    sql = "select count(1) from xmllanguagename xml \n" +
                            "join rdflanguagename rdf\n" +
                            "on trim(rdf.effectiveFrom) = trim(xml.effectiveFrom)\n" +
                            "and trim(rdf.LangVal) = trim(xml.LangVal)\n" +
                            "and trim(rdf.LanguageNameType) = trim(xml.LanguageNameType)\n" +
                            "and trim(rdf.Language) = trim(xml.Language)\n" +
                            "and trim(rdf.LanguageNameSequence) = trim(xml.LanguageNameSequence)\n" +
                            "and trim(rdf.languageNameMetadataViewId) = trim(xml.languageNameMetadataViewId)";


                    System.out.println("Running: " + sql);
                    res = stmt.executeQuery(sql);
                    while (res.next()) {
                        System.out.println(res.getString(1));
                    }

                    System.out.println("Mismatch Rows for LanguageName");
                    sql = "select * from xmllanguagename xml \n" +
                            "left outer join rdflanguagename rdf\n" +
                            "on trim(rdf.effectiveFrom) = trim(xml.effectiveFrom)\n" +
                            "and trim(rdf.LangVal) = trim(xml.LangVal)\n" +
                            "and trim(rdf.LanguageNameType) = trim(xml.LanguageNameType)\n" +
                            "and trim(rdf.Language) = trim(xml.Language)\n" +
                            "and trim(rdf.LanguageNameSequence) = trim(xml.LanguageNameSequence)\n" +
                            "and trim(rdf.languageNameMetadataViewId) = trim(xml.languageNameMetadataViewId)\n" +
                            "where rdf.LangVal is null or xml.LangVal is null\n" +
                            "   or rdf.LanguageNameType is null or xml.LanguageNameType is null\n" +
                            "       or rdf.Language is null or xml.Language is null   \n" +
                            "       or rdf.LanguageNameSequence is null or xml.LanguageNameSequence is null\n" +
                            "        or rdf.languageNameMetadataViewId is null or xml.languageNameMetadataViewId is null";


                    System.out.println("Running: " + sql);
                    res = stmt.executeQuery(sql);
                    while (res.next()) {
                        System.out.println(res.getString(1));
                    }

                    System.out.println("Test Case For Language");
                    sql = "select count(1) from xmllanguage xml\n" +
                            "join rdflanguage rdf\n" +
                            "on xml.languageid = rdf.languageid";


                    System.out.println("Running: " + sql);
                    res = stmt.executeQuery(sql);
                    while (res.next()) {
                        System.out.println(res.getString(1));
                    }


                    System.out.println("Mismatch Rows for Language");
                    sql = " select xml.languageid , rdf.languageid , \n" +
                            "xml.activefrom, rdf.activefrom,\n" +
                            "xml.baselanguageid ,rdf.baselanguageid ,\n" +
                            "xml.languageuniquename, rdf.languageuniquename,\n" +
                            "xml.languagescriptid, rdf.languagescriptid,\n" +
                            "xml.languagegeographyid, rdf.languagegeographyid\n" +
                            "from xmllanguage xml\n" +
                            "join rdflanguage rdf\n" +
                            "on xml.languageid = trim(rdf.languageid)\n" +
                            "where  trim(xml.activefrom) <> trim(rdf.activefrom)\n" +
                            "or xml.baselanguageid <> rdf.baselanguageid\n" +
                            "or trim(xml.languageuniquename) <> trim(rdf.languageuniquename)\n" +
                            "or xml.languagescriptid <> rdf.languagescriptid\n" +
                            "or xml.languagegeographyid <> rdf.languagegeographyid";


                    System.out.println("Running: " + sql);
                    res = stmt.executeQuery(sql);
                    while (res.next()) {
                        System.out.println(res.getString(1)+","+ res.getString(2)+","+res.getString(3)+","+res.getString(4)+","+res.getString(5)+","+res.getString(6));
                    }

//
//
//                    System.out.println("Mismatch Rows Geography Name");
//                    sql = " select count(*) from xmllanguagename xml \n" +
//                            "full outer join rdflanguagename rdf\n" +
//                            "on trim(rdf.effectiveFrom) = trim(xml.effectiveFrom)\n" +
//                            "and trim(rdf.LangVal) = trim(xml.LangVal)\n" +
//                            "and trim(rdf.LanguageNameType) = trim(xml.LanguageNameType)\n" +
//                            "and trim(rdf.Language) = trim(xml.Language)\n" +
//                            "and trim(rdf.LanguageNameSequence) = trim(xml.LanguageNameSequence)\n" +
//                            "and trim(rdf.languageNameMetadataViewId) = trim(xml.languageNameMetadataViewId)\n" +
//                            "where rdf.LangVal is null or xml.LangVal is null\n" +
//                            "   or rdf.LanguageNameType is null or xml.LanguageNameType is null\n" +
//                            "       or rdf.Language is null or xml.Language is null   \n" +
//                            "       or rdf.LanguageNameSequence is null or xml.LanguageNameSequence is null\n" +
//                            "        or rdf.languageNameMetadataViewId is null or xml.languageNameMetadataViewId is null";
////
//
//                    System.out.println("Running: " + sql);
//                    res = stmt.executeQuery(sql);
//                    while (res.next()) {
//                        System.out.println(res.getString(1));
//                    }
//                    //stmt.executeUpdate("drop table " + tableName);
//                    con.close();
               } catch (Exception e) {
                   e.printStackTrace();
              }


        }
    }











