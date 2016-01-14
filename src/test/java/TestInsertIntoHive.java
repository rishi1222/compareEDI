import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Created by rishikapoor on 29/10/2015.
 */
public class TestInsertIntoHive {

    @Test
    public void test_query_generation() throws Exception {

        String tableName1 = "testHiveDriverTable";
        String tableName2 = "temp_geographyvalues";

        String sql = "INSERT OVERWRITE TABLE " +  tableName2 + " select \n"+
                " (case when trim(s.predicate) = 'permId' \n"+
                " then trim(s.object) else null END ) as PermId, \n"+
                " (case when trim(p.predicate) = 'geographyLatitude' \n"+
                " then trim(p.object) else null END ) as GeographyLatitude, \n"+
                " (case when trim(q.predicate) = 'geographyLongitude' \n"+
                " then trim(q.object) else null END ) as GeographyLongitude, \n"+
                " (case when trim(r.predicate) = 'geographyUniqueName' \n"+
                " then trim(r.object) else null END ) as GeographyName \n"+
                " from \n"+
                "(select " + tableName1+".subject, " + tableName1+".predicate," + tableName1+".object from " + tableName1+" where trim(" + tableName1+".predicate) ='permId') s \n"+
                "left join (select " + tableName1+".subject, " + tableName1+".predicate," + tableName1+".object from " + tableName1+" where trim(" + tableName1+".predicate) ='geographyLatitude') p on \n"+
                "p.subject = s.subject \n"+
                "left join (select " + tableName1+".subject, " + tableName1+".predicate," + tableName1+".object from " + tableName1+" where trim(" + tableName1+".predicate) ='geographyLongitude') q on \n"+
                "q.subject = s.subject \n"+
                "left join (select " + tableName1+".subject, " + tableName1+".predicate," + tableName1+".object from " + tableName1+" where trim(" + tableName1+".predicate) ='geographyUniqueName') r on \n"+
                "r.subject = s.subject";

        final String query = "INSERT OVERWRITE TABLE temp_geographyvalues select \n" +
                " (case when trim(s.predicate) = 'permId' \n" +
                " then trim(s.object) else null END ) as PermId, \n" +
                " (case when trim(p.predicate) = 'geographyLatitude' \n" +
                " then trim(p.object) else null END ) as GeographyLatitude, \n" +
                " (case when trim(q.predicate) = 'geographyLongitude' \n" +
                " then trim(q.object) else null END ) as GeographyLongitude, \n" +
                " (case when trim(r.predicate) = 'geographyUniqueName' \n" +
                " then trim(r.object) else null END ) as GeographyName \n" +
                " from \n" +
                "(select testHiveDriverTable.subject, testHiveDriverTable.predicate,testHiveDriverTable.object from testHiveDriverTable where trim(testHiveDriverTable.predicate) ='permId') s \n" +
                "left join (select testHiveDriverTable.subject, testHiveDriverTable.predicate,testHiveDriverTable.object from testHiveDriverTable where trim(testHiveDriverTable.predicate) ='geographyLatitude') p on \n" +
                "p.subject = s.subject \n" +
                "left join (select testHiveDriverTable.subject, testHiveDriverTable.predicate,testHiveDriverTable.object from testHiveDriverTable where trim(testHiveDriverTable.predicate) ='geographyLongitude') q on \n" +
                "q.subject = s.subject \n" +
                "left join (select testHiveDriverTable.subject, testHiveDriverTable.predicate,testHiveDriverTable.object from testHiveDriverTable where trim(testHiveDriverTable.predicate) ='geographyUniqueName') r on \n" +
                "r.subject = s.subject";

        System.out.println(sql);
        System.out.println(query);
        assert(query.equals(sql));

    }

    @Test
    public void test_insert_overwrite() throws Exception {

        final String driverName = "org.apache.hive.jdbc.HiveDriver";

        final String query = "INSERT OVERWRITE TABLE  temp_geographyvalues select \n" +
                "(case when trim(s.predicate) = 'permId'\n" +
                "then trim(s.object) else null END ) as PermId,\n" +
                "(case when trim(p.predicate) = 'geographyLatitude'\n" +
                "then trim(p.object) else null END ) as GeographyLatitude,\n" +
                "(case when trim(q.predicate) = 'geographyLongitude'\n" +
                "then trim(q.object) else null END ) as GeographyLongitude,\n" +
                "(case when trim(r.predicate) = 'geographyUniqueName'\n" +
                "then trim(r.object) else null END ) as GeographyName\n" +
                " from\n" +
                "(select testHiveDriverTable.subject,testHiveDriverTable.predicate,testHiveDriverTable.object from testHiveDriverTable where trim(testHiveDriverTable.predicate) ='permId') s\n" +
                "left join (select testHiveDriverTable.subject,testHiveDriverTable.predicate,testHiveDriverTable.object from testHiveDriverTable where trim(testHiveDriverTable.predicate) ='geographyLatitude') p on \n" +
                "p.subject = s.subject\n" +
                "left join (select testHiveDriverTable.subject,testHiveDriverTable.predicate,testHiveDriverTable.object from testHiveDriverTable where trim(testHiveDriverTable.predicate) ='geographyLongitude') q on \n" +
                "q.subject = s.subject\n" +
                "left join (select testHiveDriverTable.subject,testHiveDriverTable.predicate,testHiveDriverTable.object from testHiveDriverTable where trim(testHiveDriverTable.predicate) ='geographyUniqueName') r on \n" +
                "r.subject = s.subject";

        Class.forName(driverName);

        Connection con = DriverManager.getConnection("jdbc:hive2://hadoop-titan.int.westgroup.com:10000/linked_data;user=platform;password=novus");
        Statement stmt = con.createStatement();
        stmt.executeUpdate(query);
        con.close();

    }
}