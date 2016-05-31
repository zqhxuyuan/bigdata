package com.datastax.docs;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.*;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

import java.util.List;

/**
 * Created by zhengqh on 16/5/31.
 */
public class DynamicQueryDSL extends SimpleClient{

    public List<Row> getRows(String keyspace, String table) {
        Statement statement = QueryBuilder
                .select()
                .all()
                .from(keyspace, table);
        return getSession()
                .execute(statement)
                .all();
    }

    /*
    public void getRows(){
        Select select = QueryBuilder.select()
                .all()
                .distinct()
                .from("addressbook", "contact")
                .where(eq("type", "Friend"))
                .and(eq("city", "San Francisco"));
        ResultSet results = getSession().execute(select);
    }
    */

    public void insert(){
        Insert insert = QueryBuilder
                .insertInto("addressbook", "contact")
                .value("firstName", "Dwayne")
                .value("lastName", "Garcia")
                .value("email", "dwayne@example.com");
        ResultSet results = getSession().execute(insert);
    }

    public void update(){
        Update.Where where = QueryBuilder.update("addressbook", "contact")
                .with(set("email", "dwayne.garcia@example.com")).where(eq("username", "dgarcia"));
        ResultSet results = getSession().execute(where);
    }

    public void delete(){
        Delete delete = QueryBuilder.delete()
                .from("addresbook", "contact");
        delete.where(eq("username", "dgarcia"));

    }

}
