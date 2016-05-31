package com.datastax.map;

import com.datastax.docs.SimpleClient;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.Result;
import com.datastax.driver.mapping.UDTMapper;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by zhengqh on 16/5/31.
 */
public class MappingClient extends SimpleClient{

    public static void main(String[] args) {

    }

    public void crud(){
        Mapper<Account> mapper = new MappingManager(getSession()).mapper(Account.class);
        Phone phone = new Phone("home", "707-555-3537");
        List<Phone> phones = new ArrayList<Phone>();
        phones.add(phone);
        Address address = new Address("25800 Arnold Drive", "Sonoma", 95476, phones);
        Account account = new Account("John Doe", "jd@example.com", address);
        mapper.save(account);

        Account whose = mapper.get("jd@example.com");
        System.out.println("Account name: " + whose.getName());
        mapper.delete(account);
    }

    public void udt(){
        UDTMapper<Address> mapper = new MappingManager(getSession())
                .udtMapper(Address.class);

        ResultSet results = getSession().execute("SELECT * FROM complex.users " +
                "WHERE id = 756716f7-2e54-4715-9f00-91dcbea6cf50;");
        for (Row row : results) {
            System.out.println(row.getString("name"));
            Map<String, UDTValue> addresses = row.getMap("addresses", String.class, UDTValue.class);
            for (String key : addresses.keySet()) {
                Address address = mapper.fromUDT(addresses.get(key));
                System.out.println(key + " address: " + address);
            }
        }
    }

    public void accesor(){
        MappingManager manager = new MappingManager (getSession());
        Address address = new Address();
        UserAccessor userAccessor = manager.createAccessor(UserAccessor.class);
        Result<User> users = userAccessor.getAll();
        for(User user : users) {
            System.out.println(user.getName());
        }
    }

}
