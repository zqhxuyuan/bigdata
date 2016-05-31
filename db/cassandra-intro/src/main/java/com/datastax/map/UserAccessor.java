package com.datastax.map;

import java.util.UUID;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.mapping.Result;
import com.datastax.driver.mapping.annotations.Accessor;
import com.datastax.driver.mapping.annotations.Param;
import com.datastax.driver.mapping.annotations.Query;
import com.google.common.util.concurrent.ListenableFuture;

@Accessor
public interface UserAccessor {
    @Query("SELECT * FROM complex.users WHERE id = :id")
    User getUserNamed(@Param("id") UUID userId);

    @Query("SELECT * FROM complex.users WHERE id = ?")
    User getOnePosition(@Param("id") UUID userId);

    @Query("UPDATE complex.users SET addresses[:name] = :address WHERE id = :id")
    ResultSet addAddress(@Param("id") UUID userId, @Param("name") String addressName, @Param("address") Address address);

    @Query("SELECT * FROM complex.users")
    public Result<User> getAll();

    @Query("SELECT * FROM complex.users")
    public ListenableFuture<Result<User>> getAllAsync();
}