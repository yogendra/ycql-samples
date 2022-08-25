package com.yugabyte.sample.apps;

import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParallelExecutor<T> {

  private static final Logger logger = LoggerFactory.getLogger(ParallelExecutor.class);
  private final String query;
  private final Session session;
  private final List<Object[]> arguments;
  private final RowMapper<T> rowMapper;

  public ParallelExecutor(String query, Session session, List<Object[]> arguments,
    RowMapper<T> rowMapper) {
    this.query = query;
    this.session = session;
    this.arguments = arguments;
    this.rowMapper = rowMapper;
  }
  public List<T> execute(){
    var result = Collections.synchronizedList(new ArrayList<T>());

    var preparedStatement = session.prepare(query);

   var futureResultList = new ArrayList<ResultSetFuture>();
    for(Object [] args : arguments){
      futureResultList.add(session.executeAsync(query, args));
    }

    return futureResultList.stream()
      .map(ResultSetFuture::getUninterruptibly)
      .flatMap(rs -> {
        return rs.all().stream();
      })
      .map(rowMapper::map)
      .collect(Collectors.toList());
  }
  public interface RowMapper<T>{
    T map(Row row);
  }
  public static class NoOpRowMapper implements RowMapper<Row>{
    @Override
    public Row map(Row row) {
      return row;
    }
  }
}

