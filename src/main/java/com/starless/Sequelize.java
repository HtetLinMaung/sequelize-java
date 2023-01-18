package com.starless;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import lombok.Data;

@Data
public class Sequelize {
    private final Connection connection;
    private boolean autoclose = true;

    public Sequelize(String url) throws SQLException {
        connection = DriverManager.getConnection(url);
    }

    public Sequelize(String url, String username, String password) throws SQLException {
        connection = DriverManager.getConnection(url, username, password);
    }

    public void closeConnection() throws SQLException {
        connection.close();
    }

    public CompletableFuture<QueryResult> query(String sql) {
        return query(sql, QueryOptions.builder().build());
    }

    public CompletableFuture<QueryResult> query(String sql, QueryOptions options) {
        return query(sql, options, (m) -> m);
    }

    public CompletableFuture<QueryResult> query(String sql, QueryOptions options, ICallBack icb) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return querySync(sql, options, icb);
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        });
    }

    public QueryResult querySync(String sql) throws SQLException {
        return querySync(sql, QueryOptions.builder().build());
    }

    public QueryResult querySync(String sql, QueryOptions options) throws SQLException {
        return querySync(sql, options, (m) -> m);
    }

    public QueryResult querySync(String sql, QueryOptions options, ICallBack icb) throws SQLException {
        System.out.println(sql);
        try (PreparedStatement stmt = connection.prepareStatement(sql);) {
            String lsql = sql.toLowerCase().trim();
            if (lsql.contains("select") && !lsql.startsWith("insert") && !lsql.startsWith("update")
                    && !lsql.startsWith("delete")) {
                List<Map<String, Object>> datalist = new ArrayList<>();
                int i = 1;
                for (Object value : options.getBind()) {
                    stmt.setObject(i++, value);
                }
                ResultSet rs = stmt.executeQuery();
                while (rs.next()) {
                    Map<String, Object> data = new HashMap<>();
                    for (i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
                        data.put(rs.getMetaData().getColumnLabel(i).toLowerCase(), rs.getObject(i));
                    }
                    datalist.add(icb.cb(data));
                }
                if (autoclose) {
                    closeConnection();
                }
                return QueryResult.builder().data(datalist).build();
            } else {
                int i = 1;
                for (Object value : options.getBind()) {
                    stmt.setObject(i++, value);
                }
                int affectedRows = stmt.executeUpdate();
                if (autoclose) {
                    closeConnection();
                }
                return QueryResult.builder().affectedRows(affectedRows).build();
            }
        }
    }

    public List<Map<String, Object>> findAllSync(String table) throws SQLException {
        return findAllSync(table, FindOptions.builder().build());
    }

    public List<Map<String, Object>> findAllSync(String table, FindOptions options) throws SQLException {
        return findAllSync(table, options, (m) -> m);
    }

    private String getAliasKey(String key) {
        if (key.isEmpty()) {
            return key;
        }
        if (key.contains(" as ")) {
            return key.split(" as ")[key.split(" as ").length - 1].trim();
        }
        if (key.contains(" AS ")) {
            return key.split(" AS ")[key.split(" AS ").length - 1].trim();
        }
        if (key.contains(" ")) {
            return key.split(" ")[key.split(" ").length - 1].trim();
        }
        return key;
    }

    private boolean isList(Object value) {
        return value instanceof List;
    }

    private boolean isString(Object value) {
        return value instanceof String;
    }

    private boolean isMap(Object value) {
        return value instanceof Map;
    }

    public String getWhereConditions(Map<String, Object> where, QueryOptions options, String parentKey) {
        List<Object> bind = options.getBind();
        List<String> conditions = new ArrayList<>();
        for (Entry<String, Object> entry : where.entrySet()) {
            if (entry.getKey().equals("$or")) {
                List<Map<String, Object>> orList = (List<Map<String, Object>>) entry.getValue();
                conditions.add("(" + orList.stream()
                        .map(m -> "(" + getWhereConditions(m, options, entry.getKey()) + ")")
                        .collect(Collectors.joining(" or ")) + ")");
            } else if (entry.getKey().equals("$not")) {
                List<Map<String, Object>> notList = (List<Map<String, Object>>) entry.getValue();
                conditions.add("not (" + notList.stream()
                        .map(m -> "(" + getWhereConditions(m, options, entry.getKey()) + ")")
                        .collect(Collectors.joining(" and ")) + ")");
            } else if (entry.getKey().equals("$gt")) {
                conditions.add(parentKey + " > ?");
                bind.add(entry.getValue());
            } else if (entry.getKey().equals("$lt")) {
                conditions.add(parentKey + " < ?");
                bind.add(entry.getValue());
            } else if (entry.getKey().equals("$gte")) {
                conditions.add(parentKey + " >= ?");
                bind.add(entry.getValue());
            } else if (entry.getKey().equals("$lte")) {
                conditions.add(parentKey + " <= ?");
                bind.add(entry.getValue());
            } else if (entry.getKey().equals("$ne")) {
                conditions.add(parentKey + " <> ?");
                bind.add(entry.getValue());
            } else if (entry.getKey().equals("$like")) {
                conditions.add(parentKey + " like ?");
                bind.add(entry.getValue());
            } else if (entry.getKey().equals("$notlike")) {
                conditions.add(parentKey + " not like ?");
                bind.add(entry.getValue());
            } else if (entry.getKey().equals("$startswith")) {
                conditions.add(parentKey + " like ?");
                bind.add(entry.getValue() + "%");
            } else if (entry.getKey().equals("$endswith")) {
                conditions.add(parentKey + " like ?");
                bind.add("%" + entry.getValue());
            } else if (entry.getKey().equals("$substring")) {
                conditions.add(parentKey + " like ?");
                bind.add("%" + entry.getValue() + "%");
            } else if (entry.getKey().equals("$is")) {
                if (entry.getValue() == null) {
                    conditions.add(parentKey + " is null");
                } else {
                    conditions.add(parentKey + " is not null");
                }
            } else if (entry.getKey().equals("$between")) {
                List<Object> between = (List<Object>) entry.getValue();
                conditions.add(parentKey + " between ? and ?");
                bind.addAll(between);
            } else if (entry.getKey().equals("$notbetween")) {
                List<Object> between = (List<Object>) entry.getValue();
                conditions.add(parentKey + " not between ? and ?");
                bind.addAll(between);
            } else {
                if (isMap(entry.getValue())) {
                    conditions.add(getWhereConditions((Map<String, Object>) entry.getValue(), options, entry.getKey()));
                } else if (isList(entry.getValue())) {
                    List<Object> list = (List<Object>) entry.getValue();
                    if (list.size() == 1) {
                        conditions.add(entry.getKey() + " = ?");
                        bind.add(list.get(0));
                    } else if (list.size() > 1) {
                        conditions.add(entry.getKey() + " in ("
                                + list.stream().map(v -> isString(v) ? "'" + v + "'" : String.valueOf(v))
                                        .collect(Collectors.joining(", "))
                                + ")");
                    }
                } else {
                    conditions.add(entry.getKey() + " = ?");
                    bind.add(entry.getValue());
                }

            }

        }
        options.setBind(bind);
        return conditions.stream().collect(Collectors.joining(" and "));
    }

    public List<Map<String, Object>> findAllSync(String table, FindOptions options, ICallBack icb) throws SQLException {
        String sql = "select "
                + (options.getSelect().size() > 0
                        ? options.getSelect().stream().collect(Collectors.joining(", "))
                        : "*")
                + " from " + table + " ";

        QueryOptions queryOptions = QueryOptions.builder().build();
        if (!options.getWhere().isEmpty()) {
            sql += "where "
                    + getWhereConditions(options.getWhere(), queryOptions, "")
                    + " ";
        }
        if (options.getGroup().size() > 0) {
            sql += "group by " + options.getGroup().stream().collect(Collectors.joining(", ")) + " ";
        }
        if (!options.getOrder().isEmpty()) {
            sql += "order by "
                    + options.getOrder().entrySet().stream().map(s -> getAliasKey(s.getKey()) + " " + s.getValue())
                            .collect(Collectors.joining(", "))
                    + " ";
        }
        if (options.getLimit() != 0) {
            sql += String.format("offset %d rows fetch first %d rows only ", options.getOffset(), options.getLimit());
        }
        return querySync(sql, queryOptions, icb).getData();
    }

    public CompletableFuture<List<Map<String, Object>>> findAll(String table, FindOptions options, ICallBack icb) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return findAllSync(table, options, icb);
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        });
    }

    public CompletableFuture<List<Map<String, Object>>> findAll(String table, FindOptions options) {
        return findAll(table, options, (m) -> m);
    }

    public CompletableFuture<List<Map<String, Object>>> findAll(String table) {
        return findAll(table, FindOptions.builder().build());
    }

    public Optional<Map<String, Object>> findOneSync(String table) throws SQLException {
        return findOneSync(table, FindOptions.builder().build());
    }

    public Optional<Map<String, Object>> findOneSync(String table, FindOptions options) throws SQLException {
        return findAllSync(table, options).stream().findFirst();
    }

    public CompletableFuture<Optional<Map<String, Object>>> findOne(String table, FindOptions options) {
        return findAll(table, options).thenApply(l -> l.stream().findFirst());
    }

    public CompletableFuture<Optional<Map<String, Object>>> findOne(String table) {
        return findOne(table, FindOptions.builder().build());
    }

    public Map<String, Object> createSync(String table, Map<String, Object> data) throws SQLException {
        return createSync(table, data, "id");
    }

    public Map<String, Object> createSync(String table, Map<String, Object> data, String pk) throws SQLException {
        return createSync(table, data, pk, CreateOptions.builder().build());
    }

    public Map<String, Object> createSync(String table, Map<String, Object> data, String pk, CreateOptions options)
            throws SQLException {
        boolean oldAutoClose = autoclose;
        setAutoclose(false);

        List<String> keys = new ArrayList<>();
        if (options.getFields().size() > 0) {
            keys = data.keySet().stream()
                    .filter(k -> options.getFields().contains(k)).collect(Collectors.toList());
        } else {
            keys = data.keySet().stream().collect(Collectors.toList());
        }

        querySync(String.format("insert into %s (%s) values (%s)", table,
                String.join(",", keys),
                String.join(",", keys.stream().map(k -> "?").collect(Collectors.toList()))),
                QueryOptions.builder().bind(keys.stream().map(k -> data.get(k)).collect(Collectors.toList())).build());
        if (data.containsKey(pk)) {
            Map<String, Object> where = new HashMap<>();
            where.put(pk, data.get(pk));
            return findOneSync(table, FindOptions.builder().where(where).build()).orElse(data);
        }
        if (oldAutoClose) {
            closeConnection();
        }
        setAutoclose(oldAutoClose);
        return data;
    }

    public CompletableFuture<Map<String, Object>> create(String table, Map<String, Object> data) {
        return create(table, data, "id");
    }

    public CompletableFuture<Map<String, Object>> create(String table, Map<String, Object> data, String pk) {
        return create(table, data, pk, CreateOptions.builder().build());
    }

    public CompletableFuture<Map<String, Object>> create(String table, Map<String, Object> data, String pk,
            CreateOptions options) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return createSync(table, data, pk, options);
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        });
    }

    public FindOrCreateResult findOrCreateSync(String table, FindOptions options) throws SQLException {
        return findOrCreateSync(table, options, "id");
    }

    public FindOrCreateResult findOrCreateSync(String table, FindOptions options, String pk) throws SQLException {
        boolean oldAutoClose = autoclose;
        setAutoclose(false);
        Optional<Map<String, Object>> opt = findOneSync(table, options);
        if (!opt.isPresent()) {
            Map<String, Object> newData = createSync(table, options.getDefaults(), pk);
            if (oldAutoClose) {
                closeConnection();
            }
            setAutoclose(oldAutoClose);
            return FindOrCreateResult
                    .builder()
                    .newData(Optional.of(newData))
                    .oldData(Optional.empty())
                    .build();
        } else {
            if (oldAutoClose) {
                closeConnection();
            }
            setAutoclose(oldAutoClose);
            return FindOrCreateResult
                    .builder()
                    .oldData(opt)
                    .newData(Optional.empty())
                    .build();
        }

    }

    public CompletableFuture<FindOrCreateResult> findOrCreate(String table,
            FindOptions options) {
        return findOrCreate(table, options, "id");
    }

    public CompletableFuture<FindOrCreateResult> findOrCreate(String table,
            FindOptions options, String pk) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return findOrCreateSync(table, options, pk);
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        });
    }

    public List<Map<String, Object>> bulkCreateSync(String table, List<Map<String, Object>> datalist)
            throws SQLException, InterruptedException, ExecutionException {
        return bulkCreateSync(table, datalist, "id");
    }

    public List<Map<String, Object>> bulkCreateSync(String table, List<Map<String, Object>> datalist, String pk)
            throws SQLException, InterruptedException, ExecutionException {
        return bulkCreateSync(table, datalist, pk, CreateOptions.builder().build());
    }

    public List<Map<String, Object>> bulkCreateSync(String table, List<Map<String, Object>> datalist, String pk,
            CreateOptions options)
            throws SQLException, InterruptedException, ExecutionException {
        boolean oldAutoClose = autoclose;
        setAutoclose(false);
        List<CompletableFuture<Map<String, Object>>> futures = new ArrayList<>();
        List<Map<String, Object>> results = new ArrayList<>();

        if (options.isUnorder()) {
            for (Map<String, Object> data : datalist) {
                futures.add(create(table, data, pk, options));
            }
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).get();
            results.addAll(futures.stream().map(f -> {
                try {
                    return f.get();
                } catch (Exception e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
            }).collect(Collectors.toList()));
        } else {
            for (Map<String, Object> data : datalist) {
                results.add(createSync(table, data, pk, options));
            }
        }
        if (oldAutoClose) {
            closeConnection();
        }
        setAutoclose(oldAutoClose);
        return results;
    }

    public CompletableFuture<List<Map<String, Object>>> bulkCreate(String table,
            List<Map<String, Object>> datalist) {
        return bulkCreate(table, datalist, "id");
    }

    public CompletableFuture<List<Map<String, Object>>> bulkCreate(String table,
            List<Map<String, Object>> datalist, String pk) {
        return bulkCreate(table, datalist, pk, CreateOptions.builder().build());
    }

    public CompletableFuture<List<Map<String, Object>>> bulkCreate(String table,
            List<Map<String, Object>> datalist, String pk, CreateOptions options) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return bulkCreateSync(table, datalist, pk, options);
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        });
    }

    public List<Map<String, Object>> updateSync(String table, Map<String, Object> data) throws SQLException {
        return updateSync(table, data, FindOptions.builder().build());
    }

    public List<Map<String, Object>> updateSync(String table, Map<String, Object> data, FindOptions options)
            throws SQLException {
        return updateSync(table, data, options, false);
    }

    public List<Map<String, Object>> updateSync(String table, Map<String, Object> data, FindOptions options,
            boolean noSelectQuery)
            throws SQLException {
        String sql = String.format("update %s set %s ", table,
                data.keySet().stream().map(k -> k + " = ?").collect(Collectors.joining(", ")));
        QueryOptions queryOptions = QueryOptions
                .builder()
                .bind(data.entrySet().stream().map(s -> s.getValue()).collect(Collectors.toList()))
                .build();
        if (!options.getWhere().isEmpty()) {
            sql += "where "
                    + getWhereConditions(options.getWhere(), queryOptions, "")
                    + " ";
        }
        boolean oldAutoClose = autoclose;
        if (!noSelectQuery)
            setAutoclose(false);

        querySync(sql, queryOptions);

        if (noSelectQuery) {
            return Arrays.asList(data);
        }

        queryOptions = QueryOptions
                .builder()
                .build();
        sql = String.format("select * from %s ", table);
        if (!options.getWhere().isEmpty()) {
            sql += "where "
                    + getWhereConditions(options.getWhere(), queryOptions, "")
                    + " ";
        }
        List<Map<String, Object>> datalist = querySync(sql, queryOptions).getData();
        if (oldAutoClose) {
            closeConnection();
        }
        setAutoclose(oldAutoClose);
        return datalist;
    }

    public CompletableFuture<List<Map<String, Object>>> update(String table, Map<String, Object> data) {
        return update(table, data, FindOptions.builder().build());
    }

    public CompletableFuture<List<Map<String, Object>>> update(String table, Map<String, Object> data,
            FindOptions options) {
        return update(table, data, options, false);
    }

    public CompletableFuture<List<Map<String, Object>>> update(String table, Map<String, Object> data,
            FindOptions options, boolean noSelectQuery) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return updateSync(table, data, options, noSelectQuery);
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        });
    }

    public int destroySync(String table) throws SQLException {
        return destroySync(table, FindOptions.builder().build());
    }

    public int destroySync(String table, FindOptions options) throws SQLException {
        String sql = "";
        if (options.isTruncate()) {
            sql = String.format("truncate table %s ", table);
            return querySync(sql).getAffectedRows();
        } else {
            sql = String.format("delete from %s ", table);
            QueryOptions queryOptions = QueryOptions
                    .builder()
                    .build();
            if (!options.getWhere().isEmpty()) {
                sql += "where "
                        + getWhereConditions(options.getWhere(), queryOptions, "")
                        + " ";
            }
            return querySync(sql, queryOptions).getAffectedRows();
        }
    }

    public CompletableFuture<Integer> destroy(String table) {
        return destroy(table, FindOptions.builder().build());
    }

    public CompletableFuture<Integer> destroy(String table, FindOptions options) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return destroySync(table, options);
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        });
    }

    public long countSync(String table) throws SQLException {
        return countSync(table, FindOptions.builder().build());
    }

    public long countSync(String table, FindOptions options) throws SQLException {
        return Long.parseLong(String.valueOf(findOneSync(table, FindOptions.builder()
                .select(Arrays.asList("count(*) as count"))
                .limit(0)
                .offset(0)
                .where(options.getWhere())
                .defaults(options.getDefaults())
                .truncate(options.isTruncate())
                .group(options.getGroup())
                .build()).get().get("count")));
    }

    public CompletableFuture<Long> count(String table) {
        return count(table, FindOptions.builder().build());
    }

    public CompletableFuture<Long> count(String table, FindOptions options) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return countSync(table, options);
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        });
    }

    public int maxSync(String table, String column) throws SQLException {
        return maxSync(table, column, FindOptions.builder().build());
    }

    public int maxSync(String table, String column, FindOptions options) throws SQLException {
        options.setSelect(Arrays.asList(String.format("max(%s) as max", column)));
        return (int) findOneSync(table, options).get().get("max");
    }

    public CompletableFuture<Integer> max(String table, String column) {
        return max(table, column, FindOptions.builder().build());
    }

    public CompletableFuture<Integer> max(String table, String column, FindOptions options) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return maxSync(table, column, options);
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        });
    }

    public int minSync(String table, String column) throws SQLException {
        return minSync(table, column, FindOptions.builder().build());
    }

    public int minSync(String table, String column, FindOptions options) throws SQLException {
        options.setSelect(Arrays.asList(String.format("min(%s) as min", column)));
        return (int) findOneSync(table, options).get().get("min");
    }

    public CompletableFuture<Integer> min(String table, String column) {
        return min(table, column, FindOptions.builder().build());
    }

    public CompletableFuture<Integer> min(String table, String column, FindOptions options) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return minSync(table, column, options);
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        });
    }

    public List<Map<String, Object>> incrementSync(String table, Map<String, Object> data)
            throws SQLException {
        return incrementSync(table, data, FindOptions.builder().build());
    }

    public List<Map<String, Object>> incrementSync(String table, Map<String, Object> data, FindOptions options)
            throws SQLException {
        String sql = String.format("update %s set %s ", table,
                data.entrySet().stream().map(s -> s.getKey() + " + " + String.valueOf(s.getValue()))
                        .collect(Collectors.joining(", ")));
        QueryOptions queryOptions = QueryOptions.builder().build();
        if (!options.getWhere().isEmpty()) {
            sql += "where "
                    + getWhereConditions(options.getWhere(), queryOptions, "")
                    + " ";
        }
        boolean oldAutoClose = autoclose;
        setAutoclose(false);
        querySync(sql, queryOptions);

        queryOptions = QueryOptions
                .builder()
                .build();
        sql = String.format("select * from %s ", table);
        if (!options.getWhere().isEmpty()) {
            sql += "where "
                    + getWhereConditions(options.getWhere(), queryOptions, "")
                    + " ";
        }
        List<Map<String, Object>> datalist = querySync(sql, queryOptions).getData();
        if (oldAutoClose) {
            closeConnection();
        }
        setAutoclose(oldAutoClose);
        return datalist;
    }

    public CompletableFuture<List<Map<String, Object>>> increment(String table, Map<String, Object> data) {
        return increment(table, data, FindOptions.builder().build());
    }

    public CompletableFuture<List<Map<String, Object>>> increment(String table, Map<String, Object> data,
            FindOptions options) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return incrementSync(table, data, options);
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        });
    }

    public FindAndCountResult findAndCountAllSync(String table) throws SQLException {
        return findAndCountAllSync(table, FindOptions.builder().build());
    }

    public FindAndCountResult findAndCountAllSync(String table, FindOptions options) throws SQLException {
        return findAndCountAllSync(table, options, m -> m);
    }

    public FindAndCountResult findAndCountAllSync(String table, FindOptions options, ICallBack icb)
            throws SQLException {
        boolean oldAutoClose = autoclose;
        setAutoclose(false);
        long count = countSync(table, options);
        List<Map<String, Object>> rows = findAllSync(table, options, icb);
        if (oldAutoClose) {
            closeConnection();
        }
        setAutoclose(oldAutoClose);
        return FindAndCountResult
                .builder()
                .count(count)
                .rows(rows)
                .build();
    }

    public CompletableFuture<FindAndCountResult> findAndCountAll(String table) {
        return findAndCountAll(table, FindOptions.builder().build());
    }

    public CompletableFuture<FindAndCountResult> findAndCountAll(String table, FindOptions options) {
        return findAndCountAll(table, options, m -> m);
    }

    public CompletableFuture<FindAndCountResult> findAndCountAll(String table, FindOptions options, ICallBack icb) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return findAndCountAllSync(table, options, icb);
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        });
    }

    public Connection transactionSync() throws SQLException {
        connection.setAutoCommit(false);
        setAutoclose(false);
        return connection;
    }

    public Object transactionSync(ITransCb icb) throws SQLException {
        Object result = null;
        Connection t = transactionSync();
        try {
            result = icb.cb(t);
            t.commit();
        } catch (Exception e) {
            e.printStackTrace();
            t.rollback();
        }
        return result;
    }

    public CompletableFuture<Object> transaction(ITransCb icb) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return transactionSync(icb);
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        });
    }

    public CompletableFuture<Connection> transaction() {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return transactionSync();
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        });
    }

    public Optional<Map<String, Object>> findOneAndUpdateSync(String table, FindOptions options,
            Map<String, Object> data)
            throws SQLException {
        boolean oldAutoClose = autoclose;
        setAutoclose(false);
        Optional<Map<String, Object>> opt = findOneSync(table, options);
        if (opt.isPresent()) {
            updateSync(table, data, options, true);
        }
        if (oldAutoClose) {
            closeConnection();
        }
        setAutoclose(oldAutoClose);
        return opt;
    }

    public CompletableFuture<Optional<Map<String, Object>>> findOneAndUpdate(String table, FindOptions options,
            Map<String, Object> data) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return findOneAndUpdateSync(table, options, data);
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        });
    }

    public Optional<Map<String, Object>> findOneAndDestroySync(String table, FindOptions options)
            throws SQLException {
        boolean oldAutoClose = autoclose;
        setAutoclose(false);
        Optional<Map<String, Object>> opt = findOneSync(table, options);
        if (opt.isPresent()) {
            destroySync(table, options);
        }
        if (oldAutoClose) {
            closeConnection();
        }
        setAutoclose(oldAutoClose);
        return opt;
    }

    public CompletableFuture<Optional<Map<String, Object>>> findOneAndDestroy(String table, FindOptions options) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return findOneAndDestroySync(table, options);
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        });
    }
}
