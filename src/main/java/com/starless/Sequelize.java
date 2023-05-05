package com.starless;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;

import com.starless.FindOrCreateResult.FindOrCreateResultBuilder;

import lombok.Data;

@Data
public class Sequelize {
    private final Connection connection;
    private boolean autoclose = false;
    private ExecutorService executor = null;

    public Sequelize(Connection connection) {
        this.connection = connection;
    }

    public Sequelize(Connection connection, ExecutorService executor) {
        this.connection = connection;
        this.executor = executor;
    }

    public Sequelize(String url) throws SQLException {
        connection = DriverManager.getConnection(url);
    }

    public Sequelize(String url, ExecutorService executor) throws SQLException {
        connection = DriverManager.getConnection(url);
        this.executor = executor;
    }

    public Sequelize(String url, String username, String password) throws SQLException {
        connection = DriverManager.getConnection(url, username, password);
    }

    public Sequelize(String url, String username, String password, ExecutorService excuetor) throws SQLException {
        connection = DriverManager.getConnection(url, username, password);
        this.executor = excuetor;
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

    public CompletableFuture<QueryResult> query(String sql, QueryOptions options, ICallBack callback) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return querySync(sql, options, callback);
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }, getExecutor());
    }

    private Executor getExecutor() {
        return executor != null ? executor : ForkJoinPool.commonPool();
    }

    public QueryResult querySync(String sql) throws SQLException {
        return querySync(sql, QueryOptions.builder().build());
    }

    public QueryResult querySync(String sql, QueryOptions options) throws SQLException {
        return querySync(sql, options, (m) -> m);
    }

    // public QueryResult querySync(String sql, QueryOptions options, ICallBack icb)
    // throws SQLException {
    // System.out.println(sql);
    // try (PreparedStatement stmt = connection.prepareStatement(sql);) {
    // String lsql = sql.toLowerCase().trim();
    // if (lsql.contains("select") && !lsql.startsWith("insert") &&
    // !lsql.startsWith("update")
    // && !lsql.startsWith("delete")) {
    // List<Map<String, Object>> datalist = new ArrayList<>();
    // int i = 1;
    // for (Object value : options.getBind()) {
    // stmt.setObject(i++, value);
    // }
    // ResultSet rs = stmt.executeQuery();
    // while (rs.next()) {
    // Map<String, Object> data = new HashMap<>();
    // for (i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
    // data.put(rs.getMetaData().getColumnLabel(i).toLowerCase(), rs.getObject(i));
    // }
    // datalist.add(icb.cb(data));
    // }
    // if (autoclose) {
    // closeConnection();
    // }
    // return QueryResult.builder().data(datalist).build();
    // } else {
    // int i = 1;
    // for (Object value : options.getBind()) {
    // stmt.setObject(i++, value);
    // }
    // int affectedRows = stmt.executeUpdate();
    // if (autoclose) {
    // closeConnection();
    // }
    // return QueryResult.builder().affectedRows(affectedRows).build();
    // }
    // }
    // }

    public QueryResult querySync(String sql, QueryOptions options, ICallBack callback) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            String lowerCaseSql = sql.toLowerCase().trim();
            System.out.println(lowerCaseSql);
            boolean isSelectQuery = lowerCaseSql.startsWith("select");

            setStatementParameters(statement, options);

            if (isSelectQuery) {
                return executeSelectQuery(statement, callback);
            } else {
                return executeNonSelectQuery(statement);
            }
        }
    }

    private void setStatementParameters(PreparedStatement statement, QueryOptions options) throws SQLException {
        int index = 1;
        for (Object value : options.getBind()) {
            statement.setObject(index++, value);
        }
    }

    private QueryResult executeSelectQuery(PreparedStatement statement, ICallBack callback) throws SQLException {
        List<Map<String, Object>> dataList = new ArrayList<>();

        try (ResultSet resultSet = statement.executeQuery()) {
            while (resultSet.next()) {
                Map<String, Object> rowData = extractRowData(resultSet);
                dataList.add(callback.cb(rowData));
            }
        }

        if (autoclose) {
            closeConnection();
        }

        return QueryResult.builder().data(dataList).build();
    }

    private Map<String, Object> extractRowData(ResultSet resultSet) throws SQLException {
        Map<String, Object> rowData = new HashMap<>();
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();

        for (int i = 1; i <= columnCount; i++) {
            String columnName = metaData.getColumnLabel(i).toLowerCase();
            Object value = resultSet.getObject(i);
            rowData.put(columnName, value);
        }

        return rowData;
    }

    private QueryResult executeNonSelectQuery(PreparedStatement statement) throws SQLException {
        int affectedRows = statement.executeUpdate();

        if (autoclose) {
            closeConnection();
        }

        return QueryResult.builder().affectedRows(affectedRows).build();
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

    // public String getWhereConditions(Map<String, Object> where, QueryOptions
    // options, String parentKey) {
    // List<Object> bind = options.getBind();
    // List<String> conditions = new ArrayList<>();
    // for (Entry<String, Object> entry : where.entrySet()) {
    // if (entry.getKey().equals("$or")) {
    // List<Map<String, Object>> orList = (List<Map<String, Object>>)
    // entry.getValue();
    // conditions.add("(" + orList.stream()
    // .map(m -> "(" + getWhereConditions(m, options, entry.getKey()) + ")")
    // .collect(Collectors.joining(" or ")) + ")");
    // } else if (entry.getKey().equals("$not")) {
    // List<Map<String, Object>> notList = (List<Map<String, Object>>)
    // entry.getValue();
    // conditions.add("not (" + notList.stream()
    // .map(m -> "(" + getWhereConditions(m, options, entry.getKey()) + ")")
    // .collect(Collectors.joining(" and ")) + ")");
    // } else if (entry.getKey().equals("$gt")) {
    // conditions.add(parentKey + " > ?");
    // bind.add(entry.getValue());
    // } else if (entry.getKey().equals("$lt")) {
    // conditions.add(parentKey + " < ?");
    // bind.add(entry.getValue());
    // } else if (entry.getKey().equals("$gte")) {
    // conditions.add(parentKey + " >= ?");
    // bind.add(entry.getValue());
    // } else if (entry.getKey().equals("$lte")) {
    // conditions.add(parentKey + " <= ?");
    // bind.add(entry.getValue());
    // } else if (entry.getKey().equals("$ne")) {
    // conditions.add(parentKey + " <> ?");
    // bind.add(entry.getValue());
    // } else if (entry.getKey().equals("$like")) {
    // conditions.add(parentKey + " like ?");
    // bind.add(entry.getValue());
    // } else if (entry.getKey().equals("$notlike")) {
    // conditions.add(parentKey + " not like ?");
    // bind.add(entry.getValue());
    // } else if (entry.getKey().equals("$startswith")) {
    // conditions.add(parentKey + " like ?");
    // bind.add(entry.getValue() + "%");
    // } else if (entry.getKey().equals("$endswith")) {
    // conditions.add(parentKey + " like ?");
    // bind.add("%" + entry.getValue());
    // } else if (entry.getKey().equals("$substring")) {
    // conditions.add(parentKey + " like ?");
    // bind.add("%" + entry.getValue() + "%");
    // } else if (entry.getKey().equals("$is")) {
    // if (entry.getValue() == null) {
    // conditions.add(parentKey + " is null");
    // } else {
    // conditions.add(parentKey + " is not null");
    // }
    // } else if (entry.getKey().equals("$between")) {
    // List<Object> between = (List<Object>) entry.getValue();
    // conditions.add(parentKey + " between ? and ?");
    // bind.addAll(between);
    // } else if (entry.getKey().equals("$notbetween")) {
    // List<Object> between = (List<Object>) entry.getValue();
    // conditions.add(parentKey + " not between ? and ?");
    // bind.addAll(between);
    // } else {
    // if (isMap(entry.getValue())) {
    // conditions.add(getWhereConditions((Map<String, Object>) entry.getValue(),
    // options, entry.getKey()));
    // } else if (isList(entry.getValue())) {
    // List<Object> list = (List<Object>) entry.getValue();
    // if (list.size() == 1) {
    // conditions.add(entry.getKey() + " = ?");
    // bind.add(list.get(0));
    // } else if (list.size() > 1) {
    // conditions.add(entry.getKey() + " in ("
    // + list.stream().map(v -> isString(v) ? "'" + v + "'" : String.valueOf(v))
    // .collect(Collectors.joining(", "))
    // + ")");
    // }
    // } else {
    // conditions.add(entry.getKey() + " = ?");
    // bind.add(entry.getValue());
    // }

    // }

    // }
    // options.setBind(bind);
    // return conditions.stream().collect(Collectors.joining(" and "));
    // }

    public String getWhereConditions(Map<String, Object> where, QueryOptions options, String parentKey) {
        List<Object> bind = options.getBind();
        List<String> conditions = new ArrayList<>();

        for (Entry<String, Object> entry : where.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();

            switch (key) {
                case "$or":
                    List<Map<String, Object>> orList = (List<Map<String, Object>>) value;
                    conditions.add("(" + orList.stream()
                            .map(m -> "(" + getWhereConditions(m, options, key) + ")")
                            .collect(Collectors.joining(" or ")) + ")");
                    break;
                case "$not":
                    List<Map<String, Object>> notList = (List<Map<String, Object>>) value;
                    conditions.add("not (" + notList.stream()
                            .map(m -> "(" + getWhereConditions(m, options, key) + ")")
                            .collect(Collectors.joining(" and ")) + ")");
                    break;
                case "$gt":
                case "$lt":
                case "$gte":
                case "$lte":
                case "$ne":
                case "$like":
                case "$notlike":
                    String operator = getOperatorForKey(key);
                    conditions.add(parentKey + " " + operator + " ?");
                    bind.add(value);
                    break;
                case "$startswith":
                    conditions.add(parentKey + " like ?");
                    bind.add(value + "%");
                    break;
                case "$endswith":
                    conditions.add(parentKey + " like ?");
                    bind.add("%" + value);
                    break;
                case "$substring":
                    conditions.add(parentKey + " like ?");
                    bind.add("%" + value + "%");
                    break;
                case "$is":
                    if (value == null) {
                        conditions.add(parentKey + " is null");
                    } else {
                        conditions.add(parentKey + " is not null");
                    }
                    break;
                case "$between":
                case "$notbetween":
                    List<Object> between = (List<Object>) value;
                    String betweenOperator = key.equals("$between") ? "between" : "not between";
                    conditions.add(parentKey + " " + betweenOperator + " ? and ?");
                    bind.addAll(between);
                    break;
                default:
                    handleDefaultCondition(conditions, bind, entry, options, parentKey);
                    break;
            }
        }

        options.setBind(bind);
        return String.join(" and ", conditions);
    }

    private String getOperatorForKey(String key) {
        switch (key) {
            case "$gt":
                return ">";
            case "$lt":
                return "<";
            case "$gte":
                return ">=";
            case "$lte":
                return "<=";
            case "$ne":
                return "<>";
            case "$like":
                return "like";
            case "$notlike":
                return "not like";
            default:
                throw new IllegalArgumentException("Invalid key: " + key);
        }
    }

    private void handleDefaultCondition(List<String> conditions, List<Object> bind, Entry<String, Object> entry,
            QueryOptions options, String parentKey) {
        Object value = entry.getValue();

        if (isMap(value)) {
            conditions.add(getWhereConditions((Map<String, Object>) value, options, entry.getKey()));
        } else if (isList(value)) {
            List<Object> list = (List<Object>) value;
            handleListCondition(conditions, bind, entry, list);
        } else {
            conditions.add(entry.getKey() + " = ?");
            bind.add(value);
        }
    }

    private void handleListCondition(List<String> conditions, List<Object> bind, Entry<String, Object> entry,
            List<Object> list) {
        String key = entry.getKey();
        int listSize = list.size();
        if (listSize == 1) {
            conditions.add(key + " = ?");
            bind.add(list.get(0));
        } else if (listSize > 1) {
            String inValues = list.stream()
                    .map(v -> isString(v) ? "'" + v + "'" : String.valueOf(v))
                    .collect(Collectors.joining(", "));
            conditions.add(key + " in (" + inValues + ")");
        }
    }

    // public List<Map<String, Object>> findAllSync(String table, FindOptions
    // options, ICallBack icb) throws SQLException {
    // String sql = "select "
    // + (options.getSelect().size() > 0
    // ? options.getSelect().stream().collect(Collectors.joining(", "))
    // : "*")
    // + " from " + table + " ";

    // QueryOptions queryOptions = QueryOptions.builder().build();
    // if (!options.getWhere().isEmpty()) {
    // sql += "where "
    // + getWhereConditions(options.getWhere(), queryOptions, "")
    // + " ";
    // }
    // if (options.getGroup().size() > 0) {
    // sql += "group by " +
    // options.getGroup().stream().collect(Collectors.joining(", ")) + " ";
    // }
    // if (!options.getOrder().isEmpty()) {
    // sql += "order by "
    // + options.getOrder().entrySet().stream().map(s -> getAliasKey(s.getKey()) + "
    // " + s.getValue())
    // .collect(Collectors.joining(", "))
    // + " ";
    // }
    // if (options.getLimit() != 0) {
    // sql += String.format("offset %d rows fetch first %d rows only ",
    // options.getOffset(), options.getLimit());
    // }
    // return querySync(sql, queryOptions, icb).getData();
    // }

    public List<Map<String, Object>> findAllSync(String table, FindOptions options, ICallBack callback)
            throws SQLException {
        StringBuilder sql = new StringBuilder("SELECT ");

        if (!options.getSelect().isEmpty()) {
            sql.append(options.getSelect().stream().collect(Collectors.joining(", ")));
        } else {
            sql.append("*");
        }

        sql.append(" FROM ").append(table).append(" ");

        QueryOptions queryOptions = QueryOptions.builder().build();
        if (!options.getWhere().isEmpty()) {
            sql.append("WHERE ")
                    .append(getWhereConditions(options.getWhere(), queryOptions, ""))
                    .append(" ");
        }

        if (!options.getGroup().isEmpty()) {
            sql.append("GROUP BY ")
                    .append(options.getGroup().stream().collect(Collectors.joining(", ")))
                    .append(" ");
        }

        if (!options.getOrder().isEmpty()) {
            sql.append("ORDER BY ")
                    .append(options.getOrder().entrySet().stream()
                            .map(s -> getAliasKey(s.getKey()) + " " + s.getValue())
                            .collect(Collectors.joining(", ")))
                    .append(" ");
        }

        if (options.getLimit() != 0) {
            sql.append(
                    String.format("OFFSET %d ROWS FETCH FIRST %d ROWS ONLY ", options.getOffset(), options.getLimit()));
        }

        return querySync(sql.toString(), queryOptions, callback).getData();
    }

    public CompletableFuture<List<Map<String, Object>>> findAll(String table, FindOptions options, ICallBack icb) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return findAllSync(table, options, icb);
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }, getExecutor());
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

    // public Map<String, Object> createSync(String table, Map<String, Object> data,
    // String pk, CreateOptions options)
    // throws SQLException {
    // boolean oldAutoClose = autoclose;
    // setAutoclose(false);

    // List<String> keys = new ArrayList<>();
    // if (options.getFields().size() > 0) {
    // keys = data.keySet().stream()
    // .filter(k -> options.getFields().contains(k)).collect(Collectors.toList());
    // } else {
    // keys = data.keySet().stream().collect(Collectors.toList());
    // }

    // querySync(String.format("insert into %s (%s) values (%s)", table,
    // String.join(",", keys),
    // String.join(",", keys.stream().map(k -> "?").collect(Collectors.toList()))),
    // QueryOptions.builder().bind(keys.stream().map(k ->
    // data.get(k)).collect(Collectors.toList())).build());
    // if (data.containsKey(pk)) {
    // Map<String, Object> where = new HashMap<>();
    // where.put(pk, data.get(pk));
    // return findOneSync(table,
    // FindOptions.builder().where(where).build()).orElse(data);
    // }
    // if (oldAutoClose) {
    // closeConnection();
    // }
    // setAutoclose(oldAutoClose);
    // return data;
    // }

    public Map<String, Object> createSync(String table, Map<String, Object> data, String primaryKey,
            CreateOptions options)
            throws SQLException {
        boolean oldAutoClose = autoclose;
        setAutoclose(false);

        List<String> keys;
        if (!options.getFields().isEmpty()) {
            keys = data.keySet().stream()
                    .filter(dataKey -> options.getFields().contains(dataKey))
                    .collect(Collectors.toList());
        } else {
            keys = new ArrayList<>(data.keySet());
        }

        String columns = String.join(",", keys);
        String placeholders = keys.stream().map(k -> "?").collect(Collectors.joining(","));

        querySync(String.format("INSERT INTO %s (%s) VALUES (%s)", table, columns, placeholders),
                QueryOptions.builder().bind(keys.stream().map(data::get).collect(Collectors.toList())).build());

        Map<String, Object> result = data;
        if (data.containsKey(primaryKey)) {
            Map<String, Object> where = new HashMap<>();
            where.put(primaryKey, data.get(primaryKey));
            result = findOneSync(table, FindOptions.builder().where(where).build()).orElse(data);
        }

        if (oldAutoClose) {
            closeConnection();
        }
        setAutoclose(oldAutoClose);
        return result;
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
        }, getExecutor());
    }

    public FindOrCreateResult findOrCreateSync(String table, FindOptions options) throws SQLException {
        return findOrCreateSync(table, options, "id");
    }

    // public FindOrCreateResult findOrCreateSync(String table, FindOptions options,
    // String pk) throws SQLException {
    // boolean oldAutoClose = autoclose;
    // setAutoclose(false);
    // Optional<Map<String, Object>> opt = findOneSync(table, options);
    // if (!opt.isPresent()) {
    // Map<String, Object> newData = createSync(table, options.getDefaults(), pk);
    // if (oldAutoClose) {
    // closeConnection();
    // }
    // setAutoclose(oldAutoClose);
    // return FindOrCreateResult
    // .builder()
    // .newData(Optional.of(newData))
    // .oldData(Optional.empty())
    // .build();
    // } else {
    // if (oldAutoClose) {
    // closeConnection();
    // }
    // setAutoclose(oldAutoClose);
    // return FindOrCreateResult
    // .builder()
    // .oldData(opt)
    // .newData(Optional.empty())
    // .build();
    // }

    // }

    public FindOrCreateResult findOrCreateSync(String table, FindOptions options, String primaryKey)
            throws SQLException {
        boolean oldAutoClose = autoclose;
        setAutoclose(false);

        Optional<Map<String, Object>> foundData = findOneSync(table, options);
        FindOrCreateResultBuilder resultBuilder = FindOrCreateResult.builder();

        if (!foundData.isPresent()) {
            Map<String, Object> newData = createSync(table, options.getDefaults(), primaryKey);
            resultBuilder.newData(Optional.of(newData)).oldData(Optional.empty());
        } else {
            resultBuilder.oldData(foundData).newData(Optional.empty());
        }

        if (oldAutoClose) {
            closeConnection();
        }
        setAutoclose(oldAutoClose);
        return resultBuilder.build();
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
        }, getExecutor());
    }

    public List<Map<String, Object>> bulkCreateSync(String table, List<Map<String, Object>> datalist)
            throws SQLException, InterruptedException, ExecutionException {
        return bulkCreateSync(table, datalist, "id");
    }

    public List<Map<String, Object>> bulkCreateSync(String table, List<Map<String, Object>> datalist, String pk)
            throws SQLException, InterruptedException, ExecutionException {
        return bulkCreateSync(table, datalist, pk, CreateOptions.builder().build());
    }

    // public List<Map<String, Object>> bulkCreateSync(String table,
    // List<Map<String, Object>> datalist, String pk,
    // CreateOptions options)
    // throws SQLException, InterruptedException, ExecutionException {
    // boolean oldAutoClose = autoclose;
    // setAutoclose(false);
    // List<CompletableFuture<Map<String, Object>>> futures = new ArrayList<>();
    // List<Map<String, Object>> results = new ArrayList<>();

    // if (options.isUnorder()) {
    // for (Map<String, Object> data : datalist) {
    // futures.add(create(table, data, pk, options));
    // }
    // CompletableFuture.allOf(futures.toArray(new
    // CompletableFuture[futures.size()])).get();
    // results.addAll(futures.stream().map(f -> {
    // try {
    // return f.get();
    // } catch (Exception e) {
    // e.printStackTrace();
    // throw new RuntimeException(e);
    // }
    // }).collect(Collectors.toList()));
    // } else {
    // for (Map<String, Object> data : datalist) {
    // results.add(createSync(table, data, pk, options));
    // }
    // }
    // if (oldAutoClose) {
    // closeConnection();
    // }
    // setAutoclose(oldAutoClose);
    // return results;
    // }

    public List<Map<String, Object>> bulkCreateSync(String table, List<Map<String, Object>> dataList, String primaryKey,
            CreateOptions options)
            throws SQLException, InterruptedException, ExecutionException {
        boolean oldAutoClose = autoclose;
        setAutoclose(false);
        List<Map<String, Object>> results = new ArrayList<>();

        if (options.isUnorder()) {
            List<CompletableFuture<Map<String, Object>>> futures = dataList.stream()
                    .map(data -> create(table, data, primaryKey, options))
                    .collect(Collectors.toList());

            CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).get();

            results.addAll(futures.stream().map(future -> {
                try {
                    return future.get();
                } catch (Exception e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
            }).collect(Collectors.toList()));
        } else {
            for (Map<String, Object> data : dataList) {
                results.add(createSync(table, data, primaryKey, options));
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
        }, getExecutor());
    }

    public List<Map<String, Object>> updateSync(String table, Map<String, Object> data) throws SQLException {
        return updateSync(table, data, FindOptions.builder().build());
    }

    public List<Map<String, Object>> updateSync(String table, Map<String, Object> data, FindOptions options)
            throws SQLException {
        return updateSync(table, data, options, false);
    }

    // public List<Map<String, Object>> updateSync(String table, Map<String, Object>
    // data, FindOptions options,
    // boolean noSelectQuery)
    // throws SQLException {
    // String sql = String.format("update %s set %s ", table,
    // data.keySet().stream().map(k -> k + " = ?").collect(Collectors.joining(",
    // ")));
    // QueryOptions queryOptions = QueryOptions
    // .builder()
    // .bind(data.entrySet().stream().map(s ->
    // s.getValue()).collect(Collectors.toList()))
    // .build();
    // if (!options.getWhere().isEmpty()) {
    // sql += "where "
    // + getWhereConditions(options.getWhere(), queryOptions, "")
    // + " ";
    // }
    // boolean oldAutoClose = autoclose;
    // if (!noSelectQuery)
    // setAutoclose(false);

    // querySync(sql, queryOptions);

    // if (noSelectQuery) {
    // return Arrays.asList(data);
    // }

    // queryOptions = QueryOptions
    // .builder()
    // .build();
    // sql = String.format("select * from %s ", table);
    // if (!options.getWhere().isEmpty()) {
    // sql += "where "
    // + getWhereConditions(options.getWhere(), queryOptions, "")
    // + " ";
    // }
    // List<Map<String, Object>> datalist = querySync(sql, queryOptions).getData();
    // if (oldAutoClose) {
    // closeConnection();
    // }
    // setAutoclose(oldAutoClose);
    // return datalist;
    // }

    public List<Map<String, Object>> updateSync(String table, Map<String, Object> data, FindOptions options,
            boolean noSelectQuery)
            throws SQLException {
        // Generate the update SQL query
        String sql = String.format("UPDATE %s SET %s ", table,
                String.join(", ", data.keySet().stream().map(k -> k + " = ?").collect(Collectors.toList())));
        QueryOptions queryOptions = QueryOptions
                .builder()
                .bind(data.values().stream().collect(Collectors.toList()))
                .build();

        // Append the where conditions if any
        if (!options.getWhere().isEmpty()) {
            sql += "WHERE " + getWhereConditions(options.getWhere(), queryOptions, "") + " ";
        }

        // Remember the old autoclose state and update if necessary
        boolean oldAutoClose = autoclose;
        if (!noSelectQuery) {
            setAutoclose(false);
        }

        // Execute the update query
        querySync(sql, queryOptions);

        // Return the updated data if noSelectQuery is true
        if (noSelectQuery) {
            return Arrays.asList(data);
        }

        // Generate the select query to fetch updated data
        queryOptions = QueryOptions.builder().build();
        sql = String.format("SELECT * FROM %s ", table);
        if (!options.getWhere().isEmpty()) {
            sql += "WHERE " + getWhereConditions(options.getWhere(), queryOptions, "") + " ";
        }

        // Fetch the updated data
        List<Map<String, Object>> dataList = querySync(sql, queryOptions).getData();

        // Restore the previous autoclose state and close the connection if needed
        if (oldAutoClose) {
            closeConnection();
        }
        setAutoclose(oldAutoClose);

        return dataList;
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
        }, getExecutor());
    }

    public int destroySync(String table) throws SQLException {
        return destroySync(table, FindOptions.builder().build());
    }

    // public int destroySync(String table, FindOptions options) throws SQLException
    // {
    // String sql = "";
    // if (options.isTruncate()) {
    // sql = String.format("truncate table %s ", table);
    // return querySync(sql).getAffectedRows();
    // } else {
    // sql = String.format("delete from %s ", table);
    // QueryOptions queryOptions = QueryOptions
    // .builder()
    // .build();
    // if (!options.getWhere().isEmpty()) {
    // sql += "where "
    // + getWhereConditions(options.getWhere(), queryOptions, "")
    // + " ";
    // }
    // return querySync(sql, queryOptions).getAffectedRows();
    // }
    // }

    public int destroySync(String table, FindOptions options) throws SQLException {
        String sql;
        if (options.isTruncate()) {
            // Generate the truncate SQL query
            sql = String.format("TRUNCATE TABLE %s ", table);
            return querySync(sql).getAffectedRows();
        } else {
            // Generate the delete SQL query
            sql = String.format("DELETE FROM %s ", table);
            QueryOptions queryOptions = QueryOptions.builder().build();

            // Append the where conditions if any
            if (!options.getWhere().isEmpty()) {
                sql += "WHERE " + getWhereConditions(options.getWhere(), queryOptions, "") + " ";
            }

            // Execute the delete query and return the affected rows
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
        }, getExecutor());
    }

    public long countSync(String table) throws SQLException {
        return countSync(table, FindOptions.builder().build());
    }

    // public long countSync(String table, FindOptions options) throws SQLException
    // {
    // return Long.parseLong(String.valueOf(findOneSync(table, FindOptions.builder()
    // .select(Arrays.asList("count(*) as count"))
    // .limit(0)
    // .offset(0)
    // .where(options.getWhere())
    // .defaults(options.getDefaults())
    // .truncate(options.isTruncate())
    // .group(options.getGroup())
    // .build()).get().get("count")));
    // }

    public long countSync(String table, FindOptions options) throws SQLException {
        // Create a new FindOptions instance for the count query
        FindOptions countOptions = FindOptions.builder()
                .select(Collections.singletonList("count(*) as count"))
                .limit(0)
                .offset(0)
                .where(options.getWhere())
                .defaults(options.getDefaults())
                .truncate(options.isTruncate())
                .group(options.getGroup())
                .build();

        // Execute the findOneSync method with the countOptions
        Optional<Map<String, Object>> result = findOneSync(table, countOptions);

        // Extract the count value and return it as a long
        return Long.parseLong(String.valueOf(result.get().get("count")));
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
        }, getExecutor());
    }

    public int maxSync(String table, String column) throws SQLException {
        return maxSync(table, column, FindOptions.builder().build());
    }

    // public int maxSync(String table, String column, FindOptions options) throws
    // SQLException {
    // options.setSelect(Arrays.asList(String.format("max(%s) as max", column)));
    // return (int) findOneSync(table, options).get().get("max");
    // }

    public int maxSync(String table, String column, FindOptions options) throws SQLException {
        // Update the select option with the max function applied to the specified
        // column
        options.setSelect(Collections.singletonList(String.format("max(%s) as max", column)));

        // Execute the findOneSync method with the updated options
        Optional<Map<String, Object>> result = findOneSync(table, options);

        // Extract the maximum value and return it as an int
        return ((Number) result.get().get("max")).intValue();
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
        }, getExecutor());
    }

    public int minSync(String table, String column) throws SQLException {
        return minSync(table, column, FindOptions.builder().build());
    }

    // public int minSync(String table, String column, FindOptions options) throws
    // SQLException {
    // options.setSelect(Arrays.asList(String.format("min(%s) as min", column)));
    // return (int) findOneSync(table, options).get().get("min");
    // }

    public int minSync(String table, String column, FindOptions options) throws SQLException {
        // Update the select option with the min function applied to the specified
        // column
        options.setSelect(Collections.singletonList(String.format("min(%s) as min", column)));

        // Execute the findOneSync method with the updated options
        Optional<Map<String, Object>> result = findOneSync(table, options);

        // Extract the minimum value and return it as an int
        return ((Number) result.get().get("min")).intValue();
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
        }, getExecutor());
    }

    public List<Map<String, Object>> incrementSync(String table, Map<String, Object> data)
            throws SQLException {
        return incrementSync(table, data, FindOptions.builder().build());
    }

    // public List<Map<String, Object>> incrementSync(String table, Map<String,
    // Object> data, FindOptions options)
    // throws SQLException {
    // String sql = String.format("update %s set %s ", table,
    // data.entrySet().stream().map(s -> s.getKey() + " + " +
    // String.valueOf(s.getValue()))
    // .collect(Collectors.joining(", ")));
    // QueryOptions queryOptions = QueryOptions.builder().build();
    // if (!options.getWhere().isEmpty()) {
    // sql += "where "
    // + getWhereConditions(options.getWhere(), queryOptions, "")
    // + " ";
    // }
    // boolean oldAutoClose = autoclose;
    // setAutoclose(false);
    // querySync(sql, queryOptions);

    // queryOptions = QueryOptions
    // .builder()
    // .build();
    // sql = String.format("select * from %s ", table);
    // if (!options.getWhere().isEmpty()) {
    // sql += "where "
    // + getWhereConditions(options.getWhere(), queryOptions, "")
    // + " ";
    // }
    // List<Map<String, Object>> datalist = querySync(sql, queryOptions).getData();
    // if (oldAutoClose) {
    // closeConnection();
    // }
    // setAutoclose(oldAutoClose);
    // return datalist;
    // }

    public List<Map<String, Object>> incrementSync(String table, Map<String, Object> data, FindOptions options)
            throws SQLException {
        // Prepare the update statement with the increments
        String incrementExpression = data.entrySet().stream()
                .map(s -> s.getKey() + " = " + s.getKey() + " + " + String.valueOf(s.getValue()))
                .collect(Collectors.joining(", "));
        String sql = String.format("update %s set %s ", table, incrementExpression);

        // Build the query options
        QueryOptions queryOptions = QueryOptions.builder().build();

        // Add the where conditions if any
        if (!options.getWhere().isEmpty()) {
            sql += "where " + getWhereConditions(options.getWhere(), queryOptions, "") + " ";
        }

        // Save the current auto-close setting and set it to false temporarily
        boolean oldAutoClose = autoclose;
        setAutoclose(false);

        // Execute the update statement
        querySync(sql, queryOptions);

        // Prepare and execute the select statement to return the updated rows
        queryOptions = QueryOptions.builder().build();
        sql = String.format("select * from %s ", table);
        if (!options.getWhere().isEmpty()) {
            sql += "where " + getWhereConditions(options.getWhere(), queryOptions, "") + " ";
        }
        List<Map<String, Object>> dataList = querySync(sql, queryOptions).getData();

        // Restore the auto-close setting and close the connection if necessary
        if (oldAutoClose) {
            closeConnection();
        }
        setAutoclose(oldAutoClose);

        return dataList;
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
        }, getExecutor());
    }

    public FindAndCountResult findAndCountAllSync(String table) throws SQLException {
        return findAndCountAllSync(table, FindOptions.builder().build());
    }

    public FindAndCountResult findAndCountAllSync(String table, FindOptions options) throws SQLException {
        return findAndCountAllSync(table, options, m -> m);
    }

    // public FindAndCountResult findAndCountAllSync(String table, FindOptions
    // options, ICallBack icb)
    // throws SQLException {
    // boolean oldAutoClose = autoclose;
    // setAutoclose(false);
    // long count = countSync(table, options);
    // List<Map<String, Object>> rows = findAllSync(table, options, icb);
    // if (oldAutoClose) {
    // closeConnection();
    // }
    // setAutoclose(oldAutoClose);
    // return FindAndCountResult
    // .builder()
    // .count(count)
    // .rows(rows)
    // .build();
    // }

    public FindAndCountResult findAndCountAllSync(String table, FindOptions options, ICallBack icb)
            throws SQLException {
        // Save the current auto-close setting and set it to false temporarily
        boolean oldAutoClose = autoclose;
        setAutoclose(false);

        // Get the total count of rows matching the conditions
        long count = countSync(table, options);

        // Get the rows based on the find options
        List<Map<String, Object>> rows = findAllSync(table, options, icb);

        // Restore the auto-close setting and close the connection if necessary
        if (oldAutoClose) {
            closeConnection();
        }
        setAutoclose(oldAutoClose);

        // Build and return the FindAndCountResult object
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
        }, getExecutor());
    }

    public Connection transactionSync() throws SQLException {
        connection.setAutoCommit(false);
        setAutoclose(false);
        return connection;
    }

    // public Object transactionSync(ITransCb icb) throws SQLException {
    // Object result = null;
    // Connection t = transactionSync();
    // try {
    // result = icb.cb(t);
    // t.commit();
    // } catch (Exception e) {
    // e.printStackTrace();
    // t.rollback();
    // }
    // return result;
    // }

    public Object transactionSync(ITransCb callback) throws SQLException {
        Object result = null;
        Connection transactionConnection = transactionSync();

        try {
            // Execute the callback with the transaction connection
            result = callback.cb(transactionConnection);

            // Commit the transaction
            transactionConnection.commit();
        } catch (Exception e) {
            // Print the stack trace and rollback the transaction in case of an error
            e.printStackTrace();
            transactionConnection.rollback();
        } finally {
            // Close the transaction connection
            if (transactionConnection != null && autoclose) {
                transactionConnection.close();
            }
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
        }, getExecutor());
    }

    public CompletableFuture<Connection> transaction() {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return transactionSync();
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }, getExecutor());
    }

    // public Optional<Map<String, Object>> findOneAndUpdateSync(String table,
    // FindOptions options,
    // Map<String, Object> data)
    // throws SQLException {
    // boolean oldAutoClose = autoclose;
    // setAutoclose(false);
    // Optional<Map<String, Object>> opt = findOneSync(table, options);
    // if (opt.isPresent()) {
    // updateSync(table, data, options, true);
    // }
    // if (oldAutoClose) {
    // closeConnection();
    // }
    // setAutoclose(oldAutoClose);
    // return opt;
    // }

    public Optional<Map<String, Object>> findOneAndUpdateSync(String table, FindOptions options,
            Map<String, Object> data) throws SQLException {
        boolean originalAutoClose = autoclose;
        setAutoclose(false);

        Optional<Map<String, Object>> record = findOneSync(table, options);
        if (record.isPresent()) {
            updateSync(table, data, options, true);
        }

        if (originalAutoClose) {
            closeConnection();
        }
        setAutoclose(originalAutoClose);

        return record;
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
        }, getExecutor());
    }

    // public Optional<Map<String, Object>> findOneAndDestroySync(String table,
    // FindOptions options)
    // throws SQLException {
    // boolean oldAutoClose = autoclose;
    // setAutoclose(false);
    // Optional<Map<String, Object>> opt = findOneSync(table, options);
    // if (opt.isPresent()) {
    // destroySync(table, options);
    // }
    // if (oldAutoClose) {
    // closeConnection();
    // }
    // setAutoclose(oldAutoClose);
    // return opt;
    // }

    public Optional<Map<String, Object>> findOneAndDestroySync(String table, FindOptions options) throws SQLException {
        boolean originalAutoClose = autoclose;
        setAutoclose(false);

        Optional<Map<String, Object>> record = findOneSync(table, options);
        if (record.isPresent()) {
            destroySync(table, options);
        }

        if (originalAutoClose) {
            closeConnection();
        }
        setAutoclose(originalAutoClose);

        return record;
    }

    public CompletableFuture<Optional<Map<String, Object>>> findOneAndDestroy(String table, FindOptions options) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return findOneAndDestroySync(table, options);
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }, getExecutor());
    }
}
