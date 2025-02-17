// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package org.custom.connector.jdbc.client;

import com.amazonaws.appflow.custom.connector.model.ConnectorContext;
import com.amazonaws.appflow.custom.connector.model.metadata.DescribeEntityRequest;
import com.amazonaws.appflow.custom.connector.model.metadata.Entity;
import com.amazonaws.appflow.custom.connector.model.metadata.FieldDataType;
import com.amazonaws.appflow.custom.connector.model.metadata.FieldDefinition;
import com.amazonaws.appflow.custom.connector.model.metadata.ImmutableEntity;
import com.amazonaws.appflow.custom.connector.model.metadata.ImmutableFieldDefinition;
import com.amazonaws.appflow.custom.connector.model.metadata.ImmutableReadOperationProperty;
import com.amazonaws.appflow.custom.connector.model.metadata.ImmutableWriteOperationProperty;
import com.amazonaws.appflow.custom.connector.model.metadata.ListEntitiesRequest;
import com.amazonaws.appflow.custom.connector.model.query.QueryDataRequest;
import com.amazonaws.appflow.custom.connector.model.query.ImmutableQueryDataRequest;
import com.amazonaws.appflow.custom.connector.model.write.WriteDataRequest;
import com.amazonaws.appflow.custom.connector.model.write.WriteOperationType;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringEscapeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class MySQLClient implements JDBCClient {
  private static final Logger LOGGER = LoggerFactory.getLogger(MySQLClient.class);
  final ObjectMapper objectMapper = new ObjectMapper();
  private Connection conn = null;
  private final Map<String, String> credentials;

  public MySQLClient(final Map<String, String> creds) {
    credentials = creds;
  }

  @Override
  public List<WriteOperationType> getWriteOperations() {
    List<WriteOperationType> writeOperationTypes = new ArrayList<>();
    writeOperationTypes.add(WriteOperationType.UPSERT);
    writeOperationTypes.add(WriteOperationType.UPDATE);
    writeOperationTypes.add(WriteOperationType.INSERT);
    return writeOperationTypes;
  }

  @Override
  public List<Entity> getEntities(final ListEntitiesRequest request) throws SQLException {
    final List<Entity> records = new ArrayList<Entity>();

    Connection conn = getConnection();

    DatabaseMetaData metaData = conn.getMetaData();
    String[] types = {"TABLE"};
    // Retrieving the columns in the database
    ResultSet tables = metaData.getTables(null, null, "%", types);
    while (tables.next()) {
      records.add(
        ImmutableEntity.builder()
          .entityIdentifier(tables.getString("TABLE_NAME"))
          .description(tables.getString("TABLE_NAME"))
          .label(tables.getString("TABLE_NAME"))
          .hasNestedEntities(false)
          .build());
    }
    conn.close();
    return records;
  }

  @Override
  public List<FieldDefinition> getFieldDefinitions(final DescribeEntityRequest request) throws SQLException {
    final List<FieldDefinition> fieldDefinitions = new ArrayList<>();
    Connection conn = getConnection();

    Statement st = conn.createStatement();
    String sql = String.format("DESCRIBE `%s`", request.entityIdentifier());
    ResultSet rs = st.executeQuery(sql);

    while (rs.next()) {
      fieldDefinitions.add(ImmutableFieldDefinition.builder()
        .fieldName(rs.getString(1))
        .dataType(mapFieldType(rs.getString(2)))
        .dataTypeLabel(rs.getString(1))
        .label(rs.getString(1))
        .isPrimaryKey(rs.getString(4).equals("PRI"))
        .readProperties(ImmutableReadOperationProperty.builder()
          .isQueryable(true)
          .isRetrievable(true)
          .build())
        .writeProperties(ImmutableWriteOperationProperty.builder()
          .isNullable(true)
          .isUpdatable(true)
          .isCreatable(true)
          .isDefaultedOnCreate(!rs.getString(4).equals("PRI"))
          .supportedWriteOperations(getWriteOperations())
          .build())
        .build());
    }
    rs.close();
    conn.close();
    return fieldDefinitions;
  }

  @Override
  public Connection getConnection() {
    try {
      if (conn != null && conn.isValid(0)) {
        return conn;
      }
    } catch (SQLException e) {
      // Do nothing for now.
    }

    try {
      String uri = String.format(
        "jdbc:%s://%s:%s/%s?user=%s&password=%s",
        credentials.get("driver"),
        credentials.get("hostname"),
        credentials.get("port"),
        credentials.get("database"),
        credentials.get("username"),
        credentials.get("password")
      );

      conn = DriverManager.getConnection(uri);
    } catch (SQLException ex) {
      // handle any errors
      LOGGER.error("SQLException: " + ex.getMessage());
      LOGGER.error("SQLState: " + ex.getSQLState());
      LOGGER.error("VendorError: " + ex.getErrorCode());
    }
    return conn;
  }

  private FieldDataType mapFieldType(final String mysqlType) {
    String[] temp = mysqlType.split("\\(");
    String mtype = temp[0].toUpperCase();
    switch (mtype) {
      case "ARRAY":
      case "STRUCT":
        return FieldDataType.Struct;
      case "BIGINT":
      case "NUMERIC":
        return FieldDataType.BigInteger;
      case "BINARY":
      case "LONGVARBINARY":
      case "VARBINARY":
        return FieldDataType.ByteArray;
      case "BIT":
      case "SMALLINT":
      case "INTEGER":
      case "TINYINT":
      case "MEDIUMINT":
      case "INT":
        return FieldDataType.Integer;
      case "BLOB":
        return FieldDataType.String;
      case "BOOLEAN":
        return FieldDataType.Boolean;
      case "CHAR":
        return FieldDataType.String;
      case "CLOB":
        return FieldDataType.String;
      case "DATALINK":
        return FieldDataType.String;
      case "DATE":
        return FieldDataType.Date;
      case "DECIMAL":
      case "DOUBLE":
        return FieldDataType.Double;
      case "DISTINCT":
        return FieldDataType.String;
      case "FLOAT":
        return FieldDataType.Float;
      case "JAVA_OBJECT":
        return FieldDataType.Map;
      case "LONGNVARCHAR":
        return FieldDataType.String;
      case "LONGVARCHAR":
        return FieldDataType.String;
      case "NCHAR":
        return FieldDataType.String;
      case "NCLOB":
        return FieldDataType.String;
      case "NULL":
        return FieldDataType.String;
      case "NVARCHAR":
        return FieldDataType.String;
      case "OTHER":
        return FieldDataType.String;
      case "REAL":
        return FieldDataType.Long;
      case "REF":
        return FieldDataType.String;
      case "REF_CURSOR":
        return FieldDataType.String;
      case "ROWID":
        return FieldDataType.String;
      case "SQLXML":
        return FieldDataType.String;
      case "TIME":
      case "TIME_WITH_TIMEZONE":
      case "TIMESTAMP":
      case "TIMESTAMP_WITH_TIMEZONE":
        return FieldDataType.DateTime;
      case "VARCHAR":
        return FieldDataType.String;
      default:
        return FieldDataType.String;
    }
  }

  @Override
  public long getTotalData(final QueryDataRequest request) {
    return getTotalData(request, true);
  }

  private long getTotalData(final QueryDataRequest request, boolean closeConnection) {
    try (Connection conn = getConnection()) {
      Statement st = conn.createStatement();

      String sql = String.format("SELECT COUNT(*) as cnt FROM `%s`", request.entityIdentifier());
      if (request.filterExpression() != null) {
        sql = sql + String.format(" WHERE %s", request.filterExpression());
      }

      ResultSet rs = st.executeQuery(sql);
      rs.next();
      long count = rs.getLong("cnt");
      st.close();
      if (closeConnection) {
        conn.close();
      }
      return count;
    } catch (SQLException ex) {
      LOGGER.error("SQLException information");
      while (ex != null) {
        LOGGER.error("Error msg: " + ex.getMessage());
        ex = ex.getNextException();
      }
      throw new RuntimeException("Error");
    }
  }

  @Override
  public List<String> queryData(final QueryDataRequest request) {
    return queryData(request, true);
  }

  private List<String> queryData(final QueryDataRequest request, boolean closeConnection) {
    List<String> records = new ArrayList<String>();

    try (Connection conn = getConnection()) {

      Statement st = conn.createStatement();
      String sql = String.format(
        "SELECT %s FROM `%s`", String.join(",", request.selectedFieldNames()), request.entityIdentifier()
      );

      if (request.filterExpression() != null) {
        sql = sql + String.format(" WHERE %s", request.filterExpression());
      }

      if (request.maxResults() != null) {
        int nextToken = 0;
        if (request.nextToken() != null) {
          nextToken = Integer.parseInt(request.nextToken());
        }
        sql = sql + String.format(" LIMIT %s, %s", nextToken, request.maxResults());
      }

      ResultSet rs = st.executeQuery(sql);
      Map<String, String> rows = new HashMap<>();

      while (rs.next()) {
        for (int i = 0; i < request.selectedFieldNames().size(); i++) {
          rows.put(request.selectedFieldNames().get(i), rs.getString(i + 1));
        }
        try {
          records.add(objectMapper.writeValueAsString(rows));
          rows.clear();
        } catch (JsonProcessingException e) {
          e.printStackTrace();
        }
      }
      rs.close();
      if (closeConnection) {
        conn.close();
      }
    } catch (SQLException ex) {
      LOGGER.error("SQLException information");
      while (ex != null) {
        LOGGER.error("Error msg: " + ex.getMessage());
        ex = ex.getNextException();
      }
      throw new RuntimeException("Error");
    }
    return records;
  }

  @Override
  public int[] writeData(final WriteDataRequest request) {
    JsonNode recordJson;
    Map<String, String> zohoModifiedTimes;

    try {
      zohoModifiedTimes = 
        getZohoModifiedTimes(request.entityIdentifier(), request.connectorContext());
    } catch (Exception ex) {
      LOGGER.error("Error msg: " + ex.getMessage());
      throw new RuntimeException("Error");
    }
    
    try (Connection conn = getConnection()) {
      conn.setAutoCommit(true);
      Statement statement = conn.createStatement();

      String sql;

      for (String record : request.records()) {
        sql = "";
        try {
          recordJson = objectMapper.readValue(record, JsonNode.class);
        } catch (JsonProcessingException e) {
          throw new IllegalArgumentException("Invalid record provided for Write operation. Record must be valid JSON", e);
        }
        List<String> keys = new ArrayList<>();
        Iterator<String> iterator = recordJson.fieldNames();
        iterator.forEachRemaining(e -> keys.add(e));

        LOGGER.info("Incoming record: {}", recordJson.get("zoho_record_id").asText());
        if (isSkipRecord(recordJson, zohoModifiedTimes)) {
          LOGGER.info("Skipping record {} from table {} as it is unchanged in Zoho since the last sync.", 
            recordJson.get("zoho_record_id").asText(), request.entityIdentifier());
          continue;
        }
        LOGGER.info("Processing record: {}", recordJson.get("zoho_record_id").asText());

        if (WriteOperationType.INSERT.equals(request.operation()) || WriteOperationType.UPSERT.equals(request.operation())) {
          if (WriteOperationType.UPSERT.equals(request.operation())) {
            sql = "REPLACE";
          } else {
            sql = "INSERT";
          }

          sql += String.format(" INTO `%s` (%s) VALUES (", request.entityIdentifier(), String.join(",", keys));
          String value;
          JsonNode jsonValue;

          for (int i = 0; i < keys.size(); i++) {
            jsonValue = recordJson.get(keys.get(i));
            if (jsonValue == null || jsonValue.isNull()) {
              if (i > 0) {
                sql += ", NULL";
              } else {
                sql += "NULL"; 
              }
              continue;
            }

            if (jsonValue.isTextual()) {
              value = StringEscapeUtils.escapeJava(jsonValue.asText());
              if (i > 0) {
                sql += String.format(", \"%s\"", value);
              } else {
                sql += String.format("\"%s\"", value);
              }
            } else if (jsonValue.isNumber()) {
              if (i > 0) {
                sql += String.format(", %s", jsonValue.asText());
              } else {
                sql += jsonValue.asText();
              }
            } else if (jsonValue.isBoolean()) {
              if (i > 0) {
                sql += String.format(", %b", jsonValue.asBoolean());
              } else {
                sql += String.format("%b", jsonValue.asBoolean());
              }
            } else {
              // For other types like arrays/objects, convert to string
              value = StringEscapeUtils.escapeJava(jsonValue.toString());
              if (i > 0) {
                sql += String.format(", \"%s\"", value);
              } else {
                sql += String.format("\"%s\"", value);
              }
            }
          }
          sql += ")";
        } else if (WriteOperationType.UPDATE.equals(request.operation())) {
          if (Objects.requireNonNull(request.idFieldNames()).size() != 1) {
            throw new IllegalArgumentException("A single Id field is required for UPSERT operations in JDBC");
          }

          String recordIdKey = request.idFieldNames().get(0);
          String recordId = getValueFromRecord(recordJson, recordIdKey);
          JsonNode jsonValue;
          sql = String.format("UPDATE `%s` SET ", request.entityIdentifier());
          for (int i = 0; i < keys.size(); i++) {
            jsonValue = recordJson.get(keys.get(i));
            if (jsonValue == null || jsonValue.isNull()) {
              if (i > 0) {
                sql += String.format(", %s = NULL", keys.get(i));
              } else {
                sql += String.format("%s = NULL", keys.get(i));
              }
              continue;
            }

            if (jsonValue.isTextual()) {
              String value = StringEscapeUtils.escapeJava(jsonValue.asText());
              if (i > 0) {
                sql += String.format(", %s = \"%s\"", keys.get(i), value);
              } else {
                sql += String.format("%s = \"%s\"", keys.get(i), value);
              }
            } else if (jsonValue.isNumber()) {
              if (i > 0) {
                sql += String.format(", %s = %s", keys.get(i), jsonValue.asText());
              } else {
                sql += String.format("%s = %s", keys.get(i), jsonValue.asText());
              }
            } else if (jsonValue.isBoolean()) {
              if (i > 0) {
                sql += String.format(", %s = %b", keys.get(i), jsonValue.asBoolean());
              } else {
                sql += String.format("%s = %b", keys.get(i), jsonValue.asBoolean());
              }
            } else {
              // For other types like arrays/objects, convert to string
              String value = StringEscapeUtils.escapeJava(jsonValue.toString());
              if (i > 0) {
                sql += String.format(", %s = \"%s\"", keys.get(i), value);
              } else {
                sql += String.format("%s = \"%s\"", keys.get(i), value);
              }
            }
          }
          sql += String.format(" WHERE %s = %s", recordIdKey, recordId);
        }
        statement.addBatch(sql);
      }
      int[] records = statement.executeBatch();
      statement.close();
      conn.close();
      return records;
    } catch (SQLException ex) {
      LOGGER.error("SQLException information");
      while (ex != null) {
        LOGGER.error("Error msg: " + ex.getMessage());
        ex = ex.getNextException();
      }
      throw new RuntimeException("Error");
    }
  }

  private String getValueFromRecord(final JsonNode jsonRecord, final String key) {
    if (Objects.isNull(jsonRecord) || Objects.isNull(jsonRecord.get(key))) {
      throw new IllegalArgumentException(key + " key is missing from JSON record but is required");
    }
    if (StringUtils.isEmpty(jsonRecord.get(key).textValue())) {
      return null;
    }
    return jsonRecord.get(key).textValue();
  }

  private Map<String, String> getZohoModifiedTimes(
    String entityIdentifier, 
    ConnectorContext connectorContext
  ) {
    final String ZOHO_MODIFIED_TIME = "zoho_modified_time";
    final String ZOHO_RECORD_ID = "zoho_record_id";

    Map<String, String> zohoModifiedTimes = new HashMap<>();
    
    long totalRecords = getTotalData(
      ImmutableQueryDataRequest.builder()
        .entityIdentifier(entityIdentifier)
        .connectorContext(connectorContext)
        .maxResults(1L)
        .build(),
      false
    );
    
    List<String> records = queryData(ImmutableQueryDataRequest.builder()
      .entityIdentifier(entityIdentifier)
      .connectorContext(connectorContext)
      .selectedFieldNames(List.of(ZOHO_RECORD_ID, ZOHO_MODIFIED_TIME))
      .maxResults(totalRecords)
      .build(),
      false
    );

    try {
      for (String record : records) {
        JsonNode recordJson = objectMapper.readTree(record);
        String recordId = getValueFromRecord(recordJson, ZOHO_RECORD_ID);
        String modifiedTime = getValueFromRecord(recordJson, ZOHO_MODIFIED_TIME);
        zohoModifiedTimes.put(recordId, modifiedTime);
      }
    } catch (JsonProcessingException e) {
      LOGGER.error("Error parsing record: " + e.getMessage());
      throw new RuntimeException("Error parsing record", e);
    }

    return zohoModifiedTimes;
  }
  
  private boolean isSkipRecord(
    final JsonNode jsonRecord, 
    final Map<String, String> zohoModifiedTimes
  ) {
    final String ZOHO_MODIFIED_TIME = "zoho_modified_time";
    final String ZOHO_RECORD_ID = "zoho_record_id";

    if (!jsonRecord.has(ZOHO_MODIFIED_TIME) || !jsonRecord.has(ZOHO_RECORD_ID)) {
      return false;
    }

    String incomingRecordModifiedTime = getValueFromRecord(jsonRecord, ZOHO_MODIFIED_TIME);
    String incomingRecordModifiedTimeUTC = convertToUTC(incomingRecordModifiedTime);
    String incomingRecordId = getValueFromRecord(jsonRecord, ZOHO_RECORD_ID);

    String existingModifiedTime = zohoModifiedTimes.get(incomingRecordId);
    return existingModifiedTime.equals(incomingRecordModifiedTimeUTC);
  }

  private String convertToUTC(String istTime) {
    try {
      java.time.format.DateTimeFormatter inputFormatter = java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME;
      java.time.format.DateTimeFormatter outputFormatter = java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
      java.time.OffsetDateTime offsetDateTime = java.time.OffsetDateTime.parse(istTime, inputFormatter);
      java.time.ZonedDateTime utcZoned = offsetDateTime.atZoneSameInstant(java.time.ZoneId.of("UTC"));
      return utcZoned.format(outputFormatter);
    } catch (Exception e) {
      LOGGER.error("Error converting time: " + e.getMessage());
      return istTime; // Return original time if conversion fails
    }
  }
}
