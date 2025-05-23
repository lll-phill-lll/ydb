# Working with {{ ydb-short-name }} Databases

{{ ydb-full-name }} can act as an external data source for another {{ ydb-full-name }} database. This section discusses the organization of collaboration between two independent {{ ydb-short-name }} databases in federated query processing mode.

To connect to an external {{ ydb-short-name }} database from another {{ ydb-short-name }} database acting as the federated query engine, the following steps need to be performed on the latter:

1. Prepare authentication data to access the remote {{ ydb-short-name }} database. Currently, in federated queries to {{ ydb-short-name }}, the only available authentication method is [login and password](../../security/authentication.md#static-credentials) (other methods are not supported). The password to the external database is stored as a [secret](../datamodel/secrets.md):

    ```yql
    CREATE OBJECT ydb_datasource_user_password (TYPE SECRET) WITH (value = "<password>");
    ```

2. Create an [external data source](../datamodel/external_data_source.md) describing the external {{ ydb-short-name }} database. The `LOCATION` parameter contains the network address of the {{ ydb-short-name }} instance to which the network connection is made. The `DATABASE_NAME` specifies the name of the database (e.g., `local`). For authentication to the external database, the `LOGIN` and `PASSWORD_SECRET_NAME` parameters are used. Encryption of connections to the external database can be enabled using the `USE_TLS="TRUE"` parameter. If encryption is enabled, the `<port>` field in the `LOCATION` parameter should specify the gRPCs port of the external {{ ydb-short-name }}; otherwise, the gRPC port should be specified.

    ```yql
    CREATE EXTERNAL DATA SOURCE ydb_datasource WITH (
        SOURCE_TYPE="Ydb",
        LOCATION="<host>:<port>",
        DATABASE_NAME="<database>",
        AUTH_METHOD="BASIC",
        LOGIN="user",
        PASSWORD_SECRET_NAME="ydb_datasource_user_password",
        USE_TLS="TRUE"
    );
    ```

3. {% include [!](_includes/connector_deployment.md) %}
4. [Execute a query](#query) to the external data source.

## Query Syntax {#query}

To retrieve data from tables of the external {{ ydb-short-name }} database, the following form of SQL query is used:

```yql
SELECT * FROM ydb_datasource.`<table_name>`
```

Where:

- `ydb_datasource` - identifier of the external data source;
- `<table_name>` - full name of the table within the [hierarchy](../../concepts/index.html#ydb-hierarchy) of directories in the {{ ydb-short-name }} database, e.g., `table`, `dir1/table1`, or `dir1/dir2/table3`.

If the table is at the top level of the hierarchy (not belonging to any directories), it is permissible not to enclose the table name in backticks "\`":

```yql
SELECT * FROM ydb_datasource.<table_name>
```

## Limitations

There are several limitations when working with external {{ ydb-short-name }} data sources:

1. {% include [!](_includes/supported_requests.md) %}
1. {% include [!](_includes/predicate_pushdown.md) %}

    |{{ ydb-short-name }} Data Type|
    |----|
    |`Bool`|
    |`Int8`|
    |`Uint8`|
    |`Int16`|
    |`Uint16`|
    |`Int32`|
    |`Uint32`|
    |`Int64`|
    |`Uint64`|
    |`Float`|
    |`Double`|
    |`String`|
    |`Utf8`|

## Supported Data Types

When working with tables located in the external {{ ydb-short-name }} database, users have access to a limited set of data types. All other types, except for those listed below, are not supported. In some cases the type conversion is performed, meaning that the columns of the table from the external {{ ydb-short-name }} database may change their type after being read by the {{ ydb-short-name }} database processing the federated query.

|External {{ ydb-short-name }} data type|Federated {{ ydb-short-name }} data type|
|---------|---------|
|`Bool`|`Bool`|
|`Int8`|`Int8`|
|`Int16`|`Int16`|
|`Int32`|`Int32`|
|`Int64`|`Int64`|
|`Uint8`|`Uint8`|
|`Uint16`|`Uint16`|
|`Uint32`|`Uint32`|
|`Uint64`|`Uint64`|
|`Float`|`Float`|
|`Double`|`Double`|
|`String`|`String`|
|`Utf8`|`Utf8`|
|`Date`|`Date`|
|`Datetime`|`Datetime`|
|`Timestamp`|`Timestamp`|
|`Json`|`Json`|
|`JsonDocument`|`Json`|
