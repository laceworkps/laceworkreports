# `laceworkreports`

laceworkreports is a Python cli/package for exporting and creating reports from Lacework data.

**Usage**:

```console
$ laceworkreports [OPTIONS] COMMAND [ARGS]...
```

**Options**:

* `--version`
* `--account TEXT`: account subdomain of URL (i.e. <ACCOUNT>.lacework.net)  [env var: LW_ACCOUNT]
* `--subaccount TEXT`: sub-account name inside your organization (org admins only)  [env var: LW_SUBACCOUNT]
* `--api-key TEXT`: access key id  [env var: LW_API_KEY]
* `--api-secret TEXT`: secret access key  [env var: LW_API_SECRET]
* `--profile TEXT`: switch between profiles configured at ~/.lacework.toml
* `--base-domain TEXT`: lacework.net or fra.lacework.net (default: lacework.net)  [env var: LW_BASE_DOMAIN]
* `--sample / --no-sample`: print first row of response from api and exit  [default: False]
* `--install-completion`: Install completion for the current shell.
* `--show-completion`: Show completion for the current shell, to copy it or customize the installation.
* `--help`: Show this message and exit.

laceworkreports <action> <type> <subtype> <exporttype> [OPTIONS]

**Commands**:

* `export`: Export lacework events

## `laceworkreports export`

Export lacework events

**Usage**:

```console
$ laceworkreports export [OPTIONS] COMMAND [ARGS]...
```

**Options**:

* `--help`: Show this message and exit.

laceworkreports export <type> <subtype> <exporttype> [OPTIONS]

**Commands**:

* `activities`: Query lacework api activities types
* `agent_access_tokens`: Query lacework api agent_access_tokens types
* `alert_channels`: Query lacework api alert_channels types
* `alert_rules`: Query lacework api alert_rules types
* `alerts`: Query lacework api alerts types
* `audit_logs`: Query lacework api audit_logs types
* `cloud_accounts`: Query lacework api cloud_accounts types
* `cloud_activities`: Query lacework api cloud_activities types
* `configs`: Query lacework api configs types
* `container_registries`: Query lacework api container_registries types
* `entities`: Query lacework api entities types
* `queries`: Query lacework api queries types
* `report_rules`: Query lacework api report_rules types
* `resource_groups`: Query lacework api resource_groups types
* `team_members`: Query lacework api team_members types
* `vulnerabilities`: Query lacework api vulnerabilities types
* `vulnerability_exceptions`: Query lacework api vulnerability_exceptions...

### `laceworkreports export activities`

Query lacework api activities types

**Usage**:

```console
$ laceworkreports export activities [OPTIONS] COMMAND [ARGS]...
```

**Options**:

* `--help`: Show this message and exit.

laceworkreports export activities <subtype> <exporttype> [OPTIONS]

**Commands**:

* `changed_files`: Retrieve lacework activities api...
* `connections`: Retrieve lacework activities api connections...
* `dns`: Retrieve lacework activities api dns events
* `user_logins`: Retrieve lacework activities api user_logins...

#### `laceworkreports export activities changed_files`

Retrieve lacework activities api changed_files events

**Usage**:

```console
$ laceworkreports export activities changed_files [OPTIONS] COMMAND [ARGS]...
```

**Options**:

* `--help`: Show this message and exit.

laceworkreports export activities changed_files <exporttype> [OPTIONS]

**Commands**:

* `csv`: Export to csv
* `jinja2`: Use jinja template to transform export...
* `json`: Export to json
* `postgres`: Export to postgres database

##### `laceworkreports export activities changed_files csv`

Export to csv

**Usage**:

```console
$ laceworkreports export activities changed_files csv [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--append / --no-append`: Boolean value to append or replace results  [default: False]
* `--help`: Show this message and exit.

##### `laceworkreports export activities changed_files jinja2`

Use jinja template to transform export results

**Usage**:

```console
$ laceworkreports export activities changed_files jinja2 [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--template-path TEXT`: Path to jinja2 template. Results will be passed as 'dataset' variable.  [required]
* `--help`: Show this message and exit.

##### `laceworkreports export activities changed_files json`

Export to json

**Usage**:

```console
$ laceworkreports export activities changed_files json [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--append / --no-append`: Boolean value to append or replace results  [default: False]
* `--help`: Show this message and exit.

##### `laceworkreports export activities changed_files postgres`

Export to postgres database

**Usage**:

```console
$ laceworkreports export activities changed_files postgres [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--db-connection TEXT`: Postgres connection string (e.g. postgresql://postgres:password@localhost:5432/postgres)  [required]
* `--db-table TEXT`: Postgres table to store results  [default: export]
* `--db-if-exists [append|replace|fail]`: Action to take if db table already exists  [default: replace]
* `--db-create-if-missing / --no-db-create-if-missing`: Bool to create database if missing  [default: True]
* `--help`: Show this message and exit.

#### `laceworkreports export activities connections`

Retrieve lacework activities api connections events

**Usage**:

```console
$ laceworkreports export activities connections [OPTIONS] COMMAND [ARGS]...
```

**Options**:

* `--help`: Show this message and exit.

laceworkreports export activities connections <exporttype> [OPTIONS]

**Commands**:

* `csv`: Export to csv
* `jinja2`: Use jinja template to transform export...
* `json`: Export to json
* `postgres`: Export to postgres database

##### `laceworkreports export activities connections csv`

Export to csv

**Usage**:

```console
$ laceworkreports export activities connections csv [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--append / --no-append`: Boolean value to append or replace results  [default: False]
* `--help`: Show this message and exit.

##### `laceworkreports export activities connections jinja2`

Use jinja template to transform export results

**Usage**:

```console
$ laceworkreports export activities connections jinja2 [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--template-path TEXT`: Path to jinja2 template. Results will be passed as 'dataset' variable.  [required]
* `--help`: Show this message and exit.

##### `laceworkreports export activities connections json`

Export to json

**Usage**:

```console
$ laceworkreports export activities connections json [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--append / --no-append`: Boolean value to append or replace results  [default: False]
* `--help`: Show this message and exit.

##### `laceworkreports export activities connections postgres`

Export to postgres database

**Usage**:

```console
$ laceworkreports export activities connections postgres [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--db-connection TEXT`: Postgres connection string (e.g. postgresql://postgres:password@localhost:5432/postgres)  [required]
* `--db-table TEXT`: Postgres table to store results  [default: export]
* `--db-if-exists [append|replace|fail]`: Action to take if db table already exists  [default: replace]
* `--db-create-if-missing / --no-db-create-if-missing`: Bool to create database if missing  [default: True]
* `--help`: Show this message and exit.

#### `laceworkreports export activities dns`

Retrieve lacework activities api dns events

**Usage**:

```console
$ laceworkreports export activities dns [OPTIONS] COMMAND [ARGS]...
```

**Options**:

* `--help`: Show this message and exit.

laceworkreports export activities dns <exporttype> [OPTIONS]

**Commands**:

* `csv`: Export to csv
* `jinja2`: Use jinja template to transform export...
* `json`: Export to json
* `postgres`: Export to postgres database

##### `laceworkreports export activities dns csv`

Export to csv

**Usage**:

```console
$ laceworkreports export activities dns csv [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--append / --no-append`: Boolean value to append or replace results  [default: False]
* `--help`: Show this message and exit.

##### `laceworkreports export activities dns jinja2`

Use jinja template to transform export results

**Usage**:

```console
$ laceworkreports export activities dns jinja2 [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--template-path TEXT`: Path to jinja2 template. Results will be passed as 'dataset' variable.  [required]
* `--help`: Show this message and exit.

##### `laceworkreports export activities dns json`

Export to json

**Usage**:

```console
$ laceworkreports export activities dns json [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--append / --no-append`: Boolean value to append or replace results  [default: False]
* `--help`: Show this message and exit.

##### `laceworkreports export activities dns postgres`

Export to postgres database

**Usage**:

```console
$ laceworkreports export activities dns postgres [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--db-connection TEXT`: Postgres connection string (e.g. postgresql://postgres:password@localhost:5432/postgres)  [required]
* `--db-table TEXT`: Postgres table to store results  [default: export]
* `--db-if-exists [append|replace|fail]`: Action to take if db table already exists  [default: replace]
* `--db-create-if-missing / --no-db-create-if-missing`: Bool to create database if missing  [default: True]
* `--help`: Show this message and exit.

#### `laceworkreports export activities user_logins`

Retrieve lacework activities api user_logins events

**Usage**:

```console
$ laceworkreports export activities user_logins [OPTIONS] COMMAND [ARGS]...
```

**Options**:

* `--help`: Show this message and exit.

laceworkreports export activities user_logins <exporttype> [OPTIONS]

**Commands**:

* `csv`: Export to csv
* `jinja2`: Use jinja template to transform export...
* `json`: Export to json
* `postgres`: Export to postgres database

##### `laceworkreports export activities user_logins csv`

Export to csv

**Usage**:

```console
$ laceworkreports export activities user_logins csv [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--append / --no-append`: Boolean value to append or replace results  [default: False]
* `--help`: Show this message and exit.

##### `laceworkreports export activities user_logins jinja2`

Use jinja template to transform export results

**Usage**:

```console
$ laceworkreports export activities user_logins jinja2 [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--template-path TEXT`: Path to jinja2 template. Results will be passed as 'dataset' variable.  [required]
* `--help`: Show this message and exit.

##### `laceworkreports export activities user_logins json`

Export to json

**Usage**:

```console
$ laceworkreports export activities user_logins json [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--append / --no-append`: Boolean value to append or replace results  [default: False]
* `--help`: Show this message and exit.

##### `laceworkreports export activities user_logins postgres`

Export to postgres database

**Usage**:

```console
$ laceworkreports export activities user_logins postgres [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--db-connection TEXT`: Postgres connection string (e.g. postgresql://postgres:password@localhost:5432/postgres)  [required]
* `--db-table TEXT`: Postgres table to store results  [default: export]
* `--db-if-exists [append|replace|fail]`: Action to take if db table already exists  [default: replace]
* `--db-create-if-missing / --no-db-create-if-missing`: Bool to create database if missing  [default: True]
* `--help`: Show this message and exit.

### `laceworkreports export agent_access_tokens`

Query lacework api agent_access_tokens types

**Usage**:

```console
$ laceworkreports export agent_access_tokens [OPTIONS] COMMAND [ARGS]...
```

**Options**:

* `--help`: Show this message and exit.

laceworkreports export agent_access_tokens <subtype> <exporttype> [OPTIONS]

**Commands**:

* `csv`: Export to csv
* `jinja2`: Use jinja template to transform export...
* `json`: Export to json
* `postgres`: Export to postgres database

#### `laceworkreports export agent_access_tokens csv`

Export to csv

**Usage**:

```console
$ laceworkreports export agent_access_tokens csv [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--append / --no-append`: Boolean value to append or replace results  [default: False]
* `--help`: Show this message and exit.

#### `laceworkreports export agent_access_tokens jinja2`

Use jinja template to transform export results

**Usage**:

```console
$ laceworkreports export agent_access_tokens jinja2 [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--template-path TEXT`: Path to jinja2 template. Results will be passed as 'dataset' variable.  [required]
* `--help`: Show this message and exit.

#### `laceworkreports export agent_access_tokens json`

Export to json

**Usage**:

```console
$ laceworkreports export agent_access_tokens json [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--append / --no-append`: Boolean value to append or replace results  [default: False]
* `--help`: Show this message and exit.

#### `laceworkreports export agent_access_tokens postgres`

Export to postgres database

**Usage**:

```console
$ laceworkreports export agent_access_tokens postgres [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--db-connection TEXT`: Postgres connection string (e.g. postgresql://postgres:password@localhost:5432/postgres)  [required]
* `--db-table TEXT`: Postgres table to store results  [default: export]
* `--db-if-exists [append|replace|fail]`: Action to take if db table already exists  [default: replace]
* `--db-create-if-missing / --no-db-create-if-missing`: Bool to create database if missing  [default: True]
* `--help`: Show this message and exit.

### `laceworkreports export alert_channels`

Query lacework api alert_channels types

**Usage**:

```console
$ laceworkreports export alert_channels [OPTIONS] COMMAND [ARGS]...
```

**Options**:

* `--help`: Show this message and exit.

laceworkreports export alert_channels <subtype> <exporttype> [OPTIONS]

**Commands**:

* `csv`: Export to csv
* `jinja2`: Use jinja template to transform export...
* `json`: Export to json
* `postgres`: Export to postgres database

#### `laceworkreports export alert_channels csv`

Export to csv

**Usage**:

```console
$ laceworkreports export alert_channels csv [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--append / --no-append`: Boolean value to append or replace results  [default: False]
* `--help`: Show this message and exit.

#### `laceworkreports export alert_channels jinja2`

Use jinja template to transform export results

**Usage**:

```console
$ laceworkreports export alert_channels jinja2 [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--template-path TEXT`: Path to jinja2 template. Results will be passed as 'dataset' variable.  [required]
* `--help`: Show this message and exit.

#### `laceworkreports export alert_channels json`

Export to json

**Usage**:

```console
$ laceworkreports export alert_channels json [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--append / --no-append`: Boolean value to append or replace results  [default: False]
* `--help`: Show this message and exit.

#### `laceworkreports export alert_channels postgres`

Export to postgres database

**Usage**:

```console
$ laceworkreports export alert_channels postgres [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--db-connection TEXT`: Postgres connection string (e.g. postgresql://postgres:password@localhost:5432/postgres)  [required]
* `--db-table TEXT`: Postgres table to store results  [default: export]
* `--db-if-exists [append|replace|fail]`: Action to take if db table already exists  [default: replace]
* `--db-create-if-missing / --no-db-create-if-missing`: Bool to create database if missing  [default: True]
* `--help`: Show this message and exit.

### `laceworkreports export alert_rules`

Query lacework api alert_rules types

**Usage**:

```console
$ laceworkreports export alert_rules [OPTIONS] COMMAND [ARGS]...
```

**Options**:

* `--help`: Show this message and exit.

laceworkreports export alert_rules <subtype> <exporttype> [OPTIONS]

**Commands**:

* `csv`: Export to csv
* `jinja2`: Use jinja template to transform export...
* `json`: Export to json
* `postgres`: Export to postgres database

#### `laceworkreports export alert_rules csv`

Export to csv

**Usage**:

```console
$ laceworkreports export alert_rules csv [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--append / --no-append`: Boolean value to append or replace results  [default: False]
* `--help`: Show this message and exit.

#### `laceworkreports export alert_rules jinja2`

Use jinja template to transform export results

**Usage**:

```console
$ laceworkreports export alert_rules jinja2 [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--template-path TEXT`: Path to jinja2 template. Results will be passed as 'dataset' variable.  [required]
* `--help`: Show this message and exit.

#### `laceworkreports export alert_rules json`

Export to json

**Usage**:

```console
$ laceworkreports export alert_rules json [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--append / --no-append`: Boolean value to append or replace results  [default: False]
* `--help`: Show this message and exit.

#### `laceworkreports export alert_rules postgres`

Export to postgres database

**Usage**:

```console
$ laceworkreports export alert_rules postgres [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--db-connection TEXT`: Postgres connection string (e.g. postgresql://postgres:password@localhost:5432/postgres)  [required]
* `--db-table TEXT`: Postgres table to store results  [default: export]
* `--db-if-exists [append|replace|fail]`: Action to take if db table already exists  [default: replace]
* `--db-create-if-missing / --no-db-create-if-missing`: Bool to create database if missing  [default: True]
* `--help`: Show this message and exit.

### `laceworkreports export alerts`

Query lacework api alerts types

**Usage**:

```console
$ laceworkreports export alerts [OPTIONS] COMMAND [ARGS]...
```

**Options**:

* `--help`: Show this message and exit.

laceworkreports export alerts <subtype> <exporttype> [OPTIONS]

**Commands**:

* `csv`: Export to csv
* `jinja2`: Use jinja template to transform export...
* `json`: Export to json
* `postgres`: Export to postgres database

#### `laceworkreports export alerts csv`

Export to csv

**Usage**:

```console
$ laceworkreports export alerts csv [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--append / --no-append`: Boolean value to append or replace results  [default: False]
* `--help`: Show this message and exit.

#### `laceworkreports export alerts jinja2`

Use jinja template to transform export results

**Usage**:

```console
$ laceworkreports export alerts jinja2 [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--template-path TEXT`: Path to jinja2 template. Results will be passed as 'dataset' variable.  [required]
* `--help`: Show this message and exit.

#### `laceworkreports export alerts json`

Export to json

**Usage**:

```console
$ laceworkreports export alerts json [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--append / --no-append`: Boolean value to append or replace results  [default: False]
* `--help`: Show this message and exit.

#### `laceworkreports export alerts postgres`

Export to postgres database

**Usage**:

```console
$ laceworkreports export alerts postgres [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--db-connection TEXT`: Postgres connection string (e.g. postgresql://postgres:password@localhost:5432/postgres)  [required]
* `--db-table TEXT`: Postgres table to store results  [default: export]
* `--db-if-exists [append|replace|fail]`: Action to take if db table already exists  [default: replace]
* `--db-create-if-missing / --no-db-create-if-missing`: Bool to create database if missing  [default: True]
* `--help`: Show this message and exit.

### `laceworkreports export audit_logs`

Query lacework api audit_logs types

**Usage**:

```console
$ laceworkreports export audit_logs [OPTIONS] COMMAND [ARGS]...
```

**Options**:

* `--help`: Show this message and exit.

laceworkreports export audit_logs <subtype> <exporttype> [OPTIONS]

**Commands**:

* `csv`: Export to csv
* `jinja2`: Use jinja template to transform export...
* `json`: Export to json
* `postgres`: Export to postgres database

#### `laceworkreports export audit_logs csv`

Export to csv

**Usage**:

```console
$ laceworkreports export audit_logs csv [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--append / --no-append`: Boolean value to append or replace results  [default: False]
* `--help`: Show this message and exit.

#### `laceworkreports export audit_logs jinja2`

Use jinja template to transform export results

**Usage**:

```console
$ laceworkreports export audit_logs jinja2 [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--template-path TEXT`: Path to jinja2 template. Results will be passed as 'dataset' variable.  [required]
* `--help`: Show this message and exit.

#### `laceworkreports export audit_logs json`

Export to json

**Usage**:

```console
$ laceworkreports export audit_logs json [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--append / --no-append`: Boolean value to append or replace results  [default: False]
* `--help`: Show this message and exit.

#### `laceworkreports export audit_logs postgres`

Export to postgres database

**Usage**:

```console
$ laceworkreports export audit_logs postgres [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--db-connection TEXT`: Postgres connection string (e.g. postgresql://postgres:password@localhost:5432/postgres)  [required]
* `--db-table TEXT`: Postgres table to store results  [default: export]
* `--db-if-exists [append|replace|fail]`: Action to take if db table already exists  [default: replace]
* `--db-create-if-missing / --no-db-create-if-missing`: Bool to create database if missing  [default: True]
* `--help`: Show this message and exit.

### `laceworkreports export cloud_accounts`

Query lacework api cloud_accounts types

**Usage**:

```console
$ laceworkreports export cloud_accounts [OPTIONS] COMMAND [ARGS]...
```

**Options**:

* `--help`: Show this message and exit.

laceworkreports export cloud_accounts <subtype> <exporttype> [OPTIONS]

**Commands**:

* `csv`: Export to csv
* `jinja2`: Use jinja template to transform export...
* `json`: Export to json
* `postgres`: Export to postgres database

#### `laceworkreports export cloud_accounts csv`

Export to csv

**Usage**:

```console
$ laceworkreports export cloud_accounts csv [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--append / --no-append`: Boolean value to append or replace results  [default: False]
* `--help`: Show this message and exit.

#### `laceworkreports export cloud_accounts jinja2`

Use jinja template to transform export results

**Usage**:

```console
$ laceworkreports export cloud_accounts jinja2 [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--template-path TEXT`: Path to jinja2 template. Results will be passed as 'dataset' variable.  [required]
* `--help`: Show this message and exit.

#### `laceworkreports export cloud_accounts json`

Export to json

**Usage**:

```console
$ laceworkreports export cloud_accounts json [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--append / --no-append`: Boolean value to append or replace results  [default: False]
* `--help`: Show this message and exit.

#### `laceworkreports export cloud_accounts postgres`

Export to postgres database

**Usage**:

```console
$ laceworkreports export cloud_accounts postgres [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--db-connection TEXT`: Postgres connection string (e.g. postgresql://postgres:password@localhost:5432/postgres)  [required]
* `--db-table TEXT`: Postgres table to store results  [default: export]
* `--db-if-exists [append|replace|fail]`: Action to take if db table already exists  [default: replace]
* `--db-create-if-missing / --no-db-create-if-missing`: Bool to create database if missing  [default: True]
* `--help`: Show this message and exit.

### `laceworkreports export cloud_activities`

Query lacework api cloud_activities types

**Usage**:

```console
$ laceworkreports export cloud_activities [OPTIONS] COMMAND [ARGS]...
```

**Options**:

* `--help`: Show this message and exit.

laceworkreports export cloud_activities <subtype> <exporttype> [OPTIONS]

**Commands**:

* `csv`: Export to csv
* `jinja2`: Use jinja template to transform export...
* `json`: Export to json
* `postgres`: Export to postgres database

#### `laceworkreports export cloud_activities csv`

Export to csv

**Usage**:

```console
$ laceworkreports export cloud_activities csv [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--append / --no-append`: Boolean value to append or replace results  [default: False]
* `--help`: Show this message and exit.

#### `laceworkreports export cloud_activities jinja2`

Use jinja template to transform export results

**Usage**:

```console
$ laceworkreports export cloud_activities jinja2 [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--template-path TEXT`: Path to jinja2 template. Results will be passed as 'dataset' variable.  [required]
* `--help`: Show this message and exit.

#### `laceworkreports export cloud_activities json`

Export to json

**Usage**:

```console
$ laceworkreports export cloud_activities json [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--append / --no-append`: Boolean value to append or replace results  [default: False]
* `--help`: Show this message and exit.

#### `laceworkreports export cloud_activities postgres`

Export to postgres database

**Usage**:

```console
$ laceworkreports export cloud_activities postgres [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--db-connection TEXT`: Postgres connection string (e.g. postgresql://postgres:password@localhost:5432/postgres)  [required]
* `--db-table TEXT`: Postgres table to store results  [default: export]
* `--db-if-exists [append|replace|fail]`: Action to take if db table already exists  [default: replace]
* `--db-create-if-missing / --no-db-create-if-missing`: Bool to create database if missing  [default: True]
* `--help`: Show this message and exit.

### `laceworkreports export configs`

Query lacework api configs types

**Usage**:

```console
$ laceworkreports export configs [OPTIONS] COMMAND [ARGS]...
```

**Options**:

* `--help`: Show this message and exit.

laceworkreports export configs <subtype> <exporttype> [OPTIONS]

**Commands**:

* `compliance_evaluations`: Retrieve lacework activities api...

#### `laceworkreports export configs compliance_evaluations`

Retrieve lacework activities api compliance_evaluations events

**Usage**:

```console
$ laceworkreports export configs compliance_evaluations [OPTIONS] COMMAND [ARGS]...
```

**Options**:

* `--help`: Show this message and exit.

laceworkreports export configs compliance_evaluations <exporttype> [OPTIONS]

**Commands**:

* `csv`: Export to csv
* `jinja2`: Use jinja template to transform export...
* `json`: Export to json
* `postgres`: Export to postgres database

##### `laceworkreports export configs compliance_evaluations csv`

Export to csv

**Usage**:

```console
$ laceworkreports export configs compliance_evaluations csv [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--dataset [AwsCompliance|AzureCompliance|GcpCompliance]`: [default: AwsCompliance]
* `--append / --no-append`: Boolean value to append or replace results  [default: False]
* `--help`: Show this message and exit.

##### `laceworkreports export configs compliance_evaluations jinja2`

Use jinja template to transform export results

**Usage**:

```console
$ laceworkreports export configs compliance_evaluations jinja2 [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--dataset [AwsCompliance|AzureCompliance|GcpCompliance]`: [default: AwsCompliance]
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--template-path TEXT`: Path to jinja2 template. Results will be passed as 'dataset' variable.  [required]
* `--help`: Show this message and exit.

##### `laceworkreports export configs compliance_evaluations json`

Export to json

**Usage**:

```console
$ laceworkreports export configs compliance_evaluations json [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--dataset [AwsCompliance|AzureCompliance|GcpCompliance]`: [default: AwsCompliance]
* `--append / --no-append`: Boolean value to append or replace results  [default: False]
* `--help`: Show this message and exit.

##### `laceworkreports export configs compliance_evaluations postgres`

Export to postgres database

**Usage**:

```console
$ laceworkreports export configs compliance_evaluations postgres [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--dataset [AwsCompliance|AzureCompliance|GcpCompliance]`: [default: AwsCompliance]
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--db-connection TEXT`: Postgres connection string (e.g. postgresql://postgres:password@localhost:5432/postgres)  [required]
* `--db-table TEXT`: Postgres table to store results  [default: export]
* `--db-if-exists [append|replace|fail]`: Action to take if db table already exists  [default: replace]
* `--db-create-if-missing / --no-db-create-if-missing`: Bool to create database if missing  [default: True]
* `--help`: Show this message and exit.

### `laceworkreports export container_registries`

Query lacework api container_registries types

**Usage**:

```console
$ laceworkreports export container_registries [OPTIONS] COMMAND [ARGS]...
```

**Options**:

* `--help`: Show this message and exit.

laceworkreports export container_registries <subtype> <exporttype> [OPTIONS]

**Commands**:

* `csv`: Export to csv
* `jinja2`: Use jinja template to transform export...
* `json`: Export to json
* `postgres`: Export to postgres database

#### `laceworkreports export container_registries csv`

Export to csv

**Usage**:

```console
$ laceworkreports export container_registries csv [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--append / --no-append`: Boolean value to append or replace results  [default: False]
* `--help`: Show this message and exit.

#### `laceworkreports export container_registries jinja2`

Use jinja template to transform export results

**Usage**:

```console
$ laceworkreports export container_registries jinja2 [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--template-path TEXT`: Path to jinja2 template. Results will be passed as 'dataset' variable.  [required]
* `--help`: Show this message and exit.

#### `laceworkreports export container_registries json`

Export to json

**Usage**:

```console
$ laceworkreports export container_registries json [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--append / --no-append`: Boolean value to append or replace results  [default: False]
* `--help`: Show this message and exit.

#### `laceworkreports export container_registries postgres`

Export to postgres database

**Usage**:

```console
$ laceworkreports export container_registries postgres [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--db-connection TEXT`: Postgres connection string (e.g. postgresql://postgres:password@localhost:5432/postgres)  [required]
* `--db-table TEXT`: Postgres table to store results  [default: export]
* `--db-if-exists [append|replace|fail]`: Action to take if db table already exists  [default: replace]
* `--db-create-if-missing / --no-db-create-if-missing`: Bool to create database if missing  [default: True]
* `--help`: Show this message and exit.

### `laceworkreports export entities`

Query lacework api entities types

**Usage**:

```console
$ laceworkreports export entities [OPTIONS] COMMAND [ARGS]...
```

**Options**:

* `--help`: Show this message and exit.

laceworkreports export entities <subtype> <exporttype> [OPTIONS]

**Commands**:

* `applications`: Retrieve lacework activities api applications...
* `command_lines`: Retrieve lacework activities api...
* `containers`: Retrieve lacework activities api containers...
* `files`: Retrieve lacework activities api files events
* `internal_ip_addresses`: Retrieve lacework activities api...
* `k8s_pods`: Retrieve lacework activities api k8s_pods...
* `machine_details`: Retrieve lacework activities api...
* `machines`: Retrieve lacework activities api machines...
* `network_interfaces`: Retrieve lacework activities api...
* `new_file_hashes`: Retrieve lacework activities api...
* `packages`: Retrieve lacework activities api packages...
* `processes`: Retrieve lacework activities api processes...
* `users`: Retrieve lacework activities api users events

#### `laceworkreports export entities applications`

Retrieve lacework activities api applications events

**Usage**:

```console
$ laceworkreports export entities applications [OPTIONS] COMMAND [ARGS]...
```

**Options**:

* `--help`: Show this message and exit.

laceworkreports export entities applications <exporttype> [OPTIONS]

**Commands**:

* `csv`: Export to csv
* `jinja2`: Use jinja template to transform export...
* `json`: Export to json
* `postgres`: Export to postgres database

##### `laceworkreports export entities applications csv`

Export to csv

**Usage**:

```console
$ laceworkreports export entities applications csv [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--append / --no-append`: Boolean value to append or replace results  [default: False]
* `--help`: Show this message and exit.

##### `laceworkreports export entities applications jinja2`

Use jinja template to transform export results

**Usage**:

```console
$ laceworkreports export entities applications jinja2 [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--template-path TEXT`: Path to jinja2 template. Results will be passed as 'dataset' variable.  [required]
* `--help`: Show this message and exit.

##### `laceworkreports export entities applications json`

Export to json

**Usage**:

```console
$ laceworkreports export entities applications json [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--append / --no-append`: Boolean value to append or replace results  [default: False]
* `--help`: Show this message and exit.

##### `laceworkreports export entities applications postgres`

Export to postgres database

**Usage**:

```console
$ laceworkreports export entities applications postgres [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--db-connection TEXT`: Postgres connection string (e.g. postgresql://postgres:password@localhost:5432/postgres)  [required]
* `--db-table TEXT`: Postgres table to store results  [default: export]
* `--db-if-exists [append|replace|fail]`: Action to take if db table already exists  [default: replace]
* `--db-create-if-missing / --no-db-create-if-missing`: Bool to create database if missing  [default: True]
* `--help`: Show this message and exit.

#### `laceworkreports export entities command_lines`

Retrieve lacework activities api command_lines events

**Usage**:

```console
$ laceworkreports export entities command_lines [OPTIONS] COMMAND [ARGS]...
```

**Options**:

* `--help`: Show this message and exit.

laceworkreports export entities command_lines <exporttype> [OPTIONS]

**Commands**:

* `csv`: Export to csv
* `jinja2`: Use jinja template to transform export...
* `json`: Export to json
* `postgres`: Export to postgres database

##### `laceworkreports export entities command_lines csv`

Export to csv

**Usage**:

```console
$ laceworkreports export entities command_lines csv [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--append / --no-append`: Boolean value to append or replace results  [default: False]
* `--help`: Show this message and exit.

##### `laceworkreports export entities command_lines jinja2`

Use jinja template to transform export results

**Usage**:

```console
$ laceworkreports export entities command_lines jinja2 [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--template-path TEXT`: Path to jinja2 template. Results will be passed as 'dataset' variable.  [required]
* `--help`: Show this message and exit.

##### `laceworkreports export entities command_lines json`

Export to json

**Usage**:

```console
$ laceworkreports export entities command_lines json [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--append / --no-append`: Boolean value to append or replace results  [default: False]
* `--help`: Show this message and exit.

##### `laceworkreports export entities command_lines postgres`

Export to postgres database

**Usage**:

```console
$ laceworkreports export entities command_lines postgres [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--db-connection TEXT`: Postgres connection string (e.g. postgresql://postgres:password@localhost:5432/postgres)  [required]
* `--db-table TEXT`: Postgres table to store results  [default: export]
* `--db-if-exists [append|replace|fail]`: Action to take if db table already exists  [default: replace]
* `--db-create-if-missing / --no-db-create-if-missing`: Bool to create database if missing  [default: True]
* `--help`: Show this message and exit.

#### `laceworkreports export entities containers`

Retrieve lacework activities api containers events

**Usage**:

```console
$ laceworkreports export entities containers [OPTIONS] COMMAND [ARGS]...
```

**Options**:

* `--help`: Show this message and exit.

laceworkreports export entities containers <exporttype> [OPTIONS]

**Commands**:

* `csv`: Export to csv
* `jinja2`: Use jinja template to transform export...
* `json`: Export to json
* `postgres`: Export to postgres database

##### `laceworkreports export entities containers csv`

Export to csv

**Usage**:

```console
$ laceworkreports export entities containers csv [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--append / --no-append`: Boolean value to append or replace results  [default: False]
* `--help`: Show this message and exit.

##### `laceworkreports export entities containers jinja2`

Use jinja template to transform export results

**Usage**:

```console
$ laceworkreports export entities containers jinja2 [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--template-path TEXT`: Path to jinja2 template. Results will be passed as 'dataset' variable.  [required]
* `--help`: Show this message and exit.

##### `laceworkreports export entities containers json`

Export to json

**Usage**:

```console
$ laceworkreports export entities containers json [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--append / --no-append`: Boolean value to append or replace results  [default: False]
* `--help`: Show this message and exit.

##### `laceworkreports export entities containers postgres`

Export to postgres database

**Usage**:

```console
$ laceworkreports export entities containers postgres [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--db-connection TEXT`: Postgres connection string (e.g. postgresql://postgres:password@localhost:5432/postgres)  [required]
* `--db-table TEXT`: Postgres table to store results  [default: export]
* `--db-if-exists [append|replace|fail]`: Action to take if db table already exists  [default: replace]
* `--db-create-if-missing / --no-db-create-if-missing`: Bool to create database if missing  [default: True]
* `--help`: Show this message and exit.

#### `laceworkreports export entities files`

Retrieve lacework activities api files events

**Usage**:

```console
$ laceworkreports export entities files [OPTIONS] COMMAND [ARGS]...
```

**Options**:

* `--help`: Show this message and exit.

laceworkreports export entities files <exporttype> [OPTIONS]

**Commands**:

* `csv`: Export to csv
* `jinja2`: Use jinja template to transform export...
* `json`: Export to json
* `postgres`: Export to postgres database

##### `laceworkreports export entities files csv`

Export to csv

**Usage**:

```console
$ laceworkreports export entities files csv [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--append / --no-append`: Boolean value to append or replace results  [default: False]
* `--help`: Show this message and exit.

##### `laceworkreports export entities files jinja2`

Use jinja template to transform export results

**Usage**:

```console
$ laceworkreports export entities files jinja2 [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--template-path TEXT`: Path to jinja2 template. Results will be passed as 'dataset' variable.  [required]
* `--help`: Show this message and exit.

##### `laceworkreports export entities files json`

Export to json

**Usage**:

```console
$ laceworkreports export entities files json [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--append / --no-append`: Boolean value to append or replace results  [default: False]
* `--help`: Show this message and exit.

##### `laceworkreports export entities files postgres`

Export to postgres database

**Usage**:

```console
$ laceworkreports export entities files postgres [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--db-connection TEXT`: Postgres connection string (e.g. postgresql://postgres:password@localhost:5432/postgres)  [required]
* `--db-table TEXT`: Postgres table to store results  [default: export]
* `--db-if-exists [append|replace|fail]`: Action to take if db table already exists  [default: replace]
* `--db-create-if-missing / --no-db-create-if-missing`: Bool to create database if missing  [default: True]
* `--help`: Show this message and exit.

#### `laceworkreports export entities internal_ip_addresses`

Retrieve lacework activities api internal_ip_addresses events

**Usage**:

```console
$ laceworkreports export entities internal_ip_addresses [OPTIONS] COMMAND [ARGS]...
```

**Options**:

* `--help`: Show this message and exit.

laceworkreports export entities internal_ip_addresses <exporttype> [OPTIONS]

**Commands**:

* `csv`: Export to csv
* `jinja2`: Use jinja template to transform export...
* `json`: Export to json
* `postgres`: Export to postgres database

##### `laceworkreports export entities internal_ip_addresses csv`

Export to csv

**Usage**:

```console
$ laceworkreports export entities internal_ip_addresses csv [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--append / --no-append`: Boolean value to append or replace results  [default: False]
* `--help`: Show this message and exit.

##### `laceworkreports export entities internal_ip_addresses jinja2`

Use jinja template to transform export results

**Usage**:

```console
$ laceworkreports export entities internal_ip_addresses jinja2 [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--template-path TEXT`: Path to jinja2 template. Results will be passed as 'dataset' variable.  [required]
* `--help`: Show this message and exit.

##### `laceworkreports export entities internal_ip_addresses json`

Export to json

**Usage**:

```console
$ laceworkreports export entities internal_ip_addresses json [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--append / --no-append`: Boolean value to append or replace results  [default: False]
* `--help`: Show this message and exit.

##### `laceworkreports export entities internal_ip_addresses postgres`

Export to postgres database

**Usage**:

```console
$ laceworkreports export entities internal_ip_addresses postgres [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--db-connection TEXT`: Postgres connection string (e.g. postgresql://postgres:password@localhost:5432/postgres)  [required]
* `--db-table TEXT`: Postgres table to store results  [default: export]
* `--db-if-exists [append|replace|fail]`: Action to take if db table already exists  [default: replace]
* `--db-create-if-missing / --no-db-create-if-missing`: Bool to create database if missing  [default: True]
* `--help`: Show this message and exit.

#### `laceworkreports export entities k8s_pods`

Retrieve lacework activities api k8s_pods events

**Usage**:

```console
$ laceworkreports export entities k8s_pods [OPTIONS] COMMAND [ARGS]...
```

**Options**:

* `--help`: Show this message and exit.

laceworkreports export entities k8s_pods <exporttype> [OPTIONS]

**Commands**:

* `csv`: Export to csv
* `jinja2`: Use jinja template to transform export...
* `json`: Export to json
* `postgres`: Export to postgres database

##### `laceworkreports export entities k8s_pods csv`

Export to csv

**Usage**:

```console
$ laceworkreports export entities k8s_pods csv [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--append / --no-append`: Boolean value to append or replace results  [default: False]
* `--help`: Show this message and exit.

##### `laceworkreports export entities k8s_pods jinja2`

Use jinja template to transform export results

**Usage**:

```console
$ laceworkreports export entities k8s_pods jinja2 [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--template-path TEXT`: Path to jinja2 template. Results will be passed as 'dataset' variable.  [required]
* `--help`: Show this message and exit.

##### `laceworkreports export entities k8s_pods json`

Export to json

**Usage**:

```console
$ laceworkreports export entities k8s_pods json [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--append / --no-append`: Boolean value to append or replace results  [default: False]
* `--help`: Show this message and exit.

##### `laceworkreports export entities k8s_pods postgres`

Export to postgres database

**Usage**:

```console
$ laceworkreports export entities k8s_pods postgres [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--db-connection TEXT`: Postgres connection string (e.g. postgresql://postgres:password@localhost:5432/postgres)  [required]
* `--db-table TEXT`: Postgres table to store results  [default: export]
* `--db-if-exists [append|replace|fail]`: Action to take if db table already exists  [default: replace]
* `--db-create-if-missing / --no-db-create-if-missing`: Bool to create database if missing  [default: True]
* `--help`: Show this message and exit.

#### `laceworkreports export entities machine_details`

Retrieve lacework activities api machine_details events

**Usage**:

```console
$ laceworkreports export entities machine_details [OPTIONS] COMMAND [ARGS]...
```

**Options**:

* `--help`: Show this message and exit.

laceworkreports export entities machine_details <exporttype> [OPTIONS]

**Commands**:

* `csv`: Export to csv
* `jinja2`: Use jinja template to transform export...
* `json`: Export to json
* `postgres`: Export to postgres database

##### `laceworkreports export entities machine_details csv`

Export to csv

**Usage**:

```console
$ laceworkreports export entities machine_details csv [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--append / --no-append`: Boolean value to append or replace results  [default: False]
* `--help`: Show this message and exit.

##### `laceworkreports export entities machine_details jinja2`

Use jinja template to transform export results

**Usage**:

```console
$ laceworkreports export entities machine_details jinja2 [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--template-path TEXT`: Path to jinja2 template. Results will be passed as 'dataset' variable.  [required]
* `--help`: Show this message and exit.

##### `laceworkreports export entities machine_details json`

Export to json

**Usage**:

```console
$ laceworkreports export entities machine_details json [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--append / --no-append`: Boolean value to append or replace results  [default: False]
* `--help`: Show this message and exit.

##### `laceworkreports export entities machine_details postgres`

Export to postgres database

**Usage**:

```console
$ laceworkreports export entities machine_details postgres [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--db-connection TEXT`: Postgres connection string (e.g. postgresql://postgres:password@localhost:5432/postgres)  [required]
* `--db-table TEXT`: Postgres table to store results  [default: export]
* `--db-if-exists [append|replace|fail]`: Action to take if db table already exists  [default: replace]
* `--db-create-if-missing / --no-db-create-if-missing`: Bool to create database if missing  [default: True]
* `--help`: Show this message and exit.

#### `laceworkreports export entities machines`

Retrieve lacework activities api machines events

**Usage**:

```console
$ laceworkreports export entities machines [OPTIONS] COMMAND [ARGS]...
```

**Options**:

* `--help`: Show this message and exit.

laceworkreports export entities machines <exporttype> [OPTIONS]

**Commands**:

* `csv`: Export to csv
* `jinja2`: Use jinja template to transform export...
* `json`: Export to json
* `postgres`: Export to postgres database

##### `laceworkreports export entities machines csv`

Export to csv

**Usage**:

```console
$ laceworkreports export entities machines csv [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--append / --no-append`: Boolean value to append or replace results  [default: False]
* `--help`: Show this message and exit.

##### `laceworkreports export entities machines jinja2`

Use jinja template to transform export results

**Usage**:

```console
$ laceworkreports export entities machines jinja2 [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--template-path TEXT`: Path to jinja2 template. Results will be passed as 'dataset' variable.  [required]
* `--help`: Show this message and exit.

##### `laceworkreports export entities machines json`

Export to json

**Usage**:

```console
$ laceworkreports export entities machines json [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--append / --no-append`: Boolean value to append or replace results  [default: False]
* `--help`: Show this message and exit.

##### `laceworkreports export entities machines postgres`

Export to postgres database

**Usage**:

```console
$ laceworkreports export entities machines postgres [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--db-connection TEXT`: Postgres connection string (e.g. postgresql://postgres:password@localhost:5432/postgres)  [required]
* `--db-table TEXT`: Postgres table to store results  [default: export]
* `--db-if-exists [append|replace|fail]`: Action to take if db table already exists  [default: replace]
* `--db-create-if-missing / --no-db-create-if-missing`: Bool to create database if missing  [default: True]
* `--help`: Show this message and exit.

#### `laceworkreports export entities network_interfaces`

Retrieve lacework activities api network_interfaces events

**Usage**:

```console
$ laceworkreports export entities network_interfaces [OPTIONS] COMMAND [ARGS]...
```

**Options**:

* `--help`: Show this message and exit.

laceworkreports export entities network_interfaces <exporttype> [OPTIONS]

**Commands**:

* `csv`: Export to csv
* `jinja2`: Use jinja template to transform export...
* `json`: Export to json
* `postgres`: Export to postgres database

##### `laceworkreports export entities network_interfaces csv`

Export to csv

**Usage**:

```console
$ laceworkreports export entities network_interfaces csv [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--append / --no-append`: Boolean value to append or replace results  [default: False]
* `--help`: Show this message and exit.

##### `laceworkreports export entities network_interfaces jinja2`

Use jinja template to transform export results

**Usage**:

```console
$ laceworkreports export entities network_interfaces jinja2 [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--template-path TEXT`: Path to jinja2 template. Results will be passed as 'dataset' variable.  [required]
* `--help`: Show this message and exit.

##### `laceworkreports export entities network_interfaces json`

Export to json

**Usage**:

```console
$ laceworkreports export entities network_interfaces json [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--append / --no-append`: Boolean value to append or replace results  [default: False]
* `--help`: Show this message and exit.

##### `laceworkreports export entities network_interfaces postgres`

Export to postgres database

**Usage**:

```console
$ laceworkreports export entities network_interfaces postgres [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--db-connection TEXT`: Postgres connection string (e.g. postgresql://postgres:password@localhost:5432/postgres)  [required]
* `--db-table TEXT`: Postgres table to store results  [default: export]
* `--db-if-exists [append|replace|fail]`: Action to take if db table already exists  [default: replace]
* `--db-create-if-missing / --no-db-create-if-missing`: Bool to create database if missing  [default: True]
* `--help`: Show this message and exit.

#### `laceworkreports export entities new_file_hashes`

Retrieve lacework activities api new_file_hashes events

**Usage**:

```console
$ laceworkreports export entities new_file_hashes [OPTIONS] COMMAND [ARGS]...
```

**Options**:

* `--help`: Show this message and exit.

laceworkreports export entities new_file_hashes <exporttype> [OPTIONS]

**Commands**:

* `csv`: Export to csv
* `jinja2`: Use jinja template to transform export...
* `json`: Export to json
* `postgres`: Export to postgres database

##### `laceworkreports export entities new_file_hashes csv`

Export to csv

**Usage**:

```console
$ laceworkreports export entities new_file_hashes csv [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--append / --no-append`: Boolean value to append or replace results  [default: False]
* `--help`: Show this message and exit.

##### `laceworkreports export entities new_file_hashes jinja2`

Use jinja template to transform export results

**Usage**:

```console
$ laceworkreports export entities new_file_hashes jinja2 [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--template-path TEXT`: Path to jinja2 template. Results will be passed as 'dataset' variable.  [required]
* `--help`: Show this message and exit.

##### `laceworkreports export entities new_file_hashes json`

Export to json

**Usage**:

```console
$ laceworkreports export entities new_file_hashes json [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--append / --no-append`: Boolean value to append or replace results  [default: False]
* `--help`: Show this message and exit.

##### `laceworkreports export entities new_file_hashes postgres`

Export to postgres database

**Usage**:

```console
$ laceworkreports export entities new_file_hashes postgres [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--db-connection TEXT`: Postgres connection string (e.g. postgresql://postgres:password@localhost:5432/postgres)  [required]
* `--db-table TEXT`: Postgres table to store results  [default: export]
* `--db-if-exists [append|replace|fail]`: Action to take if db table already exists  [default: replace]
* `--db-create-if-missing / --no-db-create-if-missing`: Bool to create database if missing  [default: True]
* `--help`: Show this message and exit.

#### `laceworkreports export entities packages`

Retrieve lacework activities api packages events

**Usage**:

```console
$ laceworkreports export entities packages [OPTIONS] COMMAND [ARGS]...
```

**Options**:

* `--help`: Show this message and exit.

laceworkreports export entities packages <exporttype> [OPTIONS]

**Commands**:

* `csv`: Export to csv
* `jinja2`: Use jinja template to transform export...
* `json`: Export to json
* `postgres`: Export to postgres database

##### `laceworkreports export entities packages csv`

Export to csv

**Usage**:

```console
$ laceworkreports export entities packages csv [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--append / --no-append`: Boolean value to append or replace results  [default: False]
* `--help`: Show this message and exit.

##### `laceworkreports export entities packages jinja2`

Use jinja template to transform export results

**Usage**:

```console
$ laceworkreports export entities packages jinja2 [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--template-path TEXT`: Path to jinja2 template. Results will be passed as 'dataset' variable.  [required]
* `--help`: Show this message and exit.

##### `laceworkreports export entities packages json`

Export to json

**Usage**:

```console
$ laceworkreports export entities packages json [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--append / --no-append`: Boolean value to append or replace results  [default: False]
* `--help`: Show this message and exit.

##### `laceworkreports export entities packages postgres`

Export to postgres database

**Usage**:

```console
$ laceworkreports export entities packages postgres [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--db-connection TEXT`: Postgres connection string (e.g. postgresql://postgres:password@localhost:5432/postgres)  [required]
* `--db-table TEXT`: Postgres table to store results  [default: export]
* `--db-if-exists [append|replace|fail]`: Action to take if db table already exists  [default: replace]
* `--db-create-if-missing / --no-db-create-if-missing`: Bool to create database if missing  [default: True]
* `--help`: Show this message and exit.

#### `laceworkreports export entities processes`

Retrieve lacework activities api processes events

**Usage**:

```console
$ laceworkreports export entities processes [OPTIONS] COMMAND [ARGS]...
```

**Options**:

* `--help`: Show this message and exit.

laceworkreports export entities processes <exporttype> [OPTIONS]

**Commands**:

* `csv`: Export to csv
* `jinja2`: Use jinja template to transform export...
* `json`: Export to json
* `postgres`: Export to postgres database

##### `laceworkreports export entities processes csv`

Export to csv

**Usage**:

```console
$ laceworkreports export entities processes csv [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--append / --no-append`: Boolean value to append or replace results  [default: False]
* `--help`: Show this message and exit.

##### `laceworkreports export entities processes jinja2`

Use jinja template to transform export results

**Usage**:

```console
$ laceworkreports export entities processes jinja2 [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--template-path TEXT`: Path to jinja2 template. Results will be passed as 'dataset' variable.  [required]
* `--help`: Show this message and exit.

##### `laceworkreports export entities processes json`

Export to json

**Usage**:

```console
$ laceworkreports export entities processes json [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--append / --no-append`: Boolean value to append or replace results  [default: False]
* `--help`: Show this message and exit.

##### `laceworkreports export entities processes postgres`

Export to postgres database

**Usage**:

```console
$ laceworkreports export entities processes postgres [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--db-connection TEXT`: Postgres connection string (e.g. postgresql://postgres:password@localhost:5432/postgres)  [required]
* `--db-table TEXT`: Postgres table to store results  [default: export]
* `--db-if-exists [append|replace|fail]`: Action to take if db table already exists  [default: replace]
* `--db-create-if-missing / --no-db-create-if-missing`: Bool to create database if missing  [default: True]
* `--help`: Show this message and exit.

#### `laceworkreports export entities users`

Retrieve lacework activities api users events

**Usage**:

```console
$ laceworkreports export entities users [OPTIONS] COMMAND [ARGS]...
```

**Options**:

* `--help`: Show this message and exit.

laceworkreports export entities users <exporttype> [OPTIONS]

**Commands**:

* `csv`: Export to csv
* `jinja2`: Use jinja template to transform export...
* `json`: Export to json
* `postgres`: Export to postgres database

##### `laceworkreports export entities users csv`

Export to csv

**Usage**:

```console
$ laceworkreports export entities users csv [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--append / --no-append`: Boolean value to append or replace results  [default: False]
* `--help`: Show this message and exit.

##### `laceworkreports export entities users jinja2`

Use jinja template to transform export results

**Usage**:

```console
$ laceworkreports export entities users jinja2 [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--template-path TEXT`: Path to jinja2 template. Results will be passed as 'dataset' variable.  [required]
* `--help`: Show this message and exit.

##### `laceworkreports export entities users json`

Export to json

**Usage**:

```console
$ laceworkreports export entities users json [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--append / --no-append`: Boolean value to append or replace results  [default: False]
* `--help`: Show this message and exit.

##### `laceworkreports export entities users postgres`

Export to postgres database

**Usage**:

```console
$ laceworkreports export entities users postgres [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--db-connection TEXT`: Postgres connection string (e.g. postgresql://postgres:password@localhost:5432/postgres)  [required]
* `--db-table TEXT`: Postgres table to store results  [default: export]
* `--db-if-exists [append|replace|fail]`: Action to take if db table already exists  [default: replace]
* `--db-create-if-missing / --no-db-create-if-missing`: Bool to create database if missing  [default: True]
* `--help`: Show this message and exit.

### `laceworkreports export queries`

Query lacework api queries types

**Usage**:

```console
$ laceworkreports export queries [OPTIONS] COMMAND [ARGS]...
```

**Options**:

* `--help`: Show this message and exit.

laceworkreports export queries <subtype> <exporttype> [OPTIONS]

**Commands**:

* `execute`: Retrieve lacework activities api execute...

#### `laceworkreports export queries execute`

Retrieve lacework activities api execute events

**Usage**:

```console
$ laceworkreports export queries execute [OPTIONS] COMMAND [ARGS]...
```

**Options**:

* `--help`: Show this message and exit.

laceworkreports export queries execute <exporttype> [OPTIONS]

**Commands**:

* `csv`: Export to csv
* `jinja2`: Use jinja template to transform export...
* `json`: Export to json
* `postgres`: Export to postgres database

##### `laceworkreports export queries execute csv`

Export to csv

**Usage**:

```console
$ laceworkreports export queries execute csv [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--lql-query TEXT`: LQL query string for file path (use @ to specify file path)  [required]
* `--field-map TEXT`: LQL query string for file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--append / --no-append`: Boolean value to append or replace results  [default: False]
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--help`: Show this message and exit.

##### `laceworkreports export queries execute jinja2`

Use jinja template to transform export results

**Usage**:

```console
$ laceworkreports export queries execute jinja2 [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--lql-query TEXT`: LQL query string for file path (use @ to specify file path)  [required]
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--template-path TEXT`: Path to jinja2 template. Results will be passed as 'dataset' variable.  [required]
* `--help`: Show this message and exit.

##### `laceworkreports export queries execute json`

Export to json

**Usage**:

```console
$ laceworkreports export queries execute json [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--lql-query TEXT`: LQL query string for file path (use @ to specify file path)  [required]
* `--field-map TEXT`: LQL query string for file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--append / --no-append`: Boolean value to append or replace results  [default: False]
* `--help`: Show this message and exit.

##### `laceworkreports export queries execute postgres`

Export to postgres database

**Usage**:

```console
$ laceworkreports export queries execute postgres [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--lql-query TEXT`: LQL query string for file path (use @ to specify file path)  [required]
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--db-connection TEXT`: Postgres connection string (e.g. postgresql://postgres:password@localhost:5432/postgres)  [required]
* `--db-table TEXT`: Postgres table to store results  [default: export]
* `--db-if-exists [append|replace|fail]`: Action to take if db table already exists  [default: replace]
* `--db-create-if-missing / --no-db-create-if-missing`: Bool to create database if missing  [default: True]
* `--help`: Show this message and exit.

### `laceworkreports export report_rules`

Query lacework api report_rules types

**Usage**:

```console
$ laceworkreports export report_rules [OPTIONS] COMMAND [ARGS]...
```

**Options**:

* `--help`: Show this message and exit.

laceworkreports export report_rules <subtype> <exporttype> [OPTIONS]

**Commands**:

* `csv`: Export to csv
* `jinja2`: Use jinja template to transform export...
* `json`: Export to json
* `postgres`: Export to postgres database

#### `laceworkreports export report_rules csv`

Export to csv

**Usage**:

```console
$ laceworkreports export report_rules csv [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--append / --no-append`: Boolean value to append or replace results  [default: False]
* `--help`: Show this message and exit.

#### `laceworkreports export report_rules jinja2`

Use jinja template to transform export results

**Usage**:

```console
$ laceworkreports export report_rules jinja2 [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--template-path TEXT`: Path to jinja2 template. Results will be passed as 'dataset' variable.  [required]
* `--help`: Show this message and exit.

#### `laceworkreports export report_rules json`

Export to json

**Usage**:

```console
$ laceworkreports export report_rules json [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--append / --no-append`: Boolean value to append or replace results  [default: False]
* `--help`: Show this message and exit.

#### `laceworkreports export report_rules postgres`

Export to postgres database

**Usage**:

```console
$ laceworkreports export report_rules postgres [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--db-connection TEXT`: Postgres connection string (e.g. postgresql://postgres:password@localhost:5432/postgres)  [required]
* `--db-table TEXT`: Postgres table to store results  [default: export]
* `--db-if-exists [append|replace|fail]`: Action to take if db table already exists  [default: replace]
* `--db-create-if-missing / --no-db-create-if-missing`: Bool to create database if missing  [default: True]
* `--help`: Show this message and exit.

### `laceworkreports export resource_groups`

Query lacework api resource_groups types

**Usage**:

```console
$ laceworkreports export resource_groups [OPTIONS] COMMAND [ARGS]...
```

**Options**:

* `--help`: Show this message and exit.

laceworkreports export resource_groups <subtype> <exporttype> [OPTIONS]

**Commands**:

* `csv`: Export to csv
* `jinja2`: Use jinja template to transform export...
* `json`: Export to json
* `postgres`: Export to postgres database

#### `laceworkreports export resource_groups csv`

Export to csv

**Usage**:

```console
$ laceworkreports export resource_groups csv [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--append / --no-append`: Boolean value to append or replace results  [default: False]
* `--help`: Show this message and exit.

#### `laceworkreports export resource_groups jinja2`

Use jinja template to transform export results

**Usage**:

```console
$ laceworkreports export resource_groups jinja2 [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--template-path TEXT`: Path to jinja2 template. Results will be passed as 'dataset' variable.  [required]
* `--help`: Show this message and exit.

#### `laceworkreports export resource_groups json`

Export to json

**Usage**:

```console
$ laceworkreports export resource_groups json [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--append / --no-append`: Boolean value to append or replace results  [default: False]
* `--help`: Show this message and exit.

#### `laceworkreports export resource_groups postgres`

Export to postgres database

**Usage**:

```console
$ laceworkreports export resource_groups postgres [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--db-connection TEXT`: Postgres connection string (e.g. postgresql://postgres:password@localhost:5432/postgres)  [required]
* `--db-table TEXT`: Postgres table to store results  [default: export]
* `--db-if-exists [append|replace|fail]`: Action to take if db table already exists  [default: replace]
* `--db-create-if-missing / --no-db-create-if-missing`: Bool to create database if missing  [default: True]
* `--help`: Show this message and exit.

### `laceworkreports export team_members`

Query lacework api team_members types

**Usage**:

```console
$ laceworkreports export team_members [OPTIONS] COMMAND [ARGS]...
```

**Options**:

* `--help`: Show this message and exit.

laceworkreports export team_members <subtype> <exporttype> [OPTIONS]

**Commands**:

* `csv`: Export to csv
* `jinja2`: Use jinja template to transform export...
* `json`: Export to json
* `postgres`: Export to postgres database

#### `laceworkreports export team_members csv`

Export to csv

**Usage**:

```console
$ laceworkreports export team_members csv [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--append / --no-append`: Boolean value to append or replace results  [default: False]
* `--help`: Show this message and exit.

#### `laceworkreports export team_members jinja2`

Use jinja template to transform export results

**Usage**:

```console
$ laceworkreports export team_members jinja2 [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--template-path TEXT`: Path to jinja2 template. Results will be passed as 'dataset' variable.  [required]
* `--help`: Show this message and exit.

#### `laceworkreports export team_members json`

Export to json

**Usage**:

```console
$ laceworkreports export team_members json [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--append / --no-append`: Boolean value to append or replace results  [default: False]
* `--help`: Show this message and exit.

#### `laceworkreports export team_members postgres`

Export to postgres database

**Usage**:

```console
$ laceworkreports export team_members postgres [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--db-connection TEXT`: Postgres connection string (e.g. postgresql://postgres:password@localhost:5432/postgres)  [required]
* `--db-table TEXT`: Postgres table to store results  [default: export]
* `--db-if-exists [append|replace|fail]`: Action to take if db table already exists  [default: replace]
* `--db-create-if-missing / --no-db-create-if-missing`: Bool to create database if missing  [default: True]
* `--help`: Show this message and exit.

### `laceworkreports export vulnerabilities`

Query lacework api vulnerabilities types

**Usage**:

```console
$ laceworkreports export vulnerabilities [OPTIONS] COMMAND [ARGS]...
```

**Options**:

* `--help`: Show this message and exit.

laceworkreports export vulnerabilities <subtype> <exporttype> [OPTIONS]

**Commands**:

* `containers`: Retrieve lacework activities api containers...
* `hosts`: Retrieve lacework activities api hosts events

#### `laceworkreports export vulnerabilities containers`

Retrieve lacework activities api containers events

**Usage**:

```console
$ laceworkreports export vulnerabilities containers [OPTIONS] COMMAND [ARGS]...
```

**Options**:

* `--help`: Show this message and exit.

laceworkreports export vulnerabilities containers <exporttype> [OPTIONS]

**Commands**:

* `csv`: Export to csv
* `jinja2`: Use jinja template to transform export...
* `json`: Export to json
* `postgres`: Export to postgres database

##### `laceworkreports export vulnerabilities containers csv`

Export to csv

**Usage**:

```console
$ laceworkreports export vulnerabilities containers csv [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--append / --no-append`: Boolean value to append or replace results  [default: False]
* `--help`: Show this message and exit.

##### `laceworkreports export vulnerabilities containers jinja2`

Use jinja template to transform export results

**Usage**:

```console
$ laceworkreports export vulnerabilities containers jinja2 [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--template-path TEXT`: Path to jinja2 template. Results will be passed as 'dataset' variable.  [required]
* `--help`: Show this message and exit.

##### `laceworkreports export vulnerabilities containers json`

Export to json

**Usage**:

```console
$ laceworkreports export vulnerabilities containers json [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--append / --no-append`: Boolean value to append or replace results  [default: False]
* `--help`: Show this message and exit.

##### `laceworkreports export vulnerabilities containers postgres`

Export to postgres database

**Usage**:

```console
$ laceworkreports export vulnerabilities containers postgres [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--db-connection TEXT`: Postgres connection string (e.g. postgresql://postgres:password@localhost:5432/postgres)  [required]
* `--db-table TEXT`: Postgres table to store results  [default: export]
* `--db-if-exists [append|replace|fail]`: Action to take if db table already exists  [default: replace]
* `--db-create-if-missing / --no-db-create-if-missing`: Bool to create database if missing  [default: True]
* `--help`: Show this message and exit.

#### `laceworkreports export vulnerabilities hosts`

Retrieve lacework activities api hosts events

**Usage**:

```console
$ laceworkreports export vulnerabilities hosts [OPTIONS] COMMAND [ARGS]...
```

**Options**:

* `--help`: Show this message and exit.

laceworkreports export vulnerabilities hosts <exporttype> [OPTIONS]

**Commands**:

* `csv`: Export to csv
* `jinja2`: Use jinja template to transform export...
* `json`: Export to json
* `postgres`: Export to postgres database

##### `laceworkreports export vulnerabilities hosts csv`

Export to csv

**Usage**:

```console
$ laceworkreports export vulnerabilities hosts csv [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--append / --no-append`: Boolean value to append or replace results  [default: False]
* `--help`: Show this message and exit.

##### `laceworkreports export vulnerabilities hosts jinja2`

Use jinja template to transform export results

**Usage**:

```console
$ laceworkreports export vulnerabilities hosts jinja2 [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--template-path TEXT`: Path to jinja2 template. Results will be passed as 'dataset' variable.  [required]
* `--help`: Show this message and exit.

##### `laceworkreports export vulnerabilities hosts json`

Export to json

**Usage**:

```console
$ laceworkreports export vulnerabilities hosts json [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--append / --no-append`: Boolean value to append or replace results  [default: False]
* `--help`: Show this message and exit.

##### `laceworkreports export vulnerabilities hosts postgres`

Export to postgres database

**Usage**:

```console
$ laceworkreports export vulnerabilities hosts postgres [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--db-connection TEXT`: Postgres connection string (e.g. postgresql://postgres:password@localhost:5432/postgres)  [required]
* `--db-table TEXT`: Postgres table to store results  [default: export]
* `--db-if-exists [append|replace|fail]`: Action to take if db table already exists  [default: replace]
* `--db-create-if-missing / --no-db-create-if-missing`: Bool to create database if missing  [default: True]
* `--help`: Show this message and exit.

### `laceworkreports export vulnerability_exceptions`

Query lacework api vulnerability_exceptions types

**Usage**:

```console
$ laceworkreports export vulnerability_exceptions [OPTIONS] COMMAND [ARGS]...
```

**Options**:

* `--help`: Show this message and exit.

laceworkreports export vulnerability_exceptions <subtype> <exporttype> [OPTIONS]

**Commands**:

* `csv`: Export to csv
* `jinja2`: Use jinja template to transform export...
* `json`: Export to json
* `postgres`: Export to postgres database

#### `laceworkreports export vulnerability_exceptions csv`

Export to csv

**Usage**:

```console
$ laceworkreports export vulnerability_exceptions csv [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--append / --no-append`: Boolean value to append or replace results  [default: False]
* `--help`: Show this message and exit.

#### `laceworkreports export vulnerability_exceptions jinja2`

Use jinja template to transform export results

**Usage**:

```console
$ laceworkreports export vulnerability_exceptions jinja2 [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--template-path TEXT`: Path to jinja2 template. Results will be passed as 'dataset' variable.  [required]
* `--help`: Show this message and exit.

#### `laceworkreports export vulnerability_exceptions json`

Export to json

**Usage**:

```console
$ laceworkreports export vulnerability_exceptions json [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--file-path TEXT`: Path to exported CSV result  [required]
* `--append / --no-append`: Boolean value to append or replace results  [default: False]
* `--help`: Show this message and exit.

#### `laceworkreports export vulnerability_exceptions postgres`

Export to postgres database

**Usage**:

```console
$ laceworkreports export vulnerability_exceptions postgres [OPTIONS]
```

**Options**:

* `--start-time [%Y-%m-%dT%H:%M:%SZ]`: Start time for query period  [default: 2022-06-06T14:48:00Z]
* `--end-time [%Y-%m-%dT%H:%M:%SZ]`: End time for query period  [default: 2022-06-07T14:48:00Z]
* `--returns TEXT`: JSON array of fields to result. For file path (use @ to specify file path)
* `--filters TEXT`: JSON array of filters to apply to query. For file path (use @ to specify file path)
* `--field-map TEXT`: JSON fieldmap to alias results columns. For file path (use @ to specify file path)
* `--flatten-json / --no-flatten-json`: Boolean value to flatten json result or not  [default: False]
* `--db-connection TEXT`: Postgres connection string (e.g. postgresql://postgres:password@localhost:5432/postgres)  [required]
* `--db-table TEXT`: Postgres table to store results  [default: export]
* `--db-if-exists [append|replace|fail]`: Action to take if db table already exists  [default: replace]
* `--db-create-if-missing / --no-db-create-if-missing`: Bool to create database if missing  [default: True]
* `--help`: Show this message and exit.
