# Nth-Flow is a library designed to orchestrate pythonic processes in a simple an effective way

# Stage 1

The basic foundations of nth-flow is a config .yml file that defines a set of
processes, such as:

- Read data from API
- Write Data to File
- Load file to database

These two stages are dependent on each other, and the config file will define
the dependencies between the stages. The solution uses crons, and multiple processes
can run in parallel, and can be orchestrated to be dependent on each other.

An example of the config file is as follows:

```yaml
my_example_flow:
  cron: "0 0 * * *"
  default_args:
    # Similar to Airflow DAGS
    on_failure_callback: send_team_email.py
    on_success_callback: None
    retries: 3
    retry_delay: 5
    start_date: "2021-01-01"
  tasks:
    - name: read_data_from_api
      command: python read_data_from_api.py
      args:
        url: "https://api.com/data"
    - name: write_data_to_file
      command: python write_data_to_file.py
      args:
        file_path: "data.csv"
      dependencies:
        - read_data_from_api
    - name: load_file_to_database
      command: python load_file_to_database.py
      args:
        file_path: "data.csv"
        db_host: "localhost"
        db_name: "my_db"
        db_table: "my_table"
        db_user: "user"
        db_password: "password"
        db_port: 5432
      dependencies:
        - write_data_to_file
```