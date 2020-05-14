# Introduction
The `DAG_airflow_db_cleaner.py` DAG periodically cleans some records of the Airflow metadata database. The goal is to keep airflow fast and avoid performance issues.

# Setup
1. In your metadata database, create a new user and give to it `drop` and `read` permissions on the `log`, `task_instance`, `job` and `dag_run` tables in the `airflow` database.

2. In a secure directory, create a file containing your new user's password.

3. Alter the `DAG_airflow_db_cleaner.py` file and change the line of the first occurrence of the passFile variable with the password path.  
It should look like this: 
```python
# Path to the password file
passFile = open("/my/safe/directory/user.passfile", "r")
```

4. Next, change the `session` variable giving your connection informations to the `conn` function. The first parameter is the `user`; the next is a password variable called `passwd` that was defined with the result of your passfile path, so you don't need to do anything here; then you need to inform the `host`; next, the connection port; and finally the Airflow database.  
It should look like this:
```python
session = conn('user',passwd,'localhost','3306','airflow')
```

5. Finally, when defining your DAG `DAG_airflow_db_cleaner = DAG( .... )` you can adjust the `start_date` parameter.  
If everything is ok, it is done!

# How it works?
By default, every 15 days the DAG performs the following cleanings:
 - Table `log`: all the logs stored before the current date will be deleted.
 - Table `task_instance`: all records older than 15 days will be deleted, but not the last one.
 - Table `job`: records with `running` estate, `latest_heartbeat` >= 15 days and `start_date` with more than 15 days, will be deleted. These kind of records can be generated by Airflow issues, and they never changes their `estate`.
 - Table `dag_run`: all the dagruns with `running` estate and `start_date` with more than 15 days will be deleted. As on the `job` case, this is also to avoid endless dagruns.

Feel free to change these "rules" inside the `clean(...)` function of each table model, add new models, etc.
