from airflow import DAG
from airflow.executors.local_executor import LocalExecutor
from airflow.operators.python_operator import PythonOperator
import sqlalchemy
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DateTime, Float
import datetime
from datetime import date, timedelta

# SqlAlchemy ORM function (https://docs.sqlalchemy.org/en/13/orm/tutorial.html#declare-a-mapping)
Base = declarative_base()
session = None

#
# Table models
#---------------------------------------------
class Log(Base):
        __tablename__ = 'log'

        id = Column(Integer, primary_key=True)
        dttm = Column(DateTime, default=datetime.datetime.utcnow)
        dag_id = Column(String)
        task_id = Column(String)
        event = Column(String)
        execution_date = Column(DateTime, default=datetime.datetime.utcnow)

        @staticmethod
        def clean(session, self):
                try:
                        logAmount = session.query(Log).filter(Log.dttm < date.today()).delete()
                        session.commit()
                        print(logAmount, ' deleted records.')
                except Exception as e:
                        print(e)
                        raise ValueError("Error while running Clean method of class ", self.__name__)

class TaskInstance(Base):
        __tablename__ = 'task_instance'

        task_id = Column(String, primary_key=True)
        dag_id = Column(String, primary_key=True)
        execution_date = Column(DateTime, default=datetime.datetime.utcnow, primary_key=True)
        start_date = Column(DateTime, default=datetime.datetime.utcnow)
        end_date = Column(DateTime, default=datetime.datetime.utcnow)
        state = Column(String)
        try_number = Column(Integer)
        job_id = Column(Integer)
        operator = Column(String)
        pid = Column(Integer)

        @staticmethod
        def clean(session, self):
                try:
                        maxDateTInstances = session.query(TaskInstance.dag_id, func.max(TaskInstance.execution_date).\
                                                        label('execution_date')).\
                                                        group_by(TaskInstance.dag_id).subquery()

                        taskInstancesAmount = session.query(TaskInstance).\
                                                                        filter(TaskInstance.dag_id == maxDateTInstances.c.dag_id).\
                                                                        filter(TaskInstance.execution_date < maxDateTInstances.c.execution_date).\
                                                                        filter(TaskInstance.execution_date <= (date.today() - timedelta(days=15))).delete(synchronize_session=False)
                        session.commit()
                        print(taskInstancesAmount, ' deleted records.')
                except Exception as e:
                        print(e)
                        raise ValueError("Error while running Clean method of class ", self.__name__)

class Job(Base):
        __tablename__ = 'job'

        id = Column(Integer, primary_key=True)
        dag_id = Column(String)
        state = Column(String)
        start_date = Column(DateTime, default=datetime.datetime.utcnow)
        latest_heartbeat = Column(DateTime, default=datetime.datetime.utcnow)

        @staticmethod
        def clean(session, self):
                try:
                        jobAmount = session.query(Job).filter(Job.state == 'running').\
                                filter(Job.start_date <= (date.today() - timedelta(days=15))).\
                                        filter(Job.latest_heartbeat <= (date.today() - timedelta(days=15))).\
                                                delete(synchronize_session=False)
                        session.commit()
                        print(jobAmount, ' deleted records.')
                except Exception as e:
                        print(e)
                        raise ValueError("Error while running Clean method of class ", self.__name__)

class DagRun(Base):
        __tablename__ = 'dag_run'

        id = Column(Integer, primary_key=True)
        dag_id = Column(String)
        state = Column(String)
        start_date = Column(DateTime, default=datetime.datetime.utcnow)

        @staticmethod
        def clean(session, self):
                try:
                        dagRunAmount = session.query(DagRun).filter(DagRun.state == 'running').\
                                filter(DagRun.start_date <= (date.today() - timedelta(days=15))).\
                                                delete(synchronize_session=False)
                        session.commit()
                        print(dagRunAmount, ' deleted records.')
                except Exception as e:
                        print(e)
                        raise ValueError("Error while running Clean method of class ", self.__name__)

#---------------------------------------------

def conn(user, passwd, host, port, database):
        global session
        if session is None:
                try:
                        engine = create_engine('mysql+mysqldb://'+str(user)+':'+str(passwd)+'@'+str(host)+':'+str(port)+'/'+str(database), echo=False)
                        Session = sessionmaker(bind=engine)
                        session = Session()
                except Exception as e:
                        print(e)
                        raise ValueError("Error while trying to connect with MySQL.")

        return session

# Path to the password file
passFile = open("/user/home/mysql_user.passfile", "r")
passwd = passFile.readline().rstrip("\n")
passFile.close()

# Airflow metadata database connection. I recommend using a user with limited privileges
# instead of the user defined in the airflow.cfg file.
session = conn('mysql_user',passwd,'localhost','3306','airflow')

# DAG default configs
DAG_airflow_db_cleaner = DAG(
        'airflow_db_cleaner',
        default_args={'owner':'airflow','depends_on_past':False,'retries':0},
        # You can adjust here the execution interval
        schedule_interval=timedelta(days=15),
        # Define here a start_date
        start_date=datetime.datetime(2020,5,13),
        catchup=False,
        max_active_runs=1
)

# List of classes to create tasks
dbObjects = [Log, TaskInstance, Job, DagRun]
lastTask = None

for dbObject in dbObjects:
        taskCleaner = PythonOperator(
                task_id='taskCleaner_'+str(dbObject.__tablename__),
                python_callable=dbObject.clean,
                op_kwargs={'session':session, 'self':dbObject},
                dag=DAG_airflow_db_cleaner
        )

        if lastTask:
                lastTask.set_downstream(taskCleaner)
        lastTask = taskCleaner

session.close()
