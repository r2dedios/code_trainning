# Autor: Alejandro Villegas
# Brief: Ejemplo de flujo de ingesta para airflow


# Libs
import airflow
import os
import csv
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.example_dags.subdags.subdag import subdag


DAG_NAME="ingest_example_1"


# DAG Definition
args = {
	'owner': 'airflow', 
	'depends_on_past': False,
	'email_on_failure': False,
	'email_on_retry': False,
	'start_date': airflow.utils.dates.days_ago(2),
}
dag = DAG(dag_id=DAG_NAME, default_args=args, schedule_interval="@once")


# Aux Functions
def check_if_file_exists(target):
	if os.path.exists(target) and os.path.isfile(target):
		return "ingest_method_selector"
	else:
		return "call_admin"

def branch_ingest_method(target):
	_, ext = os.path.splitext(target)
	if ext == ".csv":
		return "csv_processor"
	elif ext == ".txt":
		return "txt_processor"
	else:
		return "call_admin"

def change_delimiter(target, output):
	reader = csv.reader(open(target, "rU"), delimiter=',')
	writer = csv.writer(open(output, 'w'), delimiter=';')
	writer.writerows(reader)


def clean_text(target, output):
	with open(target, 'r') as reader:
		with open(output, 'w') as writer:
			for line in reader:
				writer.write(line.lower())



# Tasks
input_path="/home/airflow/input_files"
output_path="/home/airflow/output_files"

#Landing Task
init = DummyOperator(task_id='landing_task', dag=dag)

# Ending Task
end = DummyOperator(task_id='ending_task', trigger_rule='all_success', dag=dag)


for (dirpath, dirnames, filenames) in os.walk(input_path):
	for ifile in filenames:
		target="%s/%s" % (dirpath, ifile)
		output="%s/%s" % (output_path, ifile)

		### File SUBDAG
		subdag_name="process_%s" % ifile
		subdag = DAG("%s.%s" % (DAG_NAME, subdag_name), default_args=args)
		subdagop = SubDagOperator(task_id=subdag_name, subdag=subdag, dag=dag)
		init >> subdagop >> end

		## File SUBDAG TASKS
		init_process = DummyOperator(task_id='init_processing', dag=subdag)
		# Check file exists
		input_files_check = BranchPythonOperator(task_id='input_files_check', python_callable=check_if_file_exists, op_kwargs={'target': target}, dag=subdag)
		# File not found dummy task for branching
		file_not_found = DummyOperator(task_id='file_not_found', dag=subdag)
		# Method selection
		ingest_method_selector = BranchPythonOperator(task_id='ingest_method_selector', python_callable=branch_ingest_method, op_kwargs={'target': target}, dag=subdag)
		# Call Admin to inform about an error
		call_admin = BashOperator(task_id='call_admin', bash_command="echo \"LA INGESTA NO PUEDE REALIZARSE\"", dag=subdag)
		# Ending SUBDAG
		end_process = DummyOperator(task_id='end_processing', trigger_rule='one_success', dag=subdag)
		



		### SUBSUBDAG CSV
		csv_subdag_name="csv_processor"
		csv_subdag = DAG("%s.%s.%s" % (DAG_NAME, subdag_name, csv_subdag_name), default_args=args)
		csv_subdagop = SubDagOperator(task_id=csv_subdag_name, subdag=csv_subdag, dag=subdag)
		# Change delimiter CSV
		csv_delimiter = PythonOperator(task_id="csv_change_delimiter", python_callable=change_delimiter, op_kwargs={'target': target, 'output': output}, dag=csv_subdag)
		# Clean Text
		csv_cleaner = PythonOperator(task_id="csv_clean_text", python_callable=clean_text, op_kwargs={'target': output, 'output': output}, dag=csv_subdag)	
		# SUBSUBDAG Dependencies
		csv_delimiter >> csv_cleaner
		



		### SUBSUBDAG TXT
		txt_subdag_name="txt_processor"
		txt_subdag = DAG("%s.%s.%s" % (DAG_NAME, subdag_name, txt_subdag_name), default_args=args)
		txt_subdagop = SubDagOperator(task_id=txt_subdag_name, subdag=txt_subdag, dag=subdag)
		# Clean Text
		txt_delimiter = PythonOperator(task_id="txt_clean_text", python_callable=clean_text, op_kwargs={'target': target, 'output': output}, dag=txt_subdag)
		
		


		# Dependencies
		init_process >> input_files_check >> ingest_method_selector >> [csv_subdagop, txt_subdagop] >> end_process
		init_process >> input_files_check >> file_not_found >> end_process
		ingest_method_selector >> call_admin
			




