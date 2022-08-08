# Innowise_task_6

# airflow нужна отдельная директория
# например, для установки в домашней директории добавьте:
$ export AIRFLOW_HOME=~/airflow

# правильно было бы также создать среду в этой директории:
#   python3 -m venv myvenv
#   source bin/activate

# Новая версия - 2.1.3, однако в conda-forge лежит 2.1.2
$ AIRFLOW_VERSION=2.1.3
$ PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
$ CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
$ pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"