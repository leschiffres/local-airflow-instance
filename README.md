# local-airflow-instance

This repo has some a minimum baseline installation process of an airflow instance locally. It also has some example DAGs, to use as a basis for new ones.

## Setup airflow environment and dependencies

Running the following script will make sure all dependencies are properly installed

```bash
python -m venv .
source bin/activate
python -m pip install --upgrade pip

export AIRFLOW_HOME=$HOME/Documents/local-airflow-instance/
export AIRFLOW_VERSION=2.7.0
PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"

CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
pip install -r requirements.txt
```

## Start airflow

First create a file like `env.sample` with your own credentials and name it `.env`. Set up the airflow connection variables with:

```bash
export $(grep -v '^#' .env | xargs)
source .airflow_env
```

Finally start airflow with:

```bash
airflow standalone
```
