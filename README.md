# airflow

이 repository는 하위 패키징을 포함해 airflow상 task들을 수행합니다.

## 설치

본 repository 설치를 위해서는 Python 3.11 이상의 버전을 요구합니다.
```
pip install git+https://github.com/DE32megabox/airflow.git
```

개발자의 경우 다음 코드를 실행하여 버전을 변경하여 주십시오
```
git checkout dev/d1.0.0
```

## 환경설정
본 repository를 실행하기 위해선 airflow 및 가상환경이 준비되어야 합니다.
```zsh
pyenv virtualenv 3.11 air
pyenv shell air
pip install apache-airflow
```

이후, 본 repository를 실행하기 위해 airflow상의 다음 설정을 진행하여 주십시오.

```zsh
vi ~/.zshrc

[AIRFLOW]
export AIRFLOW_HOME=~/airflow_team
export AIRFLOW__CORE__DAGS_FOLDER=<YOUR_INSTALL_DIR>/airflow/dags
export AIRFLOW__CORE__LOAD_EXAMPLES=False
```

## 실행
airflow상에서 본 repository를 실행하면 2021년의 영화 데이터를 추출, 가공 및 출력하는 모델로 구성되어있습니다.

```
airflow standalone
```
를 통해 실행하여 안내되는 webserver에 접속하면 dags에 자동으로 task가 처리되어 진행됩니다.

## 구성
### extract module
![image](https://github.com/user-attachments/assets/29d57b0c-4d2b-4750-8c3c-c5ee1f583c5e)
2021년의 영화 데이터를 1 ~ 4월, 5 ~ 8월, 9 ~ 12월로 분할하여 추출하기 위하여, 각각 extract_q1, extract_q2, extract_q3 3개의 DAG를 구성하였습니다. 각 DAG는 branch operator를 통해 디렉토리의 존재 유무를 기준으로 분기되며, 해당 디렉토리가 존재하지 않는다면 요청된 데이터를 받>아 와 parquet 형식으로 저장하게 됩니다.

저장 경로
```
cd megabox/tmp/movie_parquet
```
