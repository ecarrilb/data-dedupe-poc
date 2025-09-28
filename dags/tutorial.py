import textwrap
from datetime import datetime, timedelta

# Operadores Airflow; nosotros necesitamos operar!
from airflow.providers.standard.operators.bash import BashOperator

# El DAG es objeto; Bien necesitamos intanciar un DAG
from airflow.sdk import DAG

# Estos argumentos puedes pasar al operador
with DAG(
    "tutorial",
    default_args={
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="Un DAG de ejemplo simple",
    schedule=timedelta(days=1),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:
    # Definimos las tareas (tasks) del DAG
    t1 = BashOperator(
        task_id="print_date",
        bash_command="date",
    )

    t2 = BashOperator(
        task_id="sleep",
        depends_on_past=False,
        bash_command="sleep 5",
        retries=3,
    )

    t1.doc_md = textwrap.dedent(
        """\
        #### Imprime la fecha
        Esta tarea imprime la fecha actual en el log.
        """
    )
    dag.doc_md = __doc__  # El docstring del DAG
    t2.doc_md = """\
    #### Tarea de Sleep
    Esta tarea simplemente duerme por 5 segundos.
    Puedes usar **Markdown** aquí también.
    """
    template_command = """
    {% for i in range(5) %}
        echo "{{ ds }}"
        echo "{{ macros.ds_add(ds, 7) }}"
        echo "{{ params.my_param }}"
    {% endfor %}
    """
    t3 = BashOperator(
        task_id="print_template",
        bash_command=template_command,
        params={"my_param": "Hello World"},
    )
    t1 >> [t2, t3]  # t1 se ejecuta antes que t2 y t3