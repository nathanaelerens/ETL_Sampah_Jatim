from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

my_default_args = {
    'owner': 'nathan', 
    'retries': 5, 
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='pipeline_data_sampah_jatim_postgres_v5',
    default_args=my_default_args,
    description='membuat dag sederhana',
    start_date=datetime(2025, 11, 26), 
    schedule='@daily'
) as dag:
    create_table = SQLExecuteQueryOperator(
        task_id='create_table',
        conn_id='Sampah_jatim',
        sql="""
        CREATE TABLE IF NOT EXISTS timbulan_sampah (
            
            Tahun INT,
            Provinsi VARCHAR(50),
            kabupaten_kota VARCHAR(50) PRIMARY KEY,
            Timbulan_sampah_harian FLOAT,
            Timbulan_sampah_tahunan FLOAT
            );
        
        CREATE TABLE IF NOT EXISTS komposisi_sampah (
            
            Tahun INT,
            Provinsi VARCHAR(50),
            kabupaten_kota VARCHAR(50) PRIMARY KEY,
            Sisa_Makanan FLOAT,
            Kayu_Ranting FLOAT,
            Kertas_Karton FLOAT,
            Plastik FLOAT,
            Logam FLOAT,
            Kain FLOAT,
            Karet_Kulit FLOAT,
            Kaca FLOAT,
            Lainnya FLOAT
            );
        """
    )

    fill_table = SQLExecuteQueryOperator(
        task_id='fill_table',
        conn_id='Sampah_jatim',
        sql="""
        TRUNCATE TABLE timbulan_sampah;
        TRUNCATE TABLE komposisi_sampah;
        COPY timbulan_sampah FROM '/csv/timbulan_sampah_jatim.csv' DELIMITER ',' CSV HEADER;
        
        COPY komposisi_sampah FROM '/csv/komposisi_sampah_jatim.csv' DELIMITER ',' CSV HEADER;
        """
    )
    clean_timbulan = SQLExecuteQueryOperator(
        task_id='clean_timbulan_table',
        conn_id='Sampah_jatim',
        sql="""
        UPDATE timbulan_sampah
        SET 
            Tahun = COALESCE(Tahun, 0),
            Timbulan_sampah_harian = COALESCE(Timbulan_sampah_harian, 0),
            Timbulan_sampah_tahunan = COALESCE(Timbulan_sampah_tahunan, 0)
        WHERE 
            Tahun IS NULL
            OR Timbulan_sampah_harian IS NULL
            OR Timbulan_sampah_tahunan IS NULL;
        """
    )

    clean_komposisi = SQLExecuteQueryOperator(
        task_id='clean_komposisi_table',
        conn_id='Sampah_jatim',
        sql="""
        UPDATE komposisi_sampah
        SET 
            Sisa_Makanan   = COALESCE(Sisa_Makanan, 0),
            Kayu_Ranting   = COALESCE(Kayu_Ranting, 0),
            Kertas_Karton  = COALESCE(Kertas_Karton, 0),
            Plastik        = COALESCE(Plastik, 0),
            Logam          = COALESCE(Logam, 0),
            Kain           = COALESCE(Kain, 0),
            Karet_Kulit    = COALESCE(Karet_Kulit, 0),
            Kaca           = COALESCE(Kaca, 0),
            Lainnya        = COALESCE(Lainnya, 0)
        WHERE 
            Sisa_Makanan IS NULL
            OR Kayu_Ranting IS NULL
            OR Kertas_Karton IS NULL
            OR Plastik IS NULL
            OR Logam IS NULL
            OR Kain IS NULL
            OR Karet_Kulit IS NULL
            OR Kaca IS NULL
            OR Lainnya IS NULL;
        """
    )
    join_tables = SQLExecuteQueryOperator(
    task_id='join_tables',
    conn_id='Sampah_jatim',
    sql="""
    DROP TABLE IF EXISTS sampah_joined;

    CREATE TABLE sampah_joined AS
    SELECT
        t.Tahun AS tahun_timbulan,
        t.Provinsi AS provinsi_timbulan,
        t.kabupaten_kota,
        t.Timbulan_sampah_harian,
        t.Timbulan_sampah_tahunan,
        k.Tahun AS tahun_komposisi,
        k.Provinsi AS provinsi_komposisi,
        k.Sisa_Makanan,
        k.Kayu_Ranting,
        k.Kertas_Karton,
        k.Plastik,
        k.Logam,
        k.Kain,
        k.Karet_Kulit,
        k.Kaca,
        k.Lainnya
    FROM timbulan_sampah t
    LEFT JOIN komposisi_sampah k
        ON t.kabupaten_kota = k.kabupaten_kota;
    """
)




    create_table >> fill_table >> clean_timbulan >> clean_komposisi >> join_tables
