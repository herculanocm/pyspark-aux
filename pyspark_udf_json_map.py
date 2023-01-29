import pyspark
from pyspark.sql import SparkSession 
from pyspark.sql.functions import *
from pyspark.sql import Window
from pyspark.sql.types import IntegerType, DoubleType, StringType, DataType, TimestampType, LongType, StructField, StructType, ArrayType, DateType, MapType
import pyspark.sql.functions as f
from pyspark.sql import Row
import sys

spark=SparkSession.builder.appName("stringoperations").getOrCreate()

df2 = (spark.read
                            .option("multiline", "true")
                            .json('/home/hc/dev/py/glue-aux/json_demo/')
                              )

df2 = df2.withColumn("name_files", f.input_file_name())

def get_sub_columns(row_value):
    row_dict = row_value.asDict()
    rows_caixa = []
    rows_lctos = []

    if 'caixas' in row_dict:

        for key, value in row_dict['caixas'].asDict().items():
            if value != None and 'data' in value.asDict():
                row_line = (
                    key,
                    value.asDict()['data'],
                    value.asDict()['saldoInicial'],
                    value.asDict()['saldoEfetivo'],
                    value.asDict()['saldoPrevisto'],
                    value.asDict()['saldoReservado'],
                    value.asDict()['saldoOnline'],
                    value.asDict()['saldo']
                    )
                rows_caixa.append(row_line)

    if 'lctos' in row_dict:
        for key_dt, value_dt in row_dict['lctos'].asDict().items():
            dta_lctos_dicts = row_dict['lctos'].asDict()
            for key_num, value_num in dta_lctos_dicts[key_dt].asDict().items():
                if value_num != None and 'carteira' in value_num.asDict():
                    row_line = (
                        key_dt,
                        key_num,
                        value_num.asDict()['carteira'],
                        value_num.asDict()['motivo'],
                        value_num.asDict()['complemento'],
                        value_num.asDict()['debCred'],
                        value_num.asDict()['dataLiquidacaoEfetiva'],
                        value_num.asDict()['valorLiquidado'],
                        value_num.asDict()['debito']
                    )
                    rows_lctos.append(row_line)


    return (
        row_value['header']['mnemonico'],
        row_value['header']['nome'],
        row_value['header']['administrador'],
        row_value['header']['tipoRelatorioComposicao'],
        row_value['header']['gestor'],

        row_value['caixaInicial']['data'],
        row_value['caixaInicial']['saldoInicial'],
        row_value['caixaInicial']['saldoEfetivo'],
        row_value['caixaInicial']['saldoPrevisto'],
        row_value['caixaInicial']['saldoReservado'],
        row_value['caixaInicial']['saldoOnline'],
        row_value['caixaInicial']['saldo'],

        rows_caixa,
        rows_lctos,
        
        )

rdd_map = df2.rdd.map(get_sub_columns, True)
df = rdd_map.toDF()




