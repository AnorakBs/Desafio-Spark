from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce
from pyspark.sql.functions import col
import pandas as pd
spark = SparkSession.builder.appName('Desafio spark').getOrCreate()

path_saldo = 'Bases/tabela_saldo_inicial.txt'
path_movimentacao_02 = 'Bases/movimentacao_dia_02_04_2022.txt'
path_movimentacao_03 = 'Bases/movimentacao_dia_03_04_2022.txt'

df_saldo = spark.read.options(delimiter=';',header=True).csv(path_saldo)
df_movimentacao_02 = spark.read.options(delimiter=';',header=True).csv(path_movimentacao_02)
df_movimentacao_03 = spark.read.options(delimiter=';',header=True).csv(path_movimentacao_03)

df_saldo = df_saldo.orderBy(df_saldo.Nome.asc())
print("Tabela saldo inicial dia 01")
df_saldo.show()
tabela_final = df_saldo

def check_reembolso(df_saldo,df_movimentacao):
    reembolso_df = df_saldo.join(
                    df_movimentacao,
                    (df_saldo.Nome == df_movimentacao.Nome) & (df_saldo.data == df_movimentacao.data),
                    'leftsemi')
    if not reembolso_df.isEmpty():
        print("Reembolso detectado!")
        return True, reembolso_df
    else:
        print("Nenhum Reembolso detectado!")
        return False, None
def calcula_saldo(df_saldo,df_movimentacao):
    global tabela_final
    reembolso_flg, reembolso_df = check_reembolso(df_saldo,df_movimentacao)
    if reembolso_flg:
        reembolso_df.createOrReplaceTempView("reembolso")
        df_movimentacao.createOrReplaceTempView("movimentacao_reembolso")
        tabela_reembolso = spark.sql("""
                        SELECT
                            re.Nome,
                            re.CPF,
                            cast(re.Saldo_Inicial_CC + IFNULL(SUM(mvr.Movimentacao_dia),0) as DECIMAL(10,2)) as Saldo_Inicial_CC,
                            mvr.data
                        FROM
                            reembolso re
                        inner join movimentacao_reembolso mvr ON re.CPF = mvr.CPF and re.data = mvr.data
                        GROUP BY re.Nome, re.CPF, re.Saldo_Inicial_CC, mvr.data
                    """)
        print("Tabela com valores que devem ser corrigidos do dia anterior")
        tabela_reembolso.show()

        df_saldo = df_saldo.alias("s").join(tabela_reembolso.alias("u"), ["Nome", "CPF", "data"], "left") \
            .select("s.Nome", "s.CPF", coalesce(col("u.Saldo_Inicial_CC"),"s.Saldo_Inicial_CC").alias("Saldo_Inicial_CC"), "s.data")
        print("Tabela com valores CORRETOS do dia 02")
        df_saldo.show()

        df_saldo.createOrReplaceTempView("saldo_apos_reembolso")
        df_movimentacao = df_movimentacao.join(tabela_reembolso, ["CPF", "data"], "leftanti")
        df_movimentacao.createOrReplaceTempView("movimentacao")
        tabela_saldo_final = spark.sql("""
        with pessoas_novas as(
        select
        mv.Nome,
        mv.CPF,
        CAST(IFNULL(SUM(mv.Movimentacao_dia),0) as DECIMAL(10,2)) as Saldo_Inicial_CC,
        mv.data
        from saldo_apos_reembolso sar
        right join movimentacao mv on sar.CPF = mv.CPF 
        where sar.CPF is null
        GROUP BY mv.Nome,mv.CPF,mv.data
        ),
        saldo_apos_reembolso as (
        SELECT
        sar.Nome,
        sar.CPF,
        CAST(sar.Saldo_Inicial_CC + IFNULL(SUM(mv.Movimentacao_dia),0) as DECIMAL(10,2)) as Saldo_Inicial_CC,
        (select distinct data from movimentacao) as data
        FROM
        saldo_apos_reembolso sar
        LEFT JOIN movimentacao mv ON sar.CPF = mv.CPF
        GROUP BY sar.Nome, sar.CPF, sar.Saldo_Inicial_CC, mv.data
        )
        SELECT * FROM saldo_apos_reembolso
        UNION ALL
        SELECT * FROM pessoas_novas
        """)
        print("Tabela com saldos do dia 03")
        tabela_saldo_final.show()
        tabela_final = tabela_final.union(tabela_saldo_final)

        tabela_reembolso.createOrReplaceTempView("tabela_valor_correto")
        tabela_final.createOrReplaceTempView("tabela_final")

        tabela_final = spark.sql("""
                 select
                 tf.Nome,
                 tf.CPF,
                 COALESCE(tvc.Saldo_Inicial_CC,tf.Saldo_Inicial_CC) as Saldo_Inicial_CC,
                 tf.data
                 from tabela_final tf
                 left join tabela_valor_correto tvc on tf.CPF = tvc.CPF and tf.data = tvc.data
                 order by tf.data asc
        """)
        print("Tabela Final apos reembolso (valores do dia 02 corrigidos)")
        tabela_final.show(n=1000)
        return tabela_saldo_final,tabela_reembolso
    else:
        df_saldo.createOrReplaceTempView("saldo_inicial")
        df_movimentacao.createOrReplaceTempView("movimentacao")
        tabela_saldo_final = spark.sql("""
        with pessoas_novas as(
        select
        mv.Nome,
        mv.CPF,
        CAST(IFNULL(SUM(mv.Movimentacao_dia),0) as DECIMAL(10,2)) as Saldo_Inicial_CC,
        mv.data
        from saldo_inicial si
        right join movimentacao mv on si.CPF = mv.CPF 
        where si.CPF is null
        GROUP BY mv.Nome,mv.CPF,mv.data
        ),
        saldo_inicial as (
        SELECT
            si.Nome,
            si.CPF,
            CAST(si.Saldo_Inicial_CC + IFNULL(SUM(mv.Movimentacao_dia),0) as DECIMAL(10,2)) as Saldo_Inicial_CC,
            (select distinct data from movimentacao) as data
        FROM
        saldo_inicial si
        LEFT JOIN movimentacao mv ON si.CPF = mv.CPF
        GROUP BY si.Nome, si.CPF, si.Saldo_Inicial_CC, mv.data
        order by si.Nome asc
        )
        Select * FROM pessoas_novas
        
        UNION ALL 
        
        SELECT * FROM saldo_inicial
            """)
        print("Tabela de saldo do dia 02")
        tabela_saldo_final.show()
        tabela_final = tabela_final.union(tabela_saldo_final)
        print("Tabela final do dia 02")
        tabela_final.show()
        return tabela_saldo_final

# # RESULTADO 1
resultado_1 = calcula_saldo(df_saldo, df_movimentacao_02)
##
resultado_2,tabela_update_valores = calcula_saldo(resultado_1,df_movimentacao_03)


tabela_final = tabela_final.toPandas()

tabela_final.to_csv("Bases/df_final/tabela_final.csv")

