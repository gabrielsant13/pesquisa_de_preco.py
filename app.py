import sys
import os
import mysql.connector
import cx_Oracle

# Limpa a tela do console
os.system('cls')
print("DEVELOPED BY THE PROJECTS TEAM")


#atenção nesses caminhos
cx_Oracle.init_oracle_client(lib_dir=r"C:\app\client\product\12.2.0\client_1\bin") #caminho de diretorio local
#cx_Oracle.init_oracle_client(lib_dir=r"C:\app\product\11.2.0\client_1\BIN") #caminho de diretorio para servidor 13 - seria necessário caso o user não fosse admin

#configuracoes de conexao com o oracle
connection = cx_Oracle.connect("user", "password", "host:port/service_name")


# Configurações de conexão com o MySQL
mysql_conn = mysql.connector.connect(
    host= 'database.tradepro.com.br',
    user= 'cardealdistribuidorselect',
    password= '&Ia2@&bplNpF',
    database= 'cardealdistribuidora'
)
try:
    #abre conexao bd oracle
    cursor = connection.cursor()

    #abre conexao bd mysql
    mysql_cursor = mysql_conn.cursor()


except cx_Oracle.DatabaseError as e:
        print(f"Erro ao conectar ao banco de dados: {e}")

try:
    # Consulta para obter o valor máximo da coluna id
    cursor.execute("SELECT MAX(id) FROM CLT135075GOODDATE.INTR_TP_DCL")
    max_id = cursor.fetchone()[0]

    # Verifica se o valor retornado é nulo e atribui zero caso seja
    if max_id is None:   
        max_id = 0

    # Consulta para obter a quantidade de inserts que dará
    count = (f"""select count(*) from cardealdistribuidora.pesquisa_preco_consolidado
            WHERE id > {max_id}""")
    mysql_cursor.execute(count)
    contador_id = mysql_cursor.fetchone()[0]

    # Verifica se o valor retornado é nulo e atribui zero caso seja
    if contador_id is None:   
        contador_id = 0

except:
    print("Erro ao realizar consultas para os IDs")

print(f'max_id: {max_id}')
print(f'total contador: {contador_id}')

# Realiza select na tabela do fornecedor para pegar todas as informações que faltam e grava na variavel results
try:
    # Consulta no MySQL
    mysql_query = f"""
    select pesquisa_preco_consolidado.id,
    CONCAT('to_date(''', DATE_FORMAT(coalesce(pesquisa_preco_consolidado.data_realizado,pesquisa_preco.data), '%d/%m/%Y'), ''',''dd/mm/yyyy'')') AS data_realizado,
    IFNULL(pesquisa_preco_consolidado.id_pesquisa,"NULL") AS id_pesquisa,
    IFNULL(modelo_pesquisa_preco.id,"NULL") AS ID_MODELO_PESQUISA,
    IFNULL(CONCAT("'",modelo_pesquisa_preco.descricao,"'"),"NULL") AS DESCRICAO_MODELO_PESQUISA,
    IFNULL(pesquisa_preco_consolidado.id_produto,"NULL") AS ID_PRODUTO,
    IFNULL(pesquisa_preco_consolidado.cod_produto,"NULL") AS COD_PRODUTO,
    IFNULL(CONCAT("'",pesquisa_preco_consolidado.descricao_produto,"'"),"NULL") AS DESCRICAO_PRODUTO,
    IFNULL(pesquisa_preco_consolidado.id_familia,"NULL") AS id_familia,
    IFNULL(CONCAT("'",pesquisa_preco_consolidado.familia,"'"),"NULL") as familia,
    IFNULL(CONCAT("'",pesquisa_preco_consolidado.grupo,"'"),"NULL") as grupo,
    IFNULL(CONCAT("'",pesquisa_preco_consolidado.fornecedor,"'"),"NULL") as fornecedor,
    IFNULL(REPLACE(pesquisa_preco_consolidado.preco_coletado,",","."),"NULL") AS PRECO_COLETADO,
    IFNULL(REPLACE(pesquisa_preco_consolidado.tipo_preco,",","."),"NULL") as TIPO_PRECO,
    IFNULL(REPLACE(pesquisa_preco_consolidado.preco_sugerido,",","."),"NULL") as PRECO_SUGERIDO,
    IFNULL(preco_pesquisa_preco.id_marca_concorrente,"NULL") id_marca_concorrente,
    IFNULL(CONCAT("'",marca_concorrente.descricao,"'"),"NULL") as descricao_concorrente,
    IFNULL(CONCAT("'",fabricante_concorrente.descricao,"'"),"NULL") as fabricante_concorrente,
    IFNULL(REPLACE(preco_pesquisa_preco.preco_concorrente,",","."),"NULL") as preco_concorrente,
    IFNULL(REPLACE(preco_pesquisa_preco.tipo_preco ,",","."),"NULL") as tipo_preco_concorrente,
    IFNULL(REPLACE(pesquisa_preco_consolidado.diferenca_preco,",","."),"NULL") as diferenca_preco,
    IFNULL(REPLACE(pesquisa_preco_consolidado.percentual_gap,",","."),"NULL") as percentual_gap,
    IFNULL(CONCAT("'",pesquisa_preco_consolidado.cnpj_loja,"'"),"NULL") as codcli,
    IFNULL(CONCAT("'",pesquisa_preco_consolidado.codigo_loja,"'"),"NULL") as cnpj,
    IFNULL(CONCAT("'",pesquisa_preco_consolidado.razao_social,"'"),"NULL") as razao_social,
    IFNULL(CONCAT("'",pesquisa_preco_consolidado.fantasia,"'"),"NULL") as fantasia,
    IFNULL(CONCAT("'",pesquisa_preco_consolidado.rede,"'"),"NULL") as rede,
    IFNULL(CONCAT("'",pesquisa_preco_consolidado.ramo_atividade,"'"),"NULL") as ramo_atividade,
    IFNULL(CONCAT("'",UPPER(pesquisa_preco_consolidado.cidade),"'"),"NULL") as cidade,
    IFNULL(CONCAT("'",pesquisa_preco_consolidado.regional,"'"),"NULL") as regional,
    IFNULL(CONCAT("'",pesquisa_preco_consolidado.supervisor,"'"),"NULL") as supervisor,
    IFNULL(pesquisa_preco_consolidado.cod_colaborador,"NULL") as cod_colaborador,
    IFNULL(CONCAT("'",pesquisa_preco_consolidado.nome_colaborador,"'"),"NULL") as nome_colaborador,
    IFNULL(CONCAT("'",pesquisa_preco_consolidado.representante,"'"),"NULL") as representante
    from pesquisa_preco 
    left join pesquisa_preco_has_atividade on pesquisa_preco_has_atividade.id_pesquisa_preco = pesquisa_preco.id
    left join atividade_has_modelo_pesquisa_preco on pesquisa_preco_has_atividade.id_atividade = atividade_has_modelo_pesquisa_preco.id_atividade 
    left join modelo_pesquisa_preco on modelo_pesquisa_preco.id = atividade_has_modelo_pesquisa_preco.id_modelo_pesquisa_preco
    right join pesquisa_preco_consolidado on pesquisa_preco_consolidado.id_pesquisa = pesquisa_preco.id
    left join cardealdistribuidora.preco_pesquisa_preco ON preco_pesquisa_preco.id_pesquisa_preco = pesquisa_preco_consolidado.id_pesquisa
    left join cardealdistribuidora.marca_concorrente on marca_concorrente.id = preco_pesquisa_preco.id_marca_concorrente
    left join cardealdistribuidora.fabricante_concorrente on fabricante_concorrente.id = marca_concorrente.id_fabricante_concorrente
    WHERE pesquisa_preco_consolidado.id > {max_id}
    order by 1 asc
    """
    mysql_cursor.execute(mysql_query)
    results = mysql_cursor.fetchall()


except:
    print("Erro ao realizar consulta no BD do tradepro")

# Percorre a variavel results enquanto houver dados, e vai dando insert
try:
    for column in results:
        
        id = column[0]
        data_realizado = column[1]
        id_pesquisa = column[2]
        id_modelo_pesquisa = column[3]
        descricao_modelo_pesquisa = column[4]
        id_produto = column[5]
        cod_produto = column[6]
        descricao_produto = column[7]
        id_familia = column[8]
        familia = column[9]
        grupo = column[10]
        fornecedor = column[11]
        preco_coletado = column[12]
        tipo_preco = column[13]
        preco_sugerido = column[14]
        id_marca_concorrente = column[15]
        descricao_concorrente = column[16]
        fabricante_concorrente = column[17]
        preco_concorrente = column[18]
        tipo_preco_concorrente = column[19]
        diferenca_preco = column[20]
        percentual_gap = column[21]
        codcli = column[22]
        cnpj = column[23]
        razao_social = column[24]
        fantasia = column[25]
        rede = column[26]
        ramo_atividade = column[27]
        cidade = column[28]
        regional = column[29]
        supervisor = column[30]
        cod_colaborador = column[31]
        nome_colaborador = column[32]
        representante = column[33]

        insert_query = f"""insert into CLT135075GOODDATE.INTR_TP_DCL 
        (ID,
        DATA_REALIZADO,
        ID_PESQUISA,
        ID_MODELO_PESQUISA,
        DESCRICAO_MODELO_PESQUISA,
        ID_PRODUTO,
        COD_PRODUTO,
        DESCRICAO_PRODUTO,
        ID_FAMILIA,
        FAMILIA,
        GRUPO,
        FORNECEDOR,
        PRECO_COLETADO,
        TIPO_PRECO,
        PRECO_SUGERIDO,
        ID_MARCA_CONCORRENTE,
        DESCRICAO_CONCORRENTE,
        FABRICANTE_CONCORRENTE,
        PRECO_CONCORRENTE,
        TIPO_PRECO_CONCORRENTE,
        DIFERENCA_PRECO,
        PERCENTUAL_GAP,
        CODCLI,
        CNPJ,
        RAZAO_SOCIAL,
        FANTASIA,
        REDE,
        RAMO_ATIVIDADE,
        CIDADE,
        REGIONAL,
        SUPERVISOR,
        COD_COLABORADOR,
        NOME_COLABORADOR,
        REPRESENTANTE
        )
        values(
        {id},
        {data_realizado},
        {id_pesquisa},
        {id_modelo_pesquisa},
        {descricao_modelo_pesquisa},
        {id_produto},
        {cod_produto},
        {descricao_produto},
        {id_familia},
        {familia},
        {grupo},
        {fornecedor},
        {preco_coletado},
        {tipo_preco},
        {preco_sugerido},
        {id_marca_concorrente},
        {descricao_concorrente},
        {fabricante_concorrente},
        {preco_concorrente},
        {tipo_preco_concorrente},
        {diferenca_preco},
        {percentual_gap},
        {codcli},
        {cnpj},
        {razao_social},
        {fantasia},
        {rede},
        {ramo_atividade},
        {regional},
        {cidade},
        {supervisor},
        {cod_colaborador},
        {nome_colaborador},
        {representante}
        )"""

        cursor.execute(insert_query)
        connection.commit()

        print(id)
        contador_id -= 1

except:
    print(f"Erro no Insert ao pegar o proximo id > {id}")
    print(f"faltam {contador_id} registros")
    
    connection.close()
    mysql_cursor.close()

    sys.exit('Encerrando Programa')
            

connection.close()
mysql_cursor.close()                                
