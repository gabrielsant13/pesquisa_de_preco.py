import mysql.connector
import cx_Oracle


cx_Oracle.init_oracle_client(lib_dir=r"C:\app\client\product\12.2.0\client_1\bin")

#configuracoes de conexao com o oracle
connection = cx_Oracle.connect("user", "senha", "ip:porta/service_name")


# Configurações de conexão com o MySQL
mysql_conn = mysql.connector.connect(
    host= '',
    user= '',
    password= '',
    database= ''
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
    cursor.execute("SELECT MAX(id) FROM CLT135075CONSULTA.INTR_TP_DCL")
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

print(contador_id)
print(max_id)

while contador_id != 0:

    print(contador_id)
    try:
        # Consulta no MySQL
        mysql_query = f"""
        SELECT
        pesquisa_preco_consolidado.id,
        IFNULL(pesquisa_preco_consolidado.id_pesquisa,"NULL") AS id_pesquisa ,
        IFNULL(pesquisa_preco_consolidado.id_pesquisa_concorrente,"NULL") as id_pesquisa_concorrente,
        CONCAT('to_date(''', DATE_FORMAT(pesquisa_preco_consolidado.data_realizado, '%d/%m/%Y'), ''',''dd/mm/yyyy'')') AS data_formatada,
        pesquisa_preco_consolidado.cod_produto AS cod_produto,
        CONCAT("'",pesquisa_preco_consolidado.descricao_produto,"'") AS descricao_produto,
        IFNULL(REPLACE(pesquisa_preco_consolidado.preco_coletado,",","."),"NULL") AS preco_coletado,
        IFNULL(REPLACE(pesquisa_preco_consolidado.preco_sugerido,",","."),"NULL") as preco_sugerido,
        IFNULL(REPLACE(pesquisa_preco_consolidado.diferenca_preco,",","."),"NULL") as diferenca_preco,
        IFNULL(REPLACE(pesquisa_preco_consolidado.percentual_gap,",","."),"NULL") as percentual_gap,
        IFNULL(REPLACE(pesquisa_preco_consolidado.tipo_preco,",","."),'') as tipo_preco,
        IFNULL(pesquisa_preco_consolidado.id_produto,"NULL") AS id_produto,
        IFNULL(pesquisa_preco_consolidado.id_familia,"NULL") AS id_familia,
        IFNULL(CONCAT("'",pesquisa_preco_consolidado.familia,"'"),"NULL") as familia,
        IFNULL(pesquisa_preco_consolidado.id_marca_concorrente,"NULL") as id_marca_concorrente_consolidado,
        IFNULL(CONCAT("'",pesquisa_preco_consolidado.grupo,"'"),"NULL") as grupo,
        IFNULL(CONCAT("'",pesquisa_preco_consolidado.fornecedor,"'"),"NULL") as fornecedor,
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
        IFNULL(CONCAT("'",pesquisa_preco_consolidado.representante,"'"),"NULL") as representante,
        IFNULL(pesquisa_preco_consolidado.categoria,"NULL") as categoria,
        IFNULL(preco_pesquisa_preco.id_marca_concorrente,"NULL") id_marca_concorrente,
		    IFNULL(CONCAT("'",marca_concorrente.descricao,"'"),"NULL") as descricao_concorrente,
		    IFNULL(CONCAT("'",fabricante_concorrente.descricao,"'"),"NULL") as fabricante_concorrente,
		    IFNULL(REPLACE(preco_pesquisa_preco.preco_concorrente,",","."),"NULL") as preco_concorrente
        from cardealdistribuidora.pesquisa_preco_consolidado
        left join cardealdistribuidora.preco_pesquisa_preco ON preco_pesquisa_preco.id_pesquisa_preco = pesquisa_preco_consolidado.id_pesquisa
		    left join cardealdistribuidora.marca_concorrente on marca_concorrente.id = preco_pesquisa_preco.id_marca_concorrente
		    left join cardealdistribuidora.fabricante_concorrente on fabricante_concorrente.id = marca_concorrente.id_fabricante_concorrente
        WHERE pesquisa_preco_consolidado.id > {max_id}
        order by 1 desc
        """
        mysql_cursor.execute(mysql_query)
        results = mysql_cursor.fetchall()


    except:
        print("Erro ao realizar consulta no BD do tradepro")

    try:
        for column in results:
            
            id = column[0]
            id_pesquisa = str(column[1])
            id_pesquisa_concorrente = column[2]
            data_realizado = column[3]
            cod_produto = column[4]
            descricao_produto = column[5]
            preco_coletado = column[6]
            preco_sugerido = column[7]
            diferenca_preco = column[8]
            percentual_gap = column[9]
            tipo_preco = column[10]
            id_produto = column[11]
            id_familia = column[12]
            familia = column[13]
            id_marca_concorrente_consolidado = column[14]
            grupo = column[15]
            fornecedor = column[16]
            codcli = column[17]
            cnpj = column[18]
            razao_social = column[19]
            fantasia = column[20]
            rede = column[21]
            ramo_atividade = column[22]
            cidade = column[23]
            regional = column[24]
            supervisor = column[25]
            cod_colaborador = column[26]
            nome_colaborador = column[27]
            representante = column[28]
            categoria = column[29]
            id_marca_concorrente = column[30]
            descricao_concorrente = column[31]
            fabricante_concorrente = column[32]
            preco_concorrente = column[33]
        

        insert_query = f"""insert into CLT135075CONSULTA.INTR_TP_DCL 
        (id,
        id_pesquisa,
        id_pesquisa_concorrente,
        data_realizado,
        cod_produto,
        descricao_produto,preco_coletado,
        preco_sugerido,diferenca_preco,
        percentual_gap,
        tipo_preco,
        id_produto,
        id_familia,
        familia,
        id_marca_concorrente_consolidado,
        grupo,fornecedor,codcli,cnpj,razao_social,
        fantasia,
        rede,
        ramo_atividade,
        cidade,
        regional,
        supervisor,
        cod_colaborador,
        nome_colaborador,
        id_marca_concorrente,
        descricao_concorrente,
        fabricante_concorrente,
        preco_concorrente
        )
        values(
        {id},
        {id_pesquisa},
        {id_pesquisa_concorrente},
        {data_realizado},
        {cod_produto},
        {descricao_produto},
        {preco_coletado},
        {preco_sugerido},
        {diferenca_preco},
        {percentual_gap},
        {tipo_preco},
        {id_produto},
        {id_familia},
        {familia},
        {id_marca_concorrente_consolidado},
        {grupo},
        {fornecedor},
        {codcli},
        {cnpj},
        {razao_social},
        {fantasia},
        {rede},
        {ramo_atividade},
        {cidade},
        {regional},
        {supervisor},
        {cod_colaborador},
        {nome_colaborador},
        {id_marca_concorrente},
        {descricao_concorrente},
        {fabricante_concorrente},
        {preco_concorrente}
        )"""


        cursor.execute(insert_query)
        connection.commit()

        max_id = column[0]
        contador_id -= 1

    except:
        print(f"Erro no Insert. Pegando o proximo id > {max_id}")
        print(f"contador {contador_id}")
        
        connection.close()
        mysql_cursor.close()
            

print(id)

connection.close()
mysql_cursor.close()                                
