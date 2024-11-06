import psycopg2
import os
from datetime import datetime
import pymongo


from dotenv import load_dotenv

load_dotenv(encoding="utf-8")


conn = pymongo.MongoClient("mongodb+srv://Inaldo:acess2012@khiata.64kjc.mongodb.net/")


connection_params_segundo = {
    "dbname": "khiata",  # Nome do banco de dados
    "user": f"{os.getenv('BD_USER')}",  # Nome de usuário
    "password": f"{os.getenv('BD_PASSWORD_SEGUNDO')}",  # Senha
    "host": f"{os.getenv('BD_HOST_SEGUNDO')}",  # Host, pode ser um IP ou nome de domínio
    "port": "16334",  # Porta padrão do PostgreSQL
}


connSegundo = psycopg2.connect(**connection_params_segundo)
print("Conexão estabelecida com sucesso!")


connection_params_primeiro = {
    "dbname": "db_Khiata",  # Nome do banco de dados
    "user": f"{os.getenv('BD_USER')}",  # Nome de usuário
    "password": f"{os.getenv('BD_PASSWORD_PRIMEIRO')}",  # Senha
    "host": f"{os.getenv('BD_HOST_PRIMEIRO')}",  # Host, pode ser um IP ou nome de domínio
    "port": "10441",  # Porta padrão do PostgreSQL
}

connPrimeiro = psycopg2.connect(**connection_params_primeiro)
print("Conexão estabelecida com sucesso!")


def consulta(sql, banco: psycopg2, params=None):
    try:
        retorno = []

        C = banco.cursor()
        C.execute(sql, params)
        query = C.fetchall()
        colunas = [i[0] for i in C.description]

        for values in query:
            dic = {}
            for c in range(0, len(query[0])):
                dic[colunas[c]] = values[c]
            retorno.append(dic)

        return retorno
    except Exception as e:
        return ("Erro ao executar a consulta:", e)


# # Genero
#


try:
    gender_primeiro = consulta(
        "select DISTINCT genero from USUARIO U", banco=connPrimeiro
    )

except Exception as e:
    print("Erro ao executar a consulta:", e)


gender_atual = consulta("select g.gender from gender g", banco=connSegundo)
gender_atual = [gender["gender"] for gender in gender_atual]


try:
    connPrimeiro.rollback()
    connSegundo.rollback()
    cursorPimeiro = connPrimeiro.cursor()
    cursorSegundo = connSegundo.cursor()
    c = 0
    for gender in gender_primeiro:
        if gender["genero"] not in gender_atual:
            c += 1
            cursorSegundo.execute(
                "insert into gender (gender) values (%s)", (gender["genero"],)
            )
            connSegundo.commit()
except Exception as e:
    print("Erro ao inserir dados:", e)
finally:
    cursorSegundo.close()
    cursorPimeiro.close()
    print("total de inserts:", c)


# # Endereço
#
#
# #### pedir para adicionar numero e complemento


# ### Pegando infos do banco do primeiro ano


try:
    adress_primeiro = consulta("SELECT * FROM endereco", banco=connPrimeiro)

except Exception as e:
    print("Erro ao executar a consulta:", e)


# ### Pegando infos do banco do segundo ano


try:
    adress_atual = consulta("SELECT * FROM adress", banco=connSegundo)
    print(adress_atual)
    adress_atual = [
        (
            adress["street"],
            adress["number"],
            adress["complement"],
            adress["cep"],
            adress["state"],
            adress["country"],
        )
        for adress in adress_atual
    ]

except Exception as e:
    print("Erro ao executar a consulta:", e)


try:
    connPrimeiro.rollback()
    connSegundo.rollback()
    cursorPimeiro = connPrimeiro.cursor()
    cursorSegundo = connSegundo.cursor()
    c = 0
    for adress in adress_primeiro:
        if (
            adress["rua"],
            adress["numero"],
            adress["complemento"],
            adress["cep"],
            adress["estado"],
            adress["cidade"],
        ) not in adress_atual:
            cursorSegundo.execute(
                'insert into ADRESS(street, "number", complement, "label", cep, state, country, deactivate) values(%s, %s, %s, %s,%s,%s,%s,%s)',
                (
                    adress["rua"],
                    adress["numero"],
                    adress["complemento"],
                    "a adicionar",
                    adress["cep"],
                    adress["estado"],
                    adress["cidade"],
                    "0",
                ),
            )
            c += 1
    connSegundo.commit()
except Exception as e:
    print("Erro ao inserir dados:", e)
finally:
    cursorSegundo.close()
    cursorPimeiro.close()
    print("Total de inserts:", c)


# # User
#


# ### Pegando dados do primeiro ano


try:
    usuarios_primeiro = consulta("SELECT * FROM USUARIO", banco=connPrimeiro)

except Exception as e:
    print("Erro ao executar a consulta:", e)


# ### Pegando dados do Segundo ano


try:
    user_atual = consulta("SELECT * FROM users", banco=connSegundo)
    print(user_atual)
    user_atual = [user["cpf"] for user in user_atual]


except Exception as e:
    print("Erro ao executar a consulta:", e)


try:
    connPrimeiro.rollback()
    connSegundo.rollback()
    cursorPimeiro = connPrimeiro.cursor()
    cursorSegundo = connSegundo.cursor()
    c = 0
    for users in usuarios_primeiro:
        idate = users["data_nascimento"]
        idade = (datetime.now().date() - idate) // 365
        idade = idade.days
        if users["cpf"] not in user_atual:
            print(users["cpf"])
            if users["genero"] == "F":
                gender_id = 2
            else:
                gender_id = 1
            if users["tp_cliente"] == "costureiro":
                is_dressmaker = "1"
            else:
                is_dressmaker = "0"
            if users["premium"]:
                premium_status = "1"
            else:
                premium_status = "0"
            telefone = users["telefone"].replace("(", "")
            telefone = telefone.replace(")", "")
            telefone = telefone.replace("-", "")
            cursorSegundo.execute(
                'insert into USERS ("name", cpf, gender_id, "age", is_dressmaker, "password", email, profile_picture_url, premium_status, phones, avaliation, is_admin) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',
                (
                    users["nome"],
                    users["cpf"],
                    gender_id,
                    idade,
                    is_dressmaker,
                    users["senha"][:8],
                    users["email"],
                    ["url_foto"],
                    premium_status,
                    telefone,
                    users["nota_avaliacao"],
                    "0",
                ),
            )
            c += 1
    connSegundo.commit()
except Exception as e:
    print("Erro ao inserir dados:", e)
finally:
    cursorSegundo.close()
    cursorPimeiro.close()
    print("Total de inserts:", c)


# # User_adress
#


# ### Pegando infos do banco do primeiro ano


try:
    usuarios_primeiro = consulta(
        "SELECT cpf, cep_endereco FROM USUARIO", banco=connPrimeiro
    )

except Exception as e:
    print("Erro ao executar a consulta:", e)


# ### Pegando infos do banco do segundo ano


try:
    user_atual = consulta("SELECT * FROM users", banco=connSegundo)
    user_atual = [user["cpf"] for user in user_atual]
    ceps = consulta("SELECT * FROM adress", banco=connSegundo)

    user_atual = [cep["cep"] for cep in ceps]
    cpf_user = [user["cpf"] for user in usuarios_primeiro]

    geral = []
    for i in range(0, len(cpf_user)):
        geral.append({cpf_user[i]: user_atual[i]})
    print(geral)
except Exception as e:
    print("Erro ao executar a consulta:", e)


try:
    relacao = consulta("SELECT * FROM user_adress", banco=connSegundo)
    print(relacao)
    relacao = [(rel["pfk_user_id"], rel["pfk_adress_id"]) for rel in relacao]

except Exception as e:
    print("Erro ao executar a consulta:", e)


try:
    connPrimeiro.rollback()
    connSegundo.rollback()
    cursorPimeiro = connPrimeiro.cursor()
    cursorSegundo = connSegundo.cursor()
    c = 0

    for user in usuarios_primeiro:
        cpf = user["cpf"]
        for g in geral:
            if list(g.keys())[0] == cpf:
                cep = g[cpf]
                cpf = user["cpf"]

                id_adress = consulta(
                    "select id from adress where cep = %s",
                    banco=connSegundo,
                    params=(cep,),
                )
                id_user = consulta(
                    "select id from users where cpf = %s",
                    banco=connSegundo,
                    params=(cpf,),
                )
                id_adress = id_adress[0]["id"]
                id_user = id_user[0]["id"]
                if (id_user, id_adress) not in relacao:
                    cursorSegundo.execute(
                        "INSERT into USER_ADRESS values(%s, %s)", (id_user, id_adress)
                    )
                    connSegundo.commit()
                    c += 1
except Exception as e:
    print("Erro ao inserir dados:", e)
finally:
    cursorSegundo.close()
    cursorPimeiro.close()
    print("Total de inserts:", c)


# # Produto


try:
    classificacao = consulta("SELECT * FROM CLASSIFICACAO", banco=connPrimeiro)
    print(classificacao)
    classificacao = {cal["id_produto"]: cal["id_categoria"] for cal in classificacao}


except Exception as e:
    print("Erro ao executar a consulta:", e)


try:
    produto_atual = consulta("SELECT * FROM produto", banco=connPrimeiro)
    print(produto_atual)
    for produto in produto_atual:
        produto["preco"] = float(produto["preco"])
        produto["avaliacao"] = float(produto["avaliacao"])


except Exception as e:
    print("Erro ao executar a consulta:", e)


try:
    db = conn["Khiata"]

    product = db["product"]
    ids = product.find({}, {"id": 1, "_id": 0})
    ids = [ids["id"] for ids in ids]
    ids = list(ids)
    c = 0
    for p in produto_atual:
        id = p["id"]
        if id not in ids:
            p["category"] = classificacao[id]
            p["name"] = p["nome"]
            del p["nome"]
            p["price"] = p["preco"]
            del p["preco"]
            p["id_dressmaker"] = p["id_costureiro"]
            del p["id_costureiro"]
            p["description"] = p["descricao"]
            del p["descricao"]
            p["avaliation"] = p["avaliacao"]
            del p["avaliacao"]
            p["size"] = p["tamanho"]
            del p["tamanho"]
            product.insert_one(p)
            c += 1

except Exception as e:
    print("Todos dados inseridos")
finally:
    print("Total de inserts:", c)


# # productType


try:
    categoria_atual = consulta("select * from CATEGORIA", banco=connPrimeiro)
    print(categoria_atual)

except Exception as e:
    print("Erro ao executar a consulta:", e)


try:
    category = db["productType"]
    ids = category.find({}, {"id": 1, "_id": 0})
    ids = [ids["id"] for ids in ids]
    ids = list(ids)

    c = 0
    for categ in categoria_atual:
        if categ["id"] not in ids:
            categ["category"] = categ["categoria"]
            del categ["categoria"]
            categ["id_administrator"] = categ["id_administrador"]
            del categ["id_administrador"]
            category.insert_one(categ)
            c += 1
except Exception as e:
    print("Todos dados inseridos")

finally:
    print("Total de inserts:", c)


# # Preferences


try:
    categoria_atual = consulta(
        "select id_usuario, id_categoria from preferencias", banco=connPrimeiro
    )
    print(categoria_atual)

except Exception as e:
    print("Erro ao executar a consulta:", e)


category = db["productType"]


connSegundo = psycopg2.connect(**connection_params_segundo)
connPrimeiro = psycopg2.connect(**connection_params_primeiro)
cursorPimeiro = connPrimeiro.cursor()
cursorSegundo = connSegundo.cursor()
c = 0
for categ in categoria_atual:

    user = consulta(
        "select cpf from usuario where id = %s",
        banco=connPrimeiro,
        params=(categ["id_usuario"],),
    )
    user = consulta(
        "select id from users where cpf = %s",
        banco=connSegundo,
        params=(user[0]["cpf"],),
    )
    user = user[0]["id"]

    id_category = categ["id_categoria"]

    id_category = dict(category.find_one({"id": id_category}, {"id": 1, "_id": 0}))[
        "id"
    ]
    cursorSegundo.execute(
        "INSERT into USER_PREFERENCE values(%s, %s)", (user, id_category)
    )
    connSegundo.commit()
    c += 1


cursorSegundo.close()
cursorPimeiro.close()


# # Favoritos


product = db["product"]

try:

    favorito_primeiro = consulta(
        "select id_usuario, id_produto from favorito", banco=connPrimeiro
    )
    print(favorito_primeiro)

except Exception as e:
    print("Erro ao executar a consulta:", e)


try:

    favorito_atual = consulta("select * from favorites", banco=connSegundo)
    print(favorito_atual)

except Exception as e:
    print("Erro ao executar a consulta:", e)


try:
    connPrimeiro.rollback()
    connSegundo.rollback()
    cursorPimeiro = connPrimeiro.cursor()
    cursorSegundo = connSegundo.cursor()
    c = 0

    for fav in favorito_primeiro:
        if {"pfk_user_id": user, "value": product} not in favorito_atual:
            name = product.find({"id": fav["id_produto"]}, {"name": 1, "_id": 0})
            name = [n["name"] for n in name]
            name = list(ids)
            cursorSegundo.execute(
                "insert into FAVORITES values (%s, %s)",
                (fav["id_usuario"], fav["id_produto"]),
            )
            connSegundo.commit()
            c += 1


except Exception as e:
    print("Erro ao inserir dados:", e)
finally:
    cursorSegundo.close()
    cursorPimeiro.close()
    print("Total de inserts:", c)
