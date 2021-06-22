#!/usr/bin/env python
# coding: utf-8

# 1.Popular um banco de dados a partir dos dados crus da pesquisa:

# In[388]:


import pandas as pd
import pyodbc


# In[389]:


dados = pd.read_csv('base_de_respostas_10k_amostra.csv')
ColunasSelecionadas = ['Respondent','Country','Salary','ConvertedSalary','CommunicationTools',
                       'LanguageWorkedWith','LanguageDesireNextYear','OperatingSystem',
                       'CompanySize', 'OpenSource','Hobby']
DadosSelecionados = dados.filter(items=ColunasSelecionadas)


# In[390]:


#Conectar servidor do banco de dados SQLServer
server = 'DESKTOP-MTOME' 
database = 'DesafioTecnicoGS' 
username = 'sa' 
password = 'ticom3p@$$#02#' 
conn = pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = conn.cursor()


# In[391]:


#### Inserir Pais
NaoCadastrado_DF = pd.DataFrame(data=[['Não Cadastrado']], columns=['Pais'])
ListaPais = DadosSelecionados['Country'].unique()
Pais_DF = pd.DataFrame(data=ListaPais, columns=['Pais'])
Pais_DF = NaoCadastrado_DF.append(Pais_DF)
Pais_DF = Pais_DF.dropna()
display(Pais_DF)
for index, row in Pais_DF.iterrows():
    cursor.execute("INSERT INTO DesafioTecnicoGS.dbo.Pais (nome) VALUES(?)", row.Pais)
conn.commit()


# In[392]:


#### Inserir linguagem_programacao
LinguagemProgramacao = DadosSelecionados['LanguageWorkedWith'].unique()
LinguagemProgramacao_DF1 = pd.DataFrame(data=LinguagemProgramacao, columns=['LinguagemProgramacao'])
LinguagemProgramacao = DadosSelecionados['LanguageDesireNextYear'].unique()
LinguagemProgramacao_DF2 = pd.DataFrame(data=LinguagemProgramacao, columns=['LinguagemProgramacao'])
LinguagemProgramacao_DF = LinguagemProgramacao_DF1.append(LinguagemProgramacao_DF2)
LinguagemProgramacao_DF = LinguagemProgramacao_DF.dropna()
#display(LinguagemProgramacaos_DF)

# Separar linhas que tem mais que uma linguagem obtendo apenas não duplicados
ListaCompleta = []
for index, row in LinguagemProgramacao_DF.iterrows():
    Separado = row.LinguagemProgramacao
    Separado = Separado.split(';')
    #Verificar se tem na lista. Se não tem adiciona, se já tem verifica o próximo
    for item in Separado:
        if item not in ListaCompleta:
            ListaCompleta.append(item)
    ListaCompleta.sort()

#display(ListaCompleta)
# Inserir Linguagem de Programação
LinguagemNaoCadastrada = pd.DataFrame(data=[['Não Cadastrado']], columns=['LinguagemProgramacao'])
ListaCompleta_DF = pd.DataFrame(data=ListaCompleta, columns=['LinguagemProgramacao'])
ListaCompleta_DF = ListaCompleta_DF.dropna()
ListaCompleta_DF = LinguagemNaoCadastrada.append(ListaCompleta_DF)
for index, row in ListaCompleta_DF.iterrows():
    cursor.execute("INSERT INTO DesafioTecnicoGS.dbo.linguagem_programacao (nome) VALUES(?)", row.LinguagemProgramacao)
conn.commit()


# In[393]:


#### Inserir ferramenta_comunic
FerramentaComunicacao = DadosSelecionados['CommunicationTools'].unique()
FerramentaComunicacao_DF = pd.DataFrame(data=FerramentaComunicacao, columns=['FerramentaComunicacao'])
FerramentaComunicacao_DF = FerramentaComunicacao_DF.dropna()
#display(FerramentaComunicacao_DF)

# Separar linhas que tem mais que uma Ferramenta obtendo apenas não duplicados
ListaCompleta = []
for index, row in FerramentaComunicacao_DF.iterrows():
    Separado = row.FerramentaComunicacao
    Separado = Separado.split(';')
    #Verificar se tem na lista. Se não tem adiciona, se já tem verifica o próximo
    for item in Separado:
        if item not in ListaCompleta:
            ListaCompleta.append(item)
    ListaCompleta.sort()

#display(ListaCompleta)
# Inserir Ferramenta da Comunicação
FerramentaNaoCadastrada = pd.DataFrame(data=[['Não Cadastrada']], columns=['FerramentaComunicacao'])
ListaCompleta_DF = pd.DataFrame(data=ListaCompleta, columns=['FerramentaComunicacao'])
ListaCompleta_DF = ListaCompleta_DF.dropna()
ListaCompleta_DF = FerramentaNaoCadastrada.append(ListaCompleta_DF)
for index, row in ListaCompleta_DF.iterrows():
    cursor.execute("INSERT INTO DesafioTecnicoGS.dbo.ferramenta_comunic (nome) VALUES(?)", row.FerramentaComunicacao)
conn.commit()


# In[394]:


#### Inserir sistema_operacional
SistemaOperacional = DadosSelecionados['OperatingSystem'].unique()
SistemaOperacional_DF = pd.DataFrame(data=SistemaOperacional, columns=['SistemaOperacional'])
SistemaOperacional_DF = SistemaOperacional_DF.dropna()
#display(SistemaOperacional_DF)

# Separar linhas que tem mais que um Sistema Operacional obtendo apenas não duplicados
ListaCompleta = []
for index, row in SistemaOperacional_DF.iterrows():
    Separado = row.SistemaOperacional
    Separado = Separado.split(';')
    #Verificar se tem na lista. Se não tem adiciona, se já tem verifica o próximo
    for item in Separado:
        if item not in ListaCompleta:
            ListaCompleta.append(item)
    ListaCompleta.sort()

# Inserir Sistema Operacional
SistemaOperacionalNaoCadastrado = pd.DataFrame(data=[['Não Cadastrado']], columns=['SistemaOperacional'])
ListaCompleta_DF = pd.DataFrame(data=ListaCompleta, columns=['SistemaOperacional'])
ListaCompleta_DF = ListaCompleta_DF.dropna()
ListaCompleta_DF = SistemaOperacionalNaoCadastrado.append(ListaCompleta_DF)
for index, row in ListaCompleta_DF.iterrows():
    cursor.execute("INSERT INTO DesafioTecnicoGS.dbo.sistema_operacional (nome) VALUES(?)", row.SistemaOperacional)
conn.commit()


# In[395]:


#### Inserir empresa (tamanho da empresa)
Empresa = DadosSelecionados['CompanySize'].unique()
Empresa_DF = pd.DataFrame(data=Empresa, columns=['TamanhoEmpresa'])
Empresa_DF = Empresa_DF.dropna()

# Separar linhas que tem mais que um Tamanho da Empresa obtendo apenas não duplicados
ListaCompleta = []
for index, row in Empresa_DF.iterrows():
    Separado = row.TamanhoEmpresa
    Separado = Separado.split(';')
    #Verificar se tem na lista. Se não tem adiciona, se já tem verifica o próximo
    for item in Separado:
        if item not in ListaCompleta:
            ListaCompleta.append(item)
    ListaCompleta.sort()

# Inserir empresa (tamanho da empresa)
EmpresaNaoCadastrada = pd.DataFrame(data=[['Não Cadastrada']], columns=['TamanhoEmpresa'])
ListaCompleta_DF = pd.DataFrame(data=ListaCompleta, columns=['TamanhoEmpresa'])
ListaCompleta_DF = ListaCompleta_DF.dropna()
ListaCompleta_DF = EmpresaNaoCadastrada.append(ListaCompleta_DF)
for index, row in ListaCompleta_DF.iterrows():
    cursor.execute("INSERT INTO DesafioTecnicoGS.dbo.empresa (tamanho) VALUES(?)", row.TamanhoEmpresa)
conn.commit()


# In[396]:


# Preparar informações para Inserir respondente
Respondente_DF = DadosSelecionados

#Substituir os valores NaN por 0 na coluna Salary e ConvertedSalary por ZERO
Respondente_DF['ConvertedSalary'].fillna(0, inplace = True)

#Criar a coluna Salario
Respondente_DF['Salario'] = (Respondente_DF['ConvertedSalary'] / 12) * 5.6

#Respondente_DF = Respondente_DF.where(Respondente_DF.notnull(), None)
#display(Respondente_DF)


# In[397]:


#### Inserir Respondentes

#Monta Lista de Sistema Operacional, Pais e Tamanho da Empresa
SistemaOperacional = pd.read_sql_query('''select id as IdSistemaOperacional, nome as OperatingSystem from sistema_operacional''', conn)

Pais = pd.read_sql_query('''select id as IdPais, nome as Country from pais''', conn)
Empresa = pd.read_sql_query('''select id as IdEmpresa, tamanho as CompanySize from empresa''', conn)
#Adiciona no Data Frame o Sistema Operacional, Pais e Tamanho da Empresa
Respondente_DF = pd.merge(Respondente_DF, SistemaOperacional, how="left", on='OperatingSystem')
Respondente_DF = pd.merge(Respondente_DF, Pais, how="left", on="Country")
Respondente_DF = pd.merge(Respondente_DF, Empresa, how="left", on="CompanySize")

#Substituir os valores NaN por 1 = Não Cadastrado
Respondente_DF['IdSistemaOperacional'].fillna(1, inplace = True)
Respondente_DF['IdPais'].fillna(1, inplace = True)
Respondente_DF['IdEmpresa'].fillna(1, inplace = True)

#display(Respondente_DF.shape)

#Inserir os Respondentes
for index, row in Respondente_DF.iterrows():
    identificador = row.Respondent
    nome = 'respondent_' + str(row.Respondent)
    contrib_open_source =  row.OpenSource
    programa_hobby =  row.Hobby
    
    salario = (row.ConvertedSalary / 12) * 5.6
    
    sistema_operacional_id = row.IdSistemaOperacional
    pais_id = row.IdPais
    empresa_id = row.IdEmpresa
    
    cursor.execute('''INSERT INTO DesafioTecnicoGS.dbo.respondente (
                    id, nome, contrib_open_source, programa_hobby, salario, sistema_operacional_id, pais_id, empresa_id) 
                    VALUES(?,?,?,?,?,?,?,?)
                    ''', identificador, nome, contrib_open_source, programa_hobby, salario, sistema_operacional_id, pais_id, empresa_id)
conn.commit()


# In[398]:


#### Inserir resp_usa_linguagem --> WorkedWith
Linguagem_DF = Respondente_DF.dropna()
for index, row in Linguagem_DF.iterrows():
    #Passo 1
    # inserir resp_usa_linguagem (WorkedWith) do Respondente
    momento = 1 #Language --> WorkedWith
    Separado = row.LanguageWorkedWith
    Separado = Separado.split(';')
    for item in Separado:
        identificador = int(row.Respondent)
        #Busca o id da Linguagem de Programação
        ComandoSQL = "select id, nome as linguagem_programacao from linguagem_programacao where nome = '" +  item + "'"
        LinguagemProgramacao = pd.read_sql_query(ComandoSQL, conn)
        LinguagemProgramacaoId = int(LinguagemProgramacao.loc[0, 'id'])
       
        cursor.execute('''INSERT INTO DesafioTecnicoGS.dbo.resp_usa_linguagem (linguagem_programacao_id, respondente_id, momento)
                    VALUES(?,?,?)
                    ''', LinguagemProgramacaoId, identificador, momento)  
    #Passo 2    
    # inserir resp_usa_linguagem (DesireNextYear) do Respondente
    momento = 2 #Language --> DesireNextYear
    Separado = row.LanguageDesireNextYear
    Separado = Separado.split(';')
    for item in Separado:
        identificador = int(row.Respondent)
        #Busca o id da Linguagem de Programação
        ComandoSQL = "select id, nome as linguagem_programacao from linguagem_programacao where nome = '" +  item + "'"
        LinguagemProgramacao = pd.read_sql_query(ComandoSQL, conn)
        LinguagemProgramacaoId = int(LinguagemProgramacao.loc[0, 'id'])
       
        cursor.execute('''INSERT INTO DesafioTecnicoGS.dbo.resp_usa_linguagem (linguagem_programacao_id, respondente_id, momento)
                    VALUES(?,?,?)
                    ''', LinguagemProgramacaoId, identificador, momento)           
conn.commit()


# In[399]:


#### Inserir resp_usa_ferramenta
Ferramenta_DF = Respondente_DF.dropna()
for index, row in Ferramenta_DF.iterrows():
    Separado = row.CommunicationTools
    Separado = Separado.split(';')
    for item in Separado:
        identificador = int(row.Respondent)
        #Busca o id da Ferramenta de comunicação
        ComandoSQL = "select id, nome as ferramenta_comunic from ferramenta_comunic where nome = '" +  item + "'"
        FerramentaComunicacao = pd.read_sql_query(ComandoSQL, conn)
        FerramentaComunicacaoId = int(FerramentaComunicacao.loc[0, 'id'])
       
        cursor.execute('''INSERT INTO DesafioTecnicoGS.dbo.resp_usa_ferramenta (ferramenta_comunic_id, respondente_id)
                    VALUES(?,?)
                    ''', FerramentaComunicacaoId, identificador)            
conn.commit()


# 2. Realizar consultas no banco de dados (DATAFRAME) para responder as perguntas:

# In[401]:


# 1.Qual a quantidade de respondentes de cada país?
QtdeRespondentePorPais = dados['Country'].value_counts()
display(QtdeRespondentePorPais)
#Gravar no excel
Arquivo = 'C:/Users/TOME/Documents/_DesafioTecnicoGS/ArquivosSaida/Arquivo2.1.xlsx'
writer = pd.ExcelWriter(Arquivo)
QtdeRespondentePorPais.to_excel(writer, "Sheet1")
writer.save()


# In[402]:


# 2.Quantos usuários que moram em United States gostam de Windows?
UsuarioUS = Respondente_DF.loc[Respondente_DF['Country']=='United States']
UsuarioUSWindows = UsuarioUS.loc[UsuarioUS['OperatingSystem']=='Windows'] 
display(len(UsuarioUSWindows))


# In[403]:


# 3.Qual a média de salário dos usuários que moram em Israel e gostam de Linux?
UsuarioIsrael = Respondente_DF.loc[(Respondente_DF['Country']=='Israel') & (Respondente_DF['OperatingSystem']=='Linux-based')] 
display(UsuarioIsraelLinux['Salario'].mean())


# In[404]:


# 4.Qual a média e o desvio padrão do salário dos usuários que usam Slack para cada tamanho de empresa disponível?
UsuarioSlack = Respondente_DF[Respondente_DF['CommunicationTools'].str.contains('Slack', na=False)]
UsuarioSlack = UsuarioSlack[['CompanySize', 'Salario']]

#Média
Media_DF = UsuarioSlack.groupby('CompanySize').mean()
display(Media_DF)
#Gravar no excel
Arquivo = 'C:/Users/TOME/Documents/_DesafioTecnicoGS/ArquivosSaida/Arquivo2.4Media.xlsx'
writer = pd.ExcelWriter(Arquivo)
Media_DF.to_excel(writer, "Sheet1")
writer.save()

#Desvio Padrão
DesvioPadrao_DF = UsuarioSlack.groupby('CompanySize').std()
display(DesvioPadrao_DF)
#Gravar no excel
Arquivo = 'C:/Users/TOME/Documents/_DesafioTecnicoGS/ArquivosSaida/Arquivo2.4DesvioPadrao.xlsx'
writer = pd.ExcelWriter(Arquivo)
DesvioPadrao_DF.to_excel(writer, "Sheet1")
writer.save()


# 3. Realizar consultas no banco de dados (SQLSERVER) para responder as perguntas:

# In[405]:


Respondente_DFBD = pd.read_sql_query('''select id as respondente_id, nome, contrib_open_source, programa_hobby, salario, sistema_operacional_id, pais_id, empresa_id from respondente''', conn)
SistemaOperacional_DFBD = pd.read_sql_query('''select id as sistema_operacional_id, nome as OperatingSystem from sistema_operacional''', conn)
Pais_DFBD = pd.read_sql_query('''select id as pais_id, nome as Country from pais''', conn)
Empresa_DFBD = pd.read_sql_query('''select id as empresa_id, tamanho as CompanySize from empresa''', conn)

#Adiciona no Data Frame o Sistema Operacional, Pais e Tamanho da Empresa
Respondente_DFBD = Respondente_DFBD.merge(SistemaOperacional_DFBD)
Respondente_DFBD = Respondente_DFBD.merge(Pais_DFBD)
Respondente_DFBD = Respondente_DFBD.merge(Empresa_DFBD)
#display(Respondente_DFBD.head())


# In[409]:


#### 1.Qual a quantidade de respondentes de cada país?
QtdeRespondentePorPais = Respondente_DFBD['Country'].value_counts()
display(QtdeRespondentePorPais)
#Gravar no excel
Arquivo = 'C:/Users/TOME/Documents/_DesafioTecnicoGS/ArquivosSaida/Arquivo3.1.xlsx'
writer = pd.ExcelWriter(Arquivo)
QtdeRespondentePorPais.to_excel(writer, "Sheet1")
writer.save()


# In[406]:


#### 2.Quantos usuários que moram em "United States" gostam de Windows?
UsuarioUS = Respondente_DFBD.loc[Respondente_DFBD['Country']=='United States']
UsuarioUSWindows = UsuarioUS.loc[UsuarioUS['OperatingSystem']=='Windows'] 
display(len(UsuarioUSWindows))


# In[407]:


#### 3.Qual a média de salário dos usuários que moram em Israel e gostam de Linux?
UsuarioIsrael = Respondente_DFBD.loc[(Respondente_DFBD['Country']=='Israel') & (Respondente_DFBD['OperatingSystem']=='Linux-based')] 
display(UsuarioIsraelLinux['Salario'].mean())


# In[408]:


#### 4.Qual a média e o desvio padrão do salário dos usuários que usam Slack para cada tamanho de empresa disponível?
RespondenteUsaFerramenta_DFBD = pd.read_sql_query('''select ferramenta_comunic_id, respondente_id from resp_usa_ferramenta''', conn)
##Amarra tabela "resp_usa_ferramenta" com "respondente"
RespondenteUsaFerramenta_DFBD = RespondenteUsaFerramenta_DFBD.merge(Respondente_DFBD)

Ferramenta_DFBD = pd.read_sql_query('''select id as ferramenta_comunic_id, nome as FerramentaNome from ferramenta_comunic''', conn)
##Amarra ferramenta_comunic
RespondenteUsaFerramenta_DFBD = RespondenteUsaFerramenta_DFBD.merge(Ferramenta_DFBD)

##Localiza os usuarios que usam Slack
UsuarioSlack = RespondenteUsaFerramenta_DFBD.loc[RespondenteUsaFerramenta_DFBD['FerramentaNome']=='Slack']

##Amarra com tabela respondente
UsuarioSlack = UsuarioSlack.merge(Respondente_DFBD)
UsuarioSlack = UsuarioSlack[['CompanySize', 'salario']]

#Média
Media_DF = UsuarioSlack.groupby('CompanySize').mean()
display(Media_DF)
#Gravar no excel
Arquivo = 'C:/Users/TOME/Documents/_DesafioTecnicoGS/ArquivosSaida/Arquivo3.4Media.xlsx'
writer = pd.ExcelWriter(Arquivo)
Media_DF.to_excel(writer, "Sheet1")
writer.save()

#Desvio Padrão
DesvioPadrao_DF = UsuarioSlack.groupby('CompanySize').std()
display(DesvioPadrao_DF)
Arquivo = 'C:/Users/TOME/Documents/_DesafioTecnicoGS/ArquivosSaida/Arquivo3.4DesvioPadrao.xlsx'
writer = pd.ExcelWriter(Arquivo)
DesvioPadrao_DF.to_excel(writer, "Sheet1")
writer.save()


# In[ ]:




