#!/usr/bin/env python
# coding: utf-8

# # Práctica Python - Tecnolgías de Procesamiento Big Data
# 
# + Autor: Felipe Navarro
# + Fecha: 29/11/2020

# ## Ejercicio 2

# In[2]:


import pandas as pd
import os
import unidecode
import re

from functools import reduce


# In[58]:


#conda activate tecnologias


# Cargamos los datos

# In[3]:


folder = "ine_activos"


# In[4]:


headers = os.listdir(folder)
headers = [os.path.splitext(each)[0] for each in headers]


# In[5]:


headers = [unidecode.unidecode(bytes(x,'UTF-8').decode('utf8',"strict")) for x in headers]


# In[6]:


tablas_ine = map(lambda x: pd.read_csv(os.path.join(folder,x),header=0),os.listdir(folder))


# In[7]:


ind = 0
for item in tablas_ine:
    if ind==0:
        df_ine = item
        df_ine=df_ine.rename(columns={"valor":headers[ind]})
        ind +=1
    else:
        df_ine = df_ine.merge(item, on="cod_provincia",how="left")
        df_ine=df_ine.rename(columns={"valor_x":headers[ind-1],"valor_y":headers[ind],"valor":headers[ind]})
        ind+=1


# In[8]:


df_ine.head()


# In[9]:


df_ine.shape


# In[10]:


df_provincias = pd.read_json("provincias.json")


# In[11]:


df_provincias.head()


# Hacemos un join para unir los datos de los dos dataframes

# In[12]:


df_ine = df_provincias.merge(df_ine,on="cod_provincia",how="inner")


# In[13]:


df_ine.head()


# Guardamos el dataframe en un csv, el encoding utf-8 es el que viene por defecto

# In[14]:


df_ine.to_csv("salidas/ine_activos.csv")


# Transformar el dataframe a un formato narrow

# In[15]:


df_narrow = df_ine.melt(id_vars =["ds_provincia","cod_provincia"])


# In[16]:


df_narrow.head()


# In[21]:


df_narrow["anio"] = df_narrow["variable"].apply(lambda x: pd.Series(re.search(r'\d{4}',str(x))[0]))


# In[32]:


df_narrow["anio"] = df_narrow["anio"].astype("int64")


# In[22]:


df_narrow["trimestre"] = df_narrow["variable"].apply(lambda x: pd.Series(re.search(r'(T\d)',"2008T1_Agricultura")[1][1]))


# In[23]:


df_narrow["trimestre"] = df_narrow["variable"].apply(lambda x: pd.Series(re.search(r'(T\d)',str(x))[1][1]))


# In[25]:


df_narrow["trimestre"] = df_narrow["trimestre"].astype("int64")


# In[26]:


df_narrow[["basura","sector"]] = df_narrow.variable.str.split("_",expand=True,)


# In[27]:


df_narrow = df_narrow.drop(columns=["basura","variable"])


# In[31]:


df_narrow.head()


# In[33]:


df_narrow.dtypes


# In[34]:


df_narrow.to_csv("salidas/narrow_data.csv")


# Cálculos con el dataframe narrow

# In[35]:


activos_anio = df_narrow[df_narrow["trimestre"]==4]


# In[36]:


activos_anio = activos_anio[["value","anio","sector"]].groupby(["anio","sector"]).sum()


# In[37]:


activos_anio.head(10)


# Pivotar el df_narrow y guardar como excel

# In[38]:


df_servicios = df_narrow[df_narrow["sector"]=="Servicios"]


# In[39]:


df_servicios = df_servicios.pivot_table(values = ["value"],index=["ds_provincia"],columns=["anio","trimestre"])


# In[40]:


df_servicios.head()


# In[41]:


df_servicios.to_excel("salidas/servicios.xlsx")


# Total de población activa en España para todos los sectores para cada año y trimestre

# In[42]:


pob_activa = df_narrow[["anio","trimestre","sector","value"]].groupby(["anio","trimestre"]).sum()


# In[43]:


pob_2009 = df_narrow[df_narrow["anio"] != 2008]


# In[44]:


pob_2009 = pob_2009[["anio","trimestre","sector","value"]].groupby(["anio","trimestre"]).sum()


# In[45]:


pob2 = df_narrow[df_narrow["anio"] != 2017]


# In[46]:


pob2 = pob2[["anio","trimestre","sector","value"]].groupby(["anio","trimestre"]).sum()


# In[47]:


pob_2009.reset_index(drop=True, inplace = True)
pob2.reset_index(drop=True, inplace = True)
pob2 = pob2.drop([34,35],0)


# In[48]:


index = pob_activa.index
pob_activa.reset_index(drop=True, inplace = True)


# In[49]:


pob_activa["value"] = (pob_2009["value"]-pob2["value"])/pob2["value"]


# In[50]:


pob_activa = pob_activa.set_index(index)


# In[51]:


pob_activa.head()


# In[52]:


pob_activa.to_json("salidas/tasas_poblacion_activa.json",orient="table")


# Exportar el entorno con anaconda -- Lo he ejecutado en la consola directamente

# In[53]:


#conda env export > env_tecnologias.yml


# In[57]:


#jupyter nbconvert --to python practica1_ejer2.ipynb.

