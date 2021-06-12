def leer_csv(file, showInfo):
    """Esta función carga un csv como dataframe de pandas y elimina automaticamente las columnas que no aportan inforamcion, ya sea porque tiene todo valores nulos o porque el mismo valor se repite en todos los registros."""
    
    import pandas as pd
    if showInfo==-1:
        print("Archivo:", file)
        print("Se eliminaron 0 columnas.\n\n")
        return pd.read_csv(file)
    
    aux = pd.read_csv(file)
    isna = aux.isna().sum()
    
    c_inutiles = list(aux.columns[(isna == len(aux)) | ((isna==0) & (aux.nunique()==1))])
    c_utiles = set(aux.columns) - set(c_inutiles + ["HstryUserName","HstryTaskName","HstryDateTime"])
    
    ci = len(aux.columns)
    cf = len(c_utiles)
    if showInfo>=1:
        print("Archivo:", file)
        print("Se dejaron", cf, "columnas.")
        print("Se eliminaron", (ci-cf), "columnas:")
        
        if showInfo==2:
            for i in c_inutiles:
                line_new = '{:>30} ,  '.format(i) + "Valor:"
                print(line_new, aux[i].unique() )
        print("\n")
    return aux[c_utiles]



def info_tabla(tabla):
    """Esta función muestra información sobre cada columna del dataframe."""
    
    import matplotlib.pyplot as plt
    m, n = tabla.shape
    print("%.0f registros" % m)
    print("%.0f variables" % n)
    print("\n\n_______________________________________________________________")

    for i in range(n):
        datos = tabla.iloc[:,i]

        print("Variable:", datos.name)
        print("%.2f %% nans (%.0f/%.0f)" % (100*datos.isna().sum()/m, datos.isna().sum(), m) )
        print("%.2f %% valores únicos (%.0f/%.0f)"%(100*datos.nunique()/m, datos.nunique(), m ) )
        
        print("\nDistribución:")
        if datos.dtype == "object":
            vc = datos.value_counts()/m
            print(vc.iloc[0:5])
            if datos.nunique()>5:
                print("\nEl resto (%.0f):"%(vc.shape[0]-5), vc.iloc[5:].sum())
        elif datos.isna().sum() != m:
            plt.hist(datos, bins=100)
            plt.show()

        print("\n\n_______________________________________________________________")
        
        
def resumen_tabla(tabla, sort_key = "dtype"):
    import pandas as pd
    import numpy as np
    m, n = tabla.shape
    print("%.0f registros" % m)
    print("%.0f variables" % n)
    return pd.concat([np.round(100*tabla.isna().sum()/len(tabla),2), tabla.nunique(), np.round(100*tabla.nunique()/len(tabla),2) , tabla.dtypes], axis=1, keys=['nans', 'nunicos', "nunicos_r" , "dtype"]).sort_values(by = sort_key)




def limpiar_tabla(tabla, c_index, c_elim, c_fechas, c_num, nun_th, len_th, num_div):
    """
    Argumentos:
    -el dataframe a limpiar
    -el nombre de la columna que hace de indice principal
    -los nombres de las columnas que son seriales inutiles
    -los nombres de las columnas que son fechas
    -los nombres de las columnas que se quieren mantener como númericas, aunque tengan pocos valores únicos
    -un número indicando en cuantos tramos hay que dividir las variables numéricas para categorizarlas (los nans seran una categoria a parte), si es -1 se deja como variable numérica

    La funcion devuelve:
    -el dataframe limpio
    -los nombres de las columnas que son categoricas
    -los nombres de las columnas que han sido eliminadas (por tener siempre el mismo valor)
    -un dataframe con el resumen de la tabla
    """
    import numpy as np
    import pandas as pd
    from scipy import stats
    
    tabla = tabla.copy()
    c_categ = []

    # Revisa si hay columnas con el mismo nombre y las añade un número para diferenciar
    reps = tabla.columns.value_counts()[tabla.columns.value_counts() > 1].index
    cols = []
    count = 1
    for column in tabla.columns:
        if column in reps:
            cols.append(column+"_"+str(count))
            count+=1
            continue
        cols.append(column)
    tabla.columns = cols
    
    # Pone como índice de la tabla el indicado como argumento 'c_index'
    tabla.index = tabla[c_index]
    
    # Quita las columnas que no son útilas, indicadas en el argumento 'c_elim'
    tabla = tabla.drop(columns = c_elim + [c_index] )

    # Inicializa una matriz en la que mostrar un resumen de la tabla
    res = pd.DataFrame(np.nan,index=tabla.columns,columns=["Tipo","NumCategorias","NumValoresAgrupados","FrecRelValoresAgrupados","nans"])

    # Da formato a las columnas que son fechas, indicadas en 'c_fechas'
    for i in c_fechas:
        tabla[i] = pd.to_datetime(tabla[i])
        res.loc[i,"Tipo"] = "fecha"
        res.loc[i, "nans"] = tabla[i].isna().sum()
        
    # Las columnas indicadas como 'c_num' se dejan como numéricas, los nans se sustituyen por la mediana y se añade otra columna indicando si el valor de 'c_num' era nan
    for i in c_num:
        f_nans = tabla[i].isna()
        res.loc[i, "nans"] = f_nans.sum()
        res.loc[i,"Tipo"] = "numerica"
        if f_nans.sum() != 0:
            tabla[i+"_isNaN"] = 0
            tabla.loc[f_nans, i+"_isNaN"] = 1
            c_categ.append(i+"_isNaN")
            res.loc[i+"_isNaN","Tipo"] = "categorica"
            tabla.loc[f_nans, i] = tabla[i].median()
    
    
    columnas = list( (set(tabla.columns) - set(c_fechas)) - set(c_num) )
    
    # Recorta los strings largos: si un string tiene una longitud mayor que 'len_th' caracteres, deja el principio y le añade su hash al final, para evitar recortar strings que son diferentes pero tienen el mismo inicio.
    for i in columnas:
        if tabla[i].dtype == "object":
            muyLargo = tabla[i].str.len() > len_th
            tabla.loc[muyLargo, i] = tabla.loc[muyLargo,i].str.slice(stop=max(10, len_th-20))+"..."+tabla.loc[muyLargo, i].apply(hash).astype(str)

            
            
    for i in columnas:
        f_nans = tabla[i].isna()
        res.loc[i, "nans"] = f_nans.sum()

        if tabla[i].nunique() <= nun_th: # si tiene pocos valores unicos...
            c_categ.append(i)
            res.loc[i,"Tipo"] = "categorica" if tabla[i].dtype == "object" else "num -> categ"
            tabla.loc[f_nans, i] = "isNaN"
            
        else: # si tiene muchos valores unicos
            if tabla[i].dtype == "object": # agrupa los que menos se repiten en la misma categoria
                c_categ.append(i)
                res.loc[i,"Tipo"] = "categorica"
                tabla.loc[f_nans, i] = "isNaN"

                vc = tabla[i].value_counts()
                tabla.loc[tabla[i].isin(vc[nun_th:].index), i] = "elResto_%.0f_%.4f"%(len(vc)-nun_th, vc[nun_th:].sum()/vc.sum())
                if ~tabla[i].isin(vc[nun_th:].index).empty:
                    res.loc[i, "NumValoresAgrupados"] = len(vc)-nun_th
                    res.loc[i, "FrecRelValoresAgrupados"] = vc[nun_th:].sum()/vc.sum()
            else:
                if num_div > 0: # agrupa por percentiles
                    c_categ.append(i)
                    res.loc[i,"Tipo"] = "num -> categ"

                    quantiles = np.linspace(0, 1, num_div+1)
                    bin_edges = stats.mstats.mquantiles(tabla.loc[~f_nans, i], quantiles)
                    intervalo = pd.cut(tabla.loc[~f_nans, i], bin_edges, duplicates='drop', include_lowest=True)
                    tabla.loc[~f_nans, i] = intervalo.astype(str)
                    res.loc[i, "NumValoresAgrupados"] = intervalo.value_counts().mean()
                    tabla.loc[ f_nans, i] = "isNaN"
                else:
                    res.loc[i,"Tipo"] = "numerica"
                    if f_nans.sum() != 0:
                        tabla[i+"_isNaN"] = 0
                        tabla.loc[f_nans, i+"_isNaN"] = 1
                        c_categ.append(i+"_isNaN")
                        res.loc[i+"_isNaN","Tipo"] = "categorica"
                        tabla.loc[f_nans, i] = tabla[i].median()
    
    elim = tabla.nunique()[tabla.nunique()==1].index
    
    tabla = tabla.drop(columns = elim  )
    res["NumCategorias"] = tabla.nunique()
    res["nans"] = 100*(tabla == "isNaN").sum()/tabla.shape[0]
    
    c_categ = list(set(c_categ)-set(elim))
    
    return tabla, c_categ, elim, res.sort_values(by=["Tipo","NumCategorias","FrecRelValoresAgrupados"])




def rel_indices(idx1, idx2):
    """
    Recibe dos listas de índices referidos a la misma tabla y muestra el tamaño de la intersección y de las diferencias entre ambos conjuntos.
    """
    idx1 = set(idx1)
    idx2 = set(idx2)
    print("Longitud del primer set:", len(idx1))
    print("Longitud del segundo set:", len(idx2))

    inter = set.intersection(idx1,idx2)
    print("\nIndices comunes:", len(inter))
    
    l1 = set.difference(idx1, idx2)
    print("Indices que solo están en el primero:", len(l1))

    l2 = set.difference(idx2, idx1)
    print("Indices que solo están en el segundo:", len(l2))
    print("_____________________________________________\n")
    
    
    
    
    