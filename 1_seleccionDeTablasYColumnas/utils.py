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
        print("%.2f %% nans" % (100*datos.isna().sum()/m))
        print("%.0f valores únicos (/m = %.2f %%)" % (datos.nunique(), 100*datos.nunique()/m))
        
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




def limpiar_tabla(tabla, c_index, c_seriales, c_fechas, nun_th, len_th, num_div):
    """
    1º Pone como índice del dataframe la columna indicada como argumento ('c_index')
    
    2º Quita las columnas que son seriales que no son utiles (indicadas en el argumento 'c_seriales')
    
    3º Pone formato de fecha a las columnas indicadas en el argumento 'c_fechas'

    4º Recorta los strings largos: si un string tiene una longitud mayor que 'len_th' caracteres, deja el principio y le añade su hash al final, para evitar recortar strings que son diferentes pero tienen el mismo inicio.

    5º Para todas las columnas: si tienen menos de 'nun_th' valores únicos: la deja sin tocar y añade los nans como otra categoría.

    6º Si tiene más de 'nun_th' valores únicos y es de tipo texto: añade los nans como otra categoria; ordena todas en funcion del nº de veces que aparecen y guarda las primeras como categoricas y agrupar el resto como una 11º categoria, como nombre se pone "elresto_a_b", donde 'a' es el número de categorias que agrupa y 'b' es la frecuencia relativa de todas estas categorias agrupadas.

    7º Si tiene más de 'nun_th' valores únicos y es numérica: dependiendo del valor de 'num_div':
    -Si es -1: lo deja como está, si hay nans los rellena con la mediana y añade otra columna con nombre: "nombreColumna_isnan" que tiene dos valores categoricos 0 o 1
    -Si 'num_div' es mayor que 1:
    divide los valores numéricos en categorías dadas por los percentiles


    Argumentos de la funcion:
    -el dataframe a limpiar
    -el nombre de la columna que hace de indice principal
    -los nombres de las columnas que son seriales inutiles
    -los nombres de las columnas que son fechas
    -un número indicando en cuantos tramos hay que dividir las variables numéricas para categorizarlas (los nans seran una categoria a parte), si es -1 se deja como variable numérica

    La funcion devuelve:
    -el dataframe limpio
    -los nombres de las columnas que son categoricas
    -los nombres de las columnas que son numéricas
    -un dataframe relacionando las columnas categoricas donde se hizo agrupacion de categorias con el nº de categorias agrupadas y la frec relativa del agrupamiento
    """
    
    import numpy as np
    import pandas as pd
    from scipy import stats
    
    tabla = tabla.copy()
    c_categ = []
    c_num = []

    tabla.index = tabla[c_index]
    tabla = tabla.drop(columns = c_seriales + [c_index] )

    res = pd.DataFrame(index = tabla.columns)
    res["Tipo"] = np.nan
    res["NumCategorias"] = np.nan
    res["NumValoresAgrupados"] = np.nan
    res["FrecRelValoresAgrupados"] = np.nan

    for i in c_fechas:
        tabla[i] = pd.to_datetime(tabla[i])
        res.loc[i,"Tipo"] = "fecha"

    columnas = list(set(tabla.columns) - set(c_fechas))
    
    for i in columnas:
        if tabla[i].dtype == "object":
            muyLargo = tabla[i].str.len() > len_th
            tabla.loc[muyLargo, i] = tabla.loc[muyLargo,i].str.slice(stop=max(10, len_th-20)) + "..." + tabla.loc[muyLargo, i].apply(hash).astype(str)

    for i in columnas:
        f_nans = tabla[i].isna()

        if tabla[i].nunique() <= nun_th:
            c_categ.append(i)
            res.loc[i,"Tipo"] = "categorica"
            tabla.loc[f_nans, i] = "isNaN"
        else:
            if tabla[i].dtype == "object":
                c_categ.append(i)
                res.loc[i,"Tipo"] = "categorica"
                tabla.loc[f_nans, i] = "isNaN"

                vc = tabla[i].value_counts()
                tabla.loc[tabla[i].isin(vc[nun_th:].index), i] = "elResto_%.0f_%.4f"%(len(vc)-nun_th, vc[nun_th:].sum()/vc.sum())
                if ~tabla[i].isin(vc[nun_th:].index).empty:
                    res.loc[i, "NumValoresAgrupados"] = len(vc)-nun_th
                    res.loc[i, "FrecRelValoresAgrupados"] = vc[nun_th:].sum()/vc.sum()
            else:
                if num_div > 0:
                    c_categ.append(i)
                    res.loc[i,"Tipo"] = "num -> categ"

                    quantiles = np.linspace(0, 1, num_div+1)
                    bin_edges = stats.mstats.mquantiles(tabla.loc[~f_nans, i], quantiles)
                    intervalo = pd.cut(tabla.loc[~f_nans, i], bin_edges, duplicates='drop')
                    tabla.loc[~f_nans, i] = tabla[i].name + "_" + intervalo.astype(str)
                    res.loc[i, "NumValoresAgrupados"] = intervalo.value_counts().mean()
                    tabla.loc[ f_nans, i] = "isNaN"
                else:
                    c_num.append(i)
                    res.loc[i,"Tipo"] = "numerica"
                    if f_nans.sum() != 0:
                        tabla[i+"_isNaN"] = 0
                        tabla.loc[f_nans, i+"_isNaN"] = 1
                        c_categ.append(i+"_isNaN")
                        res.loc[i+"_isNaN","Tipo"] = "categorica"

                        tabla.loc[f_nans, i] = tabla[i].median()
    
    for i in tabla.columns:
        res.loc[i, "NumCategorias"] = tabla[i].nunique()
        
    c_categ = list(set(c_categ)-set(c_fechas))
    c_num = list(set(c_num))
    
    return tabla, c_categ, c_num, res.sort_values(by=["Tipo","NumCategorias","FrecRelValoresAgrupados"])

    
