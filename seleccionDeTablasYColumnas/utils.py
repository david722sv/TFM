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