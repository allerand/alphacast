import pandas as pd
import requests
from io import BytesIO
from dateutil.relativedelta import relativedelta


def descargar_archivo(url):
    response = requests.get(url)
    if response.status_code == 200:
        print("Archivo descargado exitosamente.")
        return BytesIO(response.content)
    else:
        raise Exception(f"Error al descargar el archivo. Código de estado: {response.status_code}")


def procesar_datos(excel_data):
    """
    Procesar los datos del archivo Excel y devolver un DataFrame transformado.
    """
    # Leer el archivo Excel
    df = pd.read_excel(excel_data, sheet_name=None)

    # Seleccionar la hoja "Nacional"
    if "Nacional" not in df:
        raise ValueError("La hoja 'Nacional' no está disponible en el archivo.")
    df_nacional = df["Nacional"].dropna(axis=1, how="all")

    # Obtener el año y el mes inicial
    anio = int(df_nacional.iloc[1, 3].split(" ")[1])  # Ejemplo: "Año 2017"
    mes = df_nacional.iloc[2, 3].capitalize()         # Ejemplo: "Junio"

    # Diccionario para traducir meses del español al inglés
    meses_traduccion = {
        "Enero": "January", "Febrero": "February", "Marzo": "March",
        "Abril": "April", "Mayo": "May", "Junio": "June",
        "Julio": "July", "Agosto": "August", "Septiembre": "September",
        "Octubre": "October", "Noviembre": "November", "Diciembre": "December"
    }
    mes_ingles = meses_traduccion.get(mes)
    if not mes_ingles:
        raise ValueError(f"Mes desconocido: {mes}")

    # Crear la fecha inicial
    fecha_inicial = pd.to_datetime(f"{mes_ingles} {anio}", format="%B %Y", errors="coerce")
    if pd.isna(fecha_inicial):
        raise ValueError("No se pudo crear la fecha inicial correctamente.")

    # Renombrar columnas
    df_nacional.columns = ["Region", "Product", "Unit"] + list(df_nacional.columns[3:])

    # Crear fechas mensuales desde `fecha_inicial`
    fechas = []
    current_date = fecha_inicial
    for col in df_nacional.columns[3:]:
        if pd.notna(col):
            fechas.append(current_date)
            current_date += relativedelta(months=1)
        else:
            fechas.append(None)
    df_nacional.columns = ["Region", "Product", "Unit"] + fechas
    df_nacional.columns = [col.date() if isinstance(col, pd.Timestamp) else col for col in df_nacional.columns]

    # Limpiar el DataFrame
    df_nacional = df_nacional.loc[4:]
    df_nacional = df_nacional[df_nacional["Product"].notna()].copy()

    # Derretir el DataFrame
    id_vars = ["Region", "Product", "Unit"]
    df_melted = pd.melt(
        df_nacional,
        id_vars=id_vars,
        var_name="Date",
        value_name="Price"
    )

    # Convertir la columna 'Date' a formato datetime
    df_melted["Date"] = pd.to_datetime(df_melted["Date"], format="%Y-%m-%d", errors="coerce")

    # Reordenar las columnas para que la primera sea "Date"
    df_melted = df_melted[["Date", "Region", "Product", "Unit", "Price"]]

    return df_melted


def construir_nombre_archivo(primer_mes, ultimo_mes):
    inicio = primer_mes.strftime("%Y-%m") 
    fin = ultimo_mes.strftime("%Y-%m")
    return f"Inflation_CPI_SelectedPrices_Region_{inicio}_to_{fin}.csv"

def main():
    url = "https://www.indec.gob.ar/ftp/cuadros/economia/sh_ipc_precios_promedio.xls"
    
    try:
        excel_data = descargar_archivo(url)
        df_melted = procesar_datos(excel_data)

        # Obtener el rango temporal para nombrar el archivo
        primer_mes = df_melted["Date"].min()
        ultimo_mes = df_melted["Date"].max()
        nombre_archivo = construir_nombre_archivo(primer_mes, ultimo_mes)

        df_melted.to_csv(nombre_archivo, index=False)
        print(f"Datos procesados y guardados en '{nombre_archivo}'.")
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    main()
