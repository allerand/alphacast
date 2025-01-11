import pandas as pd
import requests
from io import BytesIO
from dateutil.relativedelta import relativedelta
from alphacast import Alphacast


def descargar_archivo(url):
    response = requests.get(url)
    if response.status_code == 200:
        print("Archivo descargado exitosamente.")
        return BytesIO(response.content)
    else:
        raise Exception(f"Error al descargar el archivo. Código de estado: {response.status_code}")


def procesar_datos(excel_data):

    df = pd.read_excel(excel_data, sheet_name=None)
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


def subir_a_alphacast(df, api_key):
    alphacast = Alphacast(api_key)

    # Verificar si el repositorio ya existe
    repo_name = "INDEC Inflation Data"
    repo_description = "Datos del IPC de Argentina extraídos de INDEC - Hoja Nacional"
    repositorios = alphacast.repository.read_all()

    # Buscar el repositorio por nombre
    repo_id = None
    for repo in repositorios:
        if repo["name"] == repo_name:
            repo_id = repo["id"]
            print(f"Repositorio encontrado con ID: {repo_id}")
            break

    # Si el repositorio no existe, crearlo
    if not repo_id:
        repo_id = alphacast.repository.create(
            repo_name,  
            repo_description=repo_description,
            privacy="Private",
            returnIdIfExists=True
        )
        print(f"Repositorio creado con ID: {repo_id}")

    # Verificar si el dataset ya existe
    dataset_name = "IPC Nacional"
    datasets = alphacast.datasets.read_all()
    dataset_id = None
    for dataset in datasets:
        if dataset["name"] == dataset_name and dataset["repositoryId"] == repo_id:
            dataset_id = dataset["id"]
            print(f"Dataset encontrado con ID: {dataset_id}")
            break

    # Si el dataset no existe, crearlo
    if not dataset_id:
        dataset_description = "Precios por región, producto, unidad y fecha extraídos de la hoja 'Nacional' del IPC INDEC."
        dataset_id = alphacast.datasets.create(
            dataset_name,
            repo_id,
            dataset_description
        )
        print(f"Dataset creado con ID: {dataset_id}")

        # Inicializar columnas solo si el dataset es nuevo
        alphacast.datasets.dataset(dataset_id).initialize_columns(
            dateColumnName="Date",
            entitiesColumnNames=["Region", "Product", "Unit"],
            dateFormat="%Y-%m-%d"
        )
        print("Columnas inicializadas.")

    # Subir los datos al dataset
    alphacast.datasets.dataset(dataset_id).upload_data_from_df(
        df,
        deleteMissingFromDB=False,
        onConflictUpdateDB=True,
        uploadIndex=False
    )
    print("Datos subidos con éxito.")


def main():
    """
    Punto de entrada principal.
    """
    url = "https://www.indec.gob.ar/ftp/cuadros/economia/sh_ipc_precios_promedio.xls"
    api_key = "ak_wNg2Uhet4NNGvMgXyv7v" 

    try:
        # Descargar y procesar los datos
        excel_data = descargar_archivo(url)
        df_melted = procesar_datos(excel_data)

        # Subir datos a Alphacast
        subir_a_alphacast(df_melted, api_key)

    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    main()
