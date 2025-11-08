# -*- coding: utf-8 -*-
import pandas as pd
from datetime import datetime
import psycopg2
import glob


#  CONEXIÓN A POSTGRESQL
conn = psycopg2.connect(
    host="localhost",
    database="control_asistencias",
    user="postgres",
    password=""
)
cur = conn.cursor()

#  ASEGURAR ÁREA Y CARRERA POR DEFECTO

cur.execute("SELECT COUNT(*) FROM areas")
if cur.fetchone()[0] == 0:
    cur.execute("INSERT INTO areas (nombre) VALUES ('Área general')")
    print("🆕 Se creó área por defecto (id=1).")

cur.execute("SELECT COUNT(*) FROM carreras")
if cur.fetchone()[0] == 0:
    cur.execute("INSERT INTO carreras (nombre) VALUES ('Carrera general')")
    print("🆕 Se creó carrera por defecto (id=1).")

conn.commit()


#  MES Y AÑO DEL REPORTE

mes = "agosto 2025"  # 🔹 Cambia según el mes que estás procesando
mes_nombre, mes_anio = mes.split()
mes_anio = int(mes_anio)


#  FUNCIONES AUXILIARES
def horas_a_minutos(hm):
    h, m = hm.replace("m", "").split("h")
    return int(h) * 60 + int(m)

def minutos_a_horas(mins):
    h, m = divmod(mins, 60)
    return f"{h}h{m:02d}m"


#  LEER ARCHIVOS CSV

archivos = sorted(glob.glob("C:\\Users\\saave\\Desktop\\horariosSql\\Reporte Mayo 2025\\asistencia*.csv"))
empleados_datos = {}

for archivo_actual in archivos:
    df = pd.read_csv(archivo_actual, header=None)
    bloques = [(0, 9), (15, 24), (30, 39)]

    for inicio, fin in bloques:
        val = df.iloc[2, inicio + 9]
        if isinstance(val, str) and val.strip() != "":
            nombre = val.strip()
            if nombre not in empleados_datos:
                empleados_datos[nombre] = df.iloc[11:, inicio:fin]

#  CALCULAR HORAS POR DÍA

def calcular_horas_por_dia(df):
    resultados = {}
    incompletos = []
    for _, row in df.iterrows():
        fecha = row.iloc[0]
        minutos_dia = 0
        horas = [h for h in row[1:] if isinstance(h, str) and ":" in h]
        for i in range(0, min(len(horas), 4), 2):
            try:
                t1 = datetime.strptime(horas[i], "%H:%M")
                t2 = datetime.strptime(horas[i + 1], "%H:%M")
                minutos_dia += (t2 - t1).seconds // 60
            except Exception:
                incompletos.append(fecha)
                continue
        if minutos_dia > 0:
            h, m = divmod(minutos_dia, 60)
            resultados[fecha] = f"{h}h{m:02d}m"
    return resultados, incompletos


#  CALCULAR HORAS TOTALES

todos_empleados = {}
for nombre, df_emp in empleados_datos.items():
    horas, incompletos = calcular_horas_por_dia(df_emp)
    total_minutos = sum(horas_a_minutos(hm) for hm in horas.values())
    total_str = minutos_a_horas(total_minutos)

    print(f"\n{nombre}:")
    for dia, hm in horas.items():
        print(f"  {dia}: {hm}")
    th, tm = divmod(total_minutos, 60)
    print(f"  Total: {th}h{tm:02d}m")
    if incompletos:
        print(f"⚠ Días con pares incompletos: {', '.join(incompletos)}")

    todos_empleados[nombre] = total_minutos


#  SUBIR DATOS A POSTGRESQL

respuesta = input("\n¿Deseas subir todos los datos a PostgreSQL? (s/n): ").strip().lower()
if respuesta == "s":
    for nombre, total_minutos in todos_empleados.items():
        # 1️⃣ Buscar el ID del prestador por su nombre
        cur.execute("SELECT id_prestador FROM prestadores WHERE nombre = %s", (nombre,))
        prestador = cur.fetchone()

        # Si el prestador no existe → se crea automáticamente con área/carrera genéricas
        if prestador is None:
            cur.execute("""
                INSERT INTO prestadores (nombre, id_carrera, id_area)
                VALUES (%s, 1, 1)
                RETURNING id_prestador
            """, (nombre,))
            id_prestador = cur.fetchone()[0]
            print(f"🆕 Se agregó nuevo prestador: {nombre} (id={id_prestador})")
        else:
            id_prestador = prestador[0]

        # 2️⃣ Verificar si ya existe registro para ese mes y año
        cur.execute("""
            SELECT COUNT(*) FROM asistencias_mensuales
            WHERE id_prestador = %s AND mes = %s AND año = %s
        """, (id_prestador, mes_nombre, mes_anio))
        existe = cur.fetchone()[0] > 0

        if existe:
            print(f"⚠ {nombre} ya tiene registro para {mes_nombre} {mes_anio}, se omite.")
        else:
            cur.execute("""
                INSERT INTO asistencias_mensuales (id_prestador, mes, año, horas_acumuladas)
                VALUES (%s, %s, %s, %s)
            """, (id_prestador, mes_nombre, mes_anio, total_minutos))
            print(f"✅ {nombre} subido correctamente.")

    conn.commit()
    print("\n✅ Todos los datos fueron guardados en PostgreSQL.")
else:
    print("\n❌ Ningún dato fue subido a la base SQL.")

# ===============================
#  CERRAR CONEXIÓN
# ===============================
cur.close()
conn.close()
